//! Integration tests for the slot-truncation utility (`truncate_slots`).
//!
//! Each test spins up an isolated Postgres container, seeds five blocks
//! (slots 1–5) with matching transactions and account_history rows, then
//! invokes `truncate_slots` with `keep_slots = 3`.  The expected outcome is
//! that slots 1 and 2 are pruned while slots 3–5 are retained.
//!
//! Scenarios covered:
//! 1. Apply mode  – rows are deleted, `first_available_block` metadata is
//!    written, and `AccountsDB::get_first_available_block()` returns 3.
//! 2. Dry-run mode – row counts and reported deletions match apply-mode values
//!    but no rows are actually removed and metadata is not mutated.

// Keep these in their own files for readability; `#[path]` wires them into
// this same test binary so they share compile state.
#[path = "test_truncate_backup_failure.rs"]
mod backup_failure;
#[path = "test_truncate_floor_atomicity.rs"]
mod floor_atomicity;
#[path = "test_truncate_lock_contention.rs"]
mod lock_contention;

use {
    anyhow::{anyhow, Context, Result},
    private_channel_core::accounts::{
        traits::BlockInfo,
        truncate::{truncate_slots, TruncateOptions},
        AccountsDB, PostgresAccountsDB,
    },
    solana_sdk::{hash::Hash, signature::Signature},
    sqlx::PgPool,
    std::{
        fs,
        path::{Path, PathBuf},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
};

async fn start_postgres(
    db_name: &str,
) -> Result<(PostgresAccountsDB, testcontainers::ContainerAsync<Postgres>)> {
    let container = Postgres::default()
        .with_db_name(db_name)
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .context("Failed to start PostgreSQL test container")?;

    let host = container
        .get_host()
        .await
        .context("Failed to resolve PostgreSQL container host")?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .context("Failed to resolve PostgreSQL container port")?;
    let db_url = format!("postgres://postgres:password@{}:{}/{}", host, port, db_name);
    let db = PostgresAccountsDB::new(&db_url, false)
        .await
        .map_err(|e| anyhow!("Failed to initialize PostgresAccountsDB: {}", e))?;

    Ok((db, container))
}

/// A fresh directory for one test's fixture files; `cleanup_backup_artifact` removes it.
fn fixture_dir(test_name: &str) -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before UNIX_EPOCH")?
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "private_channel_truncate_{}_{}_{}",
        test_name,
        std::process::id(),
        unique
    ));

    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "Failed to create backup fixture directory {}",
            dir.display()
        )
    })?;
    Ok(dir)
}

/// A fresh file that is not a dump, the fixture SOLA6-107 showed passing the old gate.
fn create_backup_artifact(test_name: &str) -> Result<PathBuf> {
    let backup_file = fixture_dir(test_name)?.join("backup.dump");
    fs::write(&backup_file, b"fixture-backup")
        .with_context(|| format!("Failed to write backup fixture {}", backup_file.display()))?;
    Ok(backup_file)
}

/// Dump `db` from inside its test container with `args`, as an operator would.
fn container_pg_dump(
    container: &testcontainers::ContainerAsync<Postgres>,
    db: &str,
    args: &[&str],
    test_name: &str,
) -> Result<PathBuf> {
    let path = fixture_dir(test_name)?.join("ledger.dump");
    let status = std::process::Command::new("docker")
        .args(["exec", container.id(), "pg_dump", "-U", "postgres"])
        .args(args)
        .arg(db)
        .stdout(fs::File::create(&path)?)
        .status()
        .context("Failed to run pg_dump in the test container")?;
    anyhow::ensure!(status.success(), "pg_dump failed in the test container");
    Ok(path)
}

/// A `pg_restore` that runs inside the test container, so the host needs no client tools.
fn container_pg_restore_bin(
    container: &testcontainers::ContainerAsync<Postgres>,
    test_name: &str,
) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let path = fixture_dir(test_name)?.join("pg_restore");
    fs::write(
        &path,
        format!(
            "#!/bin/sh\nexec docker exec -i {} pg_restore \"$@\"\n",
            container.id()
        ),
    )?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    Ok(path)
}

fn sha256_of(path: &Path) -> Result<String> {
    let out = std::process::Command::new("sha256sum").arg(path).output()?;
    Ok(String::from_utf8(out.stdout)?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string())
}

fn cleanup_backup_artifact(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::remove_dir_all(parent);
    }
}

fn build_block(slot: u64, previous_blockhash: Hash, signature: Signature) -> BlockInfo {
    let blockhash = Hash::new_unique();
    BlockInfo {
        slot,
        blockhash,
        previous_blockhash,
        parent_slot: slot.saturating_sub(1),
        block_height: Some(slot),
        block_time: Some(slot as i64),
        transaction_signatures: vec![signature],
        transaction_recent_blockhashes: vec![blockhash],
        transaction_message_hashes: vec![Hash::new_unique()],
    }
}

async fn seed_fixture(pool: &PgPool) -> Result<Vec<Signature>> {
    sqlx::query(
        "CREATE TABLE account_history (
            id BIGSERIAL PRIMARY KEY,
            slot BIGINT NOT NULL,
            data BYTEA NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("Failed to create account_history fixture table")?;

    let signatures: Vec<Signature> = (0..5).map(|_| Signature::new_unique()).collect();
    let mut previous_blockhash = Hash::default();

    for (idx, signature) in signatures.iter().enumerate() {
        let slot = (idx + 1) as u64;
        let block = build_block(slot, previous_blockhash, *signature);
        previous_blockhash = block.blockhash;
        let block_data = bincode::serialize(&block).context("Failed to serialize fixture block")?;

        sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(block_data)
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert fixture block at slot {}", slot))?;

        sqlx::query("INSERT INTO transactions (signature, data) VALUES ($1, $2)")
            .bind(signature.as_ref().to_vec())
            .bind(vec![slot as u8])
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert fixture transaction at slot {}", slot))?;

        sqlx::query("INSERT INTO account_history (slot, data) VALUES ($1, $2)")
            .bind(slot as i64)
            .bind(vec![slot as u8])
            .execute(pool)
            .await
            .with_context(|| format!("Failed to insert account_history row at slot {}", slot))?;
    }

    Ok(signatures)
}

async fn count_rows(pool: &PgPool, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .fetch_one(pool)
        .await
        .with_context(|| format!("Failed to count rows in {}", table))?;
    Ok(count)
}

/// Verifies that `truncate_slots` with `dry_run = false` physically deletes
/// blocks/transactions/account_history rows below the keep-slots threshold,
/// updates the `first_available_block` metadata row, and that
/// `AccountsDB::get_first_available_block()` reflects the new minimum slot.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_apply_mode_e2e() -> Result<()> {
    let (db, _container) = start_postgres("truncate_apply").await?;
    let pool = db.pool.clone();
    let signatures = seed_fixture(pool.as_ref()).await?;
    let backup_path = container_pg_dump(&_container, "truncate_apply", &["-Fc"], "apply")?;
    let restore_bin = container_pg_restore_bin(&_container, "apply")?;

    let options = TruncateOptions {
        keep_slots: 3,
        pg_dump_path: Some(backup_path.clone()),
        pg_restore_bin: restore_bin.clone(),
        batch_size: 2,
        dry_run: false,
    };

    let report = truncate_slots(&db, &options).await?;

    assert_eq!(report.latest_slot, Some(5));
    assert_eq!(report.truncate_before_slot, Some(3));
    assert_eq!(report.blocks_deleted, 2);
    assert_eq!(report.transactions_deleted, 2);
    assert_eq!(report.account_history_rows_deleted, 2);
    assert_eq!(report.first_available_block, Some(3));
    assert!(report.backup_check.pg_dump_ok);
    assert!(report.backup_check.has_valid_backup());
    assert_eq!(report.backup_check.sha256, Some(sha256_of(&backup_path)?));

    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 3);
    assert_eq!(count_rows(pool.as_ref(), "transactions").await?, 3);
    assert_eq!(count_rows(pool.as_ref(), "account_history").await?, 3);

    let remaining_min_slot = sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(slot) FROM blocks")
        .fetch_one(pool.as_ref())
        .await?
        .expect("Expected remaining blocks after truncation");
    assert_eq!(remaining_min_slot, 3);

    for signature in signatures.iter().take(2) {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM transactions WHERE signature = $1")
                .bind(signature.as_ref().to_vec())
                .fetch_one(pool.as_ref())
                .await?;
        assert_eq!(exists, 0, "Old transaction should be deleted");
    }

    let metadata = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_one(pool.as_ref())
    .await?
    .expect("Expected first_available_block metadata to be set");
    assert_eq!(metadata.len(), 8);
    assert_eq!(
        u64::from_le_bytes(
            metadata
                .as_slice()
                .try_into()
                .expect("metadata len checked")
        ),
        3
    );

    let accounts_db = AccountsDB::Postgres(db.clone());
    assert_eq!(accounts_db.get_first_available_block().await?, 3);

    cleanup_backup_artifact(&backup_path);
    cleanup_backup_artifact(&restore_bin);
    Ok(())
}

/// Verifies that `truncate_slots` with `dry_run = true` reports the correct
/// deletion counts (matching what apply-mode would remove) but leaves all rows
/// intact and does not write the `first_available_block` metadata entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_dry_run_e2e() -> Result<()> {
    let (db, _container) = start_postgres("truncate_dry_run").await?;
    let pool = db.pool.clone();
    seed_fixture(pool.as_ref()).await?;
    let backup_path = container_pg_dump(&_container, "truncate_dry_run", &["-Fc"], "dry_run")?;
    let restore_bin = container_pg_restore_bin(&_container, "dry_run")?;

    let options = TruncateOptions {
        keep_slots: 3,
        pg_dump_path: Some(backup_path.clone()),
        pg_restore_bin: restore_bin.clone(),
        batch_size: 2,
        dry_run: true,
    };

    let report = tokio::time::timeout(Duration::from_secs(10), truncate_slots(&db, &options))
        .await
        .expect("dry-run truncation timed out")
        .context("dry-run truncation returned an error")?;

    assert_eq!(report.blocks_deleted, 2);
    assert_eq!(report.transactions_deleted, 2);
    assert_eq!(report.account_history_rows_deleted, 2);
    assert!(report.backup_check.pg_dump_ok);
    assert_eq!(report.backup_check.sha256, Some(sha256_of(&backup_path)?));

    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 5);
    assert_eq!(count_rows(pool.as_ref(), "transactions").await?, 5);
    assert_eq!(count_rows(pool.as_ref(), "account_history").await?, 5);

    let metadata = sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT value FROM metadata WHERE key = 'first_available_block'",
    )
    .fetch_optional(pool.as_ref())
    .await?
    .flatten();
    assert!(metadata.is_none(), "dry-run must not mutate metadata");

    cleanup_backup_artifact(&backup_path);
    cleanup_backup_artifact(&restore_bin);
    Ok(())
}

/// SOLA6-107: any fresh file used to pass the gate, even one that is not a dump.
#[tokio::test(flavor = "multi_thread")]
async fn fresh_arbitrary_file_is_refused() -> Result<()> {
    let (db, _container) = start_postgres("truncate_arbitrary").await?;
    let pool = db.pool.clone();
    seed_fixture(pool.as_ref()).await?;
    let backup_path = create_backup_artifact("arbitrary")?;
    let restore_bin = container_pg_restore_bin(&_container, "arbitrary")?;

    let options = TruncateOptions {
        keep_slots: 3,
        pg_dump_path: Some(backup_path.clone()),
        pg_restore_bin: restore_bin.clone(),
        batch_size: 2,
        dry_run: false,
    };
    let result = truncate_slots(&db, &options).await;

    assert!(
        result.is_err(),
        "an arbitrary file must not authorize deletion"
    );
    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 5);
    cleanup_backup_artifact(&backup_path);
    cleanup_backup_artifact(&restore_bin);
    Ok(())
}

/// SOLA6-51: fresh WAL archiving alone used to pass the gate with no base backup.
#[tokio::test(flavor = "multi_thread")]
async fn no_dump_is_refused() -> Result<()> {
    use testcontainers::ImageExt;
    let container = Postgres::default()
        .with_db_name("truncate_wal_only")
        .with_user("postgres")
        .with_password("password")
        .with_cmd([
            "postgres",
            "-c",
            "archive_mode=on",
            "-c",
            "archive_command=cp %p /tmp/%f",
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:password@{host}:{port}/truncate_wal_only");
    let db = PostgresAccountsDB::new(&url, false)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let pool = db.pool.clone();
    seed_fixture(pool.as_ref()).await?;
    // Archive a segment so WAL recency looks healthy.
    sqlx::query("SELECT pg_switch_wal()")
        .execute(pool.as_ref())
        .await?;
    for _ in 0..50 {
        let archived: Option<i64> =
            sqlx::query_scalar("SELECT archived_count FROM pg_stat_archiver")
                .fetch_one(pool.as_ref())
                .await?;
        if archived.unwrap_or(0) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let options = TruncateOptions {
        keep_slots: 3,
        pg_dump_path: None,
        pg_restore_bin: PathBuf::from("pg_restore"),
        batch_size: 2,
        dry_run: false,
    };
    let result = truncate_slots(&db, &options).await;

    assert!(
        result.is_err(),
        "WAL recency alone must not authorize deletion"
    );
    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, 5);
    Ok(())
}

/// Seed the fixture, dump it with `dump_args`, let `after_dump` change the live ledger,
/// then truncate with keep_slots 3. Every case here must refuse and delete nothing.
async fn assert_dump_refused(
    db_name: &str,
    dump_args: &[&str],
    after_dump: impl AsyncFnOnce(&PgPool, &Path) -> Result<()>,
) -> Result<()> {
    assert_dump_refused_with(db_name, dump_args, async |_| Ok(()), after_dump).await
}

/// The same, with `before_dump` changing the ledger the dump is taken of.
async fn assert_dump_refused_with(
    db_name: &str,
    dump_args: &[&str],
    before_dump: impl AsyncFnOnce(&PgPool) -> Result<()>,
    after_dump: impl AsyncFnOnce(&PgPool, &Path) -> Result<()>,
) -> Result<()> {
    let (db, container) = start_postgres(db_name).await?;
    let pool = db.pool.clone();
    seed_fixture(pool.as_ref()).await?;
    before_dump(pool.as_ref()).await?;
    let dump = container_pg_dump(&container, db_name, dump_args, db_name)?;
    let restore_bin = container_pg_restore_bin(&container, db_name)?;
    after_dump(pool.as_ref(), &dump).await?;
    let blocks_before = count_rows(pool.as_ref(), "blocks").await?;

    let options = TruncateOptions {
        keep_slots: 3,
        pg_dump_path: Some(dump.clone()),
        pg_restore_bin: restore_bin.clone(),
        batch_size: 2,
        dry_run: false,
    };
    let result = truncate_slots(&db, &options).await;

    let err = result.expect_err("the dump must not authorize deletion");
    assert!(
        err.to_string().contains("Backup verification failed"),
        "{db_name}: {err}"
    );
    assert_eq!(count_rows(pool.as_ref(), "blocks").await?, blocks_before);
    assert_eq!(count_rows(pool.as_ref(), "account_history").await?, 5);
    cleanup_backup_artifact(&dump);
    cleanup_backup_artifact(&restore_bin);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn dump_of_another_ledger_is_refused() -> Result<()> {
    assert_dump_refused("truncate_other_ledger", &["-Fc"], async |pool, _| {
        sqlx::query("UPDATE metadata SET value = '\\x99' WHERE key = 'deployment_id'")
            .execute(pool)
            .await?;
        Ok(())
    })
    .await
}

/// Blocks written after the dump are doomed but not in it, so the counts cannot match.
#[tokio::test(flavor = "multi_thread")]
async fn dump_older_than_doomed_rows_is_refused() -> Result<()> {
    assert_dump_refused("truncate_stale_dump", &["-Fc"], async |pool, _| {
        for slot in 6..=9_u64 {
            let block = build_block(slot, Hash::default(), Signature::new_unique());
            sqlx::query("INSERT INTO blocks (slot, data) VALUES ($1, $2)")
                .bind(slot as i64)
                .bind(bincode::serialize(&block)?)
                .execute(pool)
                .await?;
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_table_dump_is_refused() -> Result<()> {
    assert_dump_refused(
        "truncate_partial_dump",
        &["-Fc", "--exclude-table-data=transactions"],
        async |_, _| Ok(()),
    )
    .await
}

/// The transactions section is there, but without a row truncation would delete.
#[tokio::test(flavor = "multi_thread")]
async fn dump_missing_transaction_rows_is_refused() -> Result<()> {
    assert_dump_refused_with(
        "truncate_missing_tx_rows",
        &["-Fc"],
        async |pool| {
            sqlx::raw_sql(
                "CREATE TABLE held AS SELECT * FROM transactions WHERE data = '\\x01';
                 DELETE FROM transactions WHERE data = '\\x01';",
            )
            .execute(pool)
            .await?;
            Ok(())
        },
        async |pool, _| {
            sqlx::raw_sql("INSERT INTO transactions SELECT * FROM held; DROP TABLE held;")
                .execute(pool)
                .await?;
            Ok(())
        },
    )
    .await
}

/// A row the dump holds with other bytes than the live row cannot restore it.
#[tokio::test(flavor = "multi_thread")]
async fn dump_with_other_transaction_data_is_refused() -> Result<()> {
    assert_dump_refused("truncate_other_tx_data", &["-Fc"], async |pool, _| {
        let updated = sqlx::query("UPDATE transactions SET data = '\\x63' WHERE data = '\\x01'")
            .execute(pool)
            .await?
            .rows_affected();
        assert_eq!(updated, 1);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn truncated_archive_is_refused() -> Result<()> {
    assert_dump_refused("truncate_cut_dump", &["-Fc"], async |_, dump| {
        let bytes = fs::read(dump)?;
        fs::write(dump, &bytes[..bytes.len() / 2])?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn plain_sql_dump_is_refused() -> Result<()> {
    assert_dump_refused("truncate_plain_dump", &["-Fp"], async |_, _| Ok(())).await
}
