//! Integration tests for AccountsDB Postgres operations.
//!
//! Uses testcontainers to spin up an isolated Postgres instance for each test.
//! Requires Docker to be running.

use private_channel_core::accounts::AccountsDB;
use private_channel_core::stages::AccountSettlement;
use private_channel_core::test_helpers::{
    create_test_block_info, create_test_sanitized_transaction,
};
use solana_rpc_client_types::response::RpcPerfSample;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    hash::Hash,
    pubkey::Pubkey,
    signature::Keypair,
};
use solana_svm::transaction_execution_result::{ExecutedTransaction, TransactionExecutionDetails};
use solana_svm::transaction_processing_result::ProcessedTransaction;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Spin up a Postgres container, connect, and return (AccountsDB, container).
/// The container handle must be kept alive for the duration of the test.
async fn start_postgres() -> (AccountsDB, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("core_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:password@{}:{}/core_test", host, port);

    let db = AccountsDB::new(&db_url, false)
        .await
        .unwrap_or_else(|e| panic!("Failed to create AccountsDB: {}", e));

    (db, container)
}

fn make_account(lamports: u64, owner: &Pubkey) -> AccountSharedData {
    AccountSharedData::new(lamports, 0, owner)
}

fn make_executed_tx(accounts: Vec<(Pubkey, AccountSharedData)>) -> ProcessedTransaction {
    use solana_svm::account_loader::LoadedTransaction;
    ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
        loaded_transaction: LoadedTransaction {
            accounts,
            ..Default::default()
        },
        execution_details: TransactionExecutionDetails {
            status: Ok(()),
            log_messages: None,
            inner_instructions: None,
            return_data: None,
            executed_units: 0,
            accounts_data_len_delta: 0,
        },
        programs_modified_by_tx: HashMap::new(),
    }))
}

// ── Account CRUD ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_set_and_get_account() {
    let (mut db, _pg) = start_postgres().await;

    let pubkey = Pubkey::new_unique();
    let owner = Pubkey::new_unique();
    let account = make_account(1_000_000, &owner);

    db.set_account(pubkey, account.clone()).await;

    let retrieved = db.get_account_shared_data(&pubkey).await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.lamports(), 1_000_000);
    assert_eq!(retrieved.owner(), &owner);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_nonexistent_account() {
    let (db, _pg) = start_postgres().await;

    let result = db
        .get_account_shared_data(&Pubkey::new_unique())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_multiple_accounts() {
    let (mut db, _pg) = start_postgres().await;

    let owner = Pubkey::new_unique();
    let pk1 = Pubkey::new_unique();
    let pk2 = Pubkey::new_unique();
    let pk3 = Pubkey::new_unique(); // not stored

    db.set_account(pk1, make_account(100, &owner)).await;
    db.set_account(pk2, make_account(200, &owner)).await;

    let results = db.get_accounts(&[pk1, pk2, pk3]).await.unwrap();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_some());
    assert_eq!(results[0].as_ref().unwrap().lamports(), 100);
    assert!(results[1].is_some());
    assert_eq!(results[1].as_ref().unwrap().lamports(), 200);
    assert!(results[2].is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_account_matches_owners() {
    let (mut db, _pg) = start_postgres().await;

    let owner_a = Pubkey::new_unique();
    let owner_b = Pubkey::new_unique();
    let owner_c = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();

    db.set_account(pubkey, make_account(100, &owner_b)).await;

    // Wrap in AccountsDB::Postgres to call account_matches_owners via the trait
    // The trait impl is on PostgresAccountsDB which delegates to get_account + owner check
    use solana_svm_callback::TransactionProcessingCallback;
    if let AccountsDB::Postgres(ref pg) = db {
        let result = pg.account_matches_owners(&pubkey, &[owner_a, owner_b, owner_c]);
        assert_eq!(result, Some(1)); // owner_b is at index 1
    } else {
        panic!("Expected Postgres variant");
    }
}

// ── Block Operations ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_store_and_get_block() {
    let (mut db, _pg) = start_postgres().await;

    let blockhash = Hash::new_unique();
    let block = create_test_block_info(42, blockhash);

    db.store_block(block).await.unwrap();

    let retrieved = db.get_block(42).await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.slot, 42);
    assert_eq!(retrieved.blockhash, blockhash);
    assert_eq!(retrieved.block_height, Some(42));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_blocks_range() {
    let (mut db, _pg) = start_postgres().await;

    for slot in [5, 10, 15, 20, 25] {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    let slots = db.get_blocks(10, Some(20)).await.unwrap();
    assert_eq!(slots, vec![10, 15, 20]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_block_time() {
    let (mut db, _pg) = start_postgres().await;

    let block = create_test_block_info(7, Hash::new_unique());
    let expected_time = block.block_time;
    db.store_block(block).await.unwrap();

    let time = db.get_block_time(7).await.unwrap();
    assert_eq!(time, expected_time);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_first_available_block() {
    let (mut db, _pg) = start_postgres().await;

    for slot in [5, 10, 15] {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    let first = db.get_first_available_block().await.unwrap();
    assert_eq!(first, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_blocks_in_range() {
    let (mut db, _pg) = start_postgres().await;

    for slot in [1, 2, 3, 4, 5] {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    let blocks = db.get_blocks_in_range(2, 4).await.unwrap();
    assert_eq!(blocks.len(), 3);
    assert_eq!(blocks[0].slot, 2);
    assert_eq!(blocks[1].slot, 3);
    assert_eq!(blocks[2].slot, 4);
}

// ── Transaction Operations ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_accounts_and_transactions() {
    let (mut db, _pg) = start_postgres().await;

    let owner = Pubkey::new_unique();
    let pk = Pubkey::new_unique();
    let account = make_account(500, &owner);

    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let sanitized_tx = create_test_sanitized_transaction(&from, &to, 100);
    let sig = *sanitized_tx.signature();
    let processed = make_executed_tx(vec![(pk, account.clone())]);

    let block = create_test_block_info(1, Hash::new_unique());

    let settlements = vec![(
        pk,
        AccountSettlement {
            account: account.clone(),
            deleted: false,
        },
    )];

    db.write_batch(
        &settlements,
        vec![(sig, &sanitized_tx, 1, 1_700_000_001, &processed)],
        Some(block.clone()),
    )
    .await
    .unwrap();

    // Verify account stored
    let acct = db.get_account_shared_data(&pk).await.unwrap();
    assert!(acct.is_some());
    assert_eq!(acct.unwrap().lamports(), 500);

    // Verify block stored
    let blk = db.get_block(1).await.unwrap();
    assert!(blk.is_some());

    // Verify transaction stored
    let tx = db.get_transaction(&sig).await.unwrap();
    assert!(tx.is_some());
    assert_eq!(tx.unwrap().slot, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_deleted_account() {
    let (mut db, _pg) = start_postgres().await;

    let owner = Pubkey::new_unique();
    let pk = Pubkey::new_unique();

    // First store the account
    db.set_account(pk, make_account(1000, &owner)).await;
    assert!(db.get_account_shared_data(&pk).await.unwrap().is_some());

    // Delete it via write_batch
    let settlements = vec![(
        pk,
        AccountSettlement {
            account: AccountSharedData::default(),
            deleted: true,
        },
    )];

    db.write_batch(&settlements, vec![], None).await.unwrap();

    // Should be gone
    assert!(db.get_account_shared_data(&pk).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_increments_tx_count() {
    let (mut db, _pg) = start_postgres().await;

    let count_before = db.get_transaction_count().await.unwrap();
    assert_eq!(count_before, 0);

    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let tx1 = create_test_sanitized_transaction(&from, &to, 10);
    let tx2 = create_test_sanitized_transaction(&from, &to, 20);
    let sig1 = *tx1.signature();
    let sig2 = *tx2.signature();
    let processed1 = make_executed_tx(vec![]);
    let processed2 = make_executed_tx(vec![]);

    db.write_batch(
        &[],
        vec![
            (sig1, &tx1, 1, 100, &processed1),
            (sig2, &tx2, 1, 100, &processed2),
        ],
        Some(create_test_block_info(1, Hash::new_unique())),
    )
    .await
    .unwrap();

    let count_after = db.get_transaction_count().await.unwrap();
    assert_eq!(count_after, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_transaction_roundtrip() {
    let (mut db, _pg) = start_postgres().await;

    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let sanitized_tx = create_test_sanitized_transaction(&from, &to, 42);
    let sig = *sanitized_tx.signature();
    let processed = make_executed_tx(vec![]);

    db.write_batch(
        &[],
        vec![(sig, &sanitized_tx, 5, 1_700_000_005, &processed)],
        Some(create_test_block_info(5, Hash::new_unique())),
    )
    .await
    .unwrap();

    let stored = db.get_transaction(&sig).await.unwrap().unwrap();
    assert_eq!(stored.slot, 5);
    assert_eq!(stored.block_time, 1_700_000_005);
}

// ── Slot Commit Guard ─────────────────────────────────────────────────────────

/// A batch whose block does not extend the stored ledger must be rejected whole.
/// This is the duplicate or stale writer trying to overwrite the tip or rewind
/// behind it, and none of its accounts, transactions or metadata may survive.
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_rejects_a_block_at_or_below_the_stored_tip() {
    let (mut db, _pg) = start_postgres().await;

    let tip_blockhash = Hash::new_unique();
    db.write_batch(&[], vec![], Some(create_test_block_info(5, tip_blockhash)))
        .await
        .unwrap();
    let count_before = db.get_transaction_count().await.unwrap();

    let owner = Pubkey::new_unique();
    let pk = Pubkey::new_unique();
    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let tx = create_test_sanitized_transaction(&from, &to, 42);
    let sig = *tx.signature();
    let processed = make_executed_tx(vec![]);
    let settlements = vec![(
        pk,
        AccountSettlement {
            account: make_account(1_000, &owner),
            deleted: false,
        },
    )];

    // Replaying the tip slot and rewinding behind it must both fail.
    for slot in [5u64, 4] {
        let result = db
            .write_batch(
                &settlements,
                vec![(sig, &tx, slot, 1_700_000_000, &processed)],
                Some(create_test_block_info(slot, Hash::new_unique())),
            )
            .await;

        let err = result.expect_err("a block at or below the tip must be rejected");
        assert!(
            err.contains(&slot.to_string()),
            "the error must name the rejected slot, got: {err}"
        );

        assert_eq!(db.get_latest_slot().await.unwrap(), Some(5));
        assert_eq!(db.get_latest_blockhash().await.unwrap(), tip_blockhash);
        assert_eq!(
            db.get_block(5).await.unwrap().unwrap().blockhash,
            tip_blockhash,
            "the committed tip block must not be rewritten"
        );
        assert!(db.get_block(4).await.unwrap().is_none());
        assert!(db.get_account_shared_data(&pk).await.unwrap().is_none());
        assert!(db.get_transaction(&sig).await.unwrap().is_none());
        assert_eq!(db.get_transaction_count().await.unwrap(), count_before);
    }
}

/// A commit can succeed on the server and still fail to acknowledge, and the
/// settler's retry then rewrites byte-identical rows for a slot already stored.
/// That replay must be accepted, and counted once, or a lost ack stops the node.
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_accepts_an_identical_replay_of_the_stored_tip() {
    let (mut db, _pg) = start_postgres().await;

    let blockhash = Hash::new_unique();
    let owner = Pubkey::new_unique();
    let pk = Pubkey::new_unique();
    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let tx = create_test_sanitized_transaction(&from, &to, 42);
    let sig = *tx.signature();
    let processed = make_executed_tx(vec![]);
    let settlements = vec![(
        pk,
        AccountSettlement {
            account: make_account(1_000, &owner),
            deleted: false,
        },
    )];

    for attempt in 1..=2 {
        db.write_batch(
            &settlements,
            vec![(sig, &tx, 5, 1_700_000_000, &processed)],
            Some(create_test_block_info(5, blockhash)),
        )
        .await
        .unwrap_or_else(|e| panic!("attempt {attempt} must be accepted, got: {e}"));
    }

    assert_eq!(
        db.get_transaction_count().await.unwrap(),
        1,
        "a replayed commit must be counted once"
    );
    assert_eq!(db.get_latest_slot().await.unwrap(), Some(5));
    assert_eq!(db.get_block(5).await.unwrap().unwrap().blockhash, blockhash);
}

/// The guard rejects rewinds and overwrites, not gaps. The settler only ever
/// extends by one slot, so gaps cost nothing to allow and keep write_batch
/// usable for building arbitrary ledger fixtures.
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_accepts_a_block_above_the_stored_tip() {
    let (mut db, _pg) = start_postgres().await;

    db.write_batch(
        &[],
        vec![],
        Some(create_test_block_info(5, Hash::new_unique())),
    )
    .await
    .unwrap();

    for slot in [6u64, 20] {
        let blockhash = Hash::new_unique();
        db.write_batch(&[], vec![], Some(create_test_block_info(slot, blockhash)))
            .await
            .unwrap_or_else(|e| panic!("slot {slot} must commit above the tip: {e}"));

        assert_eq!(db.get_latest_slot().await.unwrap(), Some(slot));
        assert_eq!(db.get_latest_blockhash().await.unwrap(), blockhash);
    }
}

// ── Blockhash + Slot Metadata ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_latest_blockhash_after_write_batch() {
    let (mut db, _pg) = start_postgres().await;

    let blockhash = Hash::new_unique();
    let block = create_test_block_info(10, blockhash);

    db.write_batch(&[], vec![], Some(block)).await.unwrap();

    let latest = db.get_latest_blockhash().await.unwrap();
    assert_eq!(latest, blockhash);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_latest_slot() {
    let (mut db, _pg) = start_postgres().await;

    // store_block stores block data; get_latest_slot queries MAX(slot) FROM blocks
    db.store_block(create_test_block_info(100, Hash::new_unique()))
        .await
        .unwrap();

    let slot = db.get_latest_slot().await.unwrap();
    assert_eq!(slot, Some(100));
}

// ── Performance Samples ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_store_and_get_perf_samples() {
    let (mut db, _pg) = start_postgres().await;

    for i in 0..3 {
        db.store_performance_sample(RpcPerfSample {
            slot: 100 + i,
            num_transactions: 50 + i,
            num_slots: 10,
            sample_period_secs: 60,
            num_non_vote_transactions: Some(50 + i),
        })
        .await
        .unwrap();
    }

    let samples = db.get_recent_performance_samples(2).await.unwrap();
    assert_eq!(samples.len(), 2);
    // Should be ordered by slot DESC
    assert_eq!(samples[0].slot, 102);
    assert_eq!(samples[1].slot, 101);
}

// ── Edge Cases ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_read_only_noop() {
    let container = Postgres::default()
        .with_db_name("core_ro_test")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("Failed to start Postgres container");

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!(
        "postgres://postgres:password@{}:{}/core_ro_test",
        host, port
    );

    // Create tables first with a read-write connection
    let _rw_db = AccountsDB::new(&db_url, false).await.unwrap();

    // Now create a read-only connection
    let mut ro_db = AccountsDB::new(&db_url, true).await.unwrap();

    let pk = Pubkey::new_unique();
    let settlements = vec![(
        pk,
        AccountSettlement {
            account: make_account(100, &Pubkey::new_unique()),
            deleted: false,
        },
    )];

    // Should succeed but not actually write
    ro_db.write_batch(&settlements, vec![], None).await.unwrap();

    // Account should not exist (write was silently skipped)
    assert!(ro_db.get_account_shared_data(&pk).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_unsupported_url_scheme() {
    let result = AccountsDB::new("ftp://localhost/test", false).await;
    assert!(result.is_err());
    let err_msg = result.err().unwrap().to_string();
    assert!(err_msg.contains("Unsupported"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_epoch_info_empty_db() {
    let (db, _pg) = start_postgres().await;

    // epoch_info on empty DB should return an error since no blocks exist
    let result = db.get_epoch_info().await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_latest_blockhash_after_store_block() {
    let (mut db, _pg) = start_postgres().await;

    let blockhash = Hash::new_unique();
    db.store_block(create_test_block_info(1, blockhash))
        .await
        .unwrap();

    let latest = db.get_latest_blockhash().await.unwrap();
    assert_eq!(latest, blockhash);

    // Store another block and verify it updates
    let blockhash2 = Hash::new_unique();
    db.store_block(create_test_block_info(2, blockhash2))
        .await
        .unwrap();
    let latest2 = db.get_latest_blockhash().await.unwrap();
    assert_eq!(latest2, blockhash2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_block_nonexistent() {
    let (db, _pg) = start_postgres().await;
    assert!(db.get_block(999).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_transaction_nonexistent() {
    let (db, _pg) = start_postgres().await;
    let sig = solana_sdk::signature::Signature::new_unique();
    assert!(db.get_transaction(&sig).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_account_overwrite() {
    let (mut db, _pg) = start_postgres().await;

    let pk = Pubkey::new_unique();
    let owner = Pubkey::new_unique();

    db.set_account(pk, make_account(100, &owner)).await;
    assert_eq!(
        db.get_account_shared_data(&pk)
            .await
            .unwrap()
            .unwrap()
            .lamports(),
        100
    );

    // Overwrite
    db.set_account(pk, make_account(200, &owner)).await;
    assert_eq!(
        db.get_account_shared_data(&pk)
            .await
            .unwrap()
            .unwrap()
            .lamports(),
        200
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_latest_slot_empty_db() {
    let (db, _pg) = start_postgres().await;
    let slot = db.get_latest_slot().await.unwrap();
    assert_eq!(slot, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_latest_blockhash_empty_db() {
    let (db, _pg) = start_postgres().await;
    let result = db.get_latest_blockhash().await;
    assert!(result.is_err());
}

// ── Truncation ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_rejects_zero_keep_slots() {
    let (db, _pg) = start_postgres().await;

    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 0,
        max_backup_age: std::time::Duration::from_secs(300),
        pg_dump_path: None,
        batch_size: 100,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let result = private_channel_core::accounts::truncate::truncate_slots(pg, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("keep_slots"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_rejects_zero_batch_size() {
    let (db, _pg) = start_postgres().await;

    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 10,
        max_backup_age: std::time::Duration::from_secs(300),
        pg_dump_path: None,
        batch_size: 0,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let result = private_channel_core::accounts::truncate::truncate_slots(pg, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("batch_size"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_empty_db_returns_none() {
    let (db, _pg) = start_postgres().await;

    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 10,
        max_backup_age: std::time::Duration::from_secs(300),
        pg_dump_path: None,
        batch_size: 100,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let report = private_channel_core::accounts::truncate::truncate_slots(pg, &opts)
            .await
            .unwrap();
        assert_eq!(report.latest_slot, None);
        assert_eq!(report.truncate_before_slot, None);
        assert_eq!(report.blocks_deleted, 0);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_nothing_to_delete() {
    let (mut db, _pg) = start_postgres().await;

    // Store 5 blocks at slots 1-5
    for slot in 1..=5 {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    // Keep all 5 slots — nothing should be truncated
    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 10,
        max_backup_age: std::time::Duration::from_secs(300),
        pg_dump_path: None,
        batch_size: 100,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let report = private_channel_core::accounts::truncate::truncate_slots(pg, &opts)
            .await
            .unwrap();
        assert_eq!(report.latest_slot, Some(5));
        assert_eq!(report.blocks_deleted, 0);
        // Backup check should be "skipped"
        assert!(!report.backup_check.has_valid_backup());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_dry_run_with_pg_dump() {
    let (mut db, _pg) = start_postgres().await;

    // Store 10 blocks at slots 1-10
    for slot in 1..=10 {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    // Create a recent pg_dump file
    let tmp_dump = tempfile::NamedTempFile::new().unwrap();

    // Keep 5 slots → truncate before slot 6 → blocks 1-5 should be counted
    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 5,
        max_backup_age: std::time::Duration::from_secs(3600),
        pg_dump_path: Some(tmp_dump.path().to_path_buf()),
        batch_size: 100,
        dry_run: true,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let report = private_channel_core::accounts::truncate::truncate_slots(pg, &opts)
            .await
            .unwrap();
        assert_eq!(report.latest_slot, Some(10));
        assert_eq!(report.truncate_before_slot, Some(6));
        assert_eq!(report.blocks_deleted, 5);
        assert!(report.backup_check.pg_dump_ok);

        // Dry run: blocks should still exist
        assert!(db.get_block(1).await.unwrap().is_some());
        assert!(db.get_block(5).await.unwrap().is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_actually_deletes_blocks() {
    let (mut db, _pg) = start_postgres().await;

    // Store 10 blocks at slots 1-10
    for slot in 1..=10 {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    // Create a recent pg_dump file for backup verification
    let tmp_dump = tempfile::NamedTempFile::new().unwrap();

    // Keep 5 → truncate before slot 6 → delete blocks 1-5
    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 5,
        max_backup_age: std::time::Duration::from_secs(3600),
        pg_dump_path: Some(tmp_dump.path().to_path_buf()),
        batch_size: 100,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let report = private_channel_core::accounts::truncate::truncate_slots(pg, &opts)
            .await
            .unwrap();
        assert_eq!(report.blocks_deleted, 5);
        assert_eq!(report.transactions_deleted, 0);

        // Old blocks should be gone
        assert!(db.get_block(1).await.unwrap().is_none());
        assert!(db.get_block(5).await.unwrap().is_none());
        // Recent blocks should still exist
        assert!(db.get_block(6).await.unwrap().is_some());
        assert!(db.get_block(10).await.unwrap().is_some());

        // first_available_block should be updated
        assert!(report.first_available_block.is_some());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_deletes_associated_transactions() {
    let (mut db, _pg) = start_postgres().await;

    let from = Keypair::new();
    let to = Pubkey::new_unique();

    // Store blocks with transactions at slots 1-4
    for slot in 1..=4 {
        let tx = create_test_sanitized_transaction(&from, &to, slot * 10);
        let sig = *tx.signature();
        let processed = make_executed_tx(vec![]);

        let mut block = create_test_block_info(slot, Hash::new_unique());
        block.transaction_signatures = vec![sig];

        db.write_batch(
            &[],
            vec![(sig, &tx, slot, 1_700_000_000 + slot as i64, &processed)],
            Some(block),
        )
        .await
        .unwrap();
    }

    // Create pg_dump for backup verification
    let tmp_dump = tempfile::NamedTempFile::new().unwrap();

    // Keep 2 → truncate before slot 3 → delete blocks 1,2 and their transactions
    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 2,
        max_backup_age: std::time::Duration::from_secs(3600),
        pg_dump_path: Some(tmp_dump.path().to_path_buf()),
        batch_size: 2,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let report = private_channel_core::accounts::truncate::truncate_slots(pg, &opts)
            .await
            .unwrap();
        assert_eq!(report.blocks_deleted, 2);
        assert_eq!(report.transactions_deleted, 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_truncate_fails_without_backup() {
    let (mut db, _pg) = start_postgres().await;

    // Store blocks to trigger truncation
    for slot in 1..=10 {
        db.store_block(create_test_block_info(slot, Hash::new_unique()))
            .await
            .unwrap();
    }

    // No pg_dump_path, and testcontainers Postgres has no WAL archiving
    let opts = private_channel_core::accounts::truncate::TruncateOptions {
        keep_slots: 5,
        max_backup_age: std::time::Duration::from_secs(300),
        pg_dump_path: None,
        batch_size: 100,
        dry_run: false,
    };

    if let AccountsDB::Postgres(ref pg) = db {
        let result = private_channel_core::accounts::truncate::truncate_slots(pg, &opts).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Backup verification failed"));
    }
}
