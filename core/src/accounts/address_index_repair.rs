use {
    crate::{
        accounts::{
            address_index_watermark::{
                get_address_signatures_flushed_slot, upsert_address_signatures_flushed_slot_in_tx,
                ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY,
            },
            get_latest_slot::get_latest_slot,
            traits::{AccountsDB, BlockInfo},
            types::StoredTransaction,
            write_batch::{upsert_address_signature_rows, AddressSignatureRow},
        },
        stage_metrics::SharedMetrics,
    },
    anyhow::{anyhow, Context, Result},
    solana_sdk::signature::Signature,
    sqlx::{PgPool, Row},
    std::sync::Arc,
    tracing::{info, warn},
};

/// Re-derive missing rows. Idempotent.
pub async fn repair_address_signatures(db: &AccountsDB, _metrics: SharedMetrics) -> Result<()> {
    let postgres_db = match db {
        AccountsDB::Postgres(p) => p,
        AccountsDB::Redis(_) => {
            info!("address_signatures repair skipped (Redis backend)");
            return Ok(());
        }
    };

    // The repair seeds/rewrites the address_signatures index and watermark, all
    // of which are writes. A read-only node connects to the read replica, where
    // any INSERT fails with "cannot execute INSERT in a read-only transaction".
    // The writer node owns index consistency, so skip the repair here — mirrors
    // the read-only guards in store_block / set_account / write_batch.
    if postgres_db.read_only {
        info!("address_signatures repair skipped (read-only mode)");
        return Ok(());
    }

    let pool = Arc::clone(&postgres_db.pool);

    let watermark_opt = get_address_signatures_flushed_slot(&pool)
        .await
        .context("Failed to read address_signatures watermark")?;

    let Some(max_block_u64) = get_latest_slot(db)
        .await
        .context("Failed to query max block slot")?
    else {
        info!("address_signatures repair: no blocks present, nothing to do");
        return Ok(());
    };
    let max_block = i64::try_from(max_block_u64).context("max block slot exceeds i64::MAX")?;

    // No watermark row yet: first run against an existing DB. Re-deriving the
    // entire block history would block startup for the full slot range, so
    // instead trust the existing index up to the current tip and seed the
    // watermark. Every later crash is then bounded to (watermark, max_block] by
    // the watermark the live writer advances.
    let Some(watermark) = watermark_opt else {
        info!(
            max_block,
            "address_signatures repair: no watermark, seeding to current tip"
        );
        let mut pg_tx = pool
            .begin()
            .await
            .context("Failed to begin watermark seed transaction")?;
        upsert_address_signatures_flushed_slot_in_tx(&mut pg_tx, max_block)
            .await
            .context("Failed to seed address_signatures watermark")?;
        pg_tx
            .commit()
            .await
            .context("Failed to commit watermark seed")?;
        return Ok(());
    };

    if max_block <= watermark {
        info!(
            watermark,
            max_block, "address_signatures repair: already consistent"
        );
        return Ok(());
    }

    info!(
        watermark,
        max_block, "address_signatures repair: scanning slot range"
    );

    let mut repaired_rows: u64 = 0;
    let mut repaired_slots: u64 = 0;
    let mut cursor: i64 = watermark;
    const SLOT_PAGE: i64 = 256;

    while cursor < max_block {
        let upper = std::cmp::min(cursor.saturating_add(SLOT_PAGE), max_block);
        let rows = sqlx::query(
            "SELECT slot, data FROM blocks
             WHERE slot > $1 AND slot <= $2
             ORDER BY slot ASC",
        )
        .bind(cursor)
        .bind(upper)
        .fetch_all(pool.as_ref())
        .await
        .context("Failed to fetch blocks for repair")?;

        if rows.is_empty() {
            cursor = upper;
            continue;
        }

        for row in rows {
            let slot: i64 = row.get("slot");
            let data: Vec<u8> = row.get("data");
            let block: BlockInfo = bincode::deserialize(&data)
                .with_context(|| format!("Failed to deserialize block at slot {}", slot))?;

            let derived = derive_rows_for_block(pool.as_ref(), slot, &block).await?;
            let n = derived.len();

            let mut pg_tx = pool
                .begin()
                .await
                .context("Failed to begin repair transaction")?;

            upsert_address_signature_rows(&mut pg_tx, &derived)
                .await
                .with_context(|| format!("Repair insert failed for slot {}", slot))?;

            upsert_address_signatures_flushed_slot_in_tx(&mut pg_tx, slot)
                .await
                .with_context(|| format!("Repair watermark update failed for slot {}", slot))?;

            pg_tx
                .commit()
                .await
                .with_context(|| format!("Repair commit failed for slot {}", slot))?;

            repaired_rows = repaired_rows.saturating_add(n as u64);
            repaired_slots = repaired_slots.saturating_add(1);
            cursor = slot;
        }

        if cursor < upper {
            cursor = upper;
        }
    }

    info!(
        watermark,
        max_block,
        repaired_slots,
        repaired_rows,
        ?ADDRESS_SIGNATURES_FLUSHED_SLOT_KEY,
        "address_signatures repair complete"
    );
    Ok(())
}

async fn derive_rows_for_block(
    pool: &PgPool,
    slot: i64,
    block: &BlockInfo,
) -> Result<Vec<AddressSignatureRow>> {
    if block.transaction_signatures.is_empty() {
        return Ok(Vec::new());
    }

    let sig_bytes: Vec<Vec<u8>> = block
        .transaction_signatures
        .iter()
        .map(|s| s.as_ref().to_vec())
        .collect();
    let sig_refs: Vec<&[u8]> = sig_bytes.iter().map(|v| v.as_slice()).collect();

    let rows = sqlx::query(
        "SELECT signature, data FROM transactions
         WHERE signature = ANY($1::bytea[])",
    )
    .bind(&sig_refs)
    .fetch_all(pool)
    .await
    .with_context(|| format!("Failed to fetch transactions for slot {}", slot))?;

    let mut out: Vec<AddressSignatureRow> = Vec::with_capacity(rows.len() * 7);
    for row in rows {
        let sig_bytes: Vec<u8> = row.get("signature");
        let data: Vec<u8> = row.get("data");
        let stored: StoredTransaction = match bincode::deserialize(&data) {
            Ok(t) => t,
            Err(e) => {
                let sig = Signature::try_from(sig_bytes.as_slice())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<unparseable>".to_string());
                return Err(anyhow!(
                    "Repair: failed to deserialize transaction {} at slot {}: {}",
                    sig,
                    slot,
                    e
                ));
            }
        };

        let tx_with_meta = stored.transaction_with_status_meta();
        let solana_transaction_status::TransactionWithStatusMeta::Complete(versioned) =
            tx_with_meta
        else {
            return Err(anyhow!(
                "Repair: transaction_with_status_meta returned non-Complete at slot {}",
                slot
            ));
        };
        for pk in versioned.account_keys().iter() {
            out.push(AddressSignatureRow {
                address: pk.to_bytes().to_vec(),
                slot,
                signature: sig_bytes.clone(),
            });
        }
    }

    if out.is_empty() && !block.transaction_signatures.is_empty() {
        warn!(
            slot,
            block_sigs = block.transaction_signatures.len(),
            "address_signatures repair: block lists txs but transactions table has none"
        );
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::write_batch::AddressSignatureRow,
        stage_metrics::NoopMetrics,
        test_helpers::{
            create_test_block_info, create_test_sanitized_transaction, start_test_postgres,
        },
    };
    use solana_sdk::{hash::Hash, signature::Keypair};
    use solana_svm::{
        account_loader::LoadedTransaction,
        transaction_execution_result::{ExecutedTransaction, TransactionExecutionDetails},
        transaction_processing_result::ProcessedTransaction,
    };
    use std::collections::HashMap;

    async fn count_addr_sig_rows(db: &AccountsDB) -> i64 {
        let pg = match db {
            AccountsDB::Postgres(p) => p,
            _ => panic!("expected Postgres"),
        };
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM address_signatures")
            .fetch_one(pg.pool.as_ref())
            .await
            .unwrap()
    }

    async fn fetch_addr_sig_rows(db: &AccountsDB) -> Vec<(Vec<u8>, i64, Vec<u8>)> {
        let pg = match db {
            AccountsDB::Postgres(p) => p,
            _ => panic!("expected Postgres"),
        };
        let rows = sqlx::query("SELECT address, slot, signature FROM address_signatures")
            .fetch_all(pg.pool.as_ref())
            .await
            .unwrap();
        let mut out: Vec<(Vec<u8>, i64, Vec<u8>)> = rows
            .into_iter()
            .map(|r| (r.get("address"), r.get("slot"), r.get("signature")))
            .collect();
        out.sort();
        out
    }

    fn expected_rows_for(
        tx: &solana_sdk::transaction::SanitizedTransaction,
        slot: i64,
    ) -> Vec<(Vec<u8>, i64, Vec<u8>)> {
        let sig = tx.signature().as_ref().to_vec();
        let mut out: Vec<(Vec<u8>, i64, Vec<u8>)> = tx
            .message()
            .account_keys()
            .iter()
            .map(|pk| (pk.to_bytes().to_vec(), slot, sig.clone()))
            .collect();
        out.sort();
        out
    }

    async fn clear_address_signatures(db: &AccountsDB) {
        let pg = match db {
            AccountsDB::Postgres(p) => p,
            _ => panic!("expected Postgres"),
        };
        sqlx::query("DELETE FROM address_signatures")
            .execute(pg.pool.as_ref())
            .await
            .unwrap();
    }

    /// Seed the watermark below the gap, simulating a writer that crashed after
    /// progressing to `slot`. Repair only scans `(watermark, max_block]`.
    async fn set_watermark(db: &AccountsDB, slot: i64) {
        let pg = match db {
            AccountsDB::Postgres(p) => p,
            _ => panic!("expected Postgres"),
        };
        let mut tx = pg.pool.begin().await.unwrap();
        upsert_address_signatures_flushed_slot_in_tx(&mut tx, slot)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    fn make_processed() -> ProcessedTransaction {
        ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
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

    /// Repair derives rows + advances watermark.
    #[tokio::test(flavor = "multi_thread")]
    async fn repair_derives_missing_rows_and_advances_watermark() {
        let (mut db, _pg) = start_test_postgres().await;

        let from_a = Keypair::new();
        let to_a = solana_sdk::pubkey::Pubkey::new_unique();
        let tx_a = create_test_sanitized_transaction(&from_a, &to_a, 1);
        let sig_a = *tx_a.signature();
        let from_b = Keypair::new();
        let to_b = solana_sdk::pubkey::Pubkey::new_unique();
        let tx_b = create_test_sanitized_transaction(&from_b, &to_b, 1);
        let sig_b = *tx_b.signature();

        let _: Vec<AddressSignatureRow> = db
            .write_batch(
                &[],
                vec![(sig_a, &tx_a, 100, 1_700_000_000, &make_processed())],
                Some(create_test_block_info(100, Hash::new_unique())),
            )
            .await
            .unwrap();
        let _: Vec<AddressSignatureRow> = db
            .write_batch(
                &[],
                vec![(sig_b, &tx_b, 101, 1_700_000_000, &make_processed())],
                Some(create_test_block_info(101, Hash::new_unique())),
            )
            .await
            .unwrap();

        // Block must list the tx signatures.
        let pg = match &db {
            AccountsDB::Postgres(p) => p.pool.clone(),
            _ => panic!("expected Postgres"),
        };
        for (slot, sig) in [(100i64, sig_a), (101, sig_b)] {
            let mut block = create_test_block_info(slot as u64, Hash::new_unique());
            block.transaction_signatures = vec![sig];
            let data = bincode::serialize(&block).unwrap();
            sqlx::query("UPDATE blocks SET data = $1 WHERE slot = $2")
                .bind(&data)
                .bind(slot)
                .execute(pg.as_ref())
                .await
                .unwrap();
        }

        // Simulate the crash gap: writer had flushed through slot 99, then
        // crashed leaving slots 100-101 unindexed.
        clear_address_signatures(&db).await;
        assert_eq!(count_addr_sig_rows(&db).await, 0);
        set_watermark(&db, 99).await;

        repair_address_signatures(&db, Arc::new(NoopMetrics) as SharedMetrics)
            .await
            .unwrap();

        let mut expected = expected_rows_for(&tx_a, 100);
        expected.extend(expected_rows_for(&tx_b, 101));
        expected.sort();
        assert!(!expected.is_empty(), "test setup produced no expected rows");

        let actual = fetch_addr_sig_rows(&db).await;
        assert_eq!(
            actual, expected,
            "repaired rows must match the (address, slot, signature) triples \
             derived from tx_a@100 and tx_b@101 exactly"
        );

        let wm = get_address_signatures_flushed_slot(&pg).await.unwrap();
        assert_eq!(
            wm,
            Some(101),
            "watermark must advance to the max repaired slot"
        );
    }

    /// Second run inserts no new rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn repair_is_idempotent_on_rerun() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = solana_sdk::pubkey::Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 1);
        let sig = *tx.signature();

        db.write_batch(
            &[],
            vec![(sig, &tx, 50, 1_700_000_000, &make_processed())],
            Some(create_test_block_info(50, Hash::new_unique())),
        )
        .await
        .unwrap();

        let pg = match &db {
            AccountsDB::Postgres(p) => p.pool.clone(),
            _ => panic!("expected Postgres"),
        };
        let mut block = create_test_block_info(50, Hash::new_unique());
        block.transaction_signatures = vec![sig];
        let data = bincode::serialize(&block).unwrap();
        sqlx::query("UPDATE blocks SET data = $1 WHERE slot = $2")
            .bind(&data)
            .bind(50i64)
            .execute(pg.as_ref())
            .await
            .unwrap();

        clear_address_signatures(&db).await;
        set_watermark(&db, 49).await;

        let expected = expected_rows_for(&tx, 50);
        assert!(!expected.is_empty(), "test setup produced no expected rows");

        repair_address_signatures(&db, Arc::new(NoopMetrics) as SharedMetrics)
            .await
            .unwrap();
        let rows_first = fetch_addr_sig_rows(&db).await;
        let wm_first = get_address_signatures_flushed_slot(&pg).await.unwrap();

        assert_eq!(
            rows_first, expected,
            "first repair must produce the exact expected row set"
        );
        assert_eq!(
            wm_first,
            Some(50),
            "watermark must advance to slot 50 after first repair"
        );

        repair_address_signatures(&db, Arc::new(NoopMetrics) as SharedMetrics)
            .await
            .unwrap();
        let rows_second = fetch_addr_sig_rows(&db).await;
        let wm_second = get_address_signatures_flushed_slot(&pg).await.unwrap();

        assert_eq!(
            rows_second, rows_first,
            "rerun must not add, remove, or alter any row"
        );
        assert_eq!(wm_second, wm_first, "rerun must not change the watermark");
    }

    /// First run against an existing DB (no watermark) seeds to the tip and does
    /// NOT re-derive history — avoids an unbounded startup scan over all blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn repair_seeds_watermark_to_tip_when_unset() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = solana_sdk::pubkey::Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 1);
        let sig = *tx.signature();

        db.write_batch(
            &[],
            vec![(sig, &tx, 70, 1_700_000_000, &make_processed())],
            Some(create_test_block_info(70, Hash::new_unique())),
        )
        .await
        .unwrap();

        let pg = match &db {
            AccountsDB::Postgres(p) => p.pool.clone(),
            _ => panic!("expected Postgres"),
        };
        let mut block = create_test_block_info(70, Hash::new_unique());
        block.transaction_signatures = vec![sig];
        let data = bincode::serialize(&block).unwrap();
        sqlx::query("UPDATE blocks SET data = $1 WHERE slot = $2")
            .bind(&data)
            .bind(70i64)
            .execute(pg.as_ref())
            .await
            .unwrap();

        // No watermark set, address_signatures empty: simulates first run against
        // a pre-existing DB rather than a crash gap.
        clear_address_signatures(&db).await;
        assert_eq!(count_addr_sig_rows(&db).await, 0);
        assert_eq!(
            get_address_signatures_flushed_slot(&pg).await.unwrap(),
            None
        );

        repair_address_signatures(&db, Arc::new(NoopMetrics) as SharedMetrics)
            .await
            .unwrap();

        // Must NOT backfill history; must seed the watermark to the current tip.
        assert_eq!(
            count_addr_sig_rows(&db).await,
            0,
            "unset watermark must not trigger a full-history backfill"
        );
        assert_eq!(
            get_address_signatures_flushed_slot(&pg).await.unwrap(),
            Some(70),
            "watermark must be seeded to the current tip"
        );
    }
}
