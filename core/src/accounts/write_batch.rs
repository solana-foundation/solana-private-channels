use {
    super::{
        postgres::PostgresAccountsDB,
        redis::RedisAccountsDB,
        traits::{AccountsDB, BlockInfo},
        transaction_count::TransactionCount,
        utils::get_stored_transaction,
    },
    crate::stages::AccountSettlement,
    solana_sdk::{
        clock::UnixTimestamp, pubkey::Pubkey, signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_processing_result::ProcessedTransaction,
    std::sync::Arc,
    tracing::warn,
};

/// One (address, slot, signature) triple destined for the `address_signatures`
/// index. Built by the settler's atomic write_batch and shipped to the
/// background `address_index_writer` worker via a bounded channel; the writer
/// is the only thing that inserts into the table.
#[derive(Debug, Clone)]
pub struct AddressSignatureRow {
    pub address: Vec<u8>,
    pub slot: i64,
    pub signature: Vec<u8>,
}

pub async fn write_batch(
    db: &mut AccountsDB,
    account_settlements: &[(Pubkey, AccountSettlement)],
    transactions: Vec<(
        Signature,
        &SanitizedTransaction,
        u64,
        UnixTimestamp,
        &ProcessedTransaction,
    )>,
    block_info: Option<BlockInfo>,
) -> Result<Vec<AddressSignatureRow>, String> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            write_batch_postgres(postgres_db, account_settlements, transactions, block_info).await
        }
        AccountsDB::Redis(redis_db) => {
            write_batch_redis(redis_db, account_settlements, transactions, block_info)
                .await
                .map(|()| Vec::new())
        }
    }
}

/// Writes a complete slot batch (accounts + transactions + block metadata) atomically.
/// Either every write in this batch commits, or none do — no partial slot state
/// is ever visible to readers.
///
/// Uses bulk SQL operations (UNNEST for upserts, ANY for deletes) to collapse
/// hundreds of per-row round-trips into 2-3 queries per batch. This is the
/// critical performance path.
async fn write_batch_postgres(
    db: &mut PostgresAccountsDB,
    account_settlements: &[(Pubkey, AccountSettlement)],
    transactions: Vec<(
        Signature,
        &SanitizedTransaction,
        u64,
        UnixTimestamp,
        &ProcessedTransaction,
    )>,
    block_info: Option<BlockInfo>,
) -> Result<Vec<AddressSignatureRow>, String> {
    if db.read_only {
        warn!("Attempted to write batch in read-only mode");
        return Ok(Vec::new());
    }

    if account_settlements.is_empty() && transactions.is_empty() && block_info.is_none() {
        return Ok(Vec::new());
    }

    let pool = Arc::clone(&db.pool);

    // ──────────────────────────────────────────────────────────────────
    // Pre-serialize EVERYTHING before opening the Postgres transaction.
    //
    // Doing that work while holding an open BEGIN…COMMIT pins one
    // pool connection the whole time, starving the executor's
    // get_account_shared_data callbacks (which acquire from the same pool).
    // Atomicity is preserved: every DB write below still happens inside the
    // same BEGIN/COMMIT — we just shorten the window.
    // ──────────────────────────────────────────────────────────────────

    // Accounts: partition into upserts vs deletes and serialize upserts up front.
    let mut upsert_pubkeys: Vec<Vec<u8>> = Vec::new();
    let mut upsert_data: Vec<Vec<u8>> = Vec::new();
    let mut delete_pubkeys: Vec<Vec<u8>> = Vec::new();
    if !account_settlements.is_empty() {
        upsert_pubkeys.reserve(account_settlements.len());
        upsert_data.reserve(account_settlements.len());
        for (pubkey, settlement) in account_settlements {
            if settlement.deleted {
                delete_pubkeys.push(pubkey.to_bytes().to_vec());
            } else {
                let data = bincode::serialize(&settlement.account)
                    .map_err(|e| format!("Failed to serialize account: {}", e))?;
                upsert_pubkeys.push(pubkey.to_bytes().to_vec());
                upsert_data.push(data);
            }
        }
    }

    // Transactions: build StoredTransaction bytes up front.
    let tx_count = transactions.len() as i64;
    let mut sig_bytes_vec: Vec<Vec<u8>> = Vec::with_capacity(transactions.len());
    let mut tx_data_vec: Vec<Vec<u8>> = Vec::with_capacity(transactions.len());
    // Build address_signatures rows here (one per account key referenced in
    // each tx) but ship them out to the background writer after COMMIT below;
    // they are no longer part of the atomic transaction. ~5–7 rows per tx is
    // typical, so use that as the initial capacity hint.
    let mut addr_sig_rows: Vec<AddressSignatureRow> = Vec::with_capacity(transactions.len() * 7);
    for (signature, transaction, tx_slot, block_time, processed) in transactions {
        let stored_tx = get_stored_transaction(transaction, tx_slot, block_time, processed);
        sig_bytes_vec.push(signature.as_ref().to_vec());
        let data = bincode::serialize(&stored_tx)
            .map_err(|e| format!("Failed to serialize transaction: {}", e))?;
        tx_data_vec.push(data);
        // Index every account key the transaction touches, not just the fee
        // payer — getSignaturesForAddress must return a hit for any address
        // that appeared in the message (writable or read-only).
        let sig_bytes = signature.as_ref().to_vec();
        for pubkey in transaction.message().account_keys().iter() {
            addr_sig_rows.push(AddressSignatureRow {
                address: pubkey.to_bytes().to_vec(),
                slot: tx_slot as i64,
                signature: sig_bytes.clone(),
            });
        }
    }

    // Block info: serialize the row payload up front.
    let block_data: Option<Vec<u8>> = match &block_info {
        Some(b) => {
            Some(bincode::serialize(b).map_err(|e| format!("Failed to serialize block: {}", e))?)
        }
        None => None,
    };

    // Start a Postgres transaction — all writes are atomic.
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("Failed to begin transaction: {}", e))?;

    // ── Accounts: bulk DELETE pre-serialized buffers ──
    if !delete_pubkeys.is_empty() {
        sqlx::query("DELETE FROM accounts WHERE pubkey = ANY($1::bytea[])")
            .bind(&delete_pubkeys)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("Failed to bulk delete accounts: {}", e))?;
    }

    // UNNEST expands parallel arrays into rows for a single-query bulk upsert.
    // Invariant: `upsert_pubkeys` is unique within this call — duplicates would
    // trigger Postgres SQLSTATE 21000. Callers dedupe via a HashMap of settlements.
    if !upsert_pubkeys.is_empty() {
        sqlx::query(
            "INSERT INTO accounts (pubkey, data)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[])
             ON CONFLICT (pubkey) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&upsert_pubkeys)
        .bind(&upsert_data)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to bulk upsert accounts: {}", e))?;
    }

    // Same UNNEST pattern and duplicate-key invariant as the accounts upsert:
    // signatures within a block are unique (dedup stage rejects replays upstream).
    if !sig_bytes_vec.is_empty() {
        sqlx::query(
            "INSERT INTO transactions (signature, data)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[])
             ON CONFLICT (signature) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(&sig_bytes_vec)
        .bind(&tx_data_vec)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to bulk upsert transactions: {}", e))?;
    }

    // Read-modify-write inside BEGIN…COMMIT: safe because all writers serialize
    // via this path and MVCC returns the caller's own last commit.
    if tx_count > 0 {
        let current_count_bytes = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT value FROM metadata WHERE key = 'transaction_count'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| format!("Failed to fetch transaction count: {}", e))?;

        let mut count = current_count_bytes
            .and_then(|bytes| TransactionCount::from_bytes(&bytes))
            .unwrap_or_default();

        count.increment(tx_count as u64);

        sqlx::query(
            "INSERT INTO metadata (key, value) VALUES ('transaction_count', $1)
                 ON CONFLICT (key) DO UPDATE SET value = $1",
        )
        .bind(&count.to_bytes()[..])
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to update transaction count: {}", e))?;
    }

    // ── Block info: at most 2 queries (block row + latest_blockhash) ──
    if let (Some(block_info), Some(block_data)) = (&block_info, &block_data) {
        sqlx::query(
            "INSERT INTO blocks (slot, data) VALUES ($1, $2)
                 ON CONFLICT (slot) DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(block_info.slot as i64)
        .bind(block_data)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to store block: {}", e))?;

        sqlx::query(
            "INSERT INTO metadata (key, value) VALUES ('latest_blockhash', $1)
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(block_info.blockhash.as_ref())
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to update latest blockhash: {}", e))?;
    }

    // Commit — if this fails, the entire batch is rolled back.
    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit transaction: {}", e))?;

    Ok(addr_sig_rows)
}

pub(crate) async fn write_batch_redis(
    db: &mut RedisAccountsDB,
    account_settlements: &[(Pubkey, AccountSettlement)],
    transactions: Vec<(
        Signature,
        &SanitizedTransaction,
        u64,
        UnixTimestamp,
        &ProcessedTransaction,
    )>,
    block_info: Option<BlockInfo>,
) -> Result<(), String> {
    // Use Redis pipeline for atomic batch operations
    let mut pipe = redis::pipe();
    pipe.atomic();

    // Update accounts
    for (pubkey, account_settlement) in account_settlements {
        let key = format!("account:{}", pubkey);
        if account_settlement.deleted {
            pipe.del(key);
        } else {
            let serialized = bincode::serialize(&account_settlement.account)
                .map_err(|e| format!("Failed to serialize account: {}", e))?;
            pipe.set(key, serialized);
        }
    }

    // Store transactions and build the address→signatures index used by
    // getSignaturesForAddress. For each account key touched by the transaction
    // we ZADD one entry to a per-address sorted set:
    //   key:    addr_sigs:{pubkey}
    //   score:  tx_slot as f64  (enables ZRANGE BYSCORE REV ordering by recency)
    //   member: hex-encoded signature (preserves byte ordering for same-slot DESC sort)
    // Mirrors what address_signatures does in Postgres.
    // redis-rs 0.27: zadd(key, member, score) — member first, score second.
    let tx_count = transactions.len();
    for (signature, transaction, tx_slot, block_time, processed) in transactions {
        let stored_tx = get_stored_transaction(transaction, tx_slot, block_time, processed);
        let key = format!("tx:{}", signature);
        let serialized = bincode::serialize(&stored_tx).unwrap();
        pipe.set(key, serialized);

        for pubkey in transaction.message().account_keys().iter() {
            let addr_key = format!("addr_sigs:{}", pubkey);
            pipe.zadd(addr_key, hex::encode(signature.as_ref()), tx_slot as f64);
        }
    }

    // Increment transaction count
    if tx_count > 0 {
        pipe.incr("transaction_count", tx_count);
    }

    // Store block info and update latest slot
    if let Some(block) = block_info {
        pipe.set("latest_blockhash", block.blockhash.to_string());
        pipe.set("latest_slot", block.slot);
        let key = format!("block:{}", block.slot);
        let serialized = bincode::serialize(&block).unwrap();
        pipe.set(key, serialized);
        // Index all slots in a sorted set (score = slot value) for two purposes:
        // 1. getBlocks: ZRANGE block_slot_index start end BYSCORE for O(log N + M) range queries.
        // 2. getFirstAvailableBlock: ZRANGE block_slot_index 0 0 returns the minimum slot.
        // ZADD is idempotent for the same (member, score) pair, so replays are safe.
        // redis-rs 0.27: zadd(key, member, score) — member first, score second.
        pipe.zadd("block_slot_index", block.slot, block.slot as f64);
    }

    // Execute pipeline - explicitly specify the return type to fix type inference
    let _: () = pipe
        .query_async(&mut db.connection)
        .await
        .map_err(|e| format!("Redis batch write failed: {}", e))?;

    Ok(())
}
