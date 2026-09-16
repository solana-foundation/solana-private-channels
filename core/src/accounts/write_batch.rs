use {
    super::{
        counter,
        current_slot::CURRENT_SLOT_KEY,
        get_block_height::BLOCK_HEIGHT_KEY,
        get_latest_blockhash::LATEST_BLOCKHASH_KEY,
        get_latest_slot::LATEST_SLOT_KEY,
        owner_change::{rows_from_processed, upsert_owner_change_rows, OwnerChangeRow},
        postgres::PostgresAccountsDB,
        redis::RedisAccountsDB,
        traits::{AccountsDB, BlockInfo},
        transaction_count::TransactionCount,
        utils::get_stored_transaction,
        writer_epoch,
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

/// Why a batch did not commit. The two refusals mean this node is not the
/// writer any more, so the settler stops on them instead of retrying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteBatchError {
    /// A newer write node claimed the database after this one started.
    Fenced { held: u64, current: Option<u64> },
    /// The block does not build on the stored tip.
    StaleTip { slot: u64 },
    /// Storage failed; retrying may succeed.
    Other(String),
}

impl std::fmt::Display for WriteBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fenced { held, current } => write!(
                f,
                "Refusing to commit: this node holds writer epoch {held} but the database is \
                 at {}. A newer write node has taken over this database.",
                current.map_or("no epoch".to_string(), |c| c.to_string())
            ),
            // Names both causes, since the log line is what an operator sees first.
            Self::StaleTip { slot } => write!(
                f,
                "Refusing to commit slot {slot}: it does not extend the stored tip. \
                 Either a second write-capable node is running against this database, or \
                 this batch retries a slot that already committed."
            ),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for WriteBatchError {}

impl From<String> for WriteBatchError {
    fn from(msg: String) -> Self {
        Self::Other(msg)
    }
}

/// Bulk-insert into address_signatures inside an active PG tx.
pub(crate) async fn upsert_address_signature_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[AddressSignatureRow],
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let addresses: Vec<&[u8]> = rows.iter().map(|r| r.address.as_slice()).collect();
    let slots: Vec<i64> = rows.iter().map(|r| r.slot).collect();
    let sigs: Vec<&[u8]> = rows.iter().map(|r| r.signature.as_slice()).collect();
    sqlx::query(
        "INSERT INTO address_signatures (address, slot, signature)
         SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::bytea[])
         ON CONFLICT DO NOTHING",
    )
    .bind(&addresses)
    .bind(&slots)
    .bind(&sigs)
    .execute(&mut **tx)
    .await
    .map(|_| ())
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
) -> Result<Vec<AddressSignatureRow>, WriteBatchError> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            write_batch_postgres(postgres_db, account_settlements, transactions, block_info).await
        }
        AccountsDB::Redis(redis_db) => {
            write_batch_redis(redis_db, account_settlements, transactions, block_info)
                .await
                .map(|()| Vec::new())
                .map_err(WriteBatchError::Other)
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
) -> Result<Vec<AddressSignatureRow>, WriteBatchError> {
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
    // Ownership handoffs, on the other hand, commit with the block below. They
    // gate what a later owner may read, so one may never be visible later than
    // the signatures it covers. Handing an account on is rare enough that the
    // vector almost always stays empty.
    let mut owner_change_rows: Vec<OwnerChangeRow> = Vec::new();
    for (signature, transaction, tx_slot, block_time, processed) in transactions {
        let stored_tx = get_stored_transaction(transaction, tx_slot, block_time, processed);
        sig_bytes_vec.push(signature.as_ref().to_vec());
        let data = stored_tx
            .to_bytes()
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
        owner_change_rows.extend(rows_from_processed(
            transaction,
            processed,
            tx_slot,
            &signature,
        ));
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

    // First statement, so a superseded writer is refused before it takes any
    // other lock. FOR UPDATE makes a new writer's bump wait for this batch and
    // runs batches one at a time, which the parent check below relies on.
    if let Some(held) = db.writer_epoch {
        let current = writer_epoch::read_locked(&mut tx)
            .await
            .map_err(|e| format!("Failed to read the writer epoch: {}", e))?;
        if current != Some(held) {
            return Err(WriteBatchError::Fenced { held, current });
        }
    }

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

    // Inside the commit on purpose: a handoff that landed but went unrecorded
    // would let the new owner read the previous owner's history.
    upsert_owner_change_rows(&mut tx, &owner_change_rows)
        .await
        .map_err(|e| format!("Failed to bulk upsert owner changes: {}", e))?;

    // ── Block info: at most 2 queries (block row + chain tip metadata) ──
    // Runs before the counter because whether this slot is new is what decides
    // whether the counter may advance.
    let slot_is_new = if let (Some(block_info), Some(block_data)) = (&block_info, &block_data) {
        // The block must build on the stored tip (none means genesis), and a stored
        // slot only takes identical bytes, which admits a lost-ack retry. Stored bytes
        // never change, so a matching parent slot is a matching parent hash.
        let inserted: Option<bool> = sqlx::query_scalar(
            "INSERT INTO blocks (slot, data)
                 SELECT $1, $2
                 WHERE NOT EXISTS (SELECT 1 FROM blocks WHERE slot > $1)
                   AND COALESCE((SELECT MAX(slot) FROM blocks) IN ($1, $3), true)
                 ON CONFLICT (slot) DO UPDATE SET data = EXCLUDED.data
                   WHERE blocks.data = EXCLUDED.data
                 RETURNING (xmax = 0)",
        )
        .bind(block_info.slot as i64)
        .bind(block_data)
        .bind(block_info.parent_slot as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| format!("Failed to store block: {}", e))?;

        // No row means the tip is not this block's parent, or this slot holds a
        // different block. Either way another writer has passed this batch.
        let Some(inserted) = inserted else {
            return Err(WriteBatchError::StaleTip {
                slot: block_info.slot,
            });
        };

        // The tip blockhash and the chain counters go in one UNNEST upsert, so
        // making slot and height durable costs no extra round trip. They commit
        // with the block row, so a rolled-back batch leaves all three untouched.
        let keys: Vec<&str> = vec![
            LATEST_BLOCKHASH_KEY,
            LATEST_SLOT_KEY,
            CURRENT_SLOT_KEY,
            BLOCK_HEIGHT_KEY,
        ];
        let values: Vec<Vec<u8>> = vec![
            block_info.blockhash.as_ref().to_vec(),
            counter::encode(block_info.slot).to_vec(),
            counter::encode(block_info.slot).to_vec(),
            counter::encode(block_info.block_height.unwrap_or(block_info.slot)).to_vec(),
        ];
        sqlx::query(
            "INSERT INTO metadata (key, value)
                 SELECT * FROM UNNEST($1::varchar[], $2::bytea[])
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&keys)
        .bind(&values)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("Failed to update the chain tip metadata: {}", e))?;

        // `xmax = 0` is false on a replay, so the counter counts a slot once.
        inserted
    } else {
        // No slot to key on, so there is nothing to suppress.
        true
    };

    // Read-modify-write inside BEGIN…COMMIT. The block insert above already let
    // only one writer past for this slot, and a rejected writer's increment rolls
    // back with the rest of its batch.
    //
    // Skipped on a replayed slot: every other write here is an idempotent upsert,
    // so this is the one statement that would count the same batch twice.
    if tx_count > 0 && slot_is_new {
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

    // Commit — if this fails, the entire batch is rolled back.
    tx.commit()
        .await
        .map_err(|e| format!("Failed to commit transaction: {}", e))?;

    Ok(addr_sig_rows)
}

/// Drops the cache keys a failed `write_batch_redis` left holding pre-batch
/// values, so reads miss and resolve against Postgres rather than being served
/// something stale.
///
/// Only the account keys need it. A new `tx:` or `block:` key had no previous
/// value, so a skipped write leaves it absent, which already reads as a miss.
/// `latest_slot` and `latest_blockhash` are stale for at most one block, until
/// the next successful write overwrites them. An account key, by contrast, keeps
/// its pre-batch balance until that account is next touched, which may be never.
///
/// This narrows the window rather than being the only repair: the failed write
/// also left the cached tip behind, so the next batch's continuity check takes
/// the whole cache out of service and rebuilds it.
pub(crate) async fn invalidate_batch_redis(
    db: &mut RedisAccountsDB,
    account_settlements: &[(Pubkey, AccountSettlement)],
) {
    let mut pipe = redis::pipe();
    pipe.atomic();

    for (pubkey, _) in account_settlements {
        pipe.del(format!("account:{}", pubkey));
    }

    if let Err(e) = pipe.query_async::<()>(&mut db.connection).await {
        warn!("Failed to invalidate Redis keys after a failed cache write: {e}");
    }
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

    // Only the families a read can actually be served from are mirrored:
    // point lookups by pubkey, signature and slot, plus the chain tip. The
    // address index, slot index and transaction counter used to be written here
    // too, but nothing reads them from the cache any more: a range, a history
    // or a counter cannot express a cache miss, so those reads go straight to
    // Postgres. Writing them was work whose only effect was to be purged later.
    for (signature, transaction, tx_slot, block_time, processed) in transactions {
        let stored_tx = get_stored_transaction(transaction, tx_slot, block_time, processed);
        let key = format!("tx:{}", signature);
        let serialized = stored_tx
            .to_bytes()
            .map_err(|e| format!("Failed to serialize transaction: {}", e))?;
        pipe.set(key, serialized);
    }

    // Store block info and update latest slot
    if let Some(block) = block_info {
        pipe.set(LATEST_BLOCKHASH_KEY, block.blockhash.to_string());
        pipe.set(LATEST_SLOT_KEY, block.slot);
        // The live slot moves on idle ticks too, but a block still republishes
        // it so a replica never reports a slot behind the block it can fetch.
        pipe.set(CURRENT_SLOT_KEY, block.slot);
        // Mirrored so a read replica reports a height consistent with the hash
        // it serves from the same cache.
        pipe.set(BLOCK_HEIGHT_KEY, block.block_height.unwrap_or(block.slot));
        let key = format!("block:{}", block.slot);
        let serialized = bincode::serialize(&block).unwrap();
        // Only block entries expire. The tip keys the coherence check reads are
        // never given a TTL, so an expiry can neither condemn the cache nor
        // trigger a rebuild.
        match db.block_ttl_secs() {
            0 => pipe.set(key, serialized),
            ttl => pipe.set_ex(key, serialized, ttl),
        };
    }

    // Execute pipeline - explicitly specify the return type to fix type inference
    let _: () = pipe
        .query_async(&mut db.connection)
        .await
        .map_err(|e| format!("Redis batch write failed: {}", e))?;

    Ok(())
}
