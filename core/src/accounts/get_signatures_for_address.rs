use {
    super::{postgres::PostgresAccountsDB, traits::AccountsDB},
    crate::accounts::types::StoredTransaction,
    anyhow::Result,
    solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    solana_transaction_status::{extract_memos::extract_and_fmt_memos, TransactionWithStatusMeta},
    solana_transaction_status_client_types::TransactionConfirmationStatus,
    sqlx::{PgPool, Row},
    std::sync::Arc,
};

pub async fn get_signatures_for_address(
    db: &AccountsDB,
    address: &Pubkey,
    limit: usize,
    before: Option<&Signature>,
    until: Option<&Signature>,
) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
    match db {
        AccountsDB::Postgres(postgres_db) => {
            get_signatures_for_address_postgres(postgres_db, address, limit, before, until).await
        }
        // Served from the source of truth, and no address index is cached. One
        // would hold only signatures written since the cache attached, so a
        // short history would read as a complete one. The cached form also
        // never supported before/until cursors.
        AccountsDB::Redis(redis_db) => {
            get_signatures_for_address_postgres(&redis_db.fallback, address, limit, before, until)
                .await
        }
    }
}

/// Resolves a pagination cursor signature to its `(slot, raw_signature_bytes)`
/// position in `address_signatures`.
///
/// Returns `Ok(None)` when `sig` is `None` (no cursor supplied by the caller).
///
/// Returns `Err` when the signature is supplied but not found in the table.
/// We treat it as an explicit error instead,
/// since the silent fallback breaks pagination in a non-obvious way.
///
/// The resolved `(slot, signature)` pair is passed as plain bind parameters
/// to the main query, avoiding correlated subqueries in the WHERE clause.
async fn resolve_cursor(
    pool: &PgPool,
    sig: Option<&Signature>,
    label: &str,
) -> Result<Option<(i64, Vec<u8>)>> {
    let sig = match sig {
        None => return Ok(None),
        Some(s) => s,
    };

    let row = sqlx::query(
        "SELECT slot, signature \
         FROM address_signatures \
         WHERE signature = $1 \
         LIMIT 1",
    )
    .bind(sig.as_ref() as &[u8])
    .fetch_optional(pool)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to look up '{}' cursor: {}", label, e))?;

    match row {
        Some(r) => Ok(Some((r.get("slot"), r.get("signature")))),
        None => Err(anyhow::anyhow!(
            "Transaction history for signature provided in '{}' is unavailable",
            label,
        )),
    }
}

async fn get_signatures_for_address_postgres(
    db: &PostgresAccountsDB,
    address: &Pubkey,
    limit: usize,
    before: Option<&Signature>,
    until: Option<&Signature>,
) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
    let pool = Arc::clone(&db.pool);
    let addr_bytes = address.to_bytes();

    // ── Resolve pagination cursors ─────────────────────────────────────────
    //
    // Each cursor is validated and resolved to a (slot, signature) position in
    // one query per cursor. This achieves two things:
    //
    //   1. Early error on unknown cursor — if the signature isn't in
    //      address_signatures we return an explicit error rather than silently
    //      producing a wrong or empty result set.
    //
    //   2. Simpler main query — the resolved positions are passed as plain bind
    //      parameters so the WHERE clause uses direct row comparisons rather
    //      than correlated subqueries.
    let before_pos = resolve_cursor(pool.as_ref(), before, "before").await?;
    let until_pos = resolve_cursor(pool.as_ref(), until, "until").await?;

    // Unpack into separate slot / signature components for sqlx binding.
    // sqlx does not support binding tuple types directly.
    let before_slot: Option<i64> = before_pos.as_ref().map(|(slot, _)| *slot);
    let before_sig: Option<&[u8]> = before_pos.as_ref().map(|(_, sig)| sig.as_slice());
    let until_slot: Option<i64> = until_pos.as_ref().map(|(slot, _)| *slot);
    let until_sig: Option<&[u8]> = until_pos.as_ref().map(|(_, sig)| sig.as_slice());

    // ── Main query ────────────────────────────────────────────────────────
    //
    // Join address_signatures with transactions to fetch the signature, slot,
    // and full transaction blob in a single round-trip.
    //
    // Results are ordered newest-first (ORDER BY slot DESC, signature DESC).
    // The signature tiebreaker keeps the sort stable when multiple transactions
    // land in the same slot.
    //
    // Cursor conditions use PostgreSQL row comparison so the composite
    // (slot, signature) ordering mirrors the ORDER BY exactly:
    //
    //   before (exclusive): (slot, sig) < ($2, $3)
    //     Returns transactions strictly older than the before cursor.
    //
    //   until  (inclusive): (slot, sig) >= ($4, $5)
    //     Returns transactions as old as — and including — the until cursor.
    //
    // When a cursor was not supplied its slot parameter ($2 or $4) is NULL,
    // which short-circuits the OR to FALSE and skips that filter entirely.
    //
    // LEFT JOIN: a missing transactions row surfaces as NULL data rather than
    // silently dropping the row. We treat a NULL as data corruption below.
    let rows = sqlx::query(
        "SELECT address_signatures.signature,
                address_signatures.slot,
                transactions.data
         FROM address_signatures
         LEFT JOIN transactions ON address_signatures.signature = transactions.signature
         WHERE address_signatures.address = $1
           AND ($2::bigint IS NULL
                OR (address_signatures.slot, address_signatures.signature) < ($2, $3::bytea))
           AND ($4::bigint IS NULL
                OR (address_signatures.slot, address_signatures.signature) >= ($4, $5::bytea))
         ORDER BY address_signatures.slot DESC, address_signatures.signature DESC
         LIMIT $6",
    )
    .bind(&addr_bytes[..])
    .bind(before_slot)
    .bind(before_sig)
    .bind(until_slot)
    .bind(until_sig)
    .bind(limit as i64)
    .fetch_all(pool.as_ref())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to query signatures for {}: {}", address, e))?;

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let sig_bytes: Vec<u8> = row.get("signature");
        let slot: i64 = row.get("slot");
        // NULL when the transaction row is missing (should never happen since
        // address_signatures and transactions are written in the same atomic tx).
        let tx_data: Option<Vec<u8>> = row.get("data");

        let signature = match Signature::try_from(sig_bytes.as_slice()) {
            Ok(s) => s,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to deserialize signature from address_signatures: {}",
                    e
                ));
            }
        };

        // address_signatures and transactions are written in the same atomic
        // DB transaction, so a missing transaction row is data corruption.
        let tx_data = match tx_data {
            Some(data) => data,
            None => {
                return Err(anyhow::anyhow!(
                    "Transaction data missing for signature {} — address_signatures index is inconsistent",
                    signature
                ));
            }
        };

        let stored_tx = match bincode::deserialize::<StoredTransaction>(&tx_data) {
            Ok(tx) => tx,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to deserialize transaction {}: {}",
                    signature,
                    e
                ));
            }
        };

        let err = stored_tx.meta.err.clone();
        let memo = match stored_tx.transaction_with_status_meta() {
            TransactionWithStatusMeta::Complete(versioned) => extract_and_fmt_memos(&versioned),
            _ => None,
        };

        results.push(RpcConfirmedTransactionStatusWithSignature {
            signature: signature.to_string(),
            slot: slot as u64,
            err,
            memo,
            block_time: Some(stored_tx.block_time),
            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
        });
    }

    Ok(results)
}
