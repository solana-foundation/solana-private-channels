use {
    super::{
        get_block_height::read_block_height, get_first_available_block::read_first_available_block,
        get_transaction::read_transaction, postgres::PostgresAccountsDB, traits::AccountsDB,
        types::StoredTransaction,
    },
    anyhow::{Context, Result},
    solana_sdk::signature::Signature,
};

/// Transaction lookups, block height and ledger floor, all read from one committed state.
pub struct StatusSnapshot {
    /// `None` on a node that has produced no block yet.
    pub block_height: Option<u64>,
    pub first_available_block: u64,
    /// One entry per requested signature, in order; `None` means absent from this state.
    pub transactions: Vec<Option<StoredTransaction>>,
}

/// Read what an absence proof needs in one snapshot, so a stale miss never meets a newer height.
pub async fn get_signature_status_snapshot(
    db: &AccountsDB,
    signatures: &[Signature],
) -> Result<StatusSnapshot> {
    match db {
        AccountsDB::Postgres(postgres_db) => snapshot_postgres(postgres_db, signatures).await,
        // The cache can run ahead of Postgres, so every value comes from the source of truth.
        AccountsDB::Redis(redis_db) => snapshot_postgres(&redis_db.fallback, signatures).await,
    }
}

async fn snapshot_postgres(
    db: &PostgresAccountsDB,
    signatures: &[Signature],
) -> Result<StatusSnapshot> {
    let mut tx = db
        .pool
        .begin()
        .await
        .context("Failed to open the status snapshot transaction")?;
    // Repeatable read pins every statement below to the snapshot taken by the first one.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .context("Failed to pin the status snapshot to one state")?;

    let block_height = read_block_height(&mut tx).await?;
    let first_available_block = read_first_available_block(&mut tx).await?;
    let mut transactions = Vec::with_capacity(signatures.len());
    for signature in signatures {
        transactions.push(read_transaction(&mut *tx, signature).await?);
    }

    tx.commit()
        .await
        .context("Failed to close the status snapshot transaction")?;

    Ok(StatusSnapshot {
        block_height,
        first_available_block,
        transactions,
    })
}
