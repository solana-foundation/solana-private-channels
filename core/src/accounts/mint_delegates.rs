//! The permanent delegates the channel's mints name. Every key a mint names keeps its seize
//! power until that mint is rotated, so the node polices all of them, not just its own.

use {
    crate::vm::admin::CHANNEL_MINT_LEN,
    anyhow::{Context, Result},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
    },
    spl_token_2022::{
        extension::{
            permanent_delegate::PermanentDelegate, BaseStateWithExtensions, StateWithExtensions,
        },
        state::Mint,
    },
    sqlx::PgPool,
    std::collections::BTreeSet,
};

/// Stored row length of a channel mint, the only accounts that can carry a delegate.
fn mint_row_len() -> u64 {
    let mint = AccountSharedData::new(0, CHANNEL_MINT_LEN, &spl_token_2022::id());
    bincode::serialized_size(&mint).expect("an account always serializes")
}

/// Indexes the rows sized like a channel mint so boot reads them without a table scan.
/// Built by the writer before it serves; the first build scans the table once.
pub async fn ensure_index(pool: &PgPool) -> Result<()> {
    // CREATE INDEX takes its table lock before IF NOT EXISTS applies; skip it when present.
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('accounts_channel_mint_idx') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if exists {
        return Ok(());
    }
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS accounts_channel_mint_idx ON accounts (pubkey) \
         WHERE length(data) = {}",
        mint_row_len()
    ))
    .execute(pool)
    .await
    .context("Failed to build the channel mint index")?;
    Ok(())
}

/// Every key a channel mint names as permanent delegate. Empty when the accounts table does
/// not exist yet, as on a replica that has not received the writer's schema.
pub async fn read(pool: &PgPool) -> Result<BTreeSet<Pubkey>> {
    let has_table: bool = sqlx::query_scalar("SELECT to_regclass('accounts') IS NOT NULL")
        .fetch_one(pool)
        .await?;
    if !has_table {
        return Ok(BTreeSet::new());
    }
    let rows: Vec<Vec<u8>> = sqlx::query_scalar(&format!(
        "SELECT data FROM accounts WHERE length(data) = {}",
        mint_row_len()
    ))
    .fetch_all(pool)
    .await
    .context("Failed to read the channel mints")?;
    Ok(rows.iter().filter_map(|row| delegate_of(row)).collect())
}

/// The delegate a stored row names, if it is a Token-2022 mint with one.
fn delegate_of(row: &[u8]) -> Option<Pubkey> {
    let account: AccountSharedData = bincode::deserialize(row).ok()?;
    if account.owner() != &spl_token_2022::id() {
        return None;
    }
    let mint = StateWithExtensions::<Mint>::unpack(account.data()).ok()?;
    Option::<Pubkey>::from(mint.get_extension::<PermanentDelegate>().ok()?.delegate)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{test_helpers::start_test_postgres_raw, vm::admin::AdminVm},
        solana_sdk::account::WritableAccount,
    };

    async fn store(pool: &PgPool, account: &AccountSharedData) {
        sqlx::query("INSERT INTO accounts (pubkey, data) VALUES ($1, $2)")
            .bind(Pubkey::new_unique().to_bytes().to_vec())
            .bind(bincode::serialize(account).unwrap())
            .execute(pool)
            .await
            .unwrap();
    }

    /// Only Token-2022 mints count, each key once, whatever else shares the row size.
    #[tokio::test(flavor = "multi_thread")]
    async fn reads_every_key_a_mint_names() {
        let (db, _pg) = start_test_postgres_raw().await;
        let pool = db.pool.as_ref();
        let (a, b, authority) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        for delegate in [a, b, a] {
            store(
                pool,
                &AdminVm::create_mint_account(6, &authority, None, &delegate),
            )
            .await;
        }
        // Same bytes under another owner, and a mint-sized row that is not a mint.
        let mut foreign = AdminVm::create_mint_account(6, &authority, None, &authority);
        foreign.set_owner(Pubkey::new_unique());
        store(pool, &foreign).await;
        store(
            pool,
            &AccountSharedData::new(1, CHANNEL_MINT_LEN, &spl_token_2022::id()),
        )
        .await;

        ensure_index(pool).await.unwrap();
        ensure_index(pool).await.unwrap();
        assert_eq!(read(pool).await.unwrap(), BTreeSet::from([a, b]));
    }

    /// A replica that has not received the schema yet has no mints to police.
    #[tokio::test(flavor = "multi_thread")]
    async fn missing_accounts_table_reads_as_no_mints() {
        let (db, _pg) = start_test_postgres_raw().await;
        let pool = db.pool.as_ref();
        sqlx::query("DROP TABLE accounts")
            .execute(pool)
            .await
            .unwrap();
        assert!(read(pool).await.unwrap().is_empty());
    }
}
