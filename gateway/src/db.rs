use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::Role;

/// One recorded handoff of a token account between wallets, as the core node
/// wrote it. Owners come back base58-encoded to match `verified_wallets.pubkey`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerChange {
    pub slot: i64,
    pub prev_owner: String,
    pub new_owner: String,
}

/// Every recorded owner handoff for `address`, oldest first.
///
/// This is the gateway's one read outside the `private_channel_auth` schema.
/// The auth service keeps its objects in their own schema to stay clear of the
/// ledger's (see `auth/src/db.rs`), and both live in the same database, so the
/// pool already in hand can serve this. Qualified explicitly rather than left to
/// `search_path`, and read-only: the node owns these rows.
pub async fn owner_changes(pool: &PgPool, address: &[u8]) -> Result<Vec<OwnerChange>, sqlx::Error> {
    let rows: Vec<(i64, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        r#"
        SELECT slot, prev_owner, new_owner
        FROM public.token_account_owner_change
        WHERE address = $1
        ORDER BY slot ASC, signature ASC
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(slot, prev_owner, new_owner)| OwnerChange {
            slot,
            prev_owner: bs58::encode(prev_owner).into_string(),
            new_owner: bs58::encode(new_owner).into_string(),
        })
        .collect())
}

/// Returns the role currently stored for `user_id`, or `None` if the user no
/// longer exists. The gateway uses this to confirm that a JWT's Operator claim
/// still matches the DB, since a token can outlive a demotion by up to 24h.
pub async fn get_user_role(pool: &PgPool, user_id: Uuid) -> Result<Option<Role>, sqlx::Error> {
    let row: Option<(String,)> =
        sqlx::query_as(r#"SELECT role::text FROM private_channel_auth.users WHERE id = $1"#)
            .bind(user_id)
            .fetch_optional(pool)
            .await?;

    Ok(row.map(|(role,)| match role.as_str() {
        "operator" => Role::Operator,
        _ => Role::User,
    }))
}

/// Returns `true` if `pubkey` is registered in `private_channel_auth.verified_wallets`
/// for the given `user_id`.
///
/// This is the ownership check the gateway performs before allowing a User-role
/// JWT to access account data. Operators bypass this check entirely.
pub async fn is_wallet_owned_by_user(
    pool: &PgPool,
    user_id: Uuid,
    pubkey: &str,
) -> Result<bool, sqlx::Error> {
    let exists: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM private_channel_auth.verified_wallets
            WHERE user_id = $1 AND pubkey = $2
        )
        "#,
    )
    .bind(user_id)
    .bind(pubkey)
    .fetch_one(pool)
    .await?;

    Ok(exists.0)
}
