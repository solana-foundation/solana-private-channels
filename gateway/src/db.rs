use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::Role;

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
