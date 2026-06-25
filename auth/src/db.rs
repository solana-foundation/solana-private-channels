use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::AppResult,
    models::{Challenge, Role, User, VerifiedWallet},
};

/// Create all tables, enums and indexes if they don't already exist.
/// Safe to call on every startup.
///
/// All objects live under the `private_channel_auth` schema to isolate them from the
/// core application tables (accounts, transactions, etc.) in the same database.
pub async fn init_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Create the dedicated schema first.
    sqlx::query("CREATE SCHEMA IF NOT EXISTS private_channel_auth")
        .execute(pool)
        .await?;

    // Create the role enum scoped to the private_channel_auth schema.
    sqlx::query(
        r#"
        DO $$ BEGIN
            CREATE TYPE private_channel_auth.user_role AS ENUM ('operator', 'user');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS private_channel_auth.users (
            id UUID PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role private_channel_auth.user_role NOT NULL DEFAULT 'user',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS private_channel_auth.challenges (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES private_channel_auth.users(id) ON DELETE CASCADE,
            nonce UUID NOT NULL UNIQUE,
            expires_at TIMESTAMPTZ NOT NULL,
            used_at TIMESTAMPTZ
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS private_channel_auth.verified_wallets (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES private_channel_auth.users(id) ON DELETE CASCADE,
            pubkey TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, pubkey)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_verified_wallets_user_id ON private_channel_auth.verified_wallets (user_id)"#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_challenges_user_id ON private_channel_auth.challenges (user_id)"#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn find_user_by_username(pool: &PgPool, username: &str) -> AppResult<Option<User>> {
    let row: Option<(Uuid, String, String, String, DateTime<Utc>)> = sqlx::query_as(
        r#"SELECT id, username, password_hash, role::text, created_at FROM private_channel_auth.users WHERE username = $1"#,
    )
    .bind(username)
    .fetch_optional(pool)
    .await?;

    Ok(
        row.map(|(id, username, password_hash, role, created_at)| User {
            id,
            username,
            password_hash,
            role: match role.as_str() {
                "operator" => Role::Operator,
                _ => Role::User,
            },
            created_at,
        }),
    )
}

pub async fn insert_user(pool: &PgPool, username: &str, password_hash: &str) -> AppResult<User> {
    let row: (Uuid, String, String, String, DateTime<Utc>) = sqlx::query_as(
        r#"
        INSERT INTO private_channel_auth.users (id, username, password_hash, role)
        VALUES ($1, $2, $3, 'user')
        RETURNING id, username, password_hash, role::text, created_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(username)
    .bind(password_hash)
    .fetch_one(pool)
    .await?;

    Ok(User {
        id: row.0,
        username: row.1,
        password_hash: row.2,
        role: Role::User,
        created_at: row.4,
    })
}

/// Set a user's role by username. Returns `false` if no such user exists.
/// Takes the typed `Role` so the variant is compiler-enforced; the SQL casts
/// the lowercase string to the postgres `user_role` enum.
pub async fn set_user_role(pool: &PgPool, username: &str, role: Role) -> AppResult<bool> {
    let role_str = match role {
        Role::Operator => "operator",
        Role::User => "user",
    };
    let result =
        sqlx::query(r#"UPDATE private_channel_auth.users SET role = $2::private_channel_auth.user_role WHERE username = $1"#)
            .bind(username)
            .bind(role_str)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() > 0)
}

/// Insert a new challenge tied to this user. Expires in 10 minutes.
pub async fn insert_challenge(pool: &PgPool, user_id: Uuid, nonce: Uuid) -> AppResult<Challenge> {
    let expires_at = Utc::now() + chrono::Duration::minutes(10);

    let row: (Uuid, DateTime<Utc>) = sqlx::query_as(
        r#"
        INSERT INTO private_channel_auth.challenges (id, user_id, nonce, expires_at)
        VALUES ($1, $2, $3, $4)
        RETURNING nonce, expires_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(user_id)
    .bind(nonce)
    .bind(expires_at)
    .fetch_one(pool)
    .await?;

    Ok(Challenge {
        nonce: row.0,
        expires_at: row.1,
    })
}

/// Mark the challenge as used and return it. Returns None if not found, already used, or expired.
/// The atomic UPDATE prevents the same challenge from being consumed twice.
pub async fn consume_challenge(
    pool: &PgPool,
    user_id: Uuid,
    nonce: Uuid,
) -> AppResult<Option<Challenge>> {
    let row: Option<(Uuid, DateTime<Utc>)> = sqlx::query_as(
        r#"
        UPDATE private_channel_auth.challenges SET used_at = NOW()
        WHERE user_id = $1 AND nonce = $2 AND used_at IS NULL AND expires_at > NOW()
        RETURNING nonce, expires_at
        "#,
    )
    .bind(user_id)
    .bind(nonce)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(nonce, expires_at)| Challenge { nonce, expires_at }))
}

pub async fn insert_verified_wallet(
    pool: &PgPool,
    user_id: Uuid,
    pubkey: &str,
) -> AppResult<VerifiedWallet> {
    let row: (String, DateTime<Utc>) = sqlx::query_as(
        r#"
        INSERT INTO private_channel_auth.verified_wallets (id, user_id, pubkey)
        VALUES ($1, $2, $3)
        RETURNING pubkey, created_at
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(user_id)
    .bind(pubkey)
    .fetch_one(pool)
    .await?;

    Ok(VerifiedWallet {
        pubkey: row.0,
        created_at: row.1,
    })
}

/// Delete a verified wallet for the given user. Returns `true` if the wallet was found
/// and deleted, `false` if it was not associated with the user.
pub async fn delete_verified_wallet(
    pool: &PgPool,
    user_id: Uuid,
    pubkey: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"DELETE FROM private_channel_auth.verified_wallets WHERE user_id = $1 AND pubkey = $2"#,
    )
    .bind(user_id)
    .bind(pubkey)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Delete challenges that are expired or already used.
/// Safe to call at any frequency; designed to run as a periodic background task.
/// Returns the number of rows deleted.
pub async fn cleanup_stale_challenges(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"DELETE FROM private_channel_auth.challenges WHERE used_at IS NOT NULL OR expires_at < NOW()"#,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

pub async fn list_verified_wallets(pool: &PgPool, user_id: Uuid) -> AppResult<Vec<VerifiedWallet>> {
    let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
        r#"SELECT pubkey, created_at FROM private_channel_auth.verified_wallets WHERE user_id = $1"#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(pubkey, created_at)| VerifiedWallet { pubkey, created_at })
        .collect())
}
