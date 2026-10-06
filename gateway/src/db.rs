use sqlx::{PgConnection, PgPool};
use std::collections::HashSet;
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

/// The `limit` most recent recorded owner handoffs for `address`, oldest first.
///
/// This is the gateway's one read outside the `private_channel_auth` schema.
/// The auth service keeps its objects in their own schema to stay clear of the
/// ledger's (see `auth/src/db.rs`), and both live in the same database, so the
/// pool already in hand can serve this. Qualified explicitly rather than left to
/// `search_path`, and read-only: the node owns these rows.
///
/// Handing an account on is unrestricted and costs the sender nothing, so the
/// row count for one address is attacker-controlled and has to be bounded. The
/// newest rows are the ones worth spending that bound on: these rows are never
/// deleted, so reading from the oldest end would let churn that has long since
/// scrolled past decide what the current owner may read. The caller decides what
/// a chain that fills the limit means.
///
/// Takes a connection so it can be read in one snapshot with
/// `owner_change_coverage`.
pub async fn owner_changes(
    connection: &mut PgConnection,
    address: &[u8],
    limit: i64,
) -> Result<Vec<OwnerChange>, sqlx::Error> {
    let rows: Vec<(i64, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        r#"
        SELECT slot, prev_owner, new_owner
        FROM public.token_account_owner_change
        WHERE address = $1
        ORDER BY slot DESC, tx_index DESC
        LIMIT $2
        "#,
    )
    .bind(address)
    .bind(limit)
    .fetch_all(&mut *connection)
    .await?;

    // Taken newest first to bound the read, handed back oldest first because
    // that is the order the timeline chains in.
    Ok(rows
        .into_iter()
        .rev()
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

/// Mirror the node's metadata keys.
const OWNER_CHANGE_INDEXED_FROM_KEY: &str = "owner_change_indexed_from_slot";
const LATEST_SLOT_KEY: &str = "latest_slot";

/// The slots the ledger's handoff table can vouch for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerChangeCoverage {
    /// First slot handoffs are recorded from. Below it an absent handoff means
    /// "not written yet" rather than "never happened".
    pub indexed_from: i64,
    /// Newest slot the ledger has committed.
    pub tip: i64,
}

/// The watermark and the tip, or `None` if either is missing.
///
/// Read in the same snapshot as `owner_changes`: the node commits the tip with
/// its block's handoffs, so that chain holds every handoff at or below it. The
/// watermark is stored big-endian and the tip as a little-endian counter.
pub async fn owner_change_coverage(
    connection: &mut PgConnection,
) -> Result<Option<OwnerChangeCoverage>, sqlx::Error> {
    let rows: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT key, value FROM public.metadata WHERE key = ANY($1)")
            .bind([OWNER_CHANGE_INDEXED_FROM_KEY, LATEST_SLOT_KEY])
            .fetch_all(&mut *connection)
            .await?;

    let mut indexed_from = None;
    let mut tip = None;
    for (key, value) in rows {
        let Ok(bytes) = <[u8; 8]>::try_from(value.as_slice()) else {
            continue;
        };
        match key.as_str() {
            OWNER_CHANGE_INDEXED_FROM_KEY => indexed_from = Some(i64::from_be_bytes(bytes)),
            LATEST_SLOT_KEY => tip = i64::try_from(u64::from_le_bytes(bytes)).ok(),
            _ => {}
        }
    }

    Ok(indexed_from
        .zip(tip)
        .map(|(indexed_from, tip)| OwnerChangeCoverage { indexed_from, tip }))
}

/// Which of `pubkeys` are verified wallets of `user_id`, in one round trip.
///
/// A chain of handoffs can name many wallets, and asking about each one in turn
/// would put an attacker-controlled number of sequential queries on the pool the
/// auth service shares.
pub async fn owned_wallets(
    pool: &PgPool,
    user_id: Uuid,
    pubkeys: &[String],
) -> Result<HashSet<String>, sqlx::Error> {
    if pubkeys.is_empty() {
        return Ok(HashSet::new());
    }

    let owned: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT pubkey FROM private_channel_auth.verified_wallets
        WHERE user_id = $1 AND pubkey = ANY($2)
        "#,
    )
    .bind(user_id)
    .bind(pubkeys)
    .fetch_all(pool)
    .await?;

    Ok(owned.into_iter().map(|(pubkey,)| pubkey).collect())
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
