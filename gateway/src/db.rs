use sqlx::PgPool;
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

/// An address's handoffs and the slots they are known to cover, read together.
pub struct OwnerChain {
    /// `None` if the watermark or the tip is missing.
    pub coverage: Option<OwnerChangeCoverage>,
    /// The newest recorded handoffs, oldest first.
    pub changes: Vec<OwnerChange>,
}

/// The coverage and the `limit` most recent recorded handoffs for `address`.
///
/// One statement, so one snapshot: the node commits the tip with its block's
/// handoffs, so the chain read here holds every handoff at or below the tip
/// read beside it. Splitting the statement would lose that.
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
pub async fn owner_chain(
    pool: &PgPool,
    address: &[u8],
    limit: i64,
) -> Result<OwnerChain, sqlx::Error> {
    type OwnerChainRow = (
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<i64>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
    );
    // Joined on true so an address with no handoffs still returns the coverage.
    let rows: Vec<OwnerChainRow> = sqlx::query_as(
        r#"
        WITH coverage AS (
            SELECT
                (SELECT value FROM public.metadata WHERE key = $3) AS indexed_from,
                (SELECT value FROM public.metadata WHERE key = $4) AS tip
        ),
        chain AS (
            SELECT slot, tx_index, prev_owner, new_owner
            FROM public.token_account_owner_change
            WHERE address = $1
            ORDER BY slot DESC, tx_index DESC
            LIMIT $2
        )
        SELECT coverage.indexed_from, coverage.tip, chain.slot, chain.prev_owner, chain.new_owner
        FROM coverage LEFT JOIN chain ON true
        ORDER BY chain.slot DESC, chain.tx_index DESC
        "#,
    )
    .bind(address)
    .bind(limit)
    .bind(OWNER_CHANGE_INDEXED_FROM_KEY)
    .bind(LATEST_SLOT_KEY)
    .fetch_all(pool)
    .await?;

    // Every row carries the same coverage. The node writes the watermark
    // big-endian and the tip as a little-endian counter.
    let coverage = rows.first().and_then(|(indexed_from, tip, ..)| {
        let indexed_from = <[u8; 8]>::try_from(indexed_from.as_deref()?).ok()?;
        let tip = <[u8; 8]>::try_from(tip.as_deref()?).ok()?;
        Some(OwnerChangeCoverage {
            indexed_from: i64::from_be_bytes(indexed_from),
            tip: i64::try_from(u64::from_le_bytes(tip)).ok()?,
        })
    });

    // Taken newest first to bound the read, handed back oldest first because
    // that is the order the timeline chains in. The row with no slot is the
    // coverage alone.
    let changes = rows
        .into_iter()
        .rev()
        .filter_map(|(_, _, slot, prev_owner, new_owner)| {
            Some(OwnerChange {
                slot: slot?,
                prev_owner: bs58::encode(prev_owner?).into_string(),
                new_owner: bs58::encode(new_owner?).into_string(),
            })
        })
        .collect();

    Ok(OwnerChain { coverage, changes })
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
