use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hyper::body::Bytes;
use hyper::StatusCode;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::LazyLock;
use uuid::Uuid;

use dvp_swap_program_client::{accounts::SwapDvp, DVP_SWAP_PROGRAM_ID};
use solana_pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;

use tracing::warn;

use crate::db::{
    is_wallet_owned_by_user, owned_wallets, owner_change_indexed_from, owner_changes, OwnerChange,
};

// ---------------------------------------------------------------------------
// Auth types — local minimal copies of auth service types.
// The gateway only needs to verify JWTs, so we avoid depending on the full
// auth crate (which would pull in Axum, Argon2, sqlx, etc.).
// The string values here MUST match what the auth service encodes into tokens.
// ---------------------------------------------------------------------------

/// User roles. Serialized as lowercase strings inside the JWT payload,
/// matching the auth service's encoding (`"operator"` / `"user"`).
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Operator,
    User,
}

/// Expected `iss` claim. Must match `JWT_ISSUER` in the auth service's jwt.rs.
const JWT_ISSUER: &str = "private-channel-auth";

/// Expected `aud` claim. Must match `JWT_AUDIENCE` in the auth service's jwt.rs.
const JWT_AUDIENCE: &str = "private-channel-gateway";

/// The claims the auth service embeds in every JWT.
/// `jsonwebtoken` automatically validates `exp` (expiry) during decode.
#[derive(Debug, Deserialize, Serialize)]
pub struct Claims {
    /// Subject — the authenticated user's UUID (as a string; the gateway
    /// parses it into a Uuid only when a DB lookup is needed).
    pub sub: String,
    /// RBAC role, used to gate operator-only methods.
    pub role: Role,
    /// Expiry timestamp (Unix seconds). Validated automatically.
    pub exp: usize,
}

// ---------------------------------------------------------------------------
// Method policy
// ---------------------------------------------------------------------------

/// Methods that require a valid JWT with the Operator role.
/// Callers without a token receive 401; callers with a User-role JWT receive 403.
const OPERATOR_ONLY_METHODS: &[&str] = &["getBlock", "getTransaction", "simulateTransaction"];

/// Methods that require a valid JWT. For User-role callers an ownership check
/// is also performed (the requested pubkey must be in their verified wallets).
/// Operator-role callers bypass the ownership check and can access any account.
///
/// params[0] for both methods is the target account pubkey, per the Solana
/// JSON-RPC spec:
///   getAccountInfo:         params: [pubkey, {encoding, ...}]
///   getTokenAccountBalance: params: [pubkey]
///
/// Known limitation for `getSignaturesForAddress`: ownership is derived from
/// the current on-chain account state. If a TokenAccount has been closed, the
/// account fetch returns `NotFound` and the User is rejected with 403 — even for
/// signatures from when they owned the account. We accept this: closing a
/// TokenAccount is rare in our context (no rent to reclaim for users), and the
/// alternatives (snapshotting ownership at ingest, or deriving ATAs from a
/// mint param) add schema or API complexity that isn't justified today.
const ACCOUNT_GATED_METHODS: &[&str] = &[
    "getAccountInfo",
    "getTokenAccountBalance",
    GET_SIGNATURES_FOR_ADDRESS,
];

/// Whether `method` requires any auth check (operator-only or account-gated).
/// Callers use this to skip JWT verification and the operator DB re-check for
/// public methods.
pub fn is_gated(method: &str) -> bool {
    OPERATOR_ONLY_METHODS.contains(&method) || ACCOUNT_GATED_METHODS.contains(&method)
}

/// Transaction history. Gated like the two above.
pub const GET_SIGNATURES_FOR_ADDRESS: &str = "getSignaturesForAddress";

/// Gated methods that a token-account delegate must not unlock. A delegate is a
/// current spend authority, often temporary and allowance-scoped, so it may read
/// the balance it can spend. It says nothing about who controlled the address
/// when past transactions landed, so it cannot open a history page.
const OWNER_ONLY_METHODS: &[&str] = &[GET_SIGNATURES_FOR_ADDRESS];

/// Whether `method` reads history rather than current state, and so must be
/// scoped to the slots its caller owned the address for.
pub fn is_owner_only(method: &str) -> bool {
    OWNER_ONLY_METHODS.contains(&method)
}

/// Signature status lookup. Ungated, since any caller may poll a signature it
/// already holds, but its response carries the same execution errors as a
/// history page.
const GET_SIGNATURE_STATUSES: &str = "getSignatureStatuses";

/// Methods whose response carries per-transaction execution errors. A stored
/// transaction is indexed under every account it touched, and its status is
/// readable by anyone holding the signature, so neither method's authorization
/// proves the caller may see which account made execution fail.
const ERROR_BEARING_METHODS: &[&str] = &[GET_SIGNATURES_FOR_ADDRESS, GET_SIGNATURE_STATUSES];

/// Whether `method`'s response must have its transaction errors collapsed before
/// it reaches this caller. Only an Operator keeps the raw diagnostics; a User
/// and an anonymous caller are treated alike, because the attack works with no
/// token at all. Callers reaching the gateway's internal listener never get
/// here: see `Access` in lib.rs.
pub fn redacts_transaction_errors(
    auth_header: Option<&str>,
    decoding_key: &DecodingKey,
    method: &str,
) -> bool {
    redacts_transaction_errors_for(verify_bearer(auth_header, decoding_key).as_ref(), method)
}

/// Same rule, decided from claims already resolved against the auth DB. Gated
/// methods must use this form so a demoted operator loses the raw diagnostics
/// at the same moment it loses access, not when its token expires.
pub fn redacts_transaction_errors_for(claims: Option<&Claims>, method: &str) -> bool {
    if !ERROR_BEARING_METHODS.contains(&method) {
        return false;
    }

    claims.map(|c| &c.role) != Some(&Role::Operator)
}

// ---------------------------------------------------------------------------
// Token program IDs
//
// We use the program `owner` field from the getAccountInfo response (i.e. the
// program that owns the account) to confirm we are looking at a token account
// before attempting any byte-level inspection.
// ---------------------------------------------------------------------------

/// SPL Token program — owns both regular token accounts and mints.
const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Token-2022 program — same account layout for the base 165 bytes; extensions
/// are appended after that. We support it now so future programs using Token-2022
/// work without changes.
const SPL_TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

// ---------------------------------------------------------------------------
// SPL token account layout constants (shared by Token and Token-2022)
//
// Both programs use identical byte offsets for the base fields.
// Token-2022 accounts may be larger (extensions after byte 165), so we check
// `len >= TOKEN_ACCOUNT_SIZE` rather than `len == TOKEN_ACCOUNT_SIZE`.
//
// Mints (82 bytes) are also owned by the same token programs, so we still
// need the size check to distinguish mint from token account.
// ---------------------------------------------------------------------------

/// Minimum size of a valid token account. Anything smaller (e.g. mint = 82 bytes)
/// is not a token account and is denied for User-role callers.
const TOKEN_ACCOUNT_SIZE: usize = 165;

/// Byte range of the `mint` field, the first thing in a token account.
const MINT_OFFSET: usize = 0;
const MINT_END: usize = 32;

/// Byte range of the `owner` field: the wallet pubkey that controls the account.
const OWNER_OFFSET: usize = 32;
const OWNER_END: usize = 64;

/// Byte offset of the `delegate` Option discriminant (u32 LE: 0=None, 1=Some).
const DELEGATE_OPTION_OFFSET: usize = 72;

/// Byte range of the `delegate` field, only valid when bytes [72..76] == [1,0,0,0].
const DELEGATE_OFFSET: usize = 76;
const DELEGATE_END: usize = 108;

// ---------------------------------------------------------------------------
// DvP swap escrow program
//
// Accounts owned by this program hold a SwapDvp struct for a P2P token swap.
// Read access is granted to either trading party (user_a, user_b) or the
// settlement_authority. The program ID, account size, and field layout all come
// from the vendored client (dvp-swap-program-client), so they can't drift from
// the committed .so when the layout or program ID changes upstream.
// ---------------------------------------------------------------------------

/// DvP swap escrow program ID (base58), from the client's declared program ID.
static DVP_SWAP_PROGRAM: LazyLock<String> = LazyLock::new(|| DVP_SWAP_PROGRAM_ID.to_string());

// ---------------------------------------------------------------------------
// Auth decision
// ---------------------------------------------------------------------------

/// The result of an auth check. `enforce_auth` acts on this without needing
/// to know any of the policy logic.
pub enum AuthDecision {
    /// The request is allowed to proceed to the backend.
    Proceed,
    /// The request must be rejected with the given status and JSON-RPC error body.
    Reject(StatusCode, Bytes),
    /// JWT is valid and the caller is a User. `enforce_auth` must fetch the raw
    /// account data from the read node and call `check_account_data_ownership`
    /// to determine whether the caller owns the queried account.
    NeedsAccountFetch { user_id: Uuid, pubkey: String },
}

// ---------------------------------------------------------------------------
// Auth entry point — called by enforce_auth
// ---------------------------------------------------------------------------

/// Checks whether the request is authorised to call `method` with `params`,
/// given the caller's already-verified `claims` (`None` if the JWT was missing,
/// invalid, or expired).
///
/// `claims.role` is treated as the caller's effective role: the caller confirms
/// an Operator claim against the DB first and passes `User` when it was revoked,
/// so no DB lookup happens here for the role.
///
/// - Method not gated: `Proceed`.
/// - No valid token on a gated method: `Reject(401)`.
/// - Operator: `Proceed` (unrestricted read access).
/// - User: `NeedsAccountFetch`, so the caller can fetch the raw account data and
///   run the ownership check in `check_account_data_ownership`.
///
/// The User path defers the ownership DB lookup to `check_account_data_ownership`
/// because users almost always query ATAs or PDAs rather than their wallet
/// pubkeys directly, so checking `verified_wallets` up-front would usually be a
/// wasted round-trip.
pub fn check_request_auth(claims: Option<&Claims>, method: &str, params: &Value) -> AuthDecision {
    let is_operator_only = OPERATOR_ONLY_METHODS.contains(&method);
    let is_account_gated = ACCOUNT_GATED_METHODS.contains(&method);

    if !is_operator_only && !is_account_gated {
        return AuthDecision::Proceed;
    }

    let claims = match claims {
        Some(c) => c,
        None => return AuthDecision::Reject(StatusCode::UNAUTHORIZED, unauthorized_body()),
    };

    if is_operator_only && claims.role != Role::Operator {
        return AuthDecision::Reject(StatusCode::FORBIDDEN, operator_only_body());
    }

    if claims.role == Role::Operator {
        return AuthDecision::Proceed;
    }

    // params[0] is the target account pubkey per the Solana JSON-RPC spec.
    let pubkey = match params.get(0).and_then(|v| v.as_str()) {
        Some(pk) => pk,
        None => return AuthDecision::Reject(StatusCode::BAD_REQUEST, missing_pubkey_body()),
    };

    let user_id = match Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => return AuthDecision::Reject(StatusCode::UNAUTHORIZED, unauthorized_body()),
    };

    AuthDecision::NeedsAccountFetch {
        user_id,
        pubkey: pubkey.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Ownership check — called by enforce_auth after fetching the raw account data
// ---------------------------------------------------------------------------

/// Checks whether `user_id` owns the account at `pubkey`, inspecting its raw
/// `data` without full deserialization. Dispatches on `program_owner` — the
/// account-level `owner` field from the `getAccountInfo` response, i.e. the
/// program that owns this account:
///
/// - SPL Token / Token-2022: `check_token_account_ownership`.
/// - DvP swap escrow: `check_swap_dvp_ownership`.
/// - Anything else (e.g. a System Program wallet account): falls back to
///   checking whether the `pubkey` itself is a verified wallet.
///
/// `method` narrows the token-account check only: see `OWNER_ONLY_METHODS`.
pub async fn check_account_data_ownership(
    data: &[u8],
    program_owner: &str,
    pubkey: &str,
    method: &str,
    user_id: Uuid,
    auth_db: &PgPool,
) -> AuthDecision {
    match program_owner {
        SPL_TOKEN_PROGRAM | SPL_TOKEN_2022_PROGRAM => {
            check_token_account_ownership(data, method, user_id, auth_db).await
        }
        owner if owner == DVP_SWAP_PROGRAM.as_str() => {
            check_swap_dvp_ownership(data, user_id, auth_db).await
        }
        // Non-token-program account (e.g. System Program wallet, unknown PDA).
        // The account bytes don't have a meaningful owner field to inspect, so
        // fall back to checking whether the pubkey itself is a verified wallet.
        _ => match is_wallet_owned_by_user(auth_db, user_id, pubkey).await {
            Ok(true) => AuthDecision::Proceed,
            Ok(false) => AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body()),
            Err(_) => AuthDecision::Reject(StatusCode::INTERNAL_SERVER_ERROR, db_error_body()),
        },
    }
}

/// Whether `pubkey` is still the associated token account its own `owner` field
/// derives to.
///
/// A user's token accounts are all ATAs: the ingress allowlist limits System to
/// `Transfer`, so `CreateAccount` is unreachable and only the ATA program can
/// make them. An ATA's address is derived from the wallet that owned it at
/// creation, and nothing rewrites the address afterwards. So an owner the
/// address no longer derives to is proof the owner field was moved, whatever
/// moved it, including a CPI this gateway never sees.
///
/// Only meaningful for an account with no recorded handoff. After a recorded
/// one the address is expected not to derive.
fn derives_as_own_ata(data: &[u8], program_owner: &str, pubkey: &str) -> bool {
    let (Ok(mint), Ok(owner), Ok(token_program), Ok(address)) = (
        Pubkey::try_from(&data[MINT_OFFSET..MINT_END]),
        Pubkey::try_from(&data[OWNER_OFFSET..OWNER_END]),
        Pubkey::from_str(program_owner),
        Pubkey::from_str(pubkey),
    ) else {
        return false;
    };

    get_associated_token_address_with_program_id(&owner, &mint, &token_program) == address
}

/// Count how a history request's scoping resolved. An empty scope is served as
/// an empty page, which is indistinguishable from an address with no history
/// unless something counts it.
fn record_scope_outcome(outcome: &str) {
    crate::metrics::GATEWAY_HISTORY_SCOPED_TOTAL
        .with_label_values(&[outcome])
        .inc();
}

/// How long an ownership chain the gateway will read for one address. Real
/// accounts change hands rarely; a chain near this length is manufactured.
const MAX_OWNER_CHANGES: i64 = 64;

/// The slot ranges `user_id` may read `pubkey`'s history for.
///
/// `None` means no filtering: the address never changed hands, so all of it is
/// theirs. `Some(ranges)` keeps only entries whose slot falls inside one of
/// them, and an empty `Some` hides the page entirely.
///
/// Only a token account can change hands; a wallet address is its own identity
/// for as long as it exists, so it never needs a range.
pub async fn resolve_owned_slot_ranges(
    data: &[u8],
    program_owner: &str,
    pubkey: &str,
    user_id: Uuid,
    auth_db: &PgPool,
) -> Result<Option<Vec<(i64, i64)>>, sqlx::Error> {
    if !matches!(program_owner, SPL_TOKEN_PROGRAM | SPL_TOKEN_2022_PROGRAM)
        || data.len() < TOKEN_ACCOUNT_SIZE
    {
        record_scope_outcome("not_a_token_account");
        return Ok(None);
    }

    let Ok(address) = bs58::decode(pubkey).into_vec() else {
        // The fetch resolved this pubkey, so it decodes. Hide the page rather
        // than serve it unscoped if that ever stops being true.
        record_scope_outcome("undecodable_address");
        return Ok(Some(Vec::new()));
    };

    // Handoffs are only recorded from this slot on. Below it an empty table
    // means the rows were not being written yet, not that nothing happened.
    let Some(indexed_from) = owner_change_indexed_from(auth_db).await? else {
        warn!("Ledger records no owner-change watermark; serving no history for {pubkey}");
        record_scope_outcome("no_watermark");
        return Ok(Some(Vec::new()));
    };

    // One past the cap, so a full page is how an over-long chain announces itself.
    let changes = owner_changes(auth_db, &address, MAX_OWNER_CHANGES + 1).await?;
    if changes.is_empty() {
        // No row is only evidence of no handoff while the address still derives
        // from its owner. An owner it does not derive to was moved without one
        // being recorded, which the detector cannot see if it happened through
        // a CPI, so there is nothing here to vouch for the earlier history.
        if !derives_as_own_ata(data, program_owner, pubkey) {
            warn!("{pubkey} does not derive from its owner and has no recorded handoff");
            record_scope_outcome("unrecorded_handoff");
            return Ok(Some(Vec::new()));
        }
        // A ledger recorded from genesis can vouch for the whole history.
        if indexed_from == 0 {
            record_scope_outcome("never_handed_on");
            return Ok(None);
        }
        record_scope_outcome("watermarked");
        return Ok(Some(vec![(indexed_from, i64::MAX)]));
    }
    // Past the cap what we hold is a suffix of the timeline. Every window inside
    // it is still accounted for by the handoffs on either side of it, so the cap
    // decides how far back the caller reads, not whether they read at all. The
    // alternative would be permanent: these rows are never deleted, so whoever
    // held the account could churn it past the cap and take the history of every
    // later owner with it.
    let chain_is_complete = changes.len() as i64 <= MAX_OWNER_CHANGES;
    if !chain_is_complete {
        warn!(
            "Owner-change chain for {pubkey} exceeds {MAX_OWNER_CHANGES}; \
             serving only what its newest handoffs account for"
        );
    }

    // Every distinct wallet in the chain in one round trip, rather than a query
    // apiece against the pool the auth service shares.
    let candidates: Vec<String> = changes
        .iter()
        .flat_map(|change| [change.prev_owner.clone(), change.new_owner.clone()])
        .collect::<HashSet<String>>()
        .into_iter()
        .collect();
    let owned = owned_wallets(auth_db, user_id, &candidates).await?;

    let current_owner = bs58::encode(&data[OWNER_OFFSET..OWNER_END]).into_string();
    let ranges = slot_ranges_for_owner(&changes, &current_owner, &owned, chain_is_complete)
        .into_iter()
        .filter_map(|(first, last)| {
            (last >= indexed_from).then_some((first.max(indexed_from), last))
        })
        .collect::<Vec<_>>();

    if ranges.is_empty() {
        // Either the chain did not account for the current owner, or none of
        // its windows are this caller's. Both leave them nothing to read, and
        // both are worth seeing before the support ticket arrives.
        warn!("Owner-change chain for {pubkey} leaves this caller no readable window");
        record_scope_outcome("no_window");
    } else if chain_is_complete {
        record_scope_outcome("scoped");
    } else {
        record_scope_outcome("scoped_to_newest_handoffs");
    }

    Ok(Some(ranges))
}

/// Rebuild an address's ownership timeline from its handoffs and keep the
/// windows belonging to the caller.
///
/// A handoff at slot `s` gives everything below `s` to the previous owner and
/// everything above it to the new one. Slot `s` itself goes to neither: history
/// is ordered by `(slot, signature)`, so the order transactions ran inside a
/// slot is not recoverable, and the whole slot is cheaper to drop than to
/// reason about.
///
/// The rows chain: each handoff's new owner is the next one's previous owner,
/// and the last one's new owner holds the account now. A link that doesn't join
/// means a handoff went unrecorded, so no slot after it can be placed and the
/// caller gets nothing.
///
/// `chain_is_complete` says whether `changes` starts at the account's first
/// handoff. When it doesn't, the slots below the oldest row belong to whoever
/// held the account over handoffs nobody read, so only the windows between the
/// rows in hand can be handed out.
fn slot_ranges_for_owner(
    changes: &[OwnerChange],
    current_owner: &str,
    owned: &HashSet<String>,
    chain_is_complete: bool,
) -> Vec<(i64, i64)> {
    let Some(last) = changes.last() else {
        return Vec::new();
    };

    let chain_links = changes
        .windows(2)
        .all(|pair| pair[0].new_owner == pair[1].prev_owner)
        && last.new_owner == current_owner;
    if !chain_links {
        return Vec::new();
    }

    let mut ranges: Vec<(i64, i64)> = Vec::new();

    // Before the first handoff the account belonged to whoever signed it away.
    let head_end = changes[0].slot.saturating_sub(1);
    if chain_is_complete && owned.contains(&changes[0].prev_owner) && head_end >= 0 {
        ranges.push((0, head_end));
    }

    for (index, change) in changes.iter().enumerate() {
        let start = change.slot.saturating_add(1);
        let end = changes
            .get(index + 1)
            .map(|next| next.slot.saturating_sub(1))
            .unwrap_or(i64::MAX);
        // Two handoffs in the same slot leave no window between them.
        if start <= end && owned.contains(&change.new_owner) {
            ranges.push((start, end));
        }
    }

    ranges
}

/// Ownership check for SPL Token and Token-2022 accounts. Both programs share
/// the same base layout, so this checks the `owner` field (bytes 32-63) and,
/// if it doesn't match, the `delegate` field (bytes 76-107) when present. The
/// delegate grants read access for a current-state method, but is skipped for
/// an `OWNER_ONLY_METHODS` one.
///
/// Mints are owned by the same programs but are only 82 bytes, below
/// `TOKEN_ACCOUNT_SIZE`; they are not user accounts and are denied.
async fn check_token_account_ownership(
    data: &[u8],
    method: &str,
    user_id: Uuid,
    auth_db: &PgPool,
) -> AuthDecision {
    if data.len() < TOKEN_ACCOUNT_SIZE {
        return AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body());
    }

    // Extract the `owner` field (bytes 32-63) and encode as base58 to match
    // the format stored in private_channel_auth.verified_wallets.
    let owner = bs58::encode(&data[OWNER_OFFSET..OWNER_END]).into_string();

    match is_wallet_owned_by_user(auth_db, user_id, &owner).await {
        Ok(true) => return AuthDecision::Proceed,
        Ok(false) => {} // not the owner
        Err(_) => return AuthDecision::Reject(StatusCode::INTERNAL_SERVER_ERROR, db_error_body()),
    }

    // Checked after the owner so delegating an account never costs the owner
    // access to its own history.
    if OWNER_ONLY_METHODS.contains(&method) {
        return AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body());
    }

    // Check the `delegate` field if one is set.
    // Bytes 72-75 == [1, 0, 0, 0] means the Option<Pubkey> is Some.
    if data[DELEGATE_OPTION_OFFSET..DELEGATE_OPTION_OFFSET + 4] == [1, 0, 0, 0] {
        let delegate = bs58::encode(&data[DELEGATE_OFFSET..DELEGATE_END]).into_string();

        match is_wallet_owned_by_user(auth_db, user_id, &delegate).await {
            Ok(true) => return AuthDecision::Proceed,
            Ok(false) => {}
            Err(_) => {
                return AuthDecision::Reject(StatusCode::INTERNAL_SERVER_ERROR, db_error_body())
            }
        }
    }

    AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body())
}

/// Ownership check for DvP swap escrow accounts. Grants read access if any
/// verified wallet matches user_a, user_b, or settlement_authority, checked in
/// that order and short-circuiting on the first match.
///
/// Accounts smaller than a full SwapDvp (e.g. the nonce tombstone PDA) are not
/// swaps and are denied.
async fn check_swap_dvp_ownership(data: &[u8], user_id: Uuid, auth_db: &PgPool) -> AuthDecision {
    // Strict, size-checked decode from the vendored client. Anything that isn't
    // exactly a SwapDvp (e.g. the smaller nonce tombstone PDA) is rejected.
    let swap = match SwapDvp::try_from_bytes(data) {
        Ok(s) => s,
        Err(_) => return AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body()),
    };

    for candidate in [swap.user_a, swap.user_b, swap.settlement_authority] {
        match is_wallet_owned_by_user(auth_db, user_id, &candidate.to_string()).await {
            Ok(true) => return AuthDecision::Proceed,
            Ok(false) => {}
            Err(_) => {
                return AuthDecision::Reject(StatusCode::INTERNAL_SERVER_ERROR, db_error_body())
            }
        }
    }

    AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body())
}

// ---------------------------------------------------------------------------
// Base64 helper
// ---------------------------------------------------------------------------

/// Decode a base64-encoded account data string as returned by `getAccountInfo`
/// with `encoding: "base64"`. Returns `None` if the string is invalid base64.
pub fn decode_account_data(encoded: &str) -> Option<Vec<u8>> {
    BASE64.decode(encoded).ok()
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

/// Extracts and verifies a Bearer token from the raw `Authorization` header value.
/// Returns `Some(Claims)` on success, `None` if missing, malformed, or expired.
pub fn verify_bearer(auth_header: Option<&str>, decoding_key: &DecodingKey) -> Option<Claims> {
    let token = auth_header?.strip_prefix("Bearer ")?;
    let mut validation = Validation::default();
    validation.set_issuer(&[JWT_ISSUER]);
    validation.set_audience(&[JWT_AUDIENCE]);
    decode::<Claims>(token, decoding_key, &validation)
        .ok()
        .map(|data| data.claims)
}

// ---------------------------------------------------------------------------
// Error bodies (JSON-RPC style, matching the gateway's existing error format)
//
// Gateway error code registry (server-defined range -32000..-32099):
//   -32001  Unauthorized — missing, invalid, or expired JWT
//   -32002  Forbidden   — account not owned by the calling user
//   -32003  Forbidden   — method requires operator role
//   -32603  Internal    — a DB lookup (ownership or role) failed
//   -32004  Unavailable — ownership check could not reach the read node
// ---------------------------------------------------------------------------

fn unauthorized_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32001, "message": "Unauthorized: valid JWT required" }
        })
        .to_string(),
    )
}

pub fn forbidden_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32002, "message": "Forbidden: account not owned by caller" }
        })
        .to_string(),
    )
}

fn operator_only_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32003, "message": "Forbidden: operator role required" }
        })
        .to_string(),
    )
}

fn missing_pubkey_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32602, "message": "Invalid params: pubkey required as first argument" }
        })
        .to_string(),
    )
}

pub fn db_error_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32603, "message": "Internal error: could not verify account ownership" }
        })
        .to_string(),
    )
}

pub fn role_check_error_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32603, "message": "Internal error: could not verify caller role" }
        })
        .to_string(),
    )
}

/// 503 body for a gated request whose ownership check could not be completed.
/// Distinct from `forbidden_body`: the caller may well own the account, we just
/// could not find out.
pub fn auth_unavailable_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32004, "message": "Service unavailable: could not verify account ownership" }
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;
    use uuid::Uuid;

    const SECRET: &str = "test-secret";

    fn decoding_key() -> DecodingKey {
        DecodingKey::from_secret(SECRET.as_bytes())
    }

    /// Full claims struct including `iss`/`aud` so forged tokens pass gateway validation.
    #[derive(serde::Serialize)]
    struct FullClaims {
        sub: String,
        role: Role,
        exp: usize,
        iss: String,
        aud: String,
    }

    fn forge_token(role: Role, exp_offset_secs: i64) -> String {
        let claims = FullClaims {
            sub: Uuid::new_v4().to_string(),
            role,
            exp: (Utc::now().timestamp() + exp_offset_secs) as usize,
            iss: JWT_ISSUER.to_string(),
            aud: JWT_AUDIENCE.to_string(),
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    /// Build verified `Claims` for the policy tests. `check_request_auth` treats
    /// the role as already confirmed, so tests pass the effective role directly.
    fn claims(role: Role) -> Claims {
        Claims {
            sub: Uuid::new_v4().to_string(),
            role,
            exp: (Utc::now().timestamp() + 3600) as usize,
        }
    }

    /// A lazy pool that never actually connects — safe to use in tests that
    /// return before hitting the DB (e.g. the mint-size rejection path).
    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:password@localhost/fake")
            .unwrap()
    }

    // ── verify_bearer ─────────────────────────────────────────────────────────

    #[test]
    fn verify_bearer_accepts_valid_token() {
        let token = forge_token(Role::Operator, 3600);
        let parsed = verify_bearer(Some(&format!("Bearer {token}")), &decoding_key());
        assert_eq!(parsed.map(|c| c.role), Some(Role::Operator));
    }

    #[test]
    fn verify_bearer_rejects_expired_token() {
        let token = forge_token(Role::Operator, -3600);
        assert!(verify_bearer(Some(&format!("Bearer {token}")), &decoding_key()).is_none());
    }

    #[test]
    fn verify_bearer_rejects_wrong_secret() {
        let token = forge_token(Role::Operator, 3600);
        let wrong_key = DecodingKey::from_secret(b"wrong-secret");
        assert!(verify_bearer(Some(&format!("Bearer {token}")), &wrong_key).is_none());
    }

    #[test]
    fn verify_bearer_rejects_missing_bearer_prefix() {
        let token = forge_token(Role::Operator, 3600);
        assert!(verify_bearer(Some(&token), &decoding_key()).is_none());
    }

    // ── check_request_auth ────────────────────────────────────────────────────
    //
    // check_request_auth trusts claims.role as the caller's effective role;
    // enforce_auth resolves it against the DB first. These tests pass Claims
    // directly via the `claims` helper.

    #[test]
    fn ungated_method_proceeds_without_token() {
        let decision = check_request_auth(None, "getSlot", &json!([]));
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn operator_only_missing_token_is_401() {
        let decision = check_request_auth(None, "getBlock", &json!([]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn operator_only_user_role_is_403() {
        let decision = check_request_auth(Some(&claims(Role::User)), "getBlock", &json!([]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[test]
    fn operator_only_operator_role_proceeds() {
        let decision = check_request_auth(Some(&claims(Role::Operator)), "getBlock", &json!([]));
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn simulate_transaction_operator_only() {
        let decision =
            check_request_auth(Some(&claims(Role::User)), "simulateTransaction", &json!([]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[test]
    fn account_gated_no_token_is_401() {
        let decision = check_request_auth(None, "getAccountInfo", &json!(["SomePubkey"]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn account_gated_operator_role_proceeds() {
        let decision = check_request_auth(
            Some(&claims(Role::Operator)),
            "getAccountInfo",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn account_gated_user_role_returns_needs_account_fetch() {
        let decision = check_request_auth(
            Some(&claims(Role::User)),
            "getAccountInfo",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::NeedsAccountFetch { ref pubkey, .. } if pubkey == "SomePubkey"
        ));
    }

    #[test]
    fn account_gated_missing_pubkey_is_400() {
        let decision = check_request_auth(Some(&claims(Role::User)), "getAccountInfo", &json!([]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::BAD_REQUEST, _)
        ));
    }

    #[test]
    fn get_signatures_for_address_no_token_is_401() {
        let decision = check_request_auth(
            None,
            "getSignaturesForAddress",
            &json!(["So11111111111111111111111111111111111111112"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn get_signatures_for_address_operator_proceeds() {
        let decision = check_request_auth(
            Some(&claims(Role::Operator)),
            "getSignaturesForAddress",
            &json!(["So11111111111111111111111111111111111111112"]),
        );
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn get_signatures_for_address_user_role_returns_needs_account_fetch() {
        let decision = check_request_auth(
            Some(&claims(Role::User)),
            "getSignaturesForAddress",
            &json!(["So11111111111111111111111111111111111111112"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::NeedsAccountFetch { ref pubkey, .. } if pubkey == "So11111111111111111111111111111111111111112"
        ));
    }

    #[test]
    fn get_signatures_for_address_user_missing_pubkey_is_400() {
        let decision = check_request_auth(
            Some(&claims(Role::User)),
            "getSignaturesForAddress",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::BAD_REQUEST, _)
        ));
    }

    #[test]
    fn get_token_account_balance_gated_for_user() {
        let decision = check_request_auth(
            Some(&claims(Role::User)),
            "getTokenAccountBalance",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::NeedsAccountFetch { ref pubkey, .. } if pubkey == "SomePubkey"
        ));
    }

    /// An owner-only method that is not account-gated never reaches the token
    /// account check, leaving the policy silently dead.
    #[test]
    fn owner_only_methods_are_account_gated() {
        for method in OWNER_ONLY_METHODS {
            assert!(
                ACCOUNT_GATED_METHODS.contains(method),
                "{method} is owner-only but not account-gated"
            );
        }
    }

    // ── derives_as_own_ata ────────────────────────────────────────────────────

    /// Token account bytes: mint at 0..32, owner at 32..64.
    fn token_account_bytes(mint: &Pubkey, owner: &Pubkey) -> Vec<u8> {
        let mut data = vec![0u8; TOKEN_ACCOUNT_SIZE];
        data[MINT_OFFSET..MINT_END].copy_from_slice(mint.as_ref());
        data[OWNER_OFFSET..OWNER_END].copy_from_slice(owner.as_ref());
        data
    }

    /// An account still sitting at its own derived address has never had its
    /// owner moved, so an absent handoff row really does mean none happened.
    #[test]
    fn an_untouched_ata_derives_from_its_owner() {
        let mint = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let token_program = Pubkey::from_str(SPL_TOKEN_PROGRAM).unwrap();
        let address = get_associated_token_address_with_program_id(&owner, &mint, &token_program);

        assert!(derives_as_own_ata(
            &token_account_bytes(&mint, &owner),
            SPL_TOKEN_PROGRAM,
            &address.to_string(),
        ));
    }

    /// An owner the address does not derive to was moved there, and a handoff
    /// through a CPI leaves no row behind to say so. The derivation is what
    /// catches it, whatever moved the owner.
    #[test]
    fn an_owner_the_address_does_not_derive_to_is_rejected() {
        let mint = Pubkey::new_unique();
        let original_owner = Pubkey::new_unique();
        let new_owner = Pubkey::new_unique();
        let token_program = Pubkey::from_str(SPL_TOKEN_PROGRAM).unwrap();
        // The address stays put; only the owner field moved.
        let address =
            get_associated_token_address_with_program_id(&original_owner, &mint, &token_program);

        assert!(!derives_as_own_ata(
            &token_account_bytes(&mint, &new_owner),
            SPL_TOKEN_PROGRAM,
            &address.to_string(),
        ));
    }

    // ── slot_ranges_for_owner ─────────────────────────────────────────────────

    fn handoff(slot: i64, prev_owner: &str, new_owner: &str) -> OwnerChange {
        OwnerChange {
            slot,
            prev_owner: prev_owner.to_owned(),
            new_owner: new_owner.to_owned(),
        }
    }

    fn wallets(pubkeys: &[&str]) -> HashSet<String> {
        pubkeys.iter().map(|pubkey| (*pubkey).to_string()).collect()
    }

    /// The new owner gets everything after the handoff and nothing before it,
    /// and the handoff's own slot belongs to neither side.
    #[test]
    fn a_new_owner_reads_only_what_followed_the_handoff() {
        let handoff_slot = 500;
        let ranges = slot_ranges_for_owner(
            &[handoff(handoff_slot, "alice", "bob")],
            "bob",
            &wallets(&["bob"]),
            true,
        );

        assert_eq!(ranges, vec![(handoff_slot + 1, i64::MAX)]);
    }

    /// An account handed away and taken back leaves its owner both of their own
    /// windows, and none of the one in between.
    #[test]
    fn a_returning_owner_reads_both_of_their_windows_but_not_the_middle() {
        let away = 100;
        let back = 200;
        let ranges = slot_ranges_for_owner(
            &[handoff(away, "alice", "bob"), handoff(back, "bob", "alice")],
            "alice",
            &wallets(&["alice"]),
            true,
        );

        assert_eq!(ranges, vec![(0, away - 1), (back + 1, i64::MAX)]);
    }

    /// The caller holds the account now, so the last recorded handoff must name
    /// them. It naming someone else means a later handoff went unrecorded, so
    /// the account reached the caller at an unknown slot and the window after
    /// the last recorded handoff is the missing owner's, not theirs.
    #[test]
    fn a_tail_that_disagrees_with_the_current_owner_serves_nothing() {
        let ranges = slot_ranges_for_owner(
            &[handoff(500, "alice", "bob")],
            "carol",
            &wallets(&["carol", "alice"]),
            true,
        );

        assert!(ranges.is_empty());
    }

    /// Same rule for a gap in the middle: bob handed it on, but the next row
    /// claims it came from someone else.
    #[test]
    fn a_broken_link_serves_nothing() {
        let ranges = slot_ranges_for_owner(
            &[handoff(100, "alice", "bob"), handoff(200, "carol", "dave")],
            "dave",
            &wallets(&["alice", "dave"]),
            true,
        );

        assert!(ranges.is_empty());
    }

    /// A closed account re-created at the same address returns to its derived
    /// owner with no handoff recorded, so the chain ends at someone else. The
    /// slots the interim owner produced are not the re-creator's to read.
    #[test]
    fn an_account_recreated_after_a_handoff_serves_nothing() {
        let ranges = slot_ranges_for_owner(
            &[handoff(100, "alice", "bob")],
            "alice",
            &wallets(&["alice"]),
            true,
        );

        assert!(ranges.is_empty());
    }

    /// Two handoffs in one slot leave no window between them to hand out.
    #[test]
    fn handoffs_in_the_same_slot_leave_no_window_between_them() {
        let slot = 100;
        let ranges = slot_ranges_for_owner(
            &[handoff(slot, "alice", "bob"), handoff(slot, "bob", "carol")],
            "carol",
            &wallets(&["alice", "bob", "carol"]),
            true,
        );

        assert_eq!(ranges, vec![(0, slot - 1), (slot + 1, i64::MAX)]);
    }

    /// Windows belonging to other wallets are not served, even to the account's
    /// current owner.
    #[test]
    fn windows_owned_by_other_wallets_are_left_out() {
        let ranges = slot_ranges_for_owner(
            &[handoff(100, "alice", "bob"), handoff(200, "bob", "carol")],
            "carol",
            &wallets(&["carol"]),
            true,
        );

        assert_eq!(ranges, vec![(201, i64::MAX)]);
    }

    /// Past the read cap the rows in hand are the newest ones, not the whole
    /// timeline. Each still accounts for the window above it, but the slots
    /// below the oldest of them were never read and stay unreadable.
    #[test]
    fn a_truncated_chain_withholds_the_window_before_its_oldest_handoff() {
        let away = 100;
        let back = 200;
        let ranges = slot_ranges_for_owner(
            &[handoff(away, "alice", "bob"), handoff(back, "bob", "alice")],
            "alice",
            &wallets(&["alice"]),
            false,
        );

        assert_eq!(ranges, vec![(back + 1, i64::MAX)]);
    }

    // ── redacts_transaction_errors ────────────────────────────────────────────

    /// `getSignatureStatuses` is ungated, so the anonymous caller (the shape the
    /// balance probe actually takes) must be redacted just like a User.
    #[test]
    fn error_bearing_methods_redact_for_everyone_but_operator() {
        let user = format!("Bearer {}", forge_token(Role::User, 3600));
        let operator = format!("Bearer {}", forge_token(Role::Operator, 3600));

        for method in ["getSignaturesForAddress", "getSignatureStatuses"] {
            assert!(redacts_transaction_errors(None, &decoding_key(), method));
            assert!(redacts_transaction_errors(
                Some(&user),
                &decoding_key(),
                method
            ));
            assert!(!redacts_transaction_errors(
                Some(&operator),
                &decoding_key(),
                method
            ));
        }
    }

    /// An expired operator token is not an operator, so it must not unlock the
    /// raw errors.
    #[test]
    fn expired_operator_token_still_redacts() {
        let expired = format!("Bearer {}", forge_token(Role::Operator, -3600));
        assert!(redacts_transaction_errors(
            Some(&expired),
            &decoding_key(),
            "getSignatureStatuses"
        ));
    }

    #[test]
    fn methods_without_transaction_errors_are_untouched() {
        assert!(!redacts_transaction_errors(
            None,
            &decoding_key(),
            "getAccountInfo"
        ));
    }

    // ── decode_account_data ───────────────────────────────────────────────────

    #[test]
    fn valid_base64_decodes() {
        // "hello" in standard base64
        assert_eq!(decode_account_data("aGVsbG8="), Some(b"hello".to_vec()));
    }

    #[test]
    fn invalid_base64_returns_none() {
        assert_eq!(decode_account_data("not valid base64!!!"), None);
    }

    // ── check_account_data_ownership (no-DB paths) ───────────────────────────

    #[tokio::test]
    async fn spl_token_mint_rejected_by_size() {
        // Mint accounts are 82 bytes — below TOKEN_ACCOUNT_SIZE (165).
        // The function returns Reject before touching the DB.
        let data = vec![0u8; 82];
        let pool = lazy_pool();
        let decision = check_account_data_ownership(
            &data,
            SPL_TOKEN_PROGRAM,
            "SomePubkey",
            "getAccountInfo",
            Uuid::new_v4(),
            &pool,
        )
        .await;
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[tokio::test]
    async fn token_2022_mint_rejected_by_size() {
        let data = vec![0u8; 82];
        let pool = lazy_pool();
        let decision = check_account_data_ownership(
            &data,
            SPL_TOKEN_2022_PROGRAM,
            "SomePubkey",
            "getAccountInfo",
            Uuid::new_v4(),
            &pool,
        )
        .await;
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[tokio::test]
    async fn swap_dvp_undersized_rejected_by_size() {
        // A DvP-owned account smaller than a full SwapDvp (e.g. the nonce
        // tombstone PDA) is not a swap. Rejected before touching the DB.
        let data = vec![0u8; dvp_swap_program_client::verify::SWAP_DVP_ACCOUNT_LEN - 1];
        let pool = lazy_pool();
        let decision = check_account_data_ownership(
            &data,
            DVP_SWAP_PROGRAM.as_str(),
            "SomePubkey",
            "getAccountInfo",
            Uuid::new_v4(),
            &pool,
        )
        .await;
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }
}
