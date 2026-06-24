use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hyper::body::Bytes;
use hyper::StatusCode;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::is_wallet_owned_by_user;

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
/// account fetch returns `None` and the User is rejected with 403 — even for
/// signatures from when they owned the account. We accept this: closing a
/// TokenAccount is rare in our context (no rent to reclaim for users), and the
/// alternatives (snapshotting ownership at ingest, or deriving ATAs from a
/// mint param) add schema or API complexity that isn't justified today.
const ACCOUNT_GATED_METHODS: &[&str] = &[
    "getAccountInfo",
    "getTokenAccountBalance",
    "getSignaturesForAddress",
];

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
// settlement_authority, by inspecting raw bytes (no full deserialization).
//
// SWAP_DVP_SIZE and SWAP_DVP_OWNER_FIELDS mirror the SwapDvp layout in
// dvp-swap-program/program/src/state/swap_dvp.rs. They are not derived from it,
// so if a field is added or reordered there, update them here or these checks
// will read the wrong bytes.
// ---------------------------------------------------------------------------

/// DvP swap escrow program ID.
///
/// WARNING: the DvP swap program is NOT deployed yet. This value is the local
/// `declare_id!` from dvp-swap-program/program/src/lib.rs and is a placeholder.
/// Update it with the real program ID once the program is deployed, otherwise
/// no live DvP account will match and these checks will never fire.
const DVP_SWAP_PROGRAM: &str = "DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC";

/// Serialized size of a SwapDvp account (SwapDvp::LEN). Smaller DvP-owned
/// accounts (e.g. the nonce tombstone PDA) are not swaps and are denied.
const SWAP_DVP_SIZE: usize = 394;

/// Byte ranges of the SwapDvp fields whose pubkey grants read access.
/// Layout: bump(1), user_a, user_b, mint_a, mint_b, settlement_authority, ...
const SWAP_DVP_OWNER_FIELDS: [(usize, usize); 3] = [
    (1, 33),    // user_a (seller)
    (33, 65),   // user_b (buyer)
    (129, 161), // settlement_authority
];

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

/// Checks whether the request is authorised to call `method` with `params`.
///
/// - If the method is not gated, returns `Proceed` immediately.
/// - If the JWT is missing or invalid, returns `Reject(401)`.
/// - If the caller is an Operator, returns `Proceed` (unrestricted access).
/// - If the caller is a User, returns `NeedsAccountFetch` so the caller can
///   fetch the raw account data and run the ownership check in
///   `check_account_data_ownership`.
///
/// No DB lookup is performed here. Users almost always query ATAs or other
/// PDAs rather than their wallet pubkeys directly, so checking
/// `verified_wallets` up-front would almost always be a wasted round-trip.
/// `check_account_data_ownership` handles both token accounts (via owner/
/// delegate byte inspection) and direct wallet pubkeys (fallback pubkey check).
pub fn check_request_auth(
    auth_header: Option<&str>,
    decoding_key: &DecodingKey,
    method: &str,
    params: &Value,
) -> AuthDecision {
    let is_operator_only = OPERATOR_ONLY_METHODS.contains(&method);
    let is_account_gated = ACCOUNT_GATED_METHODS.contains(&method);

    if !is_operator_only && !is_account_gated {
        return AuthDecision::Proceed;
    }

    let claims = match auth_header.and_then(|h| verify_bearer(h, decoding_key)) {
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
pub async fn check_account_data_ownership(
    data: &[u8],
    program_owner: &str,
    pubkey: &str,
    user_id: Uuid,
    auth_db: &PgPool,
) -> AuthDecision {
    match program_owner {
        SPL_TOKEN_PROGRAM | SPL_TOKEN_2022_PROGRAM => {
            check_token_account_ownership(data, user_id, auth_db).await
        }
        DVP_SWAP_PROGRAM => check_swap_dvp_ownership(data, user_id, auth_db).await,
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

/// Ownership check for SPL Token and Token-2022 accounts. Both programs share
/// the same base layout, so this checks the `owner` field (bytes 32-63) and,
/// if it doesn't match, the `delegate` field (bytes 76-107) when present — a
/// delegate has spend authority and counts as ownership for read access.
///
/// Mints are owned by the same programs but are only 82 bytes, below
/// `TOKEN_ACCOUNT_SIZE`; they are not user accounts and are denied.
async fn check_token_account_ownership(
    data: &[u8],
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
        Ok(false) => {} // fall through to delegate check
        Err(_) => return AuthDecision::Reject(StatusCode::INTERNAL_SERVER_ERROR, db_error_body()),
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
    if data.len() < SWAP_DVP_SIZE {
        return AuthDecision::Reject(StatusCode::FORBIDDEN, forbidden_body());
    }

    for (start, end) in SWAP_DVP_OWNER_FIELDS {
        let candidate = bs58::encode(&data[start..end]).into_string();
        match is_wallet_owned_by_user(auth_db, user_id, &candidate).await {
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
fn verify_bearer(auth_header: &str, decoding_key: &DecodingKey) -> Option<Claims> {
    let token = auth_header.strip_prefix("Bearer ")?;
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

fn db_error_body() -> Bytes {
    Bytes::from(
        serde_json::json!({
            "error": { "code": -32603, "message": "Internal error: could not verify account ownership" }
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

    /// A lazy pool that never actually connects — safe to use in tests that
    /// return before hitting the DB (e.g. the mint-size rejection path).
    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://postgres:password@localhost/fake")
            .unwrap()
    }

    // ── check_request_auth ────────────────────────────────────────────────────

    #[test]
    fn ungated_method_proceeds_without_token() {
        let decision = check_request_auth(None, &decoding_key(), "getSlot", &json!([]));
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn operator_only_missing_token_is_401() {
        let decision = check_request_auth(None, &decoding_key(), "getBlock", &json!([]));
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn operator_only_expired_token_is_401() {
        let token = forge_token(Role::Operator, -3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getBlock",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn operator_only_wrong_secret_is_401() {
        let token = forge_token(Role::Operator, 3600);
        let wrong_key = DecodingKey::from_secret(b"wrong-secret");
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &wrong_key,
            "getBlock",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn operator_only_user_role_is_403() {
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getBlock",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[test]
    fn operator_only_operator_role_proceeds() {
        let token = forge_token(Role::Operator, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getBlock",
            &json!([]),
        );
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn simulate_transaction_operator_only() {
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "simulateTransaction",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::FORBIDDEN, _)
        ));
    }

    #[test]
    fn account_gated_no_token_is_401() {
        let decision = check_request_auth(
            None,
            &decoding_key(),
            "getAccountInfo",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::UNAUTHORIZED, _)
        ));
    }

    #[test]
    fn account_gated_operator_role_proceeds() {
        let token = forge_token(Role::Operator, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getAccountInfo",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn account_gated_user_role_returns_needs_account_fetch() {
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
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
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getAccountInfo",
            &json!([]),
        );
        assert!(matches!(
            decision,
            AuthDecision::Reject(StatusCode::BAD_REQUEST, _)
        ));
    }

    #[test]
    fn get_signatures_for_address_no_token_is_401() {
        let decision = check_request_auth(
            None,
            &decoding_key(),
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
        let token = forge_token(Role::Operator, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getSignaturesForAddress",
            &json!(["So11111111111111111111111111111111111111112"]),
        );
        assert!(matches!(decision, AuthDecision::Proceed));
    }

    #[test]
    fn get_signatures_for_address_user_role_returns_needs_account_fetch() {
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
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
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
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
        let token = forge_token(Role::User, 3600);
        let decision = check_request_auth(
            Some(&format!("Bearer {token}")),
            &decoding_key(),
            "getTokenAccountBalance",
            &json!(["SomePubkey"]),
        );
        assert!(matches!(
            decision,
            AuthDecision::NeedsAccountFetch { ref pubkey, .. } if pubkey == "SomePubkey"
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
        // A DvP-owned account smaller than SWAP_DVP_SIZE (e.g. the nonce
        // tombstone PDA) is not a swap. Rejected before touching the DB.
        let data = vec![0u8; SWAP_DVP_SIZE - 1];
        let pool = lazy_pool();
        let decision = check_account_data_ownership(
            &data,
            DVP_SWAP_PROGRAM,
            "SomePubkey",
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
