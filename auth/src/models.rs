use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::Type, Serialize, Deserialize, PartialEq)]
#[sqlx(type_name = "user_role", rename_all = "lowercase")]
// serde rename must match the gateway's Role enum (also lowercase): the JWT role
// claim is serialized here and deserialized by the gateway. Without this the
// claim would be PascalCase ("User"/"Operator") and the gateway rejects every
// token with 401 once enforcement is on.
#[serde(rename_all = "lowercase")]
pub enum Role {
    Operator,
    User,
}

impl Role {
    /// The wire and database spelling. Must stay lowercase to match the postgres
    /// `user_role` enum and the gateway's role claim.
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Operator => "operator",
            Role::User => "user",
        }
    }
}

// DB rows

#[derive(Debug, Clone, Serialize)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub role: Role,
    pub created_at: DateTime<Utc>,
}

pub struct VerifiedWallet {
    pub pubkey: String,
    pub created_at: DateTime<Utc>,
}

/// A one-time challenge issued to a user for wallet ownership verification.
/// Bound to a specific user and nonce so it cannot be replayed across accounts.
pub struct Challenge {
    pub nonce: Uuid,
    pub expires_at: DateTime<Utc>,
}

// Request/response types

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Deserialize)]
pub struct ChallengeRequest {
    /// The wallet the caller wants to link. Bound into the signed message.
    pub pubkey: String,
}

#[derive(Serialize)]
pub struct ChallengeResponse {
    /// The exact message the client must sign with their wallet.
    pub message: String,
    pub nonce: Uuid,
    pub expires_at: DateTime<Utc>,
}

/// Builds the message a user signs to prove wallet ownership.
/// The challenge and verify endpoints both call this so their messages always match.
/// The account name and wallet are spelled out so a signer can tell they are
/// linking their own wallet to their own account, not someone else's.
/// The id is the unforgeable half: a lookalike username can be registered, a uuid cannot,
/// so a client can check the id against the session it is connected to.
pub fn wallet_verification_message(
    username: &str,
    user_id: Uuid,
    pubkey: &str,
    nonce: Uuid,
    expires_at: DateTime<Utc>,
) -> String {
    format!(
        "PrivateChannel wallet verification\n\n\
         Account: {username} (id: {user_id})\n\
         Wallet: {pubkey}\n\n\
         Only sign this if \"{username}\" is YOUR PrivateChannel account.\n\n\
         nonce: {nonce}\n\
         expires: {expires}",
        expires = expires_at.timestamp(),
    )
}

#[derive(Deserialize)]
pub struct VerifyWalletRequest {
    pub pubkey: String,
    pub nonce: Uuid,
    /// Base58-encoded Ed25519 signature of the challenge message.
    pub signature: String,
}

#[derive(Serialize)]
pub struct WalletResponse {
    pub pubkey: String,
    pub created_at: DateTime<Utc>,
}
