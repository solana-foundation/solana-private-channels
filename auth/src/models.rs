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

#[derive(Serialize)]
pub struct ChallengeResponse {
    /// The exact message the client must sign with their wallet.
    pub message: String,
    pub nonce: Uuid,
    pub expires_at: DateTime<Utc>,
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
