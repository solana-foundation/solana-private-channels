use axum::{extract::State, Json};

use crate::{
    db,
    error::{AppError, AppResult},
    models::{LoginRequest, LoginResponse},
    validation::{PASSWORD_MAX_LEN, USERNAME_MAX_LEN},
    AppState,
};

/// A valid Argon2id hash used as a timing sink when the requested username does not exist.
///
/// Without this, an attacker could distinguish "user not found" (fast, no Argon2)
/// from "wrong password" (slow, Argon2 runs) by measuring response time, leaking
/// whether a username is registered.
///
/// Parameters match the argon2 crate defaults: m=19456, t=2, p=1.
/// The plaintext this was derived from is irrelevant and never used for auth.
const DUMMY_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$MTIxYmVlYzFjMGZlZTA4Yjg3MWM3ZmFjYWVjNmE3NzQ$9Z/sQzp5bnPoRwHT/eCB0oYEgk+mm/zhbgBjq8F1CLY";

pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> AppResult<Json<LoginResponse>> {
    // Cap the inputs before the lookup or any hashing. Maximums only: enforcing
    // register's minimums would lock out accounts created under an older policy.
    if req.username.len() > USERNAME_MAX_LEN || req.password.chars().count() > PASSWORD_MAX_LEN {
        return Err(AppError::Unauthorized);
    }

    let user = db::find_user_by_username(&state.pool, &req.username).await;
    state.pool_status.observe_app(&user);
    let user = user?;

    let Some(user) = user else {
        // User not found. Run Argon2 against the dummy hash to match the cost of
        // the "wrong password" path and prevent username enumeration via timing.
        // Same worker as the real path, so the permit wait can't leak either.
        state
            .passwords
            .verify(req.password, DUMMY_HASH.to_string())
            .await?;
        return Err(charge_failed_attempt(&state, &req.username));
    };

    if !state
        .passwords
        .verify(req.password, user.password_hash)
        .await?
    {
        return Err(charge_failed_attempt(&state, &req.username));
    }

    let token = state
        .jwt
        .sign(user.id, user.role)
        .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    Ok(Json(LoginResponse { token }))
}

/// Spends one attempt against the username and picks the status for a failure.
///
/// Charged on the outcome, not before it. Charging up front let an
/// unauthenticated caller drain a victim's budget just by naming them, holding
/// that account at 429 without ever knowing its password. A correct password now
/// succeeds regardless of the bucket, so the limiter cannot deny an account.
///
/// Hashing has already run by this point, so this bounds failed attempts rather
/// than the work an attacker can cause. Per-IP throttling and the Argon2
/// concurrency cap are what bound the work.
fn charge_failed_attempt(state: &AppState, username: &str) -> AppError {
    if state
        .throttle
        .per_username
        .check_key(&username.to_string())
        .is_err()
    {
        return AppError::TooManyRequests;
    }
    AppError::Unauthorized
}

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::PasswordHash;

    /// A malformed dummy hash fails to parse, which makes the unknown-user path
    /// return without doing Argon2 work and reopens the timing oracle.
    #[test]
    fn dummy_hash_is_valid() {
        assert!(PasswordHash::new(DUMMY_HASH).is_ok());
    }
}
