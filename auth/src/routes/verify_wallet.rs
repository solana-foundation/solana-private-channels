use axum::{extract::State, Json};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::str::FromStr;
use tracing::{info, warn};

use crate::{
    db,
    error::{AppError, AppResult},
    jwt::Claims,
    models::{VerifyWalletRequest, WalletResponse},
    AppState,
};

pub async fn verify_wallet(
    State(state): State<AppState>,
    claims: Claims,
    Json(req): Json<VerifyWalletRequest>,
) -> AppResult<Json<WalletResponse>> {
    // Consuming the challenge and storing the wallet go in one transaction.
    // Committed separately, anything that ended the request in between left the
    // nonce spent with no wallet recorded, and the retry could only fail: the
    // request timeout makes that deterministic, and a client hangup always could.
    let begun = state.pool.begin().await;
    state.pool_status.observe_sqlx(&begun);
    let mut tx = begun?;

    // The conditional UPDATE is still what enforces single use. It now holds the
    // row until commit, so a concurrent request for the same nonce waits and
    // then finds it spent.
    let r = db::consume_challenge(&mut *tx, claims.sub, req.nonce).await;
    state.pool_status.observe_app(&r);
    let challenge = r?.ok_or(AppError::BadRequest("invalid or expired challenge".into()))?;

    // Reconstruct the exact message the client was asked to sign.
    // Must match the format returned by /auth/challenge-wallet.
    let message = format!(
        "PrivateChannel wallet verification\nuser: {}\nnonce: {}\nexpires: {}",
        claims.sub,
        challenge.nonce,
        challenge.expires_at.timestamp()
    );

    let pubkey =
        Pubkey::from_str(&req.pubkey).map_err(|_| AppError::BadRequest("invalid pubkey".into()))?;

    let signature = Signature::from_str(&req.signature)
        .map_err(|_| AppError::BadRequest("invalid signature".into()))?;

    if !signature.verify(pubkey.as_ref(), message.as_bytes()) {
        warn!(user_id = %claims.sub, pubkey = %req.pubkey, "wallet verification failed: invalid signature");
        return Err(AppError::Unauthorized);
    }

    let raw = db::insert_verified_wallet(&mut *tx, claims.sub, &req.pubkey).await;
    state.pool_status.observe_app(&raw);
    let wallet = raw.map_err(|e| match e {
        // Unique constraint on (user_id, pubkey) — wallet already verified.
        AppError::Db(sqlx::Error::Database(ref db_err))
            if db_err.constraint() == Some("verified_wallets_user_id_pubkey_key") =>
        {
            AppError::Conflict("wallet already verified".into())
        }
        other => other,
    })?;

    let committed = tx.commit().await;
    state.pool_status.observe_sqlx(&committed);
    committed?;

    info!(user_id = %claims.sub, pubkey = %wallet.pubkey, "wallet verified");

    Ok(Json(WalletResponse {
        pubkey: wallet.pubkey,
        created_at: wallet.created_at,
    }))
}
