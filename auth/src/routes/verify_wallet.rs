use axum::{extract::State, Json};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::str::FromStr;
use tracing::{info, warn};

use crate::{
    db,
    error::{AppError, AppResult},
    jwt::Claims,
    models::{wallet_verification_message, VerifyWalletRequest, WalletResponse},
    AppState,
};

pub async fn verify_wallet(
    State(state): State<AppState>,
    claims: Claims,
    Json(req): Json<VerifyWalletRequest>,
) -> AppResult<Json<WalletResponse>> {
    // Look up the account name before consuming the challenge, so a transient lookup
    // failure cannot burn an otherwise valid one-time challenge.
    let username_result = db::find_username_by_id(&state.pool, claims.sub).await;
    state.pool_status.observe_app(&username_result);
    let username = username_result?.ok_or(AppError::Unauthorized)?;

    // Parse both inputs before consuming the challenge, so malformed input cannot
    // burn an otherwise valid one-time challenge.
    let pubkey =
        Pubkey::from_str(&req.pubkey).map_err(|_| AppError::BadRequest("invalid pubkey".into()))?;

    let signature = Signature::from_str(&req.signature)
        .map_err(|_| AppError::BadRequest("invalid signature".into()))?;

    // Consume the challenge and link the wallet in one transaction. Both statements land
    // or neither does, so a failed insert cannot leave the nonce spent with no wallet
    // attached, which would force the user to sign a fresh challenge.
    let tx_result = state.pool.begin().await;
    state.pool_status.observe_sqlx(&tx_result);
    let mut tx = tx_result?;

    // The atomic UPDATE means a concurrent request for the same nonce blocks here and
    // then finds it used, so it still cannot be replayed.
    let challenge_result = db::consume_challenge(&mut *tx, claims.sub, req.nonce).await;
    state.pool_status.observe_app(&challenge_result);
    let challenge =
        challenge_result?.ok_or(AppError::BadRequest("invalid or expired challenge".into()))?;

    // Rebuild the exact message the client was asked to sign.
    let message = wallet_verification_message(
        &username,
        claims.sub,
        &req.pubkey,
        challenge.nonce,
        challenge.expires_at,
    );

    if !signature.verify(pubkey.as_ref(), message.as_bytes()) {
        // Commit so a bad signature still burns the challenge: one attempt per nonce.
        let committed = tx.commit().await;
        state.pool_status.observe_sqlx(&committed);
        committed?;
        warn!(user_id = %claims.sub, pubkey = %req.pubkey, "wallet verification failed: invalid signature");
        return Err(AppError::Unauthorized);
    }

    // On error the transaction is dropped and rolls back, leaving the nonce usable.
    let insert_result = db::insert_verified_wallet(&mut *tx, claims.sub, &req.pubkey).await;
    state.pool_status.observe_app(&insert_result);
    let wallet = insert_result.map_err(|e| match e {
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
