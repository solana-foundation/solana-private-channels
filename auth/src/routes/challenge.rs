use axum::{extract::State, Json};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use uuid::Uuid;

use crate::{
    db,
    error::{AppError, AppResult},
    jwt::Claims,
    models::{wallet_verification_message, ChallengeRequest, ChallengeResponse},
    AppState,
};

pub async fn challenge(
    State(state): State<AppState>,
    claims: Claims,
    Json(request): Json<ChallengeRequest>,
) -> AppResult<Json<ChallengeResponse>> {
    // Reject a malformed pubkey before issuing a challenge for it.
    Pubkey::from_str(&request.pubkey).map_err(|_| AppError::BadRequest("invalid pubkey".into()))?;

    // Name the account in the signed message so the signer knows what they are linking.
    let username_result = db::find_username_by_id(&state.pool, claims.sub).await;
    state.pool_status.observe_app(&username_result);
    let username = username_result?.ok_or(AppError::Unauthorized)?;

    let nonce = Uuid::new_v4();
    let challenge_result = db::insert_challenge(&state.pool, claims.sub, nonce).await;
    state.pool_status.observe_app(&challenge_result);
    let challenge = challenge_result?;

    // The client must sign this exact string with the wallet's private key.
    let message = wallet_verification_message(
        &username,
        claims.sub,
        &request.pubkey,
        challenge.nonce,
        challenge.expires_at,
    );

    Ok(Json(ChallengeResponse {
        message,
        nonce: challenge.nonce,
        expires_at: challenge.expires_at,
    }))
}
