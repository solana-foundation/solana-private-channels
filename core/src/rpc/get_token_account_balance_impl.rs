use crate::rpc::{
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_account_decoder_client_types::token::{real_number_string_trimmed, UiTokenAmount};
use solana_rpc_client_types::config::RpcContextConfig;
use solana_rpc_client_types::response::{Response, RpcResponseContext};
use solana_sdk::{account::ReadableAccount, pubkey::Pubkey};
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::{Account as TokenAccount, Mint};
use std::str::FromStr;

pub async fn get_token_account_balance_impl(
    read_deps: &ReadDeps,
    pubkey: String,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Response<UiTokenAmount>> {
    let pubkey = Pubkey::from_str(&pubkey)
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid pubkey: {}", e)))?;

    // A missing account is the caller's mistake; a store that cannot answer is
    // ours, and must not be coded so the caller gives up instead of retrying.
    let account = read_deps
        .accounts_db
        .get_account_shared_data(&pubkey)
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| custom_error(INVALID_PARAMS_CODE, "Account not found"))?;

    if *account.owner() != spl_token::id() {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            "Account is not a token account",
        ));
    }

    let data = account.data();
    let token_account = TokenAccount::unpack(data).map_err(|e| {
        custom_error(
            INVALID_PARAMS_CODE,
            format!("Invalid token account data: {}", e),
        )
    })?;

    let amount = token_account.amount;
    let mint_pubkey = token_account.mint;

    // Fetch actual decimals from the mint account
    let mint_account = read_deps
        .accounts_db
        .get_account_shared_data(&mint_pubkey)
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| custom_error(INVALID_PARAMS_CODE, "Mint account not found"))?;

    let mint = Mint::unpack(mint_account.data()).map_err(|e| {
        custom_error(
            INVALID_PARAMS_CODE,
            format!("Invalid mint account data: {}", e),
        )
    })?;
    let decimals = mint.decimals;

    // Use f64 only for the optional numeric field (lossy for large amounts, matches Solana RPC)
    let ui_amount = amount as f64 / 10_f64.powi(decimals as i32);

    // Use Solana's canonical formatter to avoid f64 precision loss in the string field
    let ui_amount_string = real_number_string_trimmed(amount, decimals);

    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: UiTokenAmount {
            ui_amount: Some(ui_amount),
            ui_amount_string,
            amount: amount.to_string(),
            decimals,
        },
    })
}
