use crate::rpc::{
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;
use solana_rpc_client_types::response::{Response, RpcResponseContext};
use solana_sdk::hash::Hash;
use std::str::FromStr;

pub async fn is_blockhash_valid_impl(
    read_deps: &ReadDeps,
    blockhash: String,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Response<bool>> {
    // Get the current slot
    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);

    // Parse the provided blockhash
    let provided_hash = Hash::from_str(&blockhash)
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid blockhash: {}", e)))?;

    // Check if the blockhash is in the live blockhash window.
    // Validates against the full window maintained by the Dedup stage,
    // not just the single latest blockhash.
    //
    // Edge cases:
    // - Empty window: iter().any() returns false (all blockhashes rejected at startup)
    // - Lock poisoning: handled with map_err instead of unwrap()
    let live_blockhashes = read_deps
        .live_blockhashes
        .read()
        .map_err(|e| custom_error(-32603, format!("Failed to acquire blockhash lock: {}", e)))?;

    let is_valid = live_blockhashes.iter().any(|h| h == &provided_hash);

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: is_valid,
    })
}
