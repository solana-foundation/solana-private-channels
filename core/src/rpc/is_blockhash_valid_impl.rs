use crate::rpc::{
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    WriteDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;
use solana_rpc_client_types::response::{Response, RpcResponseContext};
use solana_sdk::hash::Hash;
use std::str::FromStr;
use std::sync::atomic::Ordering;

pub async fn is_blockhash_valid_impl(
    write_deps: &WriteDeps,
    blockhash: String,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Response<bool>> {
    // Public traffic reaches the writer here, so nothing below touches the DB or awaits.
    let slot = write_deps.settled_slot.load(Ordering::Acquire);

    // Parse the provided blockhash
    let provided_hash = Hash::from_str(&blockhash)
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid blockhash: {}", e)))?;

    // Ingested first: the scan below then holds every hash it counts, while announced,
    // read last, covers any hash the caller could have seen. So ingested >= announced
    // means a missing hash is not a lagging one.
    let progress = &write_deps.blockhash_progress;
    let ingested = progress.ingested.load(Ordering::Acquire);

    // Check if the blockhash is in the live blockhash window.
    // Validates against the full window maintained by the Dedup stage,
    // not just the single latest blockhash.
    //
    // Edge cases:
    // - Empty window: iter().any() returns false (all blockhashes rejected at startup)
    // - Lock poisoning: handled with map_err instead of unwrap()
    let live_blockhashes = write_deps
        .live_blockhashes
        .read()
        .map_err(|e| custom_error(-32603, format!("Failed to acquire blockhash lock: {}", e)))?;

    let is_valid = live_blockhashes.iter().any(|h| h == &provided_hash);
    drop(live_blockhashes);

    if !is_valid && ingested < progress.announced.load(Ordering::SeqCst) {
        return Err(custom_error(
            JSON_RPC_SERVER_ERROR,
            "Blockhash window is catching up, retry",
        ));
    }

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: is_valid,
    })
}
