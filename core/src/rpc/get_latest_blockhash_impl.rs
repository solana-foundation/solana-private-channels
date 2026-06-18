use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;
use solana_rpc_client_types::response::{Response, RpcBlockhash, RpcResponseContext};

pub async fn get_latest_blockhash_impl(
    read_deps: &ReadDeps,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Response<RpcBlockhash>> {
    // Get the latest slot and blockhash from the database
    let slot = read_deps
        .accounts_db
        .get_latest_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);
    let blockhash = read_deps
        .accounts_db
        .get_latest_blockhash()
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get blockhash: {}", e),
            )
        })?;

    // The dedup window holds max_blockhashes entries (block height == slot here),
    // so a hash settled at this slot is evicted once the tip reaches
    // slot + max_blockhashes.
    let last_valid_block_height = slot.saturating_add(read_deps.max_blockhashes);

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: RpcBlockhash {
            blockhash: blockhash.to_string(),
            last_valid_block_height,
        },
    })
}
