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
    // The context is a slot, as Solana reports it; the deadline below is a
    // height. They are different counters now, so both are read.
    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);
    let block_height = read_deps
        .accounts_db
        .get_block_height()
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get block height: {}", e),
            )
        })?
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

    // The window holds max_blockhashes entries and evicts one per produced block,
    // so a hash minted here is live for that many heights and the last of them is
    // the deadline. Inclusive, as Solana's is.
    let last_valid_block_height =
        block_height.saturating_add(read_deps.max_blockhashes.saturating_sub(1));

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: RpcBlockhash {
            blockhash: blockhash.to_string(),
            last_valid_block_height,
        },
    })
}
