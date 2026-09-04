use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;

/// The count of blocks produced, which is what a client polls against
/// `lastValidBlockHeight`. It is not the slot: idle ticks advance the slot
/// without producing a block.
pub async fn get_block_height_impl(
    read_deps: &ReadDeps,
    _config: Option<RpcContextConfig>,
) -> RpcResult<u64> {
    read_deps
        .accounts_db
        .get_block_height()
        .await
        .map(|opt| opt.unwrap_or(0))
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get block height: {}", e),
            )
        })
}
