use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;

pub async fn get_block_time_impl(read_deps: &ReadDeps, slot: u64) -> RpcResult<Option<i64>> {
    // A lookup failure is an error, never a "not found".
    read_deps
        .accounts_db
        .get_block_time(slot)
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get block time: {}", e),
            )
        })
}
