use crate::rpc::{
    constants::MAX_SLOT_RANGE,
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;

pub async fn get_blocks_with_limit_impl(
    read_deps: &ReadDeps,
    start_slot: u64,
    limit: u64,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Vec<u64>> {
    if limit > MAX_SLOT_RANGE {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!("Limit too large: {} (max: {})", limit, MAX_SLOT_RANGE),
        ));
    }

    // Bound to a name so the permit is held until the scan finishes.
    let _permit = read_deps.block_list_permit(limit)?;

    read_deps
        .accounts_db
        .get_blocks_with_limit(start_slot, limit)
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get blocks with limit: {}", e),
            )
        })
}
