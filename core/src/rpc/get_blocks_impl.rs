use crate::rpc::{
    constants::MAX_SLOT_RANGE,
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcContextConfig;

pub async fn get_blocks_impl(
    read_deps: &ReadDeps,
    start_slot: u64,
    end_slot: Option<u64>,
    _config: Option<RpcContextConfig>,
) -> RpcResult<Vec<u64>> {
    let effective_end = match end_slot {
        Some(end) => {
            if end < start_slot {
                return Err(custom_error(
                    INVALID_PARAMS_CODE,
                    "end_slot must be >= start_slot",
                ));
            }
            end
        }
        None => start_slot.saturating_add(MAX_SLOT_RANGE),
    };

    let range = effective_end - start_slot;
    if range > MAX_SLOT_RANGE {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!("Slot range too large: {} (max: {})", range, MAX_SLOT_RANGE),
        ));
    }

    // Bound to a name so the permit is held until the scan finishes.
    let _permit = read_deps.block_list_permit(start_slot, range).await?;

    read_deps
        .accounts_db
        .get_blocks(start_slot, Some(effective_end))
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get blocks: {}", e),
            )
        })
}
