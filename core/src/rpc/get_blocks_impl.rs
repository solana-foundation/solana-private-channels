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

    // With no end only blocks up to the newest one can match, so a call near the tip stays small.
    let span = match end_slot {
        Some(_) => range,
        None => read_deps
            .accounts_db
            .newest_block_slot()
            .await
            .map_err(|e| {
                custom_error(
                    JSON_RPC_SERVER_ERROR,
                    format!("Failed to get blocks: {}", e),
                )
            })?
            .map_or(0, |newest| {
                newest.min(effective_end).saturating_sub(start_slot)
            }),
    };

    // Bound to a name so the permit is held until the scan finishes.
    let _permit = read_deps.block_list_permit(span)?;

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
