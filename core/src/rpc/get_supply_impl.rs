use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcSupplyConfig;
use solana_rpc_client_types::response::{Response, RpcResponseContext, RpcSupply};

pub async fn get_supply_impl(
    read_deps: &ReadDeps,
    _config: Option<RpcSupplyConfig>,
) -> RpcResult<Response<RpcSupply>> {
    // Get the current slot for context
    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get latest slot: {}", e),
            )
        })?
        .unwrap_or(0);

    // PrivateChannel has no native token supply, so all values are 0
    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: RpcSupply {
            total: 0,
            circulating: 0,
            non_circulating: 0,
            non_circulating_accounts: vec![],
        },
    })
}
