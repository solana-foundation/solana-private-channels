use crate::rpc::{
    error::{custom_error, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_fee_calculator::FeeCalculator;
use solana_rpc_client_types::response::{Response, RpcBlockhashFeeCalculator, RpcResponseContext};

pub async fn get_recent_blockhash_impl(
    read_deps: &ReadDeps,
) -> RpcResult<Response<RpcBlockhashFeeCalculator>> {
    // Hash and slot come from one read, and a failed read is an error, never a zero hash.
    let snapshot = read_deps
        .accounts_db
        .get_blockhash_snapshot()
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to get blockhash: {}", e),
            )
        })?;

    Ok(Response {
        context: RpcResponseContext::new(snapshot.slot.unwrap_or(0)),
        value: RpcBlockhashFeeCalculator {
            blockhash: snapshot.blockhash.to_string(),
            fee_calculator: FeeCalculator {
                lamports_per_signature: 5000, // Standard fee
            },
        },
    })
}
