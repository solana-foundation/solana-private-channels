use crate::rpc::ReadDeps;
use jsonrpsee::core::RpcResult;
use solana_fee_calculator::FeeCalculator;
use solana_rpc_client_types::response::{Response, RpcBlockhashFeeCalculator, RpcResponseContext};
use solana_sdk::hash::Hash;

pub async fn get_recent_blockhash_impl(
    read_deps: &ReadDeps,
) -> RpcResult<Response<RpcBlockhashFeeCalculator>> {
    let blockhash = read_deps
        .accounts_db
        .get_latest_blockhash()
        .await
        .unwrap_or_else(|_| Hash::default());
    let slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .ok()
        .flatten()
        .unwrap_or(0);

    Ok(Response {
        context: RpcResponseContext::new(slot),
        value: RpcBlockhashFeeCalculator {
            blockhash: blockhash.to_string(),
            fee_calculator: FeeCalculator {
                lamports_per_signature: 5000, // Standard fee
            },
        },
    })
}
