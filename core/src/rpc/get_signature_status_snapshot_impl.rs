use crate::rpc::{
    constants::MAX_SIGNATURES,
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    get_signature_statuses_impl::stored_status,
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::TransactionStatus;
use std::str::FromStr;

/// Statuses, block height and ledger floor taken from one database snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcStatusSnapshot {
    pub block_height: u64,
    pub first_available_block: u64,
    pub value: Vec<Option<TransactionStatus>>,
}

/// One consistent read for proving a transaction absent; separate calls can mix states.
pub async fn get_signature_status_snapshot_impl(
    read_deps: &ReadDeps,
    signatures: Vec<String>,
) -> RpcResult<RpcStatusSnapshot> {
    if signatures.len() > MAX_SIGNATURES {
        return Err(custom_error(
            INVALID_PARAMS_CODE,
            format!(
                "Too many signatures: {} (max: {})",
                signatures.len(),
                MAX_SIGNATURES
            ),
        ));
    }

    // A malformed signature fails the call: as a null it would read as proof of absence.
    let signatures = signatures
        .iter()
        .map(|sig| Signature::from_str(sig))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| custom_error(INVALID_PARAMS_CODE, format!("Invalid signature: {}", e)))?;

    let snapshot = read_deps
        .accounts_db
        .get_signature_status_snapshot(&signatures)
        .await
        .map_err(|e| {
            custom_error(
                JSON_RPC_SERVER_ERROR,
                format!("Failed to read the status snapshot: {}", e),
            )
        })?;

    Ok(RpcStatusSnapshot {
        block_height: snapshot.block_height,
        first_available_block: snapshot.first_available_block,
        value: snapshot
            .transactions
            .iter()
            .map(|tx| tx.as_ref().map(stored_status))
            .collect(),
    })
}
