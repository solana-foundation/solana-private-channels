use crate::rpc::{
    constants::MAX_SIGNATURES,
    error::{custom_error, INVALID_PARAMS_CODE, JSON_RPC_SERVER_ERROR},
    ReadDeps,
};
use jsonrpsee::core::RpcResult;
use solana_rpc_client_types::config::RpcSignatureStatusConfig;
use solana_rpc_client_types::response::{Response, RpcResponseContext};
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::{TransactionConfirmationStatus, TransactionStatus};
use std::str::FromStr;
use tracing::{debug, warn};

pub async fn get_signature_statuses_impl(
    read_deps: &ReadDeps,
    signatures: Vec<String>,
    _config: Option<RpcSignatureStatusConfig>,
) -> RpcResult<Response<Vec<Option<TransactionStatus>>>> {
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

    // Read the slot BEFORE the per-signature lookups. The lookups then observe a
    // state at or after it, so the reported context understates freshness and a
    // null can never claim to cover a block the response has not yet seen.
    let current_slot = read_deps
        .accounts_db
        .get_current_slot()
        .await
        .map_err(|e| custom_error(JSON_RPC_SERVER_ERROR, format!("Failed to get slot: {}", e)))?
        .unwrap_or(0);

    let mut statuses = Vec::with_capacity(signatures.len());

    for sig_str in signatures {
        // An unparseable signature is a client error, not an absence: rendering it
        // as null would make a caller read a typo as proof of non-inclusion.
        let signature = Signature::from_str(&sig_str).map_err(|e| {
            warn!(
                signature = %sig_str.get(..20).unwrap_or(&sig_str),
                error = %e,
                "Invalid signature format in getSignatureStatuses"
            );
            custom_error(INVALID_PARAMS_CODE, format!("Invalid signature: {}", e))
        })?;

        // A lookup failure is an error, never a null: absence must mean absence.
        let stored_tx = read_deps
            .accounts_db
            .get_transaction(&signature)
            .await
            .map_err(|e| {
                custom_error(
                    JSON_RPC_SERVER_ERROR,
                    format!("Failed to get transaction status: {}", e),
                )
            })?;

        match stored_tx {
            Some(tx) => {
                // Transaction found - return its status
                // In PrivateChannel, all found transactions are confirmed (finalized)
                debug!(
                    signature = %signature,
                    status = ?tx.meta.status,
                    err = ?tx.meta.err,
                    "getSignatureStatuses transaction found"
                );

                let err = tx.meta.err.clone();
                statuses.push(Some(TransactionStatus {
                    slot: tx.slot,
                    confirmations: None,
                    status: err.clone().map_or(Ok(()), Err),
                    err,
                    confirmation_status: Some(TransactionConfirmationStatus::Finalized),
                }));
            }
            None => {
                debug!(
                    signature = %signature,
                    "getSignatureStatuses transaction not found"
                );
                // Transaction not found
                statuses.push(None);
            }
        }
    }

    Ok(Response {
        context: RpcResponseContext::new(current_slot),
        value: statuses,
    })
}
