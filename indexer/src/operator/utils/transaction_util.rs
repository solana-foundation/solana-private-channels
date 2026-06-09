use std::sync::Arc;

use crate::error::TransactionError;
use crate::operator::utils::instruction_util::RetryPolicy;
use crate::operator::ExtraErrorCheckPolicy;
use crate::operator::{sender::types::InstructionWithSigners, RpcClientWithRetry};
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use solana_keychain::SolanaSigner;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::InstructionError;
use solana_sdk::{
    commitment_config::CommitmentConfig, message::Message, signature::Signature,
    transaction::Transaction,
};
use tracing::{debug, warn};

pub const MAX_POLL_ATTEMPTS_CONFIRMATION: u32 = 5;

/// Result of transaction confirmation
#[derive(Debug, Clone)]
pub enum ConfirmationResult {
    /// Transaction confirmed on-chain
    Confirmed,
    /// Transaction failed with optional program error from PrivateChannelEscrowProgram
    Failed(Option<PrivateChannelEscrowProgramError>),
    /// Mint account not initialized (triggers initialization)
    MintNotInitialized,
    /// Transaction couldn't be confirmed after polling max attempts
    Retry,
}

/// Prepare and sign a transaction from an instruction and recent blockhash
///
/// # Arguments
/// * `rpc_client` - RPC client for sending transactions
/// * `ix_with_signers` - Instruction and signers
/// * `retry_policy` - Controls retry behavior for transaction send
///
/// # Signers
/// * Mint: Single signer (admin) as fee payer + mint authority
/// * ReleaseFunds: Dual signers (admin as fee payer, operator for authorization)
pub async fn sign_and_send_transaction(
    rpc_client: Arc<RpcClientWithRetry>,
    mut ix_with_signers: InstructionWithSigners,
    retry_policy: RetryPolicy,
) -> Result<(Signature, u64), TransactionError> {
    if let Some(compute_unit_price) = ix_with_signers.compute_unit_price {
        let compute_budget_ix =
            ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price);
        ix_with_signers.instructions.insert(0, compute_budget_ix);
    }

    // Prepend compute budget instruction if specified
    if let Some(compute_units) = ix_with_signers.compute_budget {
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(compute_units);
        ix_with_signers.instructions.insert(0, compute_budget_ix);
    }

    let (recent_blockhash, last_valid_block_height) = rpc_client
        .get_latest_blockhash_with_commitment()
        .await
        .map_err(TransactionError::Rpc)?;

    let message = Message::new_with_blockhash(
        &ix_with_signers.instructions,
        Some(&ix_with_signers.fee_payer),
        &recent_blockhash,
    );

    let mut transaction = Transaction::new_unsigned(message);

    for signer in ix_with_signers.signers.iter() {
        signer
            .sign_partial_transaction(&mut transaction)
            .await
            .map_err(TransactionError::Signer)?;
    }

    let signature = rpc_client
        .send_transaction(&transaction, retry_policy)
        .await
        .map_err(TransactionError::Rpc)?;

    Ok((signature, last_valid_block_height))
}

/// Check transaction status with polling.
///
/// Polls up to `MAX_POLL_ATTEMPTS_CONFIRMATION` times, sleeping `poll_interval_ms` between
/// each attempt. Pass `OperatorConfig::confirmation_poll_interval_ms` as the interval.
pub async fn check_transaction_status(
    rpc_client: Arc<RpcClientWithRetry>,
    signature: &Signature,
    commitment_config: CommitmentConfig,
    extra_error_checks_policy: &ExtraErrorCheckPolicy,
    poll_interval_ms: u64,
) -> Result<ConfirmationResult, TransactionError> {
    debug!("Checking transaction status: {}", signature);

    let mut attempts = 0;

    while attempts < MAX_POLL_ATTEMPTS_CONFIRMATION {
        let response = rpc_client
            .get_signature_statuses(&[*signature])
            .await
            .map_err(|e| {
                warn!("RPC error checking transaction status: {}", e);
                TransactionError::Rpc(e)
            })?;

        if let Some(status) = response.value.first().and_then(|s| s.as_ref()) {
            if status.satisfies_commitment(commitment_config) {
                if let Some(tx_err) = &status.err {
                    debug!("Transaction failed: {:?}", tx_err);

                    if let ExtraErrorCheckPolicy::Extra(error_checks) = extra_error_checks_policy {
                        for error_check in error_checks.iter() {
                            if let Some(result) = error_check(tx_err) {
                                return Ok(result);
                            }
                        }
                    }

                    return Ok(ConfirmationResult::Failed(parse_program_error(tx_err)));
                }

                debug!("Transaction confirmed: {}", signature);
                return Ok(ConfirmationResult::Confirmed);
            }
            debug!("Transaction not yet at commitment level: {}", signature);
        } else {
            debug!("Transaction not found: {}", signature);
        }

        attempts += 1;
        if attempts < MAX_POLL_ATTEMPTS_CONFIRMATION {
            tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;
        }
    }

    Ok(ConfirmationResult::Retry)
}

/// Check if transaction error indicates a mint account is not initialized
///
/// Detects Solana built-in errors for uninitialized or invalid account data:
/// - InvalidAccountData: "invalid account data for instruction"
/// - UninitializedAccount: "instruction requires an initialized account"
/// - IncorrectProgramId: "incorrect program id for instruction"
pub fn is_mint_not_initialized_error(
    err: &solana_sdk::transaction::TransactionError,
) -> Option<ConfirmationResult> {
    if matches!(
        err,
        solana_sdk::transaction::TransactionError::InstructionError(
            _,
            InstructionError::InvalidAccountData
                | InstructionError::UninitializedAccount
                | InstructionError::IncorrectProgramId
        )
    ) {
        return Some(ConfirmationResult::MintNotInitialized);
    }

    None
}

/// Treat `AccountAlreadyInitialized` as a confirmed `InitializeMint`: another
/// caller (or a racing retry) initialized the same mint first, which is the
/// desired end state. Returning `Confirmed` avoids a read-RPC re-check that
/// can lose to replication lag.
pub fn is_mint_already_initialized_error(
    err: &solana_sdk::transaction::TransactionError,
) -> Option<ConfirmationResult> {
    if matches!(
        err,
        solana_sdk::transaction::TransactionError::InstructionError(
            _,
            InstructionError::AccountAlreadyInitialized
        )
    ) {
        return Some(ConfirmationResult::Confirmed);
    }

    None
}

/// Parse program error code from transaction error
///
/// Extracts PrivateChannelEscrowProgramError from Solana transaction errors.
/// Returns None if error is not a custom program error.
pub fn parse_program_error(
    err: &solana_sdk::transaction::TransactionError,
) -> Option<PrivateChannelEscrowProgramError> {
    match err {
        solana_sdk::transaction::TransactionError::InstructionError(
            _,
            InstructionError::Custom(code),
        ) => {
            match *code {
                11 => Some(PrivateChannelEscrowProgramError::InvalidSmtProof),
                12 => Some(
                    PrivateChannelEscrowProgramError::InvalidTransactionNonceForCurrentTreeIndex,
                ),
                13 => Some(PrivateChannelEscrowProgramError::UnexpectedTreeIndex),
                _ => None, // Ignore other program errors
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::TransactionError;

    // ====================================================================
    // is_mint_not_initialized_error
    // ====================================================================

    #[test]
    fn mint_not_init_invalid_account_data() {
        let err = TransactionError::InstructionError(0, InstructionError::InvalidAccountData);
        let result = is_mint_not_initialized_error(&err);
        assert!(matches!(
            result,
            Some(ConfirmationResult::MintNotInitialized)
        ));
    }

    #[test]
    fn mint_not_init_uninitialized_account() {
        let err = TransactionError::InstructionError(0, InstructionError::UninitializedAccount);
        let result = is_mint_not_initialized_error(&err);
        assert!(matches!(
            result,
            Some(ConfirmationResult::MintNotInitialized)
        ));
    }

    #[test]
    fn mint_not_init_incorrect_program_id() {
        let err = TransactionError::InstructionError(0, InstructionError::IncorrectProgramId);
        let result = is_mint_not_initialized_error(&err);
        assert!(matches!(
            result,
            Some(ConfirmationResult::MintNotInitialized)
        ));
    }

    #[test]
    fn mint_not_init_custom_error_returns_none() {
        let err = TransactionError::InstructionError(0, InstructionError::Custom(42));
        assert!(is_mint_not_initialized_error(&err).is_none());
    }

    #[test]
    fn mint_not_init_non_instruction_error_returns_none() {
        let err = TransactionError::InsufficientFundsForFee;
        assert!(is_mint_not_initialized_error(&err).is_none());
    }

    // ====================================================================
    // is_mint_already_initialized_error
    // ====================================================================

    #[test]
    fn mint_already_init_maps_to_confirmed() {
        let err =
            TransactionError::InstructionError(0, InstructionError::AccountAlreadyInitialized);
        assert!(matches!(
            is_mint_already_initialized_error(&err),
            Some(ConfirmationResult::Confirmed)
        ));
    }

    #[test]
    fn mint_already_init_other_instruction_error_returns_none() {
        let err = TransactionError::InstructionError(0, InstructionError::InvalidAccountData);
        assert!(is_mint_already_initialized_error(&err).is_none());
    }

    #[test]
    fn mint_already_init_non_instruction_error_returns_none() {
        let err = TransactionError::InsufficientFundsForFee;
        assert!(is_mint_already_initialized_error(&err).is_none());
    }

    // ====================================================================
    // parse_program_error
    // ====================================================================

    #[test]
    fn parse_custom_11_invalid_smt_proof() {
        let err = TransactionError::InstructionError(0, InstructionError::Custom(11));
        let result = parse_program_error(&err);
        assert!(matches!(
            result,
            Some(PrivateChannelEscrowProgramError::InvalidSmtProof)
        ));
    }

    #[test]
    fn parse_custom_12_invalid_nonce() {
        let err = TransactionError::InstructionError(0, InstructionError::Custom(12));
        let result = parse_program_error(&err);
        assert!(matches!(
            result,
            Some(PrivateChannelEscrowProgramError::InvalidTransactionNonceForCurrentTreeIndex)
        ));
    }

    #[test]
    fn parse_custom_13_unexpected_tree_index() {
        let err = TransactionError::InstructionError(0, InstructionError::Custom(13));
        let result = parse_program_error(&err);
        assert!(matches!(
            result,
            Some(PrivateChannelEscrowProgramError::UnexpectedTreeIndex)
        ));
    }

    #[test]
    fn parse_custom_99_returns_none() {
        let err = TransactionError::InstructionError(0, InstructionError::Custom(99));
        assert!(parse_program_error(&err).is_none());
    }

    #[test]
    fn parse_non_custom_returns_none() {
        let err = TransactionError::InstructionError(0, InstructionError::InvalidAccountData);
        assert!(parse_program_error(&err).is_none());
    }

    #[test]
    fn parse_non_instruction_error_returns_none() {
        let err = TransactionError::InsufficientFundsForFee;
        assert!(parse_program_error(&err).is_none());
    }

    // ====================================================================
    // check_transaction_status tests (mockito-based RPC mocking)
    // ====================================================================

    fn make_rpc_client_for_test(url: String) -> Arc<crate::operator::RpcClientWithRetry> {
        use crate::operator::utils::rpc_util::RetryConfig;
        use std::time::Duration;
        Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            url,
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        ))
    }

    /// A confirmed status with no error in the RPC response must produce Confirmed,
    /// meaning the caller can proceed to mark the transaction as settled.
    #[tokio::test]
    async fn check_transaction_status_returns_confirmed_on_success() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "confirmationStatus": "confirmed",
                            "confirmations": 1,
                            "err": null,
                            "slot": 100,
                            "status": {"Ok": null}
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let rpc_client = make_rpc_client_for_test(server.url());
        let sig = solana_sdk::signature::Signature::new_unique();

        let result = check_transaction_status(
            rpc_client,
            &sig,
            CommitmentConfig::confirmed(),
            &ExtraErrorCheckPolicy::None,
            400,
        )
        .await;

        assert!(matches!(result, Ok(ConfirmationResult::Confirmed)));
    }

    /// A confirmed status carrying Custom(11) must decode to Failed(InvalidSmtProof) so
    /// the sender receives the exact escrow-program error rather than a generic failure.
    #[tokio::test]
    async fn check_transaction_status_returns_failed_on_program_error() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 100},
                        "value": [{
                            "confirmationStatus": "confirmed",
                            "confirmations": 1,
                            "err": {"InstructionError": [0, {"Custom": 11}]},
                            "slot": 100,
                            "status": {"Err": {"InstructionError": [0, {"Custom": 11}]}}
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let rpc_client = make_rpc_client_for_test(server.url());
        let sig = solana_sdk::signature::Signature::new_unique();

        let result = check_transaction_status(
            rpc_client,
            &sig,
            CommitmentConfig::confirmed(),
            &ExtraErrorCheckPolicy::None,
            400,
        )
        .await;

        assert!(matches!(
            result,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::InvalidSmtProof
            )))
        ));
    }

    /// An RPC-level error (-32600) must surface as Err(TransactionError::Rpc) so the
    /// caller can distinguish a network/RPC failure from an on-chain transaction failure.
    #[tokio::test]
    async fn check_transaction_status_returns_err_on_rpc_failure() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getSignatureStatuses"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Invalid request"}
                })
                .to_string(),
            )
            .create();

        let rpc_client = make_rpc_client_for_test(server.url());
        let sig = solana_sdk::signature::Signature::new_unique();

        let result = check_transaction_status(
            rpc_client,
            &sig,
            CommitmentConfig::confirmed(),
            &ExtraErrorCheckPolicy::None,
            400,
        )
        .await;

        assert!(
            matches!(result, Err(crate::error::TransactionError::Rpc(_))),
            "expected TransactionError::Rpc, got: {:?}",
            result
        );
    }

    // ====================================================================
    // sign_and_send_transaction tests (mockito-based RPC mocking)
    // ====================================================================

    fn make_instruction_with_empty_signers(
    ) -> super::super::super::sender::types::InstructionWithSigners {
        use solana_keychain::Signer;
        super::super::super::sender::types::InstructionWithSigners {
            instructions: vec![],
            fee_payer: solana_sdk::pubkey::Pubkey::default(),
            signers: Vec::<&'static Signer>::new(),
            compute_unit_price: None,
            compute_budget: None,
        }
    }

    /// A successful getLatestBlockhash + sendTransaction round-trip must return the exact
    /// signature string echoed by the RPC server, not just any Ok value.
    #[tokio::test]
    async fn sign_and_send_transaction_returns_signature_on_success() {
        let mut server = mockito::Server::new_async().await;
        let expected_sig = solana_sdk::signature::Signature::default().to_string();

        let _m_hash = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "GHtXQBsoZHjzkAm2Sdm6FTyFHBCqBnLanJJhZFCFJXoe",
                            "lastValidBlockHeight": 100
                        }
                    }
                })
                .to_string(),
            )
            .create();

        let _m_send = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "sendTransaction"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": expected_sig
                })
                .to_string(),
            )
            .create();

        let rpc_client = make_rpc_client_for_test(server.url());
        let ix = make_instruction_with_empty_signers();

        let result = sign_and_send_transaction(rpc_client, ix, RetryPolicy::None).await;

        let (sig, _) = result.unwrap();
        assert_eq!(sig.to_string(), expected_sig);
    }

    /// When getLatestBlockhash fails the function must return Err(TransactionError::Rpc)
    /// immediately — sendTransaction must never be called with a stale or missing blockhash.
    #[tokio::test]
    async fn sign_and_send_transaction_returns_err_on_blockhash_failure() {
        let mut server = mockito::Server::new_async().await;

        let _m = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getLatestBlockhash"
            })))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32600, "message": "Server error"}
                })
                .to_string(),
            )
            .create();

        let rpc_client = make_rpc_client_for_test(server.url());
        let ix = make_instruction_with_empty_signers();

        let result = sign_and_send_transaction(rpc_client, ix, RetryPolicy::None).await;

        assert!(
            matches!(result, Err(crate::error::TransactionError::Rpc(_))),
            "expected TransactionError::Rpc, got: {:?}",
            result
        );
    }
}
