use std::sync::Arc;

use crate::config::ProgramType;
use crate::error::TransactionError;
use crate::metrics::OPERATOR_TRANSACTION_ERRORS;
use crate::operator::utils::instruction_util::{RetryPolicy, TransactionKind};
use crate::operator::ExtraErrorCheckPolicy;
use crate::operator::{sender::types::InstructionWithSigners, RpcClientWithRetry};
use private_channel_escrow_program_client::errors::PrivateChannelEscrowProgramError;
use private_channel_escrow_program_client::programs::PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
use private_channel_metrics::MetricLabel;
use solana_commitment_config::CommitmentConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keychain::SolanaSigner;
use solana_sdk::instruction::InstructionError;
use solana_sdk::{
    message::Message, pubkey::Pubkey, signature::Signature, transaction::Transaction,
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
    ix_with_signers: InstructionWithSigners,
    retry_policy: RetryPolicy,
) -> Result<(Signature, u64, u64), TransactionError> {
    let (transaction, signature, last_valid_block_height, blockhash_slot) =
        build_and_sign(&rpc_client, ix_with_signers).await?;
    send_signed(&rpc_client, &transaction, retry_policy).await?;
    Ok((signature, last_valid_block_height, blockhash_slot))
}

/// Build and sign a transaction without broadcasting it.
///
/// Returns the signed transaction, its signature, the blockhash's
/// `last_valid_block_height` and the slot that blockhash was read at. Splitting
/// this from the send lets the release path persist the signature write-ahead
/// before the broadcast; the slot is journaled with it so a later finality
/// verdict knows the earliest block the signature could appear in.
pub async fn build_and_sign(
    rpc_client: &RpcClientWithRetry,
    mut ix_with_signers: InstructionWithSigners,
) -> Result<(Transaction, Signature, u64, u64), TransactionError> {
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

    let (recent_blockhash, blockhash_slot, last_valid_block_height) = rpc_client
        .get_latest_blockhash_with_commitment_and_context()
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
            .sign_transaction(&mut transaction)
            .await
            .map_err(TransactionError::Signer)?;
    }

    let signature = transaction.signatures[0];

    Ok((
        transaction,
        signature,
        last_valid_block_height,
        blockhash_slot,
    ))
}

/// Broadcast an already-signed transaction.
pub async fn send_signed(
    rpc_client: &RpcClientWithRetry,
    transaction: &Transaction,
    retry_policy: RetryPolicy,
) -> Result<Signature, TransactionError> {
    rpc_client
        .send_transaction(transaction, retry_policy)
        .await
        .map_err(TransactionError::Rpc)
}

/// Check transaction status with polling.
///
/// Polls up to `MAX_POLL_ATTEMPTS_CONFIRMATION` times, sleeping `poll_interval_ms` between
/// each attempt. Pass `OperatorConfig::confirmation_poll_interval_ms` as the interval.
pub async fn check_transaction_status(
    rpc_client: Arc<RpcClientWithRetry>,
    signature: &Signature,
    kind: TransactionKind,
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

                    return Ok(classify_failure(&rpc_client, signature, tx_err, kind).await);
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

/// Route a failed transaction by what its custom code can prove, given the kind that sent it.
///
/// A code names an escrow error only if the escrow raised it. RotateBitmap's only CPI is
/// the escrow's own event, so its code is trusted. ReleaseFunds calls Token-2022 and any
/// transfer hook, which fail under the same top-level instruction with their own
/// numbering, so the runtime log decides: another program's error is a generic failure,
/// and an unreadable log is retried, since the failed release moved nothing. A channel
/// transaction cannot raise an escrow error at all.
pub async fn classify_failure(
    rpc_client: &RpcClientWithRetry,
    signature: &Signature,
    err: &solana_sdk::transaction::TransactionError,
    kind: TransactionKind,
) -> ConfirmationResult {
    let solana_sdk::transaction::TransactionError::InstructionError(
        _,
        InstructionError::Custom(code),
    ) = err
    else {
        return ConfirmationResult::Failed(None);
    };
    let Some(escrow_error) = escrow_error_for_code(*code) else {
        return ConfirmationResult::Failed(None);
    };
    match kind {
        TransactionKind::Mint | TransactionKind::InitializeMint => {
            return ConfirmationResult::Failed(None)
        }
        TransactionKind::RotateBitmap => return ConfirmationResult::Failed(Some(escrow_error)),
        TransactionKind::ReleaseFunds => {}
    }

    let logs = match rpc_client.get_transaction(signature).await {
        Ok(transaction) => transaction
            .transaction
            .meta
            .and_then(|meta| Option::<Vec<String>>::from(meta.log_messages)),
        Err(e) => {
            warn!("Could not read logs for failed transaction {signature}: {e}");
            None
        }
    };
    match logs.as_deref().and_then(failing_program) {
        Some(origin) if origin == PRIVATE_CHANNEL_ESCROW_PROGRAM_ID => {
            ConfirmationResult::Failed(Some(escrow_error))
        }
        Some(origin) => {
            warn!("Custom error {code} on {signature} raised by {origin}, not the escrow; treating it as a generic failure");
            ConfirmationResult::Failed(None)
        }
        None => {
            // The retry also counts as a confirmation timeout; this label names the cause.
            OPERATOR_TRANSACTION_ERRORS
                .with_label_values(&[
                    ProgramType::Withdraw.as_label(),
                    "escrow_error_origin_unproven",
                ])
                .inc();
            warn!(
                "Custom error {code} on {signature} has no provable origin; retrying the release"
            );
            ConfirmationResult::Retry
        }
    }
}

/// The escrow error a custom code names, if the escrow raised it.
fn escrow_error_for_code(code: u32) -> Option<PrivateChannelEscrowProgramError> {
    match code {
        11 => Some(PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap),
        12 => Some(PrivateChannelEscrowProgramError::NonceAlreadyUsed),
        13 => Some(PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration),
        14 => Some(PrivateChannelEscrowProgramError::UnexpectedGeneration),
        _ => None,
    }
}

/// The program whose error failed the transaction: the runtime logs each failing frame
/// innermost first. Program output is always prefixed `Program log:`, so it cannot pose
/// as a runtime line. A truncated log may have lost the innermost frame, so it names none.
fn failing_program(logs: &[String]) -> Option<Pubkey> {
    if logs.iter().any(|line| line == "Log truncated") {
        return None;
    }
    logs.iter().find_map(|line| {
        let (program, outcome) = line.strip_prefix("Program ")?.split_once(' ')?;
        if !outcome.starts_with("failed:") {
            return None;
        }
        program.parse().ok()
    })
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
    // classify_failure
    // ====================================================================

    /// This map is the single point where an on-chain error code becomes a
    /// named variant. The codes were renumbered with the bitmap change, and a
    /// stale entry here would silently route one rejection down another's arm,
    /// which is how a spent nonce could end up reminted. Pin every code.
    #[test]
    fn escrow_error_code_table() {
        let cases = [
            (
                11u32,
                Some(PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap),
            ),
            (12, Some(PrivateChannelEscrowProgramError::NonceAlreadyUsed)),
            (
                13,
                Some(PrivateChannelEscrowProgramError::NonceOutsideCurrentGeneration),
            ),
            (
                14,
                Some(PrivateChannelEscrowProgramError::UnexpectedGeneration),
            ),
            (99, None),
        ];

        for (code, expected) in cases {
            assert_eq!(escrow_error_for_code(code), expected, "custom code {code}");
        }
    }

    /// Runtime log of a ReleaseFunds whose transfer hook rejected the transfer. Each
    /// failing frame logs its own `failed:` line, innermost first.
    fn hook_rejection_logs(token_program: &Pubkey, hook: &Pubkey, code: u32) -> Vec<String> {
        let escrow = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
        vec![
            format!("Program {escrow} invoke [1]"),
            "Program log: Instruction: ReleaseFunds".to_string(),
            format!("Program {token_program} invoke [2]"),
            format!("Program {hook} invoke [3]"),
            format!("Program {hook} consumed 1200 of 180000 compute units"),
            format!("Program {hook} failed: custom program error: {code:#x}"),
            format!("Program {token_program} consumed 9000 of 190000 compute units"),
            format!("Program {token_program} failed: custom program error: {code:#x}"),
            format!("Program {escrow} consumed 20000 of 200000 compute units"),
            format!("Program {escrow} failed: custom program error: {code:#x}"),
        ]
    }

    /// Runtime log of a ReleaseFunds the escrow refused itself, before any CPI.
    fn escrow_rejection_logs(code: u32) -> Vec<String> {
        let escrow = PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
        vec![
            format!("Program {escrow} invoke [1]"),
            "Program log: Instruction: ReleaseFunds".to_string(),
            format!("Program {escrow} consumed 8000 of 200000 compute units"),
            format!("Program {escrow} failed: custom program error: {code:#x}"),
        ]
    }

    #[test]
    fn failing_program_names_the_innermost_failure() {
        let hook = Pubkey::new_unique();
        let logs = hook_rejection_logs(&Pubkey::new_unique(), &hook, 12);
        assert_eq!(failing_program(&logs), Some(hook));
        assert_eq!(
            failing_program(&escrow_rejection_logs(12)),
            Some(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID)
        );
    }

    /// A program can log any text, but only behind `Program log:`, so it cannot pass as the escrow.
    #[test]
    fn failing_program_ignores_program_logged_text() {
        let hook = Pubkey::new_unique();
        let mut logs = hook_rejection_logs(&Pubkey::new_unique(), &hook, 12);
        logs.insert(
            4,
            format!(
                "Program log: Program {PRIVATE_CHANNEL_ESCROW_PROGRAM_ID} failed: custom program error: 0xc"
            ),
        );
        assert_eq!(failing_program(&logs), Some(hook));
    }

    /// A truncated log may have lost the innermost failure, so it proves no origin.
    #[test]
    fn failing_program_refuses_truncated_logs() {
        let mut logs = escrow_rejection_logs(12);
        logs.push("Log truncated".to_string());
        assert_eq!(failing_program(&logs), None);
    }

    /// `getTransaction` reply carrying the given runtime log.
    fn transaction_reply_with_logs(logs: &[String], code: u32) -> String {
        let err = serde_json::json!({"InstructionError": [0, {"Custom": code}]});
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "slot": 100,
                "blockTime": null,
                "transaction": {
                    "signatures": [Signature::new_unique().to_string()],
                    "message": {
                        "header": {
                            "numRequiredSignatures": 1,
                            "numReadonlySignedAccounts": 0,
                            "numReadonlyUnsignedAccounts": 1,
                        },
                        "accountKeys": [
                            Pubkey::new_unique().to_string(),
                            PRIVATE_CHANNEL_ESCROW_PROGRAM_ID.to_string(),
                        ],
                        "recentBlockhash": "11111111111111111111111111111111",
                        "instructions": [],
                    },
                },
                "meta": {
                    "err": err,
                    "status": {"Err": err},
                    "fee": 5000,
                    "preBalances": [0, 0],
                    "postBalances": [0, 0],
                    "logMessages": logs,
                },
            },
        })
        .to_string()
    }

    fn mock_transaction_logs(
        server: &mut mockito::ServerGuard,
        logs: &[String],
        code: u32,
    ) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getTransaction"
            })))
            .with_status(200)
            .with_body(transaction_reply_with_logs(logs, code))
            .create()
    }

    /// A transfer hook's code 12 is not the escrow's NonceAlreadyUsed: the failed release
    /// consumed nothing, so it must take the generic, proof-gated failure path.
    #[tokio::test]
    async fn hook_custom_error_is_not_decoded_as_escrow() {
        let mut server = mockito::Server::new_async().await;
        let logs = hook_rejection_logs(&Pubkey::new_unique(), &Pubkey::new_unique(), 12);
        let _tx = mock_transaction_logs(&mut server, &logs, 12);
        let rpc_client = make_rpc_client_for_test(server.url());
        let err = TransactionError::InstructionError(0, InstructionError::Custom(12));

        let result = classify_failure(
            &rpc_client,
            &Signature::new_unique(),
            &err,
            TransactionKind::ReleaseFunds,
        )
        .await;

        assert!(
            matches!(result, ConfirmationResult::Failed(None)),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn escrow_custom_error_is_decoded() {
        let mut server = mockito::Server::new_async().await;
        let _tx = mock_transaction_logs(&mut server, &escrow_rejection_logs(12), 12);
        let rpc_client = make_rpc_client_for_test(server.url());
        let err = TransactionError::InstructionError(0, InstructionError::Custom(12));

        let result = classify_failure(
            &rpc_client,
            &Signature::new_unique(),
            &err,
            TransactionKind::ReleaseFunds,
        )
        .await;

        assert!(
            matches!(
                result,
                ConfirmationResult::Failed(Some(
                    PrivateChannelEscrowProgramError::NonceAlreadyUsed
                ))
            ),
            "{result:?}"
        );
    }

    /// Unreadable logs prove no origin either way. The failed release moved nothing, so it
    /// is resent under the retry budget rather than routed as a foreign failure.
    #[tokio::test]
    async fn unreadable_release_logs_retry() {
        let mut server = mockito::Server::new_async().await;
        let _tx = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getTransaction"
            })))
            .with_status(500)
            .create();
        let rpc_client = make_rpc_client_for_test(server.url());
        let err = TransactionError::InstructionError(0, InstructionError::Custom(13));
        let unproven = OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&["withdraw", "escrow_error_origin_unproven"]);
        let before = unproven.get();

        let result = classify_failure(
            &rpc_client,
            &Signature::new_unique(),
            &err,
            TransactionKind::ReleaseFunds,
        )
        .await;

        assert!(matches!(result, ConfirmationResult::Retry), "{result:?}");
        assert_eq!(
            unproven.get(),
            before + 1.0,
            "the retry must be visible as an unproven origin, not only as a timeout"
        );
    }

    /// RotateBitmap's only CPI is the escrow's own event, so its code is the escrow's.
    #[tokio::test]
    async fn rotation_custom_error_is_trusted_without_logs() {
        let mut server = mockito::Server::new_async().await;
        let logs_read = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getTransaction"
            })))
            .expect(0)
            .create();
        let rpc_client = make_rpc_client_for_test(server.url());
        let err = TransactionError::InstructionError(0, InstructionError::Custom(14));

        let result = classify_failure(
            &rpc_client,
            &Signature::new_unique(),
            &err,
            TransactionKind::RotateBitmap,
        )
        .await;

        assert!(
            matches!(
                result,
                ConfirmationResult::Failed(Some(
                    PrivateChannelEscrowProgramError::UnexpectedGeneration
                ))
            ),
            "{result:?}"
        );
        logs_read.assert();
    }

    /// A channel mint cannot raise an escrow error, so its code is never decoded as one.
    #[tokio::test]
    async fn mint_custom_error_is_never_decoded() {
        let mut server = mockito::Server::new_async().await;
        let logs_read = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "method": "getTransaction"
            })))
            .expect(0)
            .create();
        let rpc_client = make_rpc_client_for_test(server.url());
        let err = TransactionError::InstructionError(0, InstructionError::Custom(12));

        let result = classify_failure(
            &rpc_client,
            &Signature::new_unique(),
            &err,
            TransactionKind::Mint,
        )
        .await;

        assert!(
            matches!(result, ConfirmationResult::Failed(None)),
            "{result:?}"
        );
        logs_read.assert();
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
            TransactionKind::ReleaseFunds,
            CommitmentConfig::confirmed(),
            &ExtraErrorCheckPolicy::None,
            400,
        )
        .await;

        assert!(matches!(result, Ok(ConfirmationResult::Confirmed)));
    }

    /// A confirmed status carrying the escrow's own Custom(11) must decode to
    /// Failed(InvalidWithdrawalBitmap) so the sender receives the exact
    /// escrow-program error rather than a generic failure.
    #[tokio::test]
    async fn check_transaction_status_returns_failed_on_program_error() {
        let mut server = mockito::Server::new_async().await;
        let _tx = mock_transaction_logs(&mut server, &escrow_rejection_logs(11), 11);
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
            TransactionKind::ReleaseFunds,
            CommitmentConfig::confirmed(),
            &ExtraErrorCheckPolicy::None,
            400,
        )
        .await;

        assert!(matches!(
            result,
            Ok(ConfirmationResult::Failed(Some(
                PrivateChannelEscrowProgramError::InvalidWithdrawalBitmap
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
            TransactionKind::ReleaseFunds,
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

        let (sig, _, _) = result.unwrap();
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

    /// `build_and_sign` must return the signature embedded in the signed transaction (`signatures[0]`), so the write-ahead persist records the real on-chain signature.
    #[tokio::test]
    async fn build_and_sign_returns_first_transaction_signature() {
        let mut server = mockito::Server::new_async().await;

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

        let rpc_client = make_rpc_client_for_test(server.url());
        let ix = make_instruction_with_empty_signers();

        let (transaction, signature, _lvbh, _slot) = build_and_sign(&rpc_client, ix)
            .await
            .expect("build_and_sign");

        assert_eq!(
            signature, transaction.signatures[0],
            "returned signature must be the transaction's first signature"
        );
    }
}
