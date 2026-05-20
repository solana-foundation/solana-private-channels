#[cfg(test)]
use super::types::InFlightQueue;
use super::types::SenderState;
use crate::{
    channel_utils::send_guaranteed,
    operator::{
        check_transaction_status, remint_idempotency_memo,
        sender::{
            find_existing_mint_signature_with_memo,
            transaction::FINALITY_SAFETY_DELAY,
            types::{InstructionWithSigners, PendingRemint},
        },
        sign_and_send_transaction,
        utils::instruction_util::WithdrawalRemintInfo,
        ConfirmationResult, ExtraErrorCheckPolicy, MintToBuilder, MintToBuilderWithTxnId,
        RetryPolicy, SignerUtil, TransactionStatusUpdate,
    },
    storage::TransactionStatus,
};
use chrono::Utc;
use solana_keychain::SolanaSigner;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Maximum number of finality-check retries before giving up and sending to ManualReview.
const MAX_FINALITY_CHECK_ATTEMPTS: u32 = 3;

/// Attempt to remint burned PrivateChannel tokens back to user after permanent withdrawal failure.
/// Builds a MintTo instruction with an idempotency memo (same pattern as deposits).
/// No sender-level retry — RPC-level retries may still occur via RpcClientWithRetry.
async fn attempt_remint(
    state: &SenderState,
    info: &WithdrawalRemintInfo,
) -> Result<Signature, String> {
    let memo = remint_idempotency_memo(info.transaction_id);
    let admin_pubkey = SignerUtil::admin_signer().pubkey();

    // Build remint transaction with idempotency memo to prevent duplicate mints across restarts
    let mut builder = MintToBuilder::new();
    builder
        .mint(info.mint)
        .recipient(info.user)
        .recipient_ata(info.user_ata)
        .payer(admin_pubkey)
        .mint_authority(admin_pubkey)
        .token_program(info.token_program)
        .amount(info.amount)
        .idempotency_memo(memo.clone());

    // Check for an already-confirmed remint before sending (guards against duplicate
    // remints when the operator restarts after a successful remint but before the
    // FailedReminted status is persisted to the database).
    let builder_for_lookup = MintToBuilderWithTxnId {
        builder: builder.clone(),
        txn_id: info.transaction_id,
        trace_id: info.trace_id.clone(),
    };
    match find_existing_mint_signature_with_memo(&state.rpc_client, &builder_for_lookup, &memo)
        .await
    {
        Ok(Some(existing_signature)) => {
            info!(
                "Remint already confirmed for transaction {}: {}",
                info.transaction_id, existing_signature
            );
            return Ok(existing_signature);
        }
        Ok(None) => {}
        Err(e) => {
            warn!(
                "Remint idempotency lookup failed for transaction {}: {} — proceeding with send",
                info.transaction_id, e
            );
        }
    }

    let instructions = builder
        .instructions()
        .map_err(|e| format!("Failed to build remint instructions: {}", e))?;

    let ix = InstructionWithSigners {
        instructions,
        fee_payer: admin_pubkey,
        signers: vec![SignerUtil::admin_signer()],
        compute_unit_price: None,
        compute_budget: None,
    };

    let signature = sign_and_send_transaction(state.rpc_client.clone(), ix, RetryPolicy::None)
        .await
        .map_err(|e| format!("Failed to send remint transaction: {}", e))?;

    let result = check_transaction_status(
        state.rpc_client.clone(),
        &signature,
        CommitmentConfig::confirmed(),
        &ExtraErrorCheckPolicy::None,
        state.confirmation_poll_interval_ms,
    )
    .await
    .map_err(|e| format!("Failed to confirm remint transaction: {}", e))?;

    match result {
        ConfirmationResult::Confirmed => {
            info!("Remint confirmed: {}", signature);
            Ok(signature)
        }
        other => Err(format!("Remint not confirmed: {:?}", other)),
    }
}

/// Execute the actual remint for a matured PendingRemint entry.
pub async fn execute_deferred_remint(
    state: &SenderState,
    entry: &super::types::PendingRemint,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    match attempt_remint(state, &entry.remint_info).await {
        Ok(signature) => {
            info!(
                "Withdrawal failed but tokens reminted successfully: {}",
                signature
            );
            if let Some(transaction_id) = entry.ctx.transaction_id {
                if let Err(e) = send_guaranteed(
                    storage_tx,
                    TransactionStatusUpdate {
                        transaction_id,
                        trace_id: entry.ctx.trace_id.clone(),
                        status: TransactionStatus::FailedReminted,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(entry.original_error.clone()),
                        remint_signature: Some(signature.to_string()),
                        remint_attempted: true,
                    },
                    "transaction status update",
                )
                .await
                {
                    error!(
                        "Failed to send FailedReminted status for txn {}: {}. \
                         Remint sig {} confirmed on-chain but not recorded.",
                        transaction_id, e, signature
                    );
                }
            } else {
                error!(
                    "Remint succeeded (sig: {}) but no transaction_id to record status",
                    signature
                );
            }
        }
        Err(remint_error) => {
            error!("Remint also failed: {}", remint_error);
            let combined = format!("{} | remint failed: {}", entry.original_error, remint_error);
            if let Some(transaction_id) = entry.ctx.transaction_id {
                send_guaranteed(
                    storage_tx,
                    TransactionStatusUpdate {
                        transaction_id,
                        trace_id: entry.ctx.trace_id.clone(),
                        status: TransactionStatus::ManualReview,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(combined),
                        remint_signature: None,
                        remint_attempted: true,
                    },
                    "transaction status update",
                )
                .await
                .ok();
            }
        }
    }
}

/// Process matured entries in the deferred remint queue.
/// Called from the sender loop tick. For each matured entry, checks whether any
/// previously sent withdrawal signature reached finalized commitment. If so, the
/// withdrawal actually succeeded and we report Completed. Otherwise we attempt remint.
pub async fn process_pending_remints(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let now = Utc::now();

    // Partition: matured entries get processed, immature stay in the queue
    let mut remaining = Vec::new();
    let mut matured = Vec::new();
    for entry in state.pending_remints.drain(..) {
        if entry.deadline <= now {
            matured.push(entry);
        } else {
            remaining.push(entry);
        }
    }

    for entry in matured {
        let nonce_label = entry
            .ctx
            .withdrawal_nonce
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string());

        match state
            .rpc_client
            .get_signature_statuses_with_history(&entry.signatures)
            .await
        {
            Ok(response) => {
                let mut found_finalized = false;
                for (i, status_opt) in response.value.iter().enumerate() {
                    if let Some(status) = status_opt {
                        if status.satisfies_commitment(CommitmentConfig::finalized())
                            && status.err.is_none()
                        {
                            info!(
                                "Withdrawal nonce {} actually finalized (sig: {}) — skipping remint",
                                nonce_label, entry.signatures[i]
                            );
                            if let Some(transaction_id) = entry.ctx.transaction_id {
                                send_guaranteed(
                                    storage_tx,
                                    TransactionStatusUpdate {
                                        transaction_id,
                                        trace_id: entry.ctx.trace_id.clone(),
                                        status: TransactionStatus::Completed,
                                        counterpart_signature: Some(
                                            entry.signatures[i].to_string(),
                                        ),
                                        processed_at: Some(Utc::now()),
                                        error_message: None,
                                        remint_signature: None,
                                        remint_attempted: false,
                                    },
                                    "transaction status update",
                                )
                                .await
                                .ok();
                            }
                            found_finalized = true;
                            break;
                        }
                    }
                }
                if found_finalized {
                    continue;
                }
                // No sig finalized → proceed to remint
                info!(
                    "No finalized withdrawal for nonce {} — attempting remint",
                    nonce_label
                );
                execute_deferred_remint(state, &entry, storage_tx).await;
            }
            Err(e) => {
                let attempt = entry.finality_check_attempts + 1;
                if attempt >= MAX_FINALITY_CHECK_ATTEMPTS {
                    error!(
                        "Finality check for nonce {} failed after {} attempts — \
                         cannot verify withdrawal status, sending to ManualReview: {}",
                        nonce_label, attempt, e
                    );
                    if let Some(transaction_id) = entry.ctx.transaction_id {
                        send_guaranteed(
                            storage_tx,
                            TransactionStatusUpdate {
                                transaction_id,
                                trace_id: entry.ctx.trace_id.clone(),
                                status: TransactionStatus::ManualReview,
                                counterpart_signature: None,
                                processed_at: Some(Utc::now()),
                                error_message: Some(format!(
                                    "{} | finality check failed after {} attempts: {}",
                                    entry.original_error, attempt, e
                                )),
                                remint_signature: None,
                                remint_attempted: false,
                            },
                            "transaction status update",
                        )
                        .await
                        .ok();
                    }
                } else {
                    warn!(
                        "Finality check for nonce {} failed (attempt {}/{}) — \
                         re-queuing with extended deadline: {}",
                        nonce_label, attempt, MAX_FINALITY_CHECK_ATTEMPTS, e
                    );
                    remaining.push(PendingRemint {
                        finality_check_attempts: attempt,
                        deadline: Utc::now()
                            + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap(),
                        ..entry
                    });
                }
            }
        }
    }

    state.pending_remints = remaining;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::types::{
        PendingRemint, SenderState, TransactionContext, MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::operator::MintCache;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Once;
    use tokio::sync::{mpsc, Semaphore};

    static INIT_TEST_SIGNER: Once = Once::new();
    fn ensure_test_signer() {
        INIT_TEST_SIGNER.call_once(|| {
            let kp = solana_sdk::signer::keypair::Keypair::new();
            let b58 = bs58::encode(kp.to_bytes()).into_string();
            std::env::set_var("ADMIN_SIGNER", "memory");
            std::env::set_var("ADMIN_PRIVATE_KEY", &b58);
        });
    }

    fn make_sender_state() -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            crate::operator::RetryConfig::default(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc,
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    fn make_remint_info(txn_id: i64) -> WithdrawalRemintInfo {
        WithdrawalRemintInfo {
            transaction_id: txn_id,
            trace_id: format!("trace-{txn_id}"),
            mint: solana_sdk::pubkey::Pubkey::new_unique(),
            user: solana_sdk::pubkey::Pubkey::new_unique(),
            user_ata: solana_sdk::pubkey::Pubkey::new_unique(),
            token_program: spl_token::id(),
            amount: 5000,
        }
    }

    fn make_sender_state_with_rpc(rpc_url: &str) -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            rpc_url.to_string(),
            crate::operator::RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        SenderState {
            rpc_client: rpc,
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: crate::config::ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    #[tokio::test]
    async fn process_pending_remints_requeues_on_rpc_error() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Push a matured entry — RPC will fail (no real endpoint)
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![Signature::new_unique()],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // RPC error on first attempt → re-queued, not resolved
        assert!(
            storage_rx.try_recv().is_err(),
            "should NOT send status on first RPC failure"
        );
        assert_eq!(
            state.pending_remints.len(),
            1,
            "should re-queue entry after RPC error"
        );
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    #[tokio::test]
    async fn process_pending_remints_manual_review_after_max_rpc_failures() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Push entry already at max attempts — next RPC failure triggers ManualReview
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![Signature::new_unique()],
            original_error: "max retries".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // MAX_FINALITY_CHECK_ATTEMPTS - 1
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("should receive status update");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(
            update.status,
            TransactionStatus::ManualReview,
            "exhausted finality check retries should produce ManualReview"
        );

        let err = update.error_message.as_deref().unwrap();
        assert!(
            err.contains("finality check failed"),
            "should mention finality check failure: {err}"
        );
        assert!(
            err.contains("max retries"),
            "should contain original error: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "should not re-queue after max attempts"
        );
    }

    /// When the pending_remints queue contains both matured entries (deadline
    /// in the past) and immature ones (deadline in the future), only the
    /// matured entries should be processed on a given tick.
    ///
    /// The immature entry must remain in the queue completely unchanged —
    /// same deadline, same attempt count. Processing it early would violate
    /// the finality window guarantee that prevents double-minting.
    #[tokio::test]
    async fn process_pending_remints_handles_mixed_matured_and_immature() {
        let mut state = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let future_deadline = Utc::now() + chrono::Duration::seconds(600);

        // Entry 1: matured — RPC will fail (localhost unreachable), so it
        // gets re-queued with attempt=1. This is the observable side-effect
        // that proves it was processed.
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(10),
                withdrawal_nonce: Some(1),
                trace_id: Some("trace-10".to_string()),
            },
            remint_info: make_remint_info(10),
            signatures: vec![Signature::new_unique()],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        // Entry 2: immature — must not be touched at all.
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(2),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![Signature::new_unique()],
            original_error: "release_funds failed".to_string(),
            deadline: future_deadline,
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // No status update yet — the matured entry's RPC failed and was re-queued,
        // the immature entry was skipped entirely.
        assert!(
            storage_rx.try_recv().is_err(),
            "no status update expected on first RPC failure"
        );

        // Both entries are still in the queue.
        assert_eq!(state.pending_remints.len(), 2);

        // The matured entry was processed: attempt counter incremented.
        let matured = state
            .pending_remints
            .iter()
            .find(|e| e.ctx.transaction_id == Some(10))
            .expect("matured entry should still be in queue");
        assert_eq!(
            matured.finality_check_attempts, 1,
            "matured entry should have attempt=1 after first RPC failure"
        );

        // The immature entry was not touched: attempt counter and deadline unchanged.
        let immature = state
            .pending_remints
            .iter()
            .find(|e| e.ctx.transaction_id == Some(20))
            .expect("immature entry should still be in queue");
        assert_eq!(
            immature.finality_check_attempts, 0,
            "immature entry must not be processed"
        );
        assert_eq!(
            immature.deadline, future_deadline,
            "immature entry deadline must be unchanged"
        );
    }

    /// The core anti-duplication invariant: if the original withdrawal
    /// transaction reached finality on Solana, the remint must be skipped
    /// and the transaction marked Completed instead.
    ///
    /// Skipping this check would mean reminting tokens that were already
    /// successfully withdrawn — a direct double-credit to the user.
    #[tokio::test]
    async fn process_pending_remints_marks_completed_when_withdrawal_finalized() {
        let mut rpc_server = mockito::Server::new_async().await;
        let mut state = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _mock = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "jsonrpc": "2.0",
                    "result": {
                        "context": {"slot": 200},
                        "value": [{
                            "slot": 100,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    },
                    "id": 0
                }"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(99),
                withdrawal_nonce: Some(7),
                trace_id: Some("trace-99".to_string()),
            },
            remint_info: make_remint_info(99),
            signatures: vec![sig],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive Completed status");
        assert_eq!(update.transaction_id, 99);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig.to_string().as_str()),
            "counterpart_signature must be the finalized withdrawal sig"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "should send exactly one status update — no remint attempted"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry should be removed from queue after Completed"
        );
    }

    // ── execute_deferred_remint paths ───────────────────────────────

    /// When the finality check returns null for a withdrawal signature
    /// (transaction was dropped), `execute_deferred_remint` is called.
    /// If the remint itself also fails (RPC unreachable after the finality
    /// check mock is consumed), the combined error must be sent as ManualReview.
    #[tokio::test]
    async fn process_pending_remints_not_finalized_remint_fails_sends_manual_review() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let mut state = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        // Finality check: null means the tx was dropped — proceed to remint.
        let _mock = rpc_server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(77),
                withdrawal_nonce: Some(11),
                trace_id: Some("trace-77".to_string()),
            },
            remint_info: make_remint_info(77),
            signatures: vec![sig],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx.try_recv().expect("should receive ManualReview");
        assert_eq!(update.transaction_id, 77);
        assert_eq!(update.status, TransactionStatus::ManualReview);

        let err = update.error_message.as_deref().unwrap();
        assert!(
            err.contains("remint failed"),
            "error should mention remint failure: {err}"
        );
        assert!(
            err.contains("release_funds failed"),
            "error should include original withdrawal error: {err}"
        );

        assert!(state.pending_remints.is_empty());
    }

    /// A withdrawal that reached finality but failed on-chain (err field is set)
    /// is NOT a successful withdrawal — the user's funds never left the escrow.
    /// The operator must proceed to remint, not mark Completed.
    #[tokio::test]
    async fn process_pending_remints_finalized_with_onchain_error_proceeds_to_remint() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let mut state = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _mock = rpc_server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "jsonrpc": "2.0",
                    "result": {
                        "context": {"slot": 200},
                        "value": [{
                            "slot": 100,
                            "confirmations": null,
                            "err": {"InstructionError": [0, {"Custom": 1}]},
                            "status": {"Err": {"InstructionError": [0, {"Custom": 1}]}},
                            "confirmationStatus": "finalized"
                        }]
                    },
                    "id": 0
                }"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(88),
                withdrawal_nonce: Some(12),
                trace_id: Some("trace-88".to_string()),
            },
            remint_info: make_remint_info(88),
            signatures: vec![sig],
            original_error: "timeout".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive a status update");
        assert_ne!(
            update.status,
            TransactionStatus::Completed,
            "finalized-with-error must NOT produce Completed — funds never left escrow"
        );
        assert_eq!(update.transaction_id, 88);
    }

    /// When a withdrawal was retried and produced multiple signatures, one of the
    /// later retry signatures may reach finality. The operator must identify which
    /// specific signature finalized and record it as the counterpart_signature.
    #[tokio::test]
    async fn process_pending_remints_second_of_two_sigs_finalized_marks_completed() {
        let mut rpc_server = mockito::Server::new_async().await;
        let mut state = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig1 = Signature::new_unique(); // first attempt — dropped
        let sig2 = Signature::new_unique(); // retry — finalized

        let _mock = rpc_server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "jsonrpc": "2.0",
                    "result": {
                        "context": {"slot": 200},
                        "value": [
                            null,
                            {
                                "slot": 100,
                                "confirmations": null,
                                "err": null,
                                "status": {"Ok": null},
                                "confirmationStatus": "finalized"
                            }
                        ]
                    },
                    "id": 0
                }"#,
            )
            .create_async()
            .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(55),
                withdrawal_nonce: Some(6),
                trace_id: Some("trace-55".to_string()),
            },
            remint_info: make_remint_info(55),
            signatures: vec![sig1, sig2],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive Completed status");
        assert_eq!(update.transaction_id, 55);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(
            update.counterpart_signature.as_deref(),
            Some(sig2.to_string().as_str()),
            "counterpart_signature must be the finalized sig (sig2), not the dropped sig1"
        );
        assert!(
            state.pending_remints.is_empty(),
            "entry consumed after Completed"
        );
    }
}
