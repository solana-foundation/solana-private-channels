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
            types::{InstructionWithSigners, PendingRemint, PendingSig},
        },
        sign_and_send_transaction,
        utils::instruction_util::WithdrawalRemintInfo,
        ConfirmationResult, ExtraErrorCheckPolicy, MintToBuilder, MintToBuilderWithTxnId,
        RetryPolicy, RpcClientWithRetry, SignerUtil, TransactionStatusUpdate,
    },
    storage::TransactionStatus,
};
use chrono::Utc;
use solana_keychain::SolanaSigner;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Cap on total deferrals of a single pending remint. Covers both transient
/// RPC errors during the finality check AND liveness extensions when a stored
/// signature is still within blockhash validity. Past this cap we escalate
/// to ManualReview rather than loop indefinitely.
const MAX_FINALITY_CHECK_ATTEMPTS: u32 = 3;

/// Attempt to remint burned PrivateChannel tokens back to user after permanent withdrawal failure.
/// Builds a MintTo instruction with an idempotency memo (same pattern as deposits).
/// No sender-level retry; RPC-level retries may still occur via RpcClientWithRetry.
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
                "Remint idempotency lookup failed for transaction {}: {}; proceeding with send",
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

    let (signature, _) = sign_and_send_transaction(state.rpc_client.clone(), ix, RetryPolicy::None)
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

/// On-chain finality verdict for a set of broadcast release signatures. Shared
/// by the remint gate and recovery so both agree before mutating a withdrawal.
pub(crate) enum SigFinality {
    /// A signature finalized successfully — the release landed.
    Landed(Signature),
    /// A signature could still land; carries a reason for triage logs.
    Live(String),
    /// Every signature is finalized-failed or expired — safe to remint/demote.
    Dead,
    /// Could not classify (RPC/length error); callers must NOT treat as Dead.
    Uncertain(String),
}

/// Classify `sigs` against on-chain state (see `SigFinality` variants).
pub(crate) async fn classify_release_signatures(
    rpc: &RpcClientWithRetry,
    sigs: &[PendingSig],
) -> SigFinality {
    let flat: Vec<Signature> = sigs.iter().map(|p| p.signature).collect();

    let response = match rpc.get_signature_statuses_with_history(&flat).await {
        Ok(r) => r,
        Err(e) => {
            return SigFinality::Uncertain(format!("signature status RPC failed: {}", e));
        }
    };

    // RPC returns one status per signature in order; a length mismatch would
    // silently skip checks below, so treat it as uncertain.
    if response.value.len() != flat.len() {
        return SigFinality::Uncertain(format!(
            "RPC returned {} statuses for {} signatures",
            response.value.len(),
            flat.len()
        ));
    }

    // Any sig finalized successfully → the release landed.
    let finalized_success_index = response.value.iter().position(|signature_status| {
        signature_status.as_ref().is_some_and(|status| {
            status.satisfies_commitment(CommitmentConfig::finalized()) && status.err.is_none()
        })
    });
    if let Some(index) = finalized_success_index {
        return SigFinality::Landed(flat[index]);
    }

    // Fetch block height only for the lvbh check on null-status sigs, so a
    // transient getBlockHeight outage isn't treated as uncertainty otherwise.
    let current_height = if response.value.iter().any(|s| s.is_none()) {
        match rpc.get_block_height().await {
            Ok(h) => h,
            Err(e) => {
                return SigFinality::Uncertain(format!("block height RPC failed: {}", e));
            }
        }
    } else {
        // Unused: the null-status branch below only fires when some status is None.
        0
    };

    // Walk the sigs to see if any could still land (index-aligned with response.value).
    for (index, pending_sig) in sigs.iter().enumerate() {
        let signature_status = &response.value[index];

        if let Some(status) = signature_status.as_ref() {
            // Only `finalized` is definitive; success was handled above, so this is failure.
            if status.satisfies_commitment(CommitmentConfig::finalized()) {
                continue;
            }
            // confirmed/processed: in a block, will finalize regardless of blockhash validity.
            return SigFinality::Live(
                "signature is on-chain (confirmed/processed) and awaiting finalization".to_string(),
            );
        }

        // No status entry. lvbh is the only thing keeping it alive.
        if current_height > pending_sig.last_valid_block_height {
            continue;
        }
        return SigFinality::Live(format!(
            "signatures still within blockhash validity (current_height={})",
            current_height
        ));
    }

    // Every sig is finalized-failed or expired.
    SigFinality::Dead
}

/// Process matured entries in the deferred remint queue. For each matured
/// entry, classify the stored withdrawal signatures and pick one of:
///   1. Any sig finalized + success → report Completed.
///   2. Any sig still live (has a non-finalized status entry, OR has no
///      status entry but still within blockhash validity)
///      → defer with extended deadline.
///   3. Every sig finalized-failed, or null-status with expired blockhash
///      → remint.
///
/// RPC failures during classification fall through the same defer-or-escalate
/// path as case 2.
pub async fn process_pending_remints(
    state: &mut SenderState,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let now = Utc::now();

    // Drain the queue and split: due now vs. wait longer.
    let mut remaining = Vec::new();
    let mut matured = Vec::new();
    for entry in state.pending_remints.drain(..) {
        if entry.deadline <= now {
            matured.push(entry);
        } else {
            remaining.push(entry);
        }
    }

    // Each matured entry leaves the queue unless we push it back into `remaining`.
    for entry in matured {
        let nonce_label = entry
            .ctx
            .withdrawal_nonce
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string());

        // Classify the stored signatures against on-chain state.
        match classify_release_signatures(&state.rpc_client, &entry.signatures).await {
            // Case 1: a sig finalized successfully — the withdrawal landed.
            SigFinality::Landed(sig) => {
                send_completed(storage_tx, &entry, &nonce_label, sig).await;
            }
            // Case 2: could still land or unclassifiable → defer, don't remint.
            SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
                defer_or_escalate(
                    &mut remaining,
                    entry,
                    &nonce_label,
                    &reason,
                    &state.storage,
                    storage_tx,
                )
                .await;
            }
            // Case 3: every sig is finalized-failed or expired, safe to remint.
            SigFinality::Dead => {
                info!(
                    "All withdrawal signatures for nonce {} are finalized-failed or expired; attempting remint",
                    nonce_label
                );
                execute_deferred_remint(state, &entry, storage_tx).await;
            }
        }
    }

    // `remaining` = entries not yet due + entries `defer_or_escalate` re-queued.
    state.pending_remints = remaining;
}

/// Report a pending-remint entry as Completed because one of its withdrawal
/// signatures finalized on Solana.
async fn send_completed(
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    entry: &PendingRemint,
    nonce_label: &str,
    sig: Signature,
) {
    info!(
        "Withdrawal nonce {} finalized on-chain (sig: {}); skipping remint",
        nonce_label, sig
    );
    // Always Some in practice: entries are only queued for withdrawals, which
    // carry a DB id. A None here drops a finalized withdrawal with no DB trace,
    // so log it instead of returning silently.
    let Some(transaction_id) = entry.ctx.transaction_id else {
        error!(
            "send_completed for nonce {} has no transaction_id; finalized withdrawal (sig: {}) cannot be marked Completed",
            nonce_label, sig
        );
        return;
    };
    send_guaranteed(
        storage_tx,
        TransactionStatusUpdate {
            transaction_id,
            trace_id: entry.ctx.trace_id.clone(),
            status: TransactionStatus::Completed,
            counterpart_signature: Some(sig.to_string()),
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

/// Bump the entry's deferral counter and either re-queue with an extended
/// deadline or escalate to ManualReview when the cap is hit. Used by every
/// "couldn't classify this entry as ready-to-remint" branch.
async fn defer_or_escalate(
    remaining: &mut Vec<PendingRemint>,
    entry: PendingRemint,
    nonce_label: &str,
    reason: &str,
    storage: &crate::storage::Storage,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let attempt = entry.finality_check_attempts + 1;

    if attempt >= MAX_FINALITY_CHECK_ATTEMPTS {
        error!(
            "Pending remint for nonce {} reached attempt cap ({}); escalating to ManualReview: {}",
            nonce_label, attempt, reason
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
                        "{} | escalated to ManualReview after {} attempts: {}",
                        entry.original_error, attempt, reason
                    )),
                    remint_signature: None,
                    remint_attempted: false,
                },
                "transaction status update",
            )
            .await
            .ok();
        }
        return;
    }

    let new_deadline = Utc::now() + chrono::Duration::from_std(FINALITY_SAFETY_DELAY).unwrap();

    // Fail-closed: an inability to persist the bumped counter is itself
    // ambiguity. Escalate to ManualReview rather than continue deferring with
    // a counter we can't trust to survive a restart.
    if let Some(transaction_id) = entry.ctx.transaction_id {
        if let Err(persist_err) = storage
            .bump_pending_remint_finality_attempt(transaction_id, attempt as i32, new_deadline)
            .await
        {
            error!(
                "Pending remint for nonce {} counter persist failed, escalating to ManualReview: {}",
                nonce_label, persist_err
            );
            send_guaranteed(
                storage_tx,
                TransactionStatusUpdate {
                    transaction_id,
                    trace_id: entry.ctx.trace_id.clone(),
                    status: TransactionStatus::ManualReview,
                    counterpart_signature: None,
                    processed_at: Some(Utc::now()),
                    error_message: Some(format!(
                        "{} | counter persist failed at attempt {}: {}",
                        entry.original_error, attempt, persist_err
                    )),
                    remint_signature: None,
                    remint_attempted: false,
                },
                "transaction status update",
            )
            .await
            .ok();
            return;
        }
    }

    warn!(
        "Pending remint for nonce {} deferred (attempt {}/{}): {}",
        nonce_label, attempt, MAX_FINALITY_CHECK_ATTEMPTS, reason
    );
    remaining.push(PendingRemint {
        finality_check_attempts: attempt,
        deadline: new_deadline,
        ..entry
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::types::{
        PendingRemint, PendingSig, SenderState, TransactionContext, MAX_IN_FLIGHT,
    };
    use crate::operator::utils::instruction_util::WithdrawalRemintInfo;
    use crate::operator::MintCache;
    use crate::operator::RetryConfig;
    use crate::operator::RpcClientWithRetry;
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
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

    fn make_sender_state() -> (SenderState, MockStorage) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            crate::operator::RetryConfig::default(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let state = SenderState {
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
        };
        (state, mock)
    }

    /// Push a stub PendingRemint row into the mock so a subsequent
    /// `bump_pending_remint_finality_attempt(id, ...)` can find a row to update.
    /// Only the id and attempts fields matter for the bump path.
    fn seed_pending_remint_row(mock: &MockStorage, id: i64, attempts: i32) {
        use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
        let now = Utc::now();
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: Signature::new_unique().to_string(),
                trace_id: format!("trace-{id}"),
                slot: 0,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: 0,
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(id),
                status: TransactionStatus::PendingRemint,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: None,
                remint_last_valid_block_heights: None,
                pending_remint_deadline_at: Some(now),
                finality_check_attempts: attempts,
                recovery_requeue_attempts: 0,
            });
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

    fn make_sender_state_with_rpc(rpc_url: &str) -> (SenderState, MockStorage) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let rpc = Arc::new(crate::operator::RpcClientWithRetry::with_retry_config(
            rpc_url.to_string(),
            crate::operator::RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let state = SenderState {
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
        };
        (state, mock)
    }

    /// Register a mockito response for a specific Solana RPC method.
    async fn mock_rpc(server: &mut mockito::Server, method: &str, body: &str) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(format!(
                r#""method"\s*:\s*"{}""#,
                method
            )))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .create_async()
            .await
    }

    #[tokio::test]
    async fn process_pending_remints_requeues_on_rpc_error() {
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The defer path persists the bumped counter; the row must exist in the
        // mock for that write to succeed (otherwise the counter is held).
        seed_pending_remint_row(&mock, 20, 0);

        // Push a matured entry — RPC will fail (no real endpoint)
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
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

        // The bumped counter must also be persisted so it survives a restart.
        let persisted = mock
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 20)
            .map(|t| t.finality_check_attempts);
        assert_eq!(persisted, Some(1));
    }

    /// Fail-closed on persist failure: if the counter bump can't be written,
    /// the safety fuse is no longer trustworthy, so the entry must escalate
    /// to ManualReview rather than continue deferring on shaky state.
    #[tokio::test]
    async fn process_pending_remints_escalates_when_bump_persist_fails() {
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 30, 1);
        mock.set_should_fail("bump_pending_remint_finality_attempt", true);

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(30),
                withdrawal_nonce: Some(9),
                trace_id: Some("trace-30".to_string()),
            },
            remint_info: make_remint_info(30),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 1,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Entry dropped from in-memory queue, not re-queued.
        assert!(state.pending_remints.is_empty());

        // ManualReview update was sent with the persist error in the message.
        let update = storage_rx
            .try_recv()
            .expect("persist failure must produce a ManualReview update");
        assert_eq!(update.transaction_id, 30);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap();
        assert!(err.contains("counter persist failed"), "got: {err}");
        assert!(err.contains("release_funds failed"), "got: {err}");

        // DB row was not modified by the failed bump.
        let persisted = mock
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == 30)
            .map(|t| t.finality_check_attempts);
        assert_eq!(persisted, Some(1));
    }

    #[tokio::test]
    async fn process_pending_remints_manual_review_after_max_rpc_failures() {
        let (mut state, _mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // Push entry already at max attempts — next RPC failure triggers ManualReview
        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(20),
                withdrawal_nonce: Some(8),
                trace_id: Some("trace-20".to_string()),
            },
            remint_info: make_remint_info(20),
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
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
            err.contains("escalated to ManualReview"),
            "should mention ManualReview escalation: {err}"
        );
        assert!(
            err.contains("signature status RPC failed"),
            "should mention the underlying failure: {err}"
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
        let (mut state, mock) = make_sender_state();
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        // The matured entry (id 10) defers, which now persists the bump.
        seed_pending_remint_row(&mock, 10, 0);

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
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
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
            signatures: vec![PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            }],
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
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
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
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
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
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        // Finality check: null means the tx was dropped, proceed to remint.
        let _status_mock = rpc_server
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

        // Block height ahead of the stored lvbh (0) so every sig is treated as
        // expired and the gate falls through to Case 3 (remint).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(77),
                withdrawal_nonce: Some(11),
                trace_id: Some("trace-77".to_string()),
            },
            remint_info: make_remint_info(77),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
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
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
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
        .await;

        // Block height ahead of the stored lvbh (0) so the finalized-failed sig
        // counts as dead and the gate falls through to Case 3 (remint).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(88),
                withdrawal_nonce: Some(12),
                trace_id: Some("trace-88".to_string()),
            },
            remint_info: make_remint_info(88),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 0,
            }],
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

    /// Regression: when every stored signature already has a status entry, the
    /// liveness decision is already implied (finalized-failed) and no block
    /// height RPC is needed. A transient `getBlockHeight` outage in that
    /// scenario must NOT consume defer attempts.
    #[tokio::test]
    async fn process_pending_remints_skips_block_height_when_all_sigs_classifiable() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        // Finalized-failed: status present, finalized commitment, error set.
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
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
        .await;

        // Deliberately NOT mocking getBlockHeight: if the code reaches that
        // call mockito returns 501, the call errors, and defer_or_escalate
        // fires with "block height RPC failed" instead of execute_deferred_remint.

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(89),
                withdrawal_nonce: Some(13),
                trace_id: Some("trace-89".to_string()),
            },
            remint_info: make_remint_info(89),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive a status update");
        assert_eq!(update.transaction_id, 89);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("remint failed"),
            "must reach execute_deferred_remint; if this contains 'block height' \
             the pre-check regressed: {err}"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// When a withdrawal was retried and produced multiple signatures, one of the
    /// later retry signatures may reach finality. The operator must identify which
    /// specific signature finalized and record it as the counterpart_signature.
    #[tokio::test]
    async fn process_pending_remints_second_of_two_sigs_finalized_marks_completed() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
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
            signatures: vec![
                PendingSig {
                    signature: sig1,
                    last_valid_block_height: 0,
                },
                PendingSig {
                    signature: sig2,
                    last_valid_block_height: 0,
                },
            ],
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

    // ── classify_release_signatures (multi-sig) ─────────────────

    /// Bare RPC client (1 attempt, fast) for direct classifier tests.
    fn make_rpc(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: std::time::Duration::from_millis(1),
                max_delay: std::time::Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    /// Finalized success after an earlier finalized failure must win (full-list scan, not first-match).
    #[tokio::test]
    async fn classify_release_signatures_finalized_success_wins_over_earlier_failure() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        let failed = Signature::new_unique();
        let success = Signature::new_unique();

        // value[0] finalized-failed, value[1] finalized-success (positional).
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[
                {"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"},
                {"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}
            ]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: failed,
                last_valid_block_height: 0,
            },
            PendingSig {
                signature: success,
                last_valid_block_height: 0,
            },
        ];

        match classify_release_signatures(&rpc, &sigs).await {
            SigFinality::Landed(s) => assert_eq!(
                s, success,
                "must return the finalized-success sig, not the failed one"
            ),
            _ => panic!("expected Landed(success sig), got a different verdict"),
        }
    }

    /// Confirmed success behind a finalized failure must stay Live, never Dead.
    #[tokio::test]
    async fn classify_release_signatures_confirmed_success_after_failure_is_live_not_dead() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        // value[0] finalized-failed, value[1] confirmed-success (in a block,
        // will finalize).
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[
                {"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":1}]},"status":{"Err":{"InstructionError":[0,{"Custom":1}]}},"confirmationStatus":"finalized"},
                {"slot":100,"confirmations":10,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}
            ]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Live(_)
            ),
            "confirmed success behind a finalized failure must be Live, not Dead"
        );
    }

    /// A still-valid null after an expired null must be Live: nulls are walked fully, not cut at the first.
    #[tokio::test]
    async fn classify_release_signatures_live_null_after_expired_null_is_live() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null,null]},"id":0}"#,
        )
        .await;
        // current_height 1000: sig[0] lvbh 100 expired, sig[1] lvbh 2000 live.
        let _height = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 100,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 2000,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Live(_)
            ),
            "a still-valid null after an expired null must be Live, not Dead"
        );
    }

    /// A truncated status list (fewer statuses than sigs) must be Uncertain, never read as "missing = dead".
    #[tokio::test]
    async fn classify_release_signatures_status_length_mismatch_is_uncertain() {
        let mut rpc_server = mockito::Server::new_async().await;
        let rpc = make_rpc(&rpc_server.url());

        // Two sigs requested, one status returned.
        let _status = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        let sigs = vec![
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
            PendingSig {
                signature: Signature::new_unique(),
                last_valid_block_height: 0,
            },
        ];

        assert!(
            matches!(
                classify_release_signatures(&rpc, &sigs).await,
                SigFinality::Uncertain(_)
            ),
            "length mismatch must be Uncertain"
        );
    }

    // ── liveness gate paths ────────────────────────────────────────────

    /// Sig has no on-chain record AND its blockhash is past validity. Dead.
    /// The gate must proceed to remint.
    #[tokio::test]
    async fn process_pending_remints_all_sigs_expired_proceeds_to_remint() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // current_height (1000) > lvbh (100): sig is expired.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(100),
                withdrawal_nonce: Some(20),
                trace_id: Some("trace-100".to_string()),
            },
            remint_info: make_remint_info(100),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        // Reaching Case 3 triggers execute_deferred_remint, whose RPC calls
        // have no matching mocks; the remint fails and writes ManualReview
        // with "remint failed" in the error message.
        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview from execute_deferred_remint");
        assert_eq!(update.transaction_id, 100);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert!(
            update
                .error_message
                .as_deref()
                .unwrap_or("")
                .contains("remint failed"),
            "reaching Case 3 means execute_deferred_remint ran"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// Sig has no on-chain record but its blockhash is still within validity.
    /// Could still land. The gate must defer (no remint, no status update)
    /// and bump the counter.
    #[tokio::test]
    async fn process_pending_remints_one_sig_still_live_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 101, 0);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // current_height (50) <= lvbh (1000): sig still within validity.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":50,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(101),
                withdrawal_nonce: Some(21),
                trace_id: Some("trace-101".to_string()),
            },
            remint_info: make_remint_info(101),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: row must stay PendingRemint while the broadcast could still land"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(
            state.pending_remints[0].finality_check_attempts, 1,
            "counter must be bumped after a liveness deferral"
        );
    }

    /// Entry already at the deferral cap on the liveness branch must escalate
    /// to ManualReview, and the error message must identify the cause as the
    /// liveness check (not an RPC failure).
    #[tokio::test]
    async fn process_pending_remints_live_sig_at_cap_escalates_to_manual_review() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, _mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // Sig still live: lvbh (1000) > current_height (50).
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":50,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(102),
                withdrawal_nonce: Some(22),
                trace_id: Some("trace-102".to_string()),
            },
            remint_info: make_remint_info(102),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 1000,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 2, // one more attempt hits the cap
        });

        process_pending_remints(&mut state, &storage_tx).await;

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview at the cap");
        assert_eq!(update.transaction_id, 102);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("signatures still within blockhash validity"),
            "escalation message must identify the liveness cause: {err}"
        );
        assert!(state.pending_remints.is_empty());
    }

    /// getBlockHeight RPC fails. The gate cannot evaluate liveness, so it
    /// must defer (not remint blindly). Same shape as the existing
    /// sig-status RPC failure handling.
    #[tokio::test]
    async fn process_pending_remints_block_height_rpc_failure_defers() {
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 103, 0);

        let sig = Signature::new_unique();

        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":0}"#,
        )
        .await;

        // getBlockHeight returns an RPC-level error.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","error":{"code":-32600,"message":"server error"},"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(103),
                withdrawal_nonce: Some(23),
                trace_id: Some("trace-103".to_string()),
            },
            remint_info: make_remint_info(103),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: RPC failure under cap just defers the entry"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }

    /// Sig is already on-chain at `confirmed` (in a block, awaiting
    /// finalization) but its blockhash has expired. The tx will finalize
    /// regardless of blockhash validity, so the gate must defer rather than
    /// remint. Reminting here would cause a double-payout once the tx
    /// finalizes a few slots later.
    #[tokio::test]
    async fn process_pending_remints_confirmed_not_finalized_past_lvbh_defers() {
        ensure_test_signer();
        let mut rpc_server = mockito::Server::new_async().await;
        let (mut state, mock) = make_sender_state_with_rpc(&rpc_server.url());
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        seed_pending_remint_row(&mock, 105, 0);

        let sig = Signature::new_unique();

        // Status: confirmed (in a block) but not yet finalized, no error.
        let _status_mock = mock_rpc(
            &mut rpc_server,
            "getSignatureStatuses",
            r#"{
                "jsonrpc": "2.0",
                "result": {
                    "context": {"slot": 200},
                    "value": [{
                        "slot": 100,
                        "confirmations": 1,
                        "err": null,
                        "status": {"Ok": null},
                        "confirmationStatus": "confirmed"
                    }]
                },
                "id": 0
            }"#,
        )
        .await;

        // current_height (1000) > lvbh (100): blockhash validity has passed.
        let _block_height_mock = mock_rpc(
            &mut rpc_server,
            "getBlockHeight",
            r#"{"jsonrpc":"2.0","result":1000,"id":0}"#,
        )
        .await;

        state.pending_remints.push(PendingRemint {
            ctx: TransactionContext {
                transaction_id: Some(105),
                withdrawal_nonce: Some(25),
                trace_id: Some("trace-105".to_string()),
            },
            remint_info: make_remint_info(105),
            signatures: vec![PendingSig {
                signature: sig,
                last_valid_block_height: 100,
            }],
            original_error: "release_funds failed".to_string(),
            deadline: Utc::now() - chrono::Duration::seconds(1),
            finality_check_attempts: 0,
        });

        process_pending_remints(&mut state, &storage_tx).await;

        assert!(
            storage_rx.try_recv().is_err(),
            "no status update: a confirmed-but-not-finalized sig must defer the remint"
        );
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].finality_check_attempts, 1);
    }
}
