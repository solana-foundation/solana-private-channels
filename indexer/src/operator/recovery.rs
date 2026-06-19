//! Recovers rows stuck in `Processing` after an operator crash.

use crate::channel_utils::send_guaranteed;
use crate::config::ProgramType;
use crate::error::OperatorError;
use crate::metrics::OPERATOR_STALE_PROCESSING_RECOVERED;
use crate::operator::sender::types::PendingSig;
use crate::operator::sender::{classify_release_signatures, SigFinality};
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use crate::operator::TransactionStatusUpdate;
use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
use crate::storage::common::storage::Storage;
use chrono::{DateTime, Utc};
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// How often the recovery loop runs.
pub(crate) const RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

/// Age cutoff for "stuck"; must exceed the sender's 30s drain + retries.
pub(crate) const STALE_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Per-tick batch cap; leftovers are picked up next tick.
pub(crate) const RECOVERY_BATCH_LIMIT: i64 = 100;

/// Max durable Demote requeues before a stuck row is quarantined (paged).
const MAX_RECOVERY_REQUEUE_ATTEMPTS: i32 = 3;

/// Deposit recovery outcome. Uncertainty must NOT demote (double-mint risk); an
/// in-flight signature leaves the row Processing for the next sweep.
enum DepositOutcome {
    Landed { signature: String },
    NotLanded,
    Live { reason: String },
    Ambiguous { reason: String },
}

/// Withdrawal recovery outcome. We verify on-chain finality before demoting so
/// a release that already landed is never re-sent.
enum WithdrawalAction {
    /// Release finalized on-chain → mark Completed with that signature.
    Complete { signature: String },
    /// Every recorded signature is dead → safe to requeue.
    Demote,
    /// A recorded signature could still land → re-evaluate next sweep.
    LeaveProcessing { reason: String },
    /// Uncertain (no signatures, or RPC could not classify) → page.
    Quarantine { reason: String },
}

/// Unified action for the storage router.
enum RecoveryAction {
    Complete {
        signature: String,
    },
    Demote,
    /// Leave the row in Processing this tick (no CAS write).
    NoAction {
        reason: String,
    },
    Quarantine {
        reason: String,
    },
}

/// Recovery loop. First tick runs on boot (the prime crash-recovery moment).
pub async fn run_recovery_worker(
    storage: Arc<Storage>,
    rpc_client: Arc<RpcClientWithRetry>,
    program_type: ProgramType,
    storage_tx: mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting recovery worker");
    let mut interval = tokio::time::interval(RECOVERY_INTERVAL);
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Recovery worker received cancellation, exiting");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = recover_once(
                    &storage,
                    &rpc_client,
                    program_type,
                    &storage_tx,
                    &cancellation_token,
                    STALE_THRESHOLD,
                )
                .await
                {
                    // Per-row writes are independent; retry next tick.
                    warn!("Recovery tick failed: {}", e);
                }
            }
        }
    }
    Ok(())
}

async fn recover_once(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    threshold: Duration,
) -> Result<(), OperatorError> {
    // Best-effort GC of release signatures whose parent is no longer Processing;
    // a failure here must not block recovery.
    match storage.gc_stale_release_signatures().await {
        Ok(removed) => debug!(removed, "Recovery GC'd stale release signatures"),
        Err(e) => warn!("Recovery release-signature GC failed: {}", e),
    }

    let stale = storage
        .get_stale_processing_transactions(threshold, RECOVERY_BATCH_LIMIT)
        .await?;

    if !stale.is_empty() {
        debug!(
            count = stale.len(),
            "Recovery sweep found stale Processing rows"
        );
    }

    for row in stale {
        // Cooperate with shutdown between rows so long batches exit cleanly.
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining rows deferred");
            return Ok(());
        }
        // Capture `updated_at` before the RPC so the write below CAS-checks it.
        let captured = row.updated_at;
        let action = decide_action(&row, storage, rpc_client).await;
        route_outcome(storage, &row, captured, action, program_type, storage_tx).await;
    }

    // Rescue parked withdrawals orphaned by a restart. A live sender unparks
    // these itself, so anything stale here lost its in-memory driver. Parked
    // rows were never sent on-chain, so requeue them without verifying finality.
    let stale_parked = storage
        .get_stale_parked_transactions(threshold, RECOVERY_BATCH_LIMIT)
        .await?;
    for row in stale_parked {
        if cancellation_token.is_cancelled() {
            info!("Recovery sweep cancelled; remaining parked rows deferred");
            return Ok(());
        }
        requeue_parked(storage, &row, program_type).await;
    }
    Ok(())
}

async fn decide_action(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> RecoveryAction {
    let action = match row.transaction_type {
        TransactionType::Deposit => match check_deposit(row, storage, rpc_client).await {
            DepositOutcome::Landed { signature } => RecoveryAction::Complete { signature },
            DepositOutcome::NotLanded => RecoveryAction::Demote,
            DepositOutcome::Live { reason } => RecoveryAction::NoAction { reason },
            DepositOutcome::Ambiguous { reason } => RecoveryAction::Quarantine { reason },
        },
        TransactionType::Withdrawal => match check_withdrawal(row, storage, rpc_client).await {
            WithdrawalAction::Complete { signature } => RecoveryAction::Complete { signature },
            WithdrawalAction::Demote => RecoveryAction::Demote,
            WithdrawalAction::LeaveProcessing { reason } => RecoveryAction::NoAction { reason },
            WithdrawalAction::Quarantine { reason } => RecoveryAction::Quarantine { reason },
        },
    };
    // Cap recovery requeue attempts. Rows that fail to make progress after
    // MAX_RECOVERY_REQUEUE_ATTEMPTS are quarantined (and paged) rather than
    // looping between Pending and Processing indefinitely.
    if matches!(action, RecoveryAction::Demote)
        && row.recovery_requeue_attempts >= MAX_RECOVERY_REQUEUE_ATTEMPTS
    {
        return RecoveryAction::Quarantine {
            reason: format!(
                "exceeded {MAX_RECOVERY_REQUEUE_ATTEMPTS} recovery requeues without progress"
            ),
        };
    }
    action
}

/// Decide a stuck Processing deposit's fate from its persisted broadcast signatures.
/// Like `check_withdrawal`, but with no signatures a deposit Demotes (safe re-mint)
/// where a withdrawal Quarantines: the pre-broadcast persist makes "no signature" mean
/// "never broadcast", so re-minting cannot double-mint, and quarantining every such row
/// would flood manual review at deposit volume.
async fn check_deposit(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> DepositOutcome {
    let pending = match load_pending_sigs(storage, row.id).await {
        Ok(p) => p,
        Err(reason) => {
            return DepositOutcome::Ambiguous {
                reason: format!("could not verify mint landed ({reason})"),
            }
        }
    };

    if pending.is_empty() {
        return DepositOutcome::NotLanded;
    }

    match classify_release_signatures(rpc_client, &pending).await {
        SigFinality::Landed(sig) => DepositOutcome::Landed {
            signature: sig.to_string(),
        },
        SigFinality::Dead => DepositOutcome::NotLanded,
        // Still in flight; re-check next sweep rather than demote or complete.
        SigFinality::Live(reason) => DepositOutcome::Live { reason },
        // Never demote on uncertainty — risks a double-mint on re-pickup.
        SigFinality::Uncertain(reason) => DepositOutcome::Ambiguous {
            reason: format!("could not verify mint landed ({reason})"),
        },
    }
}

/// Decide a stuck Processing withdrawal's fate by verifying on-chain finality
/// of the persisted release signatures; never demote one whose release landed.
async fn check_withdrawal(
    row: &DbTransaction,
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
) -> WithdrawalAction {
    if row.withdrawal_nonce.is_none() {
        return WithdrawalAction::Quarantine {
            reason: "withdrawal row missing nonce".to_string(),
        };
    }

    let pending = match load_pending_sigs(storage, row.id).await {
        Ok(p) => p,
        Err(reason) => return WithdrawalAction::Quarantine { reason },
    };

    // No recorded signatures → can't verify a release landed; demoting risks a
    // double-payout, so page instead.
    if pending.is_empty() {
        return WithdrawalAction::Quarantine {
            reason: "no broadcast signatures recorded; cannot verify release landed".to_string(),
        };
    }

    match classify_release_signatures(rpc_client, &pending).await {
        SigFinality::Landed(sig) => WithdrawalAction::Complete {
            signature: sig.to_string(),
        },
        SigFinality::Dead => WithdrawalAction::Demote,
        SigFinality::Live(reason) => WithdrawalAction::LeaveProcessing { reason },
        SigFinality::Uncertain(reason) => WithdrawalAction::Quarantine {
            reason: format!(
                "could not verify release landed ({reason}); signatures: {}",
                pending
                    .iter()
                    .map(|p| p.signature.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        },
    }
}

/// Load and parse a row's persisted broadcast signatures into `PendingSig`s for the
/// finality classifier. Shared by deposit and withdrawal recovery. A read error or a
/// malformed stored signature returns a quarantine reason (uncertainty, never "dead"),
/// so callers never demote a row whose signatures could not be read or parsed.
async fn load_pending_sigs(storage: &Storage, id: i64) -> Result<Vec<PendingSig>, String> {
    let stored = storage
        .get_release_signatures(id)
        .await
        .map_err(|e| format!("release signature lookup failed: {e}"))?;

    let mut pending = Vec::with_capacity(stored.len());
    for (sig_str, lvbh) in &stored {
        let signature = Signature::from_str(sig_str)
            .map_err(|e| format!("malformed stored release signature {sig_str}: {e}"))?;
        pending.push(PendingSig {
            signature,
            last_valid_block_height: *lvbh as u64,
        });
    }
    Ok(pending)
}

fn pt_label(program_type: ProgramType) -> &'static str {
    match program_type {
        ProgramType::Escrow => "escrow",
        ProgramType::Withdraw => "withdraw",
    }
}

/// Requeue an orphaned `Parked` row to `Pending` so the processor rebuilds it.
async fn requeue_parked(storage: &Storage, row: &DbTransaction, program_type: ProgramType) {
    match storage.try_requeue_parked(row.id, row.updated_at).await {
        Ok(true) => {
            info!(
                transaction_id = row.id,
                "Recovery requeued orphaned Parked → Pending"
            );
            OPERATOR_STALE_PROCESSING_RECOVERED
                .with_label_values(&[pt_label(program_type), "requeued_parked", "withdrawal"])
                .inc();
        }
        Ok(false) => debug!(
            id = row.id,
            "parked requeue skipped — another writer touched the row first"
        ),
        Err(e) => warn!(id = row.id, "parked requeue write error: {}", e),
    }
}

async fn route_outcome(
    storage: &Storage,
    row: &DbTransaction,
    captured_updated_at: DateTime<Utc>,
    action: RecoveryAction,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) {
    let pt_label = pt_label(program_type);
    let type_label = match row.transaction_type {
        TransactionType::Deposit => "deposit",
        TransactionType::Withdrawal => "withdrawal",
    };

    match action {
        RecoveryAction::Complete { signature } => {
            match storage
                .try_complete_processing(row.id, captured_updated_at, Some(signature.clone()))
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        signature, "Recovery promoted stale Processing → Completed"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "completed", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::Demote => {
            // Trigger bumps `updated_at`; the next sweep skips it.
            match storage
                .try_requeue_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    info!(
                        transaction_id = row.id,
                        "Recovery demoted stale Processing → Pending"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "requeued", type_label])
                        .inc();
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
        RecoveryAction::NoAction { reason } => {
            // Release could still land; leave Processing untouched (no CAS write).
            debug!(
                transaction_id = row.id,
                reason = %reason,
                "Recovery left stale Processing row untouched — broadcast may still land"
            );
        }
        RecoveryAction::Quarantine { reason } => {
            // Noisy by design — page on uncertainty, never silently demote.
            match storage
                .try_quarantine_processing(row.id, captured_updated_at)
                .await
            {
                Ok(true) => {
                    warn!(
                        transaction_id = row.id,
                        reason = %reason,
                        "Recovery quarantined stale Processing → ManualReview"
                    );
                    OPERATOR_STALE_PROCESSING_RECOVERED
                        .with_label_values(&[pt_label, "quarantined", type_label])
                        .inc();
                    // Fire the existing webhook + alert log (see sender/state.rs).
                    let update = TransactionStatusUpdate {
                        transaction_id: row.id,
                        trace_id: Some(row.trace_id.clone()),
                        status: TransactionStatus::ManualReview,
                        counterpart_signature: None,
                        processed_at: Some(Utc::now()),
                        error_message: Some(reason),
                        remint_signature: None,
                        remint_attempted: false,
                    };
                    // Closed channel = on-call alert lost; surface it loudly.
                    if let Err(e) =
                        send_guaranteed(storage_tx, update, "recovery manual review").await
                    {
                        warn!(
                            transaction_id = row.id,
                            "Recovery quarantined row but failed to deliver alert webhook: {}", e
                        );
                    }
                }
                Ok(false) => {
                    debug!(
                        id = row.id,
                        "recovery skipped — another writer touched the row first"
                    );
                }
                Err(e) => warn!(id = row.id, "recovery write error: {}", e),
            }
        }
    }
}

/// Synchronous boot pre-flight reconcile: repeatedly run `recover_once` with a
/// `Duration::ZERO` threshold (so even a fresh crash row is reconciled) until no
/// `Processing` rows remain, bounded by `max_passes`. A withdraw operator is
/// single-active (SMT nonce ordering forbids a second sender), so at boot there
/// is no live sibling whose not-yet-stale work this could disrupt. Exhausting
/// `max_passes` with rows still `Processing` returns `Ok`: the caller's
/// `validate_smt_root` is the terminal gate that refuses to start on a real mismatch.
pub async fn boot_reconcile_processing(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    program_type: ProgramType,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    cancellation_token: &CancellationToken,
    max_passes: u32,
) -> Result<(), OperatorError> {
    for pass in 0..max_passes {
        recover_once(
            storage,
            rpc_client,
            program_type,
            storage_tx,
            cancellation_token,
            Duration::ZERO,
        )
        .await?;

        let remaining = storage
            .get_stale_processing_transactions(Duration::ZERO, RECOVERY_BATCH_LIMIT)
            .await?;
        if remaining.is_empty() {
            return Ok(());
        }
        debug!(
            pass,
            remaining = remaining.len(),
            "Boot reconcile still has Processing rows; iterating"
        );
    }
    warn!(
        max_passes,
        "Boot reconcile exhausted its pass budget with Processing rows remaining"
    );
    Ok(())
}

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only entry to drive a single recovery tick deterministically.
    use super::*;

    pub async fn run_recovery_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        program_type: ProgramType,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Fresh, never-cancelled token; tests run to completion. Uses the periodic
        // worker's STALE_THRESHOLD; the ZERO boot threshold is exercised by calling
        // recover_once directly.
        let token = CancellationToken::new();
        recover_once(
            storage,
            rpc_client,
            program_type,
            storage_tx,
            &token,
            STALE_THRESHOLD,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::storage::mock::MockStorage;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;

    fn make_deposit_row(id: i64) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: format!("sig-{id}"),
            instruction_index: 0,
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: Pubkey::new_unique().to_string(),
            recipient: Pubkey::new_unique().to_string(),
            mint: Pubkey::new_unique().to_string(),
            amount: TokenAmount(1_000),
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Processing,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            inner_index: None,
            landed_remint_signature: None,
        }
    }

    fn make_withdrawal_row(id: i64, nonce: Option<i64>) -> DbTransaction {
        let mut row = make_deposit_row(id);
        row.transaction_type = TransactionType::Withdrawal;
        row.withdrawal_nonce = nonce;
        row
    }

    fn make_rpc_client(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    // ── check_deposit outcome matrix (signature-driven) ──────────────

    fn mock_null_status(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create()
    }

    fn mock_block_height(server: &mut mockito::ServerGuard, height: u64) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(format!(r#"{{"jsonrpc":"2.0","result":{height},"id":1}}"#))
            .create()
    }

    /// The keystone divergence from withdrawal: a deposit with no persisted signature is
    /// provably never broadcast (pre-broadcast persist), so it Demotes for a safe re-mint
    /// rather than Quarantining. No RPC is consulted.
    #[tokio::test]
    async fn deposit_no_sigs_demotes() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");
        let row = make_deposit_row(1);
        let outcome = check_deposit(&row, &storage, &client).await;
        assert!(
            matches!(outcome, DepositOutcome::NotLanded),
            "empty sigs must map to NotLanded (Demote), not Ambiguous/Quarantine"
        );
        // Same state on the withdrawal side Quarantines; assert the difference.
        let wrow = make_withdrawal_row(2, Some(42));
        let waction = check_withdrawal(&wrow, &storage, &client).await;
        assert!(
            matches!(waction, WithdrawalAction::Quarantine { .. }),
            "withdrawal with no sigs must Quarantine - the deliberate deposit divergence"
        );
    }

    /// A finalized-success signature returns Landed and is never re-minted.
    #[tokio::test]
    async fn deposit_landed_sig_completes_without_remint() {
        let landed_sig = Signature::new_unique();
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        match check_deposit(&row, &storage, &client).await {
            DepositOutcome::Landed { signature } => assert_eq!(signature, landed_sig.to_string()),
            _ => panic!("expected Landed"),
        }
    }

    /// A null-status sig past blockhash validity is dead: NotLanded, safe to re-mint.
    #[tokio::test]
    async fn deposit_dead_sigs_demote() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_null_status(&mut server);
        // current_height (1000) > lvbh (100) means expired/dead.
        let _height = mock_block_height(&mut server, 1000);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &client).await,
                DepositOutcome::NotLanded
            ),
            "dead sigs map to NotLanded (Demote)"
        );
    }

    /// A sig still within blockhash validity is Live: leave Processing this sweep.
    #[tokio::test]
    async fn deposit_live_sig_leaves_processing() {
        let mut server = mockito::Server::new_async().await;
        let _status = mock_null_status(&mut server);
        // current_height (50) <= lvbh (1000) means still live.
        let _height = mock_block_height(&mut server, 50);

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        assert!(
            matches!(
                check_deposit(&row, &storage, &client).await,
                DepositOutcome::Live { .. }
            ),
            "a still-live sig must leave the row Processing, not demote"
        );
    }

    /// An RPC failure during classification is uncertain: Ambiguous, never demote.
    #[tokio::test]
    async fn deposit_rpc_uncertain_quarantines() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();

        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        match check_deposit(&row, &storage, &client).await {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.contains("could not verify mint landed"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous"),
        }
    }

    /// A malformed stored signature (via the shared `load_pending_sigs`) is uncertainty,
    /// never read as "dead"; it must Quarantine rather than demote.
    #[tokio::test]
    async fn deposit_malformed_stored_sig_quarantines() {
        let mock = MockStorage::new();
        let row = make_deposit_row(1);
        mock.insert_release_signature(row.id, "not-a-valid-base58-signature".to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");

        match check_deposit(&row, &storage, &client).await {
            DepositOutcome::Ambiguous { reason } => {
                assert!(
                    reason.contains("malformed stored release signature"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Ambiguous on malformed signature"),
        }
    }

    // ── check_withdrawal outcome matrix ───────────────────────────────

    /// Missing nonce → quarantine before any RPC/storage read.
    #[tokio::test]
    async fn check_withdrawal_quarantines_when_nonce_missing() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, None);
        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(reason.contains("withdrawal row missing nonce"));
            }
            _ => panic!("expected Quarantine"),
        }
    }

    /// No recorded signatures → quarantine, not demote (double-payout risk).
    #[tokio::test]
    async fn check_withdrawal_quarantines_when_no_signatures_recorded() {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client("http://localhost:1");
        let row = make_withdrawal_row(1, Some(42));
        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(
                    reason.contains("no broadcast signatures recorded"),
                    "reason: {reason}"
                );
            }
            _ => panic!("expected Quarantine"),
        }
    }

    /// Null-status signature past blockhash validity is dead → demote.
    #[tokio::test]
    async fn check_withdrawal_demotes_when_signature_dead() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        let _height = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":1000,"id":1}"#)
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        // current_height (1000) > lvbh (100) means expired/dead.
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(&row, &storage, &client).await;
        assert!(
            matches!(action, WithdrawalAction::Demote),
            "expected Demote"
        );
    }

    /// Finalized-success signature → Complete with that sig.
    #[tokio::test]
    async fn check_withdrawal_completes_when_signature_landed() {
        let landed_sig = Signature::new_unique();
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Complete { signature } => {
                assert_eq!(signature, landed_sig.to_string());
            }
            _ => panic!("expected Complete"),
        }
    }

    /// Signature still within blockhash validity → leave in Processing.
    #[tokio::test]
    async fn check_withdrawal_leaves_processing_when_signature_live() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[null]},"id":1}"#,
            )
            .create();
        let _height = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getBlockHeight""#.into(),
            ))
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","result":50,"id":1}"#)
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        // current_height (50) <= lvbh (1000) means still live.
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 1000)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(&row, &storage, &client).await;
        assert!(
            matches!(action, WithdrawalAction::LeaveProcessing { .. }),
            "expected LeaveProcessing"
        );
    }

    /// RPC failure during classification is uncertainty → quarantine, never demote.
    #[tokio::test]
    async fn check_withdrawal_quarantines_on_rpc_uncertainty() {
        let mut server = mockito::Server::new_async().await;
        let _status = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("internal server error")
            .create();

        let mock = MockStorage::new();
        let row = make_withdrawal_row(1, Some(42));
        let recorded_sig = Signature::new_unique().to_string();
        mock.insert_release_signature(row.id, recorded_sig.clone(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock);
        let client = make_rpc_client(&server.url());

        let action = check_withdrawal(&row, &storage, &client).await;
        match action {
            WithdrawalAction::Quarantine { reason } => {
                assert!(
                    reason.contains("could not verify release landed"),
                    "reason: {reason}"
                );
                assert!(
                    reason.contains(&recorded_sig),
                    "sig should be in reason: {reason}"
                );
            }
            _ => panic!("expected Quarantine"),
        }
    }

    // ── route_outcome calls the right storage helper per variant ─────

    async fn seed_processing_row(mock: &MockStorage, row: DbTransaction) -> DateTime<Utc> {
        let captured = row.updated_at;
        mock.pending_transactions.lock().unwrap().push(row);
        captured
    }

    #[tokio::test]
    async fn route_outcome_complete_writes_completed() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(1);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Complete {
                signature: "sig-abc".to_string(),
            },
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert_eq!(after[0].counterpart_signature.as_deref(), Some("sig-abc"));
    }

    #[tokio::test]
    async fn route_outcome_demote_writes_pending() {
        let mock = MockStorage::new();
        let mut row = make_deposit_row(2);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Pending);
    }

    #[tokio::test]
    async fn route_outcome_quarantine_writes_manual_review_and_sends_alert() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(3, None);
        row.status = TransactionStatus::Processing;
        let captured = seed_processing_row(&mock, row.clone()).await;
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        route_outcome(
            &storage,
            &row,
            captured,
            RecoveryAction::Quarantine {
                reason: "withdrawal row missing nonce".to_string(),
            },
            ProgramType::Withdraw,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::ManualReview);
        drop(after);

        let update = storage_rx
            .try_recv()
            .expect("expected manual review update");
        assert_eq!(update.transaction_id, row.id);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        assert_eq!(
            update.error_message.as_deref(),
            Some("withdrawal row missing nonce")
        );
    }

    // ── parked sweep ─────────────────────────────────────────────────

    /// A stale Parked row (orphaned by a restart) is requeued to Pending so the
    /// processor rebuilds it. No signature lookup, no alert webhook, and the
    /// requeue cap counter is left untouched.
    #[tokio::test]
    async fn stale_parked_row_requeued_to_pending_without_alert() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(70, Some(3));
        row.status = TransactionStatus::Parked;
        // Backdate past STALE_THRESHOLD so the parked sweep selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        // Parked rows need no on-chain check, so the RPC client is never called.
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Pending,
            "stale parked → requeued"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 0,
            "parked requeue must not bump the cap counter"
        );
        drop(after);
        assert!(
            storage_rx.try_recv().is_err(),
            "parked requeue must not send an alert"
        );
    }

    /// A fresh Parked row (a live sender still owns it) is left untouched.
    #[tokio::test]
    async fn fresh_parked_row_left_untouched() {
        let mock = MockStorage::new();
        let mut row = make_withdrawal_row(71, Some(3));
        row.status = TransactionStatus::Parked;
        row.updated_at = Utc::now();
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Withdraw, &storage_tx)
            .await
            .unwrap();

        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0].status,
            TransactionStatus::Parked,
            "fresh parked row must be left alone"
        );
    }

    // ── recovery requeue cap ─────────────────────────────────────────

    /// Under the cap: a NotLanded deposit is requeued AND its durable
    /// counter increments, so the next stale sweep sees the higher count.
    #[tokio::test]
    async fn requeue_under_cap_increments_counter_and_requeues() {
        // No persisted signatures: NotLanded, so Demote, with no RPC consulted.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(50);
        row.status = TransactionStatus::Processing;
        row.recovery_requeue_attempts = 0;
        // Backdate past STALE_THRESHOLD so the sweep actually selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, _rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Pending,
            "under cap → requeued"
        );
        assert_eq!(
            after[0].recovery_requeue_attempts, 1,
            "durable requeue counter must increment on demote"
        );
    }

    /// At the cap: a row that would otherwise Demote is quarantined to
    /// ManualReview and the alert webhook is sent.
    #[tokio::test]
    async fn requeue_at_cap_quarantines_and_alerts() {
        // No persisted signatures would Demote, but the cap converts it to Quarantine.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(51);
        row.status = TransactionStatus::Processing;
        // At the cap (MAX requeues already done) → the next demote is blocked.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        // Backdate past STALE_THRESHOLD so the sweep actually selects it.
        row.updated_at = Utc::now() - chrono::Duration::minutes(10);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client("http://localhost:1");
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        test_hooks::run_recovery_once(&storage, &client, ProgramType::Escrow, &storage_tx)
            .await
            .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::ManualReview,
            "at cap → quarantined, not requeued"
        );
        drop(after);

        let update = storage_rx
            .try_recv()
            .expect("cap must fire the manual-review alert webhook");
        assert_eq!(update.transaction_id, 51);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let reason = update.error_message.as_deref().unwrap_or("");
        assert!(
            reason.contains("recovery requeues")
                && reason.contains(&MAX_RECOVERY_REQUEUE_ATTEMPTS.to_string()),
            "alert must name the requeue cap and its count: {reason}"
        );
    }

    /// `decide_action` caps the Demote arm uniformly regardless of type. Uses a deposit
    /// row with no persisted signatures (NotLanded, so Demote, no RPC).
    #[tokio::test]
    async fn decide_action_caps_demote_at_threshold() {
        let storage = Storage::Mock(MockStorage::new());
        let client = make_rpc_client("http://localhost:1");

        let mut row = make_deposit_row(52);
        // One below the cap still demotes (requeues) - pins the off-by-one boundary.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS - 1;
        let below = decide_action(&row, &storage, &client).await;
        assert!(
            matches!(below, RecoveryAction::Demote),
            "one below the cap must still Demote (requeue)"
        );
        // At the cap, the demote is converted to Quarantine.
        row.recovery_requeue_attempts = MAX_RECOVERY_REQUEUE_ATTEMPTS;
        let at_cap = decide_action(&row, &storage, &client).await;
        assert!(
            matches!(at_cap, RecoveryAction::Quarantine { .. }),
            "demote at the cap must become Quarantine"
        );
    }

    #[tokio::test]
    async fn route_outcome_demote_noops_when_captured_updated_at_stale() {
        // The `updated_at` check fails → no metric increment, row unchanged.
        let mock = MockStorage::new();
        let mut row = make_deposit_row(4);
        row.status = TransactionStatus::Processing;
        mock.pending_transactions.lock().unwrap().push(row.clone());
        let storage = Storage::Mock(mock.clone());
        let (storage_tx, _rx) = mpsc::channel(8);

        // Captured timestamp that does NOT match the seeded row's updated_at.
        let stale = row.updated_at - chrono::Duration::seconds(60);
        route_outcome(
            &storage,
            &row,
            stale,
            RecoveryAction::Demote,
            ProgramType::Escrow,
            &storage_tx,
        )
        .await;

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Processing);
    }

    // ── boot pre-flight (reconcile then validate) ──────────────────

    use crate::operator::sender::validate_smt_root;
    use crate::operator::utils::smt_util::SmtState;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;

    /// Mock a finalized-success `getSignatureStatuses` so the classifier reports the release landed.
    fn mock_finalized_status(server: &mut mockito::ServerGuard) -> mockito::Mock {
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getSignatureStatuses""#.into(),
            ))
            .with_status(200)
            .with_body(
                r#"{"jsonrpc":"2.0","result":{"context":{"slot":200},"value":[{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}]},"id":1}"#,
            )
            .expect_at_least(1)
            .create()
    }

    /// Mock `getAccountInfo` to return an Instance carrying `root`.
    fn mock_instance_account(server: &mut mockito::ServerGuard, root: [u8; 32]) -> mockito::Mock {
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: root,
            current_tree_index: 0,
        };
        let mut bytes = Vec::new();
        instance.serialize(&mut bytes).unwrap();
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(
                r#""method"\s*:\s*"getAccountInfo""#.into(),
            ))
            .with_status(200)
            .with_body(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "owner": Pubkey::new_unique().to_string(),
                            "lamports": 1_000_000u64,
                            "data": [STANDARD.encode(&bytes), "base64"],
                            "executable": false,
                            "rentEpoch": 0
                        }
                    }
                })
                .to_string(),
            )
            .create()
    }

    fn processing_withdrawal(id: i64, nonce: i64) -> DbTransaction {
        let mut row = make_withdrawal_row(id, Some(nonce));
        row.status = TransactionStatus::Processing;
        row
    }

    /// A fresh `Processing` row with a landed signature is promoted to `Completed` under `Duration::ZERO` (the 5-minute default would skip it).
    #[tokio::test]
    async fn recover_once_zero_threshold_picks_up_fresh_processing_row() {
        let mut server = mockito::Server::new_async().await;
        let landed_sig = Signature::new_unique();
        let _status = mock_finalized_status(&mut server);

        let mock = MockStorage::new();
        let row = processing_withdrawal(1, 42);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        mock.insert_release_signature(row.id, landed_sig.to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);

        recover_once(
            &storage,
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &CancellationToken::new(),
            Duration::ZERO,
        )
        .await
        .unwrap();

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(
            after[0].status,
            TransactionStatus::Completed,
            "fresh landed row must be promoted under ZERO threshold"
        );
        assert_eq!(
            after[0].counterpart_signature.as_deref(),
            Some(landed_sig.to_string().as_str())
        );
    }

    /// Pre-flight happy path: a landed-but-uncompleted nonce is reconciled to Completed, then `validate_smt_root` agrees; zero rows Failed.
    #[tokio::test]
    async fn preflight_reconciles_landed_nonce_then_validates_ok() {
        let landed_nonce: u64 = 3;
        let mut onchain_tree = SmtState::new(0);
        onchain_tree.insert_nonce(landed_nonce);

        let mut server = mockito::Server::new_async().await;
        let _status = mock_finalized_status(&mut server);
        let _account = mock_instance_account(&mut server, onchain_tree.current_root());

        let mock = MockStorage::new();
        let row = processing_withdrawal(1, landed_nonce as i64);
        mock.pending_transactions.lock().unwrap().push(row.clone());
        mock.insert_release_signature(row.id, Signature::new_unique().to_string(), 100)
            .await
            .unwrap();
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);
        let token = CancellationToken::new();

        boot_reconcile_processing(
            &storage,
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated = validate_smt_root(&storage, &client, Some(Pubkey::new_unique())).await;
        assert!(
            validated.is_ok(),
            "validate must pass once the landed nonce is reconciled: {validated:?}"
        );

        let after = mock.pending_transactions.lock().unwrap();
        assert_eq!(after[0].status, TransactionStatus::Completed);
        assert!(
            after.iter().all(|t| t.status != TransactionStatus::Failed),
            "no row may be Failed by the pre-flight"
        );
    }

    /// Pre-flight refuse-to-start path: a divergence the reconcile cannot resolve
    /// (a no-signature Processing row goes to ManualReview, leaving the DB one nonce
    /// behind an on-chain root) makes `validate_smt_root` return Err. No row is
    /// Failed (the anti-SOLA2-21 assertion).
    #[tokio::test]
    async fn preflight_refuses_start_on_unreconcilable_mismatch() {
        // On-chain root includes nonce 7 that the DB will never record.
        let mut onchain_tree = SmtState::new(0);
        onchain_tree.insert_nonce(7);

        let mut server = mockito::Server::new_async().await;
        let _account = mock_instance_account(&mut server, onchain_tree.current_root());

        let mock = MockStorage::new();
        // A no-signature Processing withdrawal is quarantined to ManualReview, not Failed.
        let row = processing_withdrawal(1, 7);
        mock.pending_transactions.lock().unwrap().push(row);
        let storage = Storage::Mock(mock.clone());
        let client = make_rpc_client(&server.url());
        let (storage_tx, _rx) = mpsc::channel(8);
        let token = CancellationToken::new();

        boot_reconcile_processing(
            &storage,
            &client,
            ProgramType::Withdraw,
            &storage_tx,
            &token,
            5,
        )
        .await
        .unwrap();

        let validated = validate_smt_root(&storage, &client, Some(Pubkey::new_unique())).await;
        assert!(
            matches!(
                validated,
                Err(OperatorError::Program(
                    crate::error::ProgramError::SmtRootMismatch { .. }
                ))
            ),
            "unreconcilable divergence must refuse to start: {validated:?}"
        );

        let after = mock.pending_transactions.lock().unwrap();
        assert!(
            after.iter().all(|t| t.status != TransactionStatus::Failed),
            "refuse-to-start must never mark a row Failed"
        );
    }
}
