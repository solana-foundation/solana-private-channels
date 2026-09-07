use crate::channel_utils::send_guaranteed;
use crate::error::account::AccountError;
use crate::error::OperatorError;
use crate::operator::bitmap_constants::NONCES_PER_GENERATION;
use crate::operator::sender::types::{PendingRemint, PendingSig, TransactionContext};
use crate::operator::{
    fetch_bitmap_generation, fetch_consumed_nonces, find_withdrawal_bitmap_pda, BitmapState,
    RetryConfig, RpcClientWithRetry,
};
use crate::operator::{MintCache, TransactionKind, TransactionStatusUpdate, WithdrawalRemintInfo};
use crate::storage::common::storage::Storage;
use crate::storage::TransactionStatus;
use crate::{PrivateChannelIndexerConfig, ProgramType};
use chrono::Utc;
use private_channel_metrics::MetricLabel;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info, warn};

use super::types::{InFlightQueue, SenderState, MAX_IN_FLIGHT};
use super::{classify_release_signatures, SigFinality};

impl SenderState {
    pub(super) fn new(
        config: &PrivateChannelIndexerConfig,
        operator_commitment: CommitmentLevel,
        instance_pda: Option<Pubkey>,
        storage: Arc<Storage>,
        retry_max_attempts: u32,
        confirmation_poll_interval_ms: u64,
        source_rpc_client: Option<Arc<RpcClientWithRetry>>,
    ) -> Result<Self, OperatorError> {
        // Initialize global RPC client with retry
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            config.rpc_url.clone(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: operator_commitment,
            },
        ));

        let mint_rpc_client = source_rpc_client.unwrap_or_else(|| rpc_client.clone());
        let mint_cache = MintCache::with_rpc(storage.clone(), mint_rpc_client.clone());

        Ok(Self {
            rpc_client,
            // Source chain client (also used by MintCache). Remints broadcast here.
            source_rpc_client: mint_rpc_client,
            storage,
            instance_pda,
            in_flight_withdrawals: HashSet::new(),
            cached_generation: None,
            retry_counts: HashMap::new(),
            rotation_retry_attempts: 0,
            rotation_in_flight: None,
            rotation_rearm_attempts: 0,
            rotation_blocked_passes: 0,
            mint_cache,
            mint_builders: HashMap::new(),
            retry_max_attempts,
            confirmation_poll_interval_ms,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: config.program_type,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        })
    }
}

/// Boot pre-flight diffing the current generation's released nonces against the
/// ones the database calls Completed. A consumed nonce with no Completed row is
/// lost bookkeeping for a release that did land, so it is repaired in place and
/// boot continues. A Completed row with a clear bit claims a release the chain
/// never performed, so boot refuses.
pub(crate) async fn validate_bitmap_consistency(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    instance_pda: Option<Pubkey>,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
) -> Result<(), OperatorError> {
    let instance_pda = instance_pda.ok_or_else(|| AccountError::InstanceNotFound {
        instance: Pubkey::default(),
    })?;
    let bitmap_pda = find_withdrawal_bitmap_pda(&instance_pda);

    info!(
        instance = %instance_pda,
        bitmap = %bitmap_pda,
        "Validating withdrawal bitmap against completed withdrawals"
    );

    let bitmap = fetch_consumed_nonces(rpc_client, &bitmap_pda).await?;
    let (mut db_only, mut chain_only) = diff_bitmap(storage, &bitmap).await?;

    // The bitmap and the database are read at different instants, so a release
    // that lands between them looks exactly like the database running ahead.
    // A second read after the fact tells the two apart: a real divergence
    // survives it, a race does not.
    if !db_only.is_empty() {
        warn!(
            nonces = ?db_only,
            "Completed withdrawals appear unconsumed on-chain; re-reading the bitmap before halting"
        );
        // A re-read that cannot be taken clears nothing, so the first verdict
        // stands. Letting the read error escape instead would turn the one
        // divergence this check exists to stop into a startup warning, because
        // the caller only refuses to start on the divergence itself.
        match confirm_divergence(storage, rpc_client, &bitmap_pda).await {
            Ok((rediffed_db_only, rediffed_chain_only)) => {
                db_only = rediffed_db_only;
                chain_only = rediffed_chain_only;
            }
            Err(e) => warn!(
                "Could not re-read the bitmap to confirm the divergence, keeping the first verdict: {e}"
            ),
        }
    }

    if !db_only.is_empty() {
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
            .inc();
        error!(
            instance = %instance_pda,
            generation = bitmap.generation,
            db_only = ?db_only,
            chain_only = ?chain_only,
            "Withdrawal bitmap divergence: the database claims releases the chain never made. \
             Refusing to start; reconcile these nonces before restarting."
        );
        return Err(crate::error::ProgramError::BitmapDivergence {
            db_only,
            chain_only,
        }
        .into());
    }

    if chain_only.is_empty() {
        info!(
            generation = bitmap.generation,
            consumed = bitmap.consumed.len(),
            "Withdrawal bitmap verification passed"
        );
        return Ok(());
    }

    warn!(
        generation = bitmap.generation,
        nonces = ?chain_only,
        "Releases landed on-chain without a Completed row; resolving from broadcast signatures"
    );
    let mut paid_twice = Vec::new();
    for nonce in &chain_only {
        if let ChainAheadOutcome::DoublePayout =
            resolve_chain_ahead_nonce(storage, rpc_client, storage_tx, *nonce).await
        {
            paid_twice.push(*nonce);
        }
    }

    // A nonce that was refunded and also released is money out of the instance
    // twice over. Unlike an ordinary chain-ahead gap there is nothing to repair
    // and no version of the history in which the operator was right, so it stops
    // here rather than continuing to send withdrawals from a short balance.
    if !paid_twice.is_empty() {
        error!(
            instance = %instance_pda,
            generation = bitmap.generation,
            nonces = ?paid_twice,
            "Withdrawal bitmap divergence: these nonces were reminted to the user and released \
             on-chain. Refusing to start; reconcile the double payouts before restarting."
        );
        return Err(crate::error::ProgramError::BitmapDivergence {
            db_only: Vec::new(),
            chain_only: paid_twice,
        }
        .into());
    }

    Ok(())
}

/// Take the confirmatory second read and re-diff it against the database.
async fn confirm_divergence(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    bitmap_pda: &Pubkey,
) -> Result<(Vec<u64>, Vec<u64>), OperatorError> {
    let bitmap = fetch_consumed_nonces(rpc_client, bitmap_pda).await?;
    diff_bitmap(storage, &bitmap).await
}

/// Split the current generation into "database only" and "chain only" nonces.
/// Both sides are restricted to the window the bitmap covers, because outside it
/// the bits were cleared by a rotation and mean nothing.
async fn diff_bitmap(
    storage: &Storage,
    bitmap: &BitmapState,
) -> Result<(Vec<u64>, Vec<u64>), OperatorError> {
    // Saturating so a corrupt generation yields an empty window, never a panic.
    let min_nonce = bitmap.generation.saturating_mul(NONCES_PER_GENERATION);
    let max_nonce = min_nonce.saturating_add(NONCES_PER_GENERATION);

    let completed: HashSet<u64> = storage
        .get_completed_withdrawal_nonces(min_nonce, max_nonce)
        .await?
        .into_iter()
        .collect();
    let consumed: HashSet<u64> = bitmap.consumed.iter().copied().collect();

    let mut db_only: Vec<u64> = completed.difference(&consumed).copied().collect();
    let mut chain_only: Vec<u64> = consumed.difference(&completed).copied().collect();
    db_only.sort_unstable();
    chain_only.sort_unstable();

    Ok((db_only, chain_only))
}

/// Read a withdrawal's broadcast signatures back from durable storage.
///
/// Every release persists its signature before broadcast, so this is the record
/// that survives a restart. A read failure or an unparseable entry yields
/// nothing, which routes callers to manual review rather than letting them
/// conclude anything about whether the release landed.
pub(super) async fn load_persisted_release_signatures(
    storage: &Storage,
    transaction_id: i64,
) -> Vec<PendingSig> {
    match storage.get_release_signatures(transaction_id).await {
        Ok(stored) => stored
            .iter()
            .filter_map(|entry| {
                Signature::from_str(&entry.signature)
                    .ok()
                    .map(|signature| PendingSig {
                        signature,
                        last_valid_block_height: entry.last_valid_block_height.max(0) as u64,
                    })
            })
            .collect(),
        Err(e) => {
            error!(transaction_id, "Release signature lookup failed: {e}");
            Vec::new()
        }
    }
}

/// What a chain-ahead nonce turned out to be, which decides whether startup
/// can continue past it.
///
/// The three cases differ in what is still true afterwards: one closes the gap,
/// one leaves a real payout unattributed for a human, and one says the instance
/// paid out twice.
enum ChainAheadOutcome {
    /// The row was still open, so the repair write lands and closes the gap.
    Repaired,
    /// Nothing could be written, so the payout stays unattributed.
    Unrepaired,
    /// The user was refunded and the chain released the nonce as well.
    DoublePayout,
}

/// Count and log a chain-ahead nonce the boot repair could not close.
fn report_unrepaired(nonce: u64) {
    crate::metrics::OPERATOR_TRANSACTION_ERRORS
        .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
        .inc();
    error!(nonce, "Consumed nonce could not be repaired at boot");
}

/// Repair a single nonce the chain consumed but the database never recorded.
///
/// A landed signature proves which broadcast paid out, so the row becomes
/// Completed against it. Anything else leaves the payout real but unattributed,
/// which a human has to close out.
///
/// The status write only touches rows the operator still owns, so a row that has
/// already reached a terminal state absorbs the update silently. Those cases are
/// reported rather than repaired, because claiming a repair that the database
/// dropped is how a real divergence disappears into an info log.
async fn resolve_chain_ahead_nonce(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    nonce: u64,
) -> ChainAheadOutcome {
    let row = match storage.get_withdrawal_by_nonce(nonce).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            error!(
                nonce,
                "Nonce consumed on-chain has no withdrawal row; funds moved with no record"
            );
            report_unrepaired(nonce);
            return ChainAheadOutcome::Unrepaired;
        }
        Err(e) => {
            error!(
                nonce,
                "Could not load withdrawal row for consumed nonce: {e}"
            );
            report_unrepaired(nonce);
            return ChainAheadOutcome::Unrepaired;
        }
    };

    // The burn was already refunded, so a consumed bit means the release paid
    // the user as well. No status write can undo either half of that, and the
    // instance is short by the amount, so this is reported rather than repaired
    // and the caller stops the operator on it.
    if row.status == TransactionStatus::FailedReminted || row.landed_remint_signature.is_some() {
        error!(
            nonce,
            transaction_id = row.id,
            "Nonce was reminted to the user and also released on-chain; the funds moved twice"
        );
        crate::metrics::OPERATOR_TRANSACTION_ERRORS
            .with_label_values(&[ProgramType::Withdraw.as_label(), "bitmap_divergence"])
            .inc();
        return ChainAheadOutcome::DoublePayout;
    }

    // Only these two are still writable; anywhere else the update is discarded.
    let repairable = matches!(
        row.status,
        TransactionStatus::Processing | TransactionStatus::PendingRemint
    );

    let signatures = load_persisted_release_signatures(storage, row.id).await;

    let verdict = if signatures.is_empty() {
        None
    } else {
        match classify_release_signatures(rpc_client, &signatures).await {
            SigFinality::Landed(sig) => Some(sig),
            SigFinality::Live(reason) | SigFinality::Uncertain(reason) => {
                warn!(nonce, transaction_id = row.id, "Unresolved: {reason}");
                None
            }
            SigFinality::Dead => None,
        }
    };

    // Only a payout attributed to one of our sends is closed by the write below; the rest are left for a human.
    let attributed = verdict.is_some();

    let update = match verdict {
        Some(sig) if repairable => {
            info!(
                nonce,
                transaction_id = row.id,
                "Consumed nonce matched a landed signature; marking Completed"
            );
            TransactionStatusUpdate {
                transaction_id: row.id,
                trace_id: Some(row.trace_id.clone()),
                status: TransactionStatus::Completed,
                counterpart_signature: Some(sig.to_string()),
                processed_at: Some(Utc::now()),
                error_message: None,
                remint_signature: None,
                remint_attempted: false,
            }
        }
        verdict => {
            let reason = if repairable {
                format!(
                    "nonce {nonce} is consumed on-chain but no broadcast signature accounts for it"
                )
            } else {
                format!(
                    "nonce {nonce} is consumed on-chain but the row is already {:?}, so it cannot be reconciled automatically",
                    row.status
                )
            };
            error!(
                nonce,
                transaction_id = row.id,
                status = ?row.status,
                attributed = verdict.is_some(),
                "Consumed nonce cannot be reconciled; escalating"
            );
            TransactionStatusUpdate {
                transaction_id: row.id,
                trace_id: Some(row.trace_id.clone()),
                status: TransactionStatus::ManualReview,
                counterpart_signature: None,
                processed_at: Some(Utc::now()),
                error_message: Some(reason),
                remint_signature: None,
                remint_attempted: false,
            }
        }
    };

    send_guaranteed(storage_tx, update, "transaction status update")
        .await
        .ok();

    if repairable && attributed {
        ChainAheadOutcome::Repaired
    } else {
        report_unrepaired(nonce);
        ChainAheadOutcome::Unrepaired
    }
}

impl SenderState {
    /// Read the generation the withdrawal bitmap is currently on.
    ///
    /// The chain is the authority; this answer is never inferred from local
    /// state. Callers that want it remembered go through `refresh_generation`
    /// instead, which is the only way a generation reaches the cache and is
    /// what keeps the cache from ever holding a number nobody read.
    pub(super) async fn fetch_current_generation(&self) -> Result<u64, OperatorError> {
        let instance_pda = self.instance_pda.ok_or(AccountError::InstanceNotFound {
            instance: Pubkey::default(),
        })?;
        fetch_bitmap_generation(&self.rpc_client, &find_withdrawal_bitmap_pda(&instance_pda)).await
    }

    /// Read the current generation and remember it.
    ///
    /// Every cache write funnels through here or through a confirmed rotation,
    /// so the cached value is always something the chain reported rather than
    /// something the operator guessed. A failed read leaves the previous value
    /// alone: it was already true at some point, which is more than a guess.
    pub(super) async fn refresh_generation(&mut self) -> Result<u64, OperatorError> {
        let generation = self.fetch_current_generation().await?;
        self.cached_generation = Some(generation);
        Ok(generation)
    }

    /// Sends a ManualReview status update during startup recovery when a stored
    /// transaction cannot be reconstructed (e.g. unparseable pubkey or signature).         
    /// Using send_guaranteed so the alert is never silently dropped.                       
    async fn send_recovery_manual_review(
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
        transaction_id: i64,
        trace_id: &str,
        reason: &str,
    ) {
        send_guaranteed(
            storage_tx,
            TransactionStatusUpdate {
                transaction_id,
                trace_id: Some(trace_id.to_string()),
                status: TransactionStatus::ManualReview,
                counterpart_signature: None,
                processed_at: Some(Utc::now()),
                error_message: Some(format!("recovery failed: {}", reason)),
                remint_signature: None,
                remint_attempted: false,
            },
            "transaction status update",
        )
        .await
        .ok();
    }

    /// On an error, logs it and sends a ManualReview update. Returns `None` on error.
    async fn or_manual_review<T>(
        result: Result<T, String>,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
        tx_id: i64,
        trace_id: &str,
    ) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(msg) => {
                error!(transaction_id = tx_id, "Recovery: {}", msg);
                Self::send_recovery_manual_review(storage_tx, tx_id, trace_id, &msg).await;

                None
            }
        }
    }

    pub(super) async fn recover_pending_remints(
        &mut self,
        storage_tx: &mpsc::Sender<TransactionStatusUpdate>,
    ) -> Result<(), OperatorError> {
        // Deferred remints are Withdraw-only; another role sharing the database
        // must never claim a row it would classify against the wrong chain.
        if self.program_type != ProgramType::Withdraw {
            return Ok(());
        }

        let transactions = self.storage.get_pending_remint_transactions().await?;

        if transactions.is_empty() {
            return Ok(());
        }

        info!(
            "Recovering {} pending remint(s) from database",
            transactions.len()
        );

        // PrivateChannel only supports SPL Token for now.
        let private_channel_token_program = self.mint_cache.get_private_channel_token_program();

        for tx in transactions {
            // Parse pubkeys stored as strings. On any failure we cannot remint safely,
            // and silently skipping would leave the row stuck in PendingRemint on every
            // restart — so we escalate to ManualReview.
            let Some(mint) = Self::or_manual_review(
                Pubkey::from_str(&tx.mint).map_err(|e| format!("invalid mint pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            // The burn debited the initiator, so the remint credits them, not
            // `recipient` (the Solana destination of the failed release).
            let Some(initiator) = Self::or_manual_review(
                Pubkey::from_str(&tx.initiator)
                    .map_err(|e| format!("invalid initiator pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            let initiator_ata = get_associated_token_address_with_program_id(
                &initiator,
                &mint,
                &private_channel_token_program,
            );

            let amount = tx.amount.value();

            // Pair each stored signature with its last_valid_block_height. The
            // remint gate needs both to verify the withdrawal cannot still land.
            // An empty array, a bad signature, or an array-length mismatch means
            // we cannot safely run that check, so we escalate to ManualReview.
            let sig_strings = tx.remint_signatures.unwrap_or_default();
            let lvbhs = tx.remint_last_valid_block_heights.unwrap_or_default();

            let parsed: Result<Vec<PendingSig>, String> = if sig_strings.is_empty() {
                Err("no withdrawal signatures stored; cannot verify finality".to_string())
            } else if sig_strings.len() != lvbhs.len() {
                Err(format!(
                    "lvbh length {} != signatures length {}",
                    lvbhs.len(),
                    sig_strings.len()
                ))
            } else {
                sig_strings
                    .iter()
                    .zip(&lvbhs)
                    .map(|(sig_string, &lvbh)| {
                        let signature = Signature::from_str(sig_string)
                            .map_err(|e| format!("invalid withdrawal signature: {e}"))?;
                        let last_valid_block_height = u64::try_from(lvbh)
                            .map_err(|_| format!("negative last_valid_block_height: {lvbh}"))?;
                        Ok(PendingSig {
                            signature,
                            last_valid_block_height,
                        })
                    })
                    .collect()
            };

            let Some(signatures) =
                Self::or_manual_review(parsed, storage_tx, tx.id, &tx.trace_id).await
            else {
                continue;
            };

            // Restore the original deadline. Fall back to now() if missing (shouldn't
            // happen) so the entry fires on the next tick instead of waiting 32s more.
            let deadline = tx.pending_remint_deadline_at.unwrap_or_else(Utc::now);

            let ctx = TransactionContext {
                kind: TransactionKind::ReleaseFunds,
                transaction_id: Some(tx.id),
                // Carried so logs and the bitmap gate can name this withdrawal.
                withdrawal_nonce: tx.withdrawal_nonce.map(|n| n as u64),
                trace_id: Some(tx.trace_id.clone()),
            };

            let remint_info = WithdrawalRemintInfo {
                transaction_id: tx.id,
                trace_id: tx.trace_id.clone(),
                mint,
                user: initiator,
                user_ata: initiator_ata,
                token_program: private_channel_token_program,
                amount,
            };

            info!(
                transaction_id = tx.id,
                nonce = ctx.withdrawal_nonce.map(|n| n as i64),
                sigs = signatures.len(),
                "Recovered PendingRemint, deadline={}",
                deadline,
            );

            // A corrupt negative value would wrap to a huge u32 and skip the
            // attempt cap, defeating the whole point of persisting it.
            let Some(finality_check_attempts) = Self::or_manual_review(
                u32::try_from(tx.finality_check_attempts).map_err(|_| {
                    format!(
                        "negative finality_check_attempts: {}",
                        tx.finality_check_attempts
                    )
                }),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            self.pending_remints.push(PendingRemint {
                ctx,
                remint_info,
                signatures,
                // The original error string is not stored in DB. Only surfaced in
                // combined error messages if the remint itself also fails.
                original_error: "recovered from persistent storage".to_string(),
                deadline,
                finality_check_attempts,
                // Only a refusal the program itself made frees the refund from
                // the bitmap gate, and it is durable precisely so a restart
                // inside the finality window cannot turn an automatic refund
                // into a manual one. Anything else is held to the ordinary gate.
                release_refused_on_chain: tx.release_refused_on_chain,
                coverage_slot: None,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::test_support::{
        mock_bitmap_account, mock_bitmap_sequence, mock_bitmap_then_read_failure,
        sender_state_with_storage, sender_state_with_storage_and_role,
    };
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use tokio::sync::mpsc;

    fn make_sender_state(mock: MockStorage) -> SenderState {
        make_sender_state_with_role(mock, ProgramType::Withdraw)
    }

    fn make_sender_state_with_role(mock: MockStorage, role: ProgramType) -> SenderState {
        sender_state_with_storage_and_role("http://localhost:8899", mock, role)
    }

    /// Build a minimal DbTransaction representing a PendingRemint row.
    /// All string fields use real base58-encoded pubkeys and signatures so
    /// `recover_pending_remints` can parse them without error.
    fn make_pending_remint_row(
        id: i64,
        mint: &Pubkey,
        initiator: &Pubkey,
        recipient: &Pubkey,
        sig: &Signature,
        deadline: chrono::DateTime<Utc>,
    ) -> DbTransaction {
        let now = Utc::now();
        DbTransaction {
            id,
            signature: Signature::new_unique().to_string(),
            trace_id: format!("trace-{id}"),
            slot: 100,
            initiator: initiator.to_string(),
            recipient: recipient.to_string(),
            mint: mint.to_string(),
            amount: TokenAmount(5_000),
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(id),
            status: TransactionStatus::PendingRemint,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: Some(vec![sig.to_string()]),
            remint_last_valid_block_heights: Some(vec![12_345]),
            pending_remint_deadline_at: Some(deadline),
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
            release_refused_on_chain: false,
        }
    }

    // ── recover_pending_remints: happy path ──────────────────────────

    /// On startup, all PendingRemint rows from the database must be fully
    /// reconstructed into the in-memory `pending_remints` queue so the
    /// operator can continue where it left off before the crash.
    ///
    /// This test verifies that every field is correctly restored:
    /// - transaction_id, trace_id, amount, mint, initiator
    /// - withdrawal signatures (needed for the finality check)
    /// - the original deadline (not a fresh 32s window — the clock keeps
    ///   ticking across restarts)
    /// - finality_check_attempts round-trips from the DB so the
    ///   MAX_FINALITY_CHECK_ATTEMPTS budget survives restarts
    ///
    /// No channel messages should be sent — there is nothing wrong with
    /// these rows, they just need to be re-queued.
    #[tokio::test]
    async fn recover_pending_remints_rehydrates_queue() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        // Distinct from `recipient`: the burn debited the initiator, so the
        // remint must target them, not the withdrawal's Solana destination.
        let initiator = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Mid-budget value so the round-trip assertion is meaningful: a reset
        // to 0 on recovery would re-arm the cap and let an ambiguous row
        // outlive the intended ManualReview escalation.
        let mut row = make_pending_remint_row(42, &mint, &initiator, &recipient, &sig, deadline);
        row.finality_check_attempts = 2;
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // Exactly one entry should be re-queued.
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];

        // Identity fields.
        assert_eq!(entry.ctx.transaction_id, Some(42));
        assert_eq!(entry.ctx.trace_id.as_deref(), Some("trace-42"));

        // Amount must be correctly cast from i64 → u64.
        assert_eq!(entry.remint_info.amount, 5_000u64);

        // Pubkeys must be correctly parsed from their string representation.
        assert_eq!(entry.remint_info.mint, mint);

        // The remint reverses a burn, so it must credit the account that was
        // debited (initiator), not the withdrawal's Solana destination.
        assert_eq!(entry.remint_info.user, initiator);
        assert_ne!(entry.remint_info.user, recipient);
        assert_eq!(
            entry.remint_info.user_ata,
            get_associated_token_address_with_program_id(&initiator, &mint, &spl_token::id()),
            "remint must mint into the initiator's private channel ATA"
        );

        // Signatures must be parsed back — they drive the finality check.
        // lvbh must round-trip too: the gate needs it to prove a broadcast
        // can no longer land.
        assert_eq!(entry.signatures.len(), 1);
        assert_eq!(entry.signatures[0].signature, sig);
        assert_eq!(entry.signatures[0].last_valid_block_height, 12_345);

        // Deadline must be the stored one, not a fresh window.
        // Allows up to 1s of clock skew between DB write and assertion.
        assert!(
            (entry.deadline - deadline).num_milliseconds().abs() < 1_000,
            "deadline should be restored from DB, got {:?}",
            entry.deadline
        );

        // The counter must survive the round-trip. A reset would re-arm the
        // attempt cap on every restart.
        assert_eq!(entry.finality_check_attempts, 2);

        // Standard recovery marker so combined error messages are meaningful.
        assert_eq!(entry.original_error, "recovered from persistent storage");

        // No status update sent — valid rows are silently re-queued.
        assert!(
            storage_rx.try_recv().is_err(),
            "no channel message expected for a valid recovery row"
        );
    }

    /// A release the program refused is direct proof no payout happened, and it
    /// is the only such proof that outlives a rotation. Losing it to a restart
    /// turns a refund the operator could have made on its own into a manual one,
    /// so it has to come back off the row exactly as it was written.
    #[tokio::test]
    async fn recover_pending_remints_restores_the_on_chain_refusal() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let initiator = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut refused = make_pending_remint_row(1, &mint, &initiator, &recipient, &sig, deadline);
        refused.release_refused_on_chain = true;
        let ordinary = make_pending_remint_row(2, &mint, &initiator, &recipient, &sig, deadline);
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .extend([refused, ordinary]);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert_eq!(state.pending_remints.len(), 2);
        assert!(
            state.pending_remints[0].release_refused_on_chain,
            "a refused release must come back refused"
        );
        assert!(
            !state.pending_remints[1].release_refused_on_chain,
            "an ordinary failure must stay held to the bitmap gate"
        );
        assert!(storage_rx.try_recv().is_err(), "both rows are valid");
    }

    /// A negative `finality_check_attempts` should never appear (the column is
    /// `INTEGER NOT NULL DEFAULT 0`, only ever written to non-negative values),
    /// but a corrupt row must escalate rather than wrap silently into a huge
    /// `u32` that bypasses the attempt cap.
    #[tokio::test]
    async fn recover_pending_remints_escalates_negative_attempt_counter() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut row =
            make_pending_remint_row(7, &mint, &Pubkey::new_unique(), &recipient, &sig, deadline);
        row.finality_check_attempts = -1;
        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert!(state.pending_remints.is_empty());
        let update = storage_rx
            .try_recv()
            .expect("corrupt row must produce a ManualReview update");
        assert_eq!(update.transaction_id, 7);
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    // ── recover_pending_remints: parse error escalations ─────────────

    /// A corrupted mint pubkey in a PendingRemint row cannot be parsed back
    /// into a `Pubkey`, so the remint cannot be safely executed.
    ///
    /// The operator must escalate to ManualReview immediately rather than
    /// silently skipping — skipping would leave the row stuck in PendingRemint
    /// and re-surface the same corrupt row on every subsequent restart.
    ///
    /// Critically, the bad row must not block recovery of other valid rows:
    /// if there are two rows and one is corrupt, the valid one must still
    /// be queued.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_mint_to_manual_review() {
        let mock = MockStorage::new();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Row 1: invalid mint — should escalate to ManualReview and be skipped.
        let mut bad_row = make_pending_remint_row(
            10,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            deadline,
        );
        bad_row.mint = "not-a-valid-pubkey".to_string();

        // Row 2: valid — must still be recovered despite the bad row above.
        let good_mint = Pubkey::new_unique();
        let good_row = make_pending_remint_row(
            11,
            &good_mint,
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            deadline,
        );

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .extend([bad_row, good_row]);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // The bad row must produce exactly one ManualReview update.
        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad row");
        assert_eq!(update.transaction_id, 10);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid mint pubkey"),
            "error message should describe the parse failure: {err}"
        );

        // The valid row must still be queued — bad rows don't abort recovery.
        assert_eq!(state.pending_remints.len(), 1);
        assert_eq!(state.pending_remints[0].ctx.transaction_id, Some(11));

        // No further channel messages.
        assert!(storage_rx.try_recv().is_err());
    }

    /// A corrupted initiator pubkey cannot be parsed into a `Pubkey`, so the
    /// operator cannot compute the burner's ATA and has no valid destination
    /// for the remint.
    ///
    /// Same escalation rule as invalid mint: ManualReview immediately, do not
    /// skip silently, do not block other rows.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_initiator_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(
            20,
            &mint,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &sig,
            deadline,
        );
        bad_row.initiator = "not-a-valid-pubkey".to_string();

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad initiator");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid initiator pubkey"),
            "error message should describe the parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "bad row must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    // No negative-amount test here anymore: `TokenAmount(u64)` makes a negative
    // amount unconstructable; the rejection now lives in TokenAmount's decode tests.

    /// An unparseable withdrawal signature in a PendingRemint row breaks the
    /// finality check: the operator cannot call `get_signature_statuses` with
    /// an invalid signature, so it cannot determine whether the original
    /// withdrawal landed on-chain.
    ///
    /// Reminting without that check risks a double-credit — the operator must
    /// escalate to ManualReview instead of queuing the entry.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_signature_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(
            40,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &Signature::new_unique(),
            deadline,
        );
        // Replace the valid signature with garbage.
        bad_row.remint_signatures = Some(vec!["not-a-valid-signature".to_string()]);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for invalid signature");
        assert_eq!(update.transaction_id, 40);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid withdrawal signature"),
            "error message should describe the signature parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "row with invalid signature must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// A PendingRemint row whose `remint_signatures` and
    /// `remint_last_valid_block_heights` arrays have different lengths cannot
    /// be turned into a coherent `Vec<PendingSig>`. Index-pairing would be
    /// undefined, so the remint gate cannot reliably check liveness.
    ///
    /// Escalate to ManualReview rather than guessing which sig got which lvbh.
    #[tokio::test]
    async fn recover_pending_remints_escalates_lvbh_length_mismatch_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(
            50,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &Signature::new_unique(),
            deadline,
        );
        bad_row.remint_signatures = Some(vec![
            Signature::new_unique().to_string(),
            Signature::new_unique().to_string(),
        ]);
        bad_row.remint_last_valid_block_heights = Some(vec![100]);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for length mismatch");
        assert_eq!(update.transaction_id, 50);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("lvbh length"),
            "error message should describe the length mismatch: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "row with mismatched array lengths must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// The deferred remint queue belongs to the Withdraw role. An Escrow sender
    /// sharing the transactions database must not hydrate a PendingRemint row:
    /// it would later classify the release signature against the wrong chain.
    #[tokio::test]
    async fn recover_pending_remints_noop_for_escrow_role() {
        let mock = MockStorage::new();
        let sig = Signature::new_unique();
        let deadline = Utc::now() - chrono::Duration::seconds(10);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                70,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &sig,
                deadline,
            ));

        let mut state = make_sender_state_with_role(mock, ProgramType::Escrow);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        assert!(
            state.pending_remints.is_empty(),
            "Escrow must not claim a Withdraw remint row"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "the row must be left untouched for the Withdraw sender"
        );
    }

    /// On a clean startup with no PendingRemint rows in the database,
    /// `recover_pending_remints` must be a complete no-op: no entries queued,
    /// no channel messages sent, no errors returned.
    #[tokio::test]
    async fn recover_pending_remints_empty_db_is_noop() {
        let mock = MockStorage::new();
        // pending_remint_transactions is empty by default.
        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let result = state.recover_pending_remints(&storage_tx).await;

        assert!(result.is_ok(), "should not error on empty DB");
        assert!(
            state.pending_remints.is_empty(),
            "queue should remain empty"
        );
        assert!(
            storage_rx.try_recv().is_err(),
            "no channel messages expected"
        );
    }

    /// A PendingRemint row whose deadline has already passed (e.g. the operator
    /// was down for longer than the finality window) must still be queued on
    /// recovery. The deadline is preserved as-is so that `process_pending_remints`
    /// sees it as already matured and processes it on the very next tick.
    #[tokio::test]
    async fn recover_pending_remints_past_deadline_queued_with_past_deadline() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        // Deadline already in the past — crash happened mid-finality window.
        let past_deadline = Utc::now() - chrono::Duration::seconds(10);

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                50,
                &mint,
                &Pubkey::new_unique(),
                &recipient,
                &sig,
                past_deadline,
            ));

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        // Entry must be queued — recovery re-queues, does not process.
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];
        assert_eq!(entry.ctx.transaction_id, Some(50));

        // Past deadline preserved — process_pending_remints will fire it immediately.
        assert!(
            entry.deadline <= Utc::now(),
            "past deadline should be restored so entry matures on next tick: {:?}",
            entry.deadline
        );

        // No ManualReview
        assert!(storage_rx.try_recv().is_err());
    }

    /// When `pending_remint_deadline_at` is NULL in the database (corrupt row or
    /// schema inconsistency), recovery falls back to `Utc::now()`. This means the
    /// entry is treated as immediately matured — `process_pending_remints` will
    /// pick it up on the next tick instead of waiting a full 32s window.
    #[tokio::test]
    async fn recover_pending_remints_missing_deadline_defaults_to_now() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();

        let mut row = make_pending_remint_row(
            60,
            &mint,
            &Pubkey::new_unique(),
            &recipient,
            &sig,
            Utc::now() + chrono::Duration::seconds(30),
        );
        row.pending_remint_deadline_at = None; // simulate missing deadline

        mock.pending_remint_transactions.lock().unwrap().push(row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        let before = Utc::now();
        state.recover_pending_remints(&storage_tx).await.unwrap();
        let after = Utc::now();

        // Entry must still be queued (not skipped).
        assert_eq!(state.pending_remints.len(), 1);
        let entry = &state.pending_remints[0];
        assert_eq!(entry.ctx.transaction_id, Some(60));

        // Deadline must be ~Utc::now() at the time of recovery — entry fires on next tick.
        assert!(
            entry.deadline >= before - chrono::Duration::milliseconds(100)
                && entry.deadline <= after + chrono::Duration::milliseconds(100),
            "missing deadline should default to ~now, got {:?}",
            entry.deadline
        );

        // No ManualReview sent — missing deadline is handled gracefully.
        assert!(storage_rx.try_recv().is_err());
    }

    // ── SenderState construction tests ───────────────────────────────

    use crate::config::{PostgresConfig, ProgramType, StorageType};

    fn make_config() -> PrivateChannelIndexerConfig {
        PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: "http://localhost:8899".to_string(),
            fallback_rpc_url: None,
            source_rpc_url: None,
            postgres: PostgresConfig {
                database_url: "postgresql://localhost/test".to_string(),
                max_connections: 5,
            },
            escrow_instance_id: None,
        }
    }

    /// `SenderState::new` with no instance PDA and Escrow program type must succeed and
    /// hold no withdrawal replay state of its own.
    #[test]
    fn sender_state_new_constructs_successfully() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let config = make_config();

        let result = SenderState::new(
            &config,
            CommitmentLevel::Confirmed,
            None,
            storage,
            3,
            400,
            None,
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert!(state.instance_pda.is_none());
        assert!(state.in_flight_withdrawals.is_empty());
        assert_eq!(state.retry_max_attempts, 3);
        assert_eq!(state.program_type, ProgramType::Escrow);
    }

    /// Providing an instance PDA and a higher retry limit must be reflected in the
    /// constructed state; the PDA is stored as-is and later derives the bitmap.
    #[test]
    fn sender_state_new_with_instance_pda() {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let instance_pda = Pubkey::new_unique();
        let config = make_config();

        let result = SenderState::new(
            &config,
            CommitmentLevel::Finalized,
            Some(instance_pda),
            storage,
            5,
            400,
            None,
        );

        assert!(result.is_ok());
        let state = result.unwrap();
        assert_eq!(state.instance_pda, Some(instance_pda));
        assert_eq!(state.retry_max_attempts, 5);
    }

    // ── validate_bitmap_consistency ──────────────────────────────────

    /// Seed a Completed withdrawal row carrying `nonce`, with one broadcast
    /// signature already persisted so the chain-ahead path has something to
    /// classify.
    fn seed_withdrawal(
        mock: &MockStorage,
        id: i64,
        nonce: u64,
        status: TransactionStatus,
        signature: Option<Signature>,
    ) {
        let now = Utc::now();
        mock.pending_transactions
            .lock()
            .unwrap()
            .push(DbTransaction {
                id,
                signature: Signature::new_unique().to_string(),
                trace_id: format!("trace-{id}"),
                slot: 1,
                initiator: Pubkey::new_unique().to_string(),
                recipient: Pubkey::new_unique().to_string(),
                mint: Pubkey::new_unique().to_string(),
                amount: TokenAmount(1_000),
                memo: None,
                transaction_type: TransactionType::Withdrawal,
                withdrawal_nonce: Some(nonce as i64),
                status,
                created_at: now,
                updated_at: now,
                processed_at: None,
                counterpart_signature: None,
                remint_signatures: None,
                remint_last_valid_block_heights: None,
                pending_remint_deadline_at: None,
                finality_check_attempts: 0,
                recovery_requeue_attempts: 0,
                instruction_index: 0,
                inner_index: None,
                landed_remint_signature: None,
                release_refused_on_chain: false,
            });

        if let Some(sig) = signature {
            mock.release_signatures.lock().unwrap().insert(
                id,
                vec![crate::storage::common::models::StoredSig {
                    signature: sig.to_string(),
                    last_valid_block_height: 1,
                    blockhash_slot: None,
                }],
            );
        }
    }

    /// The pre-flight cannot diff anything without an instance to derive the
    /// bitmap from, and must say so rather than silently pass.
    #[tokio::test]
    async fn validate_bitmap_consistency_fails_without_instance_pda() {
        let state = make_sender_state(MockStorage::new());
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            None,
            &storage_tx,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Account(crate::error::AccountError::InstanceNotFound { .. })
            ),
            "expected InstanceNotFound, got: {err}"
        );
    }

    /// Chain and database agree: no halt, no repair, no status writes.
    #[tokio::test]
    async fn validate_bitmap_consistency_identical_sets_pass() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[1, 3]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 1, 1, TransactionStatus::Completed, None);
        seed_withdrawal(&mock, 3, 3, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await;

        assert!(result.is_ok(), "agreeing sets must pass: {result:?}");
        assert!(storage_rx.try_recv().is_err(), "nothing to repair");
    }

    /// Chain ahead: the release landed and a stored signature proves which one,
    /// so the row is completed and startup continues. Halting here would stop
    /// the pipeline over a bookkeeping gap where the money already moved.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_completes_and_starts() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);
        let _statuses = server
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
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, Some(landed));

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await;

        assert!(result.is_ok(), "chain-ahead must not halt: {result:?}");
        let update = storage_rx.try_recv().expect("row must be reconciled");
        assert_eq!(update.transaction_id, 7);
        assert_eq!(update.status, TransactionStatus::Completed);
        assert_eq!(update.counterpart_signature, Some(landed.to_string()));
    }

    /// Chain ahead with nothing to attribute the payout to: still start, but the
    /// nonce goes to a human rather than being silently completed.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_without_signatures_escalates() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, None);

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await;

        assert!(result.is_ok(), "chain-ahead must not halt: {result:?}");
        let update = storage_rx.try_recv().expect("row must be escalated");
        assert_eq!(update.transaction_id, 7);
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    /// A payout no signature accounts for is unexplained money leaving the instance, so it must not report as repaired.
    #[tokio::test]
    async fn chain_ahead_nonce_with_no_attributable_signature_reports_unrepaired() {
        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, None);

        let state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let outcome =
            super::resolve_chain_ahead_nonce(&state.storage, &state.rpc_client, &storage_tx, 2)
                .await;

        assert!(
            matches!(outcome, super::ChainAheadOutcome::Unrepaired),
            "an unattributed payout is not a repair"
        );
        let update = storage_rx.try_recv().expect("the row must be escalated");
        assert_eq!(update.status, TransactionStatus::ManualReview);
    }

    /// The other half of that rule: an attributed payout is genuinely closed and must raise no alert.
    #[tokio::test]
    async fn chain_ahead_nonce_matched_to_a_landed_signature_reports_repaired() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _statuses = server
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
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Processing, Some(landed));

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let outcome =
            super::resolve_chain_ahead_nonce(&state.storage, &state.rpc_client, &storage_tx, 2)
                .await;

        assert!(
            matches!(outcome, super::ChainAheadOutcome::Repaired),
            "an attributed payout closes the gap"
        );
        let update = storage_rx.try_recv().expect("the row must be completed");
        assert_eq!(update.status, TransactionStatus::Completed);
    }

    /// A row that already refunded the burn, whose nonce the chain also records
    /// as released, means both sides paid. That is a settled double payout, not
    /// a bookkeeping gap, and the operator must not keep sending withdrawals on
    /// top of a balance it can no longer account for.
    #[tokio::test]
    async fn validate_bitmap_consistency_reminted_row_with_consumed_nonce_halts() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::FailedReminted, None);

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                chain_only,
                ..
            }) => assert_eq!(
                chain_only,
                vec![2],
                "the halt must name the paid-twice nonce"
            ),
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// The repair only writes rows the operator still owns, so a chain-ahead
    /// nonce on an already-terminal row silently writes nothing. It has to reach
    /// a human instead of being logged as though it had been fixed, which is the
    /// shape a real divergence takes when it disappears.
    #[tokio::test]
    async fn validate_bitmap_consistency_chain_ahead_on_terminal_row_alerts() {
        let mut server = mockito::Server::new_async().await;
        let landed = Signature::new_unique();
        let _bitmap = mock_bitmap_account(&mut server, 0, &[2]);
        let _statuses = server
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
                            "slot": 10,
                            "confirmations": null,
                            "err": null,
                            "status": {"Ok": null},
                            "confirmationStatus": "finalized"
                        }]
                    }
                })
                .to_string(),
            )
            .create();

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 7, 2, TransactionStatus::Failed, Some(landed));

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await;

        assert!(
            result.is_ok(),
            "an ordinary chain-ahead nonce must not halt: {result:?}"
        );
        let update = storage_rx
            .try_recv()
            .expect("an unrepairable row must still be reported");
        assert_eq!(
            update.status,
            TransactionStatus::ManualReview,
            "a Completed write to a terminal row is dropped, so it must not be claimed"
        );
    }

    /// The re-read exists to fail closed on the one divergence that matters, so
    /// a re-read that cannot be taken must keep the first verdict. Letting the
    /// read error escape instead downgrades the refusal to start into a warning,
    /// because only the divergence itself stops the boot.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts_when_the_reread_fails() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_then_read_failure(&mut server, 0, &[]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Program(crate::error::ProgramError::BitmapDivergence { .. })
            ),
            "an unconfirmable divergence must still refuse to start: {err:?}"
        );
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// Database ahead: the operator believes in a release the chain denies, so
    /// every later decision it makes would rest on a false history. Refuse.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                db_only,
                ..
            }) => assert_eq!(db_only, vec![4], "the halt must name the offending nonce"),
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// Divergence in both directions still halts: the database-ahead side is the
    /// one that decides, and the chain-ahead nonces ride along in the report.
    #[tokio::test]
    async fn validate_bitmap_consistency_both_directions_halts() {
        let mut server = mockito::Server::new_async().await;
        let _bitmap = mock_bitmap_account(&mut server, 0, &[6]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);

        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence {
                db_only,
                chain_only,
            }) => {
                assert_eq!(db_only, vec![4]);
                assert_eq!(chain_only, vec![6]);
            }
            other => panic!("expected BitmapDivergence, got: {other}"),
        }
    }

    /// A release landing between the bitmap read and the database read looks
    /// exactly like database-ahead. The second read must be taken and believed,
    /// or every such race becomes a spurious refusal to start.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_rereads_before_halting() {
        let mut server = mockito::Server::new_async().await;
        // First read predates the release, second sees it. That is the race.
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(0, vec![]), (0, vec![4])]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(8);

        let result = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await;

        assert!(
            result.is_ok(),
            "a clean re-read must clear the divergence: {result:?}"
        );
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the halt direction must consult the bitmap a second time"
        );
        assert!(storage_rx.try_recv().is_err(), "nothing to repair");
    }

    /// A divergence that survives the re-read is real and must still halt, or
    /// the race handling would swallow every genuine one.
    #[tokio::test]
    async fn validate_bitmap_consistency_db_ahead_halts_after_reread_confirms() {
        let mut server = mockito::Server::new_async().await;
        let (_bitmap, reads) = mock_bitmap_sequence(&mut server, vec![(0, vec![])]);

        let mock = MockStorage::new();
        seed_withdrawal(&mock, 5, 4, TransactionStatus::Completed, None);
        let state = sender_state_with_storage(&server.url(), mock);
        let (storage_tx, _rx) = mpsc::channel(8);

        let err = super::validate_bitmap_consistency(
            &state.storage,
            &state.rpc_client,
            Some(Pubkey::new_unique()),
            &storage_tx,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            OperatorError::Program(crate::error::ProgramError::BitmapDivergence { .. })
        ));
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
