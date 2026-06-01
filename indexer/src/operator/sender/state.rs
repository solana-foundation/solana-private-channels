use crate::channel_utils::send_guaranteed;
use crate::error::account::AccountError;
use crate::error::OperatorError;
use crate::operator::sender::types::{PendingRemint, TransactionContext};
use crate::operator::tree_constants::MAX_TREE_LEAVES;
use crate::operator::utils::smt_util::SmtState;
use crate::operator::{parse_instance, RetryConfig, RpcClientWithRetry};
use crate::operator::{MintCache, TransactionStatusUpdate, WithdrawalRemintInfo};
use crate::storage::common::storage::Storage;
use crate::storage::TransactionStatus;
use crate::PrivateChannelIndexerConfig;
use chrono::Utc;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{error, info};

use super::types::{InFlightQueue, SenderSMTState, SenderState, MAX_IN_FLIGHT};

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
        let mint_cache = MintCache::with_rpc(storage.clone(), mint_rpc_client);

        Ok(Self {
            rpc_client,
            storage,
            instance_pda,
            smt_state: None,
            retry_counts: HashMap::new(),
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

    /// Initialize SMT state lazily on first use
    /// Fetches tree_index from chain and populates SMT with completed withdrawals from DB
    pub(super) async fn initialize_smt_state(&mut self) -> Result<(), OperatorError> {
        let instance_pda = self
            .instance_pda
            .ok_or_else(|| AccountError::InstanceNotFound {
                instance: Pubkey::default(),
            })?;

        info!("Initializing SMT state for instance {}", instance_pda);

        let instance_data = self
            .rpc_client
            .get_account_data(&instance_pda)
            .await
            .map_err(|_| AccountError::AccountNotFound {
                pubkey: instance_pda,
            })?;

        let instance = parse_instance(&instance_data).map_err(|e| {
            AccountError::AccountDeserializationFailed {
                pubkey: instance_pda,
                reason: e.to_string(),
            }
        })?;

        let tree_index = instance.current_tree_index;
        let min_nonce = tree_index * MAX_TREE_LEAVES as u64;
        let max_nonce = (tree_index + 1) * MAX_TREE_LEAVES as u64;

        // Fetch completed withdrawal nonces for current tree from DB
        let nonces = self
            .storage
            .get_completed_withdrawal_nonces(min_nonce, max_nonce)
            .await?;

        // Create SMT and populate with existing nonces
        let mut smt_state = SmtState::new(tree_index);
        for nonce in &nonces {
            smt_state.insert_nonce(*nonce);
        }

        info!(
            "SMT state initialized with tree_index {}, populated {} existing nonces",
            tree_index,
            nonces.len()
        );

        // CRITICAL: Verify local SMT root matches on-chain root
        // This ensures database is in sync with on-chain state
        let computed_root = smt_state.current_root();
        let onchain_root = instance.withdrawal_transactions_root;

        if computed_root != onchain_root {
            error!("SMT root mismatch detected! Database out of sync with on-chain state.");
            error!("  Instance PDA: {}", instance_pda);
            error!("  Tree Index: {}", tree_index);
            error!("  Nonces from DB: {:?}", nonces);
            error!("  Local root:    {:?}", computed_root);
            error!("  On-chain root: {:?}", onchain_root);
            error!("");
            error!("This typically means:");
            error!("  1. A withdrawal was successfully processed on-chain");
            error!("  2. But the operator crashed before updating the database");
            error!("  3. The database is now missing transaction records");
            error!("");
            error!("Resolution options:");
            error!("  1. Reset and resync the database from on-chain events");
            error!("  2. Manually reconcile missing transactions");
            error!("  3. Reset the on-chain SMT tree (requires admin)");

            return Err(crate::error::ProgramError::SmtRootMismatch {
                local_root: computed_root,
                onchain_root,
            }
            .into());
        }

        info!("SMT root verification passed: {:?}", computed_root);

        self.smt_state = Some(SenderSMTState {
            smt_state,
            nonce_to_builder: HashMap::new(),
        });

        Ok(())
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

            let Some(user) = Self::or_manual_review(
                Pubkey::from_str(&tx.recipient).map_err(|e| format!("invalid user pubkey: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            let user_ata = get_associated_token_address_with_program_id(
                &user,
                &mint,
                &private_channel_token_program,
            );

            // u64::try_from catches negative amounts. The write path already guards
            // against this (ba77249) but a corrupt DB row could still produce one —
            // casting a negative i64 to u64 would produce a massive spurious remint.
            let Some(amount) = Self::or_manual_review(
                u64::try_from(tx.amount).map_err(|_| format!("negative amount: {}", tx.amount)),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            // Parse all stored withdrawal signatures. These are passed to
            // get_signature_statuses_with_history() by process_pending_remints to verify
            // the withdrawal did not finalize before we remint. A single bad entry means
            // we cannot safely do that check — escalate to ManualReview.
            let sig_strings = tx.remint_signatures.unwrap_or_default();
            let Some(signatures) = Self::or_manual_review(
                sig_strings
                    .iter()
                    .map(|s| Signature::from_str(s))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("invalid withdrawal signature: {e}")),
                storage_tx,
                tx.id,
                &tx.trace_id,
            )
            .await
            else {
                continue;
            };

            // Restore the original deadline. Fall back to now() if missing (shouldn't
            // happen) so the entry fires on the next tick instead of waiting 32s more.
            let deadline = tx.pending_remint_deadline_at.unwrap_or_else(Utc::now);

            let ctx = TransactionContext {
                transaction_id: Some(tx.id),
                // Nonce is not needed for the remint — SMT cleanup already ran in
                // handle_permanent_failure before the row was written as PendingRemint.
                withdrawal_nonce: tx.withdrawal_nonce.map(|n| n as u64),
                trace_id: Some(tx.trace_id.clone()),
            };

            let remint_info = WithdrawalRemintInfo {
                transaction_id: tx.id,
                trace_id: tx.trace_id.clone(),
                mint,
                user,
                user_ata,
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

            self.pending_remints.push(PendingRemint {
                ctx,
                remint_info,
                signatures,
                // The original error string is not stored in DB. Only surfaced in
                // combined error messages if the remint itself also fails.
                original_error: "recovered from persistent storage".to_string(),
                deadline,
                finality_check_attempts: 0,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MintCache;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Signature;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    fn make_sender_state(mock: MockStorage) -> SenderState {
        let storage = Arc::new(Storage::Mock(mock));
        let rpc = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
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

    /// Build a minimal DbTransaction representing a PendingRemint row.
    /// All string fields use real base58-encoded pubkeys and signatures so
    /// `recover_pending_remints` can parse them without error.
    fn make_pending_remint_row(
        id: i64,
        mint: &Pubkey,
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
            initiator: Pubkey::new_unique().to_string(),
            recipient: recipient.to_string(),
            mint: mint.to_string(),
            amount: 5_000,
            memo: None,
            transaction_type: TransactionType::Withdrawal,
            withdrawal_nonce: Some(id),
            status: TransactionStatus::PendingRemint,
            created_at: now,
            updated_at: now,
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: Some(vec![sig.to_string()]),
            pending_remint_deadline_at: Some(deadline),
        }
    }

    // ── recover_pending_remints: happy path ──────────────────────────

    /// On startup, all PendingRemint rows from the database must be fully
    /// reconstructed into the in-memory `pending_remints` queue so the
    /// operator can continue where it left off before the crash.
    ///
    /// This test verifies that every field is correctly restored:
    /// - transaction_id, trace_id, amount, mint, recipient
    /// - withdrawal signatures (needed for the finality check)
    /// - the original deadline (not a fresh 32s window — the clock keeps
    ///   ticking across restarts)
    /// - finality_check_attempts reset to 0 (not stored in DB)
    ///
    /// No channel messages should be sent — there is nothing wrong with
    /// these rows, they just need to be re-queued.
    #[tokio::test]
    async fn recover_pending_remints_rehydrates_queue() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        // Simulate the row that would exist in the DB after a crash.
        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(make_pending_remint_row(
                42, &mint, &recipient, &sig, deadline,
            ));

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
        assert_eq!(entry.remint_info.user, recipient);

        // Signatures must be parsed back — they drive the finality check.
        assert_eq!(entry.signatures.len(), 1);
        assert_eq!(entry.signatures[0], sig);

        // Deadline must be the stored one, not a fresh window.
        // Allows up to 1s of clock skew between DB write and assertion.
        assert!(
            (entry.deadline - deadline).num_milliseconds().abs() < 1_000,
            "deadline should be restored from DB, got {:?}",
            entry.deadline
        );

        // Attempt counter always resets — it is in-memory only.
        assert_eq!(entry.finality_check_attempts, 0);

        // Standard recovery marker so combined error messages are meaningful.
        assert_eq!(entry.original_error, "recovered from persistent storage");

        // No status update sent — valid rows are silently re-queued.
        assert!(
            storage_rx.try_recv().is_err(),
            "no channel message expected for a valid recovery row"
        );
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
        let mut bad_row =
            make_pending_remint_row(10, &Pubkey::new_unique(), &recipient, &sig, deadline);
        bad_row.mint = "not-a-valid-pubkey".to_string();

        // Row 2: valid — must still be recovered despite the bad row above.
        let good_mint = Pubkey::new_unique();
        let good_row = make_pending_remint_row(11, &good_mint, &recipient, &sig, deadline);

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

    /// A corrupted recipient pubkey cannot be parsed into a `Pubkey`, so the
    /// operator cannot compute the user's ATA and has no valid destination
    /// for the remint.
    ///
    /// Same escalation rule as invalid mint: ManualReview immediately, do not
    /// skip silently, do not block other rows.
    #[tokio::test]
    async fn recover_pending_remints_escalates_invalid_recipient_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(20, &mint, &Pubkey::new_unique(), &sig, deadline);
        bad_row.recipient = "not-a-valid-pubkey".to_string();

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for bad recipient");
        assert_eq!(update.transaction_id, 20);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("invalid user pubkey"),
            "error message should describe the parse failure: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "bad row must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

    /// A negative amount in a PendingRemint row must never be cast to u64.
    /// `i64` to `u64` with `as` would silently wrap: `-1_i64 as u64` produces
    /// `18_446_744_073_709_551_615` — a remint of the entire token supply.
    ///
    /// The write path guards against this, but a corrupted DB row could still
    /// produce a negative value. `u64::try_from` catches it and the operator
    /// must escalate to ManualReview rather than execute a catastrophic remint.
    #[tokio::test]
    async fn recover_pending_remints_escalates_negative_amount_to_manual_review() {
        let mock = MockStorage::new();
        let mint = Pubkey::new_unique();
        let recipient = Pubkey::new_unique();
        let sig = Signature::new_unique();
        let deadline = Utc::now() + chrono::Duration::seconds(20);

        let mut bad_row = make_pending_remint_row(30, &mint, &recipient, &sig, deadline);
        bad_row.amount = -1;

        mock.pending_remint_transactions
            .lock()
            .unwrap()
            .push(bad_row);

        let mut state = make_sender_state(mock);
        let (storage_tx, mut storage_rx) = mpsc::channel(10);

        state.recover_pending_remints(&storage_tx).await.unwrap();

        let update = storage_rx
            .try_recv()
            .expect("should receive ManualReview for negative amount");
        assert_eq!(update.transaction_id, 30);
        assert_eq!(update.status, TransactionStatus::ManualReview);
        let err = update.error_message.as_deref().unwrap_or("");
        assert!(
            err.contains("negative amount"),
            "error message should describe the negative amount: {err}"
        );

        assert!(
            state.pending_remints.is_empty(),
            "negative-amount row must not be queued"
        );
        assert!(storage_rx.try_recv().is_err());
    }

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

        let mut bad_row =
            make_pending_remint_row(40, &mint, &recipient, &Signature::new_unique(), deadline);
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
    use crate::operator::utils::rpc_util::{RetryConfig, RpcClientWithRetry};
    use std::sync::Arc;

    fn make_sender_state_with_pda(pda: Option<Pubkey>) -> SenderState {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let rpc_client = Arc::new(RpcClientWithRetry::with_retry_config(
            "http://localhost:8899".to_string(),
            RetryConfig::default(),
            CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            },
        ));
        SenderState {
            rpc_client: rpc_client.clone(),
            storage: storage.clone(),
            instance_pda: pda,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            pending_rotation: None,
            program_type: ProgramType::Escrow,
            remint_cache: HashMap::new(),
            pending_signatures: HashMap::new(),
            pending_remints: Vec::new(),
            in_flight: InFlightQueue::new(),
            semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    fn make_config() -> PrivateChannelIndexerConfig {
        PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: "http://localhost:8899".to_string(),
            source_rpc_url: None,
            postgres: PostgresConfig {
                database_url: "postgresql://localhost/test".to_string(),
                max_connections: 5,
            },
            escrow_instance_id: None,
        }
    }

    /// `initialize_smt_state` requires a PDA to look up the on-chain SMT root; without one
    /// it must return an `AccountError::InstanceNotFound` wrapped as `OperatorError::Account`.
    #[tokio::test]
    async fn initialize_smt_state_fails_without_instance_pda() {
        let mut state = make_sender_state_with_pda(None);

        let result = state.initialize_smt_state().await;
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                OperatorError::Account(crate::error::AccountError::InstanceNotFound { .. })
            ),
            "expected OperatorError::Account(InstanceNotFound), got: {err}"
        );
    }

    /// `SenderState::new` with no instance PDA and Escrow program type must succeed and leave
    /// SMT state uninitialised (it is lazily loaded on first use).
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
        assert!(state.smt_state.is_none());
        assert_eq!(state.retry_max_attempts, 3);
        assert_eq!(state.program_type, ProgramType::Escrow);
    }

    /// Providing an instance PDA and a higher retry limit must be reflected in the constructed
    /// state; the PDA is stored as-is for later SMT initialisation.
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
}
