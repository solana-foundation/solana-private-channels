use crate::channel_utils::send_guaranteed;
use crate::error::account::AccountError;
use crate::error::OperatorError;
use crate::operator::sender::types::{PendingRemint, PendingSig, TransactionContext};
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
        let mint_cache = MintCache::with_rpc(storage.clone(), mint_rpc_client.clone());

        Ok(Self {
            rpc_client,
            // Source chain client (also used by MintCache). Remints broadcast here.
            source_rpc_client: mint_rpc_client,
            storage,
            instance_pda,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_cache,
            mint_builders: HashMap::new(),
            retry_max_attempts,
            confirmation_poll_interval_ms,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
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
        let smt_state =
            validate_smt_root(&self.storage, &self.rpc_client, self.instance_pda).await?;

        self.smt_state = Some(SenderSMTState {
            smt_state,
            nonce_to_builder: HashMap::new(),
        });

        Ok(())
    }
}

/// Build the local SMT for the current tree window from DB-completed nonces and
/// assert it matches the on-chain root, returning the built tree on agreement.
///
/// Shared by the sender's lazy `initialize_smt_state` (which needs the tree for
/// proofs) and the boot pre-flight (which uses it purely as a consistency gate).
pub(crate) async fn validate_smt_root(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    instance_pda: Option<Pubkey>,
) -> Result<SmtState, OperatorError> {
    let instance_pda = instance_pda.ok_or_else(|| AccountError::InstanceNotFound {
        instance: Pubkey::default(),
    })?;

    info!("Validating SMT root for instance {}", instance_pda);

    let instance_data = rpc_client
        .get_account_data(&instance_pda)
        .await
        .map_err(|_| AccountError::AccountNotFound {
            pubkey: instance_pda,
        })?;

    let instance =
        parse_instance(&instance_data).map_err(|e| AccountError::AccountDeserializationFailed {
            pubkey: instance_pda,
            reason: e.to_string(),
        })?;

    let tree_index = instance.current_tree_index;
    let min_nonce = tree_index * MAX_TREE_LEAVES as u64;
    let max_nonce = (tree_index + 1) * MAX_TREE_LEAVES as u64;

    let nonces = storage
        .get_completed_withdrawal_nonces(min_nonce, max_nonce)
        .await?;

    let mut smt_state = SmtState::new(tree_index);
    for nonce in &nonces {
        smt_state.insert_nonce(*nonce);
    }

    let computed_root = smt_state.current_root();
    let onchain_root = instance.withdrawal_transactions_root;

    if computed_root != onchain_root {
        error!(
            instance = %instance_pda,
            tree_index,
            local_root = ?computed_root,
            onchain_root = ?onchain_root,
            nonces = ?nonces,
            "SMT root mismatch: database out of sync with on-chain state. \
             A release likely landed on-chain but its Completed write was lost; \
             resync the database from on-chain events to reconcile."
        );

        return Err(crate::error::ProgramError::SmtRootMismatch {
            local_root: computed_root,
            onchain_root,
        }
        .into());
    }

    info!(
        tree_index,
        nonces = nonces.len(),
        "SMT root verification passed"
    );

    Ok(smt_state)
}

impl SenderState {
    /// Read the authoritative current_tree_index from the on-chain instance.
    pub(super) async fn fetch_onchain_tree_index(&self) -> Result<u64, OperatorError> {
        let instance_pda = self.instance_pda.ok_or(AccountError::InstanceNotFound {
            instance: Pubkey::default(),
        })?;
        let data = self
            .rpc_client
            .get_account_data(&instance_pda)
            .await
            .map_err(|_| AccountError::AccountNotFound {
                pubkey: instance_pda,
            })?;
        let instance =
            parse_instance(&data).map_err(|e| AccountError::AccountDeserializationFailed {
                pubkey: instance_pda,
                reason: e.to_string(),
            })?;
        Ok(instance.current_tree_index)
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
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::MintCache;
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::models::{DbTransaction, TransactionStatus, TransactionType};
    use crate::storage::common::storage::mock::MockStorage;
    use crate::storage::Storage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use private_channel_escrow_program_client::Instance;
    use solana_client::rpc_request::RpcRequest;
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
            rpc_client: rpc.clone(),
            source_rpc_client: rpc,
            storage: storage.clone(),
            instance_pda: None,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
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
    /// - finality_check_attempts round-trips from the DB so the
    ///   MAX_FINALITY_CHECK_ATTEMPTS budget survives restarts
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

        // Mid-budget value so the round-trip assertion is meaningful: a reset
        // to 0 on recovery would re-arm the cap and let an ambiguous row
        // outlive the intended ManualReview escalation.
        let mut row = make_pending_remint_row(42, &mint, &recipient, &sig, deadline);
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
        assert_eq!(entry.remint_info.user, recipient);

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

        let mut row = make_pending_remint_row(7, &mint, &recipient, &sig, deadline);
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

        let mut bad_row =
            make_pending_remint_row(50, &mint, &recipient, &Signature::new_unique(), deadline);
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
            source_rpc_client: rpc_client.clone(),
            storage: storage.clone(),
            instance_pda: pda,
            smt_state: None,
            retry_counts: HashMap::new(),
            mint_builders: HashMap::new(),
            mint_cache: MintCache::new(storage),
            retry_max_attempts: 3,
            confirmation_poll_interval_ms: 400,
            rotation_retry_queue: Vec::new(),
            ambiguous_retry_queue: Vec::new(),
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

    /// `validate_smt_root` without a PDA must return `AccountError::InstanceNotFound` (as `OperatorError::Account`).
    #[tokio::test]
    async fn validate_smt_root_fails_without_instance_pda() {
        let state = make_sender_state_with_pda(None);

        let result = super::validate_smt_root(&state.storage, &state.rpc_client, None).await;
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

    /// Pins the SmtRootMismatch wedge: a landed release whose nonce never reaches
    /// `Completed` leaves the DB one nonce behind the chain, so `validate_smt_root`
    /// MUST diverge and return `Err(SmtRootMismatch)`. A change that silently
    /// absorbs it breaks here.
    #[tokio::test]
    async fn validate_smt_root_halts_on_consumed_but_unrecorded_nonce() {
        let landed_nonce: u64 = 1;
        let tree_index: u64 = 0;

        // On-chain root = root of an SMT that DOES include the landed nonce.
        let mut onchain_tree = SmtState::new(tree_index);
        onchain_tree.insert_nonce(landed_nonce);
        let onchain_root = onchain_tree.current_root();

        // Craft the Instance account the operator will fetch on boot, carrying
        // the advanced on-chain root.
        let instance = Instance {
            discriminator: 0,
            bump: 0,
            version: 0,
            instance_seed: Pubkey::new_unique(),
            admin: Pubkey::new_unique(),
            withdrawal_transactions_root: onchain_root,
            current_tree_index: tree_index,
        };
        let mut instance_bytes = Vec::new();
        instance.serialize(&mut instance_bytes).unwrap();

        // Mock getAccountInfo to return that crafted Instance account.
        let account_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": Pubkey::new_unique().to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(&instance_bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        let mock_rpc = RpcClientWithRetry {
            rpc_client: Arc::new(
                solana_client::nonblocking::rpc_client::RpcClient::new_mock_with_mocks(
                    "http://127.0.0.1:8899".to_string(),
                    mocks,
                ),
            ),
            retry_config: RetryConfig::default(),
        };

        // DB returns NO completed nonces — the landed nonce was never recorded.
        // This is the divergence: chain has the nonce, DB does not.
        let mut state = make_sender_state_with_pda(Some(Pubkey::new_unique()));
        state.rpc_client = Arc::new(mock_rpc);

        let err = super::validate_smt_root(&state.storage, &state.rpc_client, state.instance_pda)
            .await
            .unwrap_err();

        match err {
            OperatorError::Program(crate::error::ProgramError::SmtRootMismatch {
                local_root,
                onchain_root: reported_onchain,
            }) => {
                // The local (DB-derived, empty) root must differ from the
                // advanced on-chain root, and the reported on-chain root must
                // be the one carrying the consumed nonce.
                assert_ne!(
                    local_root, reported_onchain,
                    "mismatch must show diverging roots"
                );
                assert_eq!(
                    reported_onchain, onchain_root,
                    "on-chain root must be the one that included the landed nonce"
                );
                assert_eq!(
                    local_root,
                    SmtState::new(tree_index).current_root(),
                    "local root must be the empty-tree root (nonce never recorded)"
                );
            }
            other => panic!("expected SmtRootMismatch, got: {other}"),
        }
    }
}
