use crate::metrics;
use crate::{
    channel_utils::send_guaranteed,
    config::ProgramType,
    error::{IndexerError, StorageError},
    indexer::{
        checkpoint::{CheckpointMsg, CheckpointUpdate},
        datasource::common::{
            parser::{escrow_instance_of, EscrowInstruction, WithdrawInstruction},
            types::{InstructionWithMetadata, ProcessorMessage, ProgramInstruction},
        },
    },
    operator::{instruction_util::SourceEventId, ConsumedMintKind, ConsumedSet},
    storage::{
        common::models::{
            DbMint, DbMintStatus, DbObservedRelease, DbTransaction, DbTransactionBuilder,
            TransactionStatus, TransactionType,
        },
        Storage,
    },
};
use private_channel_metrics::{HealthState, MetricLabel};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Production defaults for the per-slot DB-write retry. Sized to ride out a
/// routine Postgres restart or failover (about 15s of cumulative backoff)
/// before the processor gives up and exits so a restart can replay the slot.
const DEFAULT_WRITE_MAX_ATTEMPTS: usize = 6;
const DEFAULT_WRITE_BASE_DELAY: Duration = Duration::from_millis(500);
const DEFAULT_WRITE_MAX_DELAY: Duration = Duration::from_secs(8);

/// Bounded exponential-backoff policy for a slot's DB writes.
#[derive(Clone, Copy, Debug)]
pub struct WriteRetryPolicy {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for WriteRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_WRITE_MAX_ATTEMPTS,
            base_delay: DEFAULT_WRITE_BASE_DELAY,
            max_delay: DEFAULT_WRITE_MAX_DELAY,
        }
    }
}

impl WriteRetryPolicy {
    /// Backoff before the next attempt: `base_delay * 2^(attempt-1)`, capped at
    /// `max_delay`. A zero base delay yields a zero wait (used by tests).
    fn backoff(&self, attempt: usize) -> Duration {
        let shift = attempt.saturating_sub(1).min(31) as u32;
        self.base_delay
            .saturating_mul(1u32 << shift)
            .min(self.max_delay)
    }
}

/// Transaction processor that converts instructions to transactions and saves to DB
/// Tracks slot-level success/failure and emits committed checkpoints
///
/// Current implementation: Sequential slot processing with batch inserts per slot (Option 3)
pub struct TransactionProcessor {
    storage: Arc<Storage>,
    checkpoint_tx: mpsc::Sender<CheckpointMsg>,

    // Per-slot instruction buffers, so a foreign SlotComplete finalizes only its own slot's rows.
    slot_buffers: HashMap<u64, Vec<InstructionWithMetadata>>,

    // Optional health state — bumped on each SlotComplete so /health knows the
    // indexer pipeline is making progress. None in tests / standalone uses.
    health: Option<Arc<HealthState>>,

    configured_escrow_instance_id: Option<Pubkey>,

    // Bounded-backoff policy applied to a slot's DB writes before the failure
    // is treated as fatal.
    retry: WriteRetryPolicy,
    // Set only on a reconciling resync. When present, the per-slot insert rebuilds an
    // already-serviced deposit/remint into its terminal status instead of `pending`.
    // `None` on every normal/backfill/live path, so the hot path is unchanged.
    consumed: Option<Arc<ConsumedSet>>,
}

impl TransactionProcessor {
    pub fn new(storage: Arc<Storage>, checkpoint_tx: mpsc::Sender<CheckpointMsg>) -> Self {
        Self {
            storage,
            checkpoint_tx,
            slot_buffers: HashMap::new(),
            health: None,
            configured_escrow_instance_id: None,
            retry: WriteRetryPolicy::default(),
            consumed: None,
        }
    }

    pub fn with_health(mut self, health: Arc<HealthState>) -> Self {
        self.health = Some(health);
        self
    }

    pub fn with_write_retry(mut self, retry: WriteRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_escrow_instance_id(mut self, escrow_instance_id: Pubkey) -> Self {
        self.configured_escrow_instance_id = Some(escrow_instance_id);
        self
    }

    /// Inject the pre-drop consumed-set so the rebuild reconciles each row in place.
    pub fn with_consumed_set(mut self, consumed: Arc<ConsumedSet>) -> Self {
        self.consumed = Some(consumed);
        self
    }

    /// Rewrite a serviced deposit/remint row to its terminal status from the consumed-set
    /// so the rebuilt table never contains a serviceable `pending` row for an event the
    /// channel already minted. No-op when no consumed-set is configured.
    fn reconcile_against_consumed(&self, slot: u64, transactions: &mut [DbTransaction]) {
        let Some(consumed) = self.consumed.as_ref() else {
            return;
        };
        let (mut completed, mut reminted, mut pending) = (0u64, 0u64, 0u64);
        for transaction in transactions.iter_mut() {
            let id = SourceEventId::from_row(transaction);
            match consumed.get(&id) {
                Some((signature, ConsumedMintKind::Deposit))
                    if transaction.transaction_type == TransactionType::Deposit =>
                {
                    transaction.status = TransactionStatus::Completed;
                    transaction.counterpart_signature = Some(signature.to_string());
                    completed += 1;
                }
                Some((signature, ConsumedMintKind::Remint))
                    if transaction.transaction_type == TransactionType::Withdrawal =>
                {
                    transaction.status = TransactionStatus::FailedReminted;
                    transaction.landed_remint_signature = Some(signature.to_string());
                    reminted += 1;
                }
                // Not serviced (or a kind/type mismatch we refuse to act on): stays pending.
                _ => pending += 1,
            }
        }
        if completed + reminted > 0 {
            info!(
                slot,
                completed_from_chain = completed,
                failed_reminted_from_chain = reminted,
                pending,
                "Resync reconcile-in-place applied"
            );
        }
    }

    /// Start processing messages from the channel
    pub async fn start(
        mut self,
        mut instruction_rx: mpsc::Receiver<ProcessorMessage>,
    ) -> Result<(), IndexerError> {
        info!("Starting TransactionProcessor");

        while let Some(message) = instruction_rx.recv().await {
            match message {
                ProcessorMessage::Instruction(instruction_meta) => {
                    self.slot_buffers
                        .entry(instruction_meta.slot)
                        .or_default()
                        .push(instruction_meta);
                }
                ProcessorMessage::SlotComplete { slot, program_type } => {
                    let start = std::time::Instant::now();
                    self.finalize_and_checkpoint(slot, program_type).await?;
                    metrics::INDEXER_SLOT_PROCESSING_DURATION
                        .with_label_values(&[program_type.as_label()])
                        .observe(start.elapsed().as_secs_f64());
                    if let Some(h) = &self.health {
                        h.record_progress();
                    }
                }
                ProcessorMessage::Regate {
                    program_type,
                    from,
                    target,
                } => {
                    // Forward the gate re-arm in-band, ahead of the slot it precedes. No
                    // DB write and no health bump: it is a control signal, not slot
                    // progress. A send failure means the writer is gone, so exit.
                    if send_guaranteed(
                        &self.checkpoint_tx,
                        CheckpointMsg::Regate {
                            program_type,
                            from,
                            target,
                        },
                        "regate",
                    )
                    .await
                    .is_err()
                    {
                        error!(
                            "Regate send failed for {:?} target {}",
                            program_type, target
                        );
                        return Err(IndexerError::CheckpointChannelClosed);
                    }
                }
            }
        }

        info!("TransactionProcessor stopped");
        Ok(())
    }

    /// Finalize and checkpoint a slot
    /// Saves any buffered transactions and always sends checkpoint (even if empty)
    ///
    /// Slot's write fail is a fatal error so the caller exits and restart replays
    /// the slot from the last durable checkpoint.
    async fn finalize_and_checkpoint(
        &mut self,
        slot: u64,
        program_type: ProgramType,
    ) -> Result<(), IndexerError> {
        let mut mints = Vec::new();
        let mut mint_statuses: Vec<DbMintStatus> = Vec::new();
        let mut transactions = Vec::new();
        let mut observed_releases: Vec<DbObservedRelease> = Vec::new();

        let slot_instructions = self.slot_buffers.remove(&slot).unwrap_or_default();
        for instruction_meta in &slot_instructions {
            let (mint_opt, status_opt, transaction_opt, release_opt) = convert_to_db_models(
                instruction_meta,
                self.configured_escrow_instance_id.as_ref(),
            );

            if let Some(change) = status_opt {
                if let Some(sig) = instruction_meta.signature.clone() {
                    mint_statuses.push(DbMintStatus {
                        mint_address: change.mint_address,
                        status: change.status.as_str().to_string(),
                        effective_slot: slot as i64,
                        signature: sig,
                        created_at: chrono::Utc::now(),
                    });
                }
            }

            if let Some(mint) = mint_opt {
                mints.push(mint);
            }

            if let Some(transaction) = transaction_opt {
                transactions.push(transaction);
            }

            if let Some(release) = release_opt {
                observed_releases.push(release);
            }
        }

        // Reconcile rebuilt rows against already-serviced channel mints (resync only).
        // Mutates `transactions` to terminal status in place BEFORE the write, so the
        // rebuilt table never persists a serviceable `pending` row for a serviced event.
        self.reconcile_against_consumed(slot, &mut transactions);

        // Retry the whole write sequence on failure; every write is idempotent.
        let mut attempt = 1;
        loop {
            match self
                .write_slot(
                    slot,
                    program_type,
                    &mints,
                    &mint_statuses,
                    &transactions,
                    &observed_releases,
                )
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    if attempt >= self.retry.max_attempts {
                        error!(
                            "Slot {} writes failed after {} attempt(s); giving up: {}",
                            slot, attempt, e
                        );
                        // Count the slot once, only when the retry budget is spent;
                        // a transient failure ridden out by the retry is not an error.
                        metrics::INDEXER_SLOT_SAVE_ERRORS
                            .with_label_values(&[program_type.as_label()])
                            .inc();
                        return Err(e.into());
                    }
                    let backoff = self.retry.backoff(attempt);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                    attempt += 1;
                }
            }
        }

        // Count saved mints once here rather than in write_slot, which may
        // re-run the idempotent mints upsert across retries.
        if !mints.is_empty() {
            metrics::INDEXER_MINTS_SAVED
                .with_label_values(&[program_type.as_label()])
                .inc_by(mints.len() as f64);
        }

        // Send the checkpoint only after the writes commit. send_guaranteed
        // reserves capacity first, so it errors only when the checkpoint writer
        // is gone; that never recovers, so exit and let a restart rebuild the
        // pipeline instead of retrying a closed channel.
        match send_guaranteed(
            &self.checkpoint_tx,
            CheckpointMsg::Slot(CheckpointUpdate { program_type, slot }),
            "checkpoint",
        )
        .await
        {
            Ok(_) => {
                metrics::INDEXER_SLOTS_PROCESSED
                    .with_label_values(&[program_type.as_label()])
                    .inc();
                metrics::INDEXER_CURRENT_SLOT
                    .with_label_values(&[program_type.as_label()])
                    .set(slot as f64);
                Ok(())
            }
            Err(e) => {
                error!("Checkpoint send failed for slot {}: {}", slot, e);
                Err(IndexerError::CheckpointChannelClosed)
            }
        }
    }

    /// Run one attempt of a slot's DB writes in order: mints, then mint-status
    /// history, then the status mirror, then transactions. Short-circuits on the
    /// first failing write so a deposit is never inserted without its backing
    /// mint-status row.
    async fn write_slot(
        &self,
        slot: u64,
        program_type: ProgramType,
        mints: &[DbMint],
        mint_statuses: &[DbMintStatus],
        transactions: &[DbTransaction],
        observed_releases: &[DbObservedRelease],
    ) -> Result<(), StorageError> {
        // Insert mints FIRST (before transactions that might reference them)
        if !mints.is_empty() {
            info!("Finalizing slot {} with {} mint(s)", slot, mints.len());

            match self.storage.upsert_mints_batch(mints).await {
                Ok(_) => {
                    info!(
                        "Successfully saved {} mint(s) from slot {}",
                        mints.len(),
                        slot
                    );
                }
                Err(e) => {
                    error!("Failed to save mints from slot {}: {}", slot, e);
                    return Err(e);
                }
            }
        }

        if !mint_statuses.is_empty() {
            match self.storage.insert_mint_statuses_batch(mint_statuses).await {
                Ok(_) => {
                    info!(
                        "Successfully saved {} mint status row(s) from slot {}",
                        mint_statuses.len(),
                        slot,
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to save mint status history from slot {}: {}",
                        slot, e
                    );
                    return Err(e);
                }
            }

            // Derive the `mints.status` mirror for each touched mint from history.
            // Gated on the writes above, so the mirror never leads the timeline.
            let mut touched: Vec<String> = mint_statuses
                .iter()
                .map(|s| s.mint_address.clone())
                .collect();
            touched.sort_unstable();
            touched.dedup();
            if let Err(e) = self.storage.sync_mint_status(&touched).await {
                error!("Failed to sync mint status mirror for slot {}: {}", slot, e);
                return Err(e);
            }
        }

        // Written inside the retried attempt, before the checkpoint can advance
        // past this slot: that is what lets a committed checkpoint stand for
        // release coverage. Returning Err fails the slot rather than withholding
        // the checkpoint, so the next slot cannot leapfrog an unrecorded one.
        if !observed_releases.is_empty() {
            match self
                .storage
                .insert_observed_releases_batch(observed_releases)
                .await
            {
                Ok(_) => {
                    info!(
                        "Recorded {} observed release(s) from slot {}",
                        observed_releases.len(),
                        slot
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to record observed releases for slot {}: {}",
                        slot, e
                    );
                    return Err(e);
                }
            }
        }

        if transactions.is_empty() {
            // Empty slot, just checkpoint it
            debug!("Finalizing empty slot {}", slot);
        } else {
            info!(
                "Finalizing slot {} with {} transactions",
                slot,
                transactions.len()
            );

            match self
                .storage
                .insert_db_transactions_batch(transactions)
                .await
            {
                Ok(ids) => {
                    info!(
                        "Successfully saved {} transactions from slot {}",
                        ids.len(),
                        slot
                    );
                    metrics::INDEXER_TRANSACTIONS_SAVED
                        .with_label_values(&[program_type.as_label()])
                        .inc_by(ids.len() as f64);
                }
                Err(e) => {
                    error!("Failed to save transactions from slot {}: {}", slot, e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn buffer(&mut self, ix: InstructionWithMetadata) {
        self.slot_buffers.entry(ix.slot).or_default().push(ix);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MintStatus {
    Allowed,
    Blocked,
}

impl MintStatus {
    /// The string stored in the `status` columns.
    fn as_str(self) -> &'static str {
        match self {
            MintStatus::Allowed => "allowed",
            MintStatus::Blocked => "blocked",
        }
    }
}

/// A mint allow/block transition to record in `mint_status_history`.
struct MintStatusChange {
    mint_address: String,
    status: MintStatus,
}

/// Convert an instruction to a `(DbMint, MintStatusChange, DbTransaction,
/// DbObservedRelease)` tuple, each element independently optional:
/// - `AllowMint` → mints-row upsert + `"allowed"` transition.
/// - `BlockMint` → `"blocked"` transition only (mints row already exists).
/// - `Deposit` / `WithdrawFunds` → transaction row only.
/// - `ReleaseFunds` yields an observed-release row only.
///
/// Returns all-`None` for untracked instructions and for escrow instructions
/// whose `accounts.instance` doesn't match the configured instance — the
/// per-instruction scoping that keeps a foreign instance from being persisted.
fn convert_to_db_models(
    instruction_meta: &InstructionWithMetadata,
    configured_escrow_instance_id: Option<&Pubkey>,
) -> (
    Option<DbMint>,
    Option<MintStatusChange>,
    Option<DbTransaction>,
    Option<DbObservedRelease>,
) {
    let signature = match instruction_meta.signature.as_ref() {
        Some(sig) => sig,
        None => return (None, None, None, None),
    };

    match &instruction_meta.instruction {
        ProgramInstruction::Escrow(escrow_ix) => {
            // Drop any escrow ix not scoped to the configured instance.
            // `None` configured => drop all (fail-closed).
            if configured_escrow_instance_id != Some(&escrow_instance_of(escrow_ix)) {
                debug!(
                    ix = ?escrow_ix,
                    configured = ?configured_escrow_instance_id,
                    "dropping escrow instruction: instance mismatch"
                );
                return (None, None, None, None);
            }
            match escrow_ix.as_ref() {
                EscrowInstruction::Deposit {
                    accounts,
                    data,
                    event,
                } => {
                    let recipient = data
                        .recipient
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| accounts.user.to_string());

                    (
                        None,
                        None,
                        Some(
                            DbTransactionBuilder::new(
                                signature.clone(),
                                instruction_meta.slot,
                                accounts.mint.to_string(),
                                event.amount,
                            )
                            .initiator(accounts.user.to_string())
                            .recipient(recipient)
                            .transaction_type(TransactionType::Deposit)
                            .instruction_index(instruction_meta.instruction_index as i32)
                            .inner_index(instruction_meta.inner_index.map(|i| i as i32))
                            .build(),
                        ),
                        None,
                    )
                }
                EscrowInstruction::AllowMint {
                    accounts, event, ..
                } => {
                    let mint_address = accounts.mint.to_string();
                    (
                        Some(DbMint::new(
                            mint_address.clone(),
                            event.decimals as i16,
                            accounts.token_program.to_string(),
                        )),
                        Some(MintStatusChange {
                            mint_address,
                            status: MintStatus::Allowed,
                        }),
                        None,
                        None,
                    )
                }
                EscrowInstruction::BlockMint { accounts } => (
                    None,
                    Some(MintStatusChange {
                        mint_address: accounts.mint.to_string(),
                        status: MintStatus::Blocked,
                    }),
                    None,
                    None,
                ),
                // Only successful transactions reach the processor, so this
                // instruction is a payout that actually happened rather than one
                // that was attempted. Both account layouts carry the nonce, so a
                // resync from the genesis slot records the older ones too.
                EscrowInstruction::ReleaseFunds { data, .. } => (
                    None,
                    None,
                    None,
                    Some(DbObservedRelease {
                        withdrawal_nonce: data.transaction_nonce as i64,
                        signature: signature.clone(),
                        slot: instruction_meta.slot as i64,
                    }),
                ),
                _ => (None, None, None, None),
            }
        }

        ProgramInstruction::Withdraw(withdraw_ix) => match withdraw_ix.as_ref() {
            WithdrawInstruction::WithdrawFunds { accounts, data } => {
                let recipient = data.destination.to_string();

                (
                    None,
                    None,
                    Some(
                        DbTransactionBuilder::new(
                            signature.clone(),
                            instruction_meta.slot,
                            accounts.mint.to_string(),
                            data.amount,
                        )
                        .initiator(accounts.user.to_string())
                        .recipient(recipient)
                        .transaction_type(TransactionType::Withdrawal)
                        .instruction_index(instruction_meta.instruction_index as i32)
                        .inner_index(instruction_meta.inner_index.map(|i| i as i32))
                        .build(),
                    ),
                    None,
                )
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::checkpoint::CheckpointWriter;
    use crate::indexer::datasource::common::parser::{
        AllowMintAccounts, AllowMintData, AllowMintEvent, BlockMintAccounts, DepositAccounts,
        DepositData, DepositEvent, ReleaseFundsAccounts, ReleaseFundsData, RotateBitmapAccounts,
        WithdrawFundsAccounts, WithdrawFundsData,
    };
    use crate::storage::common::amount::TokenAmount;
    use crate::storage::common::storage::mock::MockStorage;
    use solana_sdk::pubkey::Pubkey;

    fn make_pubkey(i: u8) -> Pubkey {
        let mut bytes = [0u8; 32];
        bytes[0] = i;
        Pubkey::new_from_array(bytes)
    }

    /// Instance pubkey hardcoded by `make_deposit_instruction`.
    fn deposit_instance() -> Pubkey {
        make_pubkey(11)
    }

    /// Instance pubkey hardcoded by `make_allow_mint_instruction`.
    fn allow_mint_instance() -> Pubkey {
        make_pubkey(12)
    }

    /// Instance pubkey hardcoded by `make_rotate_bitmap_instruction`.
    fn rotate_bitmap_instance() -> Pubkey {
        make_pubkey(21)
    }

    fn make_deposit_instruction(
        slot: u64,
        sig: Option<String>,
        recipient: Option<Pubkey>,
    ) -> InstructionWithMetadata {
        make_deposit_instruction_on_instance(slot, sig, recipient, deposit_instance())
    }

    /// Like `make_deposit_instruction` but on a caller-chosen instance, so a
    /// deposit can share a slot with an AllowMint on the same instance.
    fn make_deposit_instruction_on_instance(
        slot: u64,
        sig: Option<String>,
        recipient: Option<Pubkey>,
        instance: Pubkey,
    ) -> InstructionWithMetadata {
        let user = make_pubkey(1);
        let mint = make_pubkey(2);
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::Deposit {
                accounts: DepositAccounts {
                    payer: make_pubkey(10),
                    user,
                    instance,
                    mint,
                    allowed_mint: make_pubkey(12),
                    user_ata: make_pubkey(13),
                    instance_ata: make_pubkey(14),
                    system_program: make_pubkey(15),
                    token_program: make_pubkey(16),
                    associated_token_program: make_pubkey(17),
                    event_authority: make_pubkey(18),
                    private_channel_escrow_program: make_pubkey(19),
                },
                data: DepositData {
                    amount: 1000,
                    recipient,
                },
                // event.amount differs from data.amount to prove the operator
                // is fed the event-reported received amount (e.g. net of a
                // Token-2022 transfer fee), not the caller-requested amount.
                event: DepositEvent { amount: 990 },
            })),
            slot,
            program_type: ProgramType::Escrow,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    fn make_allow_mint_instruction(slot: u64, sig: Option<String>) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::AllowMint {
                accounts: AllowMintAccounts {
                    payer: make_pubkey(10),
                    admin: make_pubkey(11),
                    instance: allow_mint_instance(),
                    mint: make_pubkey(2),
                    allowed_mint: make_pubkey(13),
                    instance_ata: make_pubkey(14),
                    system_program: make_pubkey(15),
                    token_program: make_pubkey(16),
                    associated_token_program: make_pubkey(17),
                    event_authority: make_pubkey(18),
                    private_channel_escrow_program: make_pubkey(19),
                },
                data: AllowMintData { bump: 255 },
                event: AllowMintEvent { decimals: 6 },
            })),
            slot,
            program_type: ProgramType::Escrow,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    /// BlockMint scoped to `allow_mint_instance()` so it follows an AllowMint on
    /// the same watched instance, mirroring the on-chain allow-then-block order.
    fn make_block_mint_instruction(slot: u64, sig: Option<String>) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::BlockMint {
                accounts: BlockMintAccounts {
                    payer: make_pubkey(10),
                    admin: make_pubkey(11),
                    instance: allow_mint_instance(),
                    mint: make_pubkey(2),
                    allowed_mint: make_pubkey(13),
                    system_program: make_pubkey(15),
                    event_authority: make_pubkey(18),
                    private_channel_escrow_program: make_pubkey(19),
                },
            })),
            slot,
            program_type: ProgramType::Escrow,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    fn make_withdraw_instruction(slot: u64, sig: Option<String>) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Withdraw(Box::new(
                WithdrawInstruction::WithdrawFunds {
                    accounts: WithdrawFundsAccounts {
                        user: make_pubkey(1),
                        mint: make_pubkey(2),
                        token_account: make_pubkey(3),
                        token_program: make_pubkey(4),
                        associated_token_program: make_pubkey(5),
                    },
                    data: WithdrawFundsData {
                        amount: 500,
                        destination: make_pubkey(20),
                    },
                },
            )),
            slot,
            program_type: ProgramType::Withdraw,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    fn make_rotate_bitmap_instruction(slot: u64, sig: Option<String>) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::RotateBitmap {
                accounts: RotateBitmapAccounts {
                    payer: make_pubkey(10),
                    operator: make_pubkey(11),
                    instance: rotate_bitmap_instance(),
                    withdrawal_bitmap: make_pubkey(12),
                    operator_pda: make_pubkey(13),
                    event_authority: make_pubkey(14),
                    private_channel_escrow_program: make_pubkey(15),
                },
            })),
            slot,
            program_type: ProgramType::Escrow,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    /// Instance pubkey hardcoded by `make_release_funds_instruction`.
    fn release_funds_instance() -> Pubkey {
        make_pubkey(22)
    }

    /// A successful `ReleaseFunds` on the instance this processor watches.
    fn make_release_funds_instruction(
        slot: u64,
        sig: Option<String>,
        nonce: u64,
    ) -> InstructionWithMetadata {
        make_release_funds_instruction_on_instance(slot, sig, nonce, release_funds_instance())
    }

    fn make_release_funds_instruction_on_instance(
        slot: u64,
        sig: Option<String>,
        nonce: u64,
        instance: Pubkey,
    ) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::ReleaseFunds {
                accounts: ReleaseFundsAccounts {
                    payer: make_pubkey(10),
                    operator: make_pubkey(11),
                    instance,
                    withdrawal_bitmap: make_pubkey(12),
                    operator_pda: make_pubkey(13),
                    mint: make_pubkey(2),
                    allowed_mint: make_pubkey(14),
                    user_ata: make_pubkey(15),
                    instance_ata: make_pubkey(16),
                    token_program: make_pubkey(17),
                    associated_token_program: make_pubkey(18),
                    event_authority: make_pubkey(19),
                    private_channel_escrow_program: make_pubkey(20),
                },
                data: ReleaseFundsData {
                    amount: 750,
                    user: make_pubkey(1),
                    transaction_nonce: nonce,
                },
            })),
            slot,
            program_type: ProgramType::Escrow,
            signature: sig,
            instruction_index: 0,
            inner_index: None,
        }
    }

    // ========================================================================
    // convert_to_db_models tests
    // ========================================================================

    #[test]
    fn convert_deposit_with_explicit_recipient() {
        let recipient = make_pubkey(99);
        let ix = make_deposit_instruction(100, Some("sig1".to_string()), Some(recipient));
        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&deposit_instance()));
        assert!(mint.is_none());
        assert!(status.is_none());
        let txn = txn.unwrap();
        assert_eq!(txn.signature, "sig1");
        assert_eq!(txn.slot, 100);
        // event.amount = 990, data.amount = 1000 (see make_deposit_instruction).
        // The DB row must carry the event-reported amount.
        assert_eq!(txn.amount, TokenAmount(990));
        assert_eq!(txn.recipient, recipient.to_string());
        assert_eq!(txn.initiator, make_pubkey(1).to_string());
        assert!(matches!(txn.transaction_type, TransactionType::Deposit));
    }

    #[test]
    fn convert_deposit_none_recipient_defaults_to_user() {
        let ix = make_deposit_instruction(50, Some("sig2".to_string()), None);
        let (_, _, txn, _) = convert_to_db_models(&ix, Some(&deposit_instance()));
        let txn = txn.unwrap();
        // recipient should default to accounts.user
        assert_eq!(txn.recipient, make_pubkey(1).to_string());
    }

    #[test]
    fn convert_allow_mint_returns_mint_no_txn() {
        let ix = make_allow_mint_instruction(200, Some("sig3".to_string()));
        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&allow_mint_instance()));
        assert!(txn.is_none());
        let status = status.expect("AllowMint must emit a status change");
        assert_eq!(status.status, MintStatus::Allowed);
        assert_eq!(status.mint_address, make_pubkey(2).to_string());
        let mint = mint.unwrap();
        assert_eq!(mint.mint_address, make_pubkey(2).to_string());
        assert_eq!(mint.decimals, 6);
        assert_eq!(mint.status, "allowed");
        // The indexer leaves Token-2022 extension resolution to the operator —
        // both flags must stay None at AllowMint time.
        assert_eq!(mint.is_pausable, None);
        assert_eq!(mint.has_permanent_delegate, None);
    }

    #[test]
    fn convert_block_mint_returns_blocked_status_no_mint_no_txn() {
        let ix = make_block_mint_instruction(210, Some("sig-block-1".to_string()));
        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&allow_mint_instance()));
        // Block never upserts a mints row and never produces a transaction —
        // only a "blocked" status transition for the already-allowed mint.
        assert!(mint.is_none());
        assert!(txn.is_none());
        let status = status.expect("BlockMint must emit a status change");
        assert_eq!(status.status, MintStatus::Blocked);
        assert_eq!(status.mint_address, make_pubkey(2).to_string());
    }

    #[test]
    fn convert_withdraw_funds() {
        let ix = make_withdraw_instruction(300, Some("sig4".to_string()));
        let (mint, status, txn, _) = convert_to_db_models(&ix, None);
        assert!(mint.is_none());
        assert!(status.is_none());
        let txn = txn.unwrap();
        assert_eq!(txn.amount, TokenAmount(500));
        assert_eq!(txn.recipient, make_pubkey(20).to_string());
        assert!(matches!(txn.transaction_type, TransactionType::Withdrawal));
    }

    #[test]
    fn convert_threads_instruction_index_into_db_rows() {
        let sig = "shared_sig".to_string();

        let mut ix0 = make_deposit_instruction(100, Some(sig.clone()), None);
        ix0.instruction_index = 0;
        let mut ix1 = make_deposit_instruction(100, Some(sig.clone()), None);
        ix1.instruction_index = 1;

        let (_, _, txn0, _) = convert_to_db_models(&ix0, Some(&deposit_instance()));
        let (_, _, txn1, _) = convert_to_db_models(&ix1, Some(&deposit_instance()));

        let txn0 = txn0.unwrap();
        let txn1 = txn1.unwrap();
        assert_eq!(txn0.signature, sig);
        assert_eq!(txn1.signature, sig);
        assert_eq!(txn0.instruction_index, 0);
        assert_eq!(txn1.instruction_index, 1);
    }

    #[test]
    fn convert_no_signature_returns_none() {
        let ix = make_deposit_instruction(100, None, None);
        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&deposit_instance()));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    #[test]
    fn convert_catchall_escrow_variant_returns_none() {
        let ix = make_rotate_bitmap_instruction(100, Some("sig5".to_string()));
        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&rotate_bitmap_instance()));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    #[test]
    fn convert_drops_deposit_targeting_foreign_instance() {
        let watched = deposit_instance();
        let foreign = make_pubkey(99);

        let ix = InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::Deposit {
                accounts: DepositAccounts {
                    payer: make_pubkey(10),
                    user: make_pubkey(1),
                    instance: foreign,
                    mint: make_pubkey(2),
                    allowed_mint: make_pubkey(12),
                    user_ata: make_pubkey(13),
                    instance_ata: make_pubkey(14),
                    system_program: make_pubkey(15),
                    token_program: make_pubkey(16),
                    associated_token_program: make_pubkey(17),
                    event_authority: make_pubkey(18),
                    private_channel_escrow_program: make_pubkey(19),
                },
                data: DepositData {
                    amount: 1000,
                    recipient: None,
                },
                event: DepositEvent { amount: 1000 },
            })),
            slot: 100,
            program_type: ProgramType::Escrow,
            signature: Some("sig_exploit".to_string()),
            instruction_index: 0,
            inner_index: None,
        };

        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&watched));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    #[test]
    fn convert_drops_allow_mint_targeting_foreign_instance() {
        let watched = allow_mint_instance();
        let foreign = make_pubkey(99);

        let ix = InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::AllowMint {
                accounts: AllowMintAccounts {
                    payer: make_pubkey(10),
                    admin: make_pubkey(11),
                    instance: foreign,
                    mint: make_pubkey(2),
                    allowed_mint: make_pubkey(13),
                    instance_ata: make_pubkey(14),
                    system_program: make_pubkey(15),
                    token_program: make_pubkey(16),
                    associated_token_program: make_pubkey(17),
                    event_authority: make_pubkey(18),
                    private_channel_escrow_program: make_pubkey(19),
                },
                data: AllowMintData { bump: 255 },
                event: AllowMintEvent { decimals: 6 },
            })),
            slot: 200,
            program_type: ProgramType::Escrow,
            signature: Some("sig_exploit".to_string()),
            instruction_index: 0,
            inner_index: None,
        };

        let (mint, status, txn, _) = convert_to_db_models(&ix, Some(&watched));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    // ========================================================================
    // TransactionProcessor tests
    // ========================================================================

    /// Fast retry policy for tests: a couple of attempts, no sleeping, so a
    /// permanent failure exhausts immediately and a transient one self-heals.
    fn fast_retry() -> WriteRetryPolicy {
        WriteRetryPolicy {
            max_attempts: 2,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Unwrap the next message as a slot checkpoint, panicking on a Regate; keeps
    /// the slot-oriented recv sites terse now that the channel carries CheckpointMsg.
    async fn recv_slot(rx: &mut mpsc::Receiver<CheckpointMsg>) -> CheckpointUpdate {
        match rx.recv().await.expect("expected a checkpoint message") {
            CheckpointMsg::Slot(update) => update,
            CheckpointMsg::Regate { target, .. } => {
                panic!("expected a Slot checkpoint, got Regate(target={target})")
            }
        }
    }

    /// Poll the committed checkpoint until it reaches `want`, so a test can await a
    /// durable flush without racing the writer's batch timer.
    async fn wait_for_checkpoint(mock: &MockStorage, program: &str, want: u64) {
        for _ in 0..200 {
            if mock.get_committed_checkpoint(program).await.unwrap() == Some(want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("checkpoint for {program} never reached {want}");
    }

    fn make_processor_and_rx(
        escrow_instance_id: Pubkey,
    ) -> (
        TransactionProcessor,
        tokio::sync::mpsc::Receiver<CheckpointMsg>,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::mpsc::channel(100);
        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id)
            .with_write_retry(fast_retry());
        (processor, checkpoint_rx)
    }

    fn make_processor_with_mock(
        escrow_instance_id: Pubkey,
    ) -> (
        TransactionProcessor,
        tokio::sync::mpsc::Receiver<CheckpointMsg>,
        MockStorage,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::mpsc::channel(100);
        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id)
            .with_write_retry(fast_retry());
        (processor, checkpoint_rx, mock)
    }

    #[tokio::test]
    async fn finalize_empty_slot_sends_checkpoint() {
        let (mut processor, mut checkpoint_rx) = make_processor_and_rx(deposit_instance());
        processor
            .finalize_and_checkpoint(42, ProgramType::Escrow)
            .await
            .unwrap();
        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 42);
        assert_eq!(cp.program_type, ProgramType::Escrow);
        assert!(processor.slot_buffers.is_empty());
    }

    #[tokio::test]
    async fn finalize_with_deposits_inserts_batch() {
        let (mut processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        processor.buffer(make_deposit_instruction(100, Some("s1".to_string()), None));
        processor
            .finalize_and_checkpoint(100, ProgramType::Escrow)
            .await
            .unwrap();

        {
            let inserted = mock.inserted_transactions.lock().unwrap();
            assert_eq!(inserted.len(), 1);
            assert_eq!(inserted[0].len(), 1);
        }

        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 100);
    }

    #[tokio::test]
    async fn finalize_with_mints_upserts_first() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        processor.buffer(make_allow_mint_instruction(200, Some("s2".to_string())));
        processor
            .finalize_and_checkpoint(200, ProgramType::Escrow)
            .await
            .unwrap();

        {
            let mints = mock.mints.lock().unwrap();
            assert_eq!(mints.len(), 1);
            assert!(mints.contains_key(&make_pubkey(2).to_string()));
        }

        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 200);
    }

    #[tokio::test]
    async fn finalize_writes_mint_status_history_on_allow_mint() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        processor.buffer(make_allow_mint_instruction(
            200,
            Some("sig-allow-1".to_string()),
        ));
        processor
            .finalize_and_checkpoint(200, ProgramType::Escrow)
            .await
            .unwrap();

        {
            let rows = mock.mint_status_history.lock().unwrap();
            assert_eq!(rows.len(), 1, "exactly one status row should be written");
            assert_eq!(rows[0].mint_address, make_pubkey(2).to_string());
            assert_eq!(rows[0].status, "allowed");
            assert_eq!(rows[0].effective_slot, 200);
            assert_eq!(rows[0].signature, "sig-allow-1");
        }

        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 200);
    }

    #[tokio::test]
    async fn finalize_writes_blocked_status_on_block_mint() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        // Seed the allowed mints row the prior AllowMint would have created.
        mock.mints.lock().unwrap().insert(
            make_pubkey(2).to_string(),
            DbMint::new(make_pubkey(2).to_string(), 6, spl_token::id().to_string()),
        );
        processor.buffer(make_block_mint_instruction(
            250,
            Some("sig-block-2".to_string()),
        ));
        processor
            .finalize_and_checkpoint(250, ProgramType::Escrow)
            .await
            .unwrap();

        {
            let rows = mock.mint_status_history.lock().unwrap();
            assert_eq!(rows.len(), 1, "exactly one status row should be written");
            assert_eq!(rows[0].mint_address, make_pubkey(2).to_string());
            assert_eq!(rows[0].status, "blocked");
            assert_eq!(rows[0].effective_slot, 250);
            assert_eq!(rows[0].signature, "sig-block-2");
        }
        {
            // Block flips the existing row to "blocked" without creating a new one.
            let mints = mock.mints.lock().unwrap();
            assert_eq!(mints.len(), 1, "BlockMint must not create a new mints row");
            assert_eq!(
                mints.get(&make_pubkey(2).to_string()).unwrap().status,
                "blocked"
            );
        }

        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 250);
    }

    // ========================================================================
    // observed release recording
    // ========================================================================

    /// One row of the observed-release table.
    struct ObservedReleaseCase {
        label: &'static str,
        nonce: u64,
        slot: u64,
        signature: &'static str,
    }

    /// The refund gate reads this record, so a release the indexer saw land has
    /// to reach storage with its nonce, signature and slot intact.
    #[tokio::test]
    async fn finalize_records_observed_release() {
        let cases = [
            ObservedReleaseCase {
                label: "first release",
                nonce: 42,
                slot: 300,
                signature: "sig-release-current",
            },
            ObservedReleaseCase {
                label: "later release",
                nonce: 43,
                slot: 301,
                signature: "sig-release-later",
            },
        ];

        for case in cases {
            let (mut processor, mut checkpoint_rx, mock) =
                make_processor_with_mock(release_funds_instance());
            processor.buffer(make_release_funds_instruction(
                case.slot,
                Some(case.signature.to_string()),
                case.nonce,
            ));
            processor
                .finalize_and_checkpoint(case.slot, ProgramType::Escrow)
                .await
                .expect("the slot must finalize");

            let recorded = mock
                .get_observed_release(case.nonce as i64)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{} must record the release", case.label));
            assert_eq!(
                recorded.withdrawal_nonce, case.nonce as i64,
                "{}",
                case.label
            );
            assert_eq!(recorded.signature, case.signature, "{}", case.label);
            assert_eq!(recorded.slot, case.slot as i64, "{}", case.label);

            let cp = recv_slot(&mut checkpoint_rx).await;
            assert_eq!(cp.slot, case.slot, "{}", case.label);
        }
    }

    /// Live indexing, a backfill and a resync all see the same release, so
    /// re-observing one must neither error nor leave a second row behind. The
    /// first observation is the one kept, so a replay cannot rewrite the
    /// signature a refund would be judged against.
    #[tokio::test]
    async fn finalize_reobserving_a_release_is_idempotent() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(release_funds_instance());

        for _ in 0..2 {
            processor.buffer(make_release_funds_instruction(
                310,
                Some("sig-release-replay".to_string()),
                44,
            ));
            processor
                .finalize_and_checkpoint(310, ProgramType::Escrow)
                .await
                .expect("the slot must finalize");
            recv_slot(&mut checkpoint_rx).await;
        }

        let store = mock.observed_releases.lock().unwrap();
        assert_eq!(store.len(), 1, "a replayed release must not duplicate");
        assert_eq!(store.get(&44).unwrap().signature, "sig-release-replay");
    }

    /// A release on an instance this indexer does not watch is not ours to
    /// record. Recording it would let anyone who can call the escrow program on
    /// an instance of their own choosing plant a record that blocks a refund
    /// this operator owes.
    #[tokio::test]
    async fn finalize_drops_observed_release_on_foreign_instance() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(release_funds_instance());
        processor.buffer(make_release_funds_instruction_on_instance(
            320,
            Some("sig-release-foreign".to_string()),
            45,
            make_pubkey(99),
        ));
        processor
            .finalize_and_checkpoint(320, ProgramType::Escrow)
            .await
            .expect("the slot must finalize");

        assert!(mock.observed_releases.lock().unwrap().is_empty());
        recv_slot(&mut checkpoint_rx).await;
    }

    /// The checkpoint is what says a slot's releases are on record, so a slot
    /// whose releases will not write has to fail outright. Merely withholding
    /// the checkpoint is not enough: `CheckpointState::apply` takes a plain
    /// `max`, so the next slot would leapfrog this one and the hole would read
    /// as a clean negative forever after.
    #[tokio::test]
    async fn finalize_observed_release_failure_fails_the_slot() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(release_funds_instance());
        mock.set_should_fail("insert_observed_releases_batch", true);
        processor.buffer(make_release_funds_instruction(
            330,
            Some("sig-release-fail".to_string()),
            46,
        ));
        let result = processor
            .finalize_and_checkpoint(330, ProgramType::Escrow)
            .await;

        assert!(
            result.is_err(),
            "a release write that outlives the retry budget must fail the slot"
        );
        assert!(checkpoint_rx.try_recv().is_err());
    }

    /// A transient write failure is ridden out by the retry: the slot finalizes
    /// Ok, the row lands, and exactly one checkpoint is sent.
    #[tokio::test]
    async fn finalize_retries_then_succeeds_transient() {
        let (mut processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        // Fail the first call, then succeed on retry (fast policy allows 2).
        mock.set_fail_times("insert_db_transactions_batch", 1);
        processor.buffer(make_deposit_instruction(
            401,
            Some("s-retry".to_string()),
            None,
        ));

        processor
            .finalize_and_checkpoint(401, ProgramType::Escrow)
            .await
            .expect("transient failure should self-heal");

        {
            let inserted = mock.inserted_transactions.lock().unwrap();
            assert_eq!(inserted.len(), 1, "row lands once after the retry");
            assert_eq!(inserted[0][0].slot, 401);
        }
        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 401);
        // Exactly one checkpoint - the failed attempt did not also send one.
        assert!(checkpoint_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn finalize_mint_status_failure_exhausts_returns_err() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("insert_mint_statuses_batch", true);
        processor.buffer(make_allow_mint_instruction(
            201,
            Some("sig-allow-2".to_string()),
        ));
        let result = processor
            .finalize_and_checkpoint(201, ProgramType::Escrow)
            .await;

        assert!(result.is_err(), "permanent write failure is fatal");
        assert!(checkpoint_rx.try_recv().is_err());
    }

    /// AllowMint + Deposit for the same mint in one slot: if the mint-status
    /// write fails permanently, the deposit row must be withheld (else the gate
    /// would quarantine it) and the slot fails fatally so it replays.
    #[tokio::test]
    async fn finalize_mint_status_failure_withholds_deposit_then_exhausts() {
        // Both instructions must target the configured instance, or the
        // instance filter would drop one and defeat the test's intent.
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("insert_mint_statuses_batch", true);
        processor.buffer(make_allow_mint_instruction(
            202,
            Some("sig-allow-3".to_string()),
        ));
        processor.buffer(make_deposit_instruction_on_instance(
            202,
            Some("sig-deposit-3".to_string()),
            None,
            allow_mint_instance(),
        ));
        let result = processor
            .finalize_and_checkpoint(202, ProgramType::Escrow)
            .await;

        assert!(result.is_err(), "permanent write failure is fatal");
        // Checkpoint withheld so the slot replays.
        assert!(checkpoint_rx.try_recv().is_err());
        // Deposit row must not be committed without its backing status row.
        assert!(
            mock.inserted_transactions.lock().unwrap().is_empty(),
            "deposit must be withheld when the mint-status write failed"
        );
    }

    #[tokio::test]
    async fn finalize_upsert_mints_failure_exhausts_returns_err() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("upsert_mints_batch", true);
        processor.buffer(make_allow_mint_instruction(300, Some("s3".to_string())));
        let result = processor
            .finalize_and_checkpoint(300, ProgramType::Escrow)
            .await;

        assert!(result.is_err(), "permanent write failure is fatal");
        assert!(checkpoint_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn finalize_transaction_failure_exhausts_returns_err() {
        let (mut processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        mock.set_should_fail("insert_db_transactions_batch", true);
        processor.buffer(make_deposit_instruction(400, Some("s4".to_string()), None));
        let result = processor
            .finalize_and_checkpoint(400, ProgramType::Escrow)
            .await;

        assert!(result.is_err(), "permanent write failure is fatal");
        assert!(checkpoint_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn start_processes_instruction_then_slot_complete() {
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let ix = make_deposit_instruction(500, Some("s5".to_string()), None);
        tx.send(ProcessorMessage::Instruction(ix)).await.unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: 500,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        let result = processor.start(rx).await;
        assert!(result.is_ok());

        {
            let inserted = mock.inserted_transactions.lock().unwrap();
            assert_eq!(inserted.len(), 1);
        }

        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 500);
    }

    /// The processor forwards a Regate in-band ahead of the slot it precedes and
    /// does no DB write for it, locking the FIFO ordering the gate re-arm depends on.
    #[tokio::test]
    async fn processor_forwards_regate_before_slot_in_order() {
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            500,
            Some("s5".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::Regate {
            program_type: ProgramType::Escrow,
            from: 100,
            target: 110,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: 500,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        processor.start(rx).await.unwrap();

        // Regate is forwarded first, ahead of slot 500's checkpoint.
        match checkpoint_rx.recv().await.unwrap() {
            CheckpointMsg::Regate {
                program_type,
                from,
                target,
            } => {
                assert_eq!(program_type, ProgramType::Escrow);
                assert_eq!(from, 100);
                assert_eq!(target, 110);
            }
            other => panic!("expected Regate first, got {other:?}"),
        }
        let cp = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(cp.slot, 500);

        // No DB write for the Regate: only slot 500's deposit row landed.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0][0].slot, 500);
    }

    /// Finalizing slot A inserts only A's rows and leaves B buffered until B's own SlotComplete.
    #[tokio::test]
    async fn interleaved_slots_finalize_independently() {
        const SLOT_A: u64 = 600;
        const SLOT_B: u64 = 601;
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            SLOT_A,
            Some("a".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            SLOT_B,
            Some("b".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: SLOT_A,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: SLOT_B,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        processor.start(rx).await.unwrap();

        // Scope the std Mutex guard so it is dropped before the awaits below.
        {
            let batches = mock.inserted_transactions.lock().unwrap();
            assert_eq!(batches.len(), 2, "each slot finalizes its own batch");
            assert_eq!(batches[0][0].signature, "a");
            assert_eq!(batches[0][0].slot, SLOT_A as i64);
            assert_eq!(batches[1][0].signature, "b");
            assert_eq!(batches[1][0].slot, SLOT_B as i64);
        }

        let first = recv_slot(&mut checkpoint_rx).await;
        let second = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(first.slot, SLOT_A);
        assert_eq!(second.slot, SLOT_B);
    }

    /// A foreign SlotComplete between a same-slot AllowMint and Deposit must not split the
    /// finalize: the later permanent mint-status failure still withholds the deposit,
    /// withholds SLOT_S's checkpoint, and fails the processor fatally.
    #[tokio::test]
    async fn same_slot_atomicity_survives_foreign_slotcomplete() {
        const SLOT_S: u64 = 700;
        const LIVE_TIP: u64 = 9_000_000;
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("insert_mint_statuses_batch", true);
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tx.send(ProcessorMessage::Instruction(make_allow_mint_instruction(
            SLOT_S,
            Some("allow".to_string()),
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: LIVE_TIP,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::Instruction(
            make_deposit_instruction_on_instance(
                SLOT_S,
                Some("deposit".to_string()),
                None,
                allow_mint_instance(),
            ),
        ))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: SLOT_S,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        let result = processor.start(rx).await;
        assert!(
            result.is_err(),
            "permanent same-slot write failure is fatal"
        );

        let mut checkpointed = Vec::new();
        while let Ok(msg) = checkpoint_rx.try_recv() {
            if let CheckpointMsg::Slot(cp) = msg {
                checkpointed.push(cp.slot);
            }
        }
        assert_eq!(
            checkpointed,
            vec![LIVE_TIP],
            "only the empty live tip checkpoints; SLOT_S is withheld"
        );
        assert!(
            mock.inserted_transactions.lock().unwrap().is_empty(),
            "deposit must be withheld when its same-slot mint-status write failed"
        );
    }

    // ========================================================================
    // Reconcile-in-place (resync consumed-set) tests
    // ========================================================================

    use solana_sdk::signature::Signature;

    const RECONCILE_SLOT: u64 = 800;
    const SERVICED_DEPOSIT_SIG: &str = "serviced-deposit-sig";
    const SERVICED_WITHDRAW_SIG: &str = "serviced-withdraw-sig";

    fn make_processor_with_consumed(
        escrow_instance_id: Pubkey,
        consumed: ConsumedSet,
    ) -> (
        TransactionProcessor,
        tokio::sync::mpsc::Receiver<CheckpointMsg>,
        MockStorage,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::mpsc::channel(100);
        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id)
            .with_consumed_set(Arc::new(consumed));
        (processor, checkpoint_rx, mock)
    }

    /// Source-event-id for a top-level deposit row.
    fn deposit_event_id(signature: &str) -> SourceEventId {
        SourceEventId::new(signature, 0, None)
    }

    /// A deposit already minted on the channel rebuilds `completed` with its mint sig,
    /// never `pending` (so the fetcher cannot re-mint it).
    #[tokio::test]
    async fn resync_reconcile_completes_serviced_deposit() {
        let mint_sig = Signature::new_unique();
        let mut consumed = ConsumedSet::new();
        consumed.insert(
            deposit_event_id(SERVICED_DEPOSIT_SIG),
            (mint_sig, ConsumedMintKind::Deposit),
        );
        let (mut processor, _rx, mock) = make_processor_with_consumed(deposit_instance(), consumed);
        processor.buffer(make_deposit_instruction(
            RECONCILE_SLOT,
            Some(SERVICED_DEPOSIT_SIG.to_string()),
            None,
        ));
        processor
            .finalize_and_checkpoint(RECONCILE_SLOT, ProgramType::Escrow)
            .await
            .unwrap();

        let inserted = mock.inserted_transactions.lock().unwrap();
        let row = &inserted[0][0];
        assert_eq!(row.status, TransactionStatus::Completed);
        assert_eq!(
            row.counterpart_signature.as_deref(),
            Some(mint_sig.to_string().as_str())
        );
    }

    /// A new, unserviced deposit rebuilds `pending` (no false-completed) even with a
    /// non-empty consumed-set that does not contain it.
    #[tokio::test]
    async fn resync_reconcile_leaves_new_deposit_pending() {
        let mut consumed = ConsumedSet::new();
        consumed.insert(
            deposit_event_id("some-other-deposit"),
            (Signature::new_unique(), ConsumedMintKind::Deposit),
        );
        let (mut processor, _rx, mock) = make_processor_with_consumed(deposit_instance(), consumed);
        processor.buffer(make_deposit_instruction(
            RECONCILE_SLOT,
            Some(SERVICED_DEPOSIT_SIG.to_string()),
            None,
        ));
        processor
            .finalize_and_checkpoint(RECONCILE_SLOT, ProgramType::Escrow)
            .await
            .unwrap();

        let inserted = mock.inserted_transactions.lock().unwrap();
        let row = &inserted[0][0];
        assert_eq!(row.status, TransactionStatus::Pending);
        assert!(row.counterpart_signature.is_none());
    }

    /// A withdrawal whose release failed and was reminted rebuilds `failed_reminted`
    /// with its remint sig, so the operator never releases (double-pays) it.
    #[tokio::test]
    async fn resync_reconcile_reclassifies_reminted_withdrawal() {
        let remint_sig = Signature::new_unique();
        let mut consumed = ConsumedSet::new();
        let id = SourceEventId::new(SERVICED_WITHDRAW_SIG, 0, None);
        consumed.insert(id, (remint_sig, ConsumedMintKind::Remint));
        let (mut processor, _rx, mock) = make_processor_with_consumed(Pubkey::default(), consumed);
        processor.buffer(make_withdraw_instruction(
            RECONCILE_SLOT,
            Some(SERVICED_WITHDRAW_SIG.to_string()),
        ));
        processor
            .finalize_and_checkpoint(RECONCILE_SLOT, ProgramType::Escrow)
            .await
            .unwrap();

        let inserted = mock.inserted_transactions.lock().unwrap();
        let row = &inserted[0][0];
        assert_eq!(row.status, TransactionStatus::FailedReminted);
        assert_eq!(
            row.landed_remint_signature.as_deref(),
            Some(remint_sig.to_string().as_str())
        );
    }

    /// A deposit id present in the set but tagged as a remint (kind/type mismatch) is
    /// not acted on: the deposit stays `pending` rather than being wrongly completed.
    #[tokio::test]
    async fn resync_reconcile_ignores_kind_type_mismatch() {
        let mut consumed = ConsumedSet::new();
        consumed.insert(
            deposit_event_id(SERVICED_DEPOSIT_SIG),
            (Signature::new_unique(), ConsumedMintKind::Remint),
        );
        let (mut processor, _rx, mock) = make_processor_with_consumed(deposit_instance(), consumed);
        processor.buffer(make_deposit_instruction(
            RECONCILE_SLOT,
            Some(SERVICED_DEPOSIT_SIG.to_string()),
            None,
        ));
        processor
            .finalize_and_checkpoint(RECONCILE_SLOT, ProgramType::Escrow)
            .await
            .unwrap();

        let inserted = mock.inserted_transactions.lock().unwrap();
        assert_eq!(inserted[0][0].status, TransactionStatus::Pending);
    }

    /// Regression contract: with no consumed-set configured, a deposit that *would*
    /// match rebuilds `pending` exactly as on the normal indexing path.
    #[tokio::test]
    async fn resync_reconcile_none_set_leaves_pending() {
        let (mut processor, _rx, mock) = make_processor_with_mock(deposit_instance());
        processor.buffer(make_deposit_instruction(
            RECONCILE_SLOT,
            Some(SERVICED_DEPOSIT_SIG.to_string()),
            None,
        ));
        processor
            .finalize_and_checkpoint(RECONCILE_SLOT, ProgramType::Escrow)
            .await
            .unwrap();

        let inserted = mock.inserted_transactions.lock().unwrap();
        assert_eq!(inserted[0][0].status, TransactionStatus::Pending);
        assert!(inserted[0][0].counterpart_signature.is_none());
    }

    #[tokio::test]
    async fn start_channel_close_exits_ok() {
        let (processor, _checkpoint_rx) = make_processor_and_rx(deposit_instance());
        let (_tx, rx) = tokio::sync::mpsc::channel(10);
        drop(_tx);

        let result = processor.start(rx).await;
        assert!(result.is_ok());
    }

    /// A permanently-failing slot write exhausts the retry and propagates out of
    /// the start loop as a fatal Err.
    #[tokio::test]
    async fn start_propagates_fatal_after_exhaustion() {
        const N: u64 = 800;
        let (processor, _checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        mock.set_should_fail("insert_db_transactions_batch", true);
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            N,
            Some("dep-n".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: N,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        let result = processor.start(rx).await;
        assert!(result.is_err());
    }

    /// The key no-leapfrog proof: when slot N's write fails permanently, start
    /// exits Err BEFORE processing N+1, so no checkpoint is sent and N+1 is
    /// never persisted. A restart would therefore replay from below N.
    #[tokio::test]
    async fn start_exhaustion_does_not_leapfrog_next_slot() {
        const N: u64 = 900;
        const N_NEXT: u64 = 901;
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        mock.set_should_fail("insert_db_transactions_batch", true);
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            N,
            Some("dep-n".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: N,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            N_NEXT,
            Some("dep-n-next".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: N_NEXT,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        let result = processor.start(rx).await;
        assert!(result.is_err(), "the failed slot is fatal");

        // No checkpoint for N or N+1.
        assert!(
            checkpoint_rx.try_recv().is_err(),
            "a withheld slot must not be leapfrogged"
        );
        // N+1's row was never written - the loop exited before processing it.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert!(
            inserted.iter().flatten().all(|t| t.slot != N_NEXT as i64),
            "N+1 must not be persisted after N fails"
        );
    }

    /// A slot that fails within the retry budget then succeeds does not break the
    /// happy path: N and N+1 both checkpoint in order and start ends Ok.
    #[tokio::test]
    async fn start_recovers_within_retries_continues() {
        const N: u64 = 1000;
        const N_NEXT: u64 = 1001;
        let (processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        // Fail N's write once, then succeed (fast policy allows 2 attempts).
        mock.set_fail_times("insert_db_transactions_batch", 1);
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        for (slot, sig) in [(N, "dep-n"), (N_NEXT, "dep-n-next")] {
            tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
                slot,
                Some(sig.to_string()),
                None,
            )))
            .await
            .unwrap();
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }
        drop(tx);

        let result = processor.start(rx).await;
        assert!(result.is_ok(), "retry self-heal keeps the loop running");

        let first = recv_slot(&mut checkpoint_rx).await;
        let second = recv_slot(&mut checkpoint_rx).await;
        assert_eq!(first.slot, N);
        assert_eq!(second.slot, N_NEXT);
    }

    // ========================================================================
    // Channel-level integration: processor + checkpoint writer over real mpsc
    // ========================================================================

    /// Wire a real processor + checkpoint writer over real channels on one `MockStorage`, optionally gated, as the indexer does.
    fn spawn_pipeline(
        escrow_instance_id: Pubkey,
        gate: Option<(u64, u64)>,
    ) -> (
        mpsc::Sender<ProcessorMessage>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
        MockStorage,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (instruction_tx, instruction_rx) = mpsc::channel(64);
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(64);

        let mut writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1)
            .with_max_batch_size(1);
        if let Some((from_slot, target)) = gate {
            writer = writer.with_gate(from_slot, target);
        }
        let checkpoint_handle = writer.start(checkpoint_rx);

        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id)
            .with_write_retry(fast_retry());
        let processor_handle = tokio::spawn(async move {
            processor.start(instruction_rx).await.unwrap();
        });

        (instruction_tx, processor_handle, checkpoint_handle, mock)
    }

    /// A live-tip SlotComplete during backfill must not advance the checkpoint past the unfilled gap (gate `(100, 105]`, fill 101..=105).
    #[tokio::test]
    async fn concurrent_backfill_live_interleave_never_skips() {
        const FROM: u64 = 100;
        const T0: u64 = 105;
        const DEPOSIT_SLOT: u64 = 103;
        const LIVE_TIP: u64 = 1_000_000;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline(deposit_instance(), Some((FROM, T0)));

        // A historical deposit, then the attack: a live-tip SlotComplete arrives
        // before backfill has filled the gap.
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            DEPOSIT_SLOT,
            Some("dep-103".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: LIVE_TIP,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();

        // Backfill now closes the gap contiguously.
        for slot in (FROM + 1)..=T0 {
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }

        drop(tx);
        processor_handle.await.unwrap();
        checkpoint_handle.await.unwrap();

        // The persisted checkpoint ends at T0 and never reached the live tip.
        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            committed, T0,
            "checkpoint hands off at T0, never the live tip"
        );
        assert!(
            committed < LIVE_TIP,
            "checkpoint must never cross the unfilled gap to the live tip"
        );

        // The historical deposit row exists, and a restart would re-backfill from
        // a checkpoint at/above DEPOSIT_SLOT — the slot is not skipped.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0][0].slot, DEPOSIT_SLOT as i64);
        assert!(committed >= DEPOSIT_SLOT);
    }

    /// With the shared startup boundary the live source resumes at target+1, so the
    /// frontier folds through the gap and continues into the live slots with no hole:
    /// gate `(100, 105]`, backfill fills 101..=105, live sends 106 and 107.
    #[tokio::test]
    async fn concurrent_backfill_hands_off_contiguously_to_live_start() {
        const FROM: u64 = 100;
        const T0: u64 = 105;
        const DEPOSIT_SLOT: u64 = 103;
        const LIVE_START: u64 = T0 + 1; // live source begins one past backfill's target
        const LIVE_END: u64 = 107;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline(deposit_instance(), Some((FROM, T0)));

        // A historical deposit inside the gap, then backfill closes the gap.
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            DEPOSIT_SLOT,
            Some("dep-103".to_string()),
            None,
        )))
        .await
        .unwrap();
        for slot in (FROM + 1)..=T0 {
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }

        // The live source, starting at target+1, continues contiguously.
        for slot in LIVE_START..=LIVE_END {
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }

        drop(tx);
        processor_handle.await.unwrap();
        checkpoint_handle.await.unwrap();

        // The checkpoint advances through the gap into the live slots with no hole.
        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            committed, LIVE_END,
            "checkpoint hands off from backfill's target into the live slots"
        );

        // The historical deposit row exists and every slot 101..=107 was covered.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0][0].slot, DEPOSIT_SLOT as i64);
        assert!(
            committed >= LIVE_END,
            "no slot in 101..=107 is skipped between backfill and live"
        );
    }

    /// The exact reconnect residual-gap reproduction, end-to-end through the real
    /// pipeline: the residual window (T_gf, T_sub] must be indexed, never leapfrogged by
    /// the live tip that resumes above it. Removing the writer's Regate arm makes this
    /// drive the checkpoint straight to the tip and skip the window, so it is the
    /// authoritative guard that value-bearing events there are never lost.
    #[tokio::test]
    async fn reconnect_residual_gap_is_not_leapfrogged() {
        const T_GF: u64 = 100; // stale tip the old gap-fill stopped at
        const T_SUB: u64 = 110; // real live resume slot observed on reconnect
        const RESIDUAL_DEPOSIT: u64 = 105; // a value-bearing event inside the window
        const LIVE_TIP: u64 = 9_000_000;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline(deposit_instance(), None);

        // Steady state before the reconnect: durable checkpoint and in-memory
        // frontier both sit at T_gf.
        tx.send(ProcessorMessage::SlotComplete {
            slot: T_GF,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        wait_for_checkpoint(&mock, "escrow", T_GF).await;

        // Reconnect: arm the gate to the observed resume slot, then the live tip and
        // the live resume slot arrive BEFORE the residual (100, 110] is backfilled.
        tx.send(ProcessorMessage::Regate {
            program_type: ProgramType::Escrow,
            from: T_GF,
            target: T_SUB,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: LIVE_TIP,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: T_SUB,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();

        // The durable checkpoint must stay frozen at T_gf even though slot 9_000_000
        // was processed - this is the leapfrog the ungated code commits.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            committed, T_GF,
            "checkpoint must not leapfrog the unfilled residual window"
        );

        // Backfill closes (100, 110] contiguously, including a real deposit at 105.
        for slot in (T_GF + 1)..=T_SUB {
            if slot == RESIDUAL_DEPOSIT {
                tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
                    RESIDUAL_DEPOSIT,
                    Some("dep-105".to_string()),
                    None,
                )))
                .await
                .unwrap();
            }
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }
        // A later live slot advances the checkpoint past the now-contiguous window.
        tx.send(ProcessorMessage::SlotComplete {
            slot: LIVE_TIP + 1,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();

        drop(tx);
        processor_handle.await.unwrap();
        checkpoint_handle.await.unwrap();

        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert!(
            committed > T_SUB,
            "checkpoint advances past the window once it is contiguous"
        );
        // The residual-window deposit was indexed, not silently lost.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert!(
            inserted
                .iter()
                .flatten()
                .any(|t| t.slot == RESIDUAL_DEPOSIT as i64),
            "the residual-window deposit must be indexed"
        );
    }

    /// A crash mid-backfill persists the contiguous frontier, not the tip, so resume re-backfills with no tail skipped.
    #[tokio::test]
    async fn interrupt_mid_backfill_resumes_from_frontier() {
        const FROM: u64 = 100;
        const T0: u64 = 110;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline(deposit_instance(), Some((FROM, T0)));

        for slot in [101u64, 102] {
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }

        // Simulated crash: drop the channel before the gap is filled.
        drop(tx);
        processor_handle.await.unwrap();
        checkpoint_handle.await.unwrap();

        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(committed, 102, "frontier persisted, not T0");
        assert!(
            committed < T0,
            "the unfilled tail (103..=110) is not skipped"
        );
    }

    /// Like `spawn_pipeline` but exposes the processor's Result so a fatal write
    /// exhaustion can be observed instead of unwrapped.
    #[allow(clippy::type_complexity)]
    fn spawn_pipeline_result(
        escrow_instance_id: Pubkey,
    ) -> (
        mpsc::Sender<ProcessorMessage>,
        tokio::task::JoinHandle<Result<(), IndexerError>>,
        tokio::task::JoinHandle<()>,
        MockStorage,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (instruction_tx, instruction_rx) = mpsc::channel(64);
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(64);

        let writer = CheckpointWriter::new(storage.clone())
            .with_batch_interval(1)
            .with_max_batch_size(1);
        let checkpoint_handle = writer.start(checkpoint_rx);

        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id)
            .with_write_retry(fast_retry());
        let processor_handle = tokio::spawn(processor.start(instruction_rx));

        (instruction_tx, processor_handle, checkpoint_handle, mock)
    }

    /// End-to-end self-heal: a transient write blip on slot N is ridden out by
    /// the retry, so the durable checkpoint advances through N with no restart.
    #[tokio::test]
    async fn live_transient_blip_self_heals_no_gap() {
        const N: u64 = 200;
        const N1: u64 = 201;
        const N2: u64 = 202;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline(deposit_instance(), None);
        // Fail N's transaction write once, then succeed on retry.
        mock.set_fail_times("insert_db_transactions_batch", 1);

        for (slot, sig) in [(N, "dep-n"), (N1, "dep-n1"), (N2, "dep-n2")] {
            tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
                slot,
                Some(sig.to_string()),
                None,
            )))
            .await
            .unwrap();
            tx.send(ProcessorMessage::SlotComplete {
                slot,
                program_type: ProgramType::Escrow,
            })
            .await
            .unwrap();
        }

        drop(tx);
        processor_handle.await.unwrap();
        checkpoint_handle.await.unwrap();

        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert!(
            committed >= N2,
            "checkpoint advanced through all slots after the healed one, not stalled at N"
        );
        // N's deposit row is present despite the transient failure.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert!(inserted.iter().flatten().any(|t| t.slot == N as i64));
    }

    /// End-to-end exhaustion: a permanent write failure on slot N freezes the
    /// durable checkpoint at the last good slot below N, the processor exits
    /// Err, and N+1 is never persisted - so a restart replays from below N.
    #[tokio::test]
    async fn live_exhaustion_freezes_checkpoint_below_failed_slot() {
        const M: u64 = 300;
        const N: u64 = 301;
        const N1: u64 = 302;
        let (tx, processor_handle, checkpoint_handle, mock) =
            spawn_pipeline_result(deposit_instance());
        mock.set_should_fail("insert_db_transactions_batch", true);

        // M is an empty slot that checkpoints cleanly (last good slot).
        tx.send(ProcessorMessage::SlotComplete {
            slot: M,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        // N carries a deposit whose write fails permanently.
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            N,
            Some("dep-n".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: N,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        // N+1 is queued but must never be processed.
        tx.send(ProcessorMessage::Instruction(make_deposit_instruction(
            N1,
            Some("dep-n1".to_string()),
            None,
        )))
        .await
        .unwrap();
        tx.send(ProcessorMessage::SlotComplete {
            slot: N1,
            program_type: ProgramType::Escrow,
        })
        .await
        .unwrap();
        drop(tx);

        let result = processor_handle.await.unwrap();
        assert!(
            result.is_err(),
            "permanent write failure exits the processor"
        );
        checkpoint_handle.await.unwrap();

        let committed = mock
            .get_committed_checkpoint("escrow")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            committed, M,
            "checkpoint frozen at the last good slot below N"
        );
        assert!(committed < N);
        // Neither N nor N+1 was persisted.
        let inserted = mock.inserted_transactions.lock().unwrap();
        assert!(inserted.iter().flatten().all(|t| t.slot != N1 as i64));
    }
}
