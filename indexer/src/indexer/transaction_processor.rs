use crate::metrics;
use crate::{
    channel_utils::send_guaranteed,
    config::ProgramType,
    error::IndexerError,
    indexer::{
        checkpoint::CheckpointUpdate,
        datasource::common::{
            parser::{escrow_instance_of, EscrowInstruction, WithdrawInstruction},
            types::{InstructionWithMetadata, ProcessorMessage, ProgramInstruction},
        },
    },
    storage::{
        common::models::{
            DbMint, DbMintStatus, DbTransaction, DbTransactionBuilder, TransactionType,
        },
        Storage,
    },
};
use private_channel_metrics::{HealthState, MetricLabel};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Transaction processor that converts instructions to transactions and saves to DB
/// Tracks slot-level success/failure and emits committed checkpoints
///
/// Current implementation: Sequential slot processing with batch inserts per slot (Option 3)
pub struct TransactionProcessor {
    storage: Arc<Storage>,
    checkpoint_tx: mpsc::Sender<CheckpointUpdate>,

    // Per-slot instruction buffers, so a foreign SlotComplete finalizes only its own slot's rows.
    slot_buffers: HashMap<u64, Vec<InstructionWithMetadata>>,

    // Optional health state — bumped on each SlotComplete so /health knows the
    // indexer pipeline is making progress. None in tests / standalone uses.
    health: Option<Arc<HealthState>>,

    configured_escrow_instance_id: Option<Pubkey>,
}

impl TransactionProcessor {
    pub fn new(storage: Arc<Storage>, checkpoint_tx: mpsc::Sender<CheckpointUpdate>) -> Self {
        Self {
            storage,
            checkpoint_tx,
            slot_buffers: HashMap::new(),
            health: None,
            configured_escrow_instance_id: None,
        }
    }

    pub fn with_health(mut self, health: Arc<HealthState>) -> Self {
        self.health = Some(health);
        self
    }

    pub fn with_escrow_instance_id(mut self, escrow_instance_id: Pubkey) -> Self {
        self.configured_escrow_instance_id = Some(escrow_instance_id);
        self
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
                    self.finalize_and_checkpoint(slot, program_type).await;
                    metrics::INDEXER_SLOT_PROCESSING_DURATION
                        .with_label_values(&[program_type.as_label()])
                        .observe(start.elapsed().as_secs_f64());
                    if let Some(h) = &self.health {
                        h.record_progress();
                    }
                }
            }
        }

        info!("TransactionProcessor stopped");
        Ok(())
    }

    /// Finalize and checkpoint a slot
    /// Saves any buffered transactions and always sends checkpoint (even if empty)
    async fn finalize_and_checkpoint(&mut self, slot: u64, program_type: ProgramType) {
        let mut mints = Vec::new();
        let mut mint_statuses: Vec<DbMintStatus> = Vec::new();
        let mut transactions = Vec::new();

        let slot_instructions = self.slot_buffers.remove(&slot).unwrap_or_default();
        for instruction_meta in &slot_instructions {
            let (mint_opt, status_opt, transaction_opt) = convert_to_db_models(
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
        }

        let mut send_checkpoint = true;

        // Insert mints FIRST (before transactions that might reference them)
        if !mints.is_empty() {
            info!("Finalizing slot {} with {} mint(s)", slot, mints.len());

            match self.storage.upsert_mints_batch(&mints).await {
                Ok(_) => {
                    info!(
                        "Successfully saved {} mint(s) from slot {}",
                        mints.len(),
                        slot
                    );
                    metrics::INDEXER_MINTS_SAVED
                        .with_label_values(&[program_type.as_label()])
                        .inc_by(mints.len() as f64);
                }
                Err(e) => {
                    error!("Failed to save mints from slot {}: {}", slot, e);
                    metrics::INDEXER_SLOT_SAVE_ERRORS
                        .with_label_values(&[program_type.as_label()])
                        .inc();
                    send_checkpoint = false;
                }
            }
        }

        if !mint_statuses.is_empty() {
            match self
                .storage
                .insert_mint_statuses_batch(&mint_statuses)
                .await
            {
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
                    metrics::INDEXER_SLOT_SAVE_ERRORS
                        .with_label_values(&[program_type.as_label()])
                        .inc();
                    send_checkpoint = false;
                }
            }
        }

        // Derive the `mints.status` mirror for each touched mint from history.
        // Gated on the writes above, so the mirror never leads the timeline.
        if send_checkpoint && !mint_statuses.is_empty() {
            let mut touched: Vec<String> = mint_statuses
                .iter()
                .map(|s| s.mint_address.clone())
                .collect();
            touched.sort_unstable();
            touched.dedup();
            if let Err(e) = self.storage.sync_mint_status(&touched).await {
                error!("Failed to sync mint status mirror for slot {}: {}", slot, e);
                metrics::INDEXER_SLOT_SAVE_ERRORS
                    .with_label_values(&[program_type.as_label()])
                    .inc();
                send_checkpoint = false;
            }
        }

        if transactions.is_empty() {
            // Empty slot, just checkpoint it
            debug!("Finalizing empty slot {}", slot);
        } else if !send_checkpoint {
            // A prerequisite write (mints/statuses) failed; skip the deposit
            // rows so we don't commit a deposit with no backing row. The slot
            // isn't checkpointed, so it replays atomically.
            warn!(
                "Skipping transaction insert for slot {} ({} row(s)) because an earlier \
                 write failed; slot will be reprocessed",
                slot,
                transactions.len()
            );
        } else {
            info!(
                "Finalizing slot {} with {} transactions",
                slot,
                transactions.len()
            );

            match self
                .storage
                .insert_db_transactions_batch(&transactions)
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
                    metrics::INDEXER_SLOT_SAVE_ERRORS
                        .with_label_values(&[program_type.as_label()])
                        .inc();
                    send_checkpoint = false;
                }
            }
        }

        if send_checkpoint {
            const MAX_ATTEMPTS: usize = 3;
            let mut attempt = 0;
            loop {
                let res = send_guaranteed(
                    &self.checkpoint_tx,
                    CheckpointUpdate { program_type, slot },
                    "checkpoint",
                )
                .await;

                match res {
                    Ok(_) => {
                        metrics::INDEXER_SLOTS_PROCESSED
                            .with_label_values(&[program_type.as_label()])
                            .inc();
                        metrics::INDEXER_CURRENT_SLOT
                            .with_label_values(&[program_type.as_label()])
                            .set(slot as f64);
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        error!(
                            "Checkpoint send failed for slot {} (attempt {}/{}): {}",
                            slot, attempt, MAX_ATTEMPTS, e
                        );
                        if attempt >= MAX_ATTEMPTS {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
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

/// Convert an instruction to a `(DbMint, MintStatusChange, DbTransaction)` triple,
/// each element independently optional:
/// - `AllowMint` → mints-row upsert + `"allowed"` transition.
/// - `BlockMint` → `"blocked"` transition only (mints row already exists).
/// - `Deposit` / `WithdrawFunds` → transaction row only.
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
) {
    let signature = match instruction_meta.signature.as_ref() {
        Some(sig) => sig,
        None => return (None, None, None),
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
                return (None, None, None);
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
                    )
                }
                EscrowInstruction::BlockMint { accounts } => (
                    None,
                    Some(MintStatusChange {
                        mint_address: accounts.mint.to_string(),
                        status: MintStatus::Blocked,
                    }),
                    None,
                ),
                _ => (None, None, None),
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
        DepositData, DepositEvent, ResetSmtRootAccounts, WithdrawFundsAccounts, WithdrawFundsData,
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

    /// Instance pubkey hardcoded by `make_reset_smt_root_instruction`.
    fn reset_smt_instance() -> Pubkey {
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

    fn make_reset_smt_root_instruction(slot: u64, sig: Option<String>) -> InstructionWithMetadata {
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::ResetSmtRoot {
                accounts: ResetSmtRootAccounts {
                    payer: make_pubkey(10),
                    operator: make_pubkey(11),
                    instance: reset_smt_instance(),
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

    // ========================================================================
    // convert_to_db_models tests
    // ========================================================================

    #[test]
    fn convert_deposit_with_explicit_recipient() {
        let recipient = make_pubkey(99);
        let ix = make_deposit_instruction(100, Some("sig1".to_string()), Some(recipient));
        let (mint, status, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
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
        let (_, _, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
        let txn = txn.unwrap();
        // recipient should default to accounts.user
        assert_eq!(txn.recipient, make_pubkey(1).to_string());
    }

    #[test]
    fn convert_allow_mint_returns_mint_no_txn() {
        let ix = make_allow_mint_instruction(200, Some("sig3".to_string()));
        let (mint, status, txn) = convert_to_db_models(&ix, Some(&allow_mint_instance()));
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
        let (mint, status, txn) = convert_to_db_models(&ix, Some(&allow_mint_instance()));
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
        let (mint, status, txn) = convert_to_db_models(&ix, None);
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

        let (_, _, txn0) = convert_to_db_models(&ix0, Some(&deposit_instance()));
        let (_, _, txn1) = convert_to_db_models(&ix1, Some(&deposit_instance()));

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
        let (mint, status, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    #[test]
    fn convert_catchall_escrow_variant_returns_none() {
        let ix = make_reset_smt_root_instruction(100, Some("sig5".to_string()));
        let (mint, status, txn) = convert_to_db_models(&ix, Some(&reset_smt_instance()));
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

        let (mint, status, txn) = convert_to_db_models(&ix, Some(&watched));
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

        let (mint, status, txn) = convert_to_db_models(&ix, Some(&watched));
        assert!(mint.is_none());
        assert!(status.is_none());
        assert!(txn.is_none());
    }

    // ========================================================================
    // TransactionProcessor tests
    // ========================================================================

    fn make_processor_and_rx(
        escrow_instance_id: Pubkey,
    ) -> (
        TransactionProcessor,
        tokio::sync::mpsc::Receiver<CheckpointUpdate>,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock));
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::mpsc::channel(100);
        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id);
        (processor, checkpoint_rx)
    }

    fn make_processor_with_mock(
        escrow_instance_id: Pubkey,
    ) -> (
        TransactionProcessor,
        tokio::sync::mpsc::Receiver<CheckpointUpdate>,
        MockStorage,
    ) {
        let mock = MockStorage::new();
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::mpsc::channel(100);
        let processor = TransactionProcessor::new(storage, checkpoint_tx)
            .with_escrow_instance_id(escrow_instance_id);
        (processor, checkpoint_rx, mock)
    }

    #[tokio::test]
    async fn finalize_empty_slot_sends_checkpoint() {
        let (mut processor, mut checkpoint_rx) = make_processor_and_rx(deposit_instance());
        processor
            .finalize_and_checkpoint(42, ProgramType::Escrow)
            .await;
        let cp = checkpoint_rx.recv().await.unwrap();
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
            .await;

        {
            let inserted = mock.inserted_transactions.lock().unwrap();
            assert_eq!(inserted.len(), 1);
            assert_eq!(inserted[0].len(), 1);
        }

        let cp = checkpoint_rx.recv().await.unwrap();
        assert_eq!(cp.slot, 100);
    }

    #[tokio::test]
    async fn finalize_with_mints_upserts_first() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        processor.buffer(make_allow_mint_instruction(200, Some("s2".to_string())));
        processor
            .finalize_and_checkpoint(200, ProgramType::Escrow)
            .await;

        {
            let mints = mock.mints.lock().unwrap();
            assert_eq!(mints.len(), 1);
            assert!(mints.contains_key(&make_pubkey(2).to_string()));
        }

        let cp = checkpoint_rx.recv().await.unwrap();
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
            .await;

        {
            let rows = mock.mint_status_history.lock().unwrap();
            assert_eq!(rows.len(), 1, "exactly one status row should be written");
            assert_eq!(rows[0].mint_address, make_pubkey(2).to_string());
            assert_eq!(rows[0].status, "allowed");
            assert_eq!(rows[0].effective_slot, 200);
            assert_eq!(rows[0].signature, "sig-allow-1");
        }

        let cp = checkpoint_rx.recv().await.unwrap();
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
            .await;

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

        let cp = checkpoint_rx.recv().await.unwrap();
        assert_eq!(cp.slot, 250);
    }

    #[tokio::test]
    async fn finalize_insert_mint_statuses_failure_skips_checkpoint() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("insert_mint_statuses_batch", true);
        processor.buffer(make_allow_mint_instruction(
            201,
            Some("sig-allow-2".to_string()),
        ));
        processor
            .finalize_and_checkpoint(201, ProgramType::Escrow)
            .await;

        assert!(checkpoint_rx.try_recv().is_err());
    }

    /// AllowMint + Deposit for the same mint in one slot: if the mint-status
    /// write fails, the deposit row must be withheld (else the gate would
    /// quarantine it) and the slot replays.
    #[tokio::test]
    async fn finalize_mint_status_failure_withholds_deposit_in_same_slot() {
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
        processor
            .finalize_and_checkpoint(202, ProgramType::Escrow)
            .await;

        // Checkpoint withheld so the slot replays.
        assert!(checkpoint_rx.try_recv().is_err());
        // Deposit row must not be committed without its backing status row.
        assert!(
            mock.inserted_transactions.lock().unwrap().is_empty(),
            "deposit must be withheld when the mint-status write failed"
        );
    }

    #[tokio::test]
    async fn finalize_upsert_mints_failure_skips_checkpoint() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("upsert_mints_batch", true);
        processor.buffer(make_allow_mint_instruction(300, Some("s3".to_string())));
        processor
            .finalize_and_checkpoint(300, ProgramType::Escrow)
            .await;

        // No checkpoint should be sent
        assert!(checkpoint_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn finalize_insert_batch_failure_skips_checkpoint() {
        let (mut processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        mock.set_should_fail("insert_db_transactions_batch", true);
        processor.buffer(make_deposit_instruction(400, Some("s4".to_string()), None));
        processor
            .finalize_and_checkpoint(400, ProgramType::Escrow)
            .await;

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

        let cp = checkpoint_rx.recv().await.unwrap();
        assert_eq!(cp.slot, 500);
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

        let batches = mock.inserted_transactions.lock().unwrap();
        assert_eq!(batches.len(), 2, "each slot finalizes its own batch");
        assert_eq!(batches[0][0].signature, "a");
        assert_eq!(batches[0][0].slot, SLOT_A as i64);
        assert_eq!(batches[1][0].signature, "b");
        assert_eq!(batches[1][0].slot, SLOT_B as i64);
        drop(batches);

        let first = checkpoint_rx.recv().await.unwrap();
        let second = checkpoint_rx.recv().await.unwrap();
        assert_eq!(first.slot, SLOT_A);
        assert_eq!(second.slot, SLOT_B);
    }

    /// A foreign SlotComplete between a same-slot AllowMint and Deposit must not split the
    /// finalize: the later mint-status failure still withholds the deposit and the checkpoint.
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

        processor.start(rx).await.unwrap();

        let mut checkpointed = Vec::new();
        while let Ok(cp) = checkpoint_rx.try_recv() {
            checkpointed.push(cp.slot);
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

    #[tokio::test]
    async fn start_channel_close_exits_ok() {
        let (processor, _checkpoint_rx) = make_processor_and_rx(deposit_instance());
        let (_tx, rx) = tokio::sync::mpsc::channel(10);
        drop(_tx);

        let result = processor.start(rx).await;
        assert!(result.is_ok());
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
            .with_escrow_instance_id(escrow_instance_id);
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
}
