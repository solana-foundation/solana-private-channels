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
        common::models::{DbMint, DbTransaction, DbTransactionBuilder, TransactionType},
        Storage,
    },
};
use private_channel_metrics::{HealthState, MetricLabel};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Transaction processor that converts instructions to transactions and saves to DB
/// Tracks slot-level success/failure and emits committed checkpoints
///
/// Current implementation: Sequential slot processing with batch inserts per slot (Option 3)
pub struct TransactionProcessor {
    storage: Arc<Storage>,
    checkpoint_tx: mpsc::Sender<CheckpointUpdate>,
    current_slot: Option<u64>,
    current_program_type: Option<ProgramType>,

    // Buffer all instructions from current slot for batch processing
    current_slot_instructions: Vec<InstructionWithMetadata>,

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
            current_slot: None,
            current_program_type: None,
            current_slot_instructions: Vec::new(),
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
                    // Buffer instruction for current slot
                    self.current_slot = Some(instruction_meta.slot);
                    self.current_program_type = Some(instruction_meta.program_type);
                    self.current_slot_instructions.push(instruction_meta);
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
        let mut transactions = Vec::new();

        for instruction_meta in &self.current_slot_instructions {
            let (mint_opt, transaction_opt) = convert_to_db_models(
                instruction_meta,
                self.configured_escrow_instance_id.as_ref(),
            );

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

        if !transactions.is_empty() {
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
        } else {
            // Empty slot, just checkpoint it
            debug!("Finalizing empty slot {}", slot);
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

        self.current_slot_instructions.clear();
        self.current_slot = None;
        self.current_program_type = None;
    }
}

/// Convert an instruction to either a DbMint or DbTransaction model
///
/// Returns None for instructions that shouldn't be tracked in the database.
/// Escrow instructions whose `accounts.instance` does not equal the configured
/// escrow instance are dropped here; this is the per-instruction scoping that
/// keeps a foreign instance from being persisted via this processor.
fn convert_to_db_models(
    instruction_meta: &InstructionWithMetadata,
    configured_escrow_instance_id: Option<&Pubkey>,
) -> (Option<DbMint>, Option<DbTransaction>) {
    let signature = match instruction_meta.signature.as_ref() {
        Some(sig) => sig,
        None => return (None, None),
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
                return (None, None);
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
                            .build(),
                        ),
                    )
                }
                EscrowInstruction::AllowMint {
                    accounts, event, ..
                } => (
                    Some(DbMint::new(
                        accounts.mint.to_string(),
                        event.decimals as i16,
                        accounts.token_program.to_string(),
                    )),
                    None,
                ),
                _ => (None, None),
            }
        }

        ProgramInstruction::Withdraw(withdraw_ix) => match withdraw_ix.as_ref() {
            WithdrawInstruction::WithdrawFunds { accounts, data } => {
                let recipient = data.destination.to_string();

                (
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
    use crate::indexer::datasource::common::parser::{
        AllowMintAccounts, AllowMintData, AllowMintEvent, DepositAccounts, DepositData,
        DepositEvent, ResetSmtRootAccounts, WithdrawFundsAccounts, WithdrawFundsData,
    };
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
        let user = make_pubkey(1);
        let mint = make_pubkey(2);
        InstructionWithMetadata {
            instruction: ProgramInstruction::Escrow(Box::new(EscrowInstruction::Deposit {
                accounts: DepositAccounts {
                    payer: make_pubkey(10),
                    user,
                    instance: deposit_instance(),
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
        }
    }

    // ========================================================================
    // convert_to_db_models tests
    // ========================================================================

    #[test]
    fn convert_deposit_with_explicit_recipient() {
        let recipient = make_pubkey(99);
        let ix = make_deposit_instruction(100, Some("sig1".to_string()), Some(recipient));
        let (mint, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
        assert!(mint.is_none());
        let txn = txn.unwrap();
        assert_eq!(txn.signature, "sig1");
        assert_eq!(txn.slot, 100);
        // event.amount = 990, data.amount = 1000 (see make_deposit_instruction).
        // The DB row must carry the event-reported amount.
        assert_eq!(txn.amount, 990);
        assert_eq!(txn.recipient, recipient.to_string());
        assert_eq!(txn.initiator, make_pubkey(1).to_string());
        assert!(matches!(txn.transaction_type, TransactionType::Deposit));
    }

    #[test]
    fn convert_deposit_none_recipient_defaults_to_user() {
        let ix = make_deposit_instruction(50, Some("sig2".to_string()), None);
        let (_, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
        let txn = txn.unwrap();
        // recipient should default to accounts.user
        assert_eq!(txn.recipient, make_pubkey(1).to_string());
    }

    #[test]
    fn convert_allow_mint_returns_mint_no_txn() {
        let ix = make_allow_mint_instruction(200, Some("sig3".to_string()));
        let (mint, txn) = convert_to_db_models(&ix, Some(&allow_mint_instance()));
        assert!(txn.is_none());
        let mint = mint.unwrap();
        assert_eq!(mint.mint_address, make_pubkey(2).to_string());
        assert_eq!(mint.decimals, 6);
        // The indexer leaves Token-2022 extension resolution to the operator —
        // both flags must stay None at AllowMint time.
        assert_eq!(mint.is_pausable, None);
        assert_eq!(mint.has_permanent_delegate, None);
    }

    #[test]
    fn convert_withdraw_funds() {
        let ix = make_withdraw_instruction(300, Some("sig4".to_string()));
        let (mint, txn) = convert_to_db_models(&ix, None);
        assert!(mint.is_none());
        let txn = txn.unwrap();
        assert_eq!(txn.amount, 500);
        assert_eq!(txn.recipient, make_pubkey(20).to_string());
        assert!(matches!(txn.transaction_type, TransactionType::Withdrawal));
    }

    #[test]
    fn convert_no_signature_returns_none() {
        let ix = make_deposit_instruction(100, None, None);
        let (mint, txn) = convert_to_db_models(&ix, Some(&deposit_instance()));
        assert!(mint.is_none());
        assert!(txn.is_none());
    }

    #[test]
    fn convert_catchall_escrow_variant_returns_none() {
        let ix = make_reset_smt_root_instruction(100, Some("sig5".to_string()));
        let (mint, txn) = convert_to_db_models(&ix, Some(&reset_smt_instance()));
        assert!(mint.is_none());
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
        };

        let (mint, txn) = convert_to_db_models(&ix, Some(&watched));
        assert!(mint.is_none());
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
        };

        let (mint, txn) = convert_to_db_models(&ix, Some(&watched));
        assert!(mint.is_none());
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
        assert!(processor.current_slot_instructions.is_empty());
    }

    #[tokio::test]
    async fn finalize_with_deposits_inserts_batch() {
        let (mut processor, mut checkpoint_rx, mock) = make_processor_with_mock(deposit_instance());
        processor
            .current_slot_instructions
            .push(make_deposit_instruction(100, Some("s1".to_string()), None));
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
        processor
            .current_slot_instructions
            .push(make_allow_mint_instruction(200, Some("s2".to_string())));
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
    async fn finalize_upsert_mints_failure_skips_checkpoint() {
        let (mut processor, mut checkpoint_rx, mock) =
            make_processor_with_mock(allow_mint_instance());
        mock.set_should_fail("upsert_mints_batch", true);
        processor
            .current_slot_instructions
            .push(make_allow_mint_instruction(300, Some("s3".to_string())));
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
        processor
            .current_slot_instructions
            .push(make_deposit_instruction(400, Some("s4".to_string()), None));
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

    #[tokio::test]
    async fn start_channel_close_exits_ok() {
        let (processor, _checkpoint_rx) = make_processor_and_rx(deposit_instance());
        let (_tx, rx) = tokio::sync::mpsc::channel(10);
        drop(_tx);

        let result = processor.start(rx).await;
        assert!(result.is_ok());
    }
}
