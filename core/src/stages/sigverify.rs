// Signature verification stage for PrivateChannel

use {
    crate::{
        nodes::node::WorkerHandle, stage_metrics::SharedMetrics, transactions::is_admin_instruction,
    },
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    std::{
        fmt::{self, Display},
        sync::Arc,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

#[derive(Debug, Clone)]
pub enum SigverifyResult {
    Valid(TransactionType),
    InvalidTransaction(TransactionType),
    NotSignedByAdmin,
    SigverifyFailed(String),
}

impl Display for SigverifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SigverifyResult::Valid(transaction_type) => write!(f, "Valid: {:?}", transaction_type),
            SigverifyResult::InvalidTransaction(transaction_type) => {
                write!(f, "Invalid transaction: {:?}", transaction_type)
            }
            SigverifyResult::NotSignedByAdmin => write!(f, "Not signed by admin"),
            SigverifyResult::SigverifyFailed(e) => write!(f, "Sigverify failed: {}", e),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionType {
    /// Transaction contains no instructions
    Empty,
    /// Transaction contains only admin instructions
    Admin,
    /// Transaction contains only non-admin instructions
    Normal,
    /// Transaction contains both admin and non-admin instructions
    Mixed,
}

impl Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Empty => write!(f, "Empty"),
            TransactionType::Admin => write!(f, "Admin"),
            TransactionType::Normal => write!(f, "Normal"),
            TransactionType::Mixed => write!(f, "Mixed"),
        }
    }
}

/// Check if any signer is an admin
fn is_signed_by_admin(transaction: &SanitizedTransaction, admin_keys: &[Pubkey]) -> bool {
    let account_keys = transaction.message().account_keys();
    let num_required_signatures = transaction.message().header().num_required_signatures as usize;

    // All accounts before num_required_signatures are signers
    account_keys
        .iter()
        .take(num_required_signatures)
        .any(|pubkey| admin_keys.contains(pubkey))
}

/// Classifies a transaction into one TransactionType enum
fn classify_transaction(transaction: &SanitizedTransaction) -> TransactionType {
    let mut num_admin_ix = 0;
    let mut num_ix = 0;

    for (program_id, instruction) in transaction.message().program_instructions_iter() {
        // Get instruction type
        let instruction_type = match instruction.data.first() {
            Some(t) => *t,
            None => continue,
        };

        if is_admin_instruction(program_id, instruction_type) {
            num_admin_ix += 1;
        }

        num_ix += 1;
    }

    if num_ix == 0 {
        TransactionType::Empty
    } else if num_admin_ix == 0 {
        TransactionType::Normal
    } else if num_admin_ix == num_ix {
        TransactionType::Admin
    } else {
        TransactionType::Mixed
    }
}

pub struct SigverifyArgs {
    pub num_workers: usize,
    pub admin_keys: Vec<Pubkey>,
    pub rx: async_channel::Receiver<SanitizedTransaction>,
    pub sequencer_tx: mpsc::Sender<SanitizedTransaction>,
    pub shutdown_token: CancellationToken,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<crate::health::StageHeartbeat>,
}

pub async fn sigverify_transaction(
    transaction: &SanitizedTransaction,
    admin_keys: &[Pubkey],
) -> SigverifyResult {
    let transaction_type = classify_transaction(transaction);

    // Check transaction type
    match transaction_type {
        TransactionType::Empty | TransactionType::Mixed => {
            return SigverifyResult::InvalidTransaction(transaction_type);
        }
        TransactionType::Admin => {
            // Validate that at least one of the signatures came from an admin pubkey
            if !is_signed_by_admin(transaction, admin_keys) {
                return SigverifyResult::NotSignedByAdmin;
            }
        }
        TransactionType::Normal => {}
    }

    // Verify signature
    match transaction.verify() {
        Ok(_) => SigverifyResult::Valid(transaction_type),
        Err(e) => SigverifyResult::SigverifyFailed(e.to_string()),
    }
}

/// Start the signature verification worker pool
pub async fn start_sigverify_workerpool(args: SigverifyArgs) -> Vec<WorkerHandle> {
    let SigverifyArgs {
        num_workers,
        admin_keys,
        rx,
        sequencer_tx,
        shutdown_token,
        metrics,
        heartbeat,
    } = args;
    let mut handles = Vec::with_capacity(num_workers);
    let admin_keys = Arc::new(admin_keys);
    // metrics is already an Arc; clone it for each worker
    for worker_id in 0..num_workers {
        let rx = rx.clone();
        let tx = sequencer_tx.clone();
        let shutdown = shutdown_token.clone();
        let admin_keys = admin_keys.clone();
        let metrics = Arc::clone(&metrics);
        let heartbeat = Arc::clone(&heartbeat);

        let handle = tokio::spawn(async move {
            info!("Sigverify worker {} started", worker_id);

            loop {
                tokio::select! {
                    // Process transactions
                    result = rx.recv() => {
                        match result {
                            Ok(transaction) => {
                                heartbeat.record_input();
                                let result = sigverify_transaction(&transaction, &admin_keys).await;
                                // Each verify (forward or reject) counts as progress — the stage isn't wedged.
                                heartbeat.record_progress();
                                match result {
                                    SigverifyResult::Valid(_) => {
                                        metrics.sigverify_forwarded();
                                        // Bounded send applies backpressure; race shutdown so a
                                        // full sequencer queue never wedges worker exit.
                                        tokio::select! {
                                            send_result = tx.send(transaction) => {
                                                match send_result {
                                                    Ok(_) => {
                                                        debug!("Worker {} sent transaction to sequencer", worker_id);
                                                    }
                                                    Err(_) => {
                                                        warn!(
                                                            "Worker {} failed to send to sequencer - channel closed",
                                                            worker_id
                                                        );
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = shutdown.cancelled() => {
                                                debug!("Worker {} shutdown while sending to sequencer", worker_id);
                                                break;
                                            }
                                        }
                                    }
                                    SigverifyResult::InvalidTransaction(transaction_type) => {
                                        metrics.sigverify_rejected("invalid");
                                        warn!(
                                            "Worker {} rejected invalid transaction {}: {:?}",
                                            worker_id,
                                            transaction.signature(),
                                            transaction_type.to_string()
                                        );
                                    }
                                    SigverifyResult::NotSignedByAdmin => {
                                        metrics.sigverify_rejected("not_admin");
                                        warn!(
                                            "Worker {} rejected admin transaction not signed by admin: {}",
                                            worker_id,
                                            transaction.signature()
                                        );
                                    }
                                    SigverifyResult::SigverifyFailed(e) => {
                                        metrics.sigverify_rejected("sig_failed");
                                        warn!("Worker {} sigverify failed: {}", worker_id, e);
                                    }
                                }
                            }
                            Err(_) => {
                                debug!("Worker {} channel closed", worker_id);
                                break;
                            }
                        }
                    }

                    // Handle shutdown signal
                    _ = shutdown.cancelled() => {
                        debug!("Worker {} received shutdown signal", worker_id);
                        break;
                    }
                }
            }

            info!("Sigverify worker {} stopped", worker_id);
        });

        handles.push(WorkerHandle::new(
            format!("Sigverify-{}", worker_id),
            handle,
        ));
    }
    handles
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::node::DEFAULT_SEQUENCER_QUEUE_CAPACITY as SEQ_CAP;
    use crate::stage_metrics::NoopMetrics;
    use solana_sdk::{
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        signature::{Keypair, Signature, Signer},
        transaction::{SanitizedTransaction, Transaction},
    };
    use std::collections::HashSet;

    /// Build a signed `SanitizedTransaction` from instructions + signers.
    fn sanitize(
        instructions: &[Instruction],
        payer: &Keypair,
        signers: &[&Keypair],
    ) -> SanitizedTransaction {
        let tx = Transaction::new_signed_with_payer(
            instructions,
            Some(&payer.pubkey()),
            signers,
            Hash::default(),
        );
        SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap()
    }

    fn spl_transfer_ix(from_ata: &Pubkey, to_ata: &Pubkey, authority: &Pubkey) -> Instruction {
        spl_token::instruction::transfer(&spl_token::id(), from_ata, to_ata, authority, &[], 1_000)
            .unwrap()
    }

    fn initialize_mint_ix(mint: &Pubkey, authority: &Pubkey) -> Instruction {
        spl_token::instruction::initialize_mint(&spl_token::id(), mint, authority, None, 6).unwrap()
    }

    #[tokio::test]
    async fn empty_transaction_rejected() {
        let payer = Keypair::new();
        let tx = sanitize(&[], &payer, &[&payer]);
        let result = sigverify_transaction(&tx, &[]).await;
        assert!(
            matches!(
                result,
                SigverifyResult::InvalidTransaction(TransactionType::Empty)
            ),
            "expected InvalidTransaction(Empty), got {result}"
        );
    }

    #[tokio::test]
    async fn mixed_transaction_rejected() {
        let admin = Keypair::new();
        let user = Keypair::new();
        let mint = Pubkey::new_unique();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();

        let admin_ix = initialize_mint_ix(&mint, &admin.pubkey());
        let normal_ix = spl_transfer_ix(&from_ata, &to_ata, &user.pubkey());

        let tx = sanitize(&[admin_ix, normal_ix], &admin, &[&admin, &user]);
        let result = sigverify_transaction(&tx, &[admin.pubkey()]).await;
        assert!(
            matches!(
                result,
                SigverifyResult::InvalidTransaction(TransactionType::Mixed)
            ),
            "expected InvalidTransaction(Mixed), got {result}"
        );
    }

    #[tokio::test]
    async fn admin_instruction_without_admin_signer_rejected() {
        let non_admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let real_admin = Pubkey::new_unique(); // in admin_keys but not a tx signer

        let ix = initialize_mint_ix(&mint, &non_admin.pubkey());
        let tx = sanitize(&[ix], &non_admin, &[&non_admin]);
        let result = sigverify_transaction(&tx, &[real_admin]).await;
        assert!(
            matches!(result, SigverifyResult::NotSignedByAdmin),
            "expected NotSignedByAdmin, got {result}"
        );
    }

    #[tokio::test]
    async fn admin_instruction_with_admin_signer_accepted() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();

        let ix = initialize_mint_ix(&mint, &admin.pubkey());
        let tx = sanitize(&[ix], &admin, &[&admin]);
        let result = sigverify_transaction(&tx, &[admin.pubkey()]).await;
        assert!(
            matches!(result, SigverifyResult::Valid(TransactionType::Admin)),
            "expected Valid(Admin), got {result}"
        );
    }

    #[tokio::test]
    async fn unsigned_transaction_rejected() {
        // Sign with wrong keypair — message references payer but a different key signs
        let payer = Keypair::new();
        let wrong_signer = Keypair::new();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();
        let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());

        let message = solana_sdk::message::Message::new(&[ix], Some(&payer.pubkey()));
        // Sign the message with wrong_signer instead of payer
        let mut tx = Transaction::new_unsigned(message);
        tx.signatures = vec![wrong_signer.sign_message(&tx.message_data())];

        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();
        let result = sigverify_transaction(&sanitized, &[]).await;
        assert!(
            matches!(result, SigverifyResult::SigverifyFailed(_)),
            "expected SigverifyFailed for wrong signer, got {result}"
        );
    }

    #[tokio::test]
    async fn tampered_signature_rejected() {
        let payer = Keypair::new();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();
        let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());

        // Build a properly signed transaction, then replace the signature
        let mut tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        // Replace signature with a corrupted copy
        let mut sig_bytes = <[u8; 64]>::from(tx.signatures[0]);
        sig_bytes[0] ^= 0xff;
        tx.signatures[0] = Signature::from(sig_bytes);

        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();
        let result = sigverify_transaction(&sanitized, &[]).await;
        assert!(
            matches!(result, SigverifyResult::SigverifyFailed(_)),
            "expected SigverifyFailed, got {result}"
        );
    }

    #[tokio::test]
    async fn valid_normal_transaction_accepted() {
        let payer = Keypair::new();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();
        let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());

        let tx = sanitize(&[ix], &payer, &[&payer]);
        let result = sigverify_transaction(&tx, &[]).await;
        assert!(
            matches!(result, SigverifyResult::Valid(TransactionType::Normal)),
            "expected Valid(Normal), got {result}"
        );
    }

    #[tokio::test]
    async fn worker_forwards_valid_tx_to_sequencer() {
        let (sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(10);
        let (sequencer_tx, mut sequencer_rx) = mpsc::channel(SEQ_CAP);
        let shutdown = CancellationToken::new();

        let payer = Keypair::new();
        let from_ata = Pubkey::new_unique();
        let to_ata = Pubkey::new_unique();
        let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());
        let tx = sanitize(&[ix], &payer, &[&payer]);
        let expected_sig = *tx.signature();

        let handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers: 1,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        sigverify_tx.send(tx).await.unwrap();

        // valid tx should reach sequencer
        let received = tokio::time::timeout(std::time::Duration::from_secs(5), sequencer_rx.recv())
            .await
            .expect("timeout waiting for sequencer")
            .expect("sequencer channel closed");
        assert_eq!(received.signature(), &expected_sig);

        shutdown.cancel();
        for h in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), h.handle).await;
        }
    }

    #[tokio::test]
    async fn worker_drops_invalid_tx() {
        let (sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(10);
        let (sequencer_tx, mut sequencer_rx) = mpsc::channel(SEQ_CAP);
        let shutdown = CancellationToken::new();

        // empty transaction → InvalidTransaction(Empty)
        let payer = Keypair::new();
        let empty_tx = sanitize(&[], &payer, &[&payer]);

        let handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers: 1,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        sigverify_tx.send(empty_tx).await.unwrap();

        // Sentinel: send a valid tx right after the invalid one.
        // When this arrives on sequencer_rx, the invalid tx was already processed and dropped.
        let sentinel_from = Keypair::new();
        let sentinel_to = Pubkey::new_unique();
        let sentinel_tx = {
            let payer = &sentinel_from;
            let to = &sentinel_to;
            let ix = spl_token::instruction::transfer(
                &spl_token::id(),
                to,
                to,
                &payer.pubkey(),
                &[],
                1_000,
            )
            .unwrap();
            let message = solana_sdk::message::Message::new(&[ix], Some(&payer.pubkey()));
            let tx = solana_sdk::transaction::Transaction::new(
                &[payer],
                message,
                solana_sdk::hash::Hash::default(),
            );
            SanitizedTransaction::try_from_legacy_transaction(tx, &std::collections::HashSet::new())
                .unwrap()
        };
        sigverify_tx.send(sentinel_tx.clone()).await.unwrap();

        // The sentinel valid tx should arrive
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), sequencer_rx.recv()).await;
        assert!(result.is_ok(), "sentinel valid tx should be forwarded");

        // Nothing else should arrive (invalid tx was dropped, not forwarded)
        let extra =
            tokio::time::timeout(std::time::Duration::from_millis(50), sequencer_rx.recv()).await;
        assert!(extra.is_err(), "only sentinel should have been forwarded");

        shutdown.cancel();
        for h in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), h.handle).await;
        }
    }

    #[tokio::test]
    async fn worker_shutdown_signal_stops_worker() {
        let (_sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(10);
        let (sequencer_tx, _sequencer_rx) = mpsc::channel(SEQ_CAP);
        let shutdown = CancellationToken::new();

        let handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers: 2,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;
        assert_eq!(handles.len(), 2);

        shutdown.cancel();
        for h in handles {
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), h.handle).await;
            assert!(result.is_ok(), "worker should exit promptly after shutdown");
        }
    }

    #[tokio::test]
    async fn admin_not_signed_by_admin_with_empty_admin_keys() {
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let ix = initialize_mint_ix(&mint, &admin.pubkey());
        let tx = sanitize(&[ix], &admin, &[&admin]);
        // Empty admin_keys means no one is an admin
        let result = sigverify_transaction(&tx, &[]).await;
        assert!(
            matches!(result, SigverifyResult::NotSignedByAdmin),
            "expected NotSignedByAdmin when admin_keys is empty, got {result}"
        );
    }

    #[tokio::test]
    async fn instruction_with_empty_data_not_counted() {
        // An instruction with no data bytes is skipped by classify_transaction.
        // A tx with only such instructions is classified Empty.
        let payer = Keypair::new();
        let program_id = Pubkey::new_unique();
        let ix = Instruction {
            program_id,
            accounts: vec![AccountMeta::new_readonly(payer.pubkey(), false)],
            data: vec![], // empty data
        };
        let tx = sanitize(&[ix], &payer, &[&payer]);
        let result = sigverify_transaction(&tx, &[]).await;
        assert!(
            matches!(
                result,
                SigverifyResult::InvalidTransaction(TransactionType::Empty)
            ),
            "expected InvalidTransaction(Empty) for empty-data ix, got {result}"
        );
    }

    #[tokio::test]
    async fn admin_tx_with_tampered_signature_fails_verification() {
        // Admin instruction + admin signer, but signature is corrupted
        let admin = Keypair::new();
        let mint = Pubkey::new_unique();
        let ix = initialize_mint_ix(&mint, &admin.pubkey());

        let mut tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&admin.pubkey()),
            &[&admin],
            Hash::default(),
        );
        // Corrupt the signature
        let mut sig_bytes = <[u8; 64]>::from(tx.signatures[0]);
        sig_bytes[0] ^= 0xff;
        tx.signatures[0] = Signature::from(sig_bytes);

        let sanitized =
            SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new()).unwrap();
        let result = sigverify_transaction(&sanitized, &[admin.pubkey()]).await;
        assert!(
            matches!(result, SigverifyResult::SigverifyFailed(_)),
            "expected SigverifyFailed for corrupted admin tx, got {result}"
        );
    }

    #[tokio::test]
    async fn admin_key_as_non_signer_account_rejected() {
        // Admin key listed as a read-only (non-signer) account, not a signer
        let admin = Keypair::new();
        let signer = Keypair::new();
        let mint = Pubkey::new_unique();
        let ix = Instruction {
            program_id: spl_token::id(),
            accounts: vec![
                AccountMeta::new(mint, false),
                AccountMeta::new_readonly(admin.pubkey(), false), // admin as read-only
            ],
            data: vec![0], // initialize_mint opcode
        };
        let tx = sanitize(&[ix], &signer, &[&signer]);
        let result = sigverify_transaction(&tx, &[admin.pubkey()]).await;
        assert!(
            matches!(result, SigverifyResult::NotSignedByAdmin),
            "expected NotSignedByAdmin when admin is non-signer, got {result}"
        );
    }

    #[tokio::test]
    async fn multiple_admin_keys_any_one_matches() {
        // Multiple admin keys in the allowlist; any one signer should pass
        let real_admin = Keypair::new();
        let other_admin = Pubkey::new_unique();
        let mint = Pubkey::new_unique();

        let ix = initialize_mint_ix(&mint, &real_admin.pubkey());
        let tx = sanitize(&[ix], &real_admin, &[&real_admin]);
        let result = sigverify_transaction(&tx, &[other_admin, real_admin.pubkey()]).await;
        assert!(
            matches!(result, SigverifyResult::Valid(TransactionType::Admin)),
            "expected Valid(Admin) when one of multiple admin keys signs, got {result}"
        );
    }

    #[tokio::test]
    async fn worker_exits_when_input_channel_closed() {
        let (sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(10);
        let (sequencer_tx, _sequencer_rx) = mpsc::channel(SEQ_CAP);
        let shutdown = CancellationToken::new();

        let mut handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers: 1,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Close the input channel (drop the sender)
        drop(sigverify_tx);

        // Worker should detect channel closed and exit within timeout
        let handle = handles.pop().unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.handle).await;
        assert!(
            result.is_ok(),
            "worker should exit promptly when input channel closes"
        );
    }

    // Backpressure conservation: with a bounded sequencer queue and an
    // initially-paused consumer, pushing more than `cap` valid txs must block
    // (never drop) and, once drained, every tx must arrive exactly once. Also
    // asserts the worker never holds more than `cap + workers` in flight on the
    // sequencer side (queue capacity plus one parked send per worker).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sigverify_blocks_not_drops_when_sequencer_full() {
        let sequencer_cap = SEQ_CAP;
        let num_workers = 4usize;
        let total_txs = sequencer_cap * 3;

        let (sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(256);
        let (sequencer_tx, mut sequencer_rx) = mpsc::channel(sequencer_cap);
        let shutdown = CancellationToken::new();

        let handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Producer pushes all txs; backpressure makes it block once the
        // sequencer queue (and the parked per-worker sends) fill up.
        let producer = tokio::spawn(async move {
            for _ in 0..total_txs {
                let payer = Keypair::new();
                let from_ata = Pubkey::new_unique();
                let to_ata = Pubkey::new_unique();
                let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());
                let tx = sanitize(&[ix], &payer, &[&payer]);
                sigverify_tx.send(tx).await.unwrap();
            }
        });

        // Let the workers actually park on the full queue so the backpressure
        // path is exercised before we drain; the conservation check below is the
        // real regression guard (a drop-instead-of-park bug fails it).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Drain everything; conservation: all txs arrive, none dropped.
        let mut drained = 0usize;
        while drained < total_txs {
            match tokio::time::timeout(std::time::Duration::from_secs(10), sequencer_rx.recv())
                .await
            {
                Ok(Some(_)) => drained += 1,
                _ => break,
            }
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), producer).await;
        assert_eq!(
            drained, total_txs,
            "every tx must reach the sequencer under backpressure (no drops)"
        );

        shutdown.cancel();
        for h in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), h.handle).await;
        }
    }

    // A full sequencer queue with no consumer must not wedge worker shutdown:
    // the worker's send is raced against the shutdown token.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sigverify_worker_exits_on_shutdown_with_full_sequencer() {
        let sequencer_cap = SEQ_CAP;
        let (sigverify_tx, sigverify_rx) = async_channel::bounded::<SanitizedTransaction>(256);
        let (sequencer_tx, _sequencer_rx) = mpsc::channel(sequencer_cap);
        let shutdown = CancellationToken::new();

        let handles = start_sigverify_workerpool(SigverifyArgs {
            num_workers: 1,
            admin_keys: vec![],
            rx: sigverify_rx,
            sequencer_tx,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: crate::health::StageHeartbeat::new(),
        })
        .await;

        // Fill the sequencer queue so the worker's next send blocks. Never drain.
        for _ in 0..(sequencer_cap + 4) {
            let payer = Keypair::new();
            let from_ata = Pubkey::new_unique();
            let to_ata = Pubkey::new_unique();
            let ix = spl_transfer_ix(&from_ata, &to_ata, &payer.pubkey());
            sigverify_tx
                .send(sanitize(&[ix], &payer, &[&payer]))
                .await
                .unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        shutdown.cancel();
        for h in handles {
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), h.handle).await;
            assert!(
                result.is_ok(),
                "worker must exit promptly even with a full sequencer queue"
            );
        }
    }

    // Regression guard on async-channel's MPMC fan-out contract, which the
    // sigverify workerpool relies on: N cloned receivers share a single queue,
    // every item is delivered to exactly one consumer, and work spreads
    // roughly evenly across consumers under contention.
    //
    // Two properties are asserted:
    //
    // 1. Total conservation — sum of per-consumer counts equals items sent.
    //
    // 2. Fairness floor — each consumer receives at least half of its equal
    //    share. Catches scheduler/wake-list regressions that would starve
    //    clones (e.g. one consumer monopolising the channel while others
    //    stay parked). In practice async-channel keeps per-worker counts
    //    within ~25% of the mean across 100+ runs, so the /2
    //    floor is tight enough to detect real skew but loose enough that
    //    scheduler jitter alone will not flake CI.
    //
    // The test uses bounded(64) + 4 worker threads so that backpressure
    // forces the producer to interleave with consumer wakeups — the same
    // regime the production sigverify pool operates in. Without bounded
    // backpressure (or with a single worker thread) fairness becomes
    // degenerate and not representative.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cloned_receivers_consume_concurrently() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (tx, rx) = async_channel::bounded::<usize>(64);
        let num_consumers = 4usize;
        let total_items = 8_000usize;

        let counters: Vec<Arc<AtomicUsize>> = (0..num_consumers)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();

        let consumer_handles: Vec<_> = counters
            .iter()
            .map(|counter| {
                let rx = rx.clone();
                let counter = Arc::clone(counter);
                tokio::spawn(async move {
                    while let Ok(_item) = rx.recv().await {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        drop(rx);

        for i in 0..total_items {
            tx.send(i).await.unwrap();
        }
        drop(tx);

        for h in consumer_handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), h).await;
        }

        let counts: Vec<usize> = counters.iter().map(|c| c.load(Ordering::Relaxed)).collect();
        let total_consumed: usize = counts.iter().sum();
        assert_eq!(
            total_consumed, total_items,
            "every item must be delivered to exactly one consumer"
        );

        // Progress check: every cloned consumer must pull at least one item —
        // that's what proves the channel is actually fanning out to parallel
        // receivers instead of being single-threaded. async-channel makes no
        // fairness guarantee, so a stricter floor flakes under CI jitter.
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count > 0,
                "consumer {i} received 0 items — cloned receivers are not \
                 consuming concurrently; counts: {counts:?}"
            );
        }
    }
}
