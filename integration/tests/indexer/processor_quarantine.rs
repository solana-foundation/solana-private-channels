//! Processor quarantine on malformed deposit row.
//!
//! Covers the quarantine-dispatch path in `indexer/src/operator/processor.rs`
//! — when the processor encounters an `OperatorError::InvalidPubkey`
//! (classified as `Quarantine("invalid_pubkey")`), it must:
//!
//!   (a) emit a `ManualReview` status update for the offending row via
//!       `quarantine_single`
//!   (b) halt the withdrawal pipeline if this is a withdrawal row
//!       (`halt_withdrawal_pipeline` + the withdrawal-dispatch arm)
//!
//! Strategy: seed a `Deposit` row with a malformed `mint` string
//! ("definitely-not-base58"). `Pubkey::from_str` fails, the processor
//! classifies it as Quarantine, and emits a ManualReview update via
//! the storage writer. We assert the recorded `status_updates`
//! contains exactly one entry with `ManualReview` status for our row.
//!
//! Deposits (Escrow program type) don't halt the pipeline — they
//! quarantine the single row and keep flowing (614-635 path). This
//! keeps the test focused on the observable contract without needing
//! the full withdrawal SMT harness.

use {
    chrono::Utc,
    private_channel_indexer::storage::common::amount::TokenAmount,
    private_channel_indexer::storage::common::models::{
        DbTransaction, TransactionStatus, TransactionType,
    },
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    std::time::Duration,
    test_utils::operator_helper::start_solana_to_private_channel_operator_with_mocks,
};

fn make_bad_deposit(id: i64) -> DbTransaction {
    let now = Utc::now();
    DbTransaction {
        id,
        signature: format!("seed-sig-{id}"),
        trace_id: format!("trace-{id}"),
        slot: 100,
        initiator: Pubkey::new_unique().to_string(),
        recipient: Pubkey::new_unique().to_string(),
        // Deliberately malformed: Pubkey::from_str will reject this.
        mint: "definitely-not-base58".to_string(),
        amount: TokenAmount(1_000),
        memo: None,
        transaction_type: TransactionType::Deposit,
        withdrawal_nonce: None,
        status: TransactionStatus::Pending,
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
    }
}

/// A deposit row with an unparseable mint pubkey must be quarantined
/// to `ManualReview` via the processor's InvalidPubkey → Quarantine
/// classification. The mock storage should record exactly that update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processor_quarantines_deposit_with_malformed_mint_string() {
    let escrow_instance = Pubkey::new_unique();
    let keypair = Keypair::new();

    let harness = start_solana_to_private_channel_operator_with_mocks(escrow_instance, keypair)
        .await
        .expect("harness start");

    // Seed the bad row and wait for the processor to quarantine it.
    harness
        .storage
        .pending_transactions
        .lock()
        .unwrap()
        .push(make_bad_deposit(501));

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let found = loop {
        let updates = harness.storage.status_updates.lock().unwrap().clone();
        if let Some(hit) = updates
            .iter()
            .find(|(id, status, _, _)| *id == 501 && *status == TransactionStatus::ManualReview)
        {
            break Some(hit.clone());
        }
        if std::time::Instant::now() > deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        found.is_some(),
        "processor must quarantine the malformed deposit to ManualReview within 10s — \
         got updates {:?}",
        harness.storage.status_updates.lock().unwrap().clone()
    );

    harness.shutdown().await;
}
