//! Startup recovery escalation for every parse-error shape.
//!
//! Covers `recover_pending_remints` (`indexer/src/operator/sender/state.rs`)
//! — every malformed row must be escalated to `ManualReview` via
//! `send_recovery_manual_review`/`or_manual_review` without blocking
//! recovery of valid siblings. `remint_recovery` already covers the
//! bad-mint shape; this test extends the coverage matrix to the other
//! parse-error shapes the recovery loop guards against:
//!
//!   - invalid `mint` pubkey string (redundant with t12, kept for
//!     regression anchoring)
//!   - invalid `recipient` pubkey string
//!   - invalid withdrawal `remint_signatures[i]` string
//!   - negative `amount` (exercises the `u64::try_from` guard)
//!
//! Plus one valid row in the same batch — it must still be recovered
//! and enqueued into `state.pending_remints`, proving the loop
//! continues past each escalation.

use {
    chrono::{DateTime, Utc},
    private_channel_indexer::{
        config::{PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator::sender::{test_hooks, TransactionStatusUpdate},
        storage::{
            common::{
                models::{DbTransaction, TransactionStatus, TransactionType},
                storage::mock::MockStorage,
            },
            Storage,
        },
    },
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    std::sync::Arc,
    tokio::sync::mpsc,
};

fn make_row(
    id: i64,
    mint: String,
    recipient: String,
    sig_strings: Vec<String>,
    amount: i64,
    deadline: DateTime<Utc>,
) -> DbTransaction {
    let now = Utc::now();
    let lvbhs = vec![0; sig_strings.len()];
    DbTransaction {
        id,
        signature: Signature::new_unique().to_string(),
        trace_id: format!("trace-{id}"),
        slot: 100,
        initiator: Pubkey::new_unique().to_string(),
        recipient,
        mint,
        amount,
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(id),
        status: TransactionStatus::PendingRemint,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: Some(sig_strings),
        remint_last_valid_block_heights: Some(lvbhs),
        pending_remint_deadline_at: Some(deadline),
        finality_check_attempts: 0,
        recovery_requeue_attempts: 0,
    }
}

fn make_config() -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        postgres: PostgresConfig {
            database_url: "postgres://placeholder/none".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None,
    }
}

/// Seed four malformed rows (one per parse-error shape) plus one
/// valid row. Assert:
///   - exactly 4 ManualReview escalations emitted on the channel
///   - each escalation carries the corresponding transaction_id
///   - the lone valid row is rehydrated into state.pending_remints
#[tokio::test]
async fn recover_escalates_every_parse_error_shape_and_preserves_valid_sibling() {
    let mock = MockStorage::new();

    let deadline = Utc::now() + chrono::Duration::seconds(20);
    let good_mint = Pubkey::new_unique();
    let good_recipient = Pubkey::new_unique();
    let good_sig = Signature::new_unique();

    // Row 10 — invalid mint string
    let bad_mint = make_row(
        10,
        "definitely-not-base58".to_string(),
        Pubkey::new_unique().to_string(),
        vec![Signature::new_unique().to_string()],
        1_000,
        deadline,
    );

    // Row 11 — invalid recipient string
    let bad_recipient = make_row(
        11,
        Pubkey::new_unique().to_string(),
        "not-a-valid-pubkey-either".to_string(),
        vec![Signature::new_unique().to_string()],
        2_000,
        deadline,
    );

    // Row 12 — invalid signature in remint_signatures
    let bad_sig = make_row(
        12,
        Pubkey::new_unique().to_string(),
        Pubkey::new_unique().to_string(),
        vec!["this-is-not-a-base58-signature".to_string()],
        3_000,
        deadline,
    );

    // Row 13 — negative amount (u64::try_from must reject)
    let negative_amount = make_row(
        13,
        Pubkey::new_unique().to_string(),
        Pubkey::new_unique().to_string(),
        vec![Signature::new_unique().to_string()],
        -42,
        deadline,
    );

    // Row 14 — valid, must still be recovered
    let good = make_row(
        14,
        good_mint.to_string(),
        good_recipient.to_string(),
        vec![good_sig.to_string()],
        4_000,
        deadline,
    );

    mock.pending_remint_transactions.lock().unwrap().extend([
        bad_mint,
        bad_recipient,
        bad_sig,
        negative_amount,
        good,
    ]);

    let storage = Arc::new(Storage::Mock(mock));
    let mut state = test_hooks::new_sender_state(
        &make_config(),
        CommitmentLevel::Confirmed,
        None,
        storage,
        3,
        400,
        None,
    )
    .expect("constructing SenderState with Mock storage must succeed");

    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(32);

    test_hooks::recover_pending_remints(&mut state, &storage_tx)
        .await
        .expect("recovery over partially-bad batch must still return Ok");

    // The valid row is recovered, the four malformed siblings are not.
    assert_eq!(
        state.pending_remints.len(),
        1,
        "exactly one row (the valid sibling) must be rehydrated"
    );
    let entry = &state.pending_remints[0];
    assert_eq!(entry.ctx.transaction_id, Some(14));
    assert_eq!(entry.remint_info.mint, good_mint);
    assert_eq!(entry.remint_info.user, good_recipient);
    assert_eq!(entry.signatures.len(), 1);
    assert_eq!(entry.signatures[0].signature, good_sig);

    // All four bad rows must have emitted ManualReview updates.
    let mut escalated_ids: Vec<i64> = Vec::new();
    while let Ok(update) = storage_rx.try_recv() {
        assert_eq!(update.status, TransactionStatus::ManualReview);
        escalated_ids.push(update.transaction_id);
    }
    escalated_ids.sort();
    assert_eq!(
        escalated_ids,
        vec![10, 11, 12, 13],
        "every malformed row must escalate exactly once; got {:?}",
        escalated_ids
    );
}
