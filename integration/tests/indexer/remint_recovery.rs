//! Pending-remint recovery across operator restart.
//!
//! Verifies the integration-level contract of
//! `SenderState::recover_pending_remints`: on startup, every PendingRemint
//! row in the database must be fully reconstructed into the in-memory
//! `pending_remints` queue so the finality-check tick can pick up where
//! the crashed operator left off. Critically:
//!
//!   - valid rows are silently re-queued (no ManualReview side effects)
//!   - the *original* deadline is restored (not a fresh window — the
//!     clock keeps ticking across restarts, otherwise deferred remints
//!     could fire indefinitely early after a restart loop)
//!   - `finality_check_attempts` always resets to 0 (in-memory only)
//!   - a malformed row escalates to ManualReview instead of silently
//!     skipping (skipping would trap the row in PendingRemint forever)
//!   - other valid rows on the same startup must still be recovered
//!     even when one row escalates
//!
//! Unlike the in-crate unit tests (`indexer/src/operator/sender/state.rs`
//! `recover_pending_remints_*`), this test exercises the recovery path
//! through the exported test hook (`private_channel_indexer::operator::sender::test_hooks`)
//! so the integration crate boundary stays honest — the same public API
//! a future external tool would use to drive recovery.

use {
    chrono::{DateTime, Utc},
    private_channel_indexer::{
        config::{PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType},
        operator::sender::{test_hooks, TransactionStatusUpdate},
        storage::{
            common::{
                amount::TokenAmount,
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
    mint: &Pubkey,
    recipient: &Pubkey,
    sig: &Signature,
    amount: u64,
    deadline: DateTime<Utc>,
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
        amount: TokenAmount(amount),
        memo: None,
        transaction_type: TransactionType::Withdrawal,
        withdrawal_nonce: Some(id),
        status: TransactionStatus::PendingRemint,
        created_at: now,
        updated_at: now,
        processed_at: None,
        counterpart_signature: None,
        remint_signatures: Some(vec![sig.to_string()]),
        remint_last_valid_block_heights: Some(vec![0]),
        pending_remint_deadline_at: Some(deadline),
        finality_check_attempts: 0,
        recovery_requeue_attempts: 0,
        instruction_index: 0,
        inner_index: None,
        landed_remint_signature: None,
    }
}

fn make_config() -> PrivateChannelIndexerConfig {
    PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        // RpcClientWithRetry is only constructed here — we never make an
        // actual RPC call in these tests, so a non-routable URL is fine.
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        postgres: PostgresConfig {
            database_url: "postgres://placeholder/none".to_string(),
            max_connections: 1,
        },
        escrow_instance_id: None,
    }
}

/// Happy path: a single valid PendingRemint row is fully re-queued with
/// all identity + timing fields restored.
#[tokio::test]
async fn recover_rehydrates_valid_pending_remint_row() {
    let mock = MockStorage::new();

    let mint = Pubkey::new_unique();
    let recipient = Pubkey::new_unique();
    let sig = Signature::new_unique();
    let deadline = Utc::now() + chrono::Duration::seconds(20);

    mock.pending_remint_transactions
        .lock()
        .unwrap()
        .push(make_row(42, &mint, &recipient, &sig, 5_000, deadline));

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
    .expect("constructing SenderState with Mock storage should succeed");

    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(16);

    test_hooks::recover_pending_remints(&mut state, &storage_tx)
        .await
        .expect("recovery over valid rows must succeed");

    assert_eq!(state.pending_remints.len(), 1, "exactly one row queued");
    let entry = &state.pending_remints[0];

    assert_eq!(entry.ctx.transaction_id, Some(42));
    assert_eq!(entry.ctx.trace_id.as_deref(), Some("trace-42"));
    assert_eq!(entry.remint_info.mint, mint);
    assert_eq!(entry.remint_info.user, recipient);
    assert_eq!(entry.remint_info.amount, 5_000u64);
    assert_eq!(entry.signatures.len(), 1);
    assert_eq!(entry.signatures[0].signature, sig);
    assert_eq!(entry.finality_check_attempts, 0);
    assert_eq!(entry.original_error, "recovered from persistent storage");

    // Deadline preserved across "restart" — must not reset to now() + 32s.
    let skew_ms = (entry.deadline - deadline).num_milliseconds().abs();
    assert!(
        skew_ms < 1_000,
        "deadline should be restored from DB, not regenerated; got skew {skew_ms}ms"
    );

    // No ManualReview alerts should fire for a clean row.
    assert!(
        storage_rx.try_recv().is_err(),
        "no status update expected for a valid recovery row"
    );
}

/// Empty DB path: no pending remints means no queue entries AND no
/// channel messages AND a clean Ok(). This guards against an off-by-one
/// in the empty-queue early return.
#[tokio::test]
async fn recover_is_noop_when_no_pending_rows() {
    let mock = MockStorage::new();
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
    .expect("constructing SenderState with Mock storage should succeed");

    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(16);

    test_hooks::recover_pending_remints(&mut state, &storage_tx)
        .await
        .expect("recovery over empty DB must be a no-op");

    assert_eq!(state.pending_remints.len(), 0);
    assert!(storage_rx.try_recv().is_err());
}

/// A corrupt mint string in one row must escalate that row to
/// ManualReview without blocking recovery of the other valid rows on
/// the same startup.
#[tokio::test]
async fn recover_escalates_malformed_row_but_continues_with_others() {
    let mock = MockStorage::new();

    let recipient = Pubkey::new_unique();
    let sig = Signature::new_unique();
    let deadline = Utc::now() + chrono::Duration::seconds(20);

    // Row 1 — invalid mint string, cannot be parsed back to a Pubkey.
    let mut bad = make_row(10, &Pubkey::new_unique(), &recipient, &sig, 1_000, deadline);
    bad.mint = "definitely-not-base58".to_string();

    // Row 2 — valid, must still be queued despite the bad sibling.
    let good_mint = Pubkey::new_unique();
    let good = make_row(11, &good_mint, &recipient, &sig, 2_500, deadline);

    mock.pending_remint_transactions
        .lock()
        .unwrap()
        .extend([bad, good]);

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
    .expect("constructing SenderState with Mock storage should succeed");

    let (storage_tx, mut storage_rx) = mpsc::channel::<TransactionStatusUpdate>(16);

    test_hooks::recover_pending_remints(&mut state, &storage_tx)
        .await
        .expect("recovery of a partially-bad startup must still return Ok");

    // Valid row queued, bad row skipped — one entry in the queue.
    assert_eq!(state.pending_remints.len(), 1);
    assert_eq!(state.pending_remints[0].ctx.transaction_id, Some(11));
    assert_eq!(state.pending_remints[0].remint_info.mint, good_mint);

    // The bad row MUST have emitted a ManualReview update so operators
    // get paged rather than the row silently rotting in PendingRemint.
    let update = storage_rx
        .try_recv()
        .expect("bad row must emit a ManualReview status update");
    assert_eq!(update.transaction_id, 10);
    assert_eq!(update.status, TransactionStatus::ManualReview);

    // No further messages — the valid row is silent.
    assert!(
        storage_rx.try_recv().is_err(),
        "only one escalation should fire for the one bad row"
    );
}
