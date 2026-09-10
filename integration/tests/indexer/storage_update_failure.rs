//! Storage update failure handling in `DbTransactionWriter`.
//!
//! Covers the storage-error arm inside `DbTransactionWriter::run`
//! (`indexer/src/operator/db_transaction_writer.rs`) — when
//! `Storage::update_transaction_status` returns an error, the
//! `DbTransactionWriter` must log it, increment the
//! `OPERATOR_DB_UPDATE_ERRORS` metric, and keep looping on subsequent
//! updates rather than panicking or exiting.
//!
//! Strategy: construct a `DbTransactionWriter` directly with a
//! `Storage::Mock` handle, toggle `set_should_fail("update_transaction_status", true)`,
//! push a status update through the channel, assert it was consumed
//! and no panic occurred. Clear the flag, push a second update, and
//! assert it succeeds — the status update is recorded in
//! `MockStorage::status_updates`. This proves the loop "keeps
//! looping" contract.

use {
    chrono::Utc,
    private_channel_indexer::{
        config::ProgramType,
        operator::{sender::TransactionStatusUpdate, DbTransactionWriter},
        storage::{common::models::TransactionStatus, common::storage::mock::MockStorage, Storage},
    },
    std::{sync::Arc, time::Duration},
    tokio::sync::mpsc,
};

fn make_update(id: i64, status: TransactionStatus) -> TransactionStatusUpdate {
    TransactionStatusUpdate {
        transaction_id: id,
        trace_id: Some(format!("trace-{id}")),
        status,
        counterpart_signature: Some("sig".to_string()),
        error_message: None,
        processed_at: Some(Utc::now()),
        remint_signature: None,
        remint_attempted: false,
    }
}

/// Scripted failure on `update_transaction_status` must not kill the
/// writer loop. After clearing the flag, subsequent updates must
/// succeed — proving the writer's error branch logs and continues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_logs_and_continues_after_storage_update_failure() {
    let mock_storage = MockStorage::new();
    let storage: Arc<Storage> = Arc::new(Storage::Mock(mock_storage.clone()));

    let (tx, rx) = mpsc::channel::<TransactionStatusUpdate>(16);
    let writer = DbTransactionWriter::new(
        storage.clone(),
        rx,
        None, // no webhook
        ProgramType::Escrow,
    );

    // Kick off the writer task.
    let writer_handle = tokio::spawn(async move { writer.start().await });

    // 1) Script storage failure and push one update.
    mock_storage.set_should_fail("update_transaction_status", true);
    tx.send(make_update(101, TransactionStatus::Failed))
        .await
        .expect("channel must accept first update");

    // 2) Give the writer a moment to consume the update and hit the
    //    error path. We then clear the flag and push a second update.
    //    If the writer exited on the first error, this second send
    //    would either succeed-into-closed-channel (tokio) but the
    //    writer task would be done.
    tokio::time::sleep(Duration::from_millis(100)).await;

    mock_storage.set_should_fail("update_transaction_status", false);
    tx.send(make_update(102, TransactionStatus::Completed))
        .await
        .expect("channel must still accept second update — writer loop must not have exited");

    // 3) Poll the mock storage for the second update landing. The
    //    first update's write was swallowed (by the scripted failure),
    //    so only the second one should appear in `status_updates`.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let recorded = loop {
        let updates = mock_storage.status_updates.lock().unwrap().clone();
        if !updates.is_empty() {
            break updates;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "writer did not persist the second update within 5s — loop may have exited on error"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Exactly one recorded status update — the second one.  The first
    // was intentionally dropped by the scripted failure.
    assert_eq!(
        recorded.len(),
        1,
        "expected only the post-recovery update to land in storage, got {:?}",
        recorded
    );
    let (recorded_id, recorded_status, _sig, _ts) = &recorded[0];
    assert_eq!(*recorded_id, 102);
    assert_eq!(*recorded_status, TransactionStatus::Completed);

    // Close the channel so the writer exits gracefully.
    drop(tx);
    let result = tokio::time::timeout(Duration::from_secs(5), writer_handle)
        .await
        .expect("writer must terminate after channel close");
    assert!(
        result.is_ok(),
        "writer task must not panic after storage failure; got {:?}",
        result
    );
}
