#![allow(dead_code)]

use super::db;
use super::test_types::WAIT_TIMEOUT_SECS;

/// Wait until at least `expected_count` transactions reach `completed` status.
///
/// Polls every 200 ms and gives up after `WAIT_TIMEOUT_SECS`.
/// Exits early if the combined completed + failed count already equals
/// `expected_count` — in that case not all transactions succeeded, so the
/// assertion below will fail with a descriptive message showing the breakdown.
pub async fn wait_for_operator_completion(
    pool: &sqlx::PgPool,
    expected_count: usize,
    operation_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let mut last_logged = 0u64;
    let mut ready = false;

    while start.elapsed().as_secs() < *WAIT_TIMEOUT_SECS {
        let completed = db::count_transactions_by_status(pool, "completed").await?;
        let failed = db::count_transactions_by_status(pool, "failed").await?;

        if completed >= expected_count as i64 {
            println!(
                "✓ Reached target: {}/{} transactions completed",
                completed, expected_count
            );
            ready = true;
            break;
        }

        // If every terminal-state transaction has been decided and none are still
        // pending, there is no point waiting for the full timeout.
        if failed > 0 && completed + failed >= expected_count as i64 {
            println!(
                "✗ All transactions terminal but only {}/{} completed ({} failed)",
                completed, expected_count, failed
            );
            break;
        }

        // Log progress every 5 seconds.
        let elapsed = start.elapsed().as_secs();
        if elapsed - last_logged >= 5 {
            println!(
                "  Progress: {}/{} completed ({:.1}%), {} failed — elapsed: {}s",
                completed,
                expected_count,
                (completed as f64 / expected_count as f64) * 100.0,
                failed,
                elapsed
            );
            last_logged = elapsed;
        }

        // Issue 7 fix: poll every 200 ms instead of 1 s.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    if !ready {
        let completed = db::count_transactions_by_status(pool, "completed").await?;
        let failed = db::count_transactions_by_status(pool, "failed").await?;
        let pending = db::count_transactions_by_status(pool, "pending").await?;
        println!(
            "✗ Timeout after {}s: completed={}, failed={}, pending={} (expected {} completed)",
            *WAIT_TIMEOUT_SECS, completed, failed, pending, expected_count
        );
    }

    assert!(
        ready,
        "Operator did not process all {} within timeout \
         (check output above for completed/failed/pending breakdown)",
        operation_name
    );

    Ok(())
}

/// Wait until a specific transaction reaches `completed` status.
///
/// Fails immediately (rather than timing out) if the transaction is already
/// in `failed` status, which avoids burning the whole budget when the operator
/// has already decided the transaction is unprocessable.
///
/// The budget is `WAIT_TIMEOUT_SECS` and is deliberately not a parameter. A
/// caller-chosen number is tuned against whatever build the author ran, and a
/// coverage-instrumented run is several times slower, so those numbers turn
/// into CI flakes that look like hangs.
pub async fn wait_for_transaction_completion(
    pool: &sqlx::PgPool,
    signature: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout_secs = *WAIT_TIMEOUT_SECS;
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < timeout_secs {
        if let Some(tx) = db::get_transaction(pool, signature).await? {
            match tx.status.as_str() {
                "completed" => return Ok(()),
                "failed" => {
                    return Err(format!(
                        "Transaction {} reached 'failed' status (not 'completed')",
                        signature
                    )
                    .into())
                }
                _ => {}
            }
        }
        // Issue 7 fix: poll every 200 ms instead of 500 ms.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    Err(format!(
        "Transaction {} did not complete within {}s",
        signature, timeout_secs
    )
    .into())
}
