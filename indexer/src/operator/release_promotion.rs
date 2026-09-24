//! Marks released withdrawals `Completed` once a journaled release signature is finalized.
//!
//! The sender never terminalizes a release at confirmed, since a fork can still drop it.
//! The row and its journal are the durable record, so this pass holds no state of its own.

use crate::config::ProgramType;
use crate::error::OperatorError;
use crate::metrics::OPERATOR_DB_UPDATES;
use crate::operator::sender::fetch_statuses_checked;
use crate::operator::utils::rpc_util::RpcClientWithRetry;
use crate::storage::common::models::TransactionStatus;
use crate::storage::common::storage::Storage;
use private_channel_metrics::MetricLabel;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// How often released rows are checked for finality.
pub(crate) const PROMOTION_INTERVAL: Duration = Duration::from_secs(1);

/// Rows per pass, one status call's worth.
pub(crate) const PROMOTION_BATCH_LIMIT: i64 = 256;

/// Run until cancelled. Its own task, so a sender blocked on a send or a halted
/// pipeline never delays promotion.
pub async fn run_release_promotion(
    storage: Arc<Storage>,
    rpc_client: Arc<RpcClientWithRetry>,
    cancellation_token: CancellationToken,
) {
    info!("Release promotion started");
    let mut interval = tokio::time::interval(PROMOTION_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut cursor = 0;
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => break,
            _ = interval.tick() => {}
        }
        if let Err(e) =
            promote_once(&storage, &rpc_client, &mut cursor, PROMOTION_BATCH_LIMIT).await
        {
            warn!("Release promotion pass failed, retrying next tick: {e}");
        }
    }
    info!("Release promotion stopped");
}

/// One page of released rows. Completes each row with a finalized, successful
/// signature through the same `updated_at` CAS recovery uses, so any concurrent
/// writer wins or loses cleanly. An error keeps the cursor for the next tick.
async fn promote_once(
    storage: &Storage,
    rpc_client: &RpcClientWithRetry,
    cursor: &mut i64,
    limit: i64,
) -> Result<(), OperatorError> {
    let rows = storage.get_released_withdrawals(*cursor, limit).await?;

    // Every row's attempts go in one positional lookup, so remember whose each one is.
    let mut owners = Vec::new();
    let mut signatures = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        for raw in &row.signatures {
            match Signature::from_str(raw) {
                Ok(signature) => {
                    owners.push(index);
                    signatures.push(signature);
                }
                Err(e) => warn!(id = row.id, "Unparseable journaled signature {raw}: {e}"),
            }
        }
    }

    // History, so an attempt that has left the status cache is still found.
    let statuses = fetch_statuses_checked(rpc_client, &signatures, true)
        .await
        .map_err(|e| OperatorError::RpcError(format!("release status lookup failed: {e:?}")))?;

    let mut promoted = vec![false; rows.len()];
    for ((owner, signature), status) in owners.iter().zip(&signatures).zip(statuses) {
        let Some(status) = status else { continue };
        let landed =
            status.satisfies_commitment(CommitmentConfig::finalized()) && status.err.is_none();
        if !landed || promoted[*owner] {
            continue;
        }
        let row = &rows[*owner];
        if storage
            .try_complete_processing(row.id, row.updated_at, Some(signature.to_string()), None)
            .await?
        {
            info!(id = row.id, %signature, "Release finalized; withdrawal Completed");
            OPERATOR_DB_UPDATES
                .with_label_values(&[
                    ProgramType::Withdraw.as_label(),
                    &format!("{:?}", TransactionStatus::Completed),
                ])
                .inc();
        }
        promoted[*owner] = true;
    }

    // A short page means the end of the table, so the next pass starts over.
    *cursor = if (rows.len() as i64) < limit {
        0
    } else {
        rows.last().map_or(0, |row| row.id)
    };
    Ok(())
}

#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_hooks {
    //! Test-only entry to drive a single promotion pass.
    use super::*;

    pub async fn promote_once(
        storage: &Storage,
        rpc_client: &RpcClientWithRetry,
        cursor: &mut i64,
        limit: i64,
    ) -> Result<(), OperatorError> {
        super::promote_once(storage, rpc_client, cursor, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::sender::test_support::{push_processing_row, row_status};
    use crate::operator::utils::rpc_util::RetryConfig;
    use crate::storage::common::storage::mock::MockStorage;

    const CONFIRMED_OK: &str = r#"{"slot":100,"confirmations":5,"err":null,"status":{"Ok":null},"confirmationStatus":"confirmed"}"#;
    const FINALIZED_OK: &str = r#"{"slot":100,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"}"#;
    const FINALIZED_ERR: &str = r#"{"slot":100,"confirmations":null,"err":{"InstructionError":[0,{"Custom":12}]},"status":{"Err":{"InstructionError":[0,{"Custom":12}]}},"confirmationStatus":"finalized"}"#;
    const NULL: &str = "null";

    fn rpc(url: &str) -> RpcClientWithRetry {
        RpcClientWithRetry::with_retry_config(
            url.to_string(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
            CommitmentConfig::confirmed(),
        )
    }

    /// Answer every history lookup with `entries`, in request order.
    fn mock_statuses(server: &mut mockito::ServerGuard, entries: &[&str]) -> mockito::Mock {
        let body = format!(
            r#"{{"jsonrpc":"2.0","result":{{"context":{{"slot":200}},"value":[{}]}},"id":0}}"#,
            entries.join(",")
        );
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::AllOf(vec![
                mockito::Matcher::Regex(r#""method"\s*:\s*"getSignatureStatuses""#.into()),
                mockito::Matcher::Regex(r#""searchTransactionHistory"\s*:\s*true"#.into()),
            ]))
            .with_status(200)
            .with_body(body)
            .create()
    }

    /// A processing withdrawal with `n` journaled attempts, returned oldest first.
    async fn released_row(mock: &MockStorage, id: i64, n: usize) -> Vec<Signature> {
        push_processing_row(mock, id);
        let storage = Storage::Mock(mock.clone());
        let mut sigs = Vec::new();
        for _ in 0..n {
            let sig = Signature::new_unique();
            storage
                .insert_release_signature(id, sig.to_string(), 1, None)
                .await
                .unwrap();
            sigs.push(sig);
        }
        sigs
    }

    fn counterpart(mock: &MockStorage, id: i64) -> Option<String> {
        mock.pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == id)
            .and_then(|t| t.counterpart_signature.clone())
    }

    /// Statuses are positional, so the finalized success must be matched to its own
    /// signature, and every other status must be left alone.
    #[tokio::test]
    async fn promote_routes_each_status() {
        let mut server = mockito::Server::new_async().await;
        let _m = mock_statuses(
            &mut server,
            &[CONFIRMED_OK, NULL, FINALIZED_ERR, FINALIZED_OK],
        );
        let mock = MockStorage::new();
        let sigs = released_row(&mock, 7, 4).await;

        promote_once(
            &Storage::Mock(mock.clone()),
            &rpc(&server.url()),
            &mut 0,
            256,
        )
        .await
        .unwrap();

        assert_eq!(row_status(&mock, 7), Some(TransactionStatus::Completed));
        assert_eq!(counterpart(&mock, 7), Some(sigs[3].to_string()));
    }

    /// Confirmed can still be forked out, null is unknown and an error never paid, so
    /// none of them may complete the row.
    #[tokio::test]
    async fn promote_leaves_row_without_finalized_success() {
        for (label, status) in [
            ("confirmed", CONFIRMED_OK),
            ("null", NULL),
            ("finalized error", FINALIZED_ERR),
        ] {
            let mut server = mockito::Server::new_async().await;
            let _m = mock_statuses(&mut server, &[status, status]);
            let mock = MockStorage::new();
            released_row(&mock, 7, 2).await;

            promote_once(
                &Storage::Mock(mock.clone()),
                &rpc(&server.url()),
                &mut 0,
                256,
            )
            .await
            .unwrap();

            assert_eq!(
                row_status(&mock, 7),
                Some(TransactionStatus::Processing),
                "{label}"
            );
        }
    }

    /// A failed read writes nothing and keeps the cursor, and the next pass still promotes.
    #[tokio::test]
    async fn promote_survives_rpc_and_db_errors() {
        let mut server = mockito::Server::new_async().await;
        let down = server
            .mock("POST", "/")
            .with_status(500)
            .with_body("down")
            .create();
        let mock = MockStorage::new();
        released_row(&mock, 7, 1).await;
        let storage = Storage::Mock(mock.clone());
        let mut cursor = 3;

        assert!(
            promote_once(&storage, &rpc(&server.url()), &mut cursor, 256)
                .await
                .is_err()
        );
        assert_eq!(cursor, 3);
        assert_eq!(row_status(&mock, 7), Some(TransactionStatus::Processing));
        down.remove();

        mock.set_should_fail("get_released_withdrawals", true);
        let _ok = mock_statuses(&mut server, &[FINALIZED_OK]);
        assert!(
            promote_once(&storage, &rpc(&server.url()), &mut cursor, 256)
                .await
                .is_err()
        );
        assert_eq!(cursor, 3);
        mock.set_should_fail("get_released_withdrawals", false);

        let mut cursor = 0;
        promote_once(&storage, &rpc(&server.url()), &mut cursor, 256)
            .await
            .unwrap();
        assert_eq!(row_status(&mock, 7), Some(TransactionStatus::Completed));
    }

    /// Rows that never finalize must not starve the ones behind them: the cursor walks
    /// forward a page at a time and wraps on a short page.
    #[tokio::test]
    async fn promote_cursor_wraps() {
        let mut server = mockito::Server::new_async().await;
        let lookups = mock_statuses(&mut server, &[NULL]).expect(3);
        let mock = MockStorage::new();
        for id in [1, 2, 3] {
            released_row(&mock, id, 1).await;
        }
        let storage = Storage::Mock(mock.clone());
        let rpc = rpc(&server.url());

        let mut cursor = 0;
        let mut visited = Vec::new();
        for _ in 0..4 {
            promote_once(&storage, &rpc, &mut cursor, 1).await.unwrap();
            visited.push(cursor);
        }

        assert_eq!(visited, vec![1, 2, 3, 0]);
        lookups.assert();
    }

    /// Lock loss and shutdown cancel the token, and the task must stop within a tick.
    #[tokio::test]
    async fn promote_stops_on_cancellation() {
        let token = CancellationToken::new();
        let task = tokio::spawn(run_release_promotion(
            Arc::new(Storage::Mock(MockStorage::new())),
            Arc::new(rpc("http://127.0.0.1:1")),
            token.clone(),
        ));
        token.cancel();

        tokio::time::timeout(PROMOTION_INTERVAL * 2, task)
            .await
            .expect("the task must stop within a tick")
            .unwrap();
    }
}
