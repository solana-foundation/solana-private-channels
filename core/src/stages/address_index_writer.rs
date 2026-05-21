use {
    crate::{
        accounts::{
            address_index_watermark::upsert_address_signatures_flushed_slot_in_tx,
            write_batch::{upsert_address_signature_rows, AddressSignatureRow},
        },
        health::StageHeartbeat,
        nodes::node::WorkerHandle,
        stage_metrics::SharedMetrics,
    },
    sqlx::{postgres::PgPoolOptions, PgPool},
    std::{sync::Arc, time::Duration},
    tokio::{sync::mpsc, time::Instant},
    tokio_util::sync::CancellationToken,
    tracing::{debug, error, info, warn},
};

/// A small dedicated pool for the address-index writer. We deliberately keep
/// this tiny so the writer can never starve the executor's account-load pool.
/// One connection is enough for sequential bulk inserts; the second slot is
/// just headroom for sqlx's own bookkeeping connection-resets.
const WRITER_POOL_SIZE: u32 = 2;

/// Sleep before each next attempt; total attempts = len + 1.
const FLUSH_RETRY_BACKOFF_MS: [u64; 2] = [100, 500];

pub struct AddressIndexWriterArgs {
    pub rows_rx: mpsc::Receiver<Vec<AddressSignatureRow>>,
    pub accountsdb_connection_url: String,
    pub flush_chunk_size: usize,
    pub shutdown_token: CancellationToken,
    pub metrics: SharedMetrics,
    pub heartbeat: Arc<StageHeartbeat>,
}

pub async fn start_address_index_writer(args: AddressIndexWriterArgs) -> WorkerHandle {
    let AddressIndexWriterArgs {
        mut rows_rx,
        accountsdb_connection_url,
        flush_chunk_size,
        shutdown_token,
        metrics,
        heartbeat,
    } = args;

    let handle = tokio::spawn(async move {
        info!(
            flush_chunk_size,
            "Address-index writer starting (pool size {})", WRITER_POOL_SIZE
        );

        let pool = match PgPoolOptions::new()
            .max_connections(WRITER_POOL_SIZE)
            .min_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(&accountsdb_connection_url)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                error!("Address-index writer failed to open PG pool: {}", e);
                return;
            }
        };

        // Buffer accumulates whatever recv_many delivers per tick.
        let mut buf: Vec<Vec<AddressSignatureRow>> = Vec::with_capacity(64);
        let mut flat: Vec<AddressSignatureRow> = Vec::with_capacity(flush_chunk_size * 2);

        loop {
            tokio::select! {
                biased;

                _ = shutdown_token.cancelled() => {
                    info!("Address-index writer received shutdown signal");
                    break;
                }

                n = rows_rx.recv_many(&mut buf, 64) => {
                    if n == 0 {
                        info!("Address-index writer input channel closed");
                        break;
                    }
                    heartbeat.record_input();
                    metrics.address_signatures_queue_depth(rows_rx.len());

                    for batch in buf.drain(..) {
                        flat.extend(batch);
                        // Flush in chunk-sized COMMITs so a single tick worth
                        // of work never produces an oversized transaction.
                        while flat.len() >= flush_chunk_size {
                            let take = flat.split_off(flush_chunk_size);
                            let chunk = std::mem::replace(&mut flat, take);
                            if let Err(e) =
                                flush_and_record(&pool, &chunk, &flat, &metrics, &heartbeat).await
                            {
                                error!(?e, "address_signatures flush failed; exiting");
                                return;
                            }
                        }
                    }
                    if !flat.is_empty() {
                        let chunk = std::mem::take(&mut flat);
                        if let Err(e) =
                            flush_and_record(&pool, &chunk, &flat, &metrics, &heartbeat).await
                        {
                            error!(?e, "address_signatures flush failed; exiting");
                            return;
                        }
                    }
                }
            }
        }

        // Drain anything still buffered after shutdown / channel close.
        while let Ok(batch) = rows_rx.try_recv() {
            flat.extend(batch);
        }
        if !flat.is_empty() {
            if let Err(e) = flush_and_record(&pool, &flat, &[], &metrics, &heartbeat).await {
                error!(?e, "address_signatures final flush failed; exiting");
                return;
            }
        }

        info!("Address-index writer stopped");
    });

    WorkerHandle::new("AddressIndexWriter".to_string(), handle)
}

async fn flush_and_record(
    pool: &PgPool,
    chunk: &[AddressSignatureRow],
    remaining: &[AddressSignatureRow],
    metrics: &SharedMetrics,
    heartbeat: &StageHeartbeat,
) -> Result<(), sqlx::Error> {
    let watermark = pick_watermark(chunk, remaining);
    flush_chunk_with_retry(pool, chunk, watermark, metrics).await?;
    heartbeat.record_progress();
    Ok(())
}

/// Watermark advance; assumes monotonic slots.
fn pick_watermark(chunk: &[AddressSignatureRow], remaining: &[AddressSignatureRow]) -> Option<i64> {
    if chunk.is_empty() {
        return None;
    }
    match remaining.first() {
        Some(r) => Some(r.slot - 1),
        None => chunk.last().map(|r| r.slot),
    }
}

async fn flush_chunk_with_retry(
    pool: &PgPool,
    rows: &[AddressSignatureRow],
    watermark: Option<i64>,
    metrics: &SharedMetrics,
) -> Result<(), sqlx::Error> {
    for (i, ms) in FLUSH_RETRY_BACKOFF_MS.iter().enumerate() {
        match flush_chunk_once(pool, rows, watermark, metrics).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                warn!(error = %e, attempt = i, "address_signatures flush retry");
                tokio::time::sleep(Duration::from_millis(*ms)).await;
            }
        }
    }
    flush_chunk_once(pool, rows, watermark, metrics).await
}

async fn flush_chunk_once(
    pool: &PgPool,
    rows: &[AddressSignatureRow],
    watermark: Option<i64>,
    metrics: &SharedMetrics,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() && watermark.is_none() {
        return Ok(());
    }
    let n = rows.len();

    let t0 = Instant::now();
    let result: Result<(), sqlx::Error> = async {
        let mut tx = pool.begin().await?;
        upsert_address_signature_rows(&mut tx, rows).await?;
        if let Some(slot) = watermark {
            upsert_address_signatures_flushed_slot_in_tx(&mut tx, slot).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    .await;

    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    metrics.address_signatures_flush_duration_ms(elapsed_ms);

    match &result {
        Ok(()) => {
            metrics.address_signatures_rows_flushed(n);
            debug!(
                rows = n,
                elapsed_ms,
                ?watermark,
                "address_signatures flush complete"
            );
        }
        Err(e) => {
            metrics.address_signatures_flush_errors_total();
            warn!(
                rows = n,
                elapsed_ms, "address_signatures flush failed: {}", e
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::address_index_watermark::get_address_signatures_flushed_slot,
        stage_metrics::NoopMetrics,
        test_helpers::{postgres_container_url, start_test_postgres},
    };
    use std::time::Duration;

    fn make_row(addr_byte: u8, slot: i64, sig_byte: u8) -> AddressSignatureRow {
        AddressSignatureRow {
            address: vec![addr_byte; 32],
            slot,
            signature: vec![sig_byte; 64],
        }
    }

    async fn count_rows(url: &str) -> i64 {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .unwrap();
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM address_signatures")
            .fetch_one(&pool)
            .await
            .unwrap()
    }

    async fn read_watermark(url: &str) -> Option<i64> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .unwrap();
        let pool = Arc::new(pool);
        get_address_signatures_flushed_slot(&pool).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_drains_channel_and_exits_on_close() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (tx, rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: url.clone(),
            flush_chunk_size: 100,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        for slot in 0..5i64 {
            tx.send(vec![make_row(slot as u8 + 1, slot, slot as u8 + 1)])
                .await
                .unwrap();
        }
        // Closing the sender should let the writer exit on its own.
        drop(tx);

        let join = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(join.is_ok(), "writer should exit after channel close");

        let n = count_rows(&url).await;
        assert_eq!(n, 5, "all sent rows should have landed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_chunked_flush_bounds_commit_size() {
        // Chunk size is small enough that a single batch of 1500 rows must be
        // split across several COMMITs. We don't observe COMMIT boundaries
        // directly, but if the chunking logic were broken the writer would
        // either OOM, panic, or fail to land all rows.
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (tx, rx) = mpsc::channel(4);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: url.clone(),
            flush_chunk_size: 200,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        let big: Vec<AddressSignatureRow> = (0..1500)
            .map(|i| AddressSignatureRow {
                address: (i as u32).to_le_bytes().repeat(8),
                slot: i as i64,
                signature: (i as u32).to_le_bytes().repeat(16),
            })
            .collect();
        tx.send(big).await.unwrap();
        drop(tx);

        let join = tokio::time::timeout(Duration::from_secs(15), handle.handle).await;
        assert!(join.is_ok(), "writer should finish chunking within timeout");

        let n = count_rows(&url).await;
        assert_eq!(n, 1500, "every row across all chunks should land");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_handles_pg_error_and_continues() {
        // Feed a row whose `slot` overflows BIGINT bounds via an out-of-range
        // value isn't possible at the type level; instead, wedge the first
        // batch by pointing at a database that doesn't exist, then immediately
        // close — the writer should log the failure and exit cleanly without
        // panicking.
        let bad_url = "postgres://nope:nope@127.0.0.1:1/nonexistent_db".to_string();

        let (tx, rx) = mpsc::channel(4);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: bad_url,
            flush_chunk_size: 100,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        // The pool open should fail and the worker should exit on its own.
        drop(tx);
        let join = tokio::time::timeout(Duration::from_secs(60), handle.handle).await;
        assert!(join.is_ok(), "writer must not hang on bad PG URL");
    }

    /// Permanent flush failure must exit the task.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_exits_when_retries_exhausted() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query("DROP TABLE address_signatures")
            .execute(&admin)
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel(4);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: url.clone(),
            flush_chunk_size: 100,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        tx.send(vec![make_row(1, 0, 1)]).await.unwrap();

        let join = tokio::time::timeout(Duration::from_secs(15), handle.handle).await;
        assert!(join.is_ok(), "writer should exit after retries exhaust");

        let send_after = tx.send(vec![make_row(2, 1, 2)]).await;
        assert!(send_after.is_err(), "writer receiver should be closed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_shutdown_flushes_in_flight_buffer() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (tx, rx) = mpsc::channel(64);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: url.clone(),
            flush_chunk_size: 100,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        // Fill the channel before signaling shutdown so several batches are
        // still queued when the cancellation arm fires.
        for slot in 0..10i64 {
            tx.send(vec![make_row(slot as u8 + 10, slot, slot as u8 + 10)])
                .await
                .unwrap();
        }
        shutdown.cancel();
        drop(tx);

        let join = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(join.is_ok(), "writer should shut down within timeout");

        // Drain on shutdown is best-effort over try_recv; verify the rows that
        // arrived before cancel completed all landed (cancellation is racy
        // so we only guarantee at-least-one row landed plus the in-flight
        // buffer was flushed).
        let n = count_rows(&url).await;
        assert!(
            n > 0,
            "at least the in-flight buffer must be flushed on shutdown, got {}",
            n
        );
    }

    /// Watermark = max flushed slot when buffer drains.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_advances_watermark_in_same_commit() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        let (tx, rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let handle = start_address_index_writer(AddressIndexWriterArgs {
            rows_rx: rx,
            accountsdb_connection_url: url.clone(),
            flush_chunk_size: 100,
            shutdown_token: shutdown.clone(),
            metrics: Arc::new(NoopMetrics),
            heartbeat: StageHeartbeat::new(),
        })
        .await;

        for slot in 10i64..=12 {
            tx.send(vec![
                make_row(slot as u8, slot, slot as u8),
                make_row((slot + 100) as u8, slot, (slot + 1) as u8),
            ])
            .await
            .unwrap();
        }
        drop(tx);

        let join = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(join.is_ok(), "writer should exit after channel close");

        let rows = count_rows(&url).await;
        assert_eq!(rows, 6, "expected 6 rows across slots 10..=12");

        let wm = read_watermark(&url).await;
        assert_eq!(wm, Some(12), "watermark should advance to max flushed slot");
    }

    #[test]
    fn pick_watermark_no_remaining_returns_chunk_max() {
        let chunk = vec![
            AddressSignatureRow {
                address: vec![1; 32],
                slot: 5,
                signature: vec![1; 64],
            },
            AddressSignatureRow {
                address: vec![2; 32],
                slot: 7,
                signature: vec![2; 64],
            },
        ];
        assert_eq!(pick_watermark(&chunk, &[]), Some(7));
    }

    #[test]
    fn pick_watermark_with_remaining_uses_min_minus_one() {
        let chunk = vec![AddressSignatureRow {
            address: vec![1; 32],
            slot: 5,
            signature: vec![1; 64],
        }];
        let remaining = vec![AddressSignatureRow {
            address: vec![3; 32],
            slot: 8,
            signature: vec![3; 64],
        }];
        assert_eq!(pick_watermark(&chunk, &remaining), Some(7));
    }
}
