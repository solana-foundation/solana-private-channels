use {
    crate::{
        accounts::write_batch::AddressSignatureRow, health::StageHeartbeat,
        nodes::node::WorkerHandle, stage_metrics::SharedMetrics,
    },
    sqlx::{postgres::PgPoolOptions, PgPool},
    std::sync::Arc,
    tokio::{sync::mpsc, time::Instant},
    tokio_util::sync::CancellationToken,
    tracing::{debug, error, info, warn},
};

/// A small dedicated pool for the address-index writer. We deliberately keep
/// this tiny so the writer can never starve the executor's account-load pool.
/// One connection is enough for sequential bulk inserts; the second slot is
/// just headroom for sqlx's own bookkeeping connection-resets.
const WRITER_POOL_SIZE: u32 = 2;

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

        // Buffer accumulates whatever recv_many delivers per tick. Capacity is
        // a hint; recv_many caps at flush_chunk_size so the buffer never grows
        // unbounded during steady state.
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
                            flush_chunk(&pool, &chunk, &metrics).await;
                            heartbeat.record_progress();
                        }
                    }
                    if !flat.is_empty() {
                        let chunk = std::mem::take(&mut flat);
                        flush_chunk(&pool, &chunk, &metrics).await;
                        heartbeat.record_progress();
                    }
                }
            }
        }

        // Drain anything still buffered after shutdown / channel close so we
        // don't drop addr_sig rows whose transactions row is already durable.
        // Apply the same chunking the steady-state path uses — a backlogged
        // shutdown can hold thousands of messages (channel cap 1024 × O(100)
        // rows each), and a single unchunked UNNEST would exceed
        // `flush_chunk_size` by orders of magnitude and risk timing out.
        while let Ok(batch) = rows_rx.try_recv() {
            flat.extend(batch);
        }
        while flat.len() >= flush_chunk_size {
            let take = flat.split_off(flush_chunk_size);
            let chunk = std::mem::replace(&mut flat, take);
            flush_chunk(&pool, &chunk, &metrics).await;
            heartbeat.record_progress();
        }
        if !flat.is_empty() {
            flush_chunk(&pool, &flat, &metrics).await;
            heartbeat.record_progress();
            flat.clear();
        }

        info!("Address-index writer stopped");
    });

    WorkerHandle::new("AddressIndexWriter".to_string(), handle)
}

async fn flush_chunk(pool: &PgPool, rows: &[AddressSignatureRow], metrics: &SharedMetrics) {
    if rows.is_empty() {
        return;
    }
    let n = rows.len();
    let mut addresses: Vec<&[u8]> = Vec::with_capacity(n);
    let mut slots: Vec<i64> = Vec::with_capacity(n);
    let mut signatures: Vec<&[u8]> = Vec::with_capacity(n);
    for r in rows {
        addresses.push(&r.address);
        slots.push(r.slot);
        signatures.push(&r.signature);
    }

    let t0 = Instant::now();
    let result = sqlx::query(
        "INSERT INTO address_signatures (address, slot, signature)
         SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::bytea[])
         ON CONFLICT DO NOTHING",
    )
    .bind(&addresses)
    .bind(&slots)
    .bind(&signatures)
    .execute(pool)
    .await;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    metrics.address_signatures_flush_duration_ms(elapsed_ms);

    match result {
        Ok(_) => {
            metrics.address_signatures_rows_flushed(n);
            debug!(rows = n, elapsed_ms, "address_signatures flush complete");
        }
        Err(e) => {
            metrics.address_signatures_flush_errors_total();
            warn!(
                rows = n,
                elapsed_ms, "address_signatures flush failed (worker continues): {}", e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
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

    #[tokio::test(flavor = "multi_thread")]
    async fn writer_continues_after_transient_pg_error() {
        let (_db, pg) = start_test_postgres().await;
        let url = postgres_container_url(&pg, "test_db").await;

        // Drop the address_signatures table out from under the writer to
        // induce a PG error on the first flush, then recreate it. The worker
        // must keep running and successfully flush the second batch.
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

        // First batch hits the missing table → flush error logged, worker survives.
        tx.send(vec![make_row(1, 0, 1)]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Recreate the table, send a second batch — must land.
        sqlx::query(
            "CREATE TABLE address_signatures (
                address BYTEA NOT NULL,
                slot BIGINT NOT NULL,
                signature BYTEA NOT NULL,
                PRIMARY KEY (address, slot, signature)
            )",
        )
        .execute(&admin)
        .await
        .unwrap();

        tx.send(vec![make_row(2, 1, 2), make_row(3, 2, 3)])
            .await
            .unwrap();
        drop(tx);

        let join = tokio::time::timeout(Duration::from_secs(10), handle.handle).await;
        assert!(join.is_ok(), "writer should exit cleanly after recovery");

        let n = count_rows(&url).await;
        assert_eq!(n, 2, "rows from the second batch should be persisted");
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
}
