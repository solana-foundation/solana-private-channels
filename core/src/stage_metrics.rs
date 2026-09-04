use std::sync::Arc;
use tracing::debug;

/// Instrumentation trait — each stage calls into this; no pipeline logic changes.
pub trait StageMetrics: Send + Sync {
    // RPC ingress
    fn rpc_ingress_shed(&self);

    // Dedup
    fn dedup_received(&self);
    fn dedup_forwarded(&self);
    fn dedup_dropped_duplicate(&self);
    fn dedup_dropped_unknown_blockhash(&self);

    // Sigverify
    fn sigverify_forwarded(&self);
    fn sigverify_rejected(&self, reason: &'static str);

    // Sequencer
    fn sequencer_collected(&self, tx_count: usize);
    fn sequencer_transactions_emitted(&self, tx_count: usize);

    // Executor — throughput counters
    fn executor_results_sent(&self, tx_count: usize);
    fn executor_results_send_failed(&self, kind: &'static str);
    fn executor_missing_results(&self, kind: &'static str);
    fn executor_dropped_expired_blockhash(&self, count: usize);
    fn executor_conservation_rejected(&self);
    /// A batch aborted because its accounts could not be loaded. Non-zero means
    /// the executor stopped rather than execute against unknown state.
    fn executor_preload_fatal(&self);
    /// A stored account row would not deserialize. Alert on any: it recurs on
    /// restart until the row is repaired.
    fn executor_corrupt_account(&self);

    // Writer lease
    fn writer_lease_probe(&self, outcome: &'static str);
    fn writer_lease_lost(&self, reason: &'static str);

    // Executor — latency histograms (durations in milliseconds)
    fn executor_batch_duration_ms(&self, ms: f64);
    fn executor_preload_duration_ms(&self, ms: f64);
    fn executor_svm_duration_ms(&self, kind: &'static str, ms: f64);
    fn executor_bob_update_duration_ms(&self, kind: &'static str, ms: f64);

    // BOB account cache size
    fn bob_cache_entries(&self, count: usize);
    fn bob_cache_dirty_entries(&self, count: usize);
    fn bob_cache_bytes(&self, bytes: usize);
    fn bob_cache_evicted(&self, count: usize);
    /// Settlement acknowledgements whose generation matched but whose bytes did
    /// not. Non-zero means BOB and the settler have diverged; alert on any.
    fn bob_settlement_divergences(&self, count: usize);

    // Settler
    fn executor_results_chunked(&self, chunks: usize);
    fn settler_buffered_account_bytes(&self, bytes: usize);
    fn settler_backpressure_engaged(&self);
    fn settler_txs_settled(&self, count: usize);
    fn settler_settle_retried(&self);
    /// Executed transactions dropped without a settled block, by the stage
    /// that was holding them.
    fn discarded_executed_transactions(&self, stage: &'static str, count: usize);
    fn settler_settle_duration_ms(&self, ms: f64);
    fn settler_db_write_duration_ms(&self, ms: f64);
    fn settler_processing_duration_ms(&self, ms: f64);

    // Redis cache (optional read-through cache in front of Postgres). These are
    // the only signal that the cache has taken itself out of service; without
    // them a node silently serves every read from Postgres.
    fn redis_cache_purged(&self);
    fn redis_cache_condemned(&self);
    fn redis_cache_write_failed(&self);
    fn redis_cache_disabled(&self);

    // Address-index writer (off-critical-path background worker)
    fn address_signatures_queue_depth(&self, depth: usize);
    fn address_signatures_send_blocked_ms(&self, ms: f64);
    fn address_signatures_flush_duration_ms(&self, ms: f64);
    fn address_signatures_rows_flushed(&self, count: usize);
    fn address_signatures_flush_errors_total(&self);
}

pub type SharedMetrics = Arc<dyn StageMetrics>;

// ---------------------------------------------------------------------------
// NoopMetrics — zero overhead in production; emits debug logs only.
// ---------------------------------------------------------------------------

pub struct NoopMetrics;

impl StageMetrics for NoopMetrics {
    fn rpc_ingress_shed(&self) {
        debug!("rpc: ingress shed");
    }
    fn dedup_received(&self) {
        debug!("dedup: received");
    }
    fn dedup_forwarded(&self) {
        debug!("dedup: forwarded");
    }
    fn dedup_dropped_duplicate(&self) {
        debug!("dedup: dropped duplicate");
    }
    fn dedup_dropped_unknown_blockhash(&self) {
        debug!("dedup: dropped unknown blockhash");
    }
    fn sigverify_forwarded(&self) {
        debug!("sigverify: forwarded");
    }
    fn sigverify_rejected(&self, reason: &'static str) {
        debug!("sigverify: rejected reason={}", reason);
    }
    fn sequencer_collected(&self, n: usize) {
        debug!("sequencer: collected {}", n);
    }
    fn sequencer_transactions_emitted(&self, n: usize) {
        debug!("sequencer: emitted {} transactions", n);
    }
    fn executor_results_sent(&self, n: usize) {
        debug!("executor: sent {} results", n);
    }
    fn executor_results_send_failed(&self, kind: &'static str) {
        debug!("executor: send failed kind={}", kind);
    }
    fn executor_missing_results(&self, kind: &'static str) {
        debug!("executor: missing results kind={}", kind);
    }
    fn executor_dropped_expired_blockhash(&self, count: usize) {
        debug!("executor: dropped {} expired blockhash txs", count);
    }
    fn executor_conservation_rejected(&self) {
        debug!("executor: rejected tx failing lamport conservation");
    }
    fn executor_preload_fatal(&self) {
        debug!("executor: batch aborted, account preload failed");
    }
    fn executor_corrupt_account(&self) {
        debug!("executor: corrupt stored account");
    }
    fn writer_lease_probe(&self, outcome: &'static str) {
        debug!("writer lease: probe {}", outcome);
    }
    fn writer_lease_lost(&self, reason: &'static str) {
        debug!("writer lease: lost, {}", reason);
    }
    fn executor_batch_duration_ms(&self, ms: f64) {
        debug!("executor: batch_duration={:.3}ms", ms);
    }
    fn executor_preload_duration_ms(&self, ms: f64) {
        debug!("executor: preload_duration={:.3}ms", ms);
    }
    fn executor_svm_duration_ms(&self, kind: &'static str, ms: f64) {
        debug!("executor: svm_duration kind={} {:.3}ms", kind, ms);
    }
    fn executor_bob_update_duration_ms(&self, kind: &'static str, ms: f64) {
        debug!("executor: bob_update_duration kind={} {:.3}ms", kind, ms);
    }
    fn bob_cache_entries(&self, count: usize) {
        debug!("bob: cache_entries={}", count);
    }
    fn bob_cache_dirty_entries(&self, count: usize) {
        debug!("bob: cache_dirty_entries={}", count);
    }
    fn bob_cache_bytes(&self, bytes: usize) {
        debug!("bob: cache_bytes={}", bytes);
    }
    fn bob_cache_evicted(&self, count: usize) {
        debug!("bob: cache_evicted={}", count);
    }
    fn bob_settlement_divergences(&self, count: usize) {
        debug!("bob: settlement_divergences={}", count);
    }
    fn executor_results_chunked(&self, n: usize) {
        debug!("executor: results split into {} chunks", n);
    }
    fn settler_buffered_account_bytes(&self, bytes: usize) {
        debug!("settler: buffered_account_bytes={}", bytes);
    }
    fn settler_backpressure_engaged(&self) {
        debug!("settler: backpressure engaged");
    }
    fn settler_txs_settled(&self, n: usize) {
        debug!("settler: settled {}", n);
    }
    fn settler_settle_retried(&self) {
        debug!("settler: settle retried");
    }
    fn discarded_executed_transactions(&self, stage: &'static str, n: usize) {
        debug!("{}: discarded {}", stage, n);
    }
    fn settler_settle_duration_ms(&self, ms: f64) {
        debug!("settler: settle_duration={:.3}ms", ms);
    }
    fn settler_db_write_duration_ms(&self, ms: f64) {
        debug!("settler: db_write_duration={:.3}ms", ms);
    }
    fn settler_processing_duration_ms(&self, ms: f64) {
        debug!("settler: processing_duration={:.3}ms", ms);
    }
    fn redis_cache_purged(&self) {
        debug!("redis cache: purged");
    }
    fn redis_cache_condemned(&self) {
        debug!("redis cache: condemned");
    }
    fn redis_cache_write_failed(&self) {
        debug!("redis cache: write failed");
    }
    fn redis_cache_disabled(&self) {
        debug!("redis cache: disabled");
    }
    fn address_signatures_queue_depth(&self, depth: usize) {
        debug!("address_signatures: queue_depth={}", depth);
    }
    fn address_signatures_send_blocked_ms(&self, ms: f64) {
        debug!("address_signatures: send_blocked={:.3}ms", ms);
    }
    fn address_signatures_flush_duration_ms(&self, ms: f64) {
        debug!("address_signatures: flush_duration={:.3}ms", ms);
    }
    fn address_signatures_rows_flushed(&self, count: usize) {
        debug!("address_signatures: rows_flushed={}", count);
    }
    fn address_signatures_flush_errors_total(&self) {
        debug!("address_signatures: flush_error");
    }
}

// ---------------------------------------------------------------------------
// PrometheusMetrics — enabled via --metrics; writes to global registry.
// ---------------------------------------------------------------------------

use private_channel_metrics::{counter_vec, gauge_vec, init_metrics};

// Counters
counter_vec!(
    REDIS_CACHE_PURGED,
    "private_channel_redis_cache_purged_total",
    "Times the Redis cache was emptied because its contents could not be trusted",
    &[]
);
counter_vec!(
    REDIS_CACHE_CONDEMNED,
    "private_channel_redis_cache_condemned_total",
    "Times the Redis cache was taken out of service after missing a settled batch",
    &[]
);
counter_vec!(
    REDIS_CACHE_WRITE_FAILED,
    "private_channel_redis_cache_write_failed_total",
    "Batches the Redis cache did not take after Postgres had committed: write failed, budget elapsed, or lease renewal refused",
    &[]
);
counter_vec!(
    REDIS_CACHE_DISABLED,
    "private_channel_redis_cache_disabled_total",
    "Times a node gave up on the Redis cache and continued Postgres-only",
    &[]
);
counter_vec!(
    RPC_INGRESS_SHED,
    "private_channel_rpc_ingress_shed_total",
    "Transactions shed at RPC ingress because the dedup queue was full",
    &[]
);
counter_vec!(
    DEDUP_RECEIVED,
    "private_channel_dedup_received_total",
    "Transactions received by dedup",
    &[]
);
counter_vec!(
    DEDUP_FORWARDED,
    "private_channel_dedup_forwarded_total",
    "Transactions forwarded by dedup",
    &[]
);
counter_vec!(
    DEDUP_DROPPED_DUP,
    "private_channel_dedup_dropped_duplicate_total",
    "Transactions dropped as duplicates",
    &[]
);
counter_vec!(
    DEDUP_DROPPED_UNK_BH,
    "private_channel_dedup_dropped_unknown_bh_total",
    "Transactions dropped for unknown blockhash",
    &[]
);
counter_vec!(
    SIGVERIFY_FORWARDED,
    "private_channel_sigverify_forwarded_total",
    "Transactions forwarded by sigverify",
    &[]
);
counter_vec!(
    SIGVERIFY_REJECTED,
    "private_channel_sigverify_rejected_total",
    "Transactions rejected by sigverify",
    &["reason"]
);
counter_vec!(
    SEQUENCER_COLLECTED,
    "private_channel_sequencer_collected_total",
    "Transactions collected by sequencer",
    &[]
);
counter_vec!(
    SEQUENCER_TXS_EMITTED,
    "private_channel_sequencer_transactions_emitted_total",
    "Transactions emitted by sequencer",
    &[]
);
counter_vec!(
    EXECUTOR_RESULTS_SENT,
    "private_channel_executor_results_sent_total",
    "Execution results sent to settler",
    &[]
);
counter_vec!(
    EXECUTOR_RESULTS_CHUNKED,
    "private_channel_executor_results_chunked_total",
    "Execution results split into byte-bounded chunks before the settler send",
    &[]
);
counter_vec!(
    EXECUTOR_RESULTS_SEND_FAILED,
    "private_channel_executor_results_send_failed_total",
    "Failed to send execution results",
    &["kind"]
);
counter_vec!(
    EXECUTOR_MISSING_RESULTS,
    "private_channel_executor_missing_results_total",
    "Missing execution results",
    &["kind"]
);
counter_vec!(
    EXECUTOR_DROPPED_EXPIRED_BH,
    "private_channel_executor_dropped_expired_bh_total",
    "Transactions dropped at execution due to expired blockhash",
    &[]
);
counter_vec!(
    EXECUTOR_CONSERVATION_REJECTED,
    "private_channel_executor_conservation_rejected_total",
    "Transactions failed at execution for leaking fabricated fee-payer lamports",
    &[]
);
counter_vec!(
    EXECUTOR_PRELOAD_FATAL,
    "private_channel_executor_preload_fatal_total",
    "Batches aborted because the accounts they reference could not be loaded",
    &[]
);
counter_vec!(
    EXECUTOR_CORRUPT_ACCOUNT,
    "private_channel_executor_corrupt_account_total",
    "Stored account rows that could not be deserialized",
    &[]
);
counter_vec!(
    WRITER_LEASE_PROBE,
    "private_channel_writer_lease_probe_total",
    "Writer lease ownership probes by outcome",
    &["outcome"]
);
counter_vec!(
    WRITER_LEASE_LOST,
    "private_channel_writer_lease_lost_total",
    "Times the writer lease stopped being provable, by reason",
    &["reason"]
);
counter_vec!(
    SETTLER_TXS_SETTLED,
    "private_channel_settler_txs_settled_total",
    "Transactions settled to DB",
    &[]
);
counter_vec!(
    SETTLER_BACKPRESSURE_ENGAGED,
    "private_channel_settler_backpressure_engaged_total",
    "Ticks that flushed a settle buffer already at or over its byte budget",
    &[]
);
counter_vec!(
    SETTLER_SETTLE_RETRIED,
    "private_channel_settler_settle_retried_total",
    "Settle attempts that failed and were retried",
    &[]
);
counter_vec!(
    DISCARDED_EXECUTED_TRANSACTIONS,
    "private_channel_discarded_executed_transactions_total",
    "Executed transactions dropped without a settled block",
    &["stage"]
);
counter_vec!(
    ADDRESS_SIGNATURES_ROWS_FLUSHED,
    "private_channel_address_signatures_rows_flushed_total",
    "Rows flushed to address_signatures by the index writer",
    &[]
);
counter_vec!(
    ADDRESS_SIGNATURES_FLUSH_ERRORS,
    "private_channel_address_signatures_flush_errors_total",
    "Address-index writer flush failures (worker continues on next batch)",
    &[]
);
gauge_vec!(
    ADDRESS_SIGNATURES_QUEUE_DEPTH,
    "private_channel_address_signatures_queue_depth",
    "Last observed depth of the address_signatures bounded mpsc channel",
    &[]
);
counter_vec!(
    BOB_CACHE_EVICTED,
    "private_channel_bob_cache_evicted_total",
    "BOB account-cache entries evicted (age sweep + hard cap)",
    &[]
);
counter_vec!(
    BOB_SETTLEMENT_DIVERGENCES,
    "private_channel_bob_settlement_divergences_total",
    "Settled accounts whose generation was covered but whose bytes differed from BOB (always 0 unless the executor and settler have diverged)",
    &[]
);
gauge_vec!(
    BOB_CACHE_ENTRIES,
    "private_channel_bob_cache_entries",
    "Total resident entries in the BOB account cache",
    &[]
);
gauge_vec!(
    BOB_CACHE_DIRTY_ENTRIES,
    "private_channel_bob_cache_dirty_entries",
    "Resident BOB entries ahead of the DB (un-evictable); refreshed at sweep cadence",
    &[]
);
gauge_vec!(
    BOB_CACHE_BYTES,
    "private_channel_bob_cache_bytes",
    "Approx resident account-data bytes in the BOB cache; refreshed at sweep cadence",
    &[]
);
gauge_vec!(
    SETTLER_BUFFERED_ACCOUNT_BYTES,
    "private_channel_settler_buffered_account_bytes",
    "Settled account-data bytes buffered since the last block, against the byte budget",
    &[]
);

// Gauges

// Executor latency histograms — buckets cover sub-millisecond to ~500 ms range.
use private_channel_metrics::histogram_vec;

histogram_vec!(
    EXECUTOR_BATCH_DURATION,
    "private_channel_executor_batch_duration_ms",
    "Total execute_batch wall time in milliseconds",
    &[]
);
histogram_vec!(
    EXECUTOR_PRELOAD_DURATION,
    "private_channel_executor_preload_duration_ms",
    "Account preload DB round-trip time in milliseconds",
    &[]
);
histogram_vec!(
    EXECUTOR_SVM_DURATION,
    "private_channel_executor_svm_duration_ms",
    "SVM load_and_execute time in milliseconds",
    &["kind"]
);
histogram_vec!(
    EXECUTOR_BOB_UPDATE_DURATION,
    "private_channel_executor_bob_update_duration_ms",
    "BOB update_accounts time in milliseconds",
    &["kind"]
);

// Settler latency histograms
histogram_vec!(
    SETTLER_SETTLE_DURATION,
    "private_channel_settler_settle_duration_ms",
    "Total settle_transactions wall time in milliseconds",
    &[]
);
histogram_vec!(
    SETTLER_DB_WRITE_DURATION,
    "private_channel_settler_db_write_duration_ms",
    "Postgres write_batch time in milliseconds",
    &[]
);
histogram_vec!(
    SETTLER_PROCESSING_DURATION,
    "private_channel_settler_processing_duration_ms",
    "Pre-DB account map building time in milliseconds",
    &[]
);
histogram_vec!(
    ADDRESS_SIGNATURES_SEND_BLOCKED,
    "private_channel_address_signatures_send_blocked_ms",
    "Settler-side mpsc::Sender::send().await blocking time in milliseconds",
    &[]
);
histogram_vec!(
    ADDRESS_SIGNATURES_FLUSH_DURATION,
    "private_channel_address_signatures_flush_duration_ms",
    "Address-index writer per-flush COMMIT time in milliseconds",
    &[]
);

pub struct PrometheusMetrics;

impl StageMetrics for PrometheusMetrics {
    fn rpc_ingress_shed(&self) {
        RPC_INGRESS_SHED.with_label_values(&[] as &[&str]).inc();
    }
    fn dedup_received(&self) {
        DEDUP_RECEIVED.with_label_values(&[] as &[&str]).inc();
    }
    fn dedup_forwarded(&self) {
        DEDUP_FORWARDED.with_label_values(&[] as &[&str]).inc();
    }
    fn dedup_dropped_duplicate(&self) {
        DEDUP_DROPPED_DUP.with_label_values(&[] as &[&str]).inc();
    }
    fn dedup_dropped_unknown_blockhash(&self) {
        DEDUP_DROPPED_UNK_BH.with_label_values(&[] as &[&str]).inc();
    }
    fn sigverify_forwarded(&self) {
        SIGVERIFY_FORWARDED.with_label_values(&[] as &[&str]).inc();
    }
    fn sigverify_rejected(&self, reason: &'static str) {
        SIGVERIFY_REJECTED.with_label_values(&[reason]).inc();
    }
    fn sequencer_collected(&self, n: usize) {
        SEQUENCER_COLLECTED
            .with_label_values(&[] as &[&str])
            .inc_by(n as f64);
    }
    fn sequencer_transactions_emitted(&self, n: usize) {
        SEQUENCER_TXS_EMITTED
            .with_label_values(&[] as &[&str])
            .inc_by(n as f64);
    }
    fn executor_results_sent(&self, n: usize) {
        EXECUTOR_RESULTS_SENT
            .with_label_values(&[] as &[&str])
            .inc_by(n as f64);
    }
    fn executor_results_send_failed(&self, kind: &'static str) {
        EXECUTOR_RESULTS_SEND_FAILED
            .with_label_values(&[kind])
            .inc();
    }
    fn executor_missing_results(&self, kind: &'static str) {
        EXECUTOR_MISSING_RESULTS.with_label_values(&[kind]).inc();
    }
    fn executor_dropped_expired_blockhash(&self, count: usize) {
        EXECUTOR_DROPPED_EXPIRED_BH
            .with_label_values(&[] as &[&str])
            .inc_by(count as f64);
    }
    fn executor_conservation_rejected(&self) {
        EXECUTOR_CONSERVATION_REJECTED
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn executor_preload_fatal(&self) {
        EXECUTOR_PRELOAD_FATAL
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn executor_corrupt_account(&self) {
        EXECUTOR_CORRUPT_ACCOUNT
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn writer_lease_probe(&self, outcome: &'static str) {
        WRITER_LEASE_PROBE.with_label_values(&[outcome]).inc();
    }
    fn writer_lease_lost(&self, reason: &'static str) {
        WRITER_LEASE_LOST.with_label_values(&[reason]).inc();
    }
    fn executor_batch_duration_ms(&self, ms: f64) {
        EXECUTOR_BATCH_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn executor_preload_duration_ms(&self, ms: f64) {
        EXECUTOR_PRELOAD_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn executor_svm_duration_ms(&self, kind: &'static str, ms: f64) {
        EXECUTOR_SVM_DURATION.with_label_values(&[kind]).observe(ms);
    }
    fn executor_bob_update_duration_ms(&self, kind: &'static str, ms: f64) {
        EXECUTOR_BOB_UPDATE_DURATION
            .with_label_values(&[kind])
            .observe(ms);
    }
    fn bob_cache_entries(&self, count: usize) {
        BOB_CACHE_ENTRIES
            .with_label_values(&[] as &[&str])
            .set(count as f64);
    }
    fn bob_cache_dirty_entries(&self, count: usize) {
        BOB_CACHE_DIRTY_ENTRIES
            .with_label_values(&[] as &[&str])
            .set(count as f64);
    }
    fn bob_cache_bytes(&self, bytes: usize) {
        BOB_CACHE_BYTES
            .with_label_values(&[] as &[&str])
            .set(bytes as f64);
    }
    fn bob_cache_evicted(&self, count: usize) {
        BOB_CACHE_EVICTED
            .with_label_values(&[] as &[&str])
            .inc_by(count as f64);
    }
    fn bob_settlement_divergences(&self, count: usize) {
        BOB_SETTLEMENT_DIVERGENCES
            .with_label_values(&[] as &[&str])
            .inc_by(count as f64);
    }
    fn executor_results_chunked(&self, n: usize) {
        EXECUTOR_RESULTS_CHUNKED
            .with_label_values(&[] as &[&str])
            .inc_by(n as f64);
    }
    fn settler_buffered_account_bytes(&self, bytes: usize) {
        SETTLER_BUFFERED_ACCOUNT_BYTES
            .with_label_values(&[] as &[&str])
            .set(bytes as f64);
    }
    fn settler_backpressure_engaged(&self) {
        SETTLER_BACKPRESSURE_ENGAGED
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn settler_txs_settled(&self, n: usize) {
        SETTLER_TXS_SETTLED
            .with_label_values(&[] as &[&str])
            .inc_by(n as f64);
    }
    fn settler_settle_retried(&self) {
        SETTLER_SETTLE_RETRIED
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn discarded_executed_transactions(&self, stage: &'static str, n: usize) {
        DISCARDED_EXECUTED_TRANSACTIONS
            .with_label_values(&[stage])
            .inc_by(n as f64);
    }
    fn settler_settle_duration_ms(&self, ms: f64) {
        SETTLER_SETTLE_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn settler_db_write_duration_ms(&self, ms: f64) {
        SETTLER_DB_WRITE_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn redis_cache_purged(&self) {
        REDIS_CACHE_PURGED.with_label_values(&[] as &[&str]).inc();
    }
    fn redis_cache_condemned(&self) {
        REDIS_CACHE_CONDEMNED
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn redis_cache_write_failed(&self) {
        REDIS_CACHE_WRITE_FAILED
            .with_label_values(&[] as &[&str])
            .inc();
    }
    fn redis_cache_disabled(&self) {
        REDIS_CACHE_DISABLED.with_label_values(&[] as &[&str]).inc();
    }
    fn settler_processing_duration_ms(&self, ms: f64) {
        SETTLER_PROCESSING_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn address_signatures_queue_depth(&self, depth: usize) {
        ADDRESS_SIGNATURES_QUEUE_DEPTH
            .with_label_values(&[] as &[&str])
            .set(depth as f64);
    }
    fn address_signatures_send_blocked_ms(&self, ms: f64) {
        ADDRESS_SIGNATURES_SEND_BLOCKED
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn address_signatures_flush_duration_ms(&self, ms: f64) {
        ADDRESS_SIGNATURES_FLUSH_DURATION
            .with_label_values(&[] as &[&str])
            .observe(ms);
    }
    fn address_signatures_rows_flushed(&self, count: usize) {
        ADDRESS_SIGNATURES_ROWS_FLUSHED
            .with_label_values(&[] as &[&str])
            .inc_by(count as f64);
    }
    fn address_signatures_flush_errors_total(&self) {
        ADDRESS_SIGNATURES_FLUSH_ERRORS
            .with_label_values(&[] as &[&str])
            .inc();
    }
}

/// Force-initialise all metric statics so they appear in /metrics from startup.
pub fn init_prometheus_metrics() {
    init_metrics!(
        RPC_INGRESS_SHED,
        DEDUP_RECEIVED,
        DEDUP_FORWARDED,
        DEDUP_DROPPED_DUP,
        DEDUP_DROPPED_UNK_BH,
        SIGVERIFY_FORWARDED,
        SIGVERIFY_REJECTED,
        SEQUENCER_COLLECTED,
        SEQUENCER_TXS_EMITTED,
        EXECUTOR_RESULTS_SENT,
        EXECUTOR_RESULTS_SEND_FAILED,
        EXECUTOR_MISSING_RESULTS,
        EXECUTOR_DROPPED_EXPIRED_BH,
        EXECUTOR_CONSERVATION_REJECTED,
        EXECUTOR_PRELOAD_FATAL,
        EXECUTOR_CORRUPT_ACCOUNT,
        WRITER_LEASE_PROBE,
        WRITER_LEASE_LOST,
        SETTLER_TXS_SETTLED,
        SETTLER_BACKPRESSURE_ENGAGED,
        EXECUTOR_RESULTS_CHUNKED,
        BOB_CACHE_EVICTED,
        BOB_CACHE_ENTRIES,
        BOB_CACHE_DIRTY_ENTRIES,
        BOB_CACHE_BYTES,
        SETTLER_BUFFERED_ACCOUNT_BYTES,
        // Executor latency histograms
        EXECUTOR_BATCH_DURATION,
        EXECUTOR_PRELOAD_DURATION,
        EXECUTOR_SVM_DURATION,
        EXECUTOR_BOB_UPDATE_DURATION,
        SETTLER_SETTLE_DURATION,
        SETTLER_DB_WRITE_DURATION,
        SETTLER_PROCESSING_DURATION,
        ADDRESS_SIGNATURES_ROWS_FLUSHED,
        ADDRESS_SIGNATURES_FLUSH_ERRORS,
        ADDRESS_SIGNATURES_QUEUE_DEPTH,
        ADDRESS_SIGNATURES_SEND_BLOCKED,
        ADDRESS_SIGNATURES_FLUSH_DURATION,
        REDIS_CACHE_PURGED,
        REDIS_CACHE_CONDEMNED,
        REDIS_CACHE_WRITE_FAILED,
        REDIS_CACHE_DISABLED
    );
}
