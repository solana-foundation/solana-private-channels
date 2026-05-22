use std::sync::Arc;
use tracing::debug;

/// Instrumentation trait — each stage calls into this; no pipeline logic changes.
pub trait StageMetrics: Send + Sync {
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

    // Executor — latency histograms (durations in milliseconds)
    fn executor_batch_duration_ms(&self, ms: f64);
    fn executor_preload_duration_ms(&self, ms: f64);
    fn executor_svm_duration_ms(&self, kind: &'static str, ms: f64);
    fn executor_bob_update_duration_ms(&self, kind: &'static str, ms: f64);

    // Settler
    fn settler_txs_settled(&self, count: usize);
    fn settler_settle_duration_ms(&self, ms: f64);
    fn settler_db_write_duration_ms(&self, ms: f64);
    fn settler_processing_duration_ms(&self, ms: f64);

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
    fn settler_txs_settled(&self, n: usize) {
        debug!("settler: settled {}", n);
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
    SETTLER_TXS_SETTLED,
    "private_channel_settler_txs_settled_total",
    "Transactions settled to DB",
    &[]
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
    fn settler_txs_settled(&self, n: usize) {
        SETTLER_TXS_SETTLED
            .with_label_values(&[] as &[&str])
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
        SETTLER_TXS_SETTLED,
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
        ADDRESS_SIGNATURES_FLUSH_DURATION
    );
}
