pub mod address_index_writer;
pub mod dedup;
pub mod execution;
pub mod sequencer;
pub mod settle;
pub mod sigverify;

pub use address_index_writer::*;
pub use dedup::*;
pub use execution::*;
pub use sequencer::*;
pub use settle::*;
pub use sigverify::*;

/// Signatures named per log line, so one discard cannot emit a single
/// unreadable record while still naming every transaction it dropped.
const DISCARD_SIGNATURES_PER_LINE: usize = 100;

/// Name every executed transaction being thrown away. Shared by the two stages
/// that can drop executed work, so a discard reads the same wherever it happens.
pub(crate) fn record_discarded(
    stage: &'static str,
    reason: &str,
    signatures: &[solana_sdk::signature::Signature],
    metrics: &crate::stage_metrics::SharedMetrics,
) {
    if signatures.is_empty() {
        return;
    }

    metrics.discarded_executed_transactions(stage, signatures.len());
    tracing::error!(
        "Discarding {} executed transactions that could not be settled: {}",
        signatures.len(),
        reason
    );
    for (index, chunk) in signatures.chunks(DISCARD_SIGNATURES_PER_LINE).enumerate() {
        let listed: Vec<String> = chunk.iter().map(|s| s.to_string()).collect();
        tracing::error!(
            "Discarded signatures {}-{} of {}: {}",
            index * DISCARD_SIGNATURES_PER_LINE + 1,
            index * DISCARD_SIGNATURES_PER_LINE + chunk.len(),
            signatures.len(),
            listed.join(",")
        );
    }
}

#[cfg(test)]
mod sponsor_replay_test;
