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

use {
    std::sync::Arc,
    tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore},
};

/// How much of a queue's memory its messages may hold, in bytes or rows. The
/// permit rides inside the message, so every way a message can end returns its
/// weight without the consumer accounting for it.
pub(crate) struct WeightBudget {
    semaphore: Arc<Semaphore>,
    /// Kept because a semaphore reports what is available, not what it started with.
    total: usize,
}

impl WeightBudget {
    pub(crate) fn new(total: usize) -> Self {
        // Permits are counted in a u32, and a total that wrapped to zero would
        // be a budget that silently never blocks.
        assert!(
            total <= u32::MAX as usize,
            "a weight budget must fit the u32 permit count"
        );
        Self {
            semaphore: Arc::new(Semaphore::new(total)),
            total,
        }
    }

    /// Take `weight`, clamped to the whole budget so an oversized message goes
    /// alone rather than waiting for room that cannot exist. `None` once `tx`
    /// has lost its receiver, so a parked producer cannot outlive its consumer.
    pub(crate) async fn acquire<T>(
        &self,
        weight: usize,
        tx: &mpsc::Sender<T>,
    ) -> Option<OwnedSemaphorePermit> {
        let wanted = weight.min(self.total) as u32;
        tokio::select! {
            // Checked first so a dead consumer always wins over free permits.
            biased;
            _ = tx.closed() => None,
            // Never closed, so the only failure this can report is impossible.
            permit = Arc::clone(&self.semaphore).acquire_many_owned(wanted) => permit.ok(),
        }
    }

    /// Only the tests read the budget back; production just parks on it.
    #[cfg(test)]
    pub(crate) fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}

/// One permit off a throwaway budget, so a test can build a queued message
/// without standing up the budget the production path uses.
#[cfg(test)]
pub(crate) fn test_permit() -> OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("a fresh semaphore has its permit")
}

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

#[cfg(test)]
mod weight_budget_tests {
    use {super::WeightBudget, std::time::Duration, tokio::sync::mpsc};

    /// The two hazards the budget exists for, plus the shapes that must never
    /// wait: a zero weight, a weight that fits, and one larger than the budget.
    #[tokio::test(flavor = "multi_thread")]
    async fn acquire_clamps_parks_and_gives_up_on_a_dead_consumer() {
        struct Case {
            name: &'static str,
            total: usize,
            prior: usize,
            weight: usize,
            parks: bool,
            close_receiver: bool,
            expect_permit: bool,
            available_after: usize,
        }

        let cases = [
            Case {
                name: "zero weight resolves at once",
                total: 10,
                prior: 0,
                weight: 0,
                parks: false,
                close_receiver: false,
                expect_permit: true,
                available_after: 10,
            },
            Case {
                name: "a weight that fits takes exactly that",
                total: 10,
                prior: 0,
                weight: 4,
                parks: false,
                close_receiver: false,
                expect_permit: true,
                available_after: 6,
            },
            Case {
                name: "an oversized weight clamps to the whole budget",
                total: 10,
                prior: 0,
                weight: 25,
                parks: false,
                close_receiver: false,
                expect_permit: true,
                available_after: 0,
            },
            Case {
                name: "a spent budget parks until a prior permit drops",
                total: 10,
                prior: 10,
                weight: 4,
                parks: true,
                close_receiver: false,
                expect_permit: true,
                available_after: 6,
            },
            Case {
                name: "a parked acquire gives up on a dropped receiver",
                total: 10,
                prior: 10,
                weight: 4,
                parks: true,
                close_receiver: true,
                expect_permit: false,
                available_after: 0,
            },
        ];

        for case in cases {
            let budget = WeightBudget::new(case.total);
            let (tx, rx) = mpsc::channel::<()>(1);
            let prior = budget.acquire(case.prior, &tx).await;
            assert!(prior.is_some(), "prior acquire for {}", case.name);

            let acquire = budget.acquire(case.weight, &tx);
            tokio::pin!(acquire);
            if case.parks {
                assert!(
                    tokio::time::timeout(Duration::from_millis(200), &mut acquire)
                        .await
                        .is_err(),
                    "{} must park",
                    case.name
                );
                if case.close_receiver {
                    drop(rx);
                } else {
                    drop(prior);
                }
            }

            let permit = tokio::time::timeout(Duration::from_secs(5), &mut acquire)
                .await
                .unwrap_or_else(|_| panic!("{} must resolve", case.name));
            assert_eq!(
                permit.is_some(),
                case.expect_permit,
                "permit for {}",
                case.name
            );
            assert_eq!(
                budget.available(),
                case.available_after,
                "available after {}",
                case.name
            );
        }
    }
}
