//! Per-batch Clock sysvar injection for the regular SVM.
//!
//! Contra has no real Clock sysvar in its account universe — `BOB`'s
//! precompile map (`crate::accounts::precompiles`) only seeds Rent, so
//! `TransactionBatchProcessor::fill_missing_sysvar_cache_entries` (called
//! once at processor creation in `crate::processor`) leaves the cached
//! Clock at its `Default` value, i.e. `unix_timestamp = 0`. A BPF program
//! that calls `Clock::get()?.unix_timestamp` would read 0, silently
//! breaking any time-dependent logic (DvP expiry, earliest-settlement
//! windows, etc).
//!
//! `set_clock_now` overwrites the cached Clock with the current wall clock
//! once per batch, before any SVM call. All transactions in a batch
//! observe the same `unix_timestamp`, mirroring how Solana stamps Clock
//! per slot.
//!
//! ## Why `set_sysvar_for_tests`
//!
//! `solana_program_runtime::sysvar_cache::SysvarCache` exposes exactly one
//! public mutator: `set_sysvar_for_tests`.
//!
//! ## Thread-safety
//!
//! `execute_parallel` shares a single `&TransactionBatchProcessor` across
//! worker threads, which take *read* locks on `sysvar_cache` during
//! syscalls. `set_clock_now` takes the *write* lock, so it must be called
//! before any worker is spawned (i.e. at the top of `execute_batch`,
//! never mid-batch). Calling it mid-batch would deadlock against an
//! in-flight syscall.

use {
    crate::processor::ContraForkGraph,
    solana_sdk::clock::Clock,
    solana_svm::transaction_processor::TransactionBatchProcessor,
    std::time::{SystemTime, UNIX_EPOCH},
};

/// Overwrite the processor's cached `Clock` with the current wall-clock
/// `unix_timestamp`. All other Clock fields (`slot`, `epoch`,
/// `epoch_start_timestamp`, `leader_schedule_epoch`) stay at their
/// defaults; Contra has no slot/epoch concept and no consumer reads
/// those fields today. If a future program needs them, populate here.
///
/// Must be called before any SVM execution in the batch — see the
/// thread-safety note in the module docs.
pub fn set_clock_now(processor: &TransactionBatchProcessor<ContraForkGraph>) {
    let unix_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before UNIX_EPOCH")
        .as_secs() as i64;

    processor
        .sysvar_cache
        .write()
        .unwrap()
        .set_sysvar_for_tests(&Clock {
            unix_timestamp,
            ..Default::default()
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// `set_clock_now` writes a `unix_timestamp` close to wall-clock
    /// `now()` into the cache, leaving all other Clock fields at default.
    /// Reads via `get_sysvar_cache_for_tests` to verify the round-trip.
    #[test]
    fn set_clock_now_writes_current_unix_timestamp() {
        let processor = TransactionBatchProcessor::<ContraForkGraph>::default();

        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        set_clock_now(&processor);
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let clock = processor
            .get_sysvar_cache_for_tests()
            .get_clock()
            .expect("Clock must be set after set_clock_now");

        assert!(
            clock.unix_timestamp >= before && clock.unix_timestamp <= after,
            "unix_timestamp {} must lie in [{}, {}]",
            clock.unix_timestamp,
            before,
            after,
        );
        // Non-timestamp fields stay at their defaults — nothing reads them.
        assert_eq!(clock.slot, 0);
        assert_eq!(clock.epoch, 0);
        assert_eq!(clock.epoch_start_timestamp, 0);
        assert_eq!(clock.leader_schedule_epoch, 0);
    }

    /// A second call advances `unix_timestamp` (or at worst stays equal
    /// when the test runs faster than 1s). Guards against a regression
    /// that latches the first-write value or ignores subsequent calls.
    #[test]
    fn set_clock_now_advances_on_repeat_call() {
        let processor = TransactionBatchProcessor::<ContraForkGraph>::default();

        set_clock_now(&processor);
        let first = processor
            .get_sysvar_cache_for_tests()
            .get_clock()
            .unwrap()
            .unix_timestamp;

        std::thread::sleep(std::time::Duration::from_secs(1));

        set_clock_now(&processor);
        let second = processor
            .get_sysvar_cache_for_tests()
            .get_clock()
            .unwrap()
            .unix_timestamp;

        assert!(
            second > first,
            "second call must advance unix_timestamp by at least 1s after a 1s sleep: first={first} second={second}"
        );
    }

    /// Default `TransactionBatchProcessor` has no Clock cached — the
    /// guard `get_clock` returns `Err`. This is exactly the silent-zero
    /// trap `set_clock_now` exists to avoid: without it, a BPF program's
    /// `Clock::get()` syscall reads `Default::default()` and sees
    /// `unix_timestamp = 0`. Documents why injection is required.
    #[test]
    fn default_processor_has_no_clock_until_injected() {
        let processor = TransactionBatchProcessor::<ContraForkGraph>::default();
        assert!(
            processor.get_sysvar_cache_for_tests().get_clock().is_err(),
            "default sysvar cache must not contain a Clock until set_clock_now is called"
        );
    }
}
