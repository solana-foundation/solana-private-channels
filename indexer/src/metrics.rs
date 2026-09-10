use private_channel_metrics::{counter_vec, gauge_vec, histogram_vec};

// ---------------------------------------------------------------------------
// Indexer metrics
// ---------------------------------------------------------------------------

counter_vec!(
    INDEXER_SLOTS_PROCESSED,
    "private_channel_indexer_slots_processed_total",
    "Total slots checkpointed by the indexer",
    &["program_type"]
);

counter_vec!(
    INDEXER_TRANSACTIONS_SAVED,
    "private_channel_indexer_transactions_saved_total",
    "Total transactions saved to the database",
    &["program_type"]
);

counter_vec!(
    INDEXER_MINTS_SAVED,
    "private_channel_indexer_mints_saved_total",
    "Total mints upserted to the database",
    &["program_type"]
);

counter_vec!(
    INDEXER_SLOT_SAVE_ERRORS,
    "private_channel_indexer_slot_save_errors_total",
    "Total slot save errors (mints or transactions)",
    &["program_type"]
);

gauge_vec!(
    INDEXER_CURRENT_SLOT,
    "private_channel_indexer_current_slot",
    "Latest slot successfully checkpointed",
    &["program_type"]
);

counter_vec!(
    INDEXER_RPC_ERRORS,
    "private_channel_indexer_rpc_errors_total",
    "Total RPC errors in datasource layer",
    &["program_type", "error_type"]
);

gauge_vec!(
    INDEXER_CHAIN_TIP_SLOT,
    "private_channel_indexer_chain_tip_slot",
    "Latest slot on the Solana chain as seen by the datasource",
    &["program_type"]
);

gauge_vec!(
    INDEXER_BACKFILL_SLOTS_REMAINING,
    "private_channel_indexer_backfill_slots_remaining",
    "Remaining slots to backfill (0 when not backfilling)",
    &["program_type"]
);

gauge_vec!(
    INDEXER_CHECKPOINT_FRONTIER_LAG,
    "private_channel_indexer_checkpoint_frontier_lag",
    "Slots between the backfill target tip and the contiguous checkpoint frontier while gated (0 when ungated or after handoff)",
    &["program_type"]
);

counter_vec!(
    INDEXER_DATASOURCE_RECONNECTS,
    "private_channel_indexer_datasource_reconnects_total",
    "Total Yellowstone gRPC reconnections",
    &["program_type"]
);

histogram_vec!(
    INDEXER_SLOT_PROCESSING_DURATION,
    "private_channel_indexer_slot_processing_duration_seconds",
    "Time to process and checkpoint a slot",
    &["program_type"]
);

// ---------------------------------------------------------------------------
// Operator metrics
// ---------------------------------------------------------------------------

counter_vec!(
    OPERATOR_TRANSACTIONS_FETCHED,
    "private_channel_operator_transactions_fetched_total",
    "Total transactions fetched from the database",
    &["program_type"]
);

counter_vec!(
    OPERATOR_DB_UPDATES,
    "private_channel_operator_db_updates_total",
    "Total transaction status DB updates",
    &["program_type", "status"]
);

counter_vec!(
    OPERATOR_DB_UPDATE_ERRORS,
    "private_channel_operator_db_update_errors_total",
    "Total transaction status DB update errors",
    &["program_type"]
);

// A status write matched no row because the transaction had already moved
// off Processing. That is routine for most statuses and is not counted here;
// only a skipped `Completed` increments, because it means an on-chain release
// the DB will never record, which wedges the next boot's bitmap diff.
// Any nonzero sample needs a human.
counter_vec!(
    OPERATOR_DB_UPDATE_SKIPPED,
    "private_channel_operator_db_updates_skipped_total",
    "Completed status DB updates that matched no row",
    &["program_type", "status"]
);

histogram_vec!(
    OPERATOR_RPC_SEND_DURATION,
    "private_channel_operator_rpc_send_duration_seconds",
    "Duration of RPC send_and_confirm calls",
    &["program_type", "result"]
);

counter_vec!(
    OPERATOR_TRANSACTION_ERRORS,
    "private_channel_operator_transaction_errors_total",
    "Total transaction errors by reason (includes retried errors)",
    &["program_type", "error_reason"]
);

counter_vec!(
    OPERATOR_MINTS_SENT,
    "private_channel_operator_mints_sent_total",
    "Total mint transactions successfully confirmed",
    &["program_type"]
);

gauge_vec!(
    OPERATOR_BACKLOG_DEPTH,
    "private_channel_operator_backlog_depth",
    "Number of pending transactions in the database",
    &["program_type"]
);

gauge_vec!(
    FEEPAYER_BALANCE_LAMPORTS,
    "private_channel_feepayer_balance_lamports",
    "Current SOL balance of the escrow operator feepayer wallet in lamports",
    &["program_type"]
);

// Poison-pill: a single transaction that could not be sent on-chain was
// quarantined to ManualReview so the pipeline could keep moving.  The `reason`
// label mirrors `classify_processor_error` (`invalid_pubkey`, `invalid_builder`,
// `program_error`) so dashboards can distinguish systemic bugs from one-off
// bad rows.  Keep `init_labels` in sync when adding a new variant.
counter_vec!(
    OPERATOR_TRANSACTION_QUARANTINED,
    "private_channel_operator_transaction_quarantined_total",
    "Transactions quarantined to ManualReview by the processor",
    &["program_type", "reason"]
);

/// Reason labels for the withdrawal bails that park one row and leave the
/// pipeline running. Kept here so the emitting code and the pre-registration
/// below read one list and a label cannot exist in only one of them.
pub const BAIL_REASON_UNSUPPORTED_MINT: &str = "unsupported_mint";
pub const BAIL_REASON_WITHDRAWALS_BLOCKED: &str = "withdrawals_blocked";
pub const BAIL_REASON_TARGET_MINT_MISSING: &str = "target_mint_missing";
pub const BAIL_REASON_MINT_PAUSED: &str = "mint_paused";
pub const BAIL_REASON_ESCROW_DRAINED: &str = "escrow_drained";
pub const BAIL_REASON_ESCROW_FROZEN: &str = "escrow_frozen";
pub const BAIL_REASON_HOOK_UNRESOLVABLE: &str = "hook_unresolvable";

pub const BAIL_REASONS: [&str; 7] = [
    BAIL_REASON_UNSUPPORTED_MINT,
    BAIL_REASON_WITHDRAWALS_BLOCKED,
    BAIL_REASON_TARGET_MINT_MISSING,
    BAIL_REASON_MINT_PAUSED,
    BAIL_REASON_ESCROW_DRAINED,
    BAIL_REASON_ESCROW_FROZEN,
    BAIL_REASON_HOOK_UNRESOLVABLE,
];

// Supervision: a critical task inside the operator exited.  The supervisor
// aborts the process immediately when this increments; the counter exists
// so dashboards can alert even if the restart is fast.
counter_vec!(
    OPERATOR_TASK_EXIT,
    "private_channel_operator_task_exit_total",
    "Critical operator task exits observed by the supervisor",
    &["program_type", "task"]
);

// Recovery worker outcome: a stuck-`Processing` row was healed by the
// stuck-row recovery worker.  `outcome` ∈ {completed, requeued, quarantined};
// `type` ∈ {deposit, withdrawal}.  All values 0 in steady state — any
// sustained nonzero is concrete evidence of operator crash-window activity.
//
// The same counter also reports withdrawals cleared out of a stalled status by
// the reconcile sweep: `manual_review_cleared` and `pending_remint_cleared`.
// Those two are withdrawal-only, and each one means a row that had stopped
// moving was proven landed on-chain and promoted to Completed.
counter_vec!(
    OPERATOR_STALE_PROCESSING_RECOVERED,
    "private_channel_operator_stale_processing_recovered_total",
    "Stale Processing rows healed by the recovery worker",
    &["program_type", "outcome", "type"]
);

// Reopened-deposit gate: a deposit picked up with persisted write-ahead mint
// signatures was resolved by classifying them on the channel before minting.
// `outcome` ∈ {completed, complete_raced, complete_write_failed, deferred_live,
// deferred_unverifiable}; the normal proceed path is the plain mint flow and is
// not counted. The gate never quarantines; an unresolved row is left Processing
// for the recovery sweep.
counter_vec!(
    OPERATOR_REOPENED_DEPOSIT_GATE,
    "private_channel_operator_reopened_deposit_gate_total",
    "Reopened deposits resolved by the pre-mint signature gate",
    &["program_type", "outcome"]
);

// Release-side confirmation gate: the on-chain bitmap verdict wherever a
// release consumer needs to know whether a nonce actually released. `site` is one
// of {recovery, remint, presend}; `verdict` is one of {landed, not_landed,
// uncertain}, plus `journal_unavailable` on `presend` only. `recovery` and
// `remint` are the terminal Dead branch of a recorded signature; `presend` is a
// Processing withdrawal with no recorded signature at all, where the verdict
// decides whether the row is re-armed. A rising `uncertain` rate is a stuck
// DB-vs-chain divergence worth alerting on, and on `presend` it also means rows
// are waiting out the escalation window. `journal_unavailable` is the same wait
// for a row whose signature journal could not be read at all.
counter_vec!(
    OPERATOR_RELEASE_VERIFY,
    "private_channel_operator_release_verify_total",
    "Release-side on-chain confirmation verdicts",
    &["site", "verdict"]
);

// A sender signed a remint but lost the pre-send claim on its transaction, which
// is only possible if a second sender is running against the same database. Zero
// under correct single-sender operation, so any increment is proof the sender's
// advisory lock was lost and is the detection mechanism for that whole class of
// problem. Alert-routed as critical.
counter_vec!(
    OPERATOR_REMINT_CLAIM_LOST,
    "private_channel_operator_remint_claim_lost_total",
    "Remint broadcasts abandoned because another sender owned the claim",
    &["program_type"]
);

// The sender could not prove it still owns its advisory lock, so it cancelled
// the whole operator. `reason` is one of {not_held, probe_error, probe_timeout,
// fenced_write}: `not_held` is a successful probe that proved the lock is gone,
// `probe_error` and `probe_timeout` are a heartbeat probe that failed or hung on
// the pinned session, and `fenced_write` is a sender-owned write that could not
// be proven to have run inside the lock's own session. Zero in steady state, so
// any increment means the singleton guarantee was broken.
counter_vec!(
    OPERATOR_SENDER_LOCK_LOST,
    "private_channel_operator_sender_lock_lost_total",
    "Sender shutdowns triggered by unprovable advisory-lock ownership",
    &["program_type", "reason"]
);

// Absence-based finality classification: how a null status past blockhash
// validity resolved once the ledger-floor retention proof ran. `chain` is one of
// {channel, solana}; `outcome` is one of {dead, uncertain}. Sized before and after
// deploy to see how much of the newly-reachable channel `dead` population is real.
counter_vec!(
    OPERATOR_ABSENCE_CLASSIFY,
    "private_channel_operator_absence_classify_total",
    "Absence-based finality verdicts after the ledger-floor retention proof",
    &["chain", "outcome"]
);

pub fn init_labels(program_type: &str) {
    INDEXER_MINTS_SAVED.with_label_values(&[program_type]);
    INDEXER_TRANSACTIONS_SAVED.with_label_values(&[program_type]);
    INDEXER_SLOT_SAVE_ERRORS.with_label_values(&[program_type]);
    INDEXER_SLOTS_PROCESSED.with_label_values(&[program_type]);
    INDEXER_DATASOURCE_RECONNECTS.with_label_values(&[program_type]);

    INDEXER_CURRENT_SLOT.with_label_values(&[program_type]);
    INDEXER_CHAIN_TIP_SLOT.with_label_values(&[program_type]);
    INDEXER_BACKFILL_SLOTS_REMAINING.with_label_values(&[program_type]);
    INDEXER_CHECKPOINT_FRONTIER_LAG.with_label_values(&[program_type]);
    INDEXER_SLOT_PROCESSING_DURATION.with_label_values(&[program_type]);

    for error_type in &[
        "stream",
        "get_slots",
        "get_block",
        "missing_meta",
        "block_unavailable",
        "gap_fill",
        "missing_anchor",
    ] {
        INDEXER_RPC_ERRORS.with_label_values(&[program_type, error_type]);
    }

    OPERATOR_TRANSACTIONS_FETCHED.with_label_values(&[program_type]);
    OPERATOR_MINTS_SENT.with_label_values(&[program_type]);
    OPERATOR_DB_UPDATE_ERRORS.with_label_values(&[program_type]);
    OPERATOR_REMINT_CLAIM_LOST.with_label_values(&[program_type]);

    for status in &["Pending", "Processing", "Completed", "Failed"] {
        OPERATOR_DB_UPDATES.with_label_values(&[program_type, status]);
    }

    // Only Completed is ever counted here. Seeding the other statuses would
    // publish series that are pinned at zero by construction, which reads on
    // a dashboard as "no skips happened" rather than "never measured".
    OPERATOR_DB_UPDATE_SKIPPED.with_label_values(&[program_type, "Completed"]);

    for result in &["success", "failure", "error"] {
        OPERATOR_RPC_SEND_DURATION.with_label_values(&[program_type, result]);
    }

    for error_reason in &[
        "build_error",
        "max_retries_exceeded",
        "rpc_send_error",
        "nonce_already_used",
        "nonce_outside_generation",
        "bitmap_unavailable",
        "rotation_already_landed",
        "rotation_rearmed",
        "rotation_lost",
        "rotation_blocked_by_lower_nonce",
        "rotation_origination_read_failed",
        "bitmap_divergence",
        "mint_not_initialized",
        "confirmation_timeout_non_idempotent",
        "confirmation_timeout",
        "program_error",
        "confirmation_error",
        "deposit_ownership_lost",
        "release_claim_lost",
        "release_missing_claim_lease",
        "jit_missing_claim_lease",
        "malformed_status_response",
        "status_poll_rpc_error",
    ] {
        OPERATOR_TRANSACTION_ERRORS.with_label_values(&[program_type, error_reason]);
    }

    OPERATOR_BACKLOG_DEPTH.with_label_values(&[program_type]);
    FEEPAYER_BALANCE_LAMPORTS.with_label_values(&[program_type]);

    // Quarantine reasons must match the string constants returned by
    // `classify_processor_error` in processor.rs - any mismatch is a dead
    // label (visible in Prometheus, never incremented).
    for reason in &[
        "invalid_pubkey",
        "invalid_builder",
        "program_error",
        "mint_not_allowed",
    ] {
        OPERATOR_TRANSACTION_QUARANTINED.with_label_values(&[program_type, reason]);
    }

    for reason in &BAIL_REASONS {
        OPERATOR_TRANSACTION_QUARANTINED.with_label_values(&[program_type, reason]);
    }

    for outcome in &[
        "completed",
        "complete_raced",
        "complete_write_failed",
        "deferred_live",
        "deferred_unverifiable",
    ] {
        OPERATOR_REOPENED_DEPOSIT_GATE.with_label_values(&[program_type, outcome]);
    }

    for task in &[
        "fetcher",
        "processor",
        "sender",
        "storage_writer",
        "reconciliation",
        "feepayer_monitor",
        "recovery",
    ] {
        OPERATOR_TASK_EXIT.with_label_values(&[program_type, task]);
    }

    // Pre-register every (outcome, type) combination so dashboards see the
    // full label space immediately rather than only after the first hit.
    for outcome in &["completed", "requeued", "quarantined"] {
        for txn_type in &["deposit", "withdrawal"] {
            OPERATOR_STALE_PROCESSING_RECOVERED.with_label_values(&[
                program_type,
                outcome,
                txn_type,
            ]);
        }
    }

    // Stalled-row clears exist only on the withdraw side; pairing them with
    // `deposit` would register a series that can never increment.
    for outcome in &["manual_review_cleared", "pending_remint_cleared"] {
        OPERATOR_STALE_PROCESSING_RECOVERED.with_label_values(&[
            program_type,
            outcome,
            "withdrawal",
        ]);
    }

    // Release-verify gate labels are program-independent (site, verdict); the
    // idempotent pre-registration is harmless across repeated init_labels calls.
    for site in &["recovery", "remint", "presend"] {
        for verdict in &["landed", "not_landed", "uncertain"] {
            OPERATOR_RELEASE_VERIFY.with_label_values(&[site, verdict]);
        }
    }
    // Only presend reads the journal before proving anything, so this pair is
    // registered on its own rather than widening the grid with dead series.
    OPERATOR_RELEASE_VERIFY.with_label_values(&["presend", "journal_unavailable"]);

    // Pre-register every reason so the alert query sees a zero series, not nothing.
    for reason in &["not_held", "probe_error", "probe_timeout", "fenced_write"] {
        OPERATOR_SENDER_LOCK_LOST.with_label_values(&[program_type, reason]);
    }

    // Chain/outcome labels are program-independent; both roles classify both chains.
    for chain in &["channel", "solana"] {
        for outcome in &["dead", "uncertain"] {
            OPERATOR_ABSENCE_CLASSIFY.with_label_values(&[chain, outcome]);
        }
    }
}

pub fn init() {
    private_channel_metrics::init_metrics!(
        INDEXER_SLOTS_PROCESSED,
        INDEXER_TRANSACTIONS_SAVED,
        INDEXER_MINTS_SAVED,
        INDEXER_SLOT_SAVE_ERRORS,
        INDEXER_CURRENT_SLOT,
        INDEXER_RPC_ERRORS,
        INDEXER_CHAIN_TIP_SLOT,
        INDEXER_BACKFILL_SLOTS_REMAINING,
        INDEXER_CHECKPOINT_FRONTIER_LAG,
        INDEXER_DATASOURCE_RECONNECTS,
        INDEXER_SLOT_PROCESSING_DURATION,
        OPERATOR_TRANSACTIONS_FETCHED,
        OPERATOR_DB_UPDATES,
        OPERATOR_DB_UPDATE_SKIPPED,
        OPERATOR_DB_UPDATE_ERRORS,
        OPERATOR_RPC_SEND_DURATION,
        OPERATOR_TRANSACTION_ERRORS,
        OPERATOR_MINTS_SENT,
        OPERATOR_BACKLOG_DEPTH,
        FEEPAYER_BALANCE_LAMPORTS,
        OPERATOR_TRANSACTION_QUARANTINED,
        OPERATOR_TASK_EXIT,
        OPERATOR_STALE_PROCESSING_RECOVERED,
        OPERATOR_REOPENED_DEPOSIT_GATE,
        OPERATOR_RELEASE_VERIFY,
        OPERATOR_ABSENCE_CLASSIFY,
        OPERATOR_REMINT_CLAIM_LOST,
        OPERATOR_SENDER_LOCK_LOST,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use private_channel_metrics::prometheus;
    use prometheus::proto::MetricFamily;

    fn find_family(name: &str) -> MetricFamily {
        prometheus::gather()
            .into_iter()
            .find(|family| family.name() == name)
            .unwrap_or_else(|| panic!("metric family not found: {}", name))
    }

    fn metric_with_labels(family: &MetricFamily, labels: &[(&str, &str)]) -> bool {
        family.get_metric().iter().any(|metric| {
            labels.iter().all(|(name, value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == *name && label.value() == *value)
            })
        })
    }

    #[test]
    fn init_labels_registers_single_label_series() {
        let program_type = "test_program_single_label";

        init_labels(program_type);

        let single_label_metrics = [
            "private_channel_indexer_mints_saved_total",
            "private_channel_indexer_transactions_saved_total",
            "private_channel_indexer_slot_save_errors_total",
            "private_channel_indexer_slots_processed_total",
            "private_channel_indexer_datasource_reconnects_total",
            "private_channel_indexer_current_slot",
            "private_channel_indexer_chain_tip_slot",
            "private_channel_indexer_backfill_slots_remaining",
            "private_channel_indexer_checkpoint_frontier_lag",
            "private_channel_indexer_slot_processing_duration_seconds",
            "private_channel_operator_transactions_fetched_total",
            "private_channel_operator_db_update_errors_total",
            "private_channel_operator_mints_sent_total",
            "private_channel_operator_backlog_depth",
            "private_channel_feepayer_balance_lamports",
            "private_channel_operator_remint_claim_lost_total",
        ];

        for name in single_label_metrics {
            let family = find_family(name);
            assert!(
                metric_with_labels(&family, &[("program_type", program_type)]),
                "missing program_type label for {}",
                name
            );
        }
    }

    #[test]
    fn init_registers_metric_families() {
        init();
        init_labels("default");

        let names = [
            "private_channel_indexer_slots_processed_total",
            "private_channel_indexer_transactions_saved_total",
            "private_channel_indexer_mints_saved_total",
            "private_channel_indexer_slot_save_errors_total",
            "private_channel_indexer_current_slot",
            "private_channel_indexer_rpc_errors_total",
            "private_channel_indexer_chain_tip_slot",
            "private_channel_indexer_backfill_slots_remaining",
            "private_channel_indexer_checkpoint_frontier_lag",
            "private_channel_indexer_datasource_reconnects_total",
            "private_channel_indexer_slot_processing_duration_seconds",
            "private_channel_operator_transactions_fetched_total",
            "private_channel_operator_db_updates_total",
            "private_channel_operator_db_update_errors_total",
            "private_channel_operator_rpc_send_duration_seconds",
            "private_channel_operator_transaction_errors_total",
            "private_channel_operator_mints_sent_total",
            "private_channel_operator_backlog_depth",
            "private_channel_feepayer_balance_lamports",
            "private_channel_operator_transaction_quarantined_total",
            "private_channel_operator_task_exit_total",
            "private_channel_operator_stale_processing_recovered_total",
            "private_channel_operator_reopened_deposit_gate_total",
            "private_channel_operator_remint_claim_lost_total",
            "private_channel_operator_sender_lock_lost_total",
        ];

        let families = prometheus::gather();
        for name in names {
            assert!(
                families.iter().any(|family| family.name() == name),
                "metric family missing after init: {}",
                name
            );
        }
    }
}
