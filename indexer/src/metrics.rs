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
counter_vec!(
    OPERATOR_STALE_PROCESSING_RECOVERED,
    "private_channel_operator_stale_processing_recovered_total",
    "Stale Processing rows healed by the recovery worker",
    &["program_type", "outcome", "type"]
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

    for error_type in &["stream", "get_slots", "get_block"] {
        INDEXER_RPC_ERRORS.with_label_values(&[program_type, error_type]);
    }

    OPERATOR_TRANSACTIONS_FETCHED.with_label_values(&[program_type]);
    OPERATOR_MINTS_SENT.with_label_values(&[program_type]);
    OPERATOR_DB_UPDATE_ERRORS.with_label_values(&[program_type]);

    for status in &["Pending", "Processing", "Completed", "Failed"] {
        OPERATOR_DB_UPDATES.with_label_values(&[program_type, status]);
    }

    for result in &["success", "failure", "error"] {
        OPERATOR_RPC_SEND_DURATION.with_label_values(&[program_type, result]);
    }

    for error_reason in &[
        "build_error",
        "max_retries_exceeded",
        "rpc_send_error",
        "invalid_smt_proof",
        "invalid_nonce_for_tree_index",
        "mint_not_initialized",
        "confirmation_timeout_non_idempotent",
        "confirmation_timeout",
        "program_error",
        "confirmation_error",
    ] {
        OPERATOR_TRANSACTION_ERRORS.with_label_values(&[program_type, error_reason]);
    }

    OPERATOR_BACKLOG_DEPTH.with_label_values(&[program_type]);
    FEEPAYER_BALANCE_LAMPORTS.with_label_values(&[program_type]);

    // Quarantine reasons must match the string constants returned by
    // `classify_processor_error` in processor.rs — any mismatch is a dead
    // label (visible in Prometheus, never incremented).
    for reason in &["invalid_pubkey", "invalid_builder", "program_error"] {
        OPERATOR_TRANSACTION_QUARANTINED.with_label_values(&[program_type, reason]);
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
        OPERATOR_DB_UPDATE_ERRORS,
        OPERATOR_RPC_SEND_DURATION,
        OPERATOR_TRANSACTION_ERRORS,
        OPERATOR_MINTS_SENT,
        OPERATOR_BACKLOG_DEPTH,
        FEEPAYER_BALANCE_LAMPORTS,
        OPERATOR_TRANSACTION_QUARANTINED,
        OPERATOR_TASK_EXIT,
        OPERATOR_STALE_PROCESSING_RECOVERED,
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
