//! Bootstrap-time validation for `private_channel_indexer::indexer::run`.
//!
//! Covers the misconfiguration branch in
//! `private_channel_indexer::indexer::run`: when `program_type = Escrow` but no
//! `escrow_instance_id` is set, startup reconciliation has nothing to
//! anchor against, so `run` must bail with an `InvalidPubkey` error
//! (wrapped in `IndexerError::Reconciliation`).
//!
//! The existing `PrivateChannelIndexerConfig::validate()` method would also catch
//! this mismatch, but the `run` fast-path guard matters on its own because
//! production TOML load skips `validate()` if the user bypasses it in
//! custom bootstrapping. This test exercises the in-line guard specifically.

use {
    private_channel_indexer::{
        config::{BackfillConfig, ReconciliationConfig},
        error::{IndexerError, ReconciliationError},
        indexer::run,
        DatasourceType, IndexerConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType,
        StorageType,
    },
    testcontainers::runners::AsyncRunner,
    testcontainers_modules::postgres::Postgres,
};

#[tokio::test(flavor = "multi_thread")]
async fn run_rejects_escrow_without_instance_id() {
    let pg_container = Postgres::default()
        .with_db_name("bootstrap_validation")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container must start");
    let pg_host = pg_container.get_host().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!(
        "postgres://postgres:password@{}:{}/bootstrap_validation",
        pg_host, pg_port
    );

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: db_url,
            max_connections: 2,
        },
        // Deliberately missing: this is the invariant under test.
        escrow_instance_id: None,
    };

    let indexer_config = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: None,
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: false,
            exit_after_backfill: false,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 100,
            max_gap_slots: 1_000,
            start_slot: None,
        },
        reconciliation: ReconciliationConfig::default(),
    };

    let result = run(common_config, indexer_config, None).await;

    let err = result.expect_err(
        "run() must reject Escrow program_type with no escrow_instance_id before touching the datasource",
    );
    match err {
        IndexerError::Reconciliation(ReconciliationError::InvalidPubkey { pubkey, reason }) => {
            assert_eq!(pubkey, "<missing>");
            assert!(
                reason.contains("escrow_instance_id"),
                "reason must mention the missing config field, got: {reason}"
            );
        }
        other => panic!(
            "expected IndexerError::Reconciliation(InvalidPubkey{{..}}) for missing escrow_instance_id, got: {other:?}"
        ),
    }
}

/// The same guard has to hold in backfill-only mode, which skips startup reconciliation.
///
/// Without an instance id the processor drops every escrow instruction as out of scope,
/// so a repair would store nothing yet still advance the checkpoint over the range it
/// claimed to fix. Refusing to start is what keeps that range repairable.
#[tokio::test(flavor = "multi_thread")]
async fn run_rejects_escrow_without_instance_id_in_backfill_only() {
    let pg_container = Postgres::default()
        .with_db_name("bootstrap_validation_backfill_only")
        .with_user("postgres")
        .with_password("password")
        .start()
        .await
        .expect("postgres container must start");
    let pg_host = pg_container.get_host().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!(
        "postgres://postgres:password@{}:{}/bootstrap_validation_backfill_only",
        pg_host, pg_port
    );

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: "http://127.0.0.1:1".to_string(),
        source_rpc_url: None,
        fallback_rpc_url: None,
        postgres: PostgresConfig {
            database_url: db_url.clone(),
            max_connections: 2,
        },
        // Deliberately missing: this is the invariant under test.
        escrow_instance_id: None,
    };

    let indexer_config = IndexerConfig {
        datasource_type: DatasourceType::RpcPolling,
        rpc_polling: None,
        yellowstone: None,
        backfill: BackfillConfig {
            enabled: true,
            exit_after_backfill: true,
            rpc_url: "http://127.0.0.1:1".to_string(),
            batch_size: 100,
            max_gap_slots: 1_000,
            start_slot: None,
        },
        reconciliation: ReconciliationConfig::default(),
    };

    let result = run(common_config, indexer_config, None).await;

    let err = result.expect_err(
        "backfill-only mode must reject Escrow program_type with no escrow_instance_id",
    );
    match err {
        IndexerError::Reconciliation(ReconciliationError::InvalidPubkey { pubkey, reason }) => {
            assert_eq!(pubkey, "<missing>");
            assert!(
                reason.contains("escrow_instance_id"),
                "reason must mention the missing config field, got: {reason}"
            );
        }
        other => panic!(
            "expected IndexerError::Reconciliation(InvalidPubkey{{..}}) in backfill-only mode, got: {other:?}"
        ),
    }

    // The guard fires before the pipeline exists, so nothing can have been checkpointed.
    let pool = sqlx::postgres::PgPool::connect(&db_url)
        .await
        .expect("assertion pool must connect");
    let checkpoint: Option<(i64,)> =
        sqlx::query_as("SELECT last_committed_slot FROM indexer_state WHERE program_type = $1")
            .bind("escrow")
            .fetch_optional(&pool)
            .await
            .expect("checkpoint lookup must succeed");
    assert!(
        checkpoint.is_none(),
        "a refused start must leave no checkpoint row behind, got {checkpoint:?}"
    );
}
