//! An operator whose configured operator signer will not load must refuse to start with a
//! config error, never fall back to the admin key. Its own binary: env is process-global
//! and the installed signers are first-wins, so no other test may load them first.

use {
    private_channel_indexer::{
        config::{OperatorConfig, PostgresConfig, ProgramType, StorageType},
        error::OperatorError,
        operator,
        storage::{common::storage::mock::MockStorage, Storage},
        PrivateChannelIndexerConfig,
    },
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    std::{sync::Arc, time::Duration},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_refuses_to_start_when_the_operator_signer_will_not_load() {
    let admin = bs58::encode(Keypair::new().to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &admin);
    std::env::set_var("OPERATOR_SIGNER", "memory");
    std::env::remove_var("OPERATOR_PRIVATE_KEY");

    let mock = MockStorage::new();
    let common = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: "http://127.0.0.1:1".to_string(),
        fallback_rpc_url: None,
        source_rpc_url: Some("http://127.0.0.1:1".to_string()),
        postgres: PostgresConfig {
            database_url: String::new(),
            max_connections: 1,
        },
        escrow_instance_id: Some(Pubkey::new_unique()),
    };
    let config = OperatorConfig {
        db_poll_interval: Duration::from_millis(50),
        batch_size: 10,
        retry_max_attempts: 1,
        retry_base_delay: Duration::from_millis(100),
        channel_buffer_size: 10,
        rpc_commitment: solana_commitment_config::CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(300),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: Some("http://127.0.0.1:1/hook".to_string()),
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    };

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        operator::run(Arc::new(Storage::Mock(mock.clone())), common, config, None),
    )
    .await
    .expect("a broken operator signer must refuse promptly");

    assert!(
        matches!(&result, Err(OperatorError::InvalidConfig(msg))
            if msg.contains("operator signer") && msg.contains("OPERATOR_PRIVATE_KEY not set")),
        "must refuse with a config error naming the operator key, got: {result:?}"
    );
    let calls = mock.call_order.lock().unwrap().clone();
    assert!(
        !calls.iter().any(|c| c == "init_schema"),
        "refusal must come before the lock and the schema, got calls: {calls:?}"
    );
}
