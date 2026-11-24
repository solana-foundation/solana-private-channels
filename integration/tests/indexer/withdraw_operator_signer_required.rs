//! The withdraw operator signs every release, so it refuses to start without its own
//! operator signer. The escrow operator never signs as operator and starts with only the
//! admin. Its own binary: env is process-global and the installed signers are first-wins.

use {
    private_channel_indexer::{
        config::{OperatorConfig, PostgresConfig, ProgramType, StorageType},
        error::OperatorError,
        operator,
        storage::{common::storage::mock::MockStorage, Storage},
        PrivateChannelIndexerConfig,
    },
    solana_sdk::{pubkey::Pubkey, signature::Keypair},
    std::{
        sync::{Arc, Once},
        time::Duration,
    },
};

/// Admin only, with no `OPERATOR_*` var. Both tests use the same env, so they cannot race.
fn admin_only_env() {
    static ENV: Once = Once::new();
    ENV.call_once(|| {
        let admin = bs58::encode(Keypair::new().to_bytes()).into_string();
        std::env::set_var("ADMIN_SIGNER", "memory");
        std::env::set_var("ADMIN_PRIVATE_KEY", &admin);
        std::env::set_var(
            "OPERATOR_PERMANENT_DELEGATE",
            Pubkey::new_unique().to_string(),
        );
        for var in [
            "OPERATOR_SIGNER",
            "OPERATOR_PRIVATE_KEY",
            "OPERATOR_VAULT_ADDR",
            "OPERATOR_TURNKEY_API_PUBLIC_KEY",
            "OPERATOR_PRIVY_APP_ID",
        ] {
            std::env::remove_var(var);
        }
    });
}

fn configs(program_type: ProgramType) -> (PrivateChannelIndexerConfig, OperatorConfig) {
    let common = PrivateChannelIndexerConfig {
        program_type,
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
    (common, config)
}

/// Runs the operator for at most `secs`; `None` when it was still running.
async fn run_for(
    program_type: ProgramType,
    secs: u64,
) -> (Option<Result<(), OperatorError>>, Vec<String>) {
    admin_only_env();
    let (common, config) = configs(program_type);
    let mock = MockStorage::new();
    let result = tokio::time::timeout(
        Duration::from_secs(secs),
        operator::run(Arc::new(Storage::Mock(mock.clone())), common, config, None),
    )
    .await
    .ok();
    let calls = mock.call_order.lock().unwrap().clone();
    (result, calls)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn withdraw_operator_without_operator_signer_refuses_at_startup() {
    let (result, calls) = run_for(ProgramType::Withdraw, 10).await;
    let result = result.expect("a withdraw operator with no release key must refuse promptly");
    assert!(
        matches!(&result, Err(OperatorError::InvalidConfig(msg)) if msg.contains("OPERATOR_SIGNER")),
        "must refuse with a config error naming OPERATOR_SIGNER, got: {result:?}"
    );
    assert!(
        calls.is_empty(),
        "refusal must come before storage, got calls: {calls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escrow_operator_with_only_admin_signer_starts() {
    let (result, calls) = run_for(ProgramType::Escrow, 3).await;
    if let Some(Err(OperatorError::InvalidConfig(msg))) = &result {
        assert!(
            !msg.contains("OPERATOR_SIGNER") && !msg.contains("signer"),
            "the escrow operator needs no operator signer: {msg}"
        );
    }
    assert!(
        !calls.is_empty(),
        "the escrow operator must get past the signers to storage: {result:?}"
    );
}
