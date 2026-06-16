use {
    private_channel_indexer::{
        config::{
            OperatorConfig, PostgresConfig, PrivateChannelIndexerConfig, ProgramType, StorageType,
        },
        operator,
        storage::{PostgresDb, Storage},
    },
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Keypair},
    std::{sync::Arc, time::Duration},
    tokio::task::JoinHandle,
};

#[cfg(feature = "test-mock-storage")]
use crate::mock_rpc::MockRpcServer;
#[cfg(feature = "test-mock-storage")]
use private_channel_indexer::storage::common::storage::mock::MockStorage;

pub struct OperatorHandle {
    pub _handle: JoinHandle<()>,
}

impl OperatorHandle {
    pub async fn shutdown(self) {
        drop(self._handle);
    }
}

fn default_operator_config() -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_millis(500),
        batch_size: 10,
        retry_max_attempts: 3,
        retry_base_delay: Duration::from_secs(1),
        channel_buffer_size: 100,
        rpc_commitment: CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        reconciliation_interval: Duration::from_secs(5 * 60),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: None,
        feepayer_monitor_interval: Duration::from_secs(60),
        confirmation_poll_interval_ms: 400,
    }
}

fn set_operator_env_vars(keypair: &Keypair) {
    let private_key_base58 = bs58::encode(keypair.to_bytes()).into_string();
    std::env::set_var("ADMIN_SIGNER", "memory");
    std::env::set_var("ADMIN_PRIVATE_KEY", &private_key_base58);
    std::env::set_var("OPERATOR_SIGNER", "memory");
    std::env::set_var("OPERATOR_PRIVATE_KEY", &private_key_base58);
}

/// Start the operator that reads from Solana indexer and mints tokens on PrivateChannel.
pub async fn start_solana_to_private_channel_operator(
    private_channel_rpc_url: String,
    solana_indexer_db_url: String,
    operator_keypair: Keypair,
    escrow_instance_id: Pubkey,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: solana_indexer_db_url,
        max_connections: 10,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: private_channel_rpc_url,
        source_rpc_url: None,
        postgres: postgres_config,
        escrow_instance_id: Some(escrow_instance_id),
    };

    let operator_config = default_operator_config();

    set_operator_env_vars(&operator_keypair);

    let task_handle = tokio::spawn(async move {
        if let Err(e) = operator::run(storage, common_config, operator_config, None).await {
            tracing::error!("Operator error: {}", e);
        }
    });

    Ok(OperatorHandle {
        _handle: task_handle,
    })
}

// ── Mock harness (no Postgres, no validator) ────────────────────────────────
//
// These helpers are for integration tests that drive the operator against a
// scripted `MockRpcServer` URL and an in-memory `Storage::Mock`. They let a
// test script specific RPC failure modes (transient -32000, permanent program
// errors, `getSignatureStatuses` poll storms) and/or storage failure modes
// (`set_should_fail("update_transaction_status", true)`) and assert the
// operator's recovery behavior — no container and no validator required.
//
// The URL-pointer approach is sufficient: `operator::run` builds its own
// `RpcClientWithRetry` from `common_config.rpc_url`, so pointing that at
// `mock.url()` is all we need. No `test_hooks::run_with_injected_clients`
// escape hatch was necessary.
//
// RPC methods the mock should be prepared to service for a full lifecycle
// (tests stub whichever subset their assertion touches):
//   - getLatestBlockhash        — every sign_and_send_transaction pass
//   - sendTransaction           — submission
//   - getSignatureStatuses      — poll-task confirmation loop
//   - getSignaturesForAddress   — mint idempotency check
//   - getAccountInfo            — mint initialization / JIT mint path
//   - getBalance                — feepayer_monitor (escrow operators only)
// Unscripted methods return JSON-RPC error -32603 ("no scripted response"),
// which production paths treat as transient — usually harmless for tests
// that care about a narrow failure mode and just want the rest of the
// operator to idle.
#[cfg(feature = "test-mock-storage")]
pub struct OperatorMockHarness {
    /// Cloned handle to the in-memory storage backing the operator. Use this
    /// for both fault injection (`set_should_fail`) and direct seeding
    /// (`pending_transactions.lock().unwrap().push(...)`).
    pub storage: MockStorage,
    /// Scripted JSON-RPC endpoint. Every RPC call the operator makes hits
    /// this server. Use `enqueue` / `enqueue_sequence` to script replies
    /// and `call_count` / `call_timestamps` to assert interactions.
    pub rpc: MockRpcServer,
    /// Handle to the spawned `operator::run` task — same type the Postgres
    /// helpers return. Dropping the handle or calling `shutdown` aborts
    /// the operator on the next await point.
    pub handle: OperatorHandle,
}

#[cfg(feature = "test-mock-storage")]
impl OperatorMockHarness {
    /// Shut down the operator task and the mock RPC server.
    pub async fn shutdown(self) {
        self.handle.shutdown().await;
        self.rpc.shutdown().await;
    }
}

/// Operator config tuned for mock-harness runs: fast poll intervals so tests
/// don't wait seconds between ticks, same other knobs as `default_operator_config`.
#[cfg(feature = "test-mock-storage")]
fn mock_operator_config() -> OperatorConfig {
    OperatorConfig {
        db_poll_interval: Duration::from_millis(100),
        batch_size: 10,
        retry_max_attempts: 3,
        retry_base_delay: Duration::from_millis(50),
        channel_buffer_size: 100,
        rpc_commitment: CommitmentLevel::Confirmed,
        alert_webhook_url: None,
        // Long reconciliation interval — a mock-harness test rarely wants it
        // to fire. Tests that do can script the relevant RPC replies.
        reconciliation_interval: Duration::from_secs(60 * 60),
        reconciliation_tolerance_bps: 10,
        reconciliation_webhook_url: None,
        // Long feepayer monitor interval so the test isn't racing against
        // unrelated `getBalance` traffic. The first tick still happens at
        // start; tests that need it stubbed should enqueue a reply.
        feepayer_monitor_interval: Duration::from_secs(60 * 60),
        confirmation_poll_interval_ms: 100,
    }
}

/// Start the operator that reads from PrivateChannel indexer and releases funds on Solana,
/// wired against a scripted `MockRpcServer` + in-memory `Storage::Mock`. Program
/// type is `Withdraw`.
#[cfg(feature = "test-mock-storage")]
pub async fn start_private_channel_to_solana_operator_with_mocks(
    escrow_instance_id: Pubkey,
    operator_keypair: Keypair,
) -> Result<OperatorMockHarness, Box<dyn std::error::Error>> {
    let rpc = MockRpcServer::start().await;
    let mock_storage = MockStorage::new();
    let storage = Arc::new(Storage::Mock(mock_storage.clone()));

    // `database_url` is never hit because we're on `Storage::Mock`; keep a
    // placeholder that wouldn't parse as a real URL to make misuse obvious.
    let postgres_config = PostgresConfig {
        database_url: "mock://unused".to_string(),
        max_connections: 1,
    };

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres, // no Mock variant on StorageType enum
        rpc_url: rpc.url(),
        // Withdraw operator requires a source chain for remints; the mock harness
        // is single-server, so point it at the same scripted RPC.
        source_rpc_url: Some(rpc.url()),
        postgres: postgres_config,
        escrow_instance_id: Some(escrow_instance_id),
    };

    let operator_config = mock_operator_config();

    set_operator_env_vars(&operator_keypair);

    let run_storage = storage.clone();
    let run_config = common_config.clone();
    let run_operator_config = operator_config.clone();
    let task_handle = tokio::spawn(async move {
        if let Err(e) = operator::run(run_storage, run_config, run_operator_config, None).await {
            tracing::error!("Operator (mock harness) error: {}", e);
        }
    });

    Ok(OperatorMockHarness {
        storage: mock_storage,
        rpc,
        handle: OperatorHandle {
            _handle: task_handle,
        },
    })
}

/// Start the operator that reads from Solana indexer and mints on PrivateChannel,
/// wired against a scripted `MockRpcServer` + in-memory `Storage::Mock`. Program
/// type is `Escrow`.
#[cfg(feature = "test-mock-storage")]
pub async fn start_solana_to_private_channel_operator_with_mocks(
    escrow_instance_id: Pubkey,
    operator_keypair: Keypair,
) -> Result<OperatorMockHarness, Box<dyn std::error::Error>> {
    let rpc = MockRpcServer::start().await;
    let mock_storage = MockStorage::new();
    let storage = Arc::new(Storage::Mock(mock_storage.clone()));

    let postgres_config = PostgresConfig {
        database_url: "mock://unused".to_string(),
        max_connections: 1,
    };

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Escrow,
        storage_type: StorageType::Postgres,
        rpc_url: rpc.url(),
        source_rpc_url: None,
        postgres: postgres_config,
        escrow_instance_id: Some(escrow_instance_id),
    };

    let operator_config = mock_operator_config();

    set_operator_env_vars(&operator_keypair);

    let run_storage = storage.clone();
    let run_config = common_config.clone();
    let run_operator_config = operator_config.clone();
    let task_handle = tokio::spawn(async move {
        if let Err(e) = operator::run(run_storage, run_config, run_operator_config, None).await {
            tracing::error!("Operator (mock harness) error: {}", e);
        }
    });

    Ok(OperatorMockHarness {
        storage: mock_storage,
        rpc,
        handle: OperatorHandle {
            _handle: task_handle,
        },
    })
}

/// Start the operator that reads from PrivateChannel indexer and releases funds on Solana.
pub async fn start_private_channel_to_solana_operator(
    solana_rpc_url: String,
    private_channel_rpc_url: String,
    private_channel_indexer_db_url: String,
    operator_keypair: Keypair,
    escrow_instance_id: Pubkey,
) -> Result<OperatorHandle, Box<dyn std::error::Error>> {
    let postgres_config = PostgresConfig {
        database_url: private_channel_indexer_db_url,
        max_connections: 10,
    };

    let storage = Arc::new(Storage::Postgres(PostgresDb::new(&postgres_config).await?));

    let common_config = PrivateChannelIndexerConfig {
        program_type: ProgramType::Withdraw,
        storage_type: StorageType::Postgres,
        rpc_url: solana_rpc_url,
        // Source chain (PrivateChannel), where the burn happened and remints land.
        source_rpc_url: Some(private_channel_rpc_url),
        postgres: postgres_config,
        escrow_instance_id: Some(escrow_instance_id),
    };

    let operator_config = default_operator_config();

    set_operator_env_vars(&operator_keypair);

    let task_handle = tokio::spawn(async move {
        if let Err(e) = operator::run(storage, common_config, operator_config, None).await {
            tracing::error!("Operator error: {}", e);
        }
    });

    Ok(OperatorHandle {
        _handle: task_handle,
    })
}
