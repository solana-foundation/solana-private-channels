use std::str::FromStr;

use serde::{Deserialize, Serialize};
use solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey};
use solana_transaction_status::UiTransactionEncoding;

use crate::indexer::datasource::common::parser::{
    PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
};
use crate::operator::SignerUtil;

/// Program type to index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ProgramType {
    /// PrivateChannel Escrow Program
    Escrow,
    /// PrivateChannel Withdraw Program
    Withdraw,
}

impl private_channel_metrics::MetricLabel for ProgramType {
    fn as_label(&self) -> &'static str {
        match self {
            ProgramType::Escrow => "escrow",
            ProgramType::Withdraw => "withdraw",
        }
    }
}

impl ProgramType {
    pub fn to_pubkey(&self) -> Pubkey {
        match self {
            ProgramType::Escrow => {
                Pubkey::from_str(PRIVATE_CHANNEL_ESCROW_PROGRAM_ID).expect("Invalid program ID")
            }
            ProgramType::Withdraw => {
                Pubkey::from_str(PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID).expect("Invalid program ID")
            }
        }
    }
}

/// Storage backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    /// PostgreSQL database
    Postgres,
}

/// Postgres configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Database connection URL (for Postgres)
    pub database_url: String,
    /// Maximum number of connections to the database
    pub max_connections: u32,
}

/// Datasource type for fetching blockchain data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DatasourceType {
    /// RPC polling (getBlock)
    RpcPolling,
    /// Yellowstone gRPC streaming
    Yellowstone,
}

/// RPC polling specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPollingConfig {
    /// Starting slot
    pub from_slot: Option<u64>,
    /// Polling interval in milliseconds
    pub poll_interval_ms: u64,
    /// Error retry interval in milliseconds
    pub error_retry_interval_ms: u64,
    /// Batch size for processing blocks
    pub batch_size: usize,
    /// RPC encoding format for getBlock calls
    pub encoding: UiTransactionEncoding,
    /// RPC commitment level for getSlot calls
    pub commitment: CommitmentLevel,
}

/// Yellowstone gRPC specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YellowstoneConfig {
    /// Yellowstone gRPC endpoint URL
    pub endpoint: String,
    /// Token to use for authentication
    pub x_token: Option<String>,
    /// Commitment level: "processed", "confirmed", or "finalized"
    pub commitment: String,
}

/// Backfill configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillConfig {
    /// Enable automatic backfill on startup
    pub enabled: bool,
    /// Enable backfill only, exits after backfill
    pub exit_after_backfill: bool,
    /// RPC endpoint URL for backfill
    pub rpc_url: String,
    /// Batch size for backfill operations
    pub batch_size: usize,
    /// Max gap in slots before requiring manual intervention
    pub max_gap_slots: u64,
    /// Optional starting slot for backfill (inclusive, first slot to process)
    pub start_slot: Option<u64>,
}

/// Common configuration shared by both indexer and operator modes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateChannelIndexerConfig {
    /// Program to index
    pub program_type: ProgramType,
    /// Storage type
    pub storage_type: StorageType,
    /// RPC endpoint URL (destination chain for operators)
    pub rpc_url: String,
    /// Source chain RPC URL for cross-chain operators (optional)
    /// Used by escrow operator to read mint metadata from Solana
    /// while sending mint transactions to PrivateChannel via rpc_url
    pub source_rpc_url: Option<String>,
    /// Postgres configuration
    pub postgres: PostgresConfig,
    /// Instance ID to filter (required for Escrow program)
    pub escrow_instance_id: Option<Pubkey>,
}

impl PrivateChannelIndexerConfig {
    pub fn validate(&self) -> Result<(), String> {
        match (self.program_type, &self.escrow_instance_id) {
            (ProgramType::Escrow, None) => {
                Err("--escrow-instance-id required when program_type is Escrow".to_string())
            }
            (ProgramType::Withdraw, Some(_)) => {
                Err("--escrow-instance-id should not be set for Withdraw program".to_string())
            }
            _ => Ok(()),
        }
    }
}

/// Configuration for startup reconciliation against on-chain state.
///
/// Only applies when `program_type = escrow`. Skipped for `withdraw` indexers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReconciliationConfig {
    /// Maximum absolute mismatch (in raw token units) allowed before blocking startup.
    /// 0 (default) means any mismatch blocks startup.
    /// Mismatches above this value log error + emit alert and abort.
    /// Mismatches at or below this value (but > 0) log a warning and continue.
    ///
    /// There is a small race window between the DB balance query and the on-chain RPC
    /// fetch: a deposit arriving in that window will appear in the ATA but not yet in
    /// the DB, producing a transient false positive. If spurious failures occur in
    /// production, set this to the raw amount of one or two minimum deposits.
    pub mismatch_threshold_raw: u64,
}

/// Indexer-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    /// Datasource type
    pub datasource_type: DatasourceType,
    /// RPC polling config (if datasource is RpcPolling)
    pub rpc_polling: Option<RpcPollingConfig>,
    /// Yellowstone config (if datasource is Yellowstone)
    pub yellowstone: Option<YellowstoneConfig>,
    /// Backfill configuration for crash recovery
    pub backfill: BackfillConfig,
    /// Startup reconciliation configuration
    #[serde(default)]
    pub reconciliation: ReconciliationConfig,
}

impl IndexerConfig {
    /// Validate indexer-specific configuration
    pub fn validate(&self) -> Result<(), String> {
        match self.datasource_type {
            DatasourceType::RpcPolling => {
                #[cfg(not(feature = "datasource-rpc"))]
                return Err(
                    "RPC datasource not compiled (enable with: --features datasource-rpc)"
                        .to_string(),
                );

                #[cfg(feature = "datasource-rpc")]
                if self.rpc_polling.is_none() {
                    return Err("RPC polling config required for RpcPolling datasource".to_string());
                }
            }
            DatasourceType::Yellowstone => {
                #[cfg(not(feature = "datasource-yellowstone"))]
                return Err(
                    "Yellowstone datasource not compiled (enable with: --features datasource-yellowstone)"
                        .to_string(),
                );

                #[cfg(feature = "datasource-yellowstone")]
                if self.yellowstone.is_none() {
                    return Err(
                        "Yellowstone config required for Yellowstone datasource".to_string()
                    );
                }
            }
        }

        Ok(())
    }
}

/// Operator-specific configuration
///
/// # Signer Configuration (via Environment Variables)
///
/// Operators require signers configured via environment variables:
///
/// ## Required for all operators:
/// - `ADMIN_SIGNER`: Signer type (memory|vault|turnkey|privy)
/// - `ADMIN_PRIVATE_KEY`: Private key or key identifier
///
/// ## Optional (falls back to admin if not set):
/// - `OPERATOR_SIGNER`: Signer type for operator-specific operations
/// - `OPERATOR_PRIVATE_KEY`: Private key or key identifier for operator
///
/// ## Type-specific variables (required based on signer type):
///
/// ### Vault signers:
/// - `ADMIN_VAULT_ADDR`, `ADMIN_VAULT_TOKEN`, `ADMIN_PUBKEY`
/// - `OPERATOR_VAULT_ADDR`, `OPERATOR_VAULT_TOKEN`, `OPERATOR_PUBKEY`
///
/// ### Turnkey signers:
/// - `ADMIN_TURNKEY_API_PUBLIC_KEY`, `ADMIN_TURNKEY_API_PRIVATE_KEY`,
///   `ADMIN_TURNKEY_ORGANIZATION_ID`, `ADMIN_PUBKEY`
/// - `OPERATOR_TURNKEY_*` (same pattern)
///
/// ### Privy signers:
/// - `ADMIN_PRIVY_APP_ID`, `ADMIN_PRIVY_APP_SECRET`, `ADMIN_PRIVY_WALLET_ID`
/// - `OPERATOR_PRIVY_*` (same pattern)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorConfig {
    /// How often to poll the database for pending transactions
    pub db_poll_interval: std::time::Duration,
    /// Maximum number of transactions to fetch per batch
    pub batch_size: u16,
    /// Maximum number of retry attempts for failed transactions
    pub retry_max_attempts: u32,
    /// Base delay between retries (exponential backoff will apply)
    pub retry_base_delay: std::time::Duration,
    /// Size of channel buffers
    pub channel_buffer_size: usize,
    /// RPC commitment level for operator transactions
    pub rpc_commitment: CommitmentLevel,
    /// Webhook URL for alerting on failed transactions. Set via ALERT_WEBHOOK env var.
    pub alert_webhook_url: Option<String>,
    /// How often to run escrow balance reconciliation checks
    #[serde(default = "default_reconciliation_interval")]
    pub reconciliation_interval: std::time::Duration,
    /// Tolerance threshold in basis points (100 bps = 1%)
    #[serde(default = "default_reconciliation_tolerance")]
    pub reconciliation_tolerance_bps: u16,
    /// Webhook URL for reconciliation alerts (optional)
    pub reconciliation_webhook_url: Option<String>,
    /// How often to check the feepayer SOL balance (escrow operators only)
    #[serde(default = "default_feepayer_monitor_interval")]
    pub feepayer_monitor_interval: std::time::Duration,
    /// Milliseconds between `getSignatureStatuses` polls when confirming a sent transaction.
    /// Lower values reduce per-tx latency on PrivateChannel (~100 ms); higher values suit Solana
    /// (~400 ms block time). Defaults to `DEFAULT_CONFIRMATION_POLL_INTERVAL_MS`.
    #[serde(default = "default_confirmation_poll_interval_ms")]
    pub confirmation_poll_interval_ms: u64,
}

/// Default poll interval for `confirmation_poll_interval_ms`, matching Solana's ~400 ms block time.
/// operator-solana overrides this to 100 ms since PrivateChannel confirms faster.
pub const DEFAULT_CONFIRMATION_POLL_INTERVAL_MS: u64 = 400;

fn default_reconciliation_interval() -> std::time::Duration {
    std::time::Duration::from_secs(5 * 60) // 5 minutes
}

fn default_reconciliation_tolerance() -> u16 {
    10 // 10 basis points = 0.1%
}

fn default_feepayer_monitor_interval() -> std::time::Duration {
    std::time::Duration::from_secs(60)
}

fn default_confirmation_poll_interval_ms() -> u64 {
    DEFAULT_CONFIRMATION_POLL_INTERVAL_MS
}

impl OperatorConfig {
    /// Validate that required signers are configured
    ///
    /// This triggers lazy initialization of signers and will fail fast
    /// if required environment variables are missing or invalid.
    pub fn validate_signers() -> Result<(), String> {
        let _ = SignerUtil::get_admin_pubkey();

        let _ = SignerUtil::get_operator_pubkey();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Test Helper Functions
    // ============================================================================

    fn create_common_config() -> PrivateChannelIndexerConfig {
        use std::str::FromStr;
        PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            storage_type: StorageType::Postgres,
            rpc_url: "http://localhost:8899".to_string(),
            source_rpc_url: Some("http://localhost:8899".to_string()),
            postgres: PostgresConfig {
                database_url: "postgresql://localhost/test".to_string(),
                max_connections: 10,
            },
            escrow_instance_id: Some(Pubkey::from_str("11111111111111111111111111111111").unwrap()),
        }
    }

    fn create_indexer_config() -> IndexerConfig {
        IndexerConfig {
            datasource_type: DatasourceType::RpcPolling,
            rpc_polling: Some(RpcPollingConfig {
                from_slot: Some(0),
                poll_interval_ms: 1000,
                error_retry_interval_ms: 5000,
                batch_size: 10,
                encoding: UiTransactionEncoding::Json,
                commitment: CommitmentLevel::Finalized,
            }),
            yellowstone: None,
            backfill: BackfillConfig {
                enabled: true,
                batch_size: 100,
                max_gap_slots: 1000,
                start_slot: None,
                exit_after_backfill: false,
                rpc_url: "http://localhost:8899".to_string(),
            },
            reconciliation: ReconciliationConfig::default(),
        }
    }

    // ============================================================================
    // Common Config Validation Tests
    // ============================================================================

    #[test]
    fn test_validate_common_config_escrow_missing_instance_id() {
        let config = PrivateChannelIndexerConfig {
            program_type: ProgramType::Escrow,
            escrow_instance_id: None, // Missing required instance ID
            ..create_common_config()
        };

        let result = config.validate();

        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("--escrow-instance-id required"));
    }

    #[test]
    fn test_validate_common_config_withdraw_with_instance_id() {
        use std::str::FromStr;
        let config = PrivateChannelIndexerConfig {
            program_type: ProgramType::Withdraw,
            escrow_instance_id: Some(Pubkey::from_str("11111111111111111111111111111111").unwrap()),
            ..create_common_config()
        };

        let result = config.validate();

        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("should not be set for Withdraw program"));
    }

    #[test]
    fn test_validate_common_config_valid_escrow() {
        let config = create_common_config();
        let result = config.validate();
        assert!(result.is_ok());
    }

    // ============================================================================
    // Indexer Config Validation Tests
    // ============================================================================

    #[test]
    fn test_validate_indexer_rpc_polling_missing_config() {
        let config = IndexerConfig {
            datasource_type: DatasourceType::RpcPolling,
            rpc_polling: None, // Missing required config
            ..create_indexer_config()
        };

        let result = config.validate();

        #[cfg(feature = "datasource-rpc")]
        {
            assert!(result.is_err());
            let err_msg = result.unwrap_err();
            assert!(err_msg.contains("RPC polling config required"));
        }

        #[cfg(not(feature = "datasource-rpc"))]
        {
            assert!(result.is_err());
            let err_msg = result.unwrap_err();
            assert!(err_msg.contains("RPC datasource not compiled"));
        }
    }

    #[test]
    fn test_validate_indexer_yellowstone_missing_config() {
        let config = IndexerConfig {
            datasource_type: DatasourceType::Yellowstone,
            rpc_polling: None,
            yellowstone: None, // Missing required config
            ..create_indexer_config()
        };

        let result = config.validate();

        #[cfg(feature = "datasource-yellowstone")]
        {
            assert!(result.is_err());
            let err_msg = result.unwrap_err();
            assert!(err_msg.contains("Yellowstone config required"));
        }

        #[cfg(not(feature = "datasource-yellowstone"))]
        {
            assert!(result.is_err());
            let err_msg = result.unwrap_err();
            assert!(err_msg.contains("Yellowstone datasource not compiled"));
        }
    }

    #[test]
    fn test_validate_indexer_valid_config() {
        let config = create_indexer_config();

        #[cfg(feature = "datasource-rpc")]
        {
            let result = config.validate();
            assert!(result.is_ok());
        }
    }
}
