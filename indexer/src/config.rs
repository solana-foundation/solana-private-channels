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
    /// RPC commitment for block ingestion. Fixed at `finalized`: indexing is
    /// the value-finalizing source of truth, so a forkable block must never be indexed.
    pub commitment: CommitmentLevel,
}

/// Yellowstone gRPC specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YellowstoneConfig {
    /// Yellowstone gRPC endpoint URL
    pub endpoint: String,
    /// Token to use for authentication
    pub x_token: Option<String>,
    /// Stream commitment. Fixed at "finalized": indexing is the
    /// value-finalizing source of truth, so a sub-finalized stream could index a forked block.
    pub commitment: String,
}

/// Floor the operator's operational RPC commitment. This knob only affects blockhash/preflight
/// lifetime, never a settlement decision, so `confirmed` and `finalized` are both accepted;
/// only `processed` is rejected as too weak for even operational use.
pub fn floor_operator_commitment(level: CommitmentLevel) -> Result<CommitmentLevel, String> {
    match level {
        CommitmentLevel::Processed => Err(
            "operator rpc_commitment=processed is too weak; use confirmed or finalized (this knob \
             sets only the operational blockhash/preflight commitment, not the finalized \
             settlement gate)"
                .to_string(),
        ),
        other => Ok(other),
    }
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
    /// Optional archival fallback RPC for `rpc_url`, used by the operator to
    /// re-check a `Dead` verdict and by the poller for missing-block failover.
    #[serde(default)]
    pub fallback_rpc_url: Option<String>,
    /// Second-chain RPC, role-dependent. Required (fatal if unset) for the escrow operator
    /// (Solana custody), the escrow indexer (channel gateway supply check), and the withdraw
    /// operator (remint target); unused by the withdraw indexer.
    pub source_rpc_url: Option<String>,
    /// Postgres configuration
    pub postgres: PostgresConfig,
    /// Instance ID to filter (required for Escrow program)
    pub escrow_instance_id: Option<Pubkey>,
}

/// Trim an optional config string, treating blank/whitespace as unset.
fn normalized(v: &Option<String>) -> Option<&str> {
    v.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

impl PrivateChannelIndexerConfig {
    pub fn validate(&self) -> Result<(), String> {
        match (self.program_type, &self.escrow_instance_id) {
            (ProgramType::Escrow, None) => {
                return Err("--escrow-instance-id required when program_type is Escrow".to_string())
            }
            (ProgramType::Withdraw, Some(_)) => {
                return Err(
                    "--escrow-instance-id should not be set for Withdraw program".to_string(),
                )
            }
            _ => {}
        }
        // Escrow reconciliation always reads the second-chain RPC, so require it (blank counts as unset).
        if self.program_type == ProgramType::Escrow && normalized(&self.source_rpc_url).is_none() {
            return Err("source_rpc_url required when program_type is Escrow".to_string());
        }
        Ok(())
    }
}

/// Refuse a fallback that is the same node as the one serving blocks: failing over to it
/// re-fetches from the endpoint that just served the slot unusably, so it can never help
/// while still making the deploy look like it has a failover.
///
/// Which URL is the block source depends on the datasource. Live polling fetches from
/// `common.rpc_url`, while Yellowstone streams its blocks and reaches RPC only through the
/// reconnect gap-fill, which uses `backfill.rpc_url`.
pub fn validate_fallback_endpoint(
    common: &PrivateChannelIndexerConfig,
    indexer: &IndexerConfig,
) -> Result<(), String> {
    let Some(fallback) = normalized(&common.fallback_rpc_url) else {
        return Ok(());
    };

    let primary = match indexer.datasource_type {
        DatasourceType::RpcPolling => common.rpc_url.trim(),
        DatasourceType::Yellowstone => indexer.backfill.rpc_url.trim(),
    };

    if fallback == primary {
        return Err(format!(
            "fallback_rpc_url must differ from the block source ({primary}): failing over to the \
             same node re-fetches from the endpoint that just failed"
        ));
    }

    Ok(())
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

        // The one-shot repair only runs when backfill is also enabled. Without this the
        // pair reads as a typo that starts a normal live indexer, which for a job the
        // operator expects to exit means it silently never does.
        if self.backfill.exit_after_backfill && !self.backfill.enabled {
            return Err("backfill.backfill_only requires backfill.enabled to be true".to_string());
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
    /// Operational RPC commitment for the operator's blockhash fetch and preflight only.
    /// It does NOT gate settlement: the terminal Completed/FailedReminted writes and the
    /// recovery reconcile always use a hardcoded `finalized` gate regardless of this value.
    /// `confirmed` is a reasonable default (a finalized blockhash is ~13s stale and shortens
    /// transaction lifetime for no safety gain); `processed` is rejected as too weak.
    pub rpc_commitment: CommitmentLevel,
    /// Webhook URL for alerting on failed transactions. Set via ALERT_WEBHOOK env var.
    pub alert_webhook_url: Option<String>,
    /// How often to run escrow balance reconciliation checks
    #[serde(default = "default_reconciliation_interval")]
    pub reconciliation_interval: std::time::Duration,
    /// Tolerance threshold in basis points (100 bps = 1%)
    #[serde(default = "default_reconciliation_tolerance")]
    pub reconciliation_tolerance_bps: u16,
    /// Webhook URL for reconciliation alerts (optional). Carries both balance
    /// mismatch alerts and orphan deposit alerts.
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
            fallback_rpc_url: None,
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
    // Fallback endpoint validation
    // ============================================================================

    /// A fallback pointed at the polling datasource's own block source cannot help, so the
    /// config is refused rather than starting with a failover that is a no-op.
    #[test]
    fn fallback_equal_to_the_polling_block_source_is_refused() {
        let mut common = create_common_config();
        common.fallback_rpc_url = Some(common.rpc_url.clone());
        let indexer = create_indexer_config();

        let err = validate_fallback_endpoint(&common, &indexer).unwrap_err();

        assert!(
            err.contains("must differ from the block source"),
            "unexpected error: {err}"
        );
    }

    /// Yellowstone streams its blocks and reaches RPC only through the gap-fill, so the
    /// URL its fallback must differ from is `backfill.rpc_url`, not `common.rpc_url`.
    /// Comparing against the wrong one would let this config through.
    #[test]
    fn fallback_equal_to_the_gap_fill_block_source_is_refused() {
        let mut common = create_common_config();
        common.rpc_url = "http://primary-is-unused-here:8899".to_string();
        let mut indexer = create_indexer_config();
        indexer.datasource_type = DatasourceType::Yellowstone;
        common.fallback_rpc_url = Some(indexer.backfill.rpc_url.clone());

        let err = validate_fallback_endpoint(&common, &indexer).unwrap_err();

        assert!(
            err.contains(&indexer.backfill.rpc_url),
            "the error must name the gap-fill block source: {err}"
        );
    }

    /// An independent endpoint is the whole point, and an unset or blank fallback is a
    /// supported deploy, so neither may be refused.
    #[test]
    fn independent_or_absent_fallback_is_accepted() {
        let indexer = create_indexer_config();

        let mut independent = create_common_config();
        independent.fallback_rpc_url = Some("http://archival:8899".to_string());
        assert!(validate_fallback_endpoint(&independent, &indexer).is_ok());

        let absent = create_common_config();
        assert!(validate_fallback_endpoint(&absent, &indexer).is_ok());

        let mut blank = create_common_config();
        blank.fallback_rpc_url = Some("   ".to_string());
        assert!(validate_fallback_endpoint(&blank, &indexer).is_ok());
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

    #[test]
    fn test_validate_common_config_escrow_missing_source_rpc_url() {
        let config = PrivateChannelIndexerConfig {
            source_rpc_url: None,
            ..create_common_config()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("source_rpc_url required"));
    }

    #[test]
    fn test_validate_common_config_escrow_blank_source_rpc_url() {
        let config = PrivateChannelIndexerConfig {
            source_rpc_url: Some("   ".to_string()),
            ..create_common_config()
        };
        let err = config.validate().unwrap_err();
        assert!(err.contains("source_rpc_url required"));
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

    /// backfill_only on its own would start a live indexer that never exits, which is the
    /// opposite of the one-shot repair the flag names, so the pair is rejected up front.
    #[cfg(feature = "datasource-rpc")]
    #[test]
    fn validate_rejects_backfill_only_without_backfill_enabled() {
        let mut config = create_indexer_config();
        config.backfill.enabled = false;
        config.backfill.exit_after_backfill = true;

        let err = config
            .validate()
            .expect_err("backfill_only without enabled must not validate");
        assert!(
            err.contains("backfill.enabled"),
            "error must name the missing flag, got: {err}"
        );

        config.backfill.enabled = true;
        assert!(config.validate().is_ok(), "the enabled pair must validate");
    }

    // ── operator operational commitment floor ───────────────────────────

    /// Operator rpc_commitment rejects only `processed`; confirmed/finalized pass.
    /// (Indexing commitment is not configurable, so it has no validation test.)
    #[test]
    fn operator_commitment_rejects_only_processed() {
        assert!(floor_operator_commitment(CommitmentLevel::Processed).is_err());
        assert_eq!(
            floor_operator_commitment(CommitmentLevel::Confirmed).unwrap(),
            CommitmentLevel::Confirmed
        );
        assert_eq!(
            floor_operator_commitment(CommitmentLevel::Finalized).unwrap(),
            CommitmentLevel::Finalized
        );
    }
}
