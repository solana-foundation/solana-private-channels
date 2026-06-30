use clap::{Parser, Subcommand};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use private_channel_indexer::config::DEFAULT_CONFIRMATION_POLL_INTERVAL_MS;
use private_channel_indexer::{
    BackfillConfig, DatasourceType, IndexerConfig, OperatorConfig, PostgresConfig,
    PrivateChannelIndexerConfig, ProgramType, ReconciliationConfig, RpcPollingConfig, StorageType,
    YellowstoneConfig,
};
use serde::Deserialize;
use solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey};
use solana_transaction_status::UiTransactionEncoding;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// Thin TOML deserialization wrappers
// These only exist to map TOML structure to internal config types

#[derive(Deserialize)]
struct CommonSection {
    program_type: ProgramType,
    rpc_url: String,
    source_rpc_url: Option<String>,
    escrow_instance_id: Option<String>,
}

#[derive(Deserialize)]
struct StorageSection {
    #[serde(rename = "type")]
    storage_type: StorageType,
    max_connections: u32,
}

#[derive(Deserialize, Default)]
struct ReconciliationSection {
    #[serde(default)]
    mismatch_threshold_raw: u64,
}

#[derive(Deserialize)]
struct IndexerSection {
    datasource_type: DatasourceType,
    rpc_polling: Option<RpcPollingSection>,
    yellowstone: Option<YellowstoneSection>,
    backfill: BackfillSection,
    #[serde(default)]
    reconciliation: ReconciliationSection,
}

#[derive(Deserialize)]
struct RpcPollingSection {
    start_slot: Option<u64>,
    poll_interval_ms: u64,
    error_retry_interval_ms: u64,
    batch_size: usize,
    #[serde(default)]
    encoding: Option<UiTransactionEncoding>,
    #[serde(default)]
    commitment: Option<CommitmentLevel>,
    #[serde(default)]
    fallback_rpc_url: Option<String>,
}

#[derive(Deserialize)]
struct YellowstoneSection {
    endpoint: Option<String>,
    commitment: String,
    x_token: Option<String>,
}

#[derive(Deserialize)]
struct BackfillSection {
    enabled: bool,
    backfill_only: bool,
    rpc_url: Option<String>,
    batch_size: usize,
    max_gap_slots: u64,
    start_slot: Option<u64>,
}

#[derive(Deserialize)]
struct OperatorSection {
    poll_interval_secs: u64,
    batch_size: u16,
    retry_max_attempts: u32,
    retry_base_delay_secs: u64,
    channel_buffer_size: usize,
    #[serde(default)]
    rpc_commitment: Option<CommitmentLevel>,
    #[serde(default = "default_reconciliation_interval_secs")]
    reconciliation_interval_secs: u64,
    #[serde(default = "default_reconciliation_tolerance_bps")]
    reconciliation_tolerance_bps: u16,
    #[serde(default)]
    reconciliation_webhook_url: Option<String>,
    #[serde(default = "default_feepayer_monitor_interval_secs")]
    feepayer_monitor_interval_secs: u64,
    #[serde(default = "default_confirmation_poll_interval_ms")]
    confirmation_poll_interval_ms: u64,
}

fn default_reconciliation_interval_secs() -> u64 {
    5 * 60
}

fn default_reconciliation_tolerance_bps() -> u16 {
    10
}

fn default_feepayer_monitor_interval_secs() -> u64 {
    60
}

fn default_confirmation_poll_interval_ms() -> u64 {
    DEFAULT_CONFIRMATION_POLL_INTERVAL_MS
}

#[derive(Parser, Debug)]
#[command(
    name = "private-channel-indexer",
    about = "Index data from PrivateChannel programs"
)]
struct Args {
    /// Path to configuration file
    #[arg(short = 'c', long = "config", env = "PRIVATE_CHANNEL_INDEXER_CONFIG")]
    config: PathBuf,

    /// Enable verbose logging
    #[arg(short = 'v', long, env = "PRIVATE_CHANNEL_INDEXER_VERBOSE")]
    verbose: bool,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Run as an indexer
    Indexer,
    /// Run as an operator
    Operator,
    /// Run as a resync operation
    Resync {
        /// Genesis slot to start from (default: 0)
        #[arg(long, default_value = "0")]
        genesis_slot: u64,
    },
}

const INDEXER_PREFIX: &str = "INDEXER";
const COMMON_PREFIX: &str = "COMMON";
const STORAGE_PREFIX: &str = "STORAGE";
const OPERATOR_PREFIX: &str = "OPERATOR";

/// Map environment variables to nested TOML config paths
///
/// Handles the conversion from flat env var names to nested config structure:
/// - COMMON_* -> common.*
/// - STORAGE_* -> storage.*
/// - INDEXER_* -> indexer.* (with special handling for nested sections)
/// - OPERATOR_* -> operator.*
fn map_env_to_config_path(
    prefix: &str,
    key: &figment::value::UncasedStr,
) -> figment::value::Uncased<'static> {
    let key_lower = key.as_str().to_lowercase();

    let path = match prefix {
        INDEXER_PREFIX => {
            // Handle nested indexer config sections
            if let Some(suffix) = key_lower.strip_prefix("yellowstone_") {
                format!("indexer.yellowstone.{}", suffix)
            } else if let Some(suffix) = key_lower.strip_prefix("rpc_polling_") {
                format!("indexer.rpc_polling.{}", suffix)
            } else if let Some(suffix) = key_lower.strip_prefix("backfill_") {
                format!("indexer.backfill.{}", suffix)
            } else if let Some(suffix) = key_lower.strip_prefix("reconciliation_") {
                format!("indexer.reconciliation.{}", suffix)
            } else {
                format!("indexer.{}", key_lower)
            }
        }
        COMMON_PREFIX => format!("common.{}", key_lower),
        STORAGE_PREFIX => format!("storage.{}", key_lower),
        OPERATOR_PREFIX => format!("operator.{}", key_lower),
        _ => key_lower,
    };

    path.into()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load configuration with figment: TOML file -> env vars
    // Environment variables override TOML config values
    let figment = Figment::new()
        .merge(Toml::file(&args.config))
        .merge(Env::prefixed("COMMON_").map(|k| map_env_to_config_path(COMMON_PREFIX, k)))
        .merge(Env::prefixed("STORAGE_").map(|k| map_env_to_config_path(STORAGE_PREFIX, k)))
        .merge(Env::prefixed("INDEXER_").map(|k| map_env_to_config_path(INDEXER_PREFIX, k)))
        .merge(Env::prefixed("OPERATOR_").map(|k| map_env_to_config_path(OPERATOR_PREFIX, k)));

    match args.mode {
        Mode::Indexer => run_indexer(figment, args.verbose).await,
        Mode::Operator => run_operator(figment, args.verbose).await,
        Mode::Resync { genesis_slot } => run_resync(figment, args.verbose, genesis_slot).await,
    }
}

async fn run_indexer(figment: Figment, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(if verbose {
            "info,private_channel_indexer=debug"
        } else {
            "info"
        })
        .init();

    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9100);
    private_channel_indexer::metrics::init();
    let health =
        private_channel_metrics::HealthState::new(private_channel_metrics::HealthConfig::indexer());
    private_channel_metrics::start_metrics_server_with_health(metrics_port, health.clone());

    let common: CommonSection = figment.extract_inner("common")?;
    private_channel_indexer::metrics::init_labels(private_channel_metrics::MetricLabel::as_label(
        &common.program_type,
    ));
    let storage: StorageSection = figment.extract_inner("storage")?;
    let indexer: IndexerSection = figment.extract_inner("indexer")?;

    // Build datasource-specific configs
    let (rpc_polling_config, yellowstone_config) = match indexer.datasource_type {
        DatasourceType::RpcPolling => {
            let rpc = indexer
                .rpc_polling
                .ok_or("rpc_polling configuration required for RpcPolling datasource")?;
            let config = RpcPollingConfig {
                poll_interval_ms: rpc.poll_interval_ms,
                error_retry_interval_ms: rpc.error_retry_interval_ms,
                batch_size: rpc.batch_size,
                from_slot: rpc.start_slot,
                encoding: rpc.encoding.unwrap_or(UiTransactionEncoding::Json),
                commitment: rpc.commitment.unwrap_or(CommitmentLevel::Finalized),
                fallback_rpc_url: rpc.fallback_rpc_url,
            };
            (Some(config), None)
        }
        DatasourceType::Yellowstone => {
            let ys = indexer
                .yellowstone
                .ok_or("yellowstone configuration required for Yellowstone datasource")?;
            let endpoint = ys
                .endpoint
                .ok_or("yellowstone.endpoint required for Yellowstone datasource")?;

            // Use token from config if provided, otherwise try env var
            let token = ys
                .x_token
                .or_else(|| std::env::var("INDEXER_YELLOWSTONE_TOKEN").ok());

            let config = YellowstoneConfig {
                endpoint,
                x_token: token,
                commitment: ys.commitment,
            };

            // Parse RPC polling config if provided (needed for backfill)
            let rpc_config = indexer.rpc_polling.map(|rpc| RpcPollingConfig {
                poll_interval_ms: rpc.poll_interval_ms,
                error_retry_interval_ms: rpc.error_retry_interval_ms,
                batch_size: rpc.batch_size,
                from_slot: rpc.start_slot,
                encoding: rpc.encoding.unwrap_or(UiTransactionEncoding::Json),
                commitment: rpc.commitment.unwrap_or(CommitmentLevel::Finalized),
                fallback_rpc_url: rpc.fallback_rpc_url,
            });

            (rpc_config, Some(config))
        }
    };

    // Get DATABASE_URL from environment
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL environment variable required")?;

    let postgres_config = PostgresConfig {
        database_url,
        max_connections: storage.max_connections,
    };

    let backfill_config = BackfillConfig {
        enabled: indexer.backfill.enabled,
        batch_size: indexer.backfill.batch_size,
        max_gap_slots: indexer.backfill.max_gap_slots,
        exit_after_backfill: indexer.backfill.backfill_only,
        rpc_url: indexer.backfill.rpc_url.unwrap_or(common.rpc_url.clone()),
        start_slot: indexer.backfill.start_slot,
    };

    // Parse escrow instance ID if provided
    let escrow_instance_id = common
        .escrow_instance_id
        .map(|id_str| {
            Pubkey::from_str(&id_str).map_err(|e| format!("Invalid escrow instance ID: {}", e))
        })
        .transpose()?;

    let common_config = PrivateChannelIndexerConfig {
        program_type: common.program_type,
        storage_type: storage.storage_type,
        postgres: postgres_config,
        rpc_url: common.rpc_url,
        source_rpc_url: common.source_rpc_url,
        escrow_instance_id,
    };

    let reconciliation_config = ReconciliationConfig {
        mismatch_threshold_raw: indexer.reconciliation.mismatch_threshold_raw,
    };

    let indexer_config = IndexerConfig {
        datasource_type: indexer.datasource_type,
        rpc_polling: rpc_polling_config,
        yellowstone: yellowstone_config,
        backfill: backfill_config,
        reconciliation: reconciliation_config,
    };

    common_config.validate()?;
    indexer_config.validate()?;

    private_channel_indexer::run(common_config, indexer_config, Some(health)).await?;

    Ok(())
}

async fn run_operator(figment: Figment, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(if verbose {
            "info,private_channel_indexer=debug"
        } else {
            "info"
        })
        .init();

    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9100);
    private_channel_indexer::metrics::init();
    let health = private_channel_metrics::HealthState::new(
        private_channel_metrics::HealthConfig::operator(),
    );
    private_channel_metrics::start_metrics_server_with_health(metrics_port, health.clone());

    let common: CommonSection = figment.extract_inner("common")?;
    private_channel_indexer::metrics::init_labels(private_channel_metrics::MetricLabel::as_label(
        &common.program_type,
    ));
    let storage_section: StorageSection = figment.extract_inner("storage")?;
    let operator: OperatorSection = figment.extract_inner("operator")?;

    // Get DATABASE_URL from environment
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL environment variable required")?;

    let postgres_config = PostgresConfig {
        database_url,
        max_connections: storage_section.max_connections,
    };

    // Initialize storage
    let storage: Arc<private_channel_indexer::storage::Storage> = match storage_section.storage_type
    {
        StorageType::Postgres => Arc::new(private_channel_indexer::storage::Storage::Postgres(
            private_channel_indexer::storage::PostgresDb::new(&postgres_config).await?,
        )),
    };
    storage
        .init_schema()
        .await
        .map_err(|e| format!("Storage error: {}", e))?;

    let escrow_instance_id = common
        .escrow_instance_id
        .map(|id_str| {
            Pubkey::from_str(&id_str).map_err(|e| format!("Invalid escrow instance ID: {}", e))
        })
        .transpose()?;

    let common_config = PrivateChannelIndexerConfig {
        program_type: common.program_type,
        storage_type: storage_section.storage_type,
        postgres: postgres_config,
        rpc_url: common.rpc_url,
        source_rpc_url: common.source_rpc_url,
        escrow_instance_id,
    };

    let operator_config = OperatorConfig {
        db_poll_interval: Duration::from_secs(operator.poll_interval_secs),
        batch_size: operator.batch_size,
        retry_max_attempts: operator.retry_max_attempts,
        retry_base_delay: Duration::from_secs(operator.retry_base_delay_secs),
        channel_buffer_size: operator.channel_buffer_size,
        rpc_commitment: operator
            .rpc_commitment
            .unwrap_or(CommitmentLevel::Confirmed),
        alert_webhook_url: std::env::var("ALERT_WEBHOOK_URL").ok(),
        reconciliation_interval: Duration::from_secs(operator.reconciliation_interval_secs),
        reconciliation_tolerance_bps: operator.reconciliation_tolerance_bps,
        reconciliation_webhook_url: operator.reconciliation_webhook_url,
        feepayer_monitor_interval: Duration::from_secs(operator.feepayer_monitor_interval_secs),
        confirmation_poll_interval_ms: operator.confirmation_poll_interval_ms,
    };

    // Validate signer configuration early (from environment variables)
    OperatorConfig::validate_signers().map_err(|e| format!("Signer configuration error: {}", e))?;

    private_channel_indexer::operator::run(storage, common_config, operator_config, Some(health))
        .await?;

    Ok(())
}

async fn run_resync(
    figment: Figment,
    verbose: bool,
    genesis_slot: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(if verbose {
            "info,private_channel_indexer=debug"
        } else {
            "info"
        })
        .init();

    let common: CommonSection = figment.extract_inner("common")?;
    let storage: StorageSection = figment.extract_inner("storage")?;
    let indexer: IndexerSection = figment.extract_inner("indexer")?;

    // Get DATABASE_URL from environment
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL environment variable required")?;

    let postgres_config = PostgresConfig {
        database_url,
        max_connections: storage.max_connections,
    };

    // Initialize storage
    let storage_instance: Arc<private_channel_indexer::storage::Storage> =
        match storage.storage_type {
            StorageType::Postgres => Arc::new(private_channel_indexer::storage::Storage::Postgres(
                private_channel_indexer::storage::PostgresDb::new(&postgres_config).await?,
            )),
        };

    // Initialize RPC poller
    let rpc_url = indexer
        .backfill
        .rpc_url
        .clone()
        .unwrap_or_else(|| common.rpc_url.clone());
    let rpc_encoding = indexer
        .rpc_polling
        .as_ref()
        .and_then(|rpc| rpc.encoding)
        .unwrap_or(UiTransactionEncoding::Json);
    let rpc_commitment = indexer
        .rpc_polling
        .as_ref()
        .and_then(|rpc| rpc.commitment)
        .unwrap_or(CommitmentLevel::Finalized);

    let rpc_poller = Arc::new(
        private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller::new(
            rpc_url,
            rpc_encoding,
            rpc_commitment,
        ),
    );

    // Parse escrow instance ID if provided
    let escrow_instance_id = common
        .escrow_instance_id
        .map(|id_str| {
            Pubkey::from_str(&id_str).map_err(|e| format!("Invalid escrow instance ID: {}", e))
        })
        .transpose()?;

    // Build backfill config base
    let backfill_config_base = BackfillConfig {
        enabled: true,
        exit_after_backfill: false,
        rpc_url: indexer.backfill.rpc_url.unwrap_or(common.rpc_url.clone()),
        batch_size: indexer.backfill.batch_size,
        max_gap_slots: u64::MAX,
        start_slot: Some(genesis_slot),
    };

    // Create ResyncService
    let resync_service = private_channel_indexer::indexer::resync::ResyncService::new(
        storage_instance,
        rpc_poller,
        common.program_type,
        backfill_config_base,
        escrow_instance_id,
    );

    // Run resync
    resync_service.run(genesis_slot).await?;

    Ok(())
}
