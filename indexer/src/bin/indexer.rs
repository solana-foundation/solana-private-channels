use clap::{Parser, Subcommand};
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use private_channel_indexer::config::{
    floor_operator_commitment, normalize_optional, parse_escrow_instance_id,
    validate_operator_startup, validate_rpc_batch_size, validate_rpc_encoding,
    DEFAULT_CONFIRMATION_POLL_INTERVAL_MS,
};
use private_channel_indexer::{
    BackfillConfig, DatasourceType, IndexerConfig, OperatorConfig, PostgresConfig,
    PrivateChannelIndexerConfig, ProgramType, ReconciliationConfig, RpcPollingConfig, StorageType,
    YellowstoneConfig,
};
use serde::Deserialize;
use solana_commitment_config::CommitmentLevel;
use solana_transaction_status::UiTransactionEncoding;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// Thin TOML deserialization wrappers
// These only exist to map TOML structure to internal config types

#[derive(Deserialize)]
struct CommonSection {
    program_type: ProgramType,
    rpc_url: String,
    #[serde(default)]
    fallback_rpc_url: Option<String>,
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
    #[serde(default)]
    reconciliation_tolerance_bps: u16,
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
}

#[derive(Deserialize)]
struct YellowstoneSection {
    endpoint: Option<String>,
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
        /// PrivateChannel RPC URL to reconcile rebuilt rows against. Required: resync
        /// refuses to run without it so it cannot rebuild without fail-closed reconciliation.
        #[arg(long)]
        channel_rpc_url: Option<String>,
        /// Acknowledge that this deletes this program's rows and rebuilds them. Required.
        /// Deliberately not bound to an environment variable, so it cannot be left
        /// switched on in a deployment's env file.
        #[arg(long)]
        destroy_existing_data: bool,
        /// Solana RPC that can read the escrow's withdrawal bitmap. A withdraw resync
        /// refuses to run without it (and common.escrow_instance_id), because a rebuild
        /// restarts the nonce sequence and must first prove the chain has issued no nonce.
        #[arg(long)]
        escrow_rpc_url: Option<String>,
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
        Mode::Resync {
            genesis_slot,
            channel_rpc_url,
            destroy_existing_data,
            escrow_rpc_url,
        } => {
            run_resync(
                figment,
                args.verbose,
                genesis_slot,
                channel_rpc_url,
                destroy_existing_data,
                escrow_rpc_url,
            )
            .await
        }
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
                // Ingestion is the value-finalizing source of truth; fixed at finalized,
                // so a forked block can never be indexed into an unbacked mint.
                commitment: CommitmentLevel::Finalized,
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
                // Fixed at finalized; the gap poller below inherits it.
                commitment: "finalized".to_string(),
            };

            // Parse RPC polling config if provided (needed for backfill)
            let rpc_config = indexer.rpc_polling.map(|rpc| RpcPollingConfig {
                poll_interval_ms: rpc.poll_interval_ms,
                error_retry_interval_ms: rpc.error_retry_interval_ms,
                batch_size: rpc.batch_size,
                from_slot: rpc.start_slot,
                encoding: rpc.encoding.unwrap_or(UiTransactionEncoding::Json),
                commitment: CommitmentLevel::Finalized,
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
    let escrow_instance_id = parse_escrow_instance_id(common.escrow_instance_id)?;

    let common_config = PrivateChannelIndexerConfig {
        program_type: common.program_type,
        storage_type: storage.storage_type,
        postgres: postgres_config,
        rpc_url: common.rpc_url,
        fallback_rpc_url: common.fallback_rpc_url,
        source_rpc_url: common.source_rpc_url,
        escrow_instance_id,
    };

    let reconciliation_config = ReconciliationConfig {
        mismatch_threshold_raw: indexer.reconciliation.mismatch_threshold_raw,
        reconciliation_tolerance_bps: indexer.reconciliation.reconciliation_tolerance_bps,
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

    // Signers load before the pool opens; a bad signer config never touches the database.
    private_channel_indexer::operator::init_signers()
        .await
        .map_err(|e| format!("Signer configuration error: {}", e))?;

    // Get DATABASE_URL from environment
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL environment variable required")?;

    // Every config rule runs before the pool opens, so a bad value never touches the database.
    let (common_config, operator_config) = build_operator_config(
        common,
        &storage_section,
        operator,
        database_url,
        std::env::var("ALERT_WEBHOOK_URL").ok(),
    )?;

    // Initialize storage
    let storage: Arc<private_channel_indexer::storage::Storage> = match storage_section.storage_type
    {
        StorageType::Postgres => Arc::new(private_channel_indexer::storage::Storage::Postgres(
            private_channel_indexer::storage::PostgresDb::new(&common_config.postgres).await?,
        )),
    };

    private_channel_indexer::operator::run(storage, common_config, operator_config, Some(health))
        .await?;

    Ok(())
}

/// Smallest reconciliation interval the binary accepts. A zero or tiny value samples one
/// finalized view several times in a row, which can trip the durable halt on a transient gap.
const MIN_RECONCILIATION_INTERVAL_SECS: u64 = 10;
/// Smallest fee-payer balance poll interval the binary accepts.
const MIN_FEEPAYER_MONITOR_INTERVAL_SECS: u64 = 5;

/// Turn the parsed sections into the runtime configs and apply every startup rule. It never
/// touches storage, so the caller can run it before opening the database pool. Interval
/// floors live here and not in `operator::run`, which library callers drive with short values.
fn build_operator_config(
    common: CommonSection,
    storage: &StorageSection,
    operator: OperatorSection,
    database_url: String,
    alert_webhook_url: Option<String>,
) -> Result<(PrivateChannelIndexerConfig, OperatorConfig), String> {
    let escrow_instance_id = parse_escrow_instance_id(common.escrow_instance_id)?;

    let postgres_config = PostgresConfig {
        database_url,
        max_connections: storage.max_connections,
    };
    postgres_config.validate()?;

    // Blank values count as unset and the trimmed value is what the workers use.
    let common_config = PrivateChannelIndexerConfig {
        program_type: common.program_type,
        storage_type: storage.storage_type,
        postgres: postgres_config,
        rpc_url: common.rpc_url,
        fallback_rpc_url: common.fallback_rpc_url,
        source_rpc_url: normalize_optional(common.source_rpc_url),
        escrow_instance_id,
    };

    let (reconciliation_secs, feepayer_secs) = (
        operator.reconciliation_interval_secs,
        operator.feepayer_monitor_interval_secs,
    );
    let operator_config = OperatorConfig {
        db_poll_interval: Duration::from_secs(operator.poll_interval_secs),
        batch_size: operator.batch_size,
        retry_max_attempts: operator.retry_max_attempts,
        retry_base_delay: Duration::from_secs(operator.retry_base_delay_secs),
        channel_buffer_size: operator.channel_buffer_size,
        rpc_commitment: floor_operator_commitment(
            operator
                .rpc_commitment
                .unwrap_or(CommitmentLevel::Confirmed),
        )?,
        alert_webhook_url,
        reconciliation_interval: Duration::from_secs(operator.reconciliation_interval_secs),
        reconciliation_tolerance_bps: operator.reconciliation_tolerance_bps,
        reconciliation_webhook_url: normalize_optional(operator.reconciliation_webhook_url),
        feepayer_monitor_interval: Duration::from_secs(operator.feepayer_monitor_interval_secs),
        confirmation_poll_interval_ms: operator.confirmation_poll_interval_ms,
    };

    validate_operator_startup(&common_config, &operator_config)?;
    // Zero is already refused above, so the floors only see non-zero values.
    for (secs, min, key, env) in [
        (
            reconciliation_secs,
            MIN_RECONCILIATION_INTERVAL_SECS,
            "operator.reconciliation_interval_secs",
            "OPERATOR_RECONCILIATION_INTERVAL_SECS",
        ),
        (
            feepayer_secs,
            MIN_FEEPAYER_MONITOR_INTERVAL_SECS,
            "operator.feepayer_monitor_interval_secs",
            "OPERATOR_FEEPAYER_MONITOR_INTERVAL_SECS",
        ),
    ] {
        if secs < min {
            return Err(format!("{key} ({env}) must be at least {min} seconds"));
        }
    }

    Ok((common_config, operator_config))
}

/// Resync skips `IndexerConfig::validate`, so it repeats the rules for what it uses: the
/// pool, the backfill batch size and the poller encoding. Runs before anything connects.
fn validate_resync_config(
    storage: &StorageSection,
    indexer: &IndexerSection,
) -> Result<(), String> {
    if storage.max_connections == 0 {
        return Err(
            "storage.max_connections (STORAGE_MAX_CONNECTIONS) must be greater than 0".into(),
        );
    }
    validate_rpc_batch_size(
        "indexer.backfill.batch_size (INDEXER_BACKFILL_BATCH_SIZE)",
        indexer.backfill.batch_size,
    )?;
    if let Some(encoding) = indexer.rpc_polling.as_ref().and_then(|rpc| rpc.encoding) {
        validate_rpc_encoding(
            "indexer.rpc_polling.encoding (INDEXER_RPC_POLLING_ENCODING)",
            encoding,
        )?;
    }
    Ok(())
}

async fn run_resync(
    figment: Figment,
    verbose: bool,
    genesis_slot: u64,
    channel_rpc_url: Option<String>,
    destroy_existing_data: bool,
    escrow_rpc_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Checked before anything connects, so a mistyped subcommand cannot get as far as
    // opening the database. The live-state lock is the real guard; this only makes the
    // destruction something the operator had to ask for by name.
    if !destroy_existing_data {
        return Err(
            "resync deletes this program's rows and rebuilds them from chain; pass \
                    --destroy-existing-data to confirm"
                .into(),
        );
    }

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
    // Reconcile reads the admin pubkey; load it before anything connects.
    private_channel_indexer::operator::init_signers()
        .await
        .map_err(|e| format!("Signer configuration error: {}", e))?;
    // Resync builds its backfill config without `IndexerConfig::validate`, so check here.
    validate_resync_config(&storage, &indexer)?;

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
    // Resync reads Solana blocks, so it uses the same fixed finalized ingestion commitment.
    let rpc_commitment = CommitmentLevel::Finalized;

    let rpc_poller = Arc::new(
        private_channel_indexer::indexer::datasource::rpc_polling::rpc::RpcPoller::new(
            rpc_url,
            rpc_encoding,
            rpc_commitment,
        ),
    );

    // Parse escrow instance ID if provided
    let escrow_instance_id = parse_escrow_instance_id(common.escrow_instance_id)?;

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

    // Reconcile-on-rebuild is mandatory: each already-serviced deposit or remint must be
    // rebuilt in its terminal state rather than as a fresh pending row the operator would
    // replay (mass double-mint). The channel RPC is the PrivateChannel whose confirmed
    // mints carry the idempotency memos; its authority is the admin mint authority. Fail
    // closed if it is not supplied rather than rebuild without reconciliation.
    let Some(channel_rpc_url) = channel_rpc_url else {
        return Err(
            "resync requires --channel-rpc-url to reconcile against the PrivateChannel; \
                    refusing to rebuild without it"
                .into(),
        );
    };
    let resync_service = resync_service.with_channel_reconcile(
        private_channel_indexer::indexer::resync::ChannelReconcileConfig {
            channel_rpc_url,
            authority: private_channel_indexer::operator::SignerUtil::get_admin_pubkey(),
        },
    );
    // A withdraw rebuild checks the escrow's withdrawal bitmap before dropping anything
    // and refuses without this RPC. The service reports the missing input itself.
    let resync_service = match escrow_rpc_url {
        Some(url) => resync_service.with_withdrawal_bitmap_rpc(url),
        None => resync_service,
    };

    // Run resync
    resync_service.run(genesis_slot).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    fn common_section(program_type: ProgramType) -> CommonSection {
        CommonSection {
            program_type,
            rpc_url: "http://127.0.0.1:8899".to_string(),
            fallback_rpc_url: None,
            source_rpc_url: Some("http://127.0.0.1:8899".to_string()),
            escrow_instance_id: Some(Pubkey::new_unique().to_string()),
        }
    }

    fn storage_section() -> StorageSection {
        StorageSection {
            storage_type: StorageType::Postgres,
            max_connections: 5,
        }
    }

    fn operator_section() -> OperatorSection {
        OperatorSection {
            poll_interval_secs: 1,
            batch_size: 10,
            retry_max_attempts: 3,
            retry_base_delay_secs: 1,
            channel_buffer_size: 100,
            rpc_commitment: None,
            reconciliation_interval_secs: 300,
            reconciliation_tolerance_bps: 10,
            reconciliation_webhook_url: Some("http://alerts.local/hook".to_string()),
            feepayer_monitor_interval_secs: 60,
            confirmation_poll_interval_ms: 250,
        }
    }

    fn build(
        common: CommonSection,
        storage: StorageSection,
        operator: OperatorSection,
    ) -> Result<(PrivateChannelIndexerConfig, OperatorConfig), String> {
        build_operator_config(common, &storage, operator, "postgres://x".to_string(), None)
    }

    #[test]
    fn a_valid_config_builds_for_both_roles() {
        for role in [ProgramType::Escrow, ProgramType::Withdraw] {
            build(common_section(role), storage_section(), operator_section())
                .expect("a valid config must build");
        }
    }

    #[test]
    fn zero_operator_values_are_refused_with_key_and_env_var() {
        type Mutate = fn(&mut OperatorSection);
        let cases: [(&str, &str, Mutate); 8] = [
            ("poll_interval_secs", "OPERATOR_POLL_INTERVAL_SECS", |o| {
                o.poll_interval_secs = 0
            }),
            ("batch_size", "OPERATOR_BATCH_SIZE", |o| o.batch_size = 0),
            ("retry_max_attempts", "OPERATOR_RETRY_MAX_ATTEMPTS", |o| {
                o.retry_max_attempts = 0
            }),
            (
                "retry_base_delay_secs",
                "OPERATOR_RETRY_BASE_DELAY_SECS",
                |o| o.retry_base_delay_secs = 0,
            ),
            ("channel_buffer_size", "OPERATOR_CHANNEL_BUFFER_SIZE", |o| {
                o.channel_buffer_size = 0
            }),
            (
                "reconciliation_interval_secs",
                "OPERATOR_RECONCILIATION_INTERVAL_SECS",
                |o| o.reconciliation_interval_secs = 0,
            ),
            (
                "feepayer_monitor_interval_secs",
                "OPERATOR_FEEPAYER_MONITOR_INTERVAL_SECS",
                |o| o.feepayer_monitor_interval_secs = 0,
            ),
            (
                "confirmation_poll_interval_ms",
                "OPERATOR_CONFIRMATION_POLL_INTERVAL_MS",
                |o| o.confirmation_poll_interval_ms = 0,
            ),
        ];
        for (key, env, mutate) in cases {
            let mut operator = operator_section();
            mutate(&mut operator);
            let err = build(
                common_section(ProgramType::Escrow),
                storage_section(),
                operator,
            )
            .expect_err("a zero value must be refused");
            assert!(err.contains(key) && err.contains(env), "got: {err}");
        }
    }

    #[test]
    fn interval_floors_apply_in_the_binary() {
        let mut operator = operator_section();
        operator.reconciliation_interval_secs = MIN_RECONCILIATION_INTERVAL_SECS - 1;
        let err = build(
            common_section(ProgramType::Escrow),
            storage_section(),
            operator,
        )
        .expect_err("a reconciliation interval under the floor must be refused");
        assert!(
            err.contains("OPERATOR_RECONCILIATION_INTERVAL_SECS"),
            "got: {err}"
        );

        let mut operator = operator_section();
        operator.feepayer_monitor_interval_secs = MIN_FEEPAYER_MONITOR_INTERVAL_SECS - 1;
        let err = build(
            common_section(ProgramType::Escrow),
            storage_section(),
            operator,
        )
        .expect_err("a fee-payer interval under the floor must be refused");
        assert!(
            err.contains("OPERATOR_FEEPAYER_MONITOR_INTERVAL_SECS"),
            "got: {err}"
        );

        let mut operator = operator_section();
        operator.reconciliation_interval_secs = MIN_RECONCILIATION_INTERVAL_SECS;
        operator.feepayer_monitor_interval_secs = MIN_FEEPAYER_MONITOR_INTERVAL_SECS;
        build(
            common_section(ProgramType::Escrow),
            storage_section(),
            operator,
        )
        .expect("the floors themselves are accepted");
    }

    #[test]
    fn storage_pool_size_zero_is_refused() {
        let storage = StorageSection {
            storage_type: StorageType::Postgres,
            max_connections: 0,
        };
        let err = build(
            common_section(ProgramType::Withdraw),
            storage,
            operator_section(),
        )
        .expect_err("a zero pool must be refused before it is opened");
        assert!(err.contains("STORAGE_MAX_CONNECTIONS"), "got: {err}");
    }

    #[test]
    fn instance_id_commitment_and_role_rules_are_enforced_before_storage() {
        let mut common = common_section(ProgramType::Withdraw);
        common.escrow_instance_id = Some("not-a-pubkey".to_string());
        assert!(build(common, storage_section(), operator_section()).is_err());

        let mut common = common_section(ProgramType::Withdraw);
        common.escrow_instance_id = None;
        assert!(build(common, storage_section(), operator_section()).is_err());

        let mut operator = operator_section();
        operator.rpc_commitment = Some(CommitmentLevel::Processed);
        assert!(build(
            common_section(ProgramType::Escrow),
            storage_section(),
            operator
        )
        .is_err());

        let mut operator = operator_section();
        operator.reconciliation_webhook_url = Some("  ".to_string());
        assert!(build(
            common_section(ProgramType::Escrow),
            storage_section(),
            operator
        )
        .is_err());

        let mut operator = operator_section();
        operator.reconciliation_webhook_url = None;
        build(
            common_section(ProgramType::Withdraw),
            storage_section(),
            operator,
        )
        .expect("the withdraw role never needs the webhook");

        let mut common = common_section(ProgramType::Withdraw);
        common.source_rpc_url = Some("   ".to_string());
        assert!(build(common, storage_section(), operator_section()).is_err());
    }

    #[test]
    fn blank_padding_is_trimmed_before_the_workers_see_it() {
        let mut common = common_section(ProgramType::Escrow);
        common.source_rpc_url = Some("  http://127.0.0.1:8899 \n".to_string());
        let mut operator = operator_section();
        operator.reconciliation_webhook_url = Some(" http://alerts.local/hook ".to_string());
        let (common_config, operator_config) =
            build(common, storage_section(), operator).expect("padded values are valid");
        assert_eq!(
            common_config.source_rpc_url.as_deref(),
            Some("http://127.0.0.1:8899")
        );
        assert_eq!(
            operator_config.reconciliation_webhook_url.as_deref(),
            Some("http://alerts.local/hook")
        );
    }

    fn indexer_section() -> IndexerSection {
        IndexerSection {
            datasource_type: DatasourceType::RpcPolling,
            rpc_polling: Some(RpcPollingSection {
                start_slot: None,
                poll_interval_ms: 1000,
                error_retry_interval_ms: 1000,
                batch_size: 10,
                encoding: None,
            }),
            yellowstone: None,
            backfill: BackfillSection {
                enabled: true,
                backfill_only: false,
                rpc_url: None,
                batch_size: 10,
                max_gap_slots: 1000,
                start_slot: None,
            },
            reconciliation: ReconciliationSection::default(),
        }
    }

    #[test]
    fn resync_refuses_a_zero_pool_a_zero_batch_and_a_non_json_encoding() {
        validate_resync_config(&storage_section(), &indexer_section())
            .expect("a valid resync config must pass");

        let storage = StorageSection {
            storage_type: StorageType::Postgres,
            max_connections: 0,
        };
        let err = validate_resync_config(&storage, &indexer_section()).expect_err("zero pool");
        assert!(err.contains("STORAGE_MAX_CONNECTIONS"), "got: {err}");

        let mut indexer = indexer_section();
        indexer.backfill.batch_size = 0;
        let err = validate_resync_config(&storage_section(), &indexer).expect_err("zero batch");
        assert!(err.contains("INDEXER_BACKFILL_BATCH_SIZE"), "got: {err}");

        let mut indexer = indexer_section();
        indexer.rpc_polling.as_mut().unwrap().encoding = Some(UiTransactionEncoding::JsonParsed);
        let err = validate_resync_config(&storage_section(), &indexer).expect_err("encoding");
        assert!(err.contains("INDEXER_RPC_POLLING_ENCODING"), "got: {err}");
    }
}
