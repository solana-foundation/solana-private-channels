pub mod channel_utils;
pub mod config;
pub mod error;
pub mod indexer;
pub mod metrics;
pub mod operator;
pub mod shutdown_utils;
pub mod storage;

// Also built for `test-mock-storage` so integration tests can construct the same
// instruction and event bytes the parser reads, instead of re-encoding the layout.
#[cfg(any(test, feature = "test-mock-storage"))]
pub mod test_utils;

pub use config::{
    BackfillConfig, DatasourceType, IndexerConfig, OperatorConfig, PostgresConfig,
    PrivateChannelIndexerConfig, ProgramType, ReconciliationConfig, RpcPollingConfig, StorageType,
    YellowstoneConfig,
};
pub use indexer::run;
