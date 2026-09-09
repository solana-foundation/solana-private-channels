use {
    jsonrpsee::{core::RpcResult, proc_macros::rpc},
    serde_json::Value,
    solana_account_decoder_client_types::{token::UiTokenAmount, UiAccount},
    solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature,
    solana_rpc_client_types::{
        config::{
            RpcAccountInfoConfig, RpcBlockConfig, RpcContextConfig, RpcEncodingConfigWrapper,
            RpcEpochConfig, RpcGetVoteAccountsConfig, RpcSendTransactionConfig,
            RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcSimulateTransactionConfig,
            RpcSupplyConfig, RpcTransactionConfig,
        },
        response::{
            Response, RpcBlockhash, RpcBlockhashFeeCalculator, RpcPerfSample,
            RpcSimulateTransactionResult, RpcSupply, RpcVoteAccountStatus,
        },
    },
    solana_transaction_status_client_types::TransactionStatus,
};

// Re-export Solana types for convenience
pub use solana_epoch_info::EpochInfo;
pub use solana_epoch_schedule::EpochSchedule;

/// The main RPC API trait for PrivateChannel
#[rpc(server)]
pub trait PrivateChannelRpc {
    /// Send a transaction to the network
    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        transaction: String,
        config: Option<RpcSendTransactionConfig>,
    ) -> RpcResult<String>;

    /// Get account information
    #[method(name = "getAccountInfo")]
    async fn get_account_info(
        &self,
        pubkey: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>>;

    /// Get the current slot, which ticks whether or not a block is produced
    #[method(name = "getSlot")]
    async fn get_slot(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    /// Get the count of blocks produced, which trails the slot while idle
    #[method(name = "getBlockHeight")]
    async fn get_block_height(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    /// Get block information
    #[method(name = "getBlock")]
    async fn get_block(
        &self,
        slot: u64,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> RpcResult<Option<Value>>;

    /// Get transaction information
    #[method(name = "getTransaction")]
    async fn get_transaction(
        &self,
        signature: String,
        config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
    ) -> RpcResult<Option<Value>>;

    /// Get recent blockhash for transaction submission
    #[method(name = "getRecentBlockhash")]
    async fn get_recent_blockhash(&self) -> RpcResult<Response<RpcBlockhashFeeCalculator>>;

    /// Get token account balance
    #[method(name = "getTokenAccountBalance")]
    async fn get_token_account_balance(
        &self,
        pubkey: String,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<UiTokenAmount>>;

    /// Get the latest blockhash and last valid block height
    #[method(name = "getLatestBlockhash")]
    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<RpcBlockhash>>;

    /// Get the statuses of a list of signatures
    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signatures: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<Response<Vec<Option<TransactionStatus>>>>;

    /// Get the current transaction count
    #[method(name = "getTransactionCount")]
    async fn get_transaction_count(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    /// Get the first available block in the ledger
    #[method(name = "getFirstAvailableBlock")]
    async fn get_first_available_block(&self) -> RpcResult<u64>;

    /// Get a list of confirmed blocks between two slots
    #[method(name = "getBlocks")]
    async fn get_blocks(
        &self,
        start_slot: u64,
        end_slot: Option<u64>,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<Vec<u64>>;

    /// Get the first `limit` confirmed blocks at or after `start_slot`
    #[method(name = "getBlocksWithLimit")]
    async fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<Vec<u64>>;

    /// Get information about the current epoch
    #[method(name = "getEpochInfo")]
    async fn get_epoch_info(&self, config: Option<RpcEpochConfig>) -> RpcResult<EpochInfo>;

    /// Get the epoch schedule
    #[method(name = "getEpochSchedule")]
    async fn get_epoch_schedule(&self) -> RpcResult<EpochSchedule>;

    /// Get recent performance samples
    #[method(name = "getRecentPerformanceSamples")]
    async fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> RpcResult<Vec<RpcPerfSample>>;

    /// Get the estimated production time of a block
    #[method(name = "getBlockTime")]
    async fn get_block_time(&self, slot: u64) -> RpcResult<Option<i64>>;

    /// Get vote accounts
    #[method(name = "getVoteAccounts")]
    async fn get_vote_accounts(
        &self,
        config: Option<RpcGetVoteAccountsConfig>,
    ) -> RpcResult<RpcVoteAccountStatus>;

    /// Get information about the current supply
    #[method(name = "getSupply")]
    async fn get_supply(&self, config: Option<RpcSupplyConfig>) -> RpcResult<Response<RpcSupply>>;

    /// Get the slot leaders for a given slot range
    #[method(name = "getSlotLeaders")]
    async fn get_slot_leaders(&self, start_slot: u64, limit: u64) -> RpcResult<Vec<String>>;

    /// Check if a blockhash is valid
    #[method(name = "isBlockhashValid")]
    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<bool>>;

    /// Get signatures for a given address
    #[method(name = "getSignaturesForAddress")]
    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>>;

    /// Simulate a transaction
    #[method(name = "simulateTransaction")]
    async fn simulate_transaction(
        &self,
        transaction: String,
        config: Option<RpcSimulateTransactionConfig>,
    ) -> RpcResult<Response<RpcSimulateTransactionResult>>;
}
