use {
    super::api::PrivateChannelRpcServer,
    crate::{
        accounts::AccountsDB,
        rpc::{
            error::{read_not_enabled, write_not_enabled},
            get_account_info_impl::get_account_info_impl,
            get_block_impl::get_block_impl,
            get_block_time_impl::get_block_time_impl,
            get_blocks_impl::get_blocks_impl,
            get_epoch_info_impl::get_epoch_info_impl,
            get_epoch_schedule_impl::get_epoch_schedule_impl,
            get_first_available_block_impl::get_first_available_block_impl,
            get_latest_blockhash_impl::get_latest_blockhash_impl,
            get_recent_blockhash_impl::get_recent_blockhash_impl,
            get_recent_performance_samples_impl::get_recent_performance_samples_impl,
            get_signature_statuses_impl::get_signature_statuses_impl,
            get_signatures_for_address_impl::get_signatures_for_address_impl,
            get_slot_impl::get_slot_impl,
            get_slot_leaders_impl::get_slot_leaders_impl,
            get_supply_impl::get_supply_impl,
            get_token_account_balance_impl::get_token_account_balance_impl,
            get_transaction_count_impl::get_transaction_count_impl,
            get_transaction_impl::get_transaction_impl,
            get_vote_accounts_impl::get_vote_accounts_impl,
            is_blockhash_valid_impl::is_blockhash_valid_impl,
            send_transaction_impl::send_transaction_impl,
            simulate_transaction_impl::simulate_transaction,
        },
        stage_metrics::SharedMetrics,
    },
    jsonrpsee::core::{async_trait, RpcResult},
    serde_json::Value,
    solana_account_decoder_client_types::{token::UiTokenAmount, UiAccount},
    solana_epoch_info::EpochInfo,
    solana_epoch_schedule::EpochSchedule,
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
    solana_sdk::{hash::Hash, pubkey::Pubkey, transaction::SanitizedTransaction},
    solana_transaction_status_client_types::TransactionStatus,
    std::{
        collections::LinkedList,
        sync::{Arc, RwLock},
    },
    tokio::sync::mpsc,
};

pub struct ReadDeps {
    pub accounts_db: AccountsDB,
    // Used for simulating sigverify
    pub admin_keys: Vec<Pubkey>,
    pub live_blockhashes: Arc<RwLock<LinkedList<Hash>>>,
    pub max_blockhashes: u64,
}

pub struct WriteDeps {
    pub dedup_tx: mpsc::Sender<SanitizedTransaction>,
    pub metrics: SharedMetrics,
}

/// RPC implementation for PrivateChannel
pub struct PrivateChannelRpcImpl {
    pub read_deps: Option<ReadDeps>,
    pub write_deps: Option<WriteDeps>,
}

impl PrivateChannelRpcImpl {
    pub async fn new(read_deps: Option<ReadDeps>, write_deps: Option<WriteDeps>) -> Self {
        Self {
            read_deps,
            write_deps,
        }
    }
}

#[async_trait]
impl PrivateChannelRpcServer for PrivateChannelRpcImpl {
    async fn send_transaction(
        &self,
        transaction: String,
        _config: Option<RpcSendTransactionConfig>,
    ) -> RpcResult<String> {
        let write_deps = self
            .write_deps
            .as_ref()
            .ok_or_else(|| write_not_enabled())?;
        send_transaction_impl(write_deps, transaction, _config).await
    }

    async fn get_account_info(
        &self,
        pubkey: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_account_info_impl(read_deps, pubkey, config).await
    }

    async fn get_slot(&self, _config: Option<RpcContextConfig>) -> RpcResult<u64> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_slot_impl(read_deps, _config).await
    }

    async fn get_block(
        &self,
        slot: u64,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> RpcResult<Option<Value>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_block_impl(read_deps, slot, config).await
    }

    async fn get_transaction(
        &self,
        signature: String,
        config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
    ) -> RpcResult<Option<Value>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_transaction_impl(read_deps, signature, config).await
    }

    async fn get_recent_blockhash(&self) -> RpcResult<Response<RpcBlockhashFeeCalculator>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_recent_blockhash_impl(read_deps).await
    }

    async fn get_token_account_balance(
        &self,
        pubkey: String,
        _config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<UiTokenAmount>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_token_account_balance_impl(read_deps, pubkey, _config).await
    }

    async fn get_latest_blockhash(
        &self,
        _config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<RpcBlockhash>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_latest_blockhash_impl(read_deps, _config).await
    }

    async fn get_signature_statuses(
        &self,
        signatures: Vec<String>,
        _config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<Response<Vec<Option<TransactionStatus>>>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_signature_statuses_impl(read_deps, signatures, _config).await
    }

    async fn get_transaction_count(&self, _config: Option<RpcContextConfig>) -> RpcResult<u64> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_transaction_count_impl(read_deps, _config).await
    }

    async fn get_first_available_block(&self) -> RpcResult<u64> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_first_available_block_impl(read_deps).await
    }

    async fn get_blocks(
        &self,
        start_slot: u64,
        end_slot: Option<u64>,
        _config: Option<RpcContextConfig>,
    ) -> RpcResult<Vec<u64>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_blocks_impl(read_deps, start_slot, end_slot, _config).await
    }

    async fn get_epoch_info(&self, _config: Option<RpcEpochConfig>) -> RpcResult<EpochInfo> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_epoch_info_impl(read_deps, _config).await
    }

    async fn get_epoch_schedule(&self) -> RpcResult<EpochSchedule> {
        get_epoch_schedule_impl().await
    }

    async fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> RpcResult<Vec<RpcPerfSample>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_recent_performance_samples_impl(read_deps, limit).await
    }

    async fn get_block_time(&self, slot: u64) -> RpcResult<Option<i64>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_block_time_impl(read_deps, slot).await
    }

    async fn get_vote_accounts(
        &self,
        _config: Option<RpcGetVoteAccountsConfig>,
    ) -> RpcResult<RpcVoteAccountStatus> {
        get_vote_accounts_impl(_config).await
    }

    async fn get_supply(&self, _config: Option<RpcSupplyConfig>) -> RpcResult<Response<RpcSupply>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_supply_impl(read_deps, _config).await
    }

    async fn get_slot_leaders(&self, _start_slot: u64, _limit: u64) -> RpcResult<Vec<String>> {
        get_slot_leaders_impl(_start_slot, _limit).await
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        _config: Option<RpcContextConfig>,
    ) -> RpcResult<Response<bool>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        is_blockhash_valid_impl(read_deps, blockhash, _config).await
    }

    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        get_signatures_for_address_impl(read_deps, address, config).await
    }

    async fn simulate_transaction(
        &self,
        transaction: String,
        _config: Option<RpcSimulateTransactionConfig>,
    ) -> RpcResult<Response<RpcSimulateTransactionResult>> {
        let read_deps = self.read_deps.as_ref().ok_or_else(|| read_not_enabled())?;
        simulate_transaction(read_deps, transaction, _config).await
    }
}
