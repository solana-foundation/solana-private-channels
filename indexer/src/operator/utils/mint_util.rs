use crate::error::{AccountError, OperatorError};
use crate::operator::utils::storage_util::with_storage_backoff;
use crate::operator::RpcClientWithRetry;
use crate::storage::common::models::MintStatusAtSlot;
use crate::storage::Storage;
use solana_rpc_client_api::client_error;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::account::Account;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_tlv_account_resolution::error::AccountResolutionError;
use spl_tlv_account_resolution::solana_program_error::ProgramError;
use spl_tlv_account_resolution::state::ExtraAccountMetaList;
use spl_token::ID as TOKEN_PROGRAM_ID;
use spl_token_2022::extension::{
    pausable::PausableConfig, transfer_hook::TransferHook, BaseStateWithExtensions,
    StateWithExtensions,
};
use spl_token_2022::state::Account as Token2022AccountState;
use spl_token_2022::state::AccountState;
use spl_token_2022::state::Mint as Token2022MintState;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use spl_transfer_hook_interface::get_extra_account_metas_address;
use spl_transfer_hook_interface::instruction::ExecuteInstruction;
use spl_transfer_hook_interface::offchain::add_extra_account_metas_for_execute;
use spl_type_length_value::state::TlvStateBorrowed;
use std::collections::HashMap;
use std::sync::Arc;

const DECIMALS_OFFSET: usize = 44;

/// `getTokenAccountBalance` returns `RpcResponseError { code: -32602, ... }`
/// when the ATA does not exist. The lowercased substring match is a fallback
/// for non-standard RPC providers that may surface the same condition with a
/// different code.
fn is_account_not_found(e: &client_error::Error) -> bool {
    let ErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) = &*e.kind else {
        return false;
    };
    if *code == -32602 {
        return true;
    }
    let msg = message.to_lowercase();
    msg.contains("could not find account") || msg.contains("account not found")
}

/// Reads a mint, telling "absent" apart from "could not read"; `get_account` merges both.
///
/// `existence_floor` is a slot the mint provably existed at. Requiring the node to
/// answer at or past it means a null cannot be lag: the node has the creation in its
/// state, so nothing is a permanent verdict without that proof.
async fn read_target_mint_account(
    rpc: &RpcClientWithRetry,
    mint: &Pubkey,
    existence_floor: Option<u64>,
) -> Result<Account, OperatorError> {
    let response = rpc
        .get_account_with_context_min_slot(mint, rpc.rpc_client.commitment(), existence_floor)
        .await
        .map_err(|e| OperatorError::RpcError(format!("get_account({mint}): {e}")))?;

    match (response.value, existence_floor) {
        (Some(account), _) => Ok(account),
        (None, Some(_)) => Err(AccountError::TargetMintMissing { pubkey: *mint }.into()),
        // Nothing proves this mint ever existed, so absence stays retryable.
        (None, None) => Err(OperatorError::RpcError(format!(
            "get_account({mint}): absent, and no allowlist slot proves it ever existed"
        ))),
    }
}

/// Mint reads and per-mint existence floors. Nothing a recreated mint can change
/// is cached: decimals come from the DB, the rest from `AllowedMint`.
pub struct MintCache {
    storage: Arc<Storage>,
    rpc_client: Option<Arc<RpcClientWithRetry>>,
    /// Per-mint slot the mint provably existed at, recorded by the caller that
    /// proved it. Absent means unproven, which keeps a missing account retryable.
    existence_floor: HashMap<String, u64>,
}

/// Outcome of resolving a mint's transfer-hook accounts.
#[derive(Debug)]
pub enum HookExtras {
    /// Accounts to append to the transfer; empty for a mint with no hook.
    Resolved(Vec<AccountMeta>),
    /// The validation account is absent, so no transfer of the mint resolves.
    ValidationMissing,
    /// The validation account's `Execute` list does not parse or cannot resolve.
    ValidationInvalid(String),
    /// The list resolves to `extras` accounts, more than a transfer can carry.
    OverCap { extras: usize },
}

impl MintCache {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            rpc_client: None,
            existence_floor: HashMap::new(),
        }
    }

    pub fn with_rpc(storage: Arc<Storage>, rpc_client: Arc<RpcClientWithRetry>) -> Self {
        Self {
            storage,
            rpc_client: Some(rpc_client),
            existence_floor: HashMap::new(),
        }
    }

    /// The RPC handle backing this cache, if one was configured.
    pub fn rpc_client(&self) -> Option<&RpcClientWithRetry> {
        self.rpc_client.as_deref()
    }

    /// Record that this mint provably existed at `slot`. The caller establishes the
    /// proof; only then may a missing account be treated as permanent rather than lag.
    pub fn record_existence_floor(&mut self, mint: &Pubkey, slot: u64) {
        self.existence_floor.insert(mint.to_string(), slot);
    }

    /// Whether a caller has already proved this mint exists on the target chain.
    pub fn has_existence_floor(&self, mint: &Pubkey) -> bool {
        self.existence_floor.contains_key(&mint.to_string())
    }

    /// The slot this mint was proved to exist at, if any. Doubles as the freshness
    /// anchor for reads that would otherwise have to establish one.
    pub fn existence_floor(&self, mint: &Pubkey) -> Option<u64> {
        self.existence_floor.get(&mint.to_string()).copied()
    }

    /// Mint decimals from the DB, or from RPC when no DB row exists.
    pub async fn get_mint_decimals(&self, mint: &Pubkey) -> Result<u8, OperatorError> {
        let mint_str = mint.to_string();

        // Retry a transient DB blip before falling through to the RPC leg.
        // transaction_id=-1: no per-call txn context here; retries log by op name.
        let db_mint = with_storage_backoff("mint decimals read", -1, || {
            self.storage.get_mint(&mint_str)
        })
        .await?;
        if let Some(m) = db_mint {
            return Ok(m.decimals as u8);
        }

        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC for unknown mint {mint_str}, but no RPC client is configured",
            ))
        })?;

        self.fetch_mint_from_rpc(mint, rpc, floor).await
    }

    /// Live check of the `PausableConfig.paused` flag. Intended for the
    /// pre-flight pause check in the operator's ReleaseFunds path: only
    /// call this once the reviewed profile says the mint is pausable.
    pub async fn check_paused(&self, mint: &Pubkey) -> Result<bool, OperatorError> {
        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError("check_paused requires an RPC client".to_string())
        })?;

        // Same split as the metadata fetch: a proven-absent mint is deterministic,
        // an unreachable or unconvinced node is not.
        let account = read_target_mint_account(rpc, mint, floor).await?;

        let state =
            StateWithExtensions::<Token2022MintState>::unpack(&account.data).map_err(|_| {
                AccountError::InvalidMint {
                    pubkey: *mint,
                    reason: "failed to parse Token-2022 mint".to_string(),
                }
            })?;

        let cfg = state.get_extension::<PausableConfig>().map_err(|_| {
            AccountError::MintProfileMismatch {
                pubkey: *mint,
                reason: "profile carries the Pausable bit but the mint has no PausableConfig \
                         extension"
                    .to_string(),
            }
        })?;

        Ok(bool::from(cfg.paused))
    }

    /// Hook program the mint's `TransferHook` points at, or `None` for a mint
    /// with no hook.
    async fn transfer_hook_program(
        &mut self,
        mint: &Pubkey,
    ) -> Result<Option<Pubkey>, OperatorError> {
        let mint_str = mint.to_string();

        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC to resolve the transfer hook for mint {mint_str}",
            ))
        })?;

        let account = read_target_mint_account(rpc, mint, floor).await?;

        let state =
            StateWithExtensions::<Token2022MintState>::unpack(&account.data).map_err(|_| {
                AccountError::InvalidMint {
                    pubkey: *mint,
                    reason: "failed to parse Token-2022 mint".to_string(),
                }
            })?;
        Ok(state
            .get_extension::<TransferHook>()
            .ok()
            .and_then(|hook| Option::<Pubkey>::from(hook.program_id)))
    }

    /// Accounts a transfer of `mint` must carry for Token-2022 to run its
    /// transfer hook: the hook program, its validation account, and whatever the
    /// `ExtraAccountMetaList` resolves to. Empty for a mint with no hook.
    ///
    /// Any variant other than `Resolved` means no transfer of this mint can
    /// resolve; the caller parks instead of retrying.
    ///
    /// Resolved per withdrawal rather than cached, since an
    /// `ExtraAccountMetaList` can derive accounts from the amount and
    /// destination.
    pub async fn resolve_hook_extras(
        &mut self,
        mint: &Pubkey,
        source: &Pubkey,
        destination: &Pubkey,
        authority: &Pubkey,
        amount: u64,
        max_extras: usize,
    ) -> Result<HookExtras, OperatorError> {
        let Some(hook_program) = self.transfer_hook_program(mint).await? else {
            return Ok(HookExtras::Resolved(Vec::new()));
        };

        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError("hook resolution requires an RPC client".to_string())
        })?;
        let commitment = rpc.rpc_client.commitment();

        // Read the validation account first so an absent one is told apart from
        // an unreachable node: absent is permanent, a failed read is not.
        let validation_pda = get_extra_account_metas_address(mint, &hook_program);
        let response = rpc
            .get_account_with_context(&validation_pda, commitment)
            .await
            .map_err(|e| OperatorError::RpcError(format!("get_account({validation_pda}): {e}")))?;
        let Some(validation_account) = response.value else {
            return Ok(HookExtras::ValidationMissing);
        };
        let validation_data = validation_account.data;

        // The resolver reads every declared entry, so an oversized list is
        // rejected before it runs. Extras are the entries plus the hook program
        // and the validation account.
        let extras_count = match TlvStateBorrowed::unpack(&validation_data).and_then(|tlv_state| {
            ExtraAccountMetaList::unpack_with_tlv_state::<ExecuteInstruction>(&tlv_state)
                .map(|list| list.len() + 2)
        }) {
            Ok(extras_count) => extras_count,
            Err(e) => return Ok(HookExtras::ValidationInvalid(e.to_string())),
        };
        if extras_count > max_extras {
            return Ok(HookExtras::OverCap {
                extras: extras_count,
            });
        }

        // The resolver requires source, mint, destination and authority in the
        // first four slots, which the escrow's layout does not have, so it runs
        // against a scratch instruction and we keep the tail.
        let mut instruction = Instruction::new_with_bytes(
            hook_program,
            &[],
            vec![
                AccountMeta::new_readonly(*source, false),
                AccountMeta::new_readonly(*mint, false),
                AccountMeta::new_readonly(*destination, false),
                AccountMeta::new_readonly(*authority, false),
            ],
        );

        // The resolver re-reads the validation account. Serving it these bytes
        // makes it resolve exactly the list counted above, which is what makes
        // the cap hold: a fresh read could return a longer list.
        let fetch_rpc = Arc::clone(rpc);
        if let Err(error) = add_extra_account_metas_for_execute(
            &mut instruction,
            &hook_program,
            source,
            mint,
            destination,
            authority,
            amount,
            move |address| {
                let rpc = Arc::clone(&fetch_rpc);
                let kept = (address == validation_pda).then(|| validation_data.clone());
                async move {
                    if kept.is_some() {
                        return Ok(kept);
                    }
                    let account = rpc.get_account_with_context(&address, commitment).await?;
                    Ok(account.value.map(|account| account.data))
                }
            },
        )
        .await
        {
            // A failed read comes back as AccountFetchFailed and stays transient.
            // Any other ProgramError is the list itself, which fails every retry.
            let fetch_failed = ProgramError::from(AccountResolutionError::AccountFetchFailed);
            return match error.downcast_ref::<ProgramError>() {
                Some(list_error) if *list_error != fetch_failed => {
                    Ok(HookExtras::ValidationInvalid(list_error.to_string()))
                }
                _ => Err(OperatorError::RpcError(format!(
                    "hook resolution for mint {mint}: {error}"
                ))),
            };
        }

        // Signer bits are dropped: the escrow strips them before the CPI too, and
        // the operator must never hand its own signature to a mint's hook.
        let extras = instruction
            .accounts
            .split_off(4)
            .into_iter()
            .map(|meta| AccountMeta {
                is_signer: false,
                ..meta
            })
            .collect();

        Ok(HookExtras::Resolved(extras))
    }

    /// Live fetch of a token account's raw balance (base units).
    ///
    /// Intended for the permanent-delegate pre-flight: we can't trust our
    /// indexed balance because a permanent delegate may have moved tokens
    /// out of the escrow ATA without emitting a PrivateChannel program event. Only
    /// call this once the reviewed profile says the mint has a permanent delegate.
    pub async fn get_ata_balance(&self, ata: &Pubkey) -> Result<u64, OperatorError> {
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError("get_ata_balance requires an RPC client".to_string())
        })?;

        // A non-existent ATA is semantically a zero balance — return Ok(0)
        // so the caller can compare it against the expected amount. Mapping
        // the not-found error to RpcError would classify it as Transient
        // and restart the operator forever on a condition that won't heal.
        match rpc.get_token_account_balance(ata).await {
            Ok(ui_amount) => ui_amount.amount.parse::<u64>().map_err(|e| {
                OperatorError::RpcError(format!(
                    "failed to parse token balance '{}' for {ata}: {e}",
                    ui_amount.amount
                ))
            }),
            Err(e) if is_account_not_found(&e) => Ok(0),
            Err(e) => Err(OperatorError::RpcError(format!(
                "get_token_account_balance({ata}): {e}"
            ))),
        }
    }

    /// Whether the token account is frozen.
    ///
    /// Reads the account rather than its balance because `state` is not part of
    /// the `getTokenAccountBalance` response. Kept separate from
    /// `get_ata_balance` so a delegated mint's balance check keeps its own
    /// round-trip; only a mint that is both freezable and delegated pays twice.
    /// Only call this after `has_freeze_authority` came back true.
    pub async fn is_ata_frozen(&self, ata: &Pubkey) -> Result<bool, OperatorError> {
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError("is_ata_frozen requires an RPC client".to_string())
        })?;

        let response = rpc
            .get_account_with_context(ata, rpc.rpc_client.commitment())
            .await
            .map_err(|e| OperatorError::RpcError(format!("get_account({ata}): {e}")))?;

        // An account that does not exist cannot be frozen. `getAccountInfo` reports
        // absence in its success shape, so unlike the balance read this needs no
        // error-shape sniffing.
        let Some(account) = response.value else {
            return Ok(false);
        };

        // Covers both token programs: the base layout is identical and an account
        // carrying no extensions unpacks as base-only.
        let state =
            StateWithExtensions::<Token2022AccountState>::unpack(&account.data).map_err(|_| {
                AccountError::AccountDeserializationFailed {
                    pubkey: *ata,
                    reason: "not a token account".to_string(),
                }
            })?;

        Ok(state.base.state == AccountState::Frozen)
    }

    async fn fetch_mint_from_rpc(
        &self,
        mint: &Pubkey,
        rpc: &RpcClientWithRetry,
        existence_floor: Option<u64>,
    ) -> Result<u8, OperatorError> {
        let account = read_target_mint_account(rpc, mint, existence_floor).await?;

        if ![TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].contains(&account.owner) {
            return Err(AccountError::InvalidMint {
                pubkey: *mint,
                reason: format!("Invalid mint owner: {}", account.owner),
            }
            .into());
        }

        // Mint layout: [option(coption_authority): 36 bytes, supply: 8 bytes,
        // decimals: 1 byte, ...]. Offset 44 works for both SPL and T22.
        if account.data.len() < DECIMALS_OFFSET + 1 {
            return Err(AccountError::InvalidMint {
                pubkey: *mint,
                reason: format!("Invalid mint account data length: {}", account.data.len()),
            }
            .into());
        }

        Ok(account.data[DECIMALS_OFFSET])
    }

    // The private channel only supports SPL for now.
    pub fn get_private_channel_token_program(&self) -> Pubkey {
        TOKEN_PROGRAM_ID
    }

    /// Operator gate: refuses deposits whose mint was not in `allowed`
    /// status at the deposit's slot, per `mint_status_history`.
    pub async fn assert_mint_allowed_at_slot(
        &self,
        mint: &Pubkey,
        deposit_slot: i64,
        transaction_id: i64,
    ) -> Result<(), OperatorError> {
        let mint_str = mint.to_string();
        let status = self
            .storage
            .get_mint_status_at_slot(&mint_str, deposit_slot)
            .await?;
        match status {
            MintStatusAtSlot::Allowed => Ok(()),
            _ => Err(OperatorError::MintNotAllowed {
                transaction_id,
                mint: mint_str,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OperatorError;
    use crate::operator::rpc_util::RpcClientWithRetry;
    use crate::operator::RetryConfig;
    use crate::storage::common::models::DbMint;
    use crate::storage::common::models::DbMintStatus;
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_request::RpcRequest;
    use solana_commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use spl_tlv_account_resolution::account::ExtraAccountMeta;
    use spl_tlv_account_resolution::seeds::Seed;
    use spl_token_2022::extension::{
        BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
    };
    use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
    use std::time::Duration;

    impl RpcClientWithRetry {
        pub fn new_mocked(mocks: solana_client::rpc_client::Mocks) -> Self {
            Self {
                rpc_client: Arc::new(RpcClient::new_mock_with_mocks(
                    "http://127.0.0.1:8899".to_string(),
                    mocks,
                )),
                retry_config: RetryConfig::default(),
            }
        }
    }

    fn create_mock_mint_account_data(decimals: u8) -> Vec<u8> {
        // Base SPL Mint layout (82 bytes). is_initialized sits at offset 45 —
        // must be 1 so Token-2022 `StateWithExtensions::unpack` accepts the
        // account; otherwise the parser surfaces UninitializedAccount.
        let mut data = vec![0u8; 82];
        data[DECIMALS_OFFSET] = decimals;
        data[45] = 1;
        data
    }

    fn create_test_mint() -> Pubkey {
        Pubkey::new_unique()
    }

    // Helper to create a mocked RPC response for getAccountInfo
    fn create_mock_account_response(mint_owner: &Pubkey, decimals: u8) -> serde_json::Value {
        let mint_data = create_mock_mint_account_data(decimals);

        serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": mint_owner.to_string(),
                "lamports": 1000000,
                "data": [STANDARD.encode(&mint_data), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        })
    }

    #[tokio::test]
    async fn get_mint_decimals_retries_transient_db_error() {
        let mint = create_test_mint();
        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                withdrawals_blocked: false,
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                profile_slot: 0,
            },
        );
        // Two transient blips then success: the read backoff must ride them out.
        mock.set_fail_times("get_mint", 2);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let cache = MintCache::new(storage);

        assert_eq!(cache.get_mint_decimals(&mint).await.unwrap(), 6);
        assert_eq!(mock.calls("get_mint"), 3, "two failures + one success");
    }

    #[tokio::test]
    async fn test_mint_not_found() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));

        let cache = MintCache::new(storage);

        let result = cache.get_mint_decimals(&mint).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rpc_fallback_spl_token() {
        let mint = create_test_mint();
        let account_response = create_mock_account_response(&TOKEN_PROGRAM_ID, 9);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should fallback to RPC since mint not in storage
        assert_eq!(cache.get_mint_decimals(&mint).await.unwrap(), 9);
    }

    #[tokio::test]
    async fn test_rpc_fallback_token_2022() {
        let mint = create_test_mint();
        let account_response = create_mock_account_response(&TOKEN_2022_PROGRAM_ID, 6);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should fallback to RPC and accept a Token-2022 owner
        assert_eq!(cache.get_mint_decimals(&mint).await.unwrap(), 6);
    }

    fn create_mock_token_account_data(amount: u64, frozen: bool) -> Vec<u8> {
        let mut data = vec![0u8; 165];
        data[64..72].copy_from_slice(&amount.to_le_bytes());
        data[108] = if frozen { 2 } else { 1 };
        data
    }

    fn token_account_response(amount: u64, frozen: bool) -> serde_json::Value {
        serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": TOKEN_PROGRAM_ID.to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(create_mock_token_account_data(amount, frozen)), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        })
    }

    #[tokio::test]
    async fn get_ata_balance_errors_without_rpc() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::new(storage);

        let err = cache
            .get_ata_balance(&create_test_mint())
            .await
            .expect_err("get_ata_balance should require RPC");
        assert!(
            matches!(err, crate::error::OperatorError::RpcError(_)),
            "expected RpcError, got {err:?}",
        );
    }

    #[tokio::test]
    async fn get_ata_balance_parses_amount_from_rpc() {
        let ata = Pubkey::new_unique();
        let balance_response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "amount": "123456789",
                "decimals": 6,
                "uiAmount": 123.456789,
                "uiAmountString": "123.456789"
            }
        });

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetTokenAccountBalance, balance_response);
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        let balance = cache.get_ata_balance(&ata).await.unwrap();
        assert_eq!(balance, 123_456_789);
    }

    #[tokio::test]
    async fn get_ata_balance_treats_a_missing_account_as_zero() {
        let mut server = mockito::Server::new_async().await;
        // The JSON-RPC code Solana returns for a token account that does not exist.
        server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"could not find account"},"id":1}"#,
            )
            .expect_at_least(1)
            .create_async()
            .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let balance = cache
            .get_ata_balance(&Pubkey::new_unique())
            .await
            .expect("a missing ATA is a zero balance, not a transient failure");
        assert_eq!(balance, 0);
    }

    #[tokio::test]
    async fn is_ata_frozen_errors_without_rpc() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::new(storage);

        let err = cache
            .is_ata_frozen(&create_test_mint())
            .await
            .expect_err("is_ata_frozen should require RPC");
        assert!(
            matches!(err, crate::error::OperatorError::RpcError(_)),
            "expected RpcError, got {err:?}",
        );
    }

    /// The whole point of reading the account instead of the balance: `state` is
    /// absent from the `getTokenAccountBalance` response.
    #[tokio::test]
    async fn is_ata_frozen_reads_the_account_state_byte() {
        for frozen in [false, true] {
            let mut mocks = std::collections::HashMap::new();
            mocks.insert(
                RpcRequest::GetAccountInfo,
                token_account_response(500, frozen),
            );
            let rpc_client = RpcClientWithRetry::new_mocked(mocks);

            let storage = Arc::new(Storage::Mock(MockStorage::new()));
            let cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

            assert_eq!(
                cache.is_ata_frozen(&Pubkey::new_unique()).await.unwrap(),
                frozen
            );
        }
    }

    #[tokio::test]
    async fn is_ata_frozen_treats_a_missing_account_as_unfrozen() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_reporting_absent_account()));

        let frozen = cache
            .is_ata_frozen(&Pubkey::new_unique())
            .await
            .expect("a missing ATA is not a transient failure");
        assert!(!frozen, "an account that does not exist cannot be frozen");
    }

    /// The gate that decides whether a withdrawal pays for the ATA read at all.
    #[tokio::test]
    async fn resolve_hook_extras_is_empty_without_a_hook() {
        let response = serde_json::json!({
            "context": {"slot": 1},
            "value": {
                "owner": TOKEN_2022_PROGRAM_ID.to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(create_mock_mint_account_data(6)), "base64"],
                "executable": false,
                "rentEpoch": 0
            }
        });
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, response);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache =
            MintCache::with_rpc(storage, Arc::new(RpcClientWithRetry::new_mocked(mocks)));

        let mint = create_test_mint();
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let resolved = cache
            .resolve_hook_extras(&mint, &source, &destination, &authority, 1_000, 15)
            .await
            .unwrap();
        assert!(
            matches!(&resolved, HookExtras::Resolved(extras) if extras.is_empty()),
            "no hook means no accounts to append, got {resolved:?}"
        );
    }

    /// A Token-2022 mint whose `TransferHook` points at `hook_program`.
    fn hook_mint_data(hook_program: &Pubkey) -> Vec<u8> {
        let len = ExtensionType::try_calculate_account_len::<Token2022MintState>(&[
            ExtensionType::TransferHook,
        ])
        .unwrap();
        let mut data = vec![0u8; len];
        let mut state =
            StateWithExtensionsMut::<Token2022MintState>::unpack_uninitialized(&mut data).unwrap();
        let hook = state.init_extension::<TransferHook>(true).unwrap();
        hook.program_id = Some(*hook_program).try_into().unwrap();
        state.base.is_initialized = true;
        state.pack_base();
        state.init_account_type().unwrap();
        data
    }

    /// Validation account bytes declaring `entries` for `Execute`.
    fn validation_data(entries: &[ExtraAccountMeta]) -> Vec<u8> {
        let mut data = vec![0u8; ExtraAccountMetaList::size_of(entries.len()).unwrap()];
        ExtraAccountMetaList::init::<ExecuteInstruction>(&mut data, entries).unwrap();
        data
    }

    /// Serves `address` to `getAccountInfo`, absent when `data` is `None`, and
    /// expects exactly `reads` requests for it.
    async fn mock_account(
        server: &mut mockito::ServerGuard,
        address: &Pubkey,
        data: Option<&[u8]>,
        reads: usize,
    ) -> mockito::Mock {
        let value = data.map(|bytes| {
            serde_json::json!({
                "owner": TOKEN_2022_PROGRAM_ID.to_string(),
                "lamports": 1_000_000u64,
                "data": [STANDARD.encode(bytes), "base64"],
                "executable": false,
                "rentEpoch": 0
            })
        });
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "result": {"context": {"slot": 1}, "value": value},
            "id": 1,
        });
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex(address.to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body.to_string())
            .expect(reads)
            .create_async()
            .await
    }

    /// Every account is read once, the mint twice (hook lookup, then the
    /// resolver), so a list at the cap costs a fixed number of reads.
    #[tokio::test]
    async fn resolve_hook_extras_at_cap_reads_validation_once() {
        let mint = create_test_mint();
        let hook_program = Pubkey::new_unique();
        let validation_pda = get_extra_account_metas_address(&mint, &hook_program);
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        let extra_accounts: Vec<Pubkey> = (0..12).map(|_| Pubkey::new_unique()).collect();
        let mut entries: Vec<ExtraAccountMeta> = extra_accounts
            .iter()
            .map(|address| ExtraAccountMeta::new_with_pubkey(address, false, false).unwrap())
            .collect();
        entries.push(ExtraAccountMeta::new_with_pubkey(&validation_pda, false, false).unwrap());

        let mut server = mockito::Server::new_async().await;
        let mint_reads =
            mock_account(&mut server, &mint, Some(&hook_mint_data(&hook_program)), 2).await;
        let validation_reads = mock_account(
            &mut server,
            &validation_pda,
            Some(&validation_data(&entries)),
            1,
        )
        .await;
        let mut single_reads = Vec::new();
        for address in [source, destination, authority]
            .iter()
            .chain(&extra_accounts)
        {
            single_reads.push(mock_account(&mut server, address, None, 1).await);
        }

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let resolved = cache
            .resolve_hook_extras(&mint, &source, &destination, &authority, 1_000, 15)
            .await
            .unwrap();

        // Declared entries plus the hook program and the validation account,
        // exactly the cap.
        let HookExtras::Resolved(extras) = resolved else {
            panic!("a list at the cap resolves, got {resolved:?}");
        };
        assert_eq!(extras.len(), entries.len() + 2);
        validation_reads.assert_async().await;
        mint_reads.assert_async().await;
        for reads in single_reads {
            reads.assert_async().await;
        }
    }

    /// Only the mint and the validation account are mocked, so any read of a
    /// declared entry fails the call: the cap must bail before resolving.
    #[tokio::test]
    async fn resolve_hook_extras_rejects_an_oversized_list_before_resolving() {
        let max_extras = 15;
        let declared = 14;
        let mint = create_test_mint();
        let hook_program = Pubkey::new_unique();
        let validation_pda = get_extra_account_metas_address(&mint, &hook_program);

        let entries: Vec<ExtraAccountMeta> = (0..declared)
            .map(|_| {
                ExtraAccountMeta::new_with_pubkey(&Pubkey::new_unique(), false, false).unwrap()
            })
            .collect();

        let mut server = mockito::Server::new_async().await;
        let mint_reads =
            mock_account(&mut server, &mint, Some(&hook_mint_data(&hook_program)), 1).await;
        let validation_reads = mock_account(
            &mut server,
            &validation_pda,
            Some(&validation_data(&entries)),
            1,
        )
        .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let resolved = cache
            .resolve_hook_extras(
                &mint,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                1_000,
                max_extras,
            )
            .await
            .unwrap();

        // Declared entries plus the hook program and the validation account.
        let total = declared + 2;
        assert!(
            matches!(resolved, HookExtras::OverCap { extras } if extras == total),
            "expected OverCap with {total} extras, got {resolved:?}"
        );
        mint_reads.assert_async().await;
        validation_reads.assert_async().await;
    }

    /// Bytes that are not a list are the mint's problem, not the node's, so
    /// they must park the row rather than read as transient.
    #[tokio::test]
    async fn resolve_hook_extras_rejects_an_unparseable_validation_account() {
        let mint = create_test_mint();
        let hook_program = Pubkey::new_unique();
        let validation_pda = get_extra_account_metas_address(&mint, &hook_program);

        let mut server = mockito::Server::new_async().await;
        mock_account(&mut server, &mint, Some(&hook_mint_data(&hook_program)), 1).await;
        mock_account(&mut server, &validation_pda, Some(&[7; 64]), 1).await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let resolved = cache
            .resolve_hook_extras(
                &mint,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                1_000,
                15,
            )
            .await
            .unwrap();

        assert!(
            matches!(resolved, HookExtras::ValidationInvalid(_)),
            "expected ValidationInvalid, got {resolved:?}"
        );
    }

    /// A list that parses but cannot resolve fails the same way on every retry,
    /// so it must park the row rather than restart the processor.
    #[tokio::test]
    async fn resolve_hook_extras_rejects_a_list_that_cannot_resolve() {
        let mint = create_test_mint();
        let hook_program = Pubkey::new_unique();
        let validation_pda = get_extra_account_metas_address(&mint, &hook_program);
        let source = Pubkey::new_unique();
        let destination = Pubkey::new_unique();
        let authority = Pubkey::new_unique();

        // Execute data is 16 bytes, so a seed at offset 16 never resolves.
        let entries = [ExtraAccountMeta::new_with_seeds(
            &[Seed::InstructionData {
                index: 16,
                length: 8,
            }],
            false,
            false,
        )
        .unwrap()];

        let mut server = mockito::Server::new_async().await;
        mock_account(&mut server, &mint, Some(&hook_mint_data(&hook_program)), 2).await;
        mock_account(
            &mut server,
            &validation_pda,
            Some(&validation_data(&entries)),
            1,
        )
        .await;
        for address in [source, destination, authority] {
            mock_account(&mut server, &address, None, 1).await;
        }

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let resolved = cache
            .resolve_hook_extras(&mint, &source, &destination, &authority, 1_000, 15)
            .await
            .unwrap();

        assert!(
            matches!(resolved, HookExtras::ValidationInvalid(_)),
            "expected ValidationInvalid, got {resolved:?}"
        );
    }

    /// A read the node fails is not the list's fault, so it must stay transient
    /// rather than park the row.
    #[tokio::test]
    async fn resolve_hook_extras_keeps_a_failed_read_transient() {
        let mint = create_test_mint();
        let hook_program = Pubkey::new_unique();
        let validation_pda = get_extra_account_metas_address(&mint, &hook_program);
        let entries =
            [ExtraAccountMeta::new_with_pubkey(&Pubkey::new_unique(), false, false).unwrap()];

        // The source is not mocked, so the resolver's first read fails.
        let mut server = mockito::Server::new_async().await;
        mock_account(&mut server, &mint, Some(&hook_mint_data(&hook_program)), 1).await;
        mock_account(
            &mut server,
            &validation_pda,
            Some(&validation_data(&entries)),
            1,
        )
        .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let err = cache
            .resolve_hook_extras(
                &mint,
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                1_000,
                15,
            )
            .await
            .unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "a failed read must stay transient, got {err:?}"
        );
    }

    #[tokio::test]
    async fn check_paused_errors_without_rpc() {
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::new(storage);

        let err = cache
            .check_paused(&create_test_mint())
            .await
            .expect_err("check_paused should require RPC");
        assert!(
            matches!(err, crate::error::OperatorError::RpcError(_)),
            "expected RpcError, got {err:?}",
        );
    }

    fn seed_status(mock: &MockStorage, mint: &Pubkey, status: &str, slot: i64) {
        mock.mint_status_history.lock().unwrap().push(DbMintStatus {
            withdrawals_blocked: false,
            mint_address: mint.to_string(),
            status: status.to_string(),
            effective_slot: slot,
            signature: format!("test-seed-{mint}-{slot}"),
            created_at: chrono::Utc::now(),
        });
    }

    #[tokio::test]
    async fn assert_mint_allowed_at_slot_passes_when_allowed_before_deposit() {
        let mint = create_test_mint();
        let mock = MockStorage::new();
        seed_status(&mock, &mint, "allowed", 10);
        let storage = Arc::new(Storage::Mock(mock));

        let cache = MintCache::new(storage);

        cache
            .assert_mint_allowed_at_slot(&mint, 50, 1)
            .await
            .expect("status allowed at slot 10 must apply at deposit slot 50");
    }

    #[tokio::test]
    async fn assert_mint_allowed_at_slot_rejects_deposit_before_allow() {
        let mint = create_test_mint();
        let mock = MockStorage::new();
        seed_status(&mock, &mint, "allowed", 50);
        let storage = Arc::new(Storage::Mock(mock));

        let cache = MintCache::new(storage);

        let err = cache
            .assert_mint_allowed_at_slot(&mint, 10, 7)
            .await
            .expect_err("deposit before allow must be rejected");
        match err {
            OperatorError::MintNotAllowed {
                transaction_id,
                mint: m,
            } => {
                assert_eq!(transaction_id, 7);
                assert_eq!(m, mint.to_string());
            }
            other => panic!("expected MintNotAllowed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assert_mint_allowed_at_slot_rejects_during_blocked_window() {
        let mint = create_test_mint();
        let mock = MockStorage::new();
        seed_status(&mock, &mint, "allowed", 10);
        seed_status(&mock, &mint, "blocked", 20);
        let storage = Arc::new(Storage::Mock(mock));

        let cache = MintCache::new(storage);

        let err = cache
            .assert_mint_allowed_at_slot(&mint, 25, 9)
            .await
            .expect_err("deposit during blocked window must be rejected");
        assert!(
            matches!(err, OperatorError::MintNotAllowed { .. }),
            "expected MintNotAllowed, got {err:?}",
        );
    }

    #[tokio::test]
    async fn assert_mint_allowed_at_slot_rejects_when_no_history() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));

        let cache = MintCache::new(storage);

        let err = cache
            .assert_mint_allowed_at_slot(&mint, 100, 42)
            .await
            .expect_err("mint with no history must be rejected");

        match err {
            OperatorError::MintNotAllowed {
                transaction_id,
                mint: m,
            } => {
                assert_eq!(transaction_id, 42);
                assert_eq!(m, mint.to_string());
            }
            other => panic!("expected MintNotAllowed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_rpc_fallback_invalid_owner() {
        let mint = create_test_mint();
        let invalid_owner = Pubkey::new_unique();
        let account_response = create_mock_account_response(&invalid_owner, 6);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should error on invalid owner
        let result = cache.get_mint_decimals(&mint).await;
        assert!(result.is_err());
    }
    /// An RPC that answers successfully but reports the account as absent.
    fn rpc_reporting_absent_account() -> RpcClientWithRetry {
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(
            RpcRequest::GetAccountInfo,
            serde_json::json!({"context": {"slot": 1}, "value": null}),
        );
        RpcClientWithRetry::new_mocked(mocks)
    }

    /// An RPC endpoint that fails at the transport layer on every attempt.
    async fn rpc_failing_transport(server: &mut mockito::ServerGuard) -> RpcClientWithRetry {
        server
            .mock("POST", "/")
            .with_status(500)
            .expect_at_least(1)
            .create_async()
            .await;
        RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 2,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        )
    }

    #[tokio::test]
    async fn mint_decimals_rpc_fallback_absent_account_stays_transient() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_reporting_absent_account()));

        let err = cache.get_mint_decimals(&mint).await.unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "an unknown mint has no proof of existence, so absence stays retryable, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn mint_decimals_rpc_fallback_transport_error_is_rpc_error() {
        let mut server = mockito::Server::new_async().await;
        let rpc = rpc_failing_transport(&mut server).await;
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let err = cache.get_mint_decimals(&mint).await.unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "a reachability failure must stay transient, got: {err:?}"
        );
    }

    /// The allow slot proves the mint existed, so a node that has passed it and still
    /// reports nothing is reporting a closed account, not lag.
    #[tokio::test]
    async fn check_paused_absent_past_the_allow_slot_is_target_mint_missing() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_reporting_absent_account()));
        cache.record_existence_floor(&mint, 42);

        let err = cache.check_paused(&mint).await.unwrap_err();

        assert!(
            matches!(
                err,
                OperatorError::Account(AccountError::TargetMintMissing { pubkey }) if pubkey == mint
            ),
            "absent mint must be deterministic, got: {err:?}"
        );
    }

    /// Without an allow slot nothing proves the mint ever existed, so a null could be
    /// a lagging node. Staying transient keeps a burned withdrawal out of manual review.
    #[tokio::test]
    async fn check_paused_absent_without_an_allow_slot_stays_transient() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc_reporting_absent_account()));

        let err = cache.check_paused(&mint).await.unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "an unprovable absence must stay retryable, got: {err:?}"
        );
    }

    /// The proof is only real if the node is actually asked to honour it, so this
    /// matches on the wire format: a request without `minContextSlot` gets no reply.
    #[tokio::test]
    async fn target_mint_read_sends_the_existence_floor_to_the_node() {
        let mint = create_test_mint();
        let mut server = mockito::Server::new_async().await;
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "result": create_mock_account_response(&TOKEN_PROGRAM_ID, 9),
            "id": 1,
        });
        let endpoint = server
            .mock("POST", "/")
            .match_body(mockito::Matcher::Regex("\"minContextSlot\":42".to_string()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body.to_string())
            .expect(1)
            .create_async()
            .await;

        let rpc = RpcClientWithRetry::with_retry_config(
            server.url(),
            RetryConfig {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
            },
            CommitmentConfig::confirmed(),
        );
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));
        cache.record_existence_floor(&mint, 42);

        cache
            .get_mint_decimals(&mint)
            .await
            .expect("the node answers when the floor is honoured");

        endpoint.assert_async().await;
    }
    #[tokio::test]
    async fn check_paused_transport_error_is_rpc_error() {
        let mut server = mockito::Server::new_async().await;
        let rpc = rpc_failing_transport(&mut server).await;
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let err = cache.check_paused(&mint).await.unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "a reachability failure must stay transient, got: {err:?}"
        );
    }
}
