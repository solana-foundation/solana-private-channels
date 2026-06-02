use crate::error::{AccountError, OperatorError};
use crate::operator::RpcClientWithRetry;
use crate::storage::Storage;
use solana_rpc_client_api::client_error;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::request::RpcError;
use solana_sdk::pubkey::Pubkey;
use spl_token::ID as TOKEN_PROGRAM_ID;
use spl_token_2022::extension::{
    pausable::PausableConfig, permanent_delegate::PermanentDelegate, BaseStateWithExtensions,
    StateWithExtensions,
};
use spl_token_2022::state::Mint as Token2022MintState;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

const DECIMALS_OFFSET: usize = 44;

/// `getTokenAccountBalance` returns `RpcResponseError { code: -32602, ... }`
/// when the ATA does not exist. The lowercased substring match is a fallback
/// for non-standard RPC providers that may surface the same condition with a
/// different code.
fn is_account_not_found(e: &client_error::Error) -> bool {
    let ErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) = &e.kind else {
        return false;
    };
    if *code == -32602 {
        return true;
    }
    let msg = message.to_lowercase();
    msg.contains("could not find account") || msg.contains("account not found")
}

/// In-memory cache for basic mint metadata (`token_program`, `decimals`).
/// Token-2022 extension flags (`is_pausable`, `has_permanent_delegate`) are
/// resolved separately via [`MintCache::get_extension_flags`], because the
/// deposit-side sender JIT-init path has a `MintCache` pointed at the
/// **PrivateChannel** RPC where the mint doesn't yet exist — forcing extension
/// resolution from `get_mint_metadata` made that path fail with
/// `AccountNotFound` and broke every fresh deposit.
pub struct MintCache {
    storage: Arc<Storage>,
    rpc_client: Option<Arc<RpcClientWithRetry>>,
    cache: HashMap<String, MintMetadata>,
    extension_flags_cache: HashMap<String, (bool, bool)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MintMetadata {
    pub token_program: Pubkey,
    pub decimals: u8,
}

impl MintCache {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            rpc_client: None,
            cache: HashMap::new(),
            extension_flags_cache: HashMap::new(),
        }
    }

    pub fn with_rpc(storage: Arc<Storage>, rpc_client: Arc<RpcClientWithRetry>) -> Self {
        Self {
            storage,
            rpc_client: Some(rpc_client),
            cache: HashMap::new(),
            extension_flags_cache: HashMap::new(),
        }
    }

    /// Basic mint metadata (decimals + token program). Cache → DB → RPC
    /// fallback only when no DB row exists.
    ///
    /// Opportunistically warms `extension_flags_cache` on both the DB-hit
    /// and RPC-fallback branches: the DB row already carries the flags if
    /// they've been resolved before, and the RPC fallback's
    /// `fetch_mint_from_rpc` returns them as a by-product of parsing the
    /// mint account. This saves the subsequent `get_extension_flags` call
    /// on the withdrawal pre-flight a second DB round-trip (or, on the
    /// RPC-fallback path, a second RPC parse of the same mint).
    pub async fn get_mint_metadata(
        &mut self,
        mint: &Pubkey,
    ) -> Result<MintMetadata, OperatorError> {
        let mint_str = mint.to_string();

        if let Some(metadata) = self.cache.get(&mint_str) {
            return Ok(metadata.clone());
        }

        if let Some(m) = self.storage.get_mint(&mint_str).await? {
            let token_program =
                Pubkey::from_str(&m.token_program).map_err(|e| OperatorError::InvalidPubkey {
                    pubkey: m.token_program.clone(),
                    reason: e.to_string(),
                })?;
            let metadata = MintMetadata {
                token_program,
                decimals: m.decimals as u8,
            };
            self.cache.insert(mint_str.clone(), metadata.clone());
            if let (Some(p), Some(d)) = (m.is_pausable, m.has_permanent_delegate) {
                self.extension_flags_cache.insert(mint_str, (p, d));
            }
            return Ok(metadata);
        }

        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC for unknown mint {mint_str}, but no RPC client is configured",
            ))
        })?;

        let (metadata, flags) = self.fetch_mint_from_rpc(mint, rpc).await?;
        self.cache.insert(mint_str.clone(), metadata.clone());
        self.extension_flags_cache.insert(mint_str, flags);
        Ok(metadata)
    }

    /// Returns `(is_pausable, has_permanent_delegate)` for the mint.
    /// Cache → DB (if both flags resolved) → RPC + write-back. Used by the
    /// withdraw pre-flight; the deposit path never calls this.
    pub async fn get_extension_flags(
        &mut self,
        mint: &Pubkey,
    ) -> Result<(bool, bool), OperatorError> {
        let mint_str = mint.to_string();

        if let Some(flags) = self.extension_flags_cache.get(&mint_str) {
            return Ok(*flags);
        }

        let db_mint = self.storage.get_mint(&mint_str).await?;
        if let Some(ref m) = db_mint {
            if let (Some(p), Some(d)) = (m.is_pausable, m.has_permanent_delegate) {
                self.extension_flags_cache.insert(mint_str, (p, d));
                return Ok((p, d));
            }
        }

        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC to resolve extension flags for mint {mint_str}",
            ))
        })?;

        let (_metadata, flags) = self.fetch_mint_from_rpc(mint, rpc).await?;

        // Write-back only when the indexer has already landed a row. No row
        // means this is a pre-AllowMint-ingested edge case; we keep the
        // resolution in-memory and let the indexer's upsert land.
        //
        // Write-back failure is logged but not propagated: the in-memory
        // flags are authoritative for this process's lifetime, and a
        // transient DB blip would otherwise escalate a healthy withdrawal
        // to ManualReview via the caller's bail path. A later restart will
        // naturally retry the write-back on the next RPC fetch.
        if db_mint.is_some() {
            if let Err(e) = self
                .storage
                .set_mint_extension_flags(&mint_str, flags.0, flags.1)
                .await
            {
                warn!(
                    mint = %mint_str, error = %e,
                    "extension-flag write-back failed; continuing with in-memory resolution",
                );
            }
        }

        self.extension_flags_cache.insert(mint_str, flags);
        Ok(flags)
    }

    /// Live check of the `PausableConfig.paused` flag. Intended for the
    /// pre-flight pause check in the operator's ReleaseFunds path: only
    /// call this after `MintMetadata.is_pausable` came back true.
    pub async fn check_paused(&self, mint: &Pubkey) -> Result<bool, OperatorError> {
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError("check_paused requires an RPC client".to_string())
        })?;

        let account = rpc
            .get_account(mint)
            .await
            .map_err(|_| AccountError::AccountNotFound { pubkey: *mint })?;

        let state =
            StateWithExtensions::<Token2022MintState>::unpack(&account.data).map_err(|_| {
                AccountError::InvalidMint {
                    pubkey: *mint,
                    reason: "failed to parse Token-2022 mint".to_string(),
                }
            })?;

        let cfg =
            state
                .get_extension::<PausableConfig>()
                .map_err(|_| AccountError::InvalidMint {
                    pubkey: *mint,
                    reason: "mint is tagged is_pausable but PausableConfig extension is missing"
                        .to_string(),
                })?;

        Ok(bool::from(cfg.paused))
    }

    /// Live fetch of a token account's raw balance (base units).
    ///
    /// Intended for the permanent-delegate pre-flight: we can't trust our
    /// indexed balance because a permanent delegate may have moved tokens
    /// out of the escrow ATA without emitting a PrivateChannel program event. Only
    /// call this after `MintMetadata.has_permanent_delegate` came back true.
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

    async fn fetch_mint_from_rpc(
        &self,
        mint: &Pubkey,
        rpc: &RpcClientWithRetry,
    ) -> Result<(MintMetadata, (bool, bool)), OperatorError> {
        let account = rpc
            .get_account(mint)
            .await
            .map_err(|_| AccountError::AccountNotFound { pubkey: *mint })?;

        let token_program = account.owner;

        if ![TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID].contains(&token_program) {
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

        let decimals = account.data[DECIMALS_OFFSET];

        // PausableConfig and PermanentDelegate can only exist on Token-2022.
        // For a Token-2022-owned account that fails to parse we surface
        // InvalidMint rather than silently caching `(false, false)`: the
        // latter would poison the DB row and permanently bypass the pause
        // and drain pre-flights for that mint.
        let mut is_pausable = false;
        let mut has_permanent_delegate = false;
        if token_program == TOKEN_2022_PROGRAM_ID {
            let m =
                StateWithExtensions::<Token2022MintState>::unpack(&account.data).map_err(|_| {
                    AccountError::InvalidMint {
                        pubkey: *mint,
                        reason: "failed to parse Token-2022 mint for extension detection"
                            .to_string(),
                    }
                })?;
            is_pausable = m.get_extension::<PausableConfig>().is_ok();
            has_permanent_delegate = m.get_extension::<PermanentDelegate>().is_ok();
        }

        Ok((
            MintMetadata {
                token_program,
                decimals,
            },
            (is_pausable, has_permanent_delegate),
        ))
    }

    /// Pre-populate cache with mint metadata
    pub async fn prefetch_mints(&mut self, mints: &[Pubkey]) -> Result<(), OperatorError> {
        for mint in mints {
            self.get_mint_metadata(mint).await?;
        }
        Ok(())
    }

    // For now private_channel only supports SPL, when we want to make the move to token 2022, we
    // can call get mint_metadata above instead of this function.
    pub fn get_private_channel_token_program(&self) -> Pubkey {
        TOKEN_PROGRAM_ID
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::rpc_util::RpcClientWithRetry;
    use crate::operator::RetryConfig;
    use crate::storage::common::models::DbMint;
    use crate::storage::common::storage::mock::MockStorage;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_request::RpcRequest;
    use solana_sdk::pubkey::Pubkey;
    use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;

    impl MintCache {
        pub fn clear(&mut self) {
            self.cache.clear();
        }

        pub fn cache_size(&self) -> usize {
            self.cache.len()
        }
    }

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

    fn create_test_storage_with_mint(
        mint: &Pubkey,
        token_program: &Pubkey,
        decimals: i16,
    ) -> Arc<Storage> {
        let mut mock = MockStorage::new();

        mock.add_mint(DbMint {
            mint_address: mint.to_string(),
            decimals,
            token_program: token_program.to_string(),
            created_at: chrono::Utc::now(),
            is_pausable: Some(false),
            has_permanent_delegate: Some(false),
        });

        Arc::new(Storage::Mock(mock))
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let mint = create_test_mint();
        let token_program = TOKEN_PROGRAM_ID;
        let storage = create_test_storage_with_mint(&mint, &token_program, 6);

        let mut cache = MintCache::new(storage);

        assert_eq!(cache.cache_size(), 0);

        // First call - cache miss, fetches from storage
        let metadata1 = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata1.token_program, token_program);
        assert_eq!(metadata1.decimals, 6);
        assert_eq!(cache.cache_size(), 1);

        // Second call - cache hit, no storage fetch
        let metadata2 = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata2, metadata1);
        assert_eq!(cache.cache_size(), 1);
    }

    #[tokio::test]
    async fn test_token_2022_mint() {
        let mint = create_test_mint();
        let token_program = TOKEN_2022_PROGRAM_ID;
        let storage = create_test_storage_with_mint(&mint, &token_program, 9);

        let mut cache = MintCache::new(storage);

        let metadata = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata.token_program, TOKEN_2022_PROGRAM_ID);
        assert_eq!(metadata.decimals, 9);
    }

    #[tokio::test]
    async fn test_mint_not_found() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));

        let mut cache = MintCache::new(storage);

        let result = cache.get_mint_metadata(&mint).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_prefetch_mints() {
        let mint1 = create_test_mint();
        let mint2 = create_test_mint();
        let mint3 = create_test_mint();

        let mut mock = MockStorage::new();
        for mint in [&mint1, &mint2, &mint3] {
            mock.add_mint(DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(false),
            });
        }

        let storage = Arc::new(Storage::Mock(mock));
        let mut cache = MintCache::new(storage);

        assert_eq!(cache.cache_size(), 0);

        cache.prefetch_mints(&[mint1, mint2, mint3]).await.unwrap();
        assert_eq!(cache.cache_size(), 3);

        let _ = cache.get_mint_metadata(&mint1).await.unwrap();
        let _ = cache.get_mint_metadata(&mint2).await.unwrap();
        let _ = cache.get_mint_metadata(&mint3).await.unwrap();
        assert_eq!(cache.cache_size(), 3);
    }

    #[tokio::test]
    async fn test_multiple_mints_different_programs() {
        let spl_mint = create_test_mint();
        let t22_mint = create_test_mint();

        let mut mock = MockStorage::new();
        mock.add_mint(DbMint {
            mint_address: spl_mint.to_string(),
            decimals: 6,
            token_program: TOKEN_PROGRAM_ID.to_string(),
            created_at: chrono::Utc::now(),
            is_pausable: Some(false),
            has_permanent_delegate: Some(false),
        });
        mock.add_mint(DbMint {
            mint_address: t22_mint.to_string(),
            decimals: 9,
            token_program: TOKEN_2022_PROGRAM_ID.to_string(),
            created_at: chrono::Utc::now(),
            is_pausable: Some(false),
            has_permanent_delegate: Some(false),
        });

        let storage = Arc::new(Storage::Mock(mock));
        let mut cache = MintCache::new(storage);

        let spl_metadata = cache.get_mint_metadata(&spl_mint).await.unwrap();
        assert_eq!(spl_metadata.token_program, TOKEN_PROGRAM_ID);
        assert_eq!(spl_metadata.decimals, 6);

        let t22_metadata = cache.get_mint_metadata(&t22_mint).await.unwrap();
        assert_eq!(t22_metadata.token_program, TOKEN_2022_PROGRAM_ID);
        assert_eq!(t22_metadata.decimals, 9);

        assert_eq!(cache.cache_size(), 2);
    }

    #[tokio::test]
    async fn test_rpc_fallback_spl_token() {
        let mint = create_test_mint();
        let account_response = create_mock_account_response(&TOKEN_PROGRAM_ID, 9);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should fallback to RPC since mint not in storage
        let metadata = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata.token_program, TOKEN_PROGRAM_ID);
        assert_eq!(metadata.decimals, 9);
        assert_eq!(cache.cache_size(), 1);
    }

    #[tokio::test]
    async fn test_rpc_fallback_token_2022() {
        let mint = create_test_mint();
        let account_response = create_mock_account_response(&TOKEN_2022_PROGRAM_ID, 6);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should fallback to RPC and detect Token-2022
        let metadata = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata.token_program, TOKEN_2022_PROGRAM_ID);
        assert_eq!(metadata.decimals, 6);
    }

    #[tokio::test]
    async fn get_extension_flags_resolves_via_rpc_and_writes_back_when_db_flags_unresolved() {
        let mint = create_test_mint();

        // Indexer has landed the mints row but the operator hasn't resolved
        // the extension flags yet — this is the state we lazily fill.
        let mock_storage = MockStorage::new();
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: None,
                has_permanent_delegate: None,
            },
        );

        // Plain SPL Token mint on RPC → no extensions → both flags false.
        let account_response = create_mock_account_response(&TOKEN_PROGRAM_ID, 6);
        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);
        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(mock_storage.clone()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        let (is_pausable, has_permanent_delegate) = cache.get_extension_flags(&mint).await.unwrap();
        assert!(!is_pausable);
        assert!(!has_permanent_delegate);

        // Write-back happened — subsequent reads don't need RPC.
        let stored = mock_storage
            .mints
            .lock()
            .unwrap()
            .get(&mint.to_string())
            .cloned()
            .expect("mint row should still exist after write-back");
        assert_eq!(stored.is_pausable, Some(false));
        assert_eq!(stored.has_permanent_delegate, Some(false));
    }

    #[tokio::test]
    async fn get_mint_metadata_does_not_require_rpc_when_db_flags_are_unresolved() {
        let mint = create_test_mint();

        // DB row has flags = None. Pre-fix, `get_mint_metadata` would force
        // RPC resolution and fail here (breaking JIT-mint init on the
        // deposit path, where the mint-cache RPC can't see the mint yet).
        // Post-fix, `get_mint_metadata` is pure decimals + token_program —
        // flags are resolved separately via `get_extension_flags`.
        let mock_storage = MockStorage::new();
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: None,
                has_permanent_delegate: None,
            },
        );

        let storage = Arc::new(Storage::Mock(mock_storage));
        let mut cache = MintCache::new(storage);

        let metadata = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata.token_program, TOKEN_PROGRAM_ID);
        assert_eq!(metadata.decimals, 6);
    }

    #[tokio::test]
    async fn get_extension_flags_errors_when_unresolved_and_no_rpc() {
        let mint = create_test_mint();

        let mock_storage = MockStorage::new();
        mock_storage.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                is_pausable: None,
                has_permanent_delegate: None,
            },
        );

        let storage = Arc::new(Storage::Mock(mock_storage));
        let mut cache = MintCache::new(storage);

        let err = cache
            .get_extension_flags(&mint)
            .await
            .expect_err("should error without RPC");
        assert!(
            matches!(err, crate::error::OperatorError::RpcError(_)),
            "expected RpcError, got {err:?}",
        );
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

    #[tokio::test]
    async fn test_rpc_fallback_invalid_owner() {
        let mint = create_test_mint();
        let invalid_owner = Pubkey::new_unique();
        let account_response = create_mock_account_response(&invalid_owner, 6);

        let mut mocks = std::collections::HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_response);

        let rpc_client = RpcClientWithRetry::new_mocked(mocks);

        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should error on invalid owner
        let result = cache.get_mint_metadata(&mint).await;
        assert!(result.is_err());
    }
}
