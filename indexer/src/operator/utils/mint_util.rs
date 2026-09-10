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
use spl_token::ID as TOKEN_PROGRAM_ID;
use spl_token_2022::extension::{
    pausable::PausableConfig, permanent_delegate::PermanentDelegate,
    transfer_hook::TransferHook, BaseStateWithExtensions, StateWithExtensions,
};
use spl_token_2022::state::Account as Token2022AccountState;
use spl_token_2022::state::AccountState;
use spl_token_2022::state::Mint as Token2022MintState;
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
use spl_transfer_hook_interface::get_extra_account_metas_address;
use spl_transfer_hook_interface::offchain::add_extra_account_metas_for_execute;
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

/// `freeze_authority` is a `COption<Pubkey>` following `is_initialized`: a 4-byte
/// little-endian tag, then the key. Same base layout for SPL Token and Token-2022.
const FREEZE_AUTHORITY_TAG_OFFSET: usize = 46;

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
    /// Whether the mint carries `TransferHook`, gating per-withdrawal hook
    /// resolution. Only the presence is cached; the hook program id is read live
    /// because its authority can swap it.
    ///
    /// A stale `true` self-heals: it only skips the shortcut, so the next read
    /// corrects it. A stale `false` needs the mint closed at zero supply and
    /// recreated with a hook, since extensions are fixed at creation. That
    /// release then fails on-chain and remints, and a restart re-resolves.
    transfer_hook_cache: HashMap<String, bool>,
    /// Whether the mint has a `freeze_authority`, which gates the escrow-ATA
    /// freeze check. In-memory only: a freeze authority can be revoked but never
    /// added, so a stale `true` only costs one avoidable read and a stale value
    /// cannot miss a freeze. Not worth a `mints` column.
    freeze_authority_cache: HashMap<String, bool>,
    /// Per-mint slot the mint provably existed at, recorded by the caller that
    /// proved it. Absent means unproven, which keeps a missing account retryable.
    existence_floor: HashMap<String, u64>,
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
            transfer_hook_cache: HashMap::new(),
            freeze_authority_cache: HashMap::new(),
            existence_floor: HashMap::new(),
        }
    }

    pub fn with_rpc(storage: Arc<Storage>, rpc_client: Arc<RpcClientWithRetry>) -> Self {
        Self {
            storage,
            rpc_client: Some(rpc_client),
            cache: HashMap::new(),
            extension_flags_cache: HashMap::new(),
            transfer_hook_cache: HashMap::new(),
            freeze_authority_cache: HashMap::new(),
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

    /// Record whether this mint has a `freeze_authority`, for a caller that has
    /// already read the mint. Saves `has_freeze_authority` its own read.
    pub fn record_freeze_authority(&mut self, mint: &Pubkey, present: bool) {
        self.freeze_authority_cache
            .insert(mint.to_string(), present);
    }

    /// Record whether this mint carries `TransferHook`, for a caller that has
    /// already read the mint. Saves the hook resolution its own read.
    pub fn record_transfer_hook(&mut self, mint: &Pubkey, present: bool) {
        self.transfer_hook_cache.insert(mint.to_string(), present);
    }

    /// Whether a caller has already proved this mint exists on the target chain.
    pub fn has_existence_floor(&self, mint: &Pubkey) -> bool {
        self.existence_floor.contains_key(&mint.to_string())
    }

    fn existence_floor(&self, mint: &Pubkey) -> Option<u64> {
        self.existence_floor.get(&mint.to_string()).copied()
    }

    /// Basic mint metadata (decimals + token program), served from cache, then DB,
    /// then RPC only when no DB row exists. Both the DB and RPC branches warm
    /// `extension_flags_cache` where they can, sparing the pre-flight a second read.
    pub async fn get_mint_metadata(
        &mut self,
        mint: &Pubkey,
    ) -> Result<MintMetadata, OperatorError> {
        let mint_str = mint.to_string();

        if let Some(metadata) = self.cache.get(&mint_str) {
            return Ok(metadata.clone());
        }

        // Retry a transient DB blip before falling through to the RPC leg, so a
        // brief outage does not surface as Transient and strand the withdrawal.
        // transaction_id=-1: no per-call txn context here; retries log by op name.
        let db_mint = with_storage_backoff("mint metadata read", -1, || {
            self.storage.get_mint(&mint_str)
        })
        .await?;
        if let Some(m) = db_mint {
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

        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC for unknown mint {mint_str}, but no RPC client is configured",
            ))
        })?;

        let (metadata, flags) = self.fetch_mint_from_rpc(mint, rpc, floor).await?;
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

        // transaction_id=-1: no per-call txn context here; retries log by op name.
        let db_mint = with_storage_backoff("mint extension-flag read", -1, || {
            self.storage.get_mint(&mint_str)
        })
        .await?;
        if let Some(ref m) = db_mint {
            if let (Some(p), Some(d)) = (m.is_pausable, m.has_permanent_delegate) {
                self.extension_flags_cache.insert(mint_str, (p, d));
                return Ok((p, d));
            }
        }

        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC to resolve extension flags for mint {mint_str}",
            ))
        })?;

        let (_metadata, flags) = self.fetch_mint_from_rpc(mint, rpc, floor).await?;

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

    /// Whether the mint has a `freeze_authority`. Only such a mint can have its
    /// pooled escrow ATA frozen, so this gates the per-withdrawal ATA read.
    ///
    /// Cached for the process lifetime. A cached value can only be wrong if the
    /// mint is closed at zero supply and recreated with a freeze authority while
    /// preserving the decimals and token program that `Deposit` pins; the cost of
    /// that is a release failing on-chain instead of parking, which is the
    /// behaviour before this check existed. A restart re-resolves it.
    pub async fn has_freeze_authority(&mut self, mint: &Pubkey) -> Result<bool, OperatorError> {
        let mint_str = mint.to_string();

        if let Some(present) = self.freeze_authority_cache.get(&mint_str) {
            return Ok(*present);
        }

        let floor = self.existence_floor(mint);
        let rpc = self.rpc_client.as_ref().ok_or_else(|| {
            OperatorError::RpcError(format!(
                "MintCache needs RPC to resolve the freeze authority for mint {mint_str}",
            ))
        })?;

        let account = read_target_mint_account(rpc, mint, floor).await?;

        if account.data.len() < FREEZE_AUTHORITY_TAG_OFFSET + 4 {
            return Err(AccountError::InvalidMint {
                pubkey: *mint,
                reason: format!("Invalid mint account data length: {}", account.data.len()),
            }
            .into());
        }

        // A COption tag of 1 means Some.
        let present = u32::from_le_bytes(
            account.data[FREEZE_AUTHORITY_TAG_OFFSET..FREEZE_AUTHORITY_TAG_OFFSET + 4]
                .try_into()
                .map_err(|_| AccountError::InvalidMint {
                    pubkey: *mint,
                    reason: "could not read freeze_authority tag".to_string(),
                })?,
        ) == 1;

        self.freeze_authority_cache.insert(mint_str, present);
        Ok(present)
    }

    /// Hook program the mint's `TransferHook` points at, or `None` for a mint
    /// with no hook.
    async fn transfer_hook_program(
        &mut self,
        mint: &Pubkey,
    ) -> Result<Option<Pubkey>, OperatorError> {
        let mint_str = mint.to_string();

        if self.transfer_hook_cache.get(&mint_str) == Some(&false) {
            return Ok(None);
        }

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
        let hook_program = state
            .get_extension::<TransferHook>()
            .ok()
            .and_then(|hook| Option::<Pubkey>::from(hook.program_id));

        self.transfer_hook_cache
            .insert(mint_str, hook_program.is_some());
        Ok(hook_program)
    }

    /// Accounts a transfer of `mint` must carry for Token-2022 to run its
    /// transfer hook: the hook program, its validation account, and whatever the
    /// `ExtraAccountMetaList` resolves to. Empty for a mint with no hook.
    ///
    /// `None` means the validation account is absent, so no transfer of this
    /// mint can resolve; the caller parks instead of retrying.
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
    ) -> Result<Option<Vec<AccountMeta>>, OperatorError> {
        let Some(hook_program) = self.transfer_hook_program(mint).await? else {
            return Ok(Some(Vec::new()));
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
        if response.value.is_none() {
            return Ok(None);
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

        let fetch_rpc = Arc::clone(rpc);
        add_extra_account_metas_for_execute(
            &mut instruction,
            &hook_program,
            source,
            mint,
            destination,
            authority,
            amount,
            move |address| {
                let rpc = Arc::clone(&fetch_rpc);
                async move {
                    let account = rpc.get_account_with_context(&address, commitment).await?;
                    Ok(account.value.map(|account| account.data))
                }
            },
        )
        .await
        .map_err(|e| OperatorError::RpcError(format!("hook resolution for mint {mint}: {e}")))?;

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

        Ok(Some(extras))
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
    ) -> Result<(MintMetadata, (bool, bool)), OperatorError> {
        let account = read_target_mint_account(rpc, mint, existence_floor).await?;

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
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::pubkey::Pubkey;
    use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;
    use std::time::Duration;

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
            status: "allowed".to_string(),
            is_pausable: Some(false),
            has_permanent_delegate: Some(false),
        });
        mock.mint_status_history.lock().unwrap().push(DbMintStatus {
            mint_address: mint.to_string(),
            status: "allowed".to_string(),
            effective_slot: 0,
            signature: format!("test-seed-{mint}"),
            created_at: chrono::Utc::now(),
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
    async fn get_mint_metadata_retries_transient_db_error() {
        let mint = create_test_mint();
        let mock = MockStorage::new();
        mock.mints.lock().unwrap().insert(
            mint.to_string(),
            DbMint {
                mint_address: mint.to_string(),
                decimals: 6,
                token_program: TOKEN_PROGRAM_ID.to_string(),
                created_at: chrono::Utc::now(),
                status: "allowed".to_string(),
                is_pausable: Some(false),
                has_permanent_delegate: Some(false),
            },
        );
        // Two transient blips then success: the read backoff must ride them out.
        mock.set_fail_times("get_mint", 2);
        let storage = Arc::new(Storage::Mock(mock.clone()));
        let mut cache = MintCache::new(storage);

        let metadata = cache.get_mint_metadata(&mint).await.unwrap();
        assert_eq!(metadata.token_program, TOKEN_PROGRAM_ID);
        assert_eq!(metadata.decimals, 6);
        assert_eq!(mock.calls("get_mint"), 3, "two failures + one success");
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
                status: "allowed".to_string(),
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
            status: "allowed".to_string(),
            is_pausable: Some(false),
            has_permanent_delegate: Some(false),
        });
        mock.add_mint(DbMint {
            mint_address: t22_mint.to_string(),
            decimals: 9,
            token_program: TOKEN_2022_PROGRAM_ID.to_string(),
            created_at: chrono::Utc::now(),
            status: "allowed".to_string(),
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
                status: "allowed".to_string(),
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
                status: "allowed".to_string(),
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
                status: "allowed".to_string(),
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

    /// A token account: 165-byte base layout, `amount` at 64, `state` at 108.
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
    async fn has_freeze_authority_reads_the_coption_tag() {
        for (tag, expected) in [(1u32, true), (0u32, false)] {
            let mut mint_data = create_mock_mint_account_data(6);
            mint_data[FREEZE_AUTHORITY_TAG_OFFSET..FREEZE_AUTHORITY_TAG_OFFSET + 4]
                .copy_from_slice(&tag.to_le_bytes());

            let response = serde_json::json!({
                "context": {"slot": 1},
                "value": {
                    "owner": TOKEN_PROGRAM_ID.to_string(),
                    "lamports": 1_000_000u64,
                    "data": [STANDARD.encode(&mint_data), "base64"],
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
            assert_eq!(
                cache.has_freeze_authority(&mint).await.unwrap(),
                expected,
                "COption tag {tag} should resolve to {expected}"
            );

            // Second call must not need the RPC, which is what keeps a
            // non-freezable mint from paying per withdrawal.
            cache.rpc_client = None;
            assert_eq!(cache.has_freeze_authority(&mint).await.unwrap(), expected);
        }
    }

    /// A mint with no `TransferHook` resolves to no extras, and the answer is
    /// cached: a hook-less mint pays one read for the life of the process, and
    /// every withdrawal of it after that pays nothing.
    #[tokio::test]
    async fn resolve_hook_extras_is_empty_and_cached_without_a_hook() {
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

        let extras = cache
            .resolve_hook_extras(&mint, &source, &destination, &authority, 1_000)
            .await
            .unwrap()
            .expect("a hook-less mint is resolvable");
        assert!(extras.is_empty(), "no hook means no accounts to append");

        cache.rpc_client = None;
        assert!(cache
            .resolve_hook_extras(&mint, &source, &destination, &authority, 1_000)
            .await
            .unwrap()
            .expect("the cached answer needs no RPC")
            .is_empty());
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
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_client));

        // Should error on invalid owner
        let result = cache.get_mint_metadata(&mint).await;
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
    async fn mint_metadata_rpc_fallback_absent_account_stays_transient() {
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc_reporting_absent_account()));

        let err = cache.get_mint_metadata(&mint).await.unwrap_err();

        assert!(
            matches!(err, OperatorError::RpcError(_)),
            "an unknown mint has no proof of existence, so absence stays retryable, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn mint_metadata_rpc_fallback_transport_error_is_rpc_error() {
        let mut server = mockito::Server::new_async().await;
        let rpc = rpc_failing_transport(&mut server).await;
        let mint = create_test_mint();
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mut cache = MintCache::with_rpc(storage, Arc::new(rpc));

        let err = cache.get_mint_metadata(&mint).await.unwrap_err();

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
            .get_mint_metadata(&mint)
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
