//! Confidential transfers on the escrow indexer side: key loading, the key checks, amount
//! decryption shared by indexing and recovery, and `confidential-recover`.

use crate::{
    error::{DataSourceError, IndexerError, ReconciliationError},
    indexer::datasource::common::parser::{
        decode_confidential_event, ConfidentialEvent, ConfidentialEventBody,
    },
    metrics,
    operator::utils::rpc_util::RpcClientWithRetry,
    storage::{
        common::{amount::TokenAmount, storage::live_lock::LiveLockGuard},
        Storage,
    },
};
use private_channel_confidential::Keys;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022::extension::{
    confidential_transfer::ConfidentialTransferAccount, BaseStateWithExtensions,
    StateWithExtensions,
};
use std::{str::FromStr, sync::Arc};
use tracing::{error, info, warn};

const CONFIDENTIAL_DEPOSIT: u8 = 13;

/// Load `ESCROW_CONFIDENTIAL_KEYS`. Unset is `None`; malformed refuses to start.
pub fn load_keys() -> Result<Option<Arc<Keys>>, IndexerError> {
    let keys = Keys::from_env().map_err(|e| IndexerError::ConfidentialKeys {
        reason: e.to_string(),
    })?;
    if keys.is_some() {
        info!("Confidential keys loaded");
    }
    Ok(keys.map(Arc::new))
}

/// Reads the ElGamal key an escrow token account is configured with.
#[async_trait::async_trait]
pub trait EscrowKeySource: Send + Sync {
    /// `None` when the account does not exist or is not configured.
    async fn configured_key(&self, escrow_ata: &Pubkey) -> Result<Option<[u8; 32]>, String>;

    /// Every escrow account of `instance` configured for confidential transfers, as
    /// `(mint, key)`, read from chain so an empty database still sees them.
    async fn configured_escrow_keys(
        &self,
        instance: &Pubkey,
    ) -> Result<Vec<(Pubkey, [u8; 32])>, String>;
}

/// Reads the escrow token account from chain.
pub struct RpcEscrowKeySource {
    pub rpc: Arc<RpcClientWithRetry>,
}

#[async_trait::async_trait]
impl EscrowKeySource for RpcEscrowKeySource {
    async fn configured_key(&self, escrow_ata: &Pubkey) -> Result<Option<[u8; 32]>, String> {
        let data = match self.rpc.get_account_data(escrow_ata).await {
            Ok(data) => data,
            Err(e) if e.to_string().contains("AccountNotFound") => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        let account = StateWithExtensions::<spl_token_2022::state::Account>::unpack(&data)
            .map_err(|e| e.to_string())?;
        Ok(account
            .get_extension::<ConfidentialTransferAccount>()
            .ok()
            .map(|extension| extension.elgamal_pubkey.0))
    }

    async fn configured_escrow_keys(
        &self,
        instance: &Pubkey,
    ) -> Result<Vec<(Pubkey, [u8; 32])>, String> {
        use solana_rpc_client_api::{request::RpcRequest, response::Response};
        let response = self
            .rpc
            .with_retry(
                "get_token_accounts_by_owner",
                crate::operator::utils::instruction_util::RetryPolicy::Idempotent,
                || async {
                    self.rpc
                        .rpc_client
                        .send::<Response<Vec<solana_rpc_client_api::response::RpcKeyedAccount>>>(
                            RpcRequest::GetTokenAccountsByOwner,
                            serde_json::json!([
                                instance.to_string(),
                                {"programId": spl_token_2022::ID.to_string()},
                                {"encoding": "base64", "commitment": "confirmed"}
                            ]),
                        )
                        .await
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut configured = Vec::new();
        for keyed in response.value {
            let data = keyed
                .account
                .data
                .decode()
                .ok_or_else(|| format!("token account {} did not decode", keyed.pubkey))?;
            let account = StateWithExtensions::<spl_token_2022::state::Account>::unpack(&data)
                .map_err(|e| format!("token account {}: {e}", keyed.pubkey))?;
            let mint = account.base.mint;
            let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                instance,
                &mint,
                &spl_token_2022::ID,
            );
            // Only the escrow account of each mint holds custody.
            if keyed.pubkey != ata.to_string() {
                continue;
            }
            if let Ok(extension) = account.get_extension::<ConfidentialTransferAccount>() {
                configured.push((mint, extension.elgamal_pubkey.0));
            }
        }
        Ok(configured)
    }
}

/// Mark `mint` poisoned (alerting only).
pub fn report_poisoned(mint: &str) {
    metrics::CONFIDENTIAL_MINT_POISONED
        .with_label_values(&[mint])
        .set(1.0);
}

/// Boot check: once any escrow account is configured the key is required and must match at
/// least one of them. A mint configured with another key is reported poisoned.
pub async fn check_keys_at_boot(
    storage: &Storage,
    keys: Option<&Keys>,
    source: &dyn EscrowKeySource,
    instance: &Pubkey,
) -> Result<(), IndexerError> {
    for mint in storage.get_confidential_poisoned_mints().await? {
        report_poisoned(&mint);
    }
    // From chain, not the database: a resync into an empty database must still see them.
    // Fail closed: a key we cannot compare is not a key we accept.
    let configured: Vec<(String, [u8; 32])> = source
        .configured_escrow_keys(instance)
        .await
        .map_err(|e| DataSourceError::InvalidConfig {
            reason: format!("could not read the instance's escrow accounts: {e}"),
        })?
        .into_iter()
        .map(|(mint, key)| (mint.to_string(), key))
        .collect();
    if configured.is_empty() {
        return Ok(());
    }
    let Some(keys) = keys else {
        return Err(IndexerError::ConfidentialKeys {
            reason: format!(
                "{} escrow account(s) are configured for confidential transfers; set {}",
                configured.len(),
                private_channel_confidential::KEYS_ENV
            ),
        });
    };
    let ours = keys.pubkey();
    if !configured.iter().any(|(_, pubkey)| *pubkey == ours) {
        return Err(IndexerError::ConfidentialKeys {
            reason: format!(
                "{} matches none of the configured escrow accounts",
                private_channel_confidential::KEYS_ENV
            ),
        });
    }
    for (mint, _) in configured.iter().filter(|(_, pubkey)| *pubkey != ours) {
        error!(%mint, "escrow account configured with another key; the mint is poisoned");
        report_poisoned(mint);
    }
    Ok(())
}

/// A deposit's net amount, `None` when `keys` cannot read it.
pub(crate) fn deposit_amount(event: &ConfidentialEvent, keys: Option<&Keys>) -> Option<u64> {
    let [lo0, hi0, lo1, hi1] = event.deposit_ciphertexts()?;
    keys?.decrypt_deposit(&lo0, &hi0, &lo1, &hi1)
}

/// A release's gross amount, `None` when `keys` cannot read it.
pub(crate) fn release_amount(event: &ConfidentialEvent, keys: Option<&Keys>) -> Option<u64> {
    let [lo, hi] = event.release_ciphertexts()?;
    keys?.decrypt_release(&lo, &hi)
}

/// Logged at INFO when a configure event names our key; the setup ceremony checks for it.
pub const CONFIGURED_UNDER_INSTANCE_KEY: &str = "escrow account configured under the instance key";

/// True when a configure event names a key other than ours (or we have none).
pub(crate) fn configured_with_foreign_key(event: &ConfidentialEvent, keys: Option<&Keys>) -> bool {
    match &event.body {
        ConfidentialEventBody::Configure { elgamal_pubkey } => {
            keys.is_none_or(|keys| keys.pubkey() != *elgamal_pubkey)
        }
        _ => false,
    }
}

/// Role recorded on the live lock while recovery holds it.
pub const RECOVER_LOCK_ROLE: &str = "confidential_recover";

/// The `confidential-recover` entry point: take the live lock exclusively, so no indexer or
/// operator runs meanwhile, recover `mint`, then release the lock.
pub async fn run_recover(
    storage: &Storage,
    keys: &Keys,
    source: &dyn EscrowKeySource,
    instance: &Pubkey,
    mint: &str,
    heartbeat_interval: std::time::Duration,
) -> Result<RecoverReport, IndexerError> {
    ensure_configured_under(keys, source, instance, mint).await?;
    let lost = tokio_util::sync::CancellationToken::new();
    let lock = storage
        .try_acquire_live_lock(
            crate::storage::common::storage::live_lock::LiveLockMode::Exclusive,
            RECOVER_LOCK_ROLE,
            lost,
            heartbeat_interval,
        )
        .await?;
    let result = recover_mint(storage, &lock, keys, mint).await;
    lock.stop_and_wait().await;
    result
}

/// Recovery only re-decrypts with the instance key, for a mint whose escrow account holds it:
/// a mint configured under a foreign key is never credited into the channel, and stays held.
async fn ensure_configured_under(
    keys: &Keys,
    source: &dyn EscrowKeySource,
    instance: &Pubkey,
    mint: &str,
) -> Result<(), IndexerError> {
    let mint_key = Pubkey::from_str(mint).map_err(|e| ReconciliationError::InvalidPubkey {
        pubkey: mint.to_string(),
        reason: e.to_string(),
    })?;
    let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        instance,
        &mint_key,
        &spl_token_2022::ID,
    );
    let configured =
        source
            .configured_key(&ata)
            .await
            .map_err(|e| DataSourceError::InvalidConfig {
                reason: format!("could not read mint {mint}'s escrow account: {e}"),
            })?;
    if configured != Some(keys.pubkey()) {
        return Err(IndexerError::ConfidentialKeys {
            reason: format!(
                "mint {mint}'s escrow account is not configured under the instance key in {}; \
                 a foreign-key mint cannot be recovered and stays poisoned",
                private_channel_confidential::KEYS_ENV
            ),
        });
    }
    Ok(())
}

/// What `confidential-recover` did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecoverReport {
    pub recovered: usize,
    pub still_unreadable: usize,
}

/// Re-decrypt a mint's undecrypted deposits from their raw bytes and clear their flags, in
/// one transaction under the exclusive live lock. Refuses during a resync.
pub async fn recover_mint(
    storage: &Storage,
    lock: &LiveLockGuard,
    keys: &Keys,
    mint: &str,
) -> Result<RecoverReport, IndexerError> {
    if let Some(program) = storage.get_unfinished_resync().await? {
        return Err(crate::error::StorageError::UnfinishedResync { program }.into());
    }
    let mut report = RecoverReport::default();
    let mut recovered = Vec::new();
    for (id, raw) in storage.get_confidential_undecrypted(mint).await? {
        let Some(event) = decode_confidential_event(CONFIDENTIAL_DEPOSIT, &raw) else {
            warn!(id, "stored confidential deposit no longer decodes");
            report.still_unreadable += 1;
            continue;
        };
        let Some(amount) = deposit_amount(&event, Some(keys)) else {
            report.still_unreadable += 1;
            continue;
        };
        recovered.push((id, TokenAmount(amount)));
    }
    report.recovered = recovered.len();
    if !recovered.is_empty() {
        lock.ensure_held().await?;
        storage.recover_confidential_fenced(lock, recovered).await?;
    }
    let poisoned = storage
        .get_confidential_poisoned_mints()
        .await?
        .contains(&mint.to_string());
    metrics::CONFIDENTIAL_MINT_POISONED
        .with_label_values(&[mint])
        .set(if poisoned { 1.0 } else { 0.0 });
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::{
        models::{DbMint, DbTransaction, DbTransactionBuilder, TransactionType},
        storage::mock::MockStorage,
    };
    use private_channel_confidential::test_support as cts;
    use std::collections::HashMap;

    const MINT: [u8; 32] = [31; 32];

    fn mint() -> String {
        Pubkey::new_from_array(MINT).to_string()
    }

    fn instance() -> Pubkey {
        Pubkey::new_from_array([200; 32])
    }

    fn deposit_raw(keys: &Keys, amount: u64) -> Vec<u8> {
        let zero = [0u8; 64];
        let (lo, hi) = private_channel_confidential::split(amount);
        let (lo1, hi1) = cts::credit(keys, &zero, &zero, lo as i64, hi);
        cts::deposit_event([0; 32], [4; 32], [5; 32], MINT, &zero, &zero, &lo1, &hi1)
    }

    fn undecrypted(id: i64, raw: Vec<u8>) -> DbTransaction {
        let mut row = DbTransactionBuilder::new(format!("sig{id}"), 10, mint(), 0)
            .initiator("user".into())
            .recipient("user".into())
            .transaction_type(TransactionType::Deposit)
            .confidential(true)
            .undecrypted(raw)
            .build();
        row.id = id;
        row
    }

    fn storage_with(rows: Vec<DbTransaction>) -> (Storage, MockStorage) {
        let mock = MockStorage::new();
        *mock.pending_transactions.lock().unwrap() = rows;
        (Storage::Mock(mock.clone()), mock)
    }

    /// Escrow accounts keyed by ATA, as `(mint, key)`, for the boot check.
    struct FakeAccounts(HashMap<Pubkey, (Pubkey, [u8; 32])>);

    #[async_trait::async_trait]
    impl EscrowKeySource for FakeAccounts {
        async fn configured_key(&self, ata: &Pubkey) -> Result<Option<[u8; 32]>, String> {
            Ok(self.0.get(ata).map(|(_, key)| *key))
        }

        async fn configured_escrow_keys(
            &self,
            _: &Pubkey,
        ) -> Result<Vec<(Pubkey, [u8; 32])>, String> {
            Ok(self.0.values().copied().collect())
        }
    }

    fn with_token_2022_mints(mock: &MockStorage, mints: &[[u8; 32]]) {
        for bytes in mints {
            let mint = Pubkey::new_from_array(*bytes).to_string();
            mock.mints.lock().unwrap().insert(
                mint.clone(),
                DbMint::new(mint, 6, spl_token_2022::ID.to_string(), TokenAmount(0)),
            );
        }
    }

    fn accounts(configured: &[([u8; 32], [u8; 32])]) -> FakeAccounts {
        FakeAccounts(
            configured
                .iter()
                .map(|(mint, key)| {
                    let mint = Pubkey::new_from_array(*mint);
                    let ata =
                        spl_associated_token_account::get_associated_token_address_with_program_id(
                            &instance(),
                            &mint,
                            &spl_token_2022::ID,
                        );
                    (ata, (mint, *key))
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn boot_check_needs_no_key_until_a_mint_is_configured() {
        let (storage, mock) = storage_with(vec![]);
        with_token_2022_mints(&mock, &[MINT]);
        check_keys_at_boot(&storage, None, &accounts(&[]), &instance())
            .await
            .unwrap();
    }

    /// A fresh database lists no mint, so the check reads the escrow accounts from chain:
    /// a resync into it without the key refuses.
    #[tokio::test]
    async fn boot_check_reads_the_configured_accounts_from_chain_not_the_database() {
        let (storage, _mock) = storage_with(vec![]);
        let source = accounts(&[(MINT, cts::keys().pubkey())]);
        let err = check_keys_at_boot(&storage, None, &source, &instance())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("configured"), "{err}");
    }

    /// The chain source lists the instance's escrow accounts with a confidential extension,
    /// and skips token accounts that are not a mint's escrow account.
    #[tokio::test]
    async fn the_rpc_source_lists_the_configured_escrow_accounts() {
        use crate::operator::utils::mint_util::confidential_fixtures::{
            account_with_confidential, AccountSpec,
        };
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let keys = cts::keys();
        let mut sim = cts::SimAccount::new(&keys);
        sim.credit(&keys, 10, 0);
        sim.apply();
        let mut spec = AccountSpec::healthy(&keys, &sim, 0, 10);
        spec.owner = instance();
        let data = STANDARD.encode(account_with_confidential(&spec));
        let escrow_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
            &instance(),
            &spec.mint,
            &spl_token_2022::ID,
        );
        let keyed = |pubkey: Pubkey| {
            serde_json::json!({"pubkey": pubkey.to_string(), "account": {
                "data": [data.clone(), "base64"], "owner": spl_token_2022::ID.to_string(),
                "lamports": 1, "executable": false, "rentEpoch": 0, "space": 0
            }})
        };
        let mut server = mockito::Server::new_async().await;
        server
            .mock("POST", "/")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({"method": "getTokenAccountsByOwner"}),
            ))
            .with_body(
                serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                    "context": {"slot": 1},
                    "value": [keyed(escrow_ata), keyed(Pubkey::new_unique())]
                }})
                .to_string(),
            )
            .create_async()
            .await;
        let source = RpcEscrowKeySource {
            rpc: Arc::new(RpcClientWithRetry::with_retry_config(
                server.url(),
                Default::default(),
                solana_commitment_config::CommitmentConfig::confirmed(),
            )),
        };
        assert_eq!(
            source.configured_escrow_keys(&instance()).await.unwrap(),
            vec![(spec.mint, keys.pubkey())]
        );
    }

    #[tokio::test]
    async fn boot_check_refuses_a_missing_or_foreign_key() {
        let (storage, mock) = storage_with(vec![]);
        with_token_2022_mints(&mock, &[MINT]);
        let source = accounts(&[(MINT, cts::keys().pubkey())]);
        assert!(check_keys_at_boot(&storage, None, &source, &instance())
            .await
            .is_err());
        let err = check_keys_at_boot(&storage, Some(&cts::other_keys()), &source, &instance())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("matches none"), "{err}");
        check_keys_at_boot(&storage, Some(&cts::keys()), &source, &instance())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn boot_check_flags_a_mint_configured_with_another_key() {
        let foreign_mint = [32u8; 32];
        let (storage, mock) = storage_with(vec![]);
        with_token_2022_mints(&mock, &[MINT, foreign_mint]);
        let source = accounts(&[
            (MINT, cts::keys().pubkey()),
            (foreign_mint, cts::other_keys().pubkey()),
        ]);
        check_keys_at_boot(&storage, Some(&cts::keys()), &source, &instance())
            .await
            .unwrap();
        assert_eq!(
            metrics::CONFIDENTIAL_MINT_POISONED
                .with_label_values(&[&Pubkey::new_from_array(foreign_mint).to_string()])
                .get(),
            1.0
        );
    }

    #[tokio::test]
    async fn recover_redecrypts_and_clears_the_flag() {
        let keys = cts::keys();
        let (storage, mock) = storage_with(vec![
            undecrypted(1, deposit_raw(&keys, 4_242)),
            undecrypted(2, deposit_raw(&keys, 7)),
            undecrypted(3, deposit_raw(&keys, 0)),
        ]);
        assert_eq!(
            storage.get_confidential_poisoned_mints().await.unwrap(),
            vec![mint()]
        );
        let report = recover(&storage, &keys, Some(keys.pubkey())).await.unwrap();
        assert_eq!(
            report,
            RecoverReport {
                recovered: 3,
                still_unreadable: 0,
            }
        );
        assert!(storage
            .get_confidential_poisoned_mints()
            .await
            .unwrap()
            .is_empty());
        let rows = mock.pending_transactions.lock().unwrap().clone();
        let amounts: Vec<(i64, u64, bool)> = rows
            .iter()
            .map(|r| (r.id, r.amount.0, r.confidential_undecrypted))
            .collect();
        // The zero deposit's row is gone, as if it had decrypted at index time.
        assert_eq!(amounts, vec![(1, 4_242, false), (2, 7, false)]);
        assert!(rows.iter().all(|r| r.confidential));
    }

    #[tokio::test]
    async fn recovered_rows_become_fetchable() {
        let keys = cts::keys();
        let (storage, mock) = storage_with(vec![undecrypted(1, deposit_raw(&keys, 9))]);
        mock.update_committed_checkpoint("escrow", 10)
            .await
            .unwrap();
        let fetch = || storage.get_and_lock_pending_transactions(TransactionType::Deposit, 10);
        assert!(fetch().await.unwrap().is_empty());
        recover(&storage, &keys, Some(keys.pubkey())).await.unwrap();
        assert_eq!(fetch().await.unwrap()[0].amount, TokenAmount(9));
    }

    /// Recovery re-decrypts only with the key the mint's escrow account is configured under,
    /// which must be the instance key it was given. A mint under any other key, or not
    /// configured, is refused and stays poisoned with nothing credited.
    #[tokio::test]
    async fn recover_refuses_a_mint_not_configured_under_the_instance_key() {
        let keys = cts::keys();
        for account_key in [Some(cts::other_keys().pubkey()), None] {
            let (storage, mock) = storage_with(vec![undecrypted(1, deposit_raw(&keys, 5))]);
            let err = recover(&storage, &keys, account_key).await.unwrap_err();
            assert!(
                matches!(&err, IndexerError::ConfidentialKeys { reason } if reason.contains("instance key")),
                "{err}"
            );
            assert_eq!(
                storage.get_confidential_poisoned_mints().await.unwrap(),
                vec![mint()]
            );
            assert_eq!(mock.pending_transactions.lock().unwrap()[0].amount.0, 0);
        }
        // A foreign-key deposit is unreadable to the instance key, and the account check
        // refuses before any decryption is tried.
        let foreign = cts::other_keys();
        let (storage, _) = storage_with(vec![undecrypted(1, deposit_raw(&foreign, 5))]);
        assert!(recover(&storage, &keys, Some(foreign.pubkey()))
            .await
            .is_err());
    }

    async fn recover(
        storage: &Storage,
        keys: &Keys,
        account_key: Option<[u8; 32]>,
    ) -> Result<RecoverReport, IndexerError> {
        let source = match account_key {
            Some(key) => accounts(&[(MINT, key)]),
            None => accounts(&[]),
        };
        run_recover(
            storage,
            keys,
            &source,
            &instance(),
            &mint(),
            std::time::Duration::from_secs(1),
        )
        .await
    }

    #[tokio::test]
    async fn recover_refuses_during_a_resync() {
        let keys = cts::keys();
        let (storage, mock) = storage_with(vec![undecrypted(1, deposit_raw(&keys, 5))]);
        *mock.unfinished_resync.lock().unwrap() = Some("escrow".to_string());
        let err = recover(&storage, &keys, Some(keys.pubkey()))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            IndexerError::Storage(crate::error::StorageError::UnfinishedResync { .. })
        ));
        assert_eq!(
            storage.get_confidential_poisoned_mints().await.unwrap(),
            vec![mint()]
        );
    }
}
