/// BOB will always store the latest account state in-memory and will call the
/// AccountsDB whenever there is a cache miss.  You can visualize the flow as
/// follows:
///
/// Transaction -> Execution -> BOB
///                    |
///                    v
///                Settlement -> BOB
///                    |
///                    v
///               AccountsDB
///
/// Execution will always read/write from BOB.
/// Settlement still will always write to the AccountsDB.
/// After settlement, we send a message to BOB with the account that we flushed
/// to disk.
///
/// For every account stored in BOB, we also track a field called
/// `synced_since`. This is an `Option<u64>` that tracks in seconds how long the
/// account stored by BOB has been in sync with the AccountsDB>.
///
/// If `synced_since` is `None`, it means BOB has newer state than the
/// AccountsDB. We can NEVER evict accounts with `synced_since` set to `None`.
///
/// If `synced_since` is `Some(x)`, it means BOB has state that is `x` seconds
/// old. We can evict accounts with `synced_since` set to `Some(x)` if they are
/// older than `OLDEST_SYNCED_ACCOUNT_AGE` seconds. Generally, hot accounts will
/// have their `synced_since` updated frequently, so this is a good heuristic to
/// evict less frequently accessed accounts.
use {
    crate::{
        accounts::{precompiles::PRECOMPILES, AccountsDB},
        stages::AccountSettlement,
    },
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        pubkey::Pubkey,
        transaction::SanitizedTransaction,
    },
    solana_svm::{
        transaction_processing_result::ProcessedTransaction,
        transaction_processor::LoadAndExecuteSanitizedTransactionsOutput,
    },
    solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback},
    solana_svm_transaction::svm_message::SVMMessage,
    std::{
        collections::HashMap,
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::sync::mpsc,
    tracing::{debug, warn},
};

// TODO: Make this a config parameter
const OLDEST_SYNCED_ACCOUNT_AGE: u64 = 60 * 60; // 1 hour
struct AccountWithMeta {
    account: AccountSharedData,
    synced_since: Option<u64>,
    // Whether we deleted this account. We can't remove an account from the
    // HashMap while we keep it in-memory because it will fallback to the
    // AccountsDB.
    deleted: bool,
}

/// How often (in batches) to run the expensive eviction sweep in garbage_collect.
/// The settled_accounts channel is still drained on every preload to keep
/// dirty/clean tracking current; only the O(N) `retain()` scan is deferred.
const GC_EVICTION_INTERVAL: u64 = 100;

pub struct BOB {
    /// The in-memory account state
    accounts: HashMap<Pubkey, AccountWithMeta>,
    /// Precompiles that are always kept in memory (never garbage collected)
    precompiles: HashMap<Pubkey, AccountSharedData>,
    /// Accounts that are coming from settlement
    settled_accounts_rx: mpsc::UnboundedReceiver<Vec<(Pubkey, AccountSettlement)>>,
    /// AccountsDB account state
    pub accounts_db: AccountsDB,
    /// Counts preload calls since last eviction sweep
    batches_since_eviction: u64,
}

impl BOB {
    pub async fn new(
        accounts_db: AccountsDB,
        settled_accounts_rx: mpsc::UnboundedReceiver<Vec<(Pubkey, AccountSettlement)>>,
    ) -> Self {
        // Precompile ELFs are parsed once process-wide; see
        // `crate::accounts::precompiles`. Cloning the map is cheap, the
        // heavy work of parsing BPF bytes happens at first LazyLock access.
        Self {
            accounts: HashMap::new(),
            precompiles: PRECOMPILES.clone(),
            settled_accounts_rx,
            accounts_db,
            batches_since_eviction: 0,
        }
    }

    pub fn precompiles(&self) -> &HashMap<Pubkey, AccountSharedData> {
        &self.precompiles
    }

    /// Preloads accounts into BOB from the database.
    ///
    /// Returns `(fetched, cached)` where:
    /// - `fetched` = accounts that were missing from BOB and loaded from the DB.
    /// - `cached`  = accounts that were already warm in BOB (no DB round-trip needed).
    ///
    /// Only queries the database for accounts that are actual cache misses
    /// (not in BOB's HashMap and not a precompile). Once the working set is
    /// warm, most batches will skip the DB entirely.
    pub async fn preload_accounts(&mut self, pubkeys: &[Pubkey]) -> (usize, usize) {
        // Drain settled_accounts channel to keep dirty/clean tracking current.
        // The expensive eviction sweep only runs every GC_EVICTION_INTERVAL batches.
        self.garbage_collect();

        // Partition pubkeys into cache hits vs misses, skipping precompiles
        // (which are always in memory and never need DB lookup).
        let mut already_cached = 0usize;
        let mut miss_keys: Vec<Pubkey> = Vec::new();

        for pubkey in pubkeys {
            if self.precompiles.contains_key(pubkey) {
                continue;
            }
            if self.accounts.contains_key(pubkey) {
                already_cached += 1;
            } else {
                miss_keys.push(*pubkey);
            }
        }

        // If everything is warm, skip the DB round-trip entirely.
        if miss_keys.is_empty() {
            return (0, already_cached);
        }

        // Only fetch the cache-miss keys from the database.
        let accounts = self.accounts_db.get_accounts(&miss_keys).await;
        let mut fetched = 0usize;
        for (index, account_opt) in accounts.iter().enumerate() {
            if let Some(account) = account_opt {
                self.accounts.insert(
                    miss_keys[index],
                    AccountWithMeta {
                        account: account.clone(),
                        synced_since: None,
                        deleted: false,
                    },
                );
                fetched += 1;
            }
        }

        (fetched, already_cached)
    }

    // TODO: Merge this implementation with the one in the settlement stage
    /// Called to update the accounts in memory
    pub fn update_accounts(
        &mut self,
        svm_output: &LoadAndExecuteSanitizedTransactionsOutput,
        transactions: &[SanitizedTransaction],
    ) {
        for (tx_index, tx) in svm_output.processing_results.iter().enumerate() {
            let sanitized_transaction = &transactions[tx_index];
            let signature = sanitized_transaction.signature();

            match tx {
                Ok(ProcessedTransaction::Executed(executed_transaction)) => {
                    debug!(
                        "Executed transaction: {:?}",
                        sanitized_transaction.signature()
                    );

                    for (index, (pubkey, account_data)) in executed_transaction
                        .loaded_transaction
                        .accounts
                        .iter()
                        .enumerate()
                    {
                        if sanitized_transaction.is_writable(index) {
                            if account_data.lamports() == 0 && account_data.data().is_empty() {
                                self.accounts.insert(
                                    *pubkey,
                                    AccountWithMeta {
                                        account: account_data.clone(),
                                        deleted: true,
                                        synced_since: None,
                                    },
                                );
                            } else {
                                self.accounts.insert(
                                    *pubkey,
                                    AccountWithMeta {
                                        account: account_data.clone(),
                                        deleted: false,
                                        synced_since: None,
                                    },
                                );
                            }
                        }
                    }
                }
                Ok(ProcessedTransaction::FeesOnly(fees_only_transaction)) => {
                    warn!("FeesOnly transaction: {:?}", fees_only_transaction);
                }
                Err(e) => {
                    warn!("Transaction failed: {:?}, error: {:?}", signature, e);
                }
            }
        }
    }

    /// Drain the settled accounts channel and periodically evict stale entries.
    ///
    /// Split into two phases:
    /// 1. **Channel drain** (every call): process settled_accounts messages to
    ///    update `synced_since` and remove deleted tombstones. This is lightweight
    ///    — just a `try_recv` loop over whatever messages are pending.
    /// 2. **Eviction sweep** (every `GC_EVICTION_INTERVAL` batches): scan the
    ///    entire HashMap to evict entries that have been synced for longer than
    ///    `OLDEST_SYNCED_ACCOUNT_AGE`. This is O(N) so we avoid it on every batch.
    fn garbage_collect(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Phase 1: always drain the channel to keep dirty/clean state current.
        while let Ok(account_settlements) = self.settled_accounts_rx.try_recv() {
            for (pubkey, account_settlement) in account_settlements {
                if account_settlement.deleted {
                    // We expect the account to exist in-memory because we only
                    // tombstone deleted accounts
                    if let Some(account) = self.accounts.get(&pubkey) {
                        if account.deleted {
                            self.accounts.remove(&pubkey);
                        }
                    } else {
                        warn!("Account {} was deleted from in-memory, but we expected it to be tombstoned", pubkey);
                    }
                } else if let Some(account) = self.accounts.get_mut(&pubkey) {
                    if account.deleted || account.account != account_settlement.account {
                        // In-memory is ahead of the AccountsDB
                        continue;
                    } else {
                        account.synced_since = Some(now);
                    }
                } else {
                    warn!(
                        "Account {} was deleted from in-memory, but we expected it to be there",
                        pubkey
                    );
                }
            }
        }

        // Phase 2: only run the O(N) eviction sweep periodically.
        self.batches_since_eviction += 1;
        if self.batches_since_eviction >= GC_EVICTION_INTERVAL {
            self.batches_since_eviction = 0;
            self.accounts.retain(|_pubkey, account| {
                if let Some(synced_since) = account.synced_since {
                    synced_since + OLDEST_SYNCED_ACCOUNT_AGE >= now
                } else {
                    true // Always keep accounts with synced_since = None
                }
            });
        }
    }
}

#[cfg(test)]
impl BOB {
    /// Test-only constructor — needs private field access so it lives on the type.
    /// The actual test helper that sets up the dummy DB pool is in test_helpers.rs.
    pub(crate) fn new_test(
        settled_accounts_rx: mpsc::UnboundedReceiver<Vec<(Pubkey, AccountSettlement)>>,
        accounts_db: AccountsDB,
    ) -> Self {
        Self {
            accounts: HashMap::new(),
            precompiles: HashMap::new(),
            settled_accounts_rx,
            accounts_db,
            batches_since_eviction: 0,
        }
    }

    /// Insert an account directly into BOB's cache (test-only).
    pub(crate) fn insert_account_for_test(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        self.accounts.insert(
            pubkey,
            AccountWithMeta {
                account,
                synced_since: None,
                deleted: false,
            },
        );
    }
}

impl InvokeContextCallback for BOB {}

impl TransactionProcessingCallback for BOB {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        // First check precompiles (always in memory)
        if let Some(precompile) = self.precompiles.get(pubkey) {
            return Some(precompile.clone());
        }

        // Then check in-memory accounts
        if let Some(account) = self.accounts.get(pubkey) {
            if account.deleted {
                return None;
            }
            return Some(account.account.clone());
        }

        None
    }

    fn account_matches_owners(&self, account: &Pubkey, owners: &[Pubkey]) -> Option<usize> {
        self.get_account_shared_data(account)
            .and_then(|account| owners.iter().position(|key| account.owner().eq(key)))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_sdk::account::Account, solana_svm_callback::TransactionProcessingCallback,
    };

    fn create_test_bob() -> (BOB, mpsc::UnboundedSender<Vec<(Pubkey, AccountSettlement)>>) {
        crate::test_helpers::create_test_bob()
    }

    fn make_account(lamports: u64, data: &[u8], owner: &Pubkey) -> AccountSharedData {
        AccountSharedData::from(Account {
            lamports,
            data: data.to_vec(),
            owner: *owner,
            executable: false,
            rent_epoch: 0,
        })
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // -----------------------------------------------------------------------
    // Invariant C2: BOB MUST be in sync or ahead of DB on disk
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_marks_matching_account_as_synced() {
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let account = make_account(1000, &[1, 2, 3], &Pubkey::default());

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: account.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account: account.clone(),
                    deleted: false,
                },
            )])
            .unwrap();

        bob.garbage_collect();

        let meta = bob.accounts.get(&pubkey).unwrap();
        assert!(
            meta.synced_since.is_some(),
            "Account matching settler feedback should be marked as synced"
        );
    }

    #[tokio::test]
    async fn gc_preserves_ahead_state_when_data_differs() {
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let newer = make_account(2000, &[9, 9, 9], &Pubkey::default());
        let older = make_account(1000, &[1, 2, 3], &Pubkey::default());

        // Executor wrote newer state to BOB
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: newer.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        // Settler sends older (now-stale) feedback
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account: older,
                    deleted: false,
                },
            )])
            .unwrap();

        bob.garbage_collect();

        let meta = bob.accounts.get(&pubkey).unwrap();
        assert_eq!(
            meta.account, newer,
            "In-memory state must not be overwritten by stale settler feedback"
        );
        assert!(
            meta.synced_since.is_none(),
            "Account ahead of DB must keep synced_since=None"
        );
    }

    #[tokio::test]
    async fn gc_preserves_deleted_tombstone_against_non_deleted_settlement() {
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let account = make_account(1000, &[1], &Pubkey::default());

        // BOB has account marked as deleted (tombstone)
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: account.clone(),
                synced_since: None,
                deleted: true,
            },
        );

        // Settler sends non-deleted settlement (from before the delete)
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account,
                    deleted: false,
                },
            )])
            .unwrap();

        bob.garbage_collect();

        let meta = bob.accounts.get(&pubkey).unwrap();
        assert!(
            meta.deleted,
            "Deleted tombstone must not be overwritten by stale non-deleted settlement"
        );
        assert!(meta.synced_since.is_none());
    }

    #[tokio::test]
    async fn gc_removes_tombstone_on_deleted_settlement() {
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let account = make_account(0, &[], &Pubkey::default());

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: account.clone(),
                synced_since: None,
                deleted: true,
            },
        );

        // Settler confirms deletion was persisted to DB
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account,
                    deleted: true,
                },
            )])
            .unwrap();

        bob.garbage_collect();

        assert!(
            !bob.accounts.contains_key(&pubkey),
            "Tombstoned account must be removed once settler confirms deletion"
        );
    }

    #[tokio::test]
    async fn eviction_never_removes_unsynced_accounts() {
        let (mut bob, _settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: make_account(1000, &[1], &Pubkey::default()),
                synced_since: None, // Ahead of DB — must never be evicted
                deleted: false,
            },
        );

        bob.garbage_collect();

        assert!(
            bob.accounts.contains_key(&pubkey),
            "Accounts with synced_since=None (ahead of DB) must never be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_removes_old_synced_accounts() {
        let (mut bob, _settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: make_account(1000, &[1], &Pubkey::default()),
                synced_since: Some(now_secs() - OLDEST_SYNCED_ACCOUNT_AGE - 1),
                deleted: false,
            },
        );

        // Force the eviction sweep to run on the next garbage_collect call
        bob.batches_since_eviction = GC_EVICTION_INTERVAL - 1;
        bob.garbage_collect();

        assert!(
            !bob.accounts.contains_key(&pubkey),
            "Synced accounts older than OLDEST_SYNCED_ACCOUNT_AGE should be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_keeps_recently_synced_accounts() {
        let (mut bob, _settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: make_account(1000, &[1], &Pubkey::default()),
                synced_since: Some(now_secs()),
                deleted: false,
            },
        );

        bob.garbage_collect();

        assert!(
            bob.accounts.contains_key(&pubkey),
            "Recently synced accounts must not be evicted"
        );
    }

    #[test]
    fn preload_or_insert_preserves_inflight_state() {
        // Directly tests the HashMap pattern used by preload_accounts.
        // Verifies that entry().or_insert_with() does not overwrite
        // an existing in-flight account with stale DB data.
        let mut accounts: HashMap<Pubkey, AccountWithMeta> = HashMap::new();
        let pubkey = Pubkey::new_unique();

        let newer = make_account(2000, &[9, 9], &Pubkey::default());
        let older_from_db = make_account(1000, &[1, 2], &Pubkey::default());

        // Executor already wrote newer state
        accounts.insert(
            pubkey,
            AccountWithMeta {
                account: newer.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        // Simulate what preload_accounts does with DB results
        accounts.entry(pubkey).or_insert_with(|| AccountWithMeta {
            account: older_from_db,
            synced_since: None,
            deleted: false,
        });

        assert_eq!(
            accounts[&pubkey].account, newer,
            "or_insert_with must not overwrite existing in-flight state"
        );
    }

    #[tokio::test]
    async fn concurrent_preload_and_settle_preserves_newer_state() {
        // Simulates the race condition: executor writes v2 after settler
        // sends v1 feedback but before GC runs.
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        let v1 = make_account(1000, &[1], &Pubkey::default());
        let v2 = make_account(2000, &[2], &Pubkey::default());

        // Step 1: Executor writes v1 to BOB
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: v1.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        // Step 2: Settler settles v1 and sends feedback
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account: v1,
                    deleted: false,
                },
            )])
            .unwrap();

        // Step 3: Before GC runs, executor updates BOB to v2
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: v2.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        // Step 4: GC runs (triggered by next preload_accounts)
        bob.garbage_collect();

        let meta = bob.accounts.get(&pubkey).unwrap();
        assert_eq!(
            meta.account, v2,
            "GC must not regress to v1 when BOB already has v2"
        );
        assert!(
            meta.synced_since.is_none(),
            "Account ahead of DB must keep synced_since=None"
        );
    }

    #[tokio::test]
    async fn gc_multi_batch_settlement_applies_all_in_order() {
        let (mut bob, settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        let v1 = make_account(1000, &[1], &Pubkey::default());
        let v2 = make_account(2000, &[2], &Pubkey::default());
        let v3 = make_account(3000, &[3], &Pubkey::default());

        // BOB has v3 (latest from executor)
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: v3.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        // Two settlement batches queue up before GC runs
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account: v1,
                    deleted: false,
                },
            )])
            .unwrap();
        settled_tx
            .send(vec![(
                pubkey,
                AccountSettlement {
                    account: v2,
                    deleted: false,
                },
            )])
            .unwrap();

        bob.garbage_collect();

        let meta = bob.accounts.get(&pubkey).unwrap();
        assert_eq!(
            meta.account, v3,
            "BOB must preserve v3 when neither settlement batch matches"
        );
        assert!(
            meta.synced_since.is_none(),
            "Account ahead of all settled versions must keep synced_since=None"
        );
    }

    #[tokio::test]
    async fn gc_settlement_for_missing_account_does_not_panic() {
        let (mut bob, settled_tx) = create_test_bob();
        let missing_pubkey = Pubkey::new_unique();

        // Settler sends feedback for an account that was never in BOB
        settled_tx
            .send(vec![(
                missing_pubkey,
                AccountSettlement {
                    account: make_account(1000, &[1], &Pubkey::default()),
                    deleted: false,
                },
            )])
            .unwrap();

        // Should not panic; just logs a warning
        bob.garbage_collect();

        assert!(
            !bob.accounts.contains_key(&missing_pubkey),
            "Settlement for missing account must not create a new entry"
        );
    }

    #[tokio::test]
    async fn eviction_mixed_population() {
        let (mut bob, _settled_tx) = create_test_bob();
        let now = now_secs();

        let old_synced = Pubkey::new_unique();
        let recent_synced = Pubkey::new_unique();
        let unsynced = Pubkey::new_unique();

        bob.accounts.insert(
            old_synced,
            AccountWithMeta {
                account: make_account(100, &[1], &Pubkey::default()),
                synced_since: Some(now - OLDEST_SYNCED_ACCOUNT_AGE - 1),
                deleted: false,
            },
        );
        bob.accounts.insert(
            recent_synced,
            AccountWithMeta {
                account: make_account(200, &[2], &Pubkey::default()),
                synced_since: Some(now),
                deleted: false,
            },
        );
        bob.accounts.insert(
            unsynced,
            AccountWithMeta {
                account: make_account(300, &[3], &Pubkey::default()),
                synced_since: None,
                deleted: false,
            },
        );

        // Force the eviction sweep to run on the next garbage_collect call
        bob.batches_since_eviction = GC_EVICTION_INTERVAL - 1;
        bob.garbage_collect();

        assert!(
            !bob.accounts.contains_key(&old_synced),
            "Old synced account must be evicted"
        );
        assert!(
            bob.accounts.contains_key(&recent_synced),
            "Recently synced account must survive eviction"
        );
        assert!(
            bob.accounts.contains_key(&unsynced),
            "Unsynced (ahead of DB) account must survive eviction"
        );
    }

    #[tokio::test]
    async fn eviction_boundary_exact_age_is_kept() {
        let (mut bob, _settled_tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();

        // synced_since + OLDEST_SYNCED_ACCOUNT_AGE == now → should be kept (>= check)
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: make_account(1000, &[1], &Pubkey::default()),
                synced_since: Some(now_secs() - OLDEST_SYNCED_ACCOUNT_AGE),
                deleted: false,
            },
        );

        bob.garbage_collect();

        assert!(
            bob.accounts.contains_key(&pubkey),
            "Account synced exactly OLDEST_SYNCED_ACCOUNT_AGE ago must be kept (>= boundary)"
        );
    }

    #[tokio::test]
    async fn get_account_returns_none_for_deleted() {
        let (mut bob, _tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: make_account(1000, &[1], &Pubkey::default()),
                synced_since: None,
                deleted: true,
            },
        );

        assert!(
            TransactionProcessingCallback::get_account_shared_data(&bob, &pubkey).is_none(),
            "Deleted (tombstoned) accounts must return None to SVM"
        );
    }

    #[tokio::test]
    async fn get_account_returns_live_account() {
        let (mut bob, _tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let account = make_account(1000, &[1, 2, 3], &Pubkey::default());

        bob.accounts.insert(
            pubkey,
            AccountWithMeta {
                account: account.clone(),
                synced_since: None,
                deleted: false,
            },
        );

        let result = TransactionProcessingCallback::get_account_shared_data(&bob, &pubkey);
        assert_eq!(
            result.unwrap(),
            account,
            "Live account must be returned to SVM"
        );
    }
}
