use {
    super::{
        get_accounts::AccountLoadError, postgres::PostgresAccountsDB, redis::RedisAccountsDB,
        types::StoredTransaction,
    },
    crate::stages::AccountSettlement,
    anyhow::Result,
    serde::{Deserialize, Serialize},
    solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature,
    solana_sdk::{
        account::AccountSharedData, clock::UnixTimestamp, hash::Hash, pubkey::Pubkey,
        signature::Signature, transaction::SanitizedTransaction,
    },
    solana_svm::transaction_processing_result::ProcessedTransaction,
};

/// Block metadata stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub slot: u64,
    pub blockhash: Hash,
    pub previous_blockhash: Hash,
    pub parent_slot: u64,
    pub block_height: Option<u64>,
    pub block_time: Option<i64>,
    /// Transaction signatures in this block, in order
    pub transaction_signatures: Vec<Signature>,
    /// The recent_blockhash each transaction referenced, parallel to transaction_signatures.
    /// Used to rebuild the dedup cache on restart.
    pub transaction_recent_blockhashes: Vec<Hash>,
    /// The message hash of each transaction, parallel to transaction_signatures.
    /// This is the dedup replay identity, rebuilt into the cache on restart so a
    /// bounce cannot reopen the replay window within blockhash validity.
    pub transaction_message_hashes: Vec<Hash>,
}

/// How a node reaches stored state.
///
/// # Variants
///
/// * `Postgres`: the source of truth for all finalized state, with ACID
///   transactions. Every node has one.
///
/// * `Redis`: a cache in front of that same Postgres, which it carries so a key
///   the cache cannot answer is resolved rather than reported absent. Only the
///   reads where a miss is detectable are served from it: point lookups by
///   pubkey, signature and slot, plus the chain tip. Ranges, history and counters
///   go to Postgres, because a short answer from a partial mirror is
///   indistinguishable from a complete one.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum AccountsDB {
    Postgres(PostgresAccountsDB),
    Redis(RedisAccountsDB),
}

impl AccountsDB {
    pub async fn get_account_shared_data(
        &self,
        pubkey: &Pubkey,
    ) -> Result<Option<AccountSharedData>, AccountLoadError> {
        super::get_account_shared_data::get_account_shared_data(self, pubkey).await
    }

    pub async fn set_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        super::set_account::set_account(self, pubkey, account).await
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> Result<Option<StoredTransaction>> {
        super::get_transaction::get_transaction(self, signature).await
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        limit: usize,
        before: Option<&Signature>,
        until: Option<&Signature>,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        super::get_signatures_for_address::get_signatures_for_address(
            self, address, limit, before, until,
        )
        .await
    }

    pub async fn get_latest_slot(&self) -> Result<Option<u64>> {
        super::get_latest_slot::get_latest_slot(self).await
    }

    pub async fn get_block_height(&self) -> Result<Option<u64>> {
        super::get_block_height::get_block_height(self).await
    }

    pub async fn get_current_slot(&self) -> Result<Option<u64>> {
        super::current_slot::get_current_slot(self).await
    }

    pub async fn store_block(&mut self, block_info: BlockInfo) -> Result<(), String> {
        super::store_block::store_block(self, block_info).await
    }

    pub async fn get_block(&self, slot: u64) -> Result<Option<BlockInfo>> {
        super::get_block::get_block(self, slot).await
    }

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        super::get_latest_blockhash::get_latest_blockhash(self).await
    }

    pub async fn get_transaction_count(&self) -> Result<u64> {
        super::get_transaction_count::get_transaction_count(self).await
    }

    pub async fn get_first_available_block(&self) -> Result<u64> {
        super::get_first_available_block::get_first_available_block(self).await
    }

    pub async fn get_blocks(&self, start_slot: u64, end_slot: Option<u64>) -> Result<Vec<u64>> {
        super::get_blocks::get_blocks(self, start_slot, end_slot).await
    }

    pub async fn get_blocks_with_limit(&self, start_slot: u64, limit: u64) -> Result<Vec<u64>> {
        super::get_blocks_with_limit::get_blocks_with_limit(self, start_slot, limit).await
    }

    pub async fn get_blocks_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<Vec<BlockInfo>> {
        super::get_blocks_in_range::get_blocks_in_range(self, start_slot, end_slot).await
    }

    pub async fn get_last_blocks(&self, limit: usize) -> Result<Vec<BlockInfo>> {
        super::get_last_blocks::get_last_blocks(self, limit).await
    }

    pub async fn get_epoch_info(&self) -> Result<crate::rpc::api::EpochInfo> {
        super::get_epoch_info::get_epoch_info(self).await
    }

    pub async fn write_batch(
        &mut self,
        account_settlements: &[(Pubkey, AccountSettlement)],
        transactions: Vec<(
            Signature,
            &SanitizedTransaction,
            u64, // slot
            UnixTimestamp,
            &ProcessedTransaction,
        )>,
        block_info: Option<BlockInfo>,
    ) -> Result<Vec<super::write_batch::AddressSignatureRow>, String> {
        super::write_batch::write_batch(self, account_settlements, transactions, block_info).await
    }

    pub async fn get_accounts(
        &self,
        accounts: &[Pubkey],
    ) -> Result<Vec<Option<AccountSharedData>>, AccountLoadError> {
        super::get_accounts::get_accounts(self, accounts).await
    }

    pub async fn store_performance_sample(
        &mut self,
        sample: solana_rpc_client_types::response::RpcPerfSample,
    ) -> Result<()> {
        super::store_performance_sample::store_performance_sample(self, sample).await
    }

    pub async fn get_recent_performance_samples(
        &self,
        limit: usize,
    ) -> Result<Vec<solana_rpc_client_types::response::RpcPerfSample>> {
        super::get_recent_performance_samples::get_recent_performance_samples(self, limit).await
    }

    pub async fn get_block_time(&self, slot: u64) -> Result<Option<i64>> {
        super::get_block_time::get_block_time(self, slot).await
    }
}

impl AccountsDB {
    pub async fn new(accountsdb_connection_url: &str, read_only: bool) -> Result<Self> {
        if accountsdb_connection_url.starts_with("postgresql://")
            || accountsdb_connection_url.starts_with("postgres://")
        {
            Ok(AccountsDB::Postgres(
                PostgresAccountsDB::new(accountsdb_connection_url, read_only)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create PostgresAccountsDB: {}", e))?,
            ))
        } else if accountsdb_connection_url.starts_with("redis://") {
            // Redis is a cache, never a source of truth: on its own it cannot
            // tell a missing key from a deleted one, and nothing verifies its
            // contents belong to this ledger. A caching node passes a Postgres
            // URL here and its Redis URL separately.
            Err(anyhow::anyhow!(
                "Redis cannot be used as the accounts database; pass the Postgres URL here and \
                 configure Redis as a cache in front of it"
            ))
        } else {
            Err(anyhow::anyhow!(
                "Unsupported accountsdb connection URL scheme: {}",
                accountsdb_connection_url
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::AccountSettlement;
    use crate::test_helpers::{
        create_test_block_info, create_test_sanitized_transaction, flush_address_signatures_sync,
        start_test_postgres, start_test_redis,
    };
    use redis::AsyncCommands;
    use solana_sdk::account::{AccountSharedData, ReadableAccount};
    use solana_sdk::signature::{Keypair, Signer};
    use solana_svm::account_loader::LoadedTransaction;
    use solana_svm::transaction_execution_result::{
        ExecutedTransaction, TransactionExecutionDetails,
    };
    use std::collections::HashMap;
    use std::str::FromStr;

    #[tokio::test(flavor = "multi_thread")]
    async fn unsupported_url_scheme_rejected() {
        let result = AccountsDB::new("ftp://localhost/db", false).await;
        assert!(result.is_err());
        let msg = format!("{}", result.err().unwrap());
        assert!(
            msg.contains("Unsupported"),
            "expected unsupported scheme error, got: {msg}"
        );
    }

    /// Redis on its own cannot tell a missing key from a deleted one, and
    /// nothing verifies its contents belong to this ledger, so it is never the
    /// accounts database. It is configured as a cache in front of Postgres.
    #[tokio::test(flavor = "multi_thread")]
    async fn redis_rejected_as_the_accounts_database() {
        let result = AccountsDB::new("redis://localhost:6379", true).await;
        assert!(result.is_err());
        let msg = format!("{}", result.err().unwrap());
        assert!(
            msg.contains("Redis cannot be used as the accounts database"),
            "expected Redis rejection, got: {msg}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn set_and_get_account_round_trip() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(42_000, 0, &owner);

        // miss before set
        assert!(db.get_account_shared_data(&pubkey).await.unwrap().is_none());

        db.set_account(pubkey, account.clone()).await;

        let loaded = db.get_account_shared_data(&pubkey).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(
            solana_sdk::account::ReadableAccount::lamports(&loaded.unwrap()),
            42_000
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_accounts_batch_partial_hit() {
        let (mut db, _pg) = start_test_postgres().await;

        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        let acct = AccountSharedData::new(1, 0, &Pubkey::new_unique());

        db.set_account(pk2, acct.clone()).await;

        let results = db.get_accounts(&[pk1, pk2, pk3]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_none(), "pk1 was never stored");
        assert!(results[1].is_some(), "pk2 should be found");
        assert!(results[2].is_none(), "pk3 was never stored");
    }

    /// Both read paths must report absence at zero lamports and still return the
    /// 1-lamport floor. `get_accounts` is positional, so indices stay aligned.
    #[tokio::test(flavor = "multi_thread")]
    async fn zero_lamport_row_reads_as_absent() {
        let (mut db, _pg) = start_test_postgres().await;

        let zero = Pubkey::new_unique();
        let floor = Pubkey::new_unique();
        let never_stored = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        db.set_account(zero, AccountSharedData::new(0, 8, &owner))
            .await;
        db.set_account(floor, AccountSharedData::new(1, 8, &owner))
            .await;

        assert!(
            db.get_account_shared_data(&zero).await.unwrap().is_none(),
            "a zero-lamport row must read as absent"
        );
        assert!(
            db.get_account_shared_data(&floor).await.unwrap().is_some(),
            "an account on the 1-lamport floor must still read back"
        );

        let results = db.get_accounts(&[zero, floor, never_stored]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_none(), "the zero-lamport row is filtered out");
        assert!(
            results[1].is_some(),
            "the floor account survives the filter"
        );
        assert!(results[2].is_none(), "never_stored was never written");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_block_and_get_block_round_trip() {
        let (mut db, _pg) = start_test_postgres().await;

        let blockhash = Hash::new_unique();
        let block = create_test_block_info(10, blockhash);

        db.store_block(block.clone()).await.unwrap();

        let loaded = db.get_block(10).await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.slot, 10);
        assert_eq!(loaded.blockhash, blockhash);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_block_miss_returns_none() {
        let (db, _pg) = start_test_postgres().await;
        assert!(db.get_block(999).await.unwrap().is_none());
    }

    /// The chain counters are read back from `metadata`, which is what lets the
    /// slot keep advancing while blocks are sparse and the height count blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_batch_persists_the_chain_counters() {
        let (mut db, _pg) = start_test_postgres().await;

        let mut block = create_test_block_info(40, Hash::new_unique());
        block.block_height = Some(7);
        db.write_batch(&[], vec![], Some(block)).await.unwrap();

        assert_eq!(db.get_latest_slot().await.unwrap(), Some(40));
        assert_eq!(db.get_block_height().await.unwrap(), Some(7));
    }

    /// A node upgrading in place has no counters yet. Both fall back to the last
    /// stored block, whose height was its slot before this change, so the live
    /// counters continue the old sequence rather than restarting it.
    #[tokio::test(flavor = "multi_thread")]
    async fn chain_counters_fall_back_to_the_last_stored_block() {
        let (mut db, _pg) = start_test_postgres().await;
        db.store_block(create_test_block_info(12, Hash::new_unique()))
            .await
            .unwrap();

        // Delete the counters to reproduce a ledger written by the old build,
        // which stored blocks but no counters at all.
        let AccountsDB::Postgres(ref postgres_db) = db else {
            panic!("expected Postgres variant")
        };
        sqlx::query(
            "DELETE FROM metadata WHERE key IN ('latest_slot', 'current_slot', 'block_height')",
        )
        .execute(postgres_db.pool.as_ref())
        .await
        .unwrap();

        assert_eq!(db.get_latest_slot().await.unwrap(), Some(12));
        assert_eq!(db.get_current_slot().await.unwrap(), Some(12));
        assert_eq!(db.get_block_height().await.unwrap(), Some(12));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_latest_slot_empty_then_populated() {
        let (mut db, _pg) = start_test_postgres().await;

        // empty DB → None
        let slot = db.get_latest_slot().await.unwrap();
        assert_eq!(slot, None);

        // store a block
        db.store_block(create_test_block_info(5, Hash::new_unique()))
            .await
            .unwrap();

        let slot = db.get_latest_slot().await.unwrap();
        assert_eq!(slot, Some(5));

        // store higher block
        db.store_block(create_test_block_info(12, Hash::new_unique()))
            .await
            .unwrap();
        let slot = db.get_latest_slot().await.unwrap();
        assert_eq!(slot, Some(12));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_latest_blockhash_after_store() {
        let (mut db, _pg) = start_test_postgres().await;

        // no blockhash stored yet → error
        let err = db.get_latest_blockhash().await;
        assert!(err.is_err());

        let bh = Hash::new_unique();
        db.store_block(create_test_block_info(1, bh)).await.unwrap();

        let loaded = db.get_latest_blockhash().await.unwrap();
        assert_eq!(loaded, bh);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_blocks_returns_slot_numbers_in_order() {
        let (mut db, _pg) = start_test_postgres().await;

        for slot in [3, 7, 1, 10] {
            db.store_block(create_test_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }

        let slots = db.get_blocks(0, Some(20)).await.unwrap();
        assert_eq!(slots, vec![1, 3, 7, 10]);
    }

    /// A read replica has no live blockhash window and answers both the hash and
    /// the height from the same mirrored tip, so the deadline it publishes matches
    /// the hash it just served.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_replica_reports_a_consistent_height() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let blockhash = Hash::new_unique();
        let mut block = create_test_block_info(40, blockhash);
        block.block_height = Some(7);
        pg_db
            .write_batch(&[], vec![], Some(block.clone()))
            .await
            .unwrap();
        crate::accounts::write_batch::write_batch_redis(
            &mut redis_raw,
            &[],
            vec![],
            Some(block.clone()),
        )
        .await
        .unwrap();

        let replica = AccountsDB::Redis(redis_raw);
        assert_eq!(replica.get_latest_blockhash().await.unwrap(), blockhash);
        assert_eq!(replica.get_block_height().await.unwrap(), Some(7));
        assert_eq!(replica.get_latest_slot().await.unwrap(), Some(40));
        assert_eq!(replica.get_current_slot().await.unwrap(), Some(40));
    }

    /// Point lookups resolve a cache miss against Postgres instead of reporting
    /// the data absent. A key missing from the cache is indistinguishable from a
    /// deleted one, which is how a cache-backed node could serve a real funded
    /// account as nonexistent.
    #[tokio::test(flavor = "multi_thread")]
    async fn point_lookups_through_cache_resolve_against_postgres() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        // A cache is only read from while it names this deployment. Unstamped,
        // these reads would skip Redis instead of missing in it, which is not
        // the path under test.
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let cache_db = AccountsDB::Redis(redis_raw);

        let pubkey = Pubkey::new_unique();
        let lamports = 500;
        let account = AccountSharedData::new(lamports, 0, &Pubkey::new_unique());
        let keypair = Keypair::new();
        let tx = create_test_sanitized_transaction(&keypair, &Pubkey::new_unique(), 1);
        let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }));
        let slot = 7u64;
        let blockhash = Hash::new_unique();

        // Written to Postgres only, never to the cache.
        pg_db
            .write_batch(
                &[(
                    pubkey,
                    AccountSettlement {
                        account,
                        deleted: false,
                    },
                )],
                vec![(*tx.signature(), &tx, slot, 1_700_000_000, &processed)],
                Some(create_test_block_info(slot, blockhash)),
            )
            .await
            .unwrap();

        assert_eq!(
            cache_db
                .get_account_shared_data(&pubkey)
                .await
                .unwrap()
                .map(|account| account.lamports()),
            Some(lamports),
            "a cached miss must not read as a nonexistent account"
        );
        assert_eq!(
            cache_db.get_accounts(&[pubkey]).await.unwrap()[0]
                .as_ref()
                .map(|account| account.lamports()),
            Some(lamports)
        );
        assert!(cache_db
            .get_transaction(tx.signature())
            .await
            .unwrap()
            .is_some());
        assert!(cache_db.get_block(slot).await.unwrap().is_some());
        assert_eq!(cache_db.get_latest_slot().await.unwrap(), Some(slot));
        assert_eq!(cache_db.get_latest_blockhash().await.unwrap(), blockhash);
        assert_eq!(cache_db.get_transaction_count().await.unwrap(), 1);
    }

    /// A batch read where only some keys are cached must resolve the rest
    /// against Postgres and keep every result on its original key. Distinct
    /// balances make a positional mix-up visible: returning the right accounts
    /// against the wrong keys is the failure this guards.
    #[tokio::test(flavor = "multi_thread")]
    async fn partially_cached_batch_read_stays_aligned_with_its_keys() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        // A cache is only read from while it names this deployment, and this
        // test needs the one cached entry to actually be served.
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let owner = Pubkey::new_unique();
        let first = (Pubkey::new_unique(), 100u64);
        let second = (Pubkey::new_unique(), 200u64);
        let third = (Pubkey::new_unique(), 300u64);
        let absent = Pubkey::new_unique();
        // The cached balance differs from the row behind it, so the assertion
        // can tell a cache hit from a fallback read at the same position.
        let second_cached_lamports = 250u64;

        for (pubkey, lamports) in [first, second, third] {
            pg_db
                .set_account(pubkey, AccountSharedData::new(lamports, 0, &owner))
                .await;
        }

        // Only the middle account is cached, so the batch is a hit sandwiched
        // between two misses.
        redis_raw
            .set_account(
                second.0,
                AccountSharedData::new(second_cached_lamports, 0, &owner),
            )
            .await;
        let cache_db = AccountsDB::Redis(redis_raw);

        let results = cache_db
            .get_accounts(&[first.0, second.0, third.0, absent])
            .await
            .unwrap();

        let lamports: Vec<Option<u64>> = results
            .iter()
            .map(|account| account.as_ref().map(|account| account.lamports()))
            .collect();
        assert_eq!(
            lamports,
            vec![
                Some(first.1),
                Some(second_cached_lamports),
                Some(third.1),
                None
            ],
            "each result must stay on the key it was asked for"
        );
    }

    /// The write node condemns a cache it has stopped maintaining by clearing the
    /// deployment stamp. That only helps if read nodes look again: one that
    /// checked at startup and never rechecked would keep serving the stale keys
    /// until someone restarted it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_condemned_cache_stops_being_served() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let postgres_db = postgres_db.clone();
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let settled_lamports = 900;
        let stale_lamports = 100;
        pg_db
            .set_account(pubkey, AccountSharedData::new(settled_lamports, 0, &owner))
            .await;
        redis_raw
            .set_account(pubkey, AccountSharedData::new(stale_lamports, 0, &owner))
            .await;

        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(&postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();
        let cache_db = AccountsDB::Redis(redis_raw);
        assert_eq!(
            cache_db
                .get_account_shared_data(&pubkey)
                .await
                .unwrap()
                .map(|account| account.lamports()),
            Some(stale_lamports),
            "a stamped cache is served, stale value and all"
        );

        let AccountsDB::Redis(ref redis_db) = cache_db else {
            panic!("expected Redis variant")
        };
        crate::accounts::redis_coherence::clear_deployment_id(redis_db)
            .await
            .unwrap();

        assert_eq!(
            cache_db
                .get_account_shared_data(&pubkey)
                .await
                .unwrap()
                .map(|account| account.lamports()),
            Some(settled_lamports),
            "once condemned, reads must bypass the cache and reach Postgres"
        );
    }

    /// A cache entry that will not deserialize is a miss, and it stays a miss
    /// forever unless it is removed: every later read would pay both the Redis
    /// hop and the Postgres hop. Reading it once must evict it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_corrupt_cache_entry_is_evicted_when_it_is_read() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        // A cache is only read from while it names this deployment, and this test
        // needs the corrupt entry to actually be read.
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let pubkey = Pubkey::new_unique();
        let lamports = 700;
        pg_db
            .set_account(
                pubkey,
                AccountSharedData::new(lamports, 0, &Pubkey::new_unique()),
            )
            .await;

        let key = format!("account:{}", pubkey);
        let mut conn = redis_raw.connection.clone();
        let _: () = conn.set(&key, vec![0xffu8; 8]).await.unwrap();

        let cache_db = AccountsDB::Redis(redis_raw);
        assert_eq!(
            cache_db
                .get_account_shared_data(&pubkey)
                .await
                .unwrap()
                .map(|account| account.lamports()),
            Some(lamports),
            "a corrupt entry must fall through to Postgres"
        );

        let still_cached: bool = conn.exists(&key).await.unwrap();
        assert!(
            !still_cached,
            "a corrupt entry must be evicted, not re-read on every request"
        );
    }

    /// A cached block or transaction that will not decode must read through to
    /// Postgres, the same as a missing one.
    ///
    /// The stamp tracks the database, not the build, so adding a field to
    /// BlockInfo or StoredTransaction leaves a correctly stamped cache full of
    /// entries the new binary cannot read. Failing the request instead of
    /// falling through would make those slots and signatures error for as long
    /// as the entries sat there.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cached_block_or_transaction_that_cannot_be_decoded_falls_through() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        // A cache is only read from while it names this deployment, and these
        // entries have to actually be read for the fallthrough to be exercised.
        let deployment_id = crate::accounts::redis_coherence::read_deployment_id(postgres_db)
            .await
            .unwrap();
        crate::accounts::redis_coherence::stamp_deployment_id(&redis_raw, &deployment_id)
            .await
            .unwrap();

        let keypair = Keypair::new();
        let tx = create_test_sanitized_transaction(&keypair, &Pubkey::new_unique(), 1);
        let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }));
        let slot = 7u64;
        let blockhash = Hash::new_unique();

        pg_db
            .write_batch(
                &[],
                vec![(*tx.signature(), &tx, slot, 1_700_000_000, &processed)],
                Some(create_test_block_info(slot, blockhash)),
            )
            .await
            .unwrap();

        // Stand in for entries an older build wrote in a layout this one cannot
        // read.
        let mut conn = redis_raw.connection.clone();
        let _: () = conn
            .set(format!("block:{}", slot), vec![0xffu8; 8])
            .await
            .unwrap();
        let _: () = conn
            .set(format!("tx:{}", tx.signature()), vec![0xffu8; 8])
            .await
            .unwrap();

        let cache_db = AccountsDB::Redis(redis_raw);
        assert_eq!(
            cache_db.get_block(slot).await.unwrap().map(|b| b.blockhash),
            Some(blockhash),
            "an undecodable cached block must fall through to Postgres"
        );
        assert!(
            cache_db
                .get_transaction(tx.signature())
                .await
                .unwrap()
                .is_some(),
            "an undecodable cached transaction must fall through to Postgres"
        );
    }

    /// A failed cache write leaves account keys holding pre-batch balances.
    /// Invalidating them turns a stale hit into a miss, which then resolves
    /// against Postgres.
    #[tokio::test(flavor = "multi_thread")]
    async fn invalidating_a_failed_batch_write_stops_serving_stale_balances() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (mut redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let settled_lamports = 900u64;
        let stale_lamports = 100u64;

        // Postgres committed the batch; the cache still holds the pre-batch
        // balance, which is what a failed cache write leaves behind.
        pg_db
            .set_account(pubkey, AccountSharedData::new(settled_lamports, 0, &owner))
            .await;
        redis_raw
            .set_account(pubkey, AccountSharedData::new(stale_lamports, 0, &owner))
            .await;

        let settlement = AccountSettlement {
            account: AccountSharedData::new(settled_lamports, 0, &owner),
            deleted: false,
        };
        crate::accounts::write_batch::invalidate_batch_redis(
            &mut redis_raw,
            &[(pubkey, settlement)],
        )
        .await;

        let cache_db = AccountsDB::Redis(redis_raw);
        assert_eq!(
            cache_db
                .get_account_shared_data(&pubkey)
                .await
                .unwrap()
                .map(|account| account.lamports()),
            Some(settled_lamports),
            "the stale cached balance must no longer be served"
        );
    }

    /// Range reads resolve against Postgres even when the cache holds nothing.
    /// A cached slot index would cover only blocks written since the cache
    /// attached, and a short range is indistinguishable from a complete one,
    /// which is how a Redis-backed streamer could silently skip finalized
    /// blocks.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_reads_through_cache_resolve_against_postgres() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let cache_db = AccountsDB::Redis(redis_raw);

        for slot in [3u64, 7, 1, 10] {
            pg_db
                .store_block(create_test_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }

        // Nothing was ever written to Redis, so every read below has to reach
        // the source of truth rather than report an empty ledger.
        assert_eq!(
            cache_db.get_blocks(0, Some(20)).await.unwrap(),
            vec![1, 3, 7, 10]
        );
        assert_eq!(
            cache_db.get_blocks_with_limit(0, 10).await.unwrap(),
            vec![1, 3, 7, 10]
        );
        assert_eq!(
            cache_db.get_blocks_with_limit(0, 2).await.unwrap(),
            vec![1, 3]
        );
        assert_eq!(cache_db.get_first_available_block().await.unwrap(), 1);
    }

    /// Postgres and Redis must agree exactly here: they are two hand-written
    /// queries against different index structures, which is where they can
    /// silently diverge.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_blocks_with_limit_returns_slots_in_order() {
        let (mut db, _pg) = start_test_postgres().await;

        for slot in [3, 7, 1, 10] {
            db.store_block(create_test_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }

        assert_eq!(
            db.get_blocks_with_limit(0, 10).await.unwrap(),
            vec![1, 3, 7, 10]
        );
        assert_eq!(db.get_blocks_with_limit(0, 2).await.unwrap(), vec![1, 3]);
        assert_eq!(db.get_blocks_with_limit(4, 10).await.unwrap(), vec![7, 10]);
        assert!(db.get_blocks_with_limit(0, 0).await.unwrap().is_empty());
        assert!(db.get_blocks_with_limit(11, 10).await.unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_blocks_in_range_filters_correctly() {
        let (mut db, _pg) = start_test_postgres().await;

        for slot in [5, 10, 15, 20] {
            db.store_block(create_test_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }

        let blocks = db.get_blocks_in_range(8, 18).await.unwrap();
        let slots: Vec<u64> = blocks.iter().map(|b| b.slot).collect();
        assert_eq!(slots, vec![10, 15]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_blocks_in_range_empty_range() {
        let (db, _pg) = start_test_postgres().await;
        let blocks = db.get_blocks_in_range(100, 200).await.unwrap();
        assert!(blocks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_transaction_count_starts_at_zero() {
        let (db, _pg) = start_test_postgres().await;
        let count = db.get_transaction_count().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_transaction_miss() {
        let (db, _pg) = start_test_postgres().await;
        let sig = Signature::new_unique();
        assert!(db.get_transaction(&sig).await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_epoch_info_after_storing_blocks() {
        let (mut db, _pg) = start_test_postgres().await;

        db.store_block(create_test_block_info(42, Hash::new_unique()))
            .await
            .unwrap();

        let info = db.get_epoch_info().await.unwrap();
        assert_eq!(info.absolute_slot, 42);
        assert_eq!(info.block_height, 42);
        assert_eq!(info.epoch, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_first_available_block_after_storing() {
        let (mut db, _pg) = start_test_postgres().await;

        for slot in [10, 5, 20] {
            db.store_block(create_test_block_info(slot, Hash::new_unique()))
                .await
                .unwrap();
        }

        let first = db.get_first_available_block().await.unwrap();
        assert_eq!(first, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_and_get_performance_sample() {
        let (mut db, _pg) = start_test_postgres().await;

        let sample = solana_rpc_client_types::response::RpcPerfSample {
            slot: 100,
            num_transactions: 500,
            num_slots: 10,
            sample_period_secs: 60,
            num_non_vote_transactions: Some(480),
        };

        db.store_performance_sample(sample.clone()).await.unwrap();

        let loaded = db.get_recent_performance_samples(10).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].slot, 100);
        assert_eq!(loaded[0].num_transactions, 500);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_recent_performance_samples_empty() {
        let (db, _pg) = start_test_postgres().await;
        let loaded = db.get_recent_performance_samples(10).await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_block_time_returns_stored_time() {
        let (mut db, _pg) = start_test_postgres().await;

        let block = create_test_block_info(7, Hash::new_unique());
        let expected_time = block.block_time;
        db.store_block(block).await.unwrap();

        let time = db.get_block_time(7).await.unwrap();
        assert_eq!(time, expected_time);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_batch_stores_accounts_and_block() {
        let (mut db, _pg) = start_test_postgres().await;

        let pk = Pubkey::new_unique();
        let acct = AccountSharedData::new(1_000, 0, &Pubkey::new_unique());
        let settlement = AccountSettlement {
            account: acct.clone(),
            deleted: false,
        };

        let bh = Hash::new_unique();
        let block = create_test_block_info(1, bh);

        db.write_batch(&[(pk, settlement)], vec![], Some(block))
            .await
            .unwrap();

        // account was stored
        assert!(db.get_account_shared_data(&pk).await.unwrap().is_some());

        // block was stored
        let loaded = db.get_block(1).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().blockhash, bh);

        // latest blockhash was updated
        assert_eq!(db.get_latest_blockhash().await.unwrap(), bh);
    }

    /// A fully-empty batch (no accounts, no transactions, no block) must be a
    /// silent no-op: no BEGIN/COMMIT round-trip, no error, no observable state
    /// change. Hot path for slots that produce no work, and a regression test
    /// for the short-circuit that skips opening a Postgres transaction.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_batch_empty_inputs_is_noop() {
        let (mut db, _pg) = start_test_postgres().await;

        // Seed a known blockhash so we can detect any unintended mutation.
        let seeded_bh = Hash::new_unique();
        db.write_batch(&[], vec![], Some(create_test_block_info(7, seeded_bh)))
            .await
            .unwrap();
        assert_eq!(db.get_latest_blockhash().await.unwrap(), seeded_bh);

        // Empty batch must not error and must not mutate any observable state.
        db.write_batch(&[], vec![], None).await.unwrap();
        assert_eq!(db.get_latest_blockhash().await.unwrap(), seeded_bh);
        assert!(db.get_block(7).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_found() {
        let (mut db, _pg) = start_test_postgres().await;

        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let tx = create_test_sanitized_transaction(&from, &to, 100);
        let sig = *tx.signature();

        let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }));

        let addr_sig_rows = db
            .write_batch(
                &[],
                vec![(sig, &tx, 7, 1_700_000_000, &processed)],
                Some(create_test_block_info(7, Hash::new_unique())),
            )
            .await
            .unwrap();
        flush_address_signatures_sync(&db, &addr_sig_rows).await;

        let results = db
            .get_signatures_for_address(&from.pubkey(), 10, None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].signature, sig.to_string());
        assert_eq!(results[0].slot, 7);
        assert!(results[0].err.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_empty() {
        let (db, _pg) = start_test_postgres().await;
        let results = db
            .get_signatures_for_address(&Pubkey::new_unique(), 10, None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_same_slot_ordered_by_signature_desc() {
        let (mut db, _pg) = start_test_postgres().await;

        let to = Pubkey::new_unique();

        // Three different senders, all to the same recipient, all in slot 5.
        let from_a = Keypair::new();
        let from_b = Keypair::new();
        let from_c = Keypair::new();
        let tx_a = create_test_sanitized_transaction(&from_a, &to, 1);
        let tx_b = create_test_sanitized_transaction(&from_b, &to, 1);
        let tx_c = create_test_sanitized_transaction(&from_c, &to, 1);
        let sig_a = *tx_a.signature();
        let sig_b = *tx_b.signature();
        let sig_c = *tx_c.signature();

        let make_processed = || {
            ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
                loaded_transaction: LoadedTransaction {
                    accounts: vec![],
                    ..Default::default()
                },
                execution_details: TransactionExecutionDetails {
                    status: Ok(()),
                    log_messages: None,
                    inner_instructions: None,
                    return_data: None,
                    executed_units: 0,
                    accounts_data_len_delta: 0,
                },
                programs_modified_by_tx: HashMap::new(),
            }))
        };

        let addr_sig_rows = db
            .write_batch(
                &[],
                vec![
                    (sig_a, &tx_a, 5, 1_700_000_000, &make_processed()),
                    (sig_b, &tx_b, 5, 1_700_000_000, &make_processed()),
                    (sig_c, &tx_c, 5, 1_700_000_000, &make_processed()),
                ],
                Some(create_test_block_info(5, Hash::new_unique())),
            )
            .await
            .unwrap();
        flush_address_signatures_sync(&db, &addr_sig_rows).await;

        let results = db
            .get_signatures_for_address(&to, 10, None, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 3, "expected all 3 transactions");

        // All three are in the same slot — verify the tiebreaker: signature DESC.
        // Postgres bytea DESC is byte-by-byte lexicographic descending.
        let mut expected_bytes: Vec<Vec<u8>> = vec![
            sig_a.as_ref().to_vec(),
            sig_b.as_ref().to_vec(),
            sig_c.as_ref().to_vec(),
        ];
        expected_bytes.sort_by(|a, b| b.cmp(a));

        let result_bytes: Vec<Vec<u8>> = results
            .iter()
            .map(|r| Signature::from_str(&r.signature).unwrap().as_ref().to_vec())
            .collect();

        assert_eq!(
            result_bytes, expected_bytes,
            "same-slot results must be ordered by signature DESC"
        );
    }

    /// Helper used by the cursor tests: stores a single transaction for `to` at `slot`
    /// and returns its signature.
    async fn store_tx_at_slot(
        db: &mut AccountsDB,
        to: &Pubkey,
        slot: u64,
    ) -> solana_sdk::signature::Signature {
        let from = Keypair::new();
        let tx = create_test_sanitized_transaction(&from, to, 1);
        let sig = *tx.signature();
        let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }));
        let addr_sig_rows = db
            .write_batch(
                &[],
                vec![(sig, &tx, slot, 1_700_000_000, &processed)],
                Some(create_test_block_info(slot, Hash::new_unique())),
            )
            .await
            .unwrap();
        flush_address_signatures_sync(db, &addr_sig_rows).await;
        sig
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_before_cursor() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();

        // Three transactions in ascending slot order.
        let sig_old = store_tx_at_slot(&mut db, &to, 10).await;
        let sig_mid = store_tx_at_slot(&mut db, &to, 20).await;
        let _sig_new = store_tx_at_slot(&mut db, &to, 30).await;

        // `before=sig_mid` must return only the transaction older than sig_mid (slot 10).
        let results = db
            .get_signatures_for_address(&to, 10, Some(&sig_mid), None)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slot, 10);
        assert_eq!(results[0].signature, sig_old.to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_until_cursor() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();

        let _sig_old = store_tx_at_slot(&mut db, &to, 10).await;
        let sig_mid = store_tx_at_slot(&mut db, &to, 20).await;
        let sig_new = store_tx_at_slot(&mut db, &to, 30).await;

        // `until=sig_mid` must return transactions from newest down to and
        // including sig_mid (slots 30 and 20), but not slot 10.
        let results = db
            .get_signatures_for_address(&to, 10, None, Some(&sig_mid))
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].signature, sig_new.to_string()); // newest first
        assert_eq!(results[1].signature, sig_mid.to_string()); // until is inclusive
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_before_and_until_cursors() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();

        let _sig_old = store_tx_at_slot(&mut db, &to, 10).await;
        let sig_mid = store_tx_at_slot(&mut db, &to, 20).await;
        let sig_new = store_tx_at_slot(&mut db, &to, 30).await;

        // Combining both cursors must return exactly sig_mid (slot 20):
        // older than slot 30 (before=sig_new) AND as recent as slot 20 (until=sig_mid).
        let results = db
            .get_signatures_for_address(&to, 10, Some(&sig_new), Some(&sig_mid))
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].signature, sig_mid.to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_unknown_before_cursor_returns_error() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();
        store_tx_at_slot(&mut db, &to, 10).await;

        // A randomly generated signature that was never stored — resolve_cursor
        // should catch this and return Err instead of silently returning empty.
        let ghost_sig = solana_sdk::signature::Signature::new_unique();
        let result = db
            .get_signatures_for_address(&to, 10, Some(&ghost_sig), None)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("'before' is unavailable"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_unknown_until_cursor_returns_error() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();
        store_tx_at_slot(&mut db, &to, 10).await;

        let ghost_sig = solana_sdk::signature::Signature::new_unique();
        let result = db
            .get_signatures_for_address(&to, 10, None, Some(&ghost_sig))
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("'until' is unavailable"));
    }

    /// Address history resolves against Postgres even when the cache holds
    /// nothing. The cached index only carries signatures written since the cache
    /// attached, so a truncated history would read as a complete one.
    #[tokio::test(flavor = "multi_thread")]
    async fn address_history_through_cache_resolves_against_postgres() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let AccountsDB::Postgres(ref postgres_db) = pg_db else {
            panic!("expected Postgres variant")
        };
        let (redis_raw, _redis) = start_test_redis(postgres_db.clone()).await;
        let cache_db = AccountsDB::Redis(redis_raw);

        let to = Pubkey::new_unique();
        let slot = 42u64;
        let block_time = 1_700_000_000;

        let txs: Vec<_> = (0..10)
            .map(|_| {
                let keypair = Keypair::new();
                let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
                    loaded_transaction: LoadedTransaction {
                        accounts: vec![],
                        ..Default::default()
                    },
                    execution_details: TransactionExecutionDetails {
                        status: Ok(()),
                        log_messages: None,
                        inner_instructions: None,
                        return_data: None,
                        executed_units: 0,
                        accounts_data_len_delta: 0,
                    },
                    programs_modified_by_tx: HashMap::new(),
                }));
                (
                    create_test_sanitized_transaction(&keypair, &to, 1),
                    processed,
                )
            })
            .collect();

        let batch_refs: Vec<_> = txs
            .iter()
            .map(|(tx, processed)| (*tx.signature(), tx, slot, block_time, processed))
            .collect();

        // Written to Postgres only, never to the cache.
        let block = create_test_block_info(slot, Hash::new_unique());
        let pg_addr_sig_rows = pg_db
            .write_batch(&[], batch_refs, Some(block))
            .await
            .unwrap();
        flush_address_signatures_sync(&pg_db, &pg_addr_sig_rows).await;

        let pg_sigs = pg_db
            .get_signatures_for_address(&to, 10, None, None)
            .await
            .unwrap();
        let cache_sigs = cache_db
            .get_signatures_for_address(&to, 10, None, None)
            .await
            .unwrap();

        assert_eq!(pg_sigs.len(), 10);
        let pg_order: Vec<&str> = pg_sigs.iter().map(|sig| sig.signature.as_str()).collect();
        let cache_order: Vec<&str> = cache_sigs
            .iter()
            .map(|sig| sig.signature.as_str())
            .collect();
        assert_eq!(
            pg_order, cache_order,
            "a cache-backed handle must return the full Postgres history"
        );
    }

    /// One executed transaction, reusable by the counter cases below.
    fn counted_tx(to: &Pubkey) -> (SanitizedTransaction, ProcessedTransaction) {
        let keypair = Keypair::new();
        let processed = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
            loaded_transaction: LoadedTransaction {
                accounts: vec![],
                ..Default::default()
            },
            execution_details: TransactionExecutionDetails {
                status: Ok(()),
                log_messages: None,
                inner_instructions: None,
                return_data: None,
                executed_units: 0,
                accounts_data_len_delta: 0,
            },
            programs_modified_by_tx: HashMap::new(),
        }));
        (
            create_test_sanitized_transaction(&keypair, to, 1),
            processed,
        )
    }

    /// A commit whose acknowledgement is lost is retried with the same slot and
    /// the same rows. Every other write is an upsert, so only the counter can
    /// double-apply, and an inflated getTransactionCount never self-heals.
    #[tokio::test(flavor = "multi_thread")]
    async fn replaying_a_committed_batch_counts_it_once() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();
        let txs = [counted_tx(&to), counted_tx(&to)];
        let block = create_test_block_info(1, Hash::new_unique());

        for _ in 0..2 {
            let refs: Vec<_> = txs
                .iter()
                .map(|(tx, p)| (*tx.signature(), tx, 1u64, 1_700_000_000i64, p))
                .collect();
            db.write_batch(&[], refs, Some(block.clone()))
                .await
                .unwrap();
        }

        assert_eq!(
            db.get_transaction_count().await.unwrap(),
            2,
            "a replayed slot must not advance the counter twice"
        );
    }

    /// The gate must key on the slot being new, not on the batch looking
    /// familiar: distinct slots are ordinary traffic and must both count.
    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_slots_each_advance_the_counter() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();

        for slot in 1..=3u64 {
            let txs = [counted_tx(&to)];
            let refs: Vec<_> = txs
                .iter()
                .map(|(tx, p)| (*tx.signature(), tx, slot, 1_700_000_000i64, p))
                .collect();
            db.write_batch(
                &[],
                refs,
                Some(create_test_block_info(slot, Hash::new_unique())),
            )
            .await
            .unwrap();
        }

        assert_eq!(db.get_transaction_count().await.unwrap(), 3);
    }

    /// A batch with no block has no slot to key the gate on, so it keeps
    /// counting unconditionally.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_blockless_batch_still_counts() {
        let (mut db, _pg) = start_test_postgres().await;
        let to = Pubkey::new_unique();

        for _ in 0..2 {
            let txs = [counted_tx(&to)];
            let refs: Vec<_> = txs
                .iter()
                .map(|(tx, p)| (*tx.signature(), tx, 0u64, 1_700_000_000i64, p))
                .collect();
            db.write_batch(&[], refs, None).await.unwrap();
        }

        assert_eq!(db.get_transaction_count().await.unwrap(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_batch_deleted_account_removes_from_db() {
        let (mut db, _pg) = start_test_postgres().await;

        let pk = Pubkey::new_unique();
        let acct = AccountSharedData::new(500, 0, &Pubkey::new_unique());

        // first store an account
        db.set_account(pk, acct.clone()).await;
        assert!(db.get_account_shared_data(&pk).await.unwrap().is_some());

        // now write_batch with deleted=true
        let settlement = AccountSettlement {
            account: acct,
            deleted: true,
        };
        db.write_batch(&[(pk, settlement)], vec![], None)
            .await
            .unwrap();

        // account is gone
        assert!(db.get_account_shared_data(&pk).await.unwrap().is_none());
    }
}
