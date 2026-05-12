use {
    super::{postgres::PostgresAccountsDB, redis::RedisAccountsDB, types::StoredTransaction},
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
}

/// AccountsDB enum supporting multiple backend storage options
///
/// # Variants
///
/// * `Postgres` - PostgreSQL database only. Provides ACID transactions and is the
///   source of truth for all finalized state.
///
/// * `Redis` - Redis cache only. Fast in-memory storage but lacks true transaction
///   support. Uses MULTI/EXEC which can fail partway through without rollback.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum AccountsDB {
    Postgres(PostgresAccountsDB),
    Redis(RedisAccountsDB),
}

impl AccountsDB {
    pub async fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        super::get_account_shared_data::get_account_shared_data(self, pubkey).await
    }

    pub async fn set_account(&mut self, pubkey: Pubkey, account: AccountSharedData) {
        super::set_account::set_account(self, pubkey, account).await
    }

    pub async fn get_transaction(&self, signature: &Signature) -> Option<StoredTransaction> {
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

    pub async fn set_latest_slot(&mut self, slot: u64) -> Result<(), String> {
        super::set_latest_slot::set_latest_slot(self, slot).await
    }

    pub async fn store_block(&mut self, block_info: BlockInfo) -> Result<(), String> {
        super::store_block::store_block(self, block_info).await
    }

    pub async fn get_block(&self, slot: u64) -> Option<BlockInfo> {
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

    pub async fn get_blocks_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<Vec<BlockInfo>> {
        super::get_blocks_in_range::get_blocks_in_range(self, start_slot, end_slot).await
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

    pub async fn get_accounts(&self, accounts: &[Pubkey]) -> Vec<Option<AccountSharedData>> {
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

    pub async fn get_block_time(&self, slot: u64) -> Option<i64> {
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
            Ok(AccountsDB::Redis(
                RedisAccountsDB::new(accountsdb_connection_url)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create RedisAccountsDB: {}", e))?,
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
    use solana_sdk::account::AccountSharedData;
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

    #[tokio::test(flavor = "multi_thread")]
    async fn set_and_get_account_round_trip() {
        let (mut db, _pg) = start_test_postgres().await;

        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(42_000, 0, &owner);

        // miss before set
        assert!(db.get_account_shared_data(&pubkey).await.is_none());

        db.set_account(pubkey, account.clone()).await;

        let loaded = db.get_account_shared_data(&pubkey).await;
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

        let results = db.get_accounts(&[pk1, pk2, pk3]).await;
        assert_eq!(results.len(), 3);
        assert!(results[0].is_none(), "pk1 was never stored");
        assert!(results[1].is_some(), "pk2 should be found");
        assert!(results[2].is_none(), "pk3 was never stored");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn store_block_and_get_block_round_trip() {
        let (mut db, _pg) = start_test_postgres().await;

        let blockhash = Hash::new_unique();
        let block = create_test_block_info(10, blockhash);

        db.store_block(block.clone()).await.unwrap();

        let loaded = db.get_block(10).await;
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.slot, 10);
        assert_eq!(loaded.blockhash, blockhash);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_block_miss_returns_none() {
        let (db, _pg) = start_test_postgres().await;
        assert!(db.get_block(999).await.is_none());
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

    #[tokio::test(flavor = "multi_thread")]
    async fn get_blocks_redis_returns_slot_numbers_in_order() {
        let (redis_raw, _redis) = start_test_redis().await;
        let mut db = AccountsDB::Redis(redis_raw);

        for slot in [3u64, 7, 1, 10] {
            db.write_batch(
                &[],
                vec![],
                Some(create_test_block_info(slot, Hash::new_unique())),
            )
            .await
            .unwrap();
        }

        let slots = db.get_blocks(0, Some(20)).await.unwrap();
        assert_eq!(slots, vec![1, 3, 7, 10]);
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
        assert!(db.get_transaction(&sig).await.is_none());
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

        let time = db.get_block_time(7).await;
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
        assert!(db.get_account_shared_data(&pk).await.is_some());

        // block was stored
        let loaded = db.get_block(1).await;
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
        assert!(db.get_block(7).await.is_some());
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

    /// Verifies that Redis and Postgres return same-slot signatures in identical order.
    /// Redis stores members as hex-encoded signatures so that sorted-set lex ordering
    /// matches Postgres's `ORDER BY signature DESC` (raw bytes). This test catches
    /// any regression where the member encoding no longer preserves byte ordering.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_signatures_for_address_redis_same_slot_ordering_matches_postgres() {
        let (mut pg_db, _pg) = start_test_postgres().await;
        let (redis_raw, _redis) = start_test_redis().await;
        let mut redis_db = AccountsDB::Redis(redis_raw);

        let to = Pubkey::new_unique();
        let slot = 42u64;
        let block_time = 1_700_000_000;

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

        let txs: Vec<_> = (0..10)
            .map(|_| {
                let kp = Keypair::new();
                (
                    create_test_sanitized_transaction(&kp, &to, 1),
                    make_processed(),
                )
            })
            .collect();

        let batch_refs: Vec<_> = txs
            .iter()
            .map(|(tx, p)| (*tx.signature(), tx, slot, block_time, p))
            .collect();

        let block = create_test_block_info(slot, Hash::new_unique());
        let pg_addr_sig_rows = pg_db
            .write_batch(&[], batch_refs.clone(), Some(block.clone()))
            .await
            .unwrap();
        flush_address_signatures_sync(&pg_db, &pg_addr_sig_rows).await;
        redis_db
            .write_batch(&[], batch_refs, Some(block))
            .await
            .unwrap();

        let pg_sigs = pg_db
            .get_signatures_for_address(&to, 10, None, None)
            .await
            .unwrap();
        let redis_sigs = redis_db
            .get_signatures_for_address(&to, 10, None, None)
            .await
            .unwrap();

        assert_eq!(pg_sigs.len(), 10);
        let pg_order: Vec<&str> = pg_sigs.iter().map(|s| s.signature.as_str()).collect();
        let redis_order: Vec<&str> = redis_sigs.iter().map(|s| s.signature.as_str()).collect();
        assert_eq!(
            pg_order, redis_order,
            "Redis and Postgres must return same-slot signatures in identical order"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_batch_deleted_account_removes_from_db() {
        let (mut db, _pg) = start_test_postgres().await;

        let pk = Pubkey::new_unique();
        let acct = AccountSharedData::new(500, 0, &Pubkey::new_unique());

        // first store an account
        db.set_account(pk, acct.clone()).await;
        assert!(db.get_account_shared_data(&pk).await.is_some());

        // now write_batch with deleted=true
        let settlement = AccountSettlement {
            account: acct,
            deleted: true,
        };
        db.write_batch(&[(pk, settlement)], vec![], None)
            .await
            .unwrap();

        // account is gone
        assert!(db.get_account_shared_data(&pk).await.is_none());
    }
}
