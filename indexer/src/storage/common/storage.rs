pub use super::models::*;

pub mod bump_pending_remint_finality_attempt;
pub mod close;
pub mod count_pending_transactions;
pub mod delete_release_signatures;
pub mod drop_tables;
pub mod gc_stale_release_signatures;
pub mod get_all_db_transactions;
pub mod get_and_lock_pending_transactions;
pub mod get_committed_checkpoint;
pub mod get_completed_withdrawal_nonces;
pub mod get_escrow_balances_by_mint;
pub mod get_mint;
pub mod get_mint_balances_for_reconciliation;
pub mod get_mint_status_at_slot;
pub mod get_orphan_deposit_ids;
pub mod get_pending_db_transactions;
pub mod get_pending_remint_transactions;
pub mod get_release_signatures;
pub mod get_stale_processing_transactions;
pub mod init_schema;
pub mod insert_db_transaction;
pub mod insert_db_transactions_batch;
pub mod insert_mint_statuses_batch;
pub mod insert_release_signature;
pub mod quarantine_all_active_withdrawals;
pub mod record_remint_result;
pub mod sender_lock;
pub mod set_mint_extension_flags;
pub mod set_pending_remint;
pub mod sync_mint_status;
pub mod try_complete_processing;
pub mod try_quarantine_processing;
pub mod try_requeue_processing;
pub mod update_committed_checkpoint;
pub mod update_transaction_status;
pub mod upsert_mints_batch;

use crate::{error::StorageError, storage::postgres::db::PostgresDb};

// `mock` is exposed when either this crate's own tests are compiling
// (`#[cfg(test)]`) OR the explicit `test-mock-storage` feature is set by
// a downstream integration-test crate.
#[cfg(any(test, feature = "test-mock-storage"))]
pub mod mock;

#[derive(Clone)]
pub enum Storage {
    Postgres(PostgresDb),
    #[cfg(any(test, feature = "test-mock-storage"))]
    Mock(mock::MockStorage),
}

impl Storage {
    /// Initialize database schema
    pub async fn init_schema(&self) -> Result<(), StorageError> {
        init_schema::init_schema(self).await
    }

    /// Drop all database tables
    pub async fn drop_tables(&self) -> Result<(), StorageError> {
        drop_tables::drop_tables(self).await
    }

    /// Insert a new transaction
    pub async fn insert_db_transaction(
        &self,
        transaction: &DbTransaction,
    ) -> Result<i64, StorageError> {
        insert_db_transaction::insert_db_transaction(self, transaction).await
    }

    /// Insert multiple transactions in a batch
    /// Returns the IDs of inserted transactions in the same order
    pub async fn insert_db_transactions_batch(
        &self,
        transactions: &[DbTransaction],
    ) -> Result<Vec<i64>, StorageError> {
        insert_db_transactions_batch::insert_db_transactions_batch(self, transactions).await
    }

    /// Get pending transactions
    pub async fn get_pending_db_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_pending_db_transactions::get_pending_db_transactions(self, transaction_type, limit)
            .await
    }

    /// Get all transactions of a given type regardless of status
    pub async fn get_all_db_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, Box<dyn std::error::Error + Send + Sync>> {
        get_all_db_transactions::get_all_db_transactions(self, transaction_type, limit).await
    }

    /// Get and lock pending transactions for processing (FOR UPDATE SKIP LOCKED)
    /// Sets status to Processing and returns locked rows
    pub async fn get_and_lock_pending_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_and_lock_pending_transactions::get_and_lock_pending_transactions(
            self,
            transaction_type,
            limit,
        )
        .await
    }

    /// Get committed checkpoint for a program type
    pub async fn get_committed_checkpoint(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, StorageError> {
        get_committed_checkpoint::get_committed_checkpoint(self, program_type).await
    }

    /// Update committed checkpoint for a program type
    pub async fn update_committed_checkpoint(
        &self,
        program_type: &str,
        slot: u64,
    ) -> Result<(), StorageError> {
        update_committed_checkpoint::update_committed_checkpoint(self, program_type, slot).await
    }

    /// Terminal status write; `Ok(false)` if row already off Processing.
    pub async fn update_transaction_status(
        &self,
        transaction_id: i64,
        status: TransactionStatus,
        counterpart_signature: Option<String>,
        processed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        update_transaction_status::update_transaction_status(
            self,
            transaction_id,
            status,
            counterpart_signature,
            processed_at,
        )
        .await
    }

    /// Insert or update multiple mints in a batch (upsert on mint_address)
    pub async fn upsert_mints_batch(&self, mints: &[DbMint]) -> Result<(), StorageError> {
        upsert_mints_batch::upsert_mints_batch(self, mints).await
    }

    /// Append Allow/Block transition rows to `mint_status_history`.
    /// Idempotent on (mint_address, effective_slot)
    pub async fn insert_mint_statuses_batch(
        &self,
        statuses: &[DbMintStatus],
    ) -> Result<(), StorageError> {
        insert_mint_statuses_batch::insert_mint_statuses_batch(self, statuses).await
    }

    /// Refresh the `mints.status` mirror for the given mints from their latest
    /// `mint_status_history` transition. No-op for mints without a row.
    pub async fn sync_mint_status(&self, mint_addresses: &[String]) -> Result<(), StorageError> {
        sync_mint_status::sync_mint_status(self, mint_addresses).await
    }

    /// Resolve a mint's status (Allowed / Blocked / NeverAllowed) as of `slot`.
    pub async fn get_mint_status_at_slot(
        &self,
        mint_address: &str,
        slot: i64,
    ) -> Result<MintStatusAtSlot, StorageError> {
        get_mint_status_at_slot::get_mint_status_at_slot(self, mint_address, slot).await
    }

    /// Get mint metadata by address
    pub async fn get_mint(&self, mint_address: &str) -> Result<Option<DbMint>, StorageError> {
        get_mint::get_mint(self, mint_address).await
    }

    /// Write-back the on-chain extension presence (PausableConfig,
    /// PermanentDelegate) for a mint. Called by the operator's MintCache
    /// after a single RPC fetch that resolves both flags together.
    pub async fn set_mint_extension_flags(
        &self,
        mint_address: &str,
        is_pausable: bool,
        has_permanent_delegate: bool,
    ) -> Result<(), StorageError> {
        set_mint_extension_flags::set_mint_extension_flags(
            self,
            mint_address,
            is_pausable,
            has_permanent_delegate,
        )
        .await
    }

    /// Return per-mint aggregate balances (completed deposits minus withdrawals) for startup reconciliation.
    pub async fn get_mint_balances_for_reconciliation(
        &self,
    ) -> Result<Vec<MintDbBalance>, StorageError> {
        get_mint_balances_for_reconciliation::get_mint_balances_for_reconciliation(self).await
    }

    /// Query escrow balances by mint for continuous reconciliation checks.
    /// Only counts **completed** transactions for both deposits and withdrawals.
    /// Returns per-mint aggregate balances where net_balance = total_deposits - total_withdrawals.
    pub async fn get_escrow_balances_by_mint(&self) -> Result<Vec<MintDbBalance>, StorageError> {
        get_escrow_balances_by_mint::get_escrow_balances_by_mint(self).await
    }

    /// `transactions.id` for every `deposit` row whose mint was not in
    /// `allowed` status at the deposit's slot, per `mint_status_history`.
    ///
    /// A non-empty result means the indexer recorded a deposit for a mint
    /// that was either never allowlisted or was blocked at the time of the
    /// deposit — a trust-boundary leak. Reconciliation queries this to
    /// alert on any such rows; they describe the same condition the
    /// deposit-side gate (`assert_mint_allowed_at_slot`) refuses at process
    /// time. So, this is a second line of defense.
    pub async fn get_orphan_deposit_ids(&self) -> Result<Vec<i64>, StorageError> {
        get_orphan_deposit_ids::get_orphan_deposit_ids(self).await
    }

    /// Close the storage connection pool gracefully
    /// Waits for active connections to complete and closes the pool
    pub async fn close(&self) -> Result<(), StorageError> {
        close::close(self).await
    }

    pub async fn count_pending_transactions(
        &self,
        transaction_type: TransactionType,
    ) -> Result<i64, StorageError> {
        count_pending_transactions::count_pending_transactions(self, transaction_type).await
    }

    /// Get completed withdrawal nonces in the given range [min_nonce, max_nonce)
    pub async fn get_completed_withdrawal_nonces(
        &self,
        min_nonce: u64,
        max_nonce: u64,
    ) -> Result<Vec<u64>, StorageError> {
        get_completed_withdrawal_nonces::get_completed_withdrawal_nonces(self, min_nonce, max_nonce)
            .await
    }

    /// Transitions a withdrawal to PendingRemint, storing withdrawal
    /// signatures + lvbh for the finality check on restart.
    pub async fn set_pending_remint(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        set_pending_remint::set_pending_remint(
            self,
            transaction_id,
            remint_signatures,
            remint_last_valid_block_heights,
            deadline_at,
        )
        .await
    }

    /// Persist an incremented defer counter and extended deadline for a
    /// PendingRemint row. Called from the sender loop each time
    /// `process_pending_remints` defers an entry, so the
    /// `MAX_FINALITY_CHECK_ATTEMPTS` budget survives restarts.
    pub async fn bump_pending_remint_finality_attempt(
        &self,
        transaction_id: i64,
        attempts: i32,
        new_deadline: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        bump_pending_remint_finality_attempt::bump_pending_remint_finality_attempt(
            self,
            transaction_id,
            attempts,
            new_deadline,
        )
        .await
    }

    /// Durably record a confirmed remint (status -> FailedReminted plus the
    /// signature) in one write, before the async status writer runs. Closes the
    /// crash window that would otherwise leave a landed remint as PendingRemint.
    pub async fn record_remint_result(
        &self,
        transaction_id: i64,
        remint_signature: String,
    ) -> Result<(), StorageError> {
        record_remint_result::record_remint_result(self, transaction_id, remint_signature).await
    }

    /// Returns all withdrawal transactions in PendingRemint status.
    /// Called on startup to re-hydrate the remint queue after a crash.
    pub async fn get_pending_remint_transactions(
        &self,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_pending_remint_transactions::get_pending_remint_transactions(self).await
    }

    /// Try to acquire the singleton sender lock for `key`. `Ok(None)` means
    /// another sender holds it, so the caller must refuse to start.
    pub async fn try_acquire_sender_lock(
        &self,
        key: i64,
    ) -> Result<Option<sender_lock::SenderLockGuard>, StorageError> {
        sender_lock::try_acquire_sender_lock(self, key).await
    }

    /// Mark every `Pending`/`Processing` withdrawal row as `ManualReview`.
    ///
    /// Invoked by the processor when a single withdrawal is unprocessable:
    /// the whole withdrawal pipeline halts so a human can inspect and
    /// decide on rotation/reinsert before drains resume. `exclude_id` is
    /// the poison row already quarantined through the async status-update
    /// channel — excluding it here avoids a duplicate webhook. Returns the
    /// number of rows flipped.
    pub async fn quarantine_all_active_withdrawals(
        &self,
        exclude_id: Option<i64>,
    ) -> Result<u64, StorageError> {
        quarantine_all_active_withdrawals::quarantine_all_active_withdrawals(self, exclude_id).await
    }

    /// Stale `Processing` rows past the threshold (used by recovery).
    pub async fn get_stale_processing_transactions(
        &self,
        threshold: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_stale_processing_transactions::get_stale_processing_transactions(self, threshold, limit)
            .await
    }

    /// CAS `Processing` → `Pending` on `updated_at`; `Ok(false)` if stale.
    pub async fn try_requeue_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        try_requeue_processing::try_requeue_processing(self, transaction_id, expected_updated_at)
            .await
    }

    /// CAS `Processing` → `Completed` on `updated_at`; `Ok(false)` if stale.
    pub async fn try_complete_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        counterpart_signature: Option<String>,
    ) -> Result<bool, StorageError> {
        try_complete_processing::try_complete_processing(
            self,
            transaction_id,
            expected_updated_at,
            counterpart_signature,
        )
        .await
    }

    /// CAS `Processing` → `ManualReview`; reason rides on the webhook, not DB.
    pub async fn try_quarantine_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        try_quarantine_processing::try_quarantine_processing(
            self,
            transaction_id,
            expected_updated_at,
        )
        .await
    }

    /// Record a broadcast release signature so recovery can verify finality
    /// before demoting. Idempotent on `signature`.
    pub async fn insert_release_signature(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
    ) -> Result<(), StorageError> {
        insert_release_signature::insert_release_signature(
            self,
            transaction_id,
            signature,
            last_valid_block_height,
        )
        .await
    }

    /// Stored release signatures for a transaction as (signature, lvbh).
    pub async fn get_release_signatures(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        get_release_signatures::get_release_signatures(self, transaction_id).await
    }

    /// Delete all stored release signatures for a transaction.
    pub async fn delete_release_signatures(&self, transaction_id: i64) -> Result<(), StorageError> {
        delete_release_signatures::delete_release_signatures(self, transaction_id).await
    }

    /// Drop release signatures whose parent transaction is no longer
    /// `Processing`. Returns the number of rows removed.
    pub async fn gc_stale_release_signatures(&self) -> Result<u64, StorageError> {
        gc_stale_release_signatures::gc_stale_release_signatures(self).await
    }
}

/// MockStorage behavior tests — only test non-trivial mock logic (filtering, recording, failure).
/// Tautological tests (mock returns Ok → assert Ok) are intentionally omitted.
/// Real storage behavior is covered by postgres_db_test.rs integration tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::common::amount::TokenAmount;
    use bigdecimal::BigDecimal;
    use chrono::Utc;
    use mock::MockStorage;

    const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    fn make_mock_storage() -> (Storage, MockStorage) {
        let mock = MockStorage::new();
        let storage = Storage::Mock(mock.clone());
        (storage, mock)
    }

    fn make_db_transaction() -> DbTransaction {
        DbTransaction {
            id: 0,
            signature: "test_sig".to_string(),
            trace_id: "trace-1".to_string(),
            slot: 100,
            initiator: "initiator".to_string(),
            recipient: "recipient".to_string(),
            mint: "mint_addr".to_string(),
            amount: TokenAmount(1000),
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
        }
    }

    // ── insert recording + failure ───────────────────────────────────

    #[tokio::test]
    async fn insert_db_transaction_records_and_returns_incremental_ids() {
        let (storage, mock) = make_mock_storage();
        let id1 = storage
            .insert_db_transaction(&make_db_transaction())
            .await
            .unwrap();
        let id2 = storage
            .insert_db_transaction(&make_db_transaction())
            .await
            .unwrap();
        assert_ne!(id1, id2);

        let recorded = mock.inserted_single_transactions.lock().unwrap();
        assert_eq!(recorded.len(), 2);
    }

    #[tokio::test]
    async fn insert_db_transaction_respects_should_fail() {
        let (storage, mock) = make_mock_storage();
        mock.set_should_fail("insert_db_transaction", true);
        assert!(storage
            .insert_db_transaction(&make_db_transaction())
            .await
            .is_err());
    }

    // ── pending transaction filtering ────────────────────────────────

    #[tokio::test]
    async fn get_pending_filters_by_type_and_respects_limit() {
        let (storage, mock) = make_mock_storage();
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for i in 0..3 {
                let mut txn = make_db_transaction();
                txn.signature = format!("dep_{i}");
                pending.push(txn);
            }
            let mut w = make_db_transaction();
            w.transaction_type = TransactionType::Withdrawal;
            w.signature = "wd_0".to_string();
            pending.push(w);
        }

        // Only deposits, capped at 2
        let deps = storage
            .get_pending_db_transactions(TransactionType::Deposit, 2)
            .await
            .unwrap();
        assert_eq!(deps.len(), 2);

        // Withdrawal type returns only the withdrawal
        let wds = storage
            .get_pending_db_transactions(TransactionType::Withdrawal, 10)
            .await
            .unwrap();
        assert_eq!(wds.len(), 1);
        assert_eq!(wds[0].signature, "wd_0");
    }

    // ── lock + drain filtering ───────────────────────────────────────

    #[tokio::test]
    async fn get_and_lock_drains_matched_leaves_rest() {
        let (storage, mock) = make_mock_storage();
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for i in 0..3 {
                let mut txn = make_db_transaction();
                txn.signature = format!("dep_{i}");
                pending.push(txn);
            }
            let mut w = make_db_transaction();
            w.transaction_type = TransactionType::Withdrawal;
            w.signature = "wd_0".to_string();
            pending.push(w);
        }

        let locked = storage
            .get_and_lock_pending_transactions(TransactionType::Deposit, 2)
            .await
            .unwrap();
        assert_eq!(locked.len(), 2);

        // 1 deposit + 1 withdrawal remain
        {
            let remaining = mock.pending_transactions.lock().unwrap();
            assert_eq!(remaining.len(), 2);
        }
        let locked2 = storage
            .get_and_lock_pending_transactions(TransactionType::Deposit, 10)
            .await
            .unwrap();
        assert_eq!(locked2.len(), 1);
    }

    // ── status update recording ──────────────────────────────────────

    #[tokio::test]
    async fn update_transaction_status_records_params() {
        let (storage, mock) = make_mock_storage();
        let now = Utc::now();
        storage
            .update_transaction_status(
                42,
                TransactionStatus::Completed,
                Some("sig_abc".to_string()),
                now,
            )
            .await
            .unwrap();

        let updates = mock.status_updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, 42);
        assert_eq!(updates[0].1, TransactionStatus::Completed);
        assert_eq!(updates[0].2.as_deref(), Some("sig_abc"));
    }

    #[tokio::test]
    async fn update_transaction_status_respects_should_fail() {
        let (storage, mock) = make_mock_storage();
        mock.set_should_fail("update_transaction_status", true);
        assert!(storage
            .update_transaction_status(1, TransactionStatus::Completed, None, Utc::now())
            .await
            .is_err());
    }

    // ── storage dispatch coverage ────────────────────────────────────

    #[tokio::test]
    async fn dispatch_init_schema_via_mock() {
        let (storage, _mock) = make_mock_storage();
        assert!(storage.init_schema().await.is_ok());
    }

    #[tokio::test]
    async fn dispatch_drop_tables_via_mock() {
        let (storage, _mock) = make_mock_storage();
        assert!(storage.drop_tables().await.is_ok());
    }

    #[tokio::test]
    async fn dispatch_close_via_mock() {
        let (storage, _mock) = make_mock_storage();
        assert!(storage.close().await.is_ok());
    }

    #[tokio::test]
    async fn dispatch_count_pending_transactions_via_mock() {
        let (storage, mock) = make_mock_storage();
        // Populate with pending transactions
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for i in 0..3 {
                let mut txn = make_db_transaction();
                txn.signature = format!("dep_{i}");
                pending.push(txn);
            }
            // Add a withdrawal (different type)
            let mut w = make_db_transaction();
            w.transaction_type = TransactionType::Withdrawal;
            pending.push(w);
        }

        // Count deposits only
        let count = storage
            .count_pending_transactions(TransactionType::Deposit)
            .await
            .unwrap();
        assert_eq!(count, 3);

        // Count withdrawals only
        let count = storage
            .count_pending_transactions(TransactionType::Withdrawal)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn dispatch_get_all_db_transactions_via_mock() {
        let (storage, mock) = make_mock_storage();
        // Populate with various transaction statuses
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for i in 0..3 {
                let mut txn = make_db_transaction();
                txn.signature = format!("dep_{i}");
                if i == 0 {
                    txn.status = TransactionStatus::Completed;
                } else {
                    txn.status = TransactionStatus::Pending;
                }
                pending.push(txn);
            }
        }

        // Get all deposits (regardless of status)
        let txns = storage
            .get_all_db_transactions(TransactionType::Deposit, 10)
            .await
            .unwrap();
        assert_eq!(txns.len(), 3);
        assert_eq!(txns[0].signature, "dep_0");
        assert_eq!(txns[1].signature, "dep_1");

        // Test limit
        let txns = storage
            .get_all_db_transactions(TransactionType::Deposit, 2)
            .await
            .unwrap();
        assert_eq!(txns.len(), 2);
    }

    #[tokio::test]
    async fn dispatch_get_completed_withdrawal_nonces_via_mock() {
        let (storage, mock) = make_mock_storage();
        // Populate with completed withdrawals with nonces
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for i in 0..3 {
                let mut txn = make_db_transaction();
                txn.transaction_type = TransactionType::Withdrawal;
                txn.status = TransactionStatus::Completed;
                txn.withdrawal_nonce = Some(i * 10 + 5);
                pending.push(txn);
            }
            // Add a pending withdrawal (should be excluded)
            let mut pending_wd = make_db_transaction();
            pending_wd.transaction_type = TransactionType::Withdrawal;
            pending_wd.status = TransactionStatus::Pending;
            pending_wd.withdrawal_nonce = Some(100);
            pending.push(pending_wd);
        }

        // Get nonces in range [0, 100)
        let nonces = storage
            .get_completed_withdrawal_nonces(0, 100)
            .await
            .unwrap();
        assert_eq!(nonces.len(), 3);
        assert!(nonces.contains(&5));
        assert!(nonces.contains(&15));
        assert!(nonces.contains(&25));

        // Get nonces in narrower range [10, 30)
        let nonces = storage
            .get_completed_withdrawal_nonces(10, 30)
            .await
            .unwrap();
        assert_eq!(nonces.len(), 2);
        assert!(nonces.contains(&15));
        assert!(nonces.contains(&25));
    }

    #[tokio::test]
    async fn dispatch_get_escrow_balances_by_mint_via_mock() {
        let (storage, mock) = make_mock_storage();

        // Populate with mint balances
        {
            let balances = vec![
                MintDbBalance {
                    mint_address: "mint_1".to_string(),
                    token_program: TOKEN_PROGRAM.to_string(),
                    total_deposits: BigDecimal::from(1000u64),
                    total_withdrawals: BigDecimal::from(300u64),
                },
                MintDbBalance {
                    mint_address: "mint_2".to_string(),
                    token_program: TOKEN_PROGRAM.to_string(),
                    total_deposits: BigDecimal::from(5000u64),
                    total_withdrawals: BigDecimal::from(2000u64),
                },
            ];
            mock.set_mint_balances(balances);
        }

        let balances = storage.get_escrow_balances_by_mint().await.unwrap();
        assert_eq!(balances.len(), 2);
        assert_eq!(balances[0].mint_address, "mint_1");
        assert_eq!(balances[0].total_deposits, BigDecimal::from(1000u64));
        assert_eq!(balances[0].total_withdrawals, BigDecimal::from(300u64));
        assert_eq!(balances[1].mint_address, "mint_2");
        assert_eq!(balances[1].total_deposits, BigDecimal::from(5000u64));
        assert_eq!(balances[1].total_withdrawals, BigDecimal::from(2000u64));
    }

    #[tokio::test]
    async fn dispatch_get_mint_balances_for_reconciliation_via_mock() {
        let (storage, mock) = make_mock_storage();
        // Populate with mint balances for reconciliation
        {
            let balances = vec![
                MintDbBalance {
                    mint_address: "usdc".to_string(),
                    token_program: TOKEN_PROGRAM.to_string(),
                    total_deposits: BigDecimal::from(10000u64),
                    total_withdrawals: BigDecimal::from(5000u64),
                },
                MintDbBalance {
                    mint_address: "usdt".to_string(),
                    token_program: TOKEN_PROGRAM.to_string(),
                    total_deposits: BigDecimal::from(8000u64),
                    total_withdrawals: BigDecimal::from(3000u64),
                },
            ];
            mock.set_mint_balances(balances);
        }

        let balances = storage
            .get_mint_balances_for_reconciliation()
            .await
            .unwrap();
        assert_eq!(balances.len(), 2);
        assert!(balances.iter().any(|b| b.mint_address == "usdc"
            && b.total_deposits == BigDecimal::from(10000u64)
            && b.total_withdrawals == BigDecimal::from(5000u64)));
        assert!(balances.iter().any(|b| b.mint_address == "usdt"
            && b.total_deposits == BigDecimal::from(8000u64)
            && b.total_withdrawals == BigDecimal::from(3000u64)));
    }

    #[tokio::test]
    async fn dispatch_upsert_mints_batch_via_mock() {
        let (storage, mock) = make_mock_storage();
        let mint = DbMint::new("test_mint".to_string(), 6, TOKEN_PROGRAM.to_string());
        storage.upsert_mints_batch(&[mint]).await.unwrap();
        assert!(mock.mints.lock().unwrap().contains_key("test_mint"));
    }

    #[tokio::test]
    async fn sync_mint_status_mirrors_latest_history_and_preserves_metadata() {
        let (storage, _mock) = make_mock_storage();
        storage
            .upsert_mints_batch(&[DbMint::new("m1".to_string(), 6, TOKEN_PROGRAM.to_string())])
            .await
            .unwrap();

        // allowed@10 then blocked@20 → mirror resolves to the latest: blocked.
        storage
            .insert_mint_statuses_batch(&[
                status_row("m1", "allowed", 10),
                status_row("m1", "blocked", 20),
            ])
            .await
            .unwrap();
        storage.sync_mint_status(&["m1".to_string()]).await.unwrap();

        let m = storage.get_mint("m1").await.unwrap().unwrap();
        assert_eq!(m.status, "blocked");
        // Metadata is untouched by the mirror sync.
        assert_eq!(m.decimals, 6);
        assert_eq!(m.token_program, TOKEN_PROGRAM);

        // Re-allow at a later slot → mirror flips back to allowed.
        storage
            .insert_mint_statuses_batch(&[status_row("m1", "allowed", 30)])
            .await
            .unwrap();
        storage.sync_mint_status(&["m1".to_string()]).await.unwrap();
        assert_eq!(
            storage.get_mint("m1").await.unwrap().unwrap().status,
            "allowed"
        );
    }

    /// A stale replay (older slot than the current head) must not move the mirror.
    #[tokio::test]
    async fn sync_mint_status_ignores_older_history_after_block() {
        let (storage, _mock) = make_mock_storage();
        storage
            .upsert_mints_batch(&[DbMint::new("m1".to_string(), 6, TOKEN_PROGRAM.to_string())])
            .await
            .unwrap();
        storage
            .insert_mint_statuses_batch(&[
                status_row("m1", "allowed", 10),
                status_row("m1", "blocked", 20),
            ])
            .await
            .unwrap();
        // Replaying the slot-10 allow re-syncs, but the latest transition is still
        // blocked@20, so the mirror stays blocked.
        storage.sync_mint_status(&["m1".to_string()]).await.unwrap();
        assert_eq!(
            storage.get_mint("m1").await.unwrap().unwrap().status,
            "blocked"
        );
    }

    #[tokio::test]
    async fn sync_mint_status_missing_row_is_noop() {
        let (storage, _mock) = make_mock_storage();
        // No row for "ghost" — must not error.
        storage
            .sync_mint_status(&["ghost".to_string()])
            .await
            .unwrap();
        assert!(storage.get_mint("ghost").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dispatch_get_mint_via_mock() {
        let (storage, _mock) = make_mock_storage();
        // Populate with mints
        let mint = DbMint::new("mint_1".to_string(), 6, TOKEN_PROGRAM.to_string());
        storage
            .upsert_mints_batch(std::slice::from_ref(&mint))
            .await
            .unwrap();

        // Retrieve the mint
        let result = storage.get_mint("mint_1").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().mint_address, "mint_1");

        // Verify nonexistent mint returns None
        let result = storage.get_mint("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dispatch_get_committed_checkpoint_via_mock() {
        let (storage, _mock) = make_mock_storage();
        let result = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn dispatch_update_committed_checkpoint_via_mock() {
        let (storage, _mock) = make_mock_storage();
        storage
            .update_committed_checkpoint("escrow", 42)
            .await
            .unwrap();
        let val = storage.get_committed_checkpoint("escrow").await.unwrap();
        assert_eq!(val, Some(42));
    }

    #[tokio::test]
    async fn dispatch_insert_db_transactions_batch_via_mock() {
        let (storage, mock) = make_mock_storage();
        let txns = vec![make_db_transaction(), make_db_transaction()];
        let ids = storage.insert_db_transactions_batch(&txns).await.unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(mock.inserted_transactions.lock().unwrap().len(), 1);
    }

    // ── quarantine_all_active_withdrawals ─────────────────────────────

    /// Only Pending and Processing withdrawals flip to ManualReview.
    /// Returns the exact number of rows affected so the caller can log
    /// the blast radius.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_flips_pending_and_processing_only() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut a = make_db_transaction();
            a.transaction_type = TransactionType::Withdrawal;
            a.status = TransactionStatus::Pending;
            a.withdrawal_nonce = Some(1);
            let mut b = make_db_transaction();
            b.transaction_type = TransactionType::Withdrawal;
            b.status = TransactionStatus::Processing;
            b.withdrawal_nonce = Some(2);
            db.push(a);
            db.push(b);
        }

        let affected = storage
            .quarantine_all_active_withdrawals(None)
            .await
            .unwrap();
        assert_eq!(affected, 2);

        let rows = mock.pending_transactions.lock().unwrap();
        for txn in rows.iter() {
            assert_eq!(txn.status, TransactionStatus::ManualReview);
        }
    }

    /// Deposits are never touched by the withdrawal-halt path — a
    /// poisoned withdrawal must not strand deposits, which have no nonce
    /// and no gap semantics.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_leaves_deposits_untouched() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut dep = make_db_transaction();
            dep.transaction_type = TransactionType::Deposit;
            dep.status = TransactionStatus::Pending;
            let mut wd = make_db_transaction();
            wd.transaction_type = TransactionType::Withdrawal;
            wd.status = TransactionStatus::Pending;
            wd.withdrawal_nonce = Some(1);
            db.push(dep);
            db.push(wd);
        }

        let affected = storage
            .quarantine_all_active_withdrawals(None)
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = mock.pending_transactions.lock().unwrap();
        let dep = rows
            .iter()
            .find(|t| t.transaction_type == TransactionType::Deposit)
            .expect("deposit present");
        assert_eq!(dep.status, TransactionStatus::Pending);

        let wd = rows
            .iter()
            .find(|t| t.transaction_type == TransactionType::Withdrawal)
            .expect("withdrawal present");
        assert_eq!(wd.status, TransactionStatus::ManualReview);
    }

    /// Terminal statuses (Completed, Failed, ManualReview, PendingRemint)
    /// are left alone so the webhook does not re-alert on already-handled
    /// rows.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_leaves_terminal_rows_untouched() {
        let (storage, mock) = make_mock_storage();
        let terminal = [
            TransactionStatus::Completed,
            TransactionStatus::Failed,
            TransactionStatus::ManualReview,
            TransactionStatus::PendingRemint,
        ];
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            for (i, status) in terminal.iter().enumerate() {
                let mut t = make_db_transaction();
                t.transaction_type = TransactionType::Withdrawal;
                t.status = *status;
                t.withdrawal_nonce = Some(i as i64 + 1);
                db.push(t);
            }
        }

        let affected = storage
            .quarantine_all_active_withdrawals(None)
            .await
            .unwrap();
        assert_eq!(affected, 0);

        let rows = mock.pending_transactions.lock().unwrap();
        for (i, status) in terminal.iter().enumerate() {
            assert_eq!(rows[i].status, *status);
        }
    }

    /// Storage-level failure surfaces as an `Err` so the processor can log
    /// and continue the channel drain without silent loss.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_propagates_mock_failure() {
        let (storage, mock) = make_mock_storage();
        mock.set_should_fail("quarantine_all_active_withdrawals", true);
        assert!(storage
            .quarantine_all_active_withdrawals(None)
            .await
            .is_err());
    }

    /// The empty-DB case returns `0` — a successful no-op, not an error.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_empty_db_returns_zero() {
        let (storage, _mock) = make_mock_storage();
        let affected = storage
            .quarantine_all_active_withdrawals(None)
            .await
            .unwrap();
        assert_eq!(affected, 0);
    }

    // ── insert_mint_statuses_batch ────────────────────────────────────

    #[tokio::test]
    async fn insert_mint_statuses_batch_persists_rows() {
        use std::sync::Arc;
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mint = solana_sdk::pubkey::Pubkey::new_unique().to_string();
        storage
            .insert_mint_statuses_batch(&[DbMintStatus {
                mint_address: mint.clone(),
                status: "allowed".to_string(),
                effective_slot: 100,
                signature: "sig-1".to_string(),
                created_at: Utc::now(),
            }])
            .await
            .unwrap();
        let rows = match storage.as_ref() {
            Storage::Mock(m) => m.mint_status_history.lock().unwrap().clone(),
            _ => panic!("expected mock"),
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].mint_address, mint);
    }

    #[tokio::test]
    async fn insert_mint_statuses_batch_idempotent_on_pk_conflict() {
        use std::sync::Arc;
        let storage = Arc::new(Storage::Mock(MockStorage::new()));
        let mint = solana_sdk::pubkey::Pubkey::new_unique().to_string();
        let row = DbMintStatus {
            mint_address: mint.clone(),
            status: "allowed".to_string(),
            effective_slot: 100,
            signature: "sig-1".to_string(),
            created_at: Utc::now(),
        };
        storage
            .insert_mint_statuses_batch(std::slice::from_ref(&row))
            .await
            .unwrap();
        storage.insert_mint_statuses_batch(&[row]).await.unwrap();
        let rows = match storage.as_ref() {
            Storage::Mock(m) => m.mint_status_history.lock().unwrap().clone(),
            _ => panic!("expected mock"),
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn insert_mint_statuses_batch_empty_input_is_noop() {
        let (storage, mock) = make_mock_storage();

        let result = storage.insert_mint_statuses_batch(&[]).await;

        assert!(result.is_ok(), "empty batch should succeed");
        assert_eq!(
            mock.mint_status_history.lock().unwrap().len(),
            0,
            "empty batch must not write any mint status rows"
        );
    }

    // ── get_mint_status_at_slot ──────────────────────────────────────

    fn status_row(mint: &str, status: &str, slot: i64) -> DbMintStatus {
        DbMintStatus {
            mint_address: mint.to_string(),
            status: status.to_string(),
            effective_slot: slot,
            signature: format!("sig-{mint}-{slot}"),
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_never_allowed_when_no_history() {
        let (storage, _mock) = make_mock_storage();
        let res = storage
            .get_mint_status_at_slot("mint_a", 100)
            .await
            .unwrap();
        assert_eq!(res, MintStatusAtSlot::NeverAllowed);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_never_allowed_when_only_future_entry_exists() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 5).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::NeverAllowed);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_allowed_at_exact_effective_slot() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 10).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Allowed);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_allowed_after_allow_entry() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let res = storage
            .get_mint_status_at_slot("mint_a", 100)
            .await
            .unwrap();
        assert_eq!(res, MintStatusAtSlot::Allowed);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_allowed_in_window_between_allow_and_block() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 20),
            ])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 15).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Allowed);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_blocked_after_block_entry() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 20),
            ])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 25).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Blocked);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_blocked_in_window_between_block_and_reallow() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 20),
                status_row("mint_a", "allowed", 30),
            ])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 25).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Blocked);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_returns_allowed_after_reallow_in_cycle() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 20),
                status_row("mint_a", "allowed", 30),
            ])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 35).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Allowed);
    }

    /// Accepted limitation: a same-slot allow + block can't both be stored — PK
    /// `(mint_address, effective_slot)` with `ON CONFLICT DO NOTHING` and no
    /// intra-slot tiebreak, so the first inserted wins. Rare (admin-only); pinned
    /// here so it can't change silently. Allow inserted first → block dropped.
    #[tokio::test]
    async fn get_mint_status_at_slot_same_slot_allow_then_block_keeps_first_inserted() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 10),
            ])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 10).await.unwrap();
        assert_eq!(
            res,
            MintStatusAtSlot::Allowed,
            "first-inserted row wins on a same-slot conflict; the block is dropped",
        );
    }

    // ── orphan query against status history ──────────────────────────

    fn seed_deposit(mock: &MockStorage, id: i64, mint: &str, slot: i64) {
        let mut pending = mock.pending_transactions.lock().unwrap();
        pending.push(DbTransaction {
            id,
            signature: format!("sig-orphan-{id}"),
            trace_id: format!("trace-orphan-{id}"),
            slot,
            initiator: "init".to_string(),
            recipient: "recip".to_string(),
            mint: mint.to_string(),
            amount: TokenAmount(1),
            memo: None,
            transaction_type: TransactionType::Deposit,
            withdrawal_nonce: None,
            status: TransactionStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            processed_at: None,
            counterpart_signature: None,
            remint_signatures: None,
            remint_last_valid_block_heights: None,
            pending_remint_deadline_at: None,
            finality_check_attempts: 0,
            recovery_requeue_attempts: 0,
            instruction_index: 0,
            inner_index: None,
            landed_remint_signature: None,
        });
    }

    #[tokio::test]
    async fn orphan_query_flags_deposit_before_mint_allowed() {
        let (storage, mock) = make_mock_storage();
        seed_deposit(&mock, 1, "mint_a", 5);
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let ids = storage.get_orphan_deposit_ids().await.unwrap();
        assert_eq!(ids, vec![1]);
    }

    #[tokio::test]
    async fn orphan_query_passes_deposit_at_or_after_allow() {
        let (storage, mock) = make_mock_storage();
        seed_deposit(&mock, 1, "mint_a", 10);
        seed_deposit(&mock, 2, "mint_a", 15);
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let ids = storage.get_orphan_deposit_ids().await.unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn orphan_query_flags_deposit_during_blocked_window() {
        let (storage, mock) = make_mock_storage();
        seed_deposit(&mock, 7, "mint_a", 25);
        storage
            .insert_mint_statuses_batch(&[
                status_row("mint_a", "allowed", 10),
                status_row("mint_a", "blocked", 20),
            ])
            .await
            .unwrap();
        let ids = storage.get_orphan_deposit_ids().await.unwrap();
        assert_eq!(ids, vec![7]);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_unrecognized_status_fails_closed_to_blocked() {
        let (storage, _mock) = make_mock_storage();
        // A status value that is neither "allowed" nor "blocked" is data
        // corruption — it must resolve to a not-allowed variant, never Allowed.
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "bogus", 10)])
            .await
            .unwrap();
        let res = storage.get_mint_status_at_slot("mint_a", 15).await.unwrap();
        assert_eq!(res, MintStatusAtSlot::Blocked);
    }

    #[tokio::test]
    async fn get_mint_status_at_slot_distinguishes_status_across_two_mints() {
        let (storage, _mock) = make_mock_storage();
        storage
            .insert_mint_statuses_batch(&[status_row("mint_a", "allowed", 10)])
            .await
            .unwrap();
        let res = storage
            .get_mint_status_at_slot("mint_b", 100)
            .await
            .unwrap();
        assert_eq!(res, MintStatusAtSlot::NeverAllowed);
    }

    /// `exclude_id` must skip the poison row so it is not flipped twice —
    /// the caller has already quarantined it via the async status-update
    /// channel and a second flip here would fire a duplicate webhook.
    #[tokio::test]
    async fn quarantine_all_active_withdrawals_exclude_id_skips_poison_row() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut poison = make_db_transaction();
            poison.id = 42;
            poison.transaction_type = TransactionType::Withdrawal;
            poison.status = TransactionStatus::Processing;
            poison.withdrawal_nonce = Some(1);
            let mut sibling = make_db_transaction();
            sibling.id = 43;
            sibling.transaction_type = TransactionType::Withdrawal;
            sibling.status = TransactionStatus::Pending;
            sibling.withdrawal_nonce = Some(2);
            db.push(poison);
            db.push(sibling);
        }

        let affected = storage
            .quarantine_all_active_withdrawals(Some(42))
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = mock.pending_transactions.lock().unwrap();
        let poison = rows.iter().find(|t| t.id == 42).unwrap();
        assert_eq!(poison.status, TransactionStatus::Processing);
        let sibling = rows.iter().find(|t| t.id == 43).unwrap();
        assert_eq!(sibling.status, TransactionStatus::ManualReview);
    }
}
