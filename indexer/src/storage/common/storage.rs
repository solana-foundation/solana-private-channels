pub use super::models::*;
pub use try_requeue_prebroadcast::RequeueOutcome;

pub mod bump_pending_remint_finality_attempt;
pub mod claim_and_persist_signature;
pub mod claim_remint_attempt;
pub mod close;
pub mod count_pending_transactions;
pub mod delete_release_signature;
pub mod delete_release_signatures;
pub mod delete_remint_signatures;
pub mod drop_tables;
pub mod gc_stale_release_signatures;
pub mod gc_stale_remint_signatures;
pub mod get_all_db_transactions;
pub mod get_and_lock_pending_transactions;
pub mod get_committed_checkpoint;
pub mod get_completed_withdrawal_nonces;
pub mod get_escrow_balances_by_mint;
pub mod get_in_flight_amounts_by_mint;
pub mod get_mint;
pub mod get_mint_balances_for_reconciliation;
pub mod get_mint_status_at_slot;
pub mod get_observed_release;
pub mod get_orphan_deposit_ids;
pub mod get_pending_db_transactions;
pub mod get_pending_remint_transactions;
pub mod get_release_signatures;
pub mod get_remint_signatures;
pub mod get_stale_parked_transactions;
pub mod get_stale_processing_transactions;
pub mod get_stalled_withdrawals_with_signatures;
pub mod get_transaction_status;
pub mod get_withdrawal_by_nonce;
pub mod init_schema;
pub mod insert_db_transaction;
pub mod insert_db_transactions_batch;
pub mod insert_mint_statuses_batch;
pub mod insert_observed_releases_batch;
pub mod insert_release_signature;
pub mod quarantine_active_withdrawals;
pub mod reconciliation_halt;
pub mod record_remint_result;
pub mod sender_lock;
pub mod set_mint_extension_flags;
pub mod set_pending_remint;
pub mod sync_mint_status;
pub mod try_complete_processing;
pub mod try_complete_stalled_withdrawal;
pub mod try_park_processing;
pub mod try_quarantine_processing;
pub mod try_requeue_parked;
pub mod try_requeue_prebroadcast;
pub mod try_requeue_processing;
pub mod try_unpark_to_processing;
pub mod unreleased_withdrawal_nonce_bounds;
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
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, StorageError> {
        update_transaction_status::update_transaction_status(
            self,
            transaction_id,
            status,
            counterpart_signature,
            processed_at,
            release_signatures,
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

    /// Record the `ReleaseFunds` instructions a slot was seen to contain.
    /// Idempotent on the withdrawal nonce, so live indexing, a backfill and a
    /// resync can all report the same release without erroring on the second
    /// write or leaving a duplicate behind.
    pub async fn insert_observed_releases_batch(
        &self,
        releases: &[DbObservedRelease],
    ) -> Result<(), StorageError> {
        insert_observed_releases_batch::insert_observed_releases_batch(self, releases).await
    }

    /// The release recorded for `nonce`, if the indexer has seen one.
    pub async fn get_observed_release(
        &self,
        nonce: u64,
    ) -> Result<Option<DbObservedRelease>, StorageError> {
        get_observed_release::get_observed_release(self, nonce).await
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

    /// Return per-mint aggregate balances (completed deposits minus withdrawals) for
    /// startup reconciliation, counting only what was indexed at or below `as_of_slot`.
    pub async fn get_mint_balances_for_reconciliation(
        &self,
        as_of_slot: u64,
    ) -> Result<Vec<MintDbBalance>, StorageError> {
        get_mint_balances_for_reconciliation::get_mint_balances_for_reconciliation(self, as_of_slot)
            .await
    }

    /// Query escrow balances by mint for continuous reconciliation checks.
    /// Only counts **completed** transactions for both deposits and withdrawals.
    /// Returns per-mint aggregate balances where net_balance = total_deposits - total_withdrawals.
    pub async fn get_escrow_balances_by_mint(&self) -> Result<Vec<MintDbBalance>, StorageError> {
        get_escrow_balances_by_mint::get_escrow_balances_by_mint(self).await
    }

    /// Per-mint sum of every unsettled transaction amount (pending / processing /
    /// parked / pending_remint), used as the in-flight envelope by the runtime
    /// reconciliation halt decision.
    pub async fn get_in_flight_amounts_by_mint(
        &self,
    ) -> Result<Vec<MintInFlightAmount>, StorageError> {
        get_in_flight_amounts_by_mint::get_in_flight_amounts_by_mint(self).await
    }

    /// Set the durable reconciliation halt flag. Idempotent.
    pub async fn set_reconciliation_halt(&self, reason: &str) -> Result<(), StorageError> {
        reconciliation_halt::set_reconciliation_halt(self, reason).await
    }

    /// Return the halt info when the flag is set, else `None` (not halted).
    pub async fn is_reconciliation_halted(&self) -> Result<Option<HaltInfo>, StorageError> {
        reconciliation_halt::is_reconciliation_halted(self).await
    }

    /// Clear the halt so both operators' fetchers resume (manual/runbook use).
    pub async fn clear_reconciliation_halt(&self) -> Result<(), StorageError> {
        reconciliation_halt::clear_reconciliation_halt(self).await
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

    /// Withdrawals stalled in `status` that still carry stored release
    /// signatures, oldest-first. Rows with no usable evidence are excluded.
    pub async fn get_stalled_withdrawals_with_signatures(
        &self,
        status: TransactionStatus,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_stalled_withdrawals_with_signatures::get_stalled_withdrawals_with_signatures(
            self, status, after_id, limit,
        )
        .await
    }

    /// The withdrawal row that owns `nonce`, if any.
    pub async fn get_withdrawal_by_nonce(
        &self,
        nonce: u64,
    ) -> Result<Option<DbTransaction>, StorageError> {
        get_withdrawal_by_nonce::get_withdrawal_by_nonce(self, nonce).await
    }

    /// Transitions a withdrawal to PendingRemint, storing the withdrawal
    /// signatures and their lvbh for the finality check on restart, plus whether
    /// the program itself refused the release. That refusal is proof no payout
    /// occurred and the only such proof that outlives a bitmap rotation, so it is
    /// durable from the moment the refund is queued.
    pub async fn set_pending_remint(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: chrono::DateTime<chrono::Utc>,
        release_refused_on_chain: bool,
    ) -> Result<(), StorageError> {
        set_pending_remint::set_pending_remint(
            self,
            transaction_id,
            remint_signatures,
            remint_last_valid_block_heights,
            deadline_at,
            release_refused_on_chain,
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

    /// Current status of one row, or `None` if it does not exist. Used to
    /// resolve which state a guarded write committed when its result was lost.
    pub async fn get_transaction_status(
        &self,
        transaction_id: i64,
    ) -> Result<Option<TransactionStatus>, StorageError> {
        get_transaction_status::get_transaction_status(self, transaction_id).await
    }

    /// Try to acquire the singleton sender lock for `key`. `Ok(None)` means
    /// another sender holds it, so the caller must refuse to start. The guard
    /// heartbeats the lock and cancels `operator_token` if ownership stops being
    /// provable; a zero `heartbeat_interval` disables probing.
    pub async fn try_acquire_sender_lock(
        &self,
        key: i64,
        program_type: &'static str,
        operator_token: tokio_util::sync::CancellationToken,
        heartbeat_interval: std::time::Duration,
    ) -> Result<Option<sender_lock::SenderLockGuard>, StorageError> {
        sender_lock::try_acquire_sender_lock(
            self,
            key,
            program_type,
            operator_token,
            heartbeat_interval,
        )
        .await
    }

    /// Mark active withdrawal rows at or above `min_nonce` as `ManualReview`.
    ///
    /// Invoked by the processor when a single withdrawal is unprocessable.
    /// The pipeline halts so the withdrawal bitmap cannot rotate past the
    /// quarantined row's generation, which would make its nonce permanently
    /// unreleasable and remove the operator's re-arm option. `min_nonce`
    /// keeps the sweep off lower rows that are still releasable; `None`
    /// sweeps every active row. `exclude_id` is the poison row already
    /// quarantined through the async status-update channel, excluded here
    /// to avoid a duplicate webhook. Returns the number of rows flipped.
    pub async fn quarantine_active_withdrawals(
        &self,
        exclude_id: Option<i64>,
        min_nonce: Option<i64>,
    ) -> Result<u64, StorageError> {
        quarantine_active_withdrawals::quarantine_active_withdrawals(self, exclude_id, min_nonce)
            .await
    }

    /// Stale `Processing` rows of one type past the threshold (used by recovery).
    /// Type-scoped so each operator only recovers rows whose broadcasts target
    /// the chain its RPC client points at.
    pub async fn get_stale_processing_transactions(
        &self,
        threshold: std::time::Duration,
        limit: i64,
        transaction_type: TransactionType,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_stale_processing_transactions::get_stale_processing_transactions(
            self,
            threshold,
            limit,
            transaction_type,
        )
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

    /// Cap-gated CAS `Processing` → `Pending` for sender-side pre-broadcast failures
    /// where the sender owns the Processing row. Enforces the requeue cap inside the
    /// write; see `RequeueOutcome`.
    pub async fn try_requeue_prebroadcast(
        &self,
        transaction_id: i64,
        max_attempts: i32,
    ) -> Result<RequeueOutcome, StorageError> {
        try_requeue_prebroadcast::try_requeue_prebroadcast(self, transaction_id, max_attempts).await
    }

    /// CAS `Processing`/`Parked` → `Parked`; `Ok(false)` if the row is neither.
    pub async fn try_park_processing(&self, transaction_id: i64) -> Result<bool, StorageError> {
        try_park_processing::try_park_processing(self, transaction_id).await
    }

    /// CAS `Parked` to `Processing`, returning the winner's post-update
    /// `updated_at` as the sender's fresh release-claim lease. `Ok(None)` if the
    /// row is not `Parked`.
    pub async fn try_unpark_to_processing(
        &self,
        transaction_id: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        try_unpark_to_processing::try_unpark_to_processing(self, transaction_id).await
    }

    /// Stale `Parked` rows of one type older than the threshold, oldest-first.
    pub async fn get_stale_parked_transactions(
        &self,
        threshold: std::time::Duration,
        limit: i64,
        transaction_type: TransactionType,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        get_stale_parked_transactions::get_stale_parked_transactions(
            self,
            threshold,
            limit,
            transaction_type,
        )
        .await
    }

    /// CAS `Parked` → `Pending` on `updated_at`; `Ok(false)` if stale.
    pub async fn try_requeue_parked(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        try_requeue_parked::try_requeue_parked(self, transaction_id, expected_updated_at).await
    }

    /// CAS `Processing` → `Completed` on `updated_at`; `Ok(false)` if stale.
    /// `release_signatures` durably records the full broadcast attempt list on an
    /// SMT-confirmed release; `None` leaves any existing value intact (COALESCE).
    pub async fn try_complete_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        counterpart_signature: Option<String>,
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, StorageError> {
        try_complete_processing::try_complete_processing(
            self,
            transaction_id,
            expected_updated_at,
            counterpart_signature,
            release_signatures,
        )
        .await
    }

    /// CAS a stalled withdrawal (`ManualReview` or `PendingRemint`) to
    /// `Completed` on `updated_at`; `Ok(false)` if stale or guard-rejected.
    pub async fn try_complete_stalled_withdrawal(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        from_status: TransactionStatus,
        counterpart_signature: Option<String>,
    ) -> Result<bool, StorageError> {
        try_complete_stalled_withdrawal::try_complete_stalled_withdrawal(
            self,
            transaction_id,
            expected_updated_at,
            from_status,
            counterpart_signature,
        )
        .await
    }

    /// CAS `Processing` → `ManualReview`; reason rides on the webhook, not DB.
    /// The optional signature arrays are recorded on the row in the same write;
    /// `None` leaves both columns untouched.
    pub async fn try_quarantine_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        remint_signatures: Option<Vec<String>>,
        remint_last_valid_block_heights: Option<Vec<i64>>,
    ) -> Result<bool, StorageError> {
        try_quarantine_processing::try_quarantine_processing(
            self,
            transaction_id,
            expected_updated_at,
            remint_signatures,
            remint_last_valid_block_heights,
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
        blockhash_slot: Option<i64>,
    ) -> Result<(), StorageError> {
        insert_release_signature::insert_release_signature(
            self,
            transaction_id,
            signature,
            last_valid_block_height,
            blockhash_slot,
        )
        .await
    }

    /// Atomically claim a `Processing` row (CAS on `updated_at`) and persist its
    /// broadcast signature in one transaction. `Ok(Some(lease))` means the sender
    /// still owns the row and may broadcast; the returned lease is the row's new
    /// `updated_at`, which a later re-claim must present. `Ok(None)` means the row
    /// was demoted or re-locked, so the builder must be dropped without
    /// broadcasting. Shared by the deposit mint and the withdrawal release.
    pub async fn claim_and_persist_signature(
        &self,
        transaction_id: i64,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        claim_and_persist_signature::claim_and_persist_signature(
            self,
            transaction_id,
            expected_updated_at,
            signature,
            last_valid_block_height,
            blockhash_slot,
        )
        .await
    }

    /// Stored release signatures for a transaction, newest journal order.
    pub async fn get_release_signatures(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, StorageError> {
        get_release_signatures::get_release_signatures(self, transaction_id).await
    }

    /// Delete all stored release signatures for a transaction.
    pub async fn delete_release_signatures(&self, transaction_id: i64) -> Result<(), StorageError> {
        delete_release_signatures::delete_release_signatures(self, transaction_id).await
    }

    /// Delete one stored release signature, keeping the transaction's others.
    pub async fn delete_release_signature(
        &self,
        transaction_id: i64,
        signature: &str,
    ) -> Result<(), StorageError> {
        delete_release_signature::delete_release_signature(self, transaction_id, signature).await
    }

    /// Lowest and highest withdrawal nonce at or above `min_nonce` that still
    /// owes a release, or `None` when none do. Drives the rotation gate: the low
    /// bound says whether the current generation is finished with, the high
    /// bound whether any work is waiting beyond it.
    pub async fn unreleased_withdrawal_nonce_bounds(
        &self,
        min_nonce: i64,
    ) -> Result<Option<(i64, i64)>, StorageError> {
        unreleased_withdrawal_nonce_bounds::unreleased_withdrawal_nonce_bounds(self, min_nonce)
            .await
    }

    /// Drop release signatures only for genuinely terminal parents (completed,
    /// failed, failed_reminted). Every non-terminal row keeps its write-ahead
    /// evidence so the pre-mint gate can re-verify it. Returns the rows removed.
    pub async fn gc_stale_release_signatures(&self) -> Result<u64, StorageError> {
        gc_stale_release_signatures::gc_stale_release_signatures(self).await
    }

    /// Claim the exclusive right to broadcast one remint attempt for a
    /// transaction, persisting the signature write-ahead in the same step.
    /// `superseded_signatures` are prior attempts the caller has already proven
    /// dead on-chain. `Ok(false)` means another sender owns the live attempt,
    /// so the caller must not broadcast.
    pub async fn claim_remint_attempt(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
        superseded_signatures: &[String],
    ) -> Result<bool, StorageError> {
        claim_remint_attempt::claim_remint_attempt(
            self,
            transaction_id,
            signature,
            last_valid_block_height,
            blockhash_slot,
            superseded_signatures,
        )
        .await
    }

    /// Stored remint signatures for a transaction, newest journal order.
    pub async fn get_remint_signatures(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, StorageError> {
        get_remint_signatures::get_remint_signatures(self, transaction_id).await
    }

    /// Delete all stored remint signatures for a transaction.
    pub async fn delete_remint_signatures(&self, transaction_id: i64) -> Result<(), StorageError> {
        delete_remint_signatures::delete_remint_signatures(self, transaction_id).await
    }

    /// Drop remint signatures whose parent transaction is no longer
    /// `PendingRemint`. Returns the number of rows removed.
    pub async fn gc_stale_remint_signatures(&self) -> Result<u64, StorageError> {
        gc_stale_remint_signatures::gc_stale_remint_signatures(self).await
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
            release_refused_on_chain: false,
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
    async fn get_and_lock_marks_processing_and_leaves_pending() {
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
        assert_eq!(locked.len(), 2, "two Pending deposits are locked");

        // Rows stay in the store (now Processing) so a later claim's CAS can find
        // them, mirroring Postgres; nothing is drained.
        {
            let all = mock.pending_transactions.lock().unwrap();
            assert_eq!(all.len(), 4, "locking keeps rows in place, none removed");
            let processing = all
                .iter()
                .filter(|t| t.status == TransactionStatus::Processing)
                .count();
            assert_eq!(processing, 2, "the two locked deposits are now Processing");
        }

        // A second lock returns only the still-Pending deposit, not the Processing ones.
        let locked2 = storage
            .get_and_lock_pending_transactions(TransactionType::Deposit, 10)
            .await
            .unwrap();
        assert_eq!(
            locked2.len(),
            1,
            "only the remaining Pending deposit re-locks"
        );
    }

    /// The resolved dequeue has no nonce frontier and orders by `created_at`.
    /// Seeded so nonce order and insertion order disagree, the mock must follow
    /// `created_at` and must not let the quarantined lower nonce withhold anything.
    #[tokio::test]
    async fn get_and_lock_withdrawals_ignores_lower_active_nonces_and_orders_by_created_at() {
        let (storage, mock) = make_mock_storage();
        let base = Utc::now();
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            for (id, nonce, status, age_secs) in [
                (1_i64, 5_i64, TransactionStatus::ManualReview, 0_i64),
                (2, 7, TransactionStatus::Pending, 1),
                (3, 6, TransactionStatus::Pending, 2),
            ] {
                let mut txn = make_db_transaction();
                txn.id = id;
                txn.transaction_type = TransactionType::Withdrawal;
                txn.status = status;
                txn.withdrawal_nonce = Some(nonce);
                txn.created_at = base + chrono::Duration::seconds(age_secs);
                pending.push(txn);
            }
        }

        let locked = storage
            .get_and_lock_pending_transactions(TransactionType::Withdrawal, 100)
            .await
            .unwrap();
        let ids: Vec<i64> = locked.iter().map(|txn| txn.id).collect();
        assert_eq!(
            ids,
            vec![2, 3],
            "both Pending withdrawals above the quarantined nonce are dequeued, oldest first"
        );
    }

    /// `set_pending_remint` is one row in Postgres, so the refusal flag has to be
    /// readable through every path, not just the rehydration list.
    #[tokio::test]
    async fn set_pending_remint_refusal_is_visible_by_nonce() {
        let (storage, mock) = make_mock_storage();
        {
            let mut pending = mock.pending_transactions.lock().unwrap();
            let mut txn = make_db_transaction();
            txn.id = 42;
            txn.transaction_type = TransactionType::Withdrawal;
            txn.status = TransactionStatus::Processing;
            txn.withdrawal_nonce = Some(9);
            pending.push(txn);
        }

        storage
            .set_pending_remint(
                42,
                vec!["sig1".to_string()],
                vec![100],
                Utc::now() + chrono::Duration::seconds(32),
                true,
            )
            .await
            .unwrap();

        let by_nonce = storage.get_withdrawal_by_nonce(9).await.unwrap().unwrap();
        assert!(
            by_nonce.release_refused_on_chain,
            "the authoritative row carries the on-chain refusal"
        );

        let rehydrated = storage.get_pending_remint_transactions().await.unwrap();
        assert_eq!(rehydrated.len(), 1);
        assert!(
            rehydrated[0].release_refused_on_chain,
            "the rehydration copy still carries it too"
        );
    }

    // ── claim_and_persist_signature disposition matrix ────────────────

    /// Seed one row directly with an explicit type, status and `updated_at` so
    /// the claim CAS can be exercised against each disposition.
    fn seed_claim_row(
        mock: &MockStorage,
        id: i64,
        transaction_type: TransactionType,
        status: TransactionStatus,
        updated_at: chrono::DateTime<Utc>,
    ) {
        let mut row = make_db_transaction();
        row.id = id;
        row.transaction_type = transaction_type;
        row.status = status;
        row.updated_at = updated_at;
        mock.pending_transactions.lock().unwrap().push(row);
    }

    /// The claim is the single gate both the deposit mint and the withdrawal
    /// release pass before broadcasting, so it is pinned for both row types: it
    /// succeeds only on the exact `Processing` incarnation the caller was handed,
    /// and every other disposition aborts without persisting a signature. A lost
    /// claim on the withdrawal side is what makes a recovery demote safe against
    /// a live sender.
    #[tokio::test]
    async fn claim_and_persist_signature_disposition_matrix() {
        // (label, seeded status, token offset from the presented one, claimable)
        let dispositions = [
            ("owned", TransactionStatus::Processing, 0, true),
            ("demoted", TransactionStatus::Pending, 0, false),
            ("token stale", TransactionStatus::Processing, 30, false),
            ("terminal", TransactionStatus::Completed, 0, false),
        ];

        for txn_type in [TransactionType::Deposit, TransactionType::Withdrawal] {
            for (id, (label, status, skew_secs, claimable)) in dispositions.iter().enumerate() {
                let (storage, mock) = make_mock_storage();
                let id = id as i64 + 1;
                let presented = Utc::now();
                let seeded = presented + chrono::Duration::seconds(*skew_secs);
                seed_claim_row(&mock, id, txn_type, *status, seeded);

                let case = format!("{txn_type:?}/{label}");
                let signature = format!("sig-{case}");
                let claimed = storage
                    .claim_and_persist_signature(id, presented, signature.clone(), 100, None)
                    .await
                    .unwrap();
                let persisted = storage.get_release_signatures(id).await.unwrap();

                if !claimable {
                    assert!(claimed.is_none(), "{case}: must not be claimable");
                    assert!(
                        persisted.is_empty(),
                        "{case}: no signature may be persisted on a lost claim"
                    );
                    continue;
                }

                let lease = claimed.expect("{case}: owning the incarnation must claim");
                assert_eq!(
                    persisted.len(),
                    1,
                    "{case}: the signature must be persisted"
                );
                assert_eq!(
                    persisted[0].signature, signature,
                    "{case}: signature mismatch"
                );

                let after = mock.pending_transactions.lock().unwrap()[0].updated_at;
                assert_ne!(after, presented, "{case}: a claim must bump updated_at");
                assert_eq!(
                    lease, after,
                    "{case}: the lease must equal the row's new updated_at so it is usable as the next CAS token"
                );
            }
        }
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
                None,
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
            .update_transaction_status(1, TransactionStatus::Completed, None, Utc::now(), None)
            .await
            .is_err());
    }

    // Durable release_signatures on completion

    /// try_complete_processing with a release-signature list persists the
    /// array alongside the single counterpart_signature.
    #[tokio::test]
    async fn try_complete_processing_persists_release_signatures() {
        let (storage, mock) = make_mock_storage();
        let now = Utc::now();
        {
            let mut txn = make_db_transaction();
            txn.id = 1;
            txn.status = TransactionStatus::Processing;
            txn.updated_at = now;
            mock.pending_transactions.lock().unwrap().push(txn);
        }

        let ok = storage
            .try_complete_processing(
                1,
                now,
                Some("s1".to_string()),
                Some(vec!["s1".to_string(), "s2".to_string()]),
            )
            .await
            .unwrap();
        assert!(ok);

        assert_eq!(
            mock.completed_release_signatures.lock().unwrap().get(&1),
            Some(&vec!["s1".to_string(), "s2".to_string()])
        );
        assert_eq!(
            mock.pending_transactions.lock().unwrap()[0]
                .counterpart_signature
                .as_deref(),
            Some("s1")
        );
    }

    /// A None release-signature list is COALESCE-guarded and never wipes an
    /// existing array.
    #[tokio::test]
    async fn try_complete_processing_none_preserves_existing_release_signatures() {
        let (storage, mock) = make_mock_storage();
        let now = Utc::now();
        {
            let mut txn = make_db_transaction();
            txn.id = 2;
            txn.status = TransactionStatus::Processing;
            txn.updated_at = now;
            mock.pending_transactions.lock().unwrap().push(txn);
        }
        // An array already recorded (e.g. a prior write).
        mock.completed_release_signatures
            .lock()
            .unwrap()
            .insert(2, vec!["old1".to_string(), "old2".to_string()]);

        let ok = storage
            .try_complete_processing(2, now, Some("cp".to_string()), None)
            .await
            .unwrap();
        assert!(ok);

        assert_eq!(
            mock.completed_release_signatures.lock().unwrap().get(&2),
            Some(&vec!["old1".to_string(), "old2".to_string()]),
            "None must not clobber an existing array"
        );
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
            .get_mint_balances_for_reconciliation(900)
            .await
            .unwrap();
        assert_eq!(
            mock.last_reconciliation_slot(),
            Some(900),
            "the slot bound must reach storage, not be dropped on the way"
        );
        assert_eq!(balances.len(), 2);
        assert!(balances.iter().any(|b| b.mint_address == "usdc"
            && b.total_deposits == 10000u64
            && b.total_withdrawals == 5000u64));
        assert!(balances.iter().any(|b| b.mint_address == "usdt"
            && b.total_deposits == 8000u64
            && b.total_withdrawals == 3000u64));
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

    /// The escalation exists because the outcome is unknown, and the broadcast
    /// signatures are the only thing that can still settle it. Every other
    /// terminal status names a decided outcome with nothing left to reconstruct,
    /// so the sweep still reclaims those.
    #[tokio::test]
    async fn release_signature_gc_keeps_processing_and_manual_review_only() {
        let cases = [
            (TransactionStatus::Processing, true),
            (TransactionStatus::ManualReview, true),
            (TransactionStatus::Completed, false),
            (TransactionStatus::Failed, false),
            (TransactionStatus::FailedReminted, false),
        ];

        for (status, retained) in cases {
            let (storage, mock) = make_mock_storage();
            let mut row = make_db_transaction();
            row.id = 1;
            row.status = status;
            mock.pending_transactions.lock().unwrap().push(row);
            storage
                .insert_release_signature(1, "sig-gc".to_string(), 10, None)
                .await
                .unwrap();

            storage.gc_stale_release_signatures().await.unwrap();

            assert_eq!(
                !storage.get_release_signatures(1).await.unwrap().is_empty(),
                retained,
                "{status:?} must {} its signatures",
                if retained { "keep" } else { "lose" }
            );
        }
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

    // ── quarantine_active_withdrawals ─────────────────────────────

    /// Only Pending and Processing withdrawals flip to ManualReview.
    /// Returns the exact number of rows affected so the caller can log
    /// the blast radius.
    #[tokio::test]
    async fn quarantine_active_withdrawals_flips_pending_and_processing_only() {
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
            .quarantine_active_withdrawals(None, None)
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
    async fn quarantine_active_withdrawals_leaves_deposits_untouched() {
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
            .quarantine_active_withdrawals(None, None)
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
    async fn quarantine_active_withdrawals_leaves_terminal_rows_untouched() {
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
            .quarantine_active_withdrawals(None, None)
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
    async fn quarantine_active_withdrawals_propagates_mock_failure() {
        let (storage, mock) = make_mock_storage();
        mock.set_should_fail("quarantine_active_withdrawals", true);
        assert!(storage
            .quarantine_active_withdrawals(None, None)
            .await
            .is_err());
    }

    /// The empty-DB case returns `0` — a successful no-op, not an error.
    #[tokio::test]
    async fn quarantine_active_withdrawals_empty_db_returns_zero() {
        let (storage, _mock) = make_mock_storage();
        let affected = storage
            .quarantine_active_withdrawals(None, None)
            .await
            .unwrap();
        assert_eq!(affected, 0);
    }

    /// `Parked` is an active status the SQL sweeps alongside Pending and
    /// Processing, so the mock has to sweep it too or every unit test in
    /// this module pins a contract production does not implement.
    #[tokio::test]
    async fn quarantine_active_withdrawals_flips_parked_rows() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            let mut parked = make_db_transaction();
            parked.transaction_type = TransactionType::Withdrawal;
            parked.status = TransactionStatus::Parked;
            parked.withdrawal_nonce = Some(1);
            db.push(parked);
        }

        let affected = storage
            .quarantine_active_withdrawals(None, None)
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = mock.pending_transactions.lock().unwrap();
        assert_eq!(rows[0].status, TransactionStatus::ManualReview);
    }

    /// Seed three active withdrawals at nonces 1, 2 and 3.
    fn seed_three_active_withdrawals(mock: &MockStorage) {
        let mut db = mock.pending_transactions.lock().unwrap();
        for nonce in 1..=3 {
            let mut txn = make_db_transaction();
            txn.id = nonce;
            txn.transaction_type = TransactionType::Withdrawal;
            txn.status = TransactionStatus::Pending;
            txn.withdrawal_nonce = Some(nonce);
            db.push(txn);
        }
    }

    /// Rows below the poison nonce are releasable and sender-owned, so the
    /// halt sweep must leave them alone.
    #[tokio::test]
    async fn quarantine_active_withdrawals_min_nonce_leaves_lower_rows_untouched() {
        let (storage, mock) = make_mock_storage();
        seed_three_active_withdrawals(&mock);

        let affected = storage
            .quarantine_active_withdrawals(None, Some(2))
            .await
            .unwrap();
        assert_eq!(affected, 2);

        let rows = mock.pending_transactions.lock().unwrap();
        let low = rows.iter().find(|t| t.withdrawal_nonce == Some(1)).unwrap();
        assert_eq!(low.status, TransactionStatus::Pending);
        for nonce in [2, 3] {
            let row = rows
                .iter()
                .find(|t| t.withdrawal_nonce == Some(nonce))
                .unwrap();
            assert_eq!(row.status, TransactionStatus::ManualReview);
        }
    }

    /// A `None` floor keeps the original unbounded sweep, which is the
    /// fail-closed fallback for a poison row that has no nonce at all.
    #[tokio::test]
    async fn quarantine_active_withdrawals_none_min_nonce_sweeps_all() {
        let (storage, mock) = make_mock_storage();
        seed_three_active_withdrawals(&mock);

        let affected = storage
            .quarantine_active_withdrawals(None, None)
            .await
            .unwrap();
        assert_eq!(affected, 3);

        let rows = mock.pending_transactions.lock().unwrap();
        for txn in rows.iter() {
            assert_eq!(txn.status, TransactionStatus::ManualReview);
        }
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
            release_refused_on_chain: false,
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
    async fn quarantine_active_withdrawals_exclude_id_skips_poison_row() {
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
            .quarantine_active_withdrawals(Some(42), None)
            .await
            .unwrap();
        assert_eq!(affected, 1);

        let rows = mock.pending_transactions.lock().unwrap();
        let poison = rows.iter().find(|t| t.id == 42).unwrap();
        assert_eq!(poison.status, TransactionStatus::Processing);
        let sibling = rows.iter().find(|t| t.id == 43).unwrap();
        assert_eq!(sibling.status, TransactionStatus::ManualReview);
    }

    /// The mock must reject exactly what the SQL rejects, otherwise every
    /// mock-backed test of the reconcile sweep rests on a filter that does not
    /// exist in production.
    #[tokio::test]
    async fn stalled_withdrawal_query_filters_match_sql() {
        let (storage, mock) = make_mock_storage();
        let stalled = |id: i64, sigs: Option<Vec<String>>| {
            let mut row = make_db_transaction();
            row.id = id;
            row.transaction_type = TransactionType::Withdrawal;
            row.status = TransactionStatus::ManualReview;
            row.withdrawal_nonce = Some(id);
            row.remint_last_valid_block_heights = sigs.as_ref().map(|s| vec![0i64; s.len()]);
            row.remint_signatures = sigs;
            row
        };
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            db.push(stalled(1, Some(vec!["sig-a".to_string()])));

            let mut wrong_status = stalled(2, Some(vec!["sig-b".to_string()]));
            wrong_status.status = TransactionStatus::PendingRemint;
            db.push(wrong_status);

            let mut deposit = stalled(3, Some(vec!["sig-c".to_string()]));
            deposit.transaction_type = TransactionType::Deposit;
            db.push(deposit);

            let mut no_nonce = stalled(4, Some(vec!["sig-d".to_string()]));
            no_nonce.withdrawal_nonce = None;
            db.push(no_nonce);

            db.push(stalled(5, Some(Vec::new())));
            db.push(stalled(6, None));

            // Reachable on a database upgraded between the two column
            // migrations: signatures present, heights never backfilled.
            let mut no_heights = stalled(7, Some(vec!["sig-e".to_string()]));
            no_heights.remint_last_valid_block_heights = None;
            db.push(no_heights);

            // Refund already landed, and refund already claimed: promoting
            // either on release evidence would pay the nonce twice.
            let mut landed_refund = stalled(8, Some(vec!["sig-f".to_string()]));
            landed_refund.landed_remint_signature = Some("sig-refund".to_string());
            db.push(landed_refund);
            db.push(stalled(9, Some(vec!["sig-g".to_string()])));
        }
        mock.claim_remint_attempt(9, "sig-claim".to_string(), 0, None, &[])
            .await
            .unwrap();

        let found = storage
            .get_stalled_withdrawals_with_signatures(TransactionStatus::ManualReview, 0, 100)
            .await
            .unwrap();

        assert_eq!(
            found.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![1],
            "only the row with a usable signature set may be returned"
        );
    }

    // ── unreleased withdrawal nonce bounds ───────────────────────────

    /// Seed a withdrawal at `nonce` in `status` into the mock's row table.
    fn seed_withdrawal(mock: &MockStorage, id: i64, nonce: Option<i64>, status: TransactionStatus) {
        let mut txn = make_db_transaction();
        txn.id = id;
        txn.transaction_type = TransactionType::Withdrawal;
        txn.withdrawal_nonce = nonce;
        txn.status = status;
        mock.pending_transactions.lock().unwrap().push(txn);
    }

    /// Every status that still owes a release, and every status that does not.
    /// One list drives both the inclusion and the exclusion tests so the two can
    /// never drift apart, and it is the same split the SQL encodes.
    const LIVE_STATUSES: [TransactionStatus; 5] = [
        TransactionStatus::Pending,
        TransactionStatus::Processing,
        TransactionStatus::Parked,
        TransactionStatus::PendingRemint,
        TransactionStatus::ManualReview,
    ];
    const TERMINAL_STATUSES: [TransactionStatus; 3] = [
        TransactionStatus::Completed,
        TransactionStatus::Failed,
        TransactionStatus::FailedReminted,
    ];

    /// A released or written-off nonce can never need its window again, so it
    /// must not hold the rotation back.
    #[tokio::test]
    async fn nonce_bounds_ignores_terminal_statuses() {
        for (offset, status) in TERMINAL_STATUSES.into_iter().enumerate() {
            let (storage, mock) = make_mock_storage();
            seed_withdrawal(&mock, offset as i64 + 1, Some(5), status);
            assert_eq!(
                storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
                None,
                "{status:?} is terminal and must not count as unreleased"
            );
        }
    }

    /// Rotating past any of these closes the only window their release can land in.
    #[tokio::test]
    async fn nonce_bounds_counts_every_live_status() {
        for (offset, status) in LIVE_STATUSES.into_iter().enumerate() {
            let (storage, mock) = make_mock_storage();
            seed_withdrawal(&mock, offset as i64 + 1, Some(7), status);
            assert_eq!(
                storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
                Some((7, 7)),
                "{status:?} still owes a release and must count"
            );
        }
    }

    #[tokio::test]
    async fn nonce_bounds_none_when_no_live_rows() {
        let (storage, mock) = make_mock_storage();
        assert_eq!(
            storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
            None,
            "an empty table owes nothing"
        );

        for (offset, status) in TERMINAL_STATUSES.into_iter().enumerate() {
            seed_withdrawal(&mock, offset as i64 + 1, Some(offset as i64), status);
        }
        assert_eq!(
            storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
            None,
            "an all-terminal table owes nothing"
        );
    }

    /// Deposits carry no nonce and a NULL nonce cannot be compared to a
    /// generation, so neither may contribute a bound.
    #[tokio::test]
    async fn nonce_bounds_ignores_deposits_and_null_nonces() {
        let (storage, mock) = make_mock_storage();

        let mut deposit = make_db_transaction();
        deposit.id = 1;
        deposit.withdrawal_nonce = Some(1);
        mock.pending_transactions.lock().unwrap().push(deposit);
        seed_withdrawal(&mock, 2, None, TransactionStatus::Pending);
        assert_eq!(
            storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
            None,
            "neither a deposit nor a NULL nonce may set a bound"
        );

        seed_withdrawal(&mock, 3, Some(4), TransactionStatus::Pending);
        seed_withdrawal(&mock, 4, Some(9), TransactionStatus::Parked);
        assert_eq!(
            storage.unreleased_withdrawal_nonce_bounds(0).await.unwrap(),
            Some((4, 9)),
            "bounds span only the live withdrawals that carry a nonce"
        );
    }

    /// The floor is what lets the rotation gate ignore nonces whose window has
    /// already closed, so it has to exclude them from both bounds.
    #[tokio::test]
    async fn nonce_bounds_honours_the_floor() {
        let (storage, mock) = make_mock_storage();
        seed_withdrawal(&mock, 1, Some(3), TransactionStatus::ManualReview);
        seed_withdrawal(&mock, 2, Some(11), TransactionStatus::Pending);
        seed_withdrawal(&mock, 3, Some(20), TransactionStatus::Parked);

        assert_eq!(
            storage
                .unreleased_withdrawal_nonce_bounds(10)
                .await
                .unwrap(),
            Some((11, 20)),
            "a nonce below the floor sets neither bound"
        );
        assert_eq!(
            storage
                .unreleased_withdrawal_nonce_bounds(21)
                .await
                .unwrap(),
            None,
            "a floor above every live nonce leaves nothing"
        );
    }

    #[tokio::test]
    async fn nonce_bounds_respects_should_fail() {
        let (storage, mock) = make_mock_storage();
        mock.set_should_fail("unreleased_withdrawal_nonce_bounds", true);
        assert!(storage.unreleased_withdrawal_nonce_bounds(0).await.is_err());
    }

    // ── reconciliation halt flag ──────────────────────────────────────

    #[tokio::test]
    async fn set_then_is_halted_returns_reason() {
        let (storage, _mock) = make_mock_storage();
        storage
            .set_reconciliation_halt("mint X insolvent")
            .await
            .unwrap();
        let info = storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("halt should be set");
        assert_eq!(info.reason, "mint X insolvent");
    }

    #[tokio::test]
    async fn absent_is_not_halted() {
        let (storage, _mock) = make_mock_storage();
        assert!(storage.is_reconciliation_halted().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn clear_unsets() {
        let (storage, _mock) = make_mock_storage();
        storage.set_reconciliation_halt("reason").await.unwrap();
        storage.clear_reconciliation_halt().await.unwrap();
        assert!(storage.is_reconciliation_halted().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_is_idempotent_on_conflict() {
        let (storage, _mock) = make_mock_storage();
        storage.set_reconciliation_halt("first").await.unwrap();
        storage.set_reconciliation_halt("second").await.unwrap();
        let info = storage
            .is_reconciliation_halted()
            .await
            .unwrap()
            .expect("halt should still be set");
        assert_eq!(info.reason, "second", "re-set overwrites the reason");
    }

    // ── in-flight envelope query ──────────────────────────────────────

    fn in_flight_txn(id: i64, mint: &str, amount: u64, status: TransactionStatus) -> DbTransaction {
        let mut t = make_db_transaction();
        t.id = id;
        t.mint = mint.to_string();
        t.amount = TokenAmount(amount);
        t.status = status;
        t
    }

    #[tokio::test]
    async fn in_flight_sums_only_in_flight_statuses() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            db.push(in_flight_txn(1, "mint_a", 100, TransactionStatus::Pending));
            db.push(in_flight_txn(
                2,
                "mint_a",
                200,
                TransactionStatus::Processing,
            ));
            db.push(in_flight_txn(3, "mint_a", 400, TransactionStatus::Parked));
            db.push(in_flight_txn(
                4,
                "mint_a",
                800,
                TransactionStatus::PendingRemint,
            ));
            // Terminal statuses must be excluded from the envelope.
            db.push(in_flight_txn(5, "mint_a", 1, TransactionStatus::Completed));
            db.push(in_flight_txn(6, "mint_a", 2, TransactionStatus::Failed));
            db.push(in_flight_txn(
                7,
                "mint_a",
                4,
                TransactionStatus::ManualReview,
            ));
        }
        let rows = storage.get_in_flight_amounts_by_mint().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].mint_address, "mint_a");
        assert_eq!(rows[0].in_flight_amount, BigDecimal::from(1500u64));
    }

    #[tokio::test]
    async fn in_flight_groups_per_mint() {
        let (storage, mock) = make_mock_storage();
        {
            let mut db = mock.pending_transactions.lock().unwrap();
            db.push(in_flight_txn(1, "mint_a", 100, TransactionStatus::Pending));
            db.push(in_flight_txn(
                2,
                "mint_b",
                250,
                TransactionStatus::Processing,
            ));
        }
        let mut rows = storage.get_in_flight_amounts_by_mint().await.unwrap();
        rows.sort_by(|a, b| a.mint_address.cmp(&b.mint_address));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].mint_address, "mint_a");
        assert_eq!(rows[0].in_flight_amount, BigDecimal::from(100u64));
        assert_eq!(rows[1].mint_address, "mint_b");
        assert_eq!(rows[1].in_flight_amount, BigDecimal::from(250u64));
    }

    #[tokio::test]
    async fn in_flight_empty_is_absent() {
        let (storage, _mock) = make_mock_storage();
        assert!(storage
            .get_in_flight_amounts_by_mint()
            .await
            .unwrap()
            .is_empty());
    }
}
