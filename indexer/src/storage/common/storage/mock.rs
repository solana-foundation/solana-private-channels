use crate::error::StorageError;
use crate::storage::common::models::{
    DbMint, DbMintStatus, DbTransaction, HaltInfo, MintDbBalance, MintInFlightAmount,
    MintStatusAtSlot, StoredSig, TransactionStatus, TransactionType,
};
use crate::storage::common::storage::RequeueOutcome;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Recorded status update from `update_transaction_status`.
pub type StatusUpdateRecord = (i64, TransactionStatus, Option<String>, DateTime<Utc>);

/// (transaction_id, signatures, last_valid_block_heights, deadline) persisted on PendingRemint transition.
pub type PendingRemintRecord = (i64, Vec<String>, Vec<i64>, DateTime<Utc>);

/// In-memory mirror of `pending_release_signatures`, keyed by transaction id.
pub type ReleaseSignatureMap = HashMap<i64, Vec<StoredSig>>;

#[derive(Clone, Default)]
pub struct MockStorage {
    pub committed_checkpoints: std::sync::Arc<Mutex<HashMap<String, u64>>>,
    pub should_fail: std::sync::Arc<Mutex<HashMap<String, bool>>>,
    /// Per-op transient-failure counters: fail the first N calls of an op, then succeed.
    pub fail_times: std::sync::Arc<Mutex<HashMap<String, usize>>>,
    /// Per-op call counts (bumped in `check_should_fail`); tests assert loop convergence.
    pub call_counts: std::sync::Arc<Mutex<HashMap<String, usize>>>,
    pub mints: std::sync::Arc<Mutex<HashMap<String, DbMint>>>,
    pub mint_balances: std::sync::Arc<Mutex<Vec<MintDbBalance>>>,
    /// Slot the last reconciliation balance read was bounded by, so a test can prove the
    /// bound reached storage. The stored balances are pre-aggregated with no slot of
    /// their own, so the mock records the bound rather than applying it.
    pub last_reconciliation_slot: std::sync::Arc<Mutex<Option<u64>>>,
    pub pending_transactions: std::sync::Arc<Mutex<Vec<DbTransaction>>>,
    pub inserted_transactions: std::sync::Arc<Mutex<Vec<Vec<DbTransaction>>>>,
    pub inserted_single_transactions: std::sync::Arc<Mutex<Vec<DbTransaction>>>,
    pub status_updates: std::sync::Arc<Mutex<Vec<StatusUpdateRecord>>>,
    /// Signatures stored per transaction on PendingRemint transition, keyed as (transaction_id, remint_signatures, deadline_at).                                                  
    /// Used in tests to verify the correct withdrawal signatures were persisted.                                                                         
    pub pending_remint_signatures: std::sync::Arc<Mutex<Vec<PendingRemintRecord>>>,
    /// Transactions currently in PendingRemint status, used in tests to simulate startup recovery.
    pub pending_remint_transactions: std::sync::Arc<Mutex<Vec<DbTransaction>>>,
    pub mint_status_history: Arc<Mutex<Vec<DbMintStatus>>>,
    /// Mirrors the `pending_release_signatures` table for verify-before-demote.
    pub release_signatures: Arc<Mutex<ReleaseSignatureMap>>,
    /// Mirrors the `pending_remint_signatures` write-ahead table.
    pub remint_signatures: Arc<Mutex<ReleaseSignatureMap>>,
    /// Mirrors the `superseded` column: attempts retired after being proven dead.
    pub superseded_remint_signatures: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Transactions whose live remint claim is held by a second sender process.
    /// A single in-process mock is the shared database, so this is the only way
    /// to represent the other operator's row that the partial unique index
    /// arbitrates against. The real arbiter is covered against Postgres.
    pub foreign_remint_claims: Arc<Mutex<std::collections::HashSet<i64>>>,
    /// Mirrors the durable `transactions.release_signatures` column: the full
    /// attempt list written on an SMT-confirmed completion. COALESCE-guarded.
    pub completed_release_signatures: Arc<Mutex<HashMap<i64, Vec<String>>>>,
    /// Mirrors the single-row `reconciliation_halt` table; `None` = not halted.
    pub reconciliation_halt: Arc<Mutex<Option<HaltInfo>>>,
    /// Mirrors `indexer_state.owed_rotation_target`: the tree generation the sender
    /// owes, per program type. An absent key is a NULL column.
    pub owed_rotation_targets: Arc<Mutex<HashMap<String, u64>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn check_should_fail(&self, operation: &str) -> Result<(), StorageError> {
        *self
            .call_counts
            .lock()
            .unwrap()
            .entry(operation.to_string())
            .or_default() += 1;
        // Transient injection takes precedence: fail the first N calls, then
        // fall through to the sticky bool (and otherwise succeed).
        {
            let mut times = self.fail_times.lock().unwrap();
            if let Some(remaining) = times.get_mut(operation) {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(StorageError::DatabaseError {
                        message: format!("Simulated transient {operation} failure"),
                    });
                }
            }
        }
        if self
            .should_fail
            .lock()
            .unwrap()
            .get(operation)
            .copied()
            .unwrap_or(false)
        {
            return Err(StorageError::DatabaseError {
                message: format!("Simulated {operation} failure"),
            });
        }
        Ok(())
    }

    pub fn set_checkpoint(&self, program_type: &str, slot: u64) {
        self.committed_checkpoints
            .lock()
            .unwrap()
            .insert(program_type.to_string(), slot);
    }

    pub fn set_should_fail(&self, program_type: &str, should_fail: bool) {
        self.should_fail
            .lock()
            .unwrap()
            .insert(program_type.to_string(), should_fail);
    }

    /// How many times `operation` has been invoked on this mock.
    pub fn calls(&self, operation: &str) -> usize {
        self.call_counts
            .lock()
            .unwrap()
            .get(operation)
            .copied()
            .unwrap_or(0)
    }

    /// Make `operation` fail its next `times` calls, then succeed. Used to
    /// simulate a transient storage blip that the write retry rides out.
    pub fn set_fail_times(&self, operation: &str, times: usize) {
        self.fail_times
            .lock()
            .unwrap()
            .insert(operation.to_string(), times);
    }

    pub fn add_mint(&mut self, mint: DbMint) {
        self.mints
            .lock()
            .unwrap()
            .insert(mint.mint_address.clone(), mint);
    }

    pub async fn init_schema(&self) -> Result<(), StorageError> {
        Ok(())
    }

    pub async fn drop_tables(&self) -> Result<(), StorageError> {
        Ok(())
    }

    pub async fn insert_db_transaction(
        &self,
        transaction: &DbTransaction,
    ) -> Result<i64, StorageError> {
        self.check_should_fail("insert_db_transaction")?;
        let mut store = self.inserted_single_transactions.lock().unwrap();
        let id = store.len() as i64 + 1;
        store.push(transaction.clone());
        Ok(id)
    }

    pub async fn insert_db_transactions_batch(
        &self,
        transactions: &[DbTransaction],
    ) -> Result<Vec<i64>, StorageError> {
        self.check_should_fail("insert_db_transactions_batch")?;
        let mut store = self.inserted_transactions.lock().unwrap();
        let base = store.iter().map(|b| b.len()).sum::<usize>() as i64;
        store.push(transactions.to_vec());
        let ids: Vec<i64> = (base + 1..=base + transactions.len() as i64).collect();
        Ok(ids)
    }

    pub async fn get_pending_db_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        let pending = self.pending_transactions.lock().unwrap();
        let result: Vec<DbTransaction> = pending
            .iter()
            .filter(|t| t.transaction_type == transaction_type)
            .take(limit as usize)
            .cloned()
            .collect();
        Ok(result)
    }

    pub async fn get_all_db_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, Box<dyn std::error::Error + Send + Sync>> {
        let pending = self.pending_transactions.lock().unwrap();
        let result: Vec<DbTransaction> = pending
            .iter()
            .filter(|t| t.transaction_type == transaction_type)
            .take(limit as usize)
            .cloned()
            .collect();
        Ok(result)
    }

    pub async fn get_and_lock_pending_transactions(
        &self,
        transaction_type: TransactionType,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        let mut pending = self.pending_transactions.lock().unwrap();

        // Withdrawals: mirror the Postgres frontier dequeue. Return Pending
        // withdrawals in nonce order, only those below the lowest active
        // (non-Pending) nonce, and mark them Processing in place (kept in the
        // store so they act as the barrier on the next call).
        if matches!(transaction_type, TransactionType::Withdrawal) {
            let barrier = pending
                .iter()
                .filter(|t| {
                    t.transaction_type == TransactionType::Withdrawal
                        && matches!(
                            t.status,
                            TransactionStatus::Processing
                                | TransactionStatus::Parked
                                | TransactionStatus::PendingRemint
                                | TransactionStatus::ManualReview
                        )
                })
                .filter_map(|t| t.withdrawal_nonce)
                .min();

            // Numbered nonces below the frontier, in nonce order.
            let mut numbered: Vec<(i64, i64)> = pending
                .iter()
                .filter(|t| {
                    t.transaction_type == TransactionType::Withdrawal
                        && t.status == TransactionStatus::Pending
                })
                .filter_map(|t| t.withdrawal_nonce.map(|nonce| (nonce, t.id)))
                .filter(|(nonce, _)| barrier.is_none_or(|b| *nonce < b))
                .collect();
            numbered.sort_by_key(|(nonce, _)| *nonce);

            // NULL-nonce rows are poison; the frontier doesn't apply. Dequeue them
            // (sorted last, mirroring SQL ORDER BY ... ASC) so the processor can
            // quarantine them.
            let null_nonce_ids = pending
                .iter()
                .filter(|t| {
                    t.transaction_type == TransactionType::Withdrawal
                        && t.status == TransactionStatus::Pending
                        && t.withdrawal_nonce.is_none()
                })
                .map(|t| t.id);

            let mut ids: Vec<i64> = numbered.into_iter().map(|(_, id)| id).collect();
            ids.extend(null_nonce_ids);
            ids.truncate(limit.max(0) as usize);

            let mut matched = Vec::new();
            for id in ids {
                if let Some(txn) = pending.iter_mut().find(|t| t.id == id) {
                    txn.status = TransactionStatus::Processing;
                    matched.push(txn.clone());
                }
            }
            return Ok(matched);
        }

        // Deposits: FIFO by insertion order. Mirror Postgres: lock only Pending
        // rows, flip them to Processing in place (keep them in the store so a
        // later claim's CAS can find the row), and hand back the post-lock token.
        let mut matched = Vec::new();
        for txn in pending.iter_mut() {
            if txn.transaction_type == transaction_type
                && txn.status == TransactionStatus::Pending
                && (matched.len() as i64) < limit
            {
                txn.status = TransactionStatus::Processing;
                txn.updated_at = Utc::now();
                matched.push(txn.clone());
            }
        }
        Ok(matched)
    }

    pub async fn has_active_withdrawal_below(&self, nonce: i64) -> Result<bool, StorageError> {
        let pending = self.pending_transactions.lock().unwrap();
        // Processing excluded on purpose: those are already dispatched ahead of
        // the rotation, so the sender's in-flight guard covers them.
        Ok(pending.iter().any(|t| {
            t.transaction_type == TransactionType::Withdrawal
                && t.withdrawal_nonce.is_some_and(|n| n < nonce)
                && matches!(
                    t.status,
                    TransactionStatus::Pending
                        | TransactionStatus::Parked
                        | TransactionStatus::PendingRemint
                        | TransactionStatus::ManualReview
                )
        }))
    }

    pub async fn lowest_unreleased_withdrawal_below(
        &self,
        nonce: i64,
    ) -> Result<Option<i64>, StorageError> {
        self.check_should_fail("lowest_unreleased_withdrawal_below")?;
        let pending = self.pending_transactions.lock().unwrap();
        // Processing included, unlike has_active_withdrawal_below: this gates the
        // sender's submit, which must hold after a restart dropped its in-flight map.
        Ok(pending
            .iter()
            .filter(|t| {
                t.transaction_type == TransactionType::Withdrawal
                    && matches!(
                        t.status,
                        TransactionStatus::Pending
                            | TransactionStatus::Processing
                            | TransactionStatus::Parked
                            | TransactionStatus::PendingRemint
                            | TransactionStatus::ManualReview
                    )
            })
            .filter_map(|t| t.withdrawal_nonce)
            .filter(|lower| *lower < nonce)
            .min())
    }

    pub async fn get_committed_checkpoint(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.check_should_fail("get_committed_checkpoint")?;
        Ok(self
            .committed_checkpoints
            .lock()
            .unwrap()
            .get(program_type)
            .copied())
    }

    pub async fn update_committed_checkpoint(
        &self,
        program_type: &str,
        slot: u64,
    ) -> Result<(), StorageError> {
        self.check_should_fail(program_type)?;
        // Mirrors postgres GREATEST(): monotonic, lower writes are ignored.
        // Use `set_checkpoint` to seed arbitrary values in tests.
        let mut map = self.committed_checkpoints.lock().unwrap();
        map.entry(program_type.to_string())
            .and_modify(|existing| {
                if slot > *existing {
                    *existing = slot;
                }
            })
            .or_insert(slot);
        Ok(())
    }

    pub async fn get_owed_rotation_target(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.check_should_fail("get_owed_rotation_target")?;
        Ok(self
            .owed_rotation_targets
            .lock()
            .unwrap()
            .get(program_type)
            .copied())
    }

    pub async fn set_owed_rotation_target(
        &self,
        program_type: &str,
        target_tree_index: u64,
    ) -> Result<(), StorageError> {
        self.check_should_fail("set_owed_rotation_target")?;
        self.owed_rotation_targets
            .lock()
            .unwrap()
            .insert(program_type.to_string(), target_tree_index);
        Ok(())
    }

    pub async fn clear_owed_rotation_target(
        &self,
        program_type: &str,
        target_tree_index: u64,
    ) -> Result<(), StorageError> {
        self.check_should_fail("clear_owed_rotation_target")?;
        // Mirrors the postgres WHERE guard: only the proven target is retired.
        let mut map = self.owed_rotation_targets.lock().unwrap();
        if map.get(program_type) == Some(&target_tree_index) {
            map.remove(program_type);
        }
        Ok(())
    }

    pub async fn update_transaction_status(
        &self,
        transaction_id: i64,
        status: TransactionStatus,
        counterpart_signature: Option<String>,
        processed_at: DateTime<Utc>,
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, StorageError> {
        self.check_should_fail("update_transaction_status")?;
        // COALESCE mirror: only overwrite the durable list when one is supplied.
        if let Some(sigs) = release_signatures {
            self.completed_release_signatures
                .lock()
                .unwrap()
                .insert(transaction_id, sigs);
        }
        // Mirror the Postgres status filter (Processing or PendingRemint only).
        let mut pending = self.pending_transactions.lock().unwrap();
        let updated = if let Some(txn) = pending.iter_mut().find(|t| t.id == transaction_id) {
            if matches!(
                txn.status,
                TransactionStatus::Processing | TransactionStatus::PendingRemint
            ) {
                txn.status = status;
                if counterpart_signature.is_some() {
                    txn.counterpart_signature = counterpart_signature.clone();
                }
                txn.processed_at = Some(processed_at);
                txn.updated_at = Utc::now();
                true
            } else {
                false
            }
        } else {
            // Unknown id — record the attempt anyway (tests assert on
            // `status_updates`), but report no row updated.
            false
        };
        self.status_updates.lock().unwrap().push((
            transaction_id,
            status,
            counterpart_signature,
            processed_at,
        ));
        Ok(updated)
    }

    pub async fn upsert_mints_batch(&self, mints: &[DbMint]) -> Result<(), StorageError> {
        self.check_should_fail("upsert_mints_batch")?;
        let mut store = self.mints.lock().unwrap();
        for mint in mints {
            // Must mirror the Postgres `ON CONFLICT DO UPDATE SET decimals,
            // token_program` semantics: the indexer upserts a `DbMint::new`
            // (flags = None) every time it sees AllowMint, but the operator
            // lazily fills `is_pausable` / `has_permanent_delegate` via
            // `set_mint_extension_flags`. A re-upsert (reorg, indexer
            // restart, retry) must preserve those flags, otherwise the next
            // withdrawal wastes an RPC round-trip re-resolving them. A
            // blanket `insert` here would silently disagree with prod and
            // let tests lock in the wrong behavior. `status` is NOT touched on
            // conflict — `sync_mint_status` is the sole writer of the mirror.
            match store.get_mut(&mint.mint_address) {
                Some(existing) => {
                    existing.decimals = mint.decimals;
                    existing.token_program = mint.token_program.clone();
                }
                None => {
                    store.insert(mint.mint_address.clone(), mint.clone());
                }
            }
        }
        Ok(())
    }

    /// Mirrors `sync_mint_status_internal`: set each mint's `status` to its
    /// latest `mint_status_history` transition; a missing row is a no-op.
    pub async fn sync_mint_status(&self, mint_addresses: &[String]) -> Result<(), StorageError> {
        self.check_should_fail("sync_mint_status")?;
        // Resolve the latest status per address without holding both locks.
        let latest: std::collections::HashMap<String, String> = {
            let history = self.mint_status_history.lock().unwrap();
            mint_addresses
                .iter()
                .filter_map(|addr| {
                    history
                        .iter()
                        .filter(|r| &r.mint_address == addr)
                        .max_by_key(|r| r.effective_slot)
                        .map(|r| (addr.clone(), r.status.clone()))
                })
                .collect()
        };
        let mut store = self.mints.lock().unwrap();
        for (addr, status) in latest {
            if let Some(existing) = store.get_mut(&addr) {
                existing.status = status;
            }
        }
        Ok(())
    }

    pub async fn get_mint(&self, mint_address: &str) -> Result<Option<DbMint>, StorageError> {
        // Inert unless a test opts in via set_fail_times/set_should_fail; lets
        // the read-retry backoff around get_mint be unit-tested.
        self.check_should_fail("get_mint")?;
        Ok(self.mints.lock().unwrap().get(mint_address).cloned())
    }

    pub async fn insert_mint_statuses_batch(
        &self,
        statuses: &[DbMintStatus],
    ) -> Result<(), StorageError> {
        self.check_should_fail("insert_mint_statuses_batch")?;
        let mut store = self.mint_status_history.lock().unwrap();
        for s in statuses {
            let exists = store
                .iter()
                .any(|r| r.mint_address == s.mint_address && r.effective_slot == s.effective_slot);
            if !exists {
                store.push(s.clone());
            }
        }
        Ok(())
    }

    pub async fn get_mint_status_at_slot(
        &self,
        mint_address: &str,
        slot: i64,
    ) -> Result<MintStatusAtSlot, StorageError> {
        let store = self.mint_status_history.lock().unwrap();
        let latest = store
            .iter()
            .filter(|r| r.mint_address == mint_address && r.effective_slot <= slot)
            .max_by_key(|r| r.effective_slot);
        match latest {
            Some(r) if r.status == "allowed" => Ok(MintStatusAtSlot::Allowed),
            Some(r) if r.status == "blocked" => Ok(MintStatusAtSlot::Blocked),
            // Mirror the postgres path: an unrecognized status fails closed to Blocked.
            Some(_) => Ok(MintStatusAtSlot::Blocked),
            None => Ok(MintStatusAtSlot::NeverAllowed),
        }
    }

    pub async fn set_mint_extension_flags(
        &self,
        mint_address: &str,
        is_pausable: bool,
        has_permanent_delegate: bool,
    ) -> Result<(), StorageError> {
        self.check_should_fail("set_mint_extension_flags")?;
        let mut mints = self.mints.lock().unwrap();
        match mints.get_mut(mint_address) {
            Some(mint) => {
                mint.is_pausable = Some(is_pausable);
                mint.has_permanent_delegate = Some(has_permanent_delegate);
                Ok(())
            }
            None => Err(StorageError::DatabaseError {
                message: format!("set_mint_extension_flags: no mints row for {mint_address}"),
            }),
        }
    }

    pub fn set_mint_balances(&self, balances: Vec<MintDbBalance>) {
        *self.mint_balances.lock().unwrap() = balances;
    }

    pub async fn get_mint_balances_for_reconciliation(
        &self,
        as_of_slot: u64,
    ) -> Result<Vec<MintDbBalance>, StorageError> {
        *self.last_reconciliation_slot.lock().unwrap() = Some(as_of_slot);
        Ok(self.mint_balances.lock().unwrap().clone())
    }

    /// Slot the last reconciliation balance read was bounded by.
    pub fn last_reconciliation_slot(&self) -> Option<u64> {
        *self.last_reconciliation_slot.lock().unwrap()
    }

    /// Reads the mints map, mirroring the Postgres query's `mints` table source.
    pub async fn get_mint_addresses(&self) -> Result<Vec<String>, StorageError> {
        self.check_should_fail("get_mint_addresses")?;
        Ok(self.mints.lock().unwrap().keys().cloned().collect())
    }

    pub async fn get_in_flight_amounts_by_mint(
        &self,
    ) -> Result<Vec<MintInFlightAmount>, StorageError> {
        self.check_should_fail("get_in_flight_amounts_by_mint")?;
        // Mirror the Postgres query: sum amounts per mint over the unsettled
        // statuses across every transaction store the mock holds, deduped by id.
        let mut seen_ids = std::collections::HashSet::new();
        let mut sums: HashMap<String, BigDecimal> = HashMap::new();
        {
            let pending = self.pending_transactions.lock().unwrap();
            let singles = self.inserted_single_transactions.lock().unwrap();
            let batches = self.inserted_transactions.lock().unwrap();
            for t in pending
                .iter()
                .chain(singles.iter())
                .chain(batches.iter().flatten())
            {
                if !seen_ids.insert(t.id) {
                    continue;
                }
                if matches!(
                    t.status,
                    TransactionStatus::Pending
                        | TransactionStatus::Processing
                        | TransactionStatus::Parked
                        | TransactionStatus::PendingRemint
                ) {
                    *sums.entry(t.mint.clone()).or_default() += BigDecimal::from(t.amount.value());
                }
            }
        }
        Ok(sums
            .into_iter()
            .map(|(mint_address, in_flight_amount)| MintInFlightAmount {
                mint_address,
                in_flight_amount,
            })
            .collect())
    }

    pub async fn set_reconciliation_halt(&self, reason: &str) -> Result<(), StorageError> {
        self.check_should_fail("set_reconciliation_halt")?;
        *self.reconciliation_halt.lock().unwrap() = Some(HaltInfo {
            reason: reason.to_string(),
            halted_at: Utc::now(),
        });
        Ok(())
    }

    pub async fn is_reconciliation_halted(&self) -> Result<Option<HaltInfo>, StorageError> {
        self.check_should_fail("is_reconciliation_halted")?;
        Ok(self.reconciliation_halt.lock().unwrap().clone())
    }

    pub async fn clear_reconciliation_halt(&self) -> Result<(), StorageError> {
        self.check_should_fail("clear_reconciliation_halt")?;
        *self.reconciliation_halt.lock().unwrap() = None;
        Ok(())
    }

    pub async fn get_orphan_deposit_ids(&self) -> Result<Vec<i64>, StorageError> {
        self.check_should_fail("get_orphan_deposit_ids")?;
        // Mirror Postgres, which scans the whole `transactions` table regardless
        // of status: union every transaction store the mock holds, deduped by id.
        let deposits: Vec<DbTransaction> = {
            let pending = self.pending_transactions.lock().unwrap();
            let singles = self.inserted_single_transactions.lock().unwrap();
            let batches = self.inserted_transactions.lock().unwrap();
            let mut seen_ids = std::collections::HashSet::new();
            pending
                .iter()
                .chain(singles.iter())
                .chain(batches.iter().flatten())
                .filter(|t| t.transaction_type == TransactionType::Deposit)
                .filter(|t| seen_ids.insert(t.id))
                .cloned()
                .collect()
        };
        let mut ids = Vec::new();
        for t in deposits {
            let status = self.get_mint_status_at_slot(&t.mint, t.slot).await?;
            if !matches!(status, MintStatusAtSlot::Allowed) {
                ids.push(t.id);
            }
        }
        ids.sort();
        Ok(ids)
    }

    pub async fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }

    pub async fn count_pending_transactions(
        &self,
        transaction_type: TransactionType,
    ) -> Result<i64, StorageError> {
        let count = self
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .filter(|t| {
                t.transaction_type == transaction_type && t.status == TransactionStatus::Pending
            })
            .count();
        Ok(count as i64)
    }

    pub fn get_completed_withdrawal_nonces(
        &self,
        min_nonce: u64,
        max_nonce: u64,
    ) -> Result<Vec<u64>, StorageError> {
        let nonces: Vec<u64> = self
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .filter(|t| {
                t.transaction_type == TransactionType::Withdrawal
                    && t.status == TransactionStatus::Completed
                    && t.withdrawal_nonce.is_some()
            })
            .filter_map(|t| t.withdrawal_nonce.map(|n| n as u64))
            .filter(|n| n >= &min_nonce && n < &max_nonce)
            .collect();
        Ok(nonces)
    }

    /// Mirror `set_pending_remint_internal`: transition a Processing row to
    /// PendingRemint and store the finality-check payload. Replaying an
    /// identical payload on an already-PendingRemint row succeeds; any other
    /// status, a different payload, or a missing row is a guard miss, matching
    /// the Postgres semantics. Honors `should_fail("set_pending_remint")`.
    pub async fn set_pending_remint(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.check_should_fail("set_pending_remint")?;

        {
            let mut rows = self.pending_transactions.lock().unwrap();
            let row = rows
                .iter_mut()
                .find(|t| t.id == transaction_id)
                .ok_or_else(|| StorageError::DatabaseError {
                    message: format!("no row for id {transaction_id}"),
                })?;
            let replay = row.status == TransactionStatus::PendingRemint
                && row.remint_signatures.as_ref() == Some(&remint_signatures);
            if row.status != TransactionStatus::Processing && !replay {
                return Err(StorageError::DatabaseError {
                    message: format!(
                        "id {transaction_id} is {:?}, not a PendingRemint transition",
                        row.status
                    ),
                });
            }
            row.status = TransactionStatus::PendingRemint;
            row.remint_signatures = Some(remint_signatures.clone());
            row.remint_last_valid_block_heights = Some(remint_last_valid_block_heights.clone());
            row.pending_remint_deadline_at = Some(deadline_at);
            row.updated_at = Utc::now();

            // Keep the rehydration list in step, so `get_pending_remint_transactions`
            // sees the row exactly as a restart would.
            let transitioned = row.clone();
            let mut rehydrate = self.pending_remint_transactions.lock().unwrap();
            match rehydrate.iter_mut().find(|t| t.id == transaction_id) {
                Some(existing) => *existing = transitioned,
                None => rehydrate.push(transitioned),
            }
        }

        self.pending_remint_signatures.lock().unwrap().push((
            transaction_id,
            remint_signatures,
            remint_last_valid_block_heights,
            deadline_at,
        ));
        Ok(())
    }

    /// Status of one row. `pending_transactions` is the live mirror of the
    /// table, so it wins; `pending_remint_transactions` is the fallback for
    /// tests that only seeded the rehydration list.
    pub async fn get_transaction_status(
        &self,
        transaction_id: i64,
    ) -> Result<Option<TransactionStatus>, StorageError> {
        self.check_should_fail("get_transaction_status")?;
        if let Some(txn) = self
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == transaction_id)
        {
            return Ok(Some(txn.status));
        }
        Ok(self
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .find(|t| t.id == transaction_id)
            .map(|t| t.status))
    }

    /// Update the in-memory pending_remint row for `transaction_id` with the
    /// new attempt counter and deadline. Returns `RowNotFound` if no row
    /// exists, matching the Postgres semantics so a test can observe a
    /// missing-row failure. Honors `should_fail("bump_pending_remint_finality_attempt")`.
    pub async fn bump_pending_remint_finality_attempt(
        &self,
        transaction_id: i64,
        attempts: i32,
        new_deadline: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.check_should_fail("bump_pending_remint_finality_attempt")?;
        let mut rows = self.pending_remint_transactions.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|t| t.id == transaction_id)
            .ok_or_else(|| StorageError::DatabaseError {
                message: format!("no PendingRemint row for id {transaction_id}"),
            })?;
        row.finality_check_attempts = attempts;
        row.pending_remint_deadline_at = Some(new_deadline);
        Ok(())
    }

    /// Mirror `record_remint_result_internal`: flip a PendingRemint row to
    /// FailedReminted and store the signature. The status guard means a row
    /// that already moved on is RowNotFound, matching Postgres semantics.
    /// Honors `should_fail("record_remint_result")`.
    pub async fn record_remint_result(
        &self,
        transaction_id: i64,
        remint_signature: String,
    ) -> Result<(), StorageError> {
        self.check_should_fail("record_remint_result")?;
        let mut rows = self.pending_remint_transactions.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|t| t.id == transaction_id && t.status == TransactionStatus::PendingRemint)
            .ok_or_else(|| StorageError::DatabaseError {
                message: format!("no PendingRemint row for id {transaction_id}"),
            })?;
        row.status = TransactionStatus::FailedReminted;
        row.landed_remint_signature = Some(remint_signature);
        row.processed_at = Some(Utc::now());
        Ok(())
    }

    pub async fn get_pending_remint_transactions(
        &self,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        // Match the Postgres query's status filter: a row that already moved to
        // FailedReminted (via record_remint_result) is not re-hydrated, so a
        // landed remint cannot be replayed on restart.
        let pending = self.pending_remint_transactions.lock().unwrap();
        Ok(pending
            .iter()
            .filter(|t| t.status == TransactionStatus::PendingRemint)
            .cloned()
            .collect())
    }

    pub async fn quarantine_all_active_withdrawals(
        &self,
        exclude_id: Option<i64>,
    ) -> Result<u64, StorageError> {
        self.check_should_fail("quarantine_all_active_withdrawals")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        let mut affected = 0u64;
        for txn in pending.iter_mut() {
            let quarantinable = matches!(
                txn.status,
                TransactionStatus::Pending | TransactionStatus::Processing
            );
            let excluded = exclude_id.is_some_and(|id| txn.id == id);
            if txn.transaction_type == TransactionType::Withdrawal && quarantinable && !excluded {
                txn.status = TransactionStatus::ManualReview;
                affected += 1;
            }
        }
        Ok(affected)
    }

    pub async fn get_stale_processing_transactions(
        &self,
        transaction_type: TransactionType,
        threshold: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        self.check_should_fail("get_stale_processing_transactions")?;
        let threshold_chrono = chrono::Duration::from_std(threshold)
            // Defensive: an overflowing Duration falls back to a 1-day cutoff.
            .unwrap_or_else(|_| chrono::Duration::days(1));
        let cutoff = Utc::now() - threshold_chrono;
        let pending = self.pending_transactions.lock().unwrap();
        // Mirrors the Postgres type filter: recovery only sees its own row type.
        let mut matched: Vec<DbTransaction> = pending
            .iter()
            .filter(|t| {
                t.transaction_type == transaction_type
                    && t.status == TransactionStatus::Processing
                    && t.updated_at < cutoff
            })
            .cloned()
            .collect();
        matched.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        matched.truncate(limit as usize);
        Ok(matched)
    }

    pub async fn try_requeue_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        self.check_should_fail("try_requeue_processing")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id
                && txn.status == TransactionStatus::Processing
                && txn.updated_at == expected_updated_at
            {
                txn.status = TransactionStatus::Pending;
                txn.recovery_requeue_attempts += 1;
                txn.updated_at = Utc::now();
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn try_requeue_prebroadcast(
        &self,
        transaction_id: i64,
        max_attempts: i32,
    ) -> Result<RequeueOutcome, StorageError> {
        self.check_should_fail("try_requeue_prebroadcast")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id && txn.status == TransactionStatus::Processing {
                if txn.recovery_requeue_attempts >= max_attempts {
                    return Ok(RequeueOutcome::AtCap);
                }
                txn.status = TransactionStatus::Pending;
                txn.recovery_requeue_attempts += 1;
                txn.updated_at = Utc::now();
                return Ok(RequeueOutcome::Requeued {
                    attempts: txn.recovery_requeue_attempts,
                });
            }
        }
        Ok(RequeueOutcome::NotProcessing)
    }

    pub async fn try_park_processing(&self, transaction_id: i64) -> Result<bool, StorageError> {
        self.check_should_fail("try_park_processing")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id
                && matches!(
                    txn.status,
                    TransactionStatus::Processing | TransactionStatus::Parked
                )
            {
                txn.status = TransactionStatus::Parked;
                txn.updated_at = Utc::now();
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn try_unpark_to_processing(
        &self,
        transaction_id: i64,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        self.check_should_fail("try_unpark_to_processing")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id && txn.status == TransactionStatus::Parked {
                txn.status = TransactionStatus::Processing;
                txn.updated_at = Utc::now();
                return Ok(Some(txn.updated_at));
            }
        }
        Ok(None)
    }

    pub async fn get_stale_parked_transactions(
        &self,
        transaction_type: TransactionType,
        threshold: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        self.check_should_fail("get_stale_parked_transactions")?;
        let threshold_chrono = chrono::Duration::from_std(threshold)
            // Defensive: an overflowing Duration falls back to a 1-day cutoff.
            .unwrap_or_else(|_| chrono::Duration::days(1));
        let cutoff = Utc::now() - threshold_chrono;
        let pending = self.pending_transactions.lock().unwrap();
        // Mirrors the Postgres type filter: recovery only sees its own row type.
        let mut matched: Vec<DbTransaction> = pending
            .iter()
            .filter(|t| {
                t.transaction_type == transaction_type
                    && t.status == TransactionStatus::Parked
                    && t.updated_at < cutoff
            })
            .cloned()
            .collect();
        matched.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        matched.truncate(limit as usize);
        Ok(matched)
    }

    pub async fn try_requeue_parked(
        &self,
        transaction_id: i64,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        self.check_should_fail("try_requeue_parked")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id
                && txn.status == TransactionStatus::Parked
                && txn.updated_at == expected_updated_at
            {
                txn.status = TransactionStatus::Pending;
                txn.updated_at = Utc::now();
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn try_complete_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: DateTime<Utc>,
        counterpart_signature: Option<String>,
        release_signatures: Option<Vec<String>>,
    ) -> Result<bool, StorageError> {
        self.check_should_fail("try_complete_processing")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id
                && txn.status == TransactionStatus::Processing
                && txn.updated_at == expected_updated_at
            {
                txn.status = TransactionStatus::Completed;
                if counterpart_signature.is_some() {
                    txn.counterpart_signature = counterpart_signature;
                }
                // COALESCE: only overwrite when a list is supplied, never wipe.
                if let Some(sigs) = release_signatures {
                    self.completed_release_signatures
                        .lock()
                        .unwrap()
                        .insert(transaction_id, sigs);
                }
                let now = Utc::now();
                txn.processed_at = Some(now);
                txn.updated_at = now;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn try_quarantine_processing(
        &self,
        transaction_id: i64,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        self.check_should_fail("try_quarantine_processing")?;
        let mut pending = self.pending_transactions.lock().unwrap();
        for txn in pending.iter_mut() {
            if txn.id == transaction_id
                && txn.status == TransactionStatus::Processing
                && txn.updated_at == expected_updated_at
            {
                txn.status = TransactionStatus::ManualReview;
                let now = Utc::now();
                txn.processed_at = Some(now);
                txn.updated_at = now;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn insert_release_signature(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
    ) -> Result<(), StorageError> {
        self.check_should_fail("insert_release_signature")?;
        let mut map = self.release_signatures.lock().unwrap();
        // Mirror Postgres `ON CONFLICT (signature) DO NOTHING`.
        if map_contains_signature(&map, &signature) {
            return Ok(());
        }
        map.entry(transaction_id).or_default().push(StoredSig {
            signature,
            last_valid_block_height,
            blockhash_slot,
        });
        Ok(())
    }

    /// Mirror `claim_and_persist_signature_internal`: CAS the row on
    /// `(id, Processing, updated_at)`; on a hit bump `updated_at`, persist the
    /// signature (mirroring the `ON CONFLICT (signature)` dedup) and return
    /// `Ok(Some(new_updated_at))`; on a miss return `Ok(None)` and persist
    /// nothing.
    pub async fn claim_and_persist_signature(
        &self,
        transaction_id: i64,
        expected_updated_at: DateTime<Utc>,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        self.check_should_fail("claim_and_persist_signature")?;
        // Scope the guard so it is released before the await below.
        let lease = {
            let mut pending = self.pending_transactions.lock().unwrap();
            let owned = pending.iter().any(|t| {
                t.id == transaction_id
                    && t.status == TransactionStatus::Processing
                    && t.updated_at == expected_updated_at
            });
            // CAS miss: Postgres updates no row and never reaches the insert.
            if !owned {
                return Ok(None);
            }
            // A simulated insert failure rolls the whole transaction back in
            // Postgres, so it must abort here with no bump once the row is owned.
            self.check_should_fail("insert_release_signature")?;
            let lease = Utc::now();
            let txn = pending
                .iter_mut()
                .find(|t| {
                    t.id == transaction_id
                        && t.status == TransactionStatus::Processing
                        && t.updated_at == expected_updated_at
                })
                .expect("row present: ownership checked under the same lock");
            txn.updated_at = lease;
            lease
        };

        // Reuse the write-ahead insert (mirrors the ON CONFLICT (signature) dedup).
        self.insert_release_signature(
            transaction_id,
            signature,
            last_valid_block_height,
            blockhash_slot,
        )
        .await?;
        Ok(Some(lease))
    }

    pub async fn get_release_signatures(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, StorageError> {
        self.check_should_fail("get_release_signatures")?;
        Ok(self
            .release_signatures
            .lock()
            .unwrap()
            .get(&transaction_id)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn delete_release_signatures(&self, transaction_id: i64) -> Result<(), StorageError> {
        self.check_should_fail("delete_release_signatures")?;
        self.release_signatures
            .lock()
            .unwrap()
            .remove(&transaction_id);
        Ok(())
    }

    pub async fn gc_stale_release_signatures(&self) -> Result<u64, StorageError> {
        self.check_should_fail("gc_stale_release_signatures")?;
        // Mirror the Postgres predicate: reclaim only sigs whose parent row is
        // terminal (completed, failed, failed_reminted). Every non-terminal row
        // keeps its write-ahead journal for the pre-mint gate to re-verify; a
        // sig with no matching row is retained, matching the SQL subquery.
        let terminal_ids: std::collections::HashSet<i64> = self
            .pending_transactions
            .lock()
            .unwrap()
            .iter()
            .filter(|t| {
                matches!(
                    t.status,
                    TransactionStatus::Completed
                        | TransactionStatus::Failed
                        | TransactionStatus::FailedReminted
                )
            })
            .map(|t| t.id)
            .collect();
        let mut map = self.release_signatures.lock().unwrap();
        let mut removed = 0u64;
        map.retain(|txn_id, sigs| {
            if terminal_ids.contains(txn_id) {
                removed += sigs.len() as u64;
                false
            } else {
                true
            }
        });
        Ok(removed)
    }

    /// Mirror `claim_remint_attempt_internal`: retire the named proven-dead
    /// attempts, then take the one live slot the partial unique index allows.
    /// `Ok(false)` means another sender already owns it, so nothing is written.
    pub async fn claim_remint_attempt(
        &self,
        transaction_id: i64,
        signature: String,
        last_valid_block_height: i64,
        blockhash_slot: Option<i64>,
        superseded_signatures: &[String],
    ) -> Result<bool, StorageError> {
        self.check_should_fail("claim_remint_attempt")?;
        let mut map = self.remint_signatures.lock().unwrap();
        let mut superseded = self.superseded_remint_signatures.lock().unwrap();

        // Compare-and-swap scoped to the observed attempts: a slot another
        // sender took in the meantime carries a signature we never classified,
        // so it can never be retired here.
        for stored in map.get(&transaction_id).into_iter().flatten() {
            if superseded_signatures.contains(&stored.signature) {
                superseded.insert(stored.signature.clone());
            }
        }

        // A lost claim still commits the supersedes above, matching Postgres:
        // `ON CONFLICT DO NOTHING` does not abort the surrounding transaction.
        let live = map
            .get(&transaction_id)
            .into_iter()
            .flatten()
            .any(|stored| !superseded.contains(&stored.signature));
        if live
            || self
                .foreign_remint_claims
                .lock()
                .unwrap()
                .contains(&transaction_id)
        {
            return Ok(false);
        }

        map.entry(transaction_id).or_default().push(StoredSig {
            signature,
            last_valid_block_height,
            blockhash_slot,
        });
        Ok(true)
    }

    pub async fn get_remint_signatures(
        &self,
        transaction_id: i64,
    ) -> Result<Vec<StoredSig>, StorageError> {
        self.check_should_fail("get_remint_signatures")?;
        Ok(self
            .remint_signatures
            .lock()
            .unwrap()
            .get(&transaction_id)
            .cloned()
            .unwrap_or_default())
    }

    pub async fn delete_remint_signatures(&self, transaction_id: i64) -> Result<(), StorageError> {
        self.check_should_fail("delete_remint_signatures")?;
        let removed = self
            .remint_signatures
            .lock()
            .unwrap()
            .remove(&transaction_id);
        // Deleting the rows drops their `superseded` column values with them.
        let mut superseded = self.superseded_remint_signatures.lock().unwrap();
        for stored in removed.into_iter().flatten() {
            superseded.remove(&stored.signature);
        }
        Ok(())
    }

    pub async fn gc_stale_remint_signatures(&self) -> Result<u64, StorageError> {
        self.check_should_fail("gc_stale_remint_signatures")?;
        // Mirror the Postgres predicate: keep sigs whose parent is still
        // `PendingRemint`; an unknown transaction id counts as non-pending.
        let pending_remint_ids: std::collections::HashSet<i64> = self
            .pending_remint_transactions
            .lock()
            .unwrap()
            .iter()
            .filter(|t| t.status == TransactionStatus::PendingRemint)
            .map(|t| t.id)
            .collect();
        let mut map = self.remint_signatures.lock().unwrap();
        let mut removed = 0u64;
        map.retain(|txn_id, sigs| {
            if pending_remint_ids.contains(txn_id) {
                true
            } else {
                removed += sigs.len() as u64;
                false
            }
        });
        Ok(removed)
    }
}

/// True if `signature` is already recorded for any transaction in the map.
fn map_contains_signature(map: &ReleaseSignatureMap, signature: &str) -> bool {
    map.values()
        .any(|sigs| sigs.iter().any(|s| s.signature == signature))
}
