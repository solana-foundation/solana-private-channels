use crate::error::StorageError;
use crate::storage::common::models::{
    DbMint, DbMintStatus, DbTransaction, MintDbBalance, MintStatusAtSlot, TransactionStatus,
    TransactionType,
};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Recorded status update from `update_transaction_status`.
pub type StatusUpdateRecord = (i64, TransactionStatus, Option<String>, DateTime<Utc>);

/// (transaction_id, signatures, last_valid_block_heights, deadline) persisted on PendingRemint transition.
pub type PendingRemintRecord = (i64, Vec<String>, Vec<i64>, DateTime<Utc>);

#[derive(Clone, Default)]
pub struct MockStorage {
    pub committed_checkpoints: std::sync::Arc<Mutex<HashMap<String, u64>>>,
    pub should_fail: std::sync::Arc<Mutex<HashMap<String, bool>>>,
    pub mints: std::sync::Arc<Mutex<HashMap<String, DbMint>>>,
    pub mint_balances: std::sync::Arc<Mutex<Vec<MintDbBalance>>>,
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
}

impl MockStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn check_should_fail(&self, operation: &str) -> Result<(), StorageError> {
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
        let mut matched = Vec::new();
        let mut remaining = Vec::new();

        for txn in pending.drain(..) {
            if txn.transaction_type == transaction_type && (matched.len() as i64) < limit {
                matched.push(txn);
            } else {
                remaining.push(txn);
            }
        }

        *pending = remaining;
        Ok(matched)
    }

    pub async fn get_committed_checkpoint(
        &self,
        program_type: &str,
    ) -> Result<Option<u64>, StorageError> {
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

    pub async fn update_transaction_status(
        &self,
        transaction_id: i64,
        status: TransactionStatus,
        counterpart_signature: Option<String>,
        processed_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.check_should_fail("update_transaction_status")?;
        self.status_updates.lock().unwrap().push((
            transaction_id,
            status,
            counterpart_signature,
            processed_at,
        ));
        Ok(())
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
            // let tests lock in the wrong behavior.
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

    pub async fn get_mint(&self, mint_address: &str) -> Result<Option<DbMint>, StorageError> {
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
    ) -> Result<Vec<MintDbBalance>, StorageError> {
        Ok(self.mint_balances.lock().unwrap().clone())
    }

    pub async fn get_escrow_balances_by_mint(&self) -> Result<Vec<MintDbBalance>, StorageError> {
        Ok(self.mint_balances.lock().unwrap().clone())
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

    pub async fn set_pending_remint(
        &self,
        transaction_id: i64,
        remint_signatures: Vec<String>,
        remint_last_valid_block_heights: Vec<i64>,
        deadline_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        if self
            .should_fail
            .lock()
            .unwrap()
            .get("set_pending_remint")
            .copied()
            .unwrap_or(false)
        {
            return Err(StorageError::DatabaseError {
                message: "Simulated set_pending_remint failure".to_string(),
            });
        }
        self.pending_remint_signatures.lock().unwrap().push((
            transaction_id,
            remint_signatures,
            remint_last_valid_block_heights,
            deadline_at,
        ));
        Ok(())
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

    pub async fn get_pending_remint_transactions(
        &self,
    ) -> Result<Vec<DbTransaction>, StorageError> {
        let pending = self.pending_remint_transactions.lock().unwrap();
        Ok(pending.clone())
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
}
