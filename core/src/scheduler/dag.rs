use {
    super::{
        traits::SchedulerTrait,
        types::{extract_accounts, ConflictFreeBatch, TransactionWithIndex},
    },
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    std::{
        collections::{HashMap, HashSet, VecDeque},
        sync::Arc,
    },
};

#[derive(Debug, Default)]
struct AccountLocks {
    read_locks: HashSet<usize>,
    write_lock: Option<usize>,
}

pub struct DAGScheduler {
    account_locks: HashMap<Pubkey, AccountLocks>,
    transaction_dependencies: HashMap<usize, HashSet<usize>>,
    transaction_dependents: HashMap<usize, HashSet<usize>>,
}

impl Default for DAGScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DAGScheduler {
    pub fn new() -> Self {
        Self {
            account_locks: HashMap::new(),
            transaction_dependencies: HashMap::new(),
            transaction_dependents: HashMap::new(),
        }
    }

    fn build_dependency_graph(&mut self, transactions: &[Arc<SanitizedTransaction>]) {
        self.account_locks.clear();
        self.transaction_dependencies.clear();
        self.transaction_dependents.clear();

        for (tx_index, transaction) in transactions.iter().enumerate() {
            let (read_accounts, write_accounts) = extract_accounts(transaction);
            let mut dependencies = HashSet::new();

            for account in &write_accounts {
                let locks = self.account_locks.entry(*account).or_default();

                for &dependent_tx in &locks.read_locks {
                    dependencies.insert(dependent_tx);
                }

                // Skip our own lock, as the read loop below already does. A key
                // repeated in the writable set would otherwise make the
                // transaction depend on itself, and it would never become ready.
                if let Some(write_tx) = locks.write_lock {
                    if write_tx != tx_index {
                        dependencies.insert(write_tx);
                    }
                }

                locks.write_lock = Some(tx_index);
            }

            for account in &read_accounts {
                let locks = self.account_locks.entry(*account).or_default();

                if let Some(write_tx) = locks.write_lock {
                    if write_tx != tx_index {
                        dependencies.insert(write_tx);
                    }
                }

                locks.read_locks.insert(tx_index);
            }

            for &dep in &dependencies {
                self.transaction_dependents
                    .entry(dep)
                    .or_default()
                    .insert(tx_index);
            }

            self.transaction_dependencies.insert(tx_index, dependencies);
        }
    }
}

impl SchedulerTrait for DAGScheduler {
    fn schedule(&mut self, transactions: Vec<SanitizedTransaction>) -> Vec<ConflictFreeBatch> {
        if transactions.is_empty() {
            return vec![];
        }

        let transactions: Vec<Arc<SanitizedTransaction>> =
            transactions.into_iter().map(Arc::new).collect();

        self.build_dependency_graph(&transactions);

        let mut batches = Vec::new();
        let mut processed = HashSet::new();
        let mut ready_queue = VecDeque::new();

        for tx_index in 0..transactions.len() {
            if self.transaction_dependencies[&tx_index].is_empty() {
                ready_queue.push_back(tx_index);
            }
        }

        while processed.len() < transactions.len() {
            let mut current_batch = ConflictFreeBatch::new();
            let mut batch_read_accounts = HashSet::new();
            let mut batch_write_accounts = HashSet::new();
            let mut next_ready = Vec::new();

            while let Some(tx_index) = ready_queue.pop_front() {
                if processed.contains(&tx_index) {
                    continue;
                }

                let transaction = &transactions[tx_index];
                let (read_accounts, write_accounts) = extract_accounts(transaction);

                // Check for conflicts:
                // 1. Write-Write conflict: Any write account already in batch writes
                // 2. Read-Write conflict: Any write account already in batch reads, or
                //                         any read account already in batch writes
                // 3. Read-Read is OK: Multiple transactions can read the same account

                let mut has_conflict = false;

                // Check write-write conflicts
                for account in &write_accounts {
                    if batch_write_accounts.contains(account) {
                        has_conflict = true;
                        break;
                    }
                }

                // Check read-write conflicts
                if !has_conflict {
                    for account in &write_accounts {
                        if batch_read_accounts.contains(account) {
                            has_conflict = true;
                            break;
                        }
                    }
                }

                if !has_conflict {
                    for account in &read_accounts {
                        if batch_write_accounts.contains(account) {
                            has_conflict = true;
                            break;
                        }
                    }
                }

                if has_conflict {
                    next_ready.push(tx_index);
                } else {
                    // Add accounts to batch tracking
                    for account in &read_accounts {
                        batch_read_accounts.insert(*account);
                    }
                    for account in &write_accounts {
                        batch_write_accounts.insert(*account);
                    }

                    current_batch.add_transaction(TransactionWithIndex {
                        transaction: transaction.clone(),
                        index: tx_index,
                    });
                    processed.insert(tx_index);

                    if let Some(dependents) = self.transaction_dependents.get(&tx_index) {
                        for &dependent in dependents {
                            let deps = &self.transaction_dependencies[&dependent];
                            if deps.iter().all(|d| processed.contains(d))
                                && !processed.contains(&dependent)
                            {
                                ready_queue.push_back(dependent);
                            }
                        }
                    }
                }
            }

            for tx_index in next_ready {
                ready_queue.push_front(tx_index);
            }

            if !current_batch.is_empty() {
                batches.push(current_batch);
            } else if ready_queue.is_empty() && processed.len() < transactions.len() {
                for tx_index in 0..transactions.len() {
                    if !processed.contains(&tx_index) {
                        let deps = &self.transaction_dependencies[&tx_index];
                        if deps.iter().all(|d| processed.contains(d)) {
                            ready_queue.push_back(tx_index);
                        }
                    }
                }

                if ready_queue.is_empty() {
                    break;
                }
            }
        }

        batches
    }
}
