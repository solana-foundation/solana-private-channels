use {
    solana_sdk::{pubkey::Pubkey, transaction::SanitizedTransaction},
    std::sync::Arc,
};

#[derive(Debug, Clone)]
pub struct TransactionWithIndex {
    pub transaction: Arc<SanitizedTransaction>,
    pub index: usize,
}

#[derive(Debug)]
pub struct ConflictFreeBatch {
    pub transactions: Vec<TransactionWithIndex>,
}

impl Default for ConflictFreeBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl ConflictFreeBatch {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
        }
    }

    pub fn add_transaction(&mut self, tx: TransactionWithIndex) {
        self.transactions.push(tx);
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }
}

pub fn extract_accounts(transaction: &SanitizedTransaction) -> (Vec<Pubkey>, Vec<Pubkey>) {
    // The unchecked accessor is the same account_keys walk as the checked one,
    // minus a validation step that can fail. Keeping the scheduler total means
    // a malformed transaction can never abort the sequencer task; structural
    // validation happens once at RPC ingress, where a rejection reason can
    // actually be returned to the caller.
    let account_locks = transaction.get_account_locks_unchecked();

    let read_accounts = account_locks.readonly.into_iter().copied().collect();
    let write_accounts = account_locks.writable.into_iter().copied().collect();

    (read_accounts, write_accounts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::{
        create_test_sanitized_transaction, duplicate_account_keys_transaction,
    };
    use solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
    };
    use std::collections::HashSet;

    // A repeated account key is attacker reachable, so lock extraction must stay total.
    #[test]
    fn extract_accounts_handles_duplicate_keys() {
        let payer = Keypair::new();
        let tx = duplicate_account_keys_transaction(&payer, Hash::default());
        let sanitized = SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("a duplicate-key message still sanitizes");

        let (read_accounts, write_accounts) = extract_accounts(&sanitized);

        assert_eq!(write_accounts, vec![payer.pubkey()]);
        assert_eq!(read_accounts, vec![spl_memo::id(), spl_memo::id()]);
    }

    // The write-lock path is the one a repeated key can actually corrupt, so prove
    // extraction reports it and the scheduler still emits the transaction instead
    // of dropping it or spinning on a dependency it can never satisfy.
    #[test]
    fn duplicate_writable_key_still_schedules() {
        use crate::scheduler::{Scheduler, SchedulerTrait};

        let payer = Keypair::new();
        let tx = crate::test_helpers::duplicate_writable_account_keys_transaction(
            &payer,
            Hash::default(),
        );
        let sanitized = SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::new())
            .expect("a writable duplicate-key message still sanitizes");

        let (read_accounts, write_accounts) = extract_accounts(&sanitized);
        assert_eq!(write_accounts, vec![payer.pubkey(), payer.pubkey()]);
        assert_eq!(read_accounts, vec![spl_memo::id()]);

        let batches = Scheduler::new_dag().schedule(vec![sanitized]);
        assert_eq!(batches.len(), 1, "the transaction must still be scheduled");
        assert_eq!(batches[0].len(), 1);
    }

    // Pins the read/write split so the unchecked accessor cannot quietly change what gets locked.
    #[test]
    fn extract_accounts_splits_read_and_write() {
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        let sanitized = create_test_sanitized_transaction(&from, &to, 1_000);

        let (read_accounts, write_accounts) = extract_accounts(&sanitized);

        assert_eq!(write_accounts, vec![from.pubkey(), to]);
        assert_eq!(read_accounts, vec![solana_sdk::system_program::id()]);
    }
}
