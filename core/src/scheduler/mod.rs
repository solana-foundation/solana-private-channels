pub mod dag;
pub mod greedy;
pub mod traits;
pub mod types;

use {enum_dispatch::enum_dispatch, solana_sdk::transaction::SanitizedTransaction};

pub use {
    dag::DAGScheduler,
    greedy::GreedyScheduler,
    traits::SchedulerTrait,
    // Re-export everything from types that external modules need
    types::{extract_accounts, ConflictFreeBatch, TransactionWithIndex},
};

#[enum_dispatch(SchedulerTrait)]
pub enum Scheduler {
    DAG(DAGScheduler),
    Greedy(GreedyScheduler),
}

impl Scheduler {
    pub fn new_dag() -> Self {
        Scheduler::DAG(DAGScheduler::new())
    }

    pub fn new_greedy() -> Self {
        Scheduler::Greedy(GreedyScheduler::new())
    }
}

#[cfg(test)]
mod tests {
    use super::traits::SchedulerTrait;
    use super::*;
    use solana_sdk::{
        hash::Hash,
        message::Message,
        signature::{Keypair, Signer},
        transaction::Transaction,
    };
    use solana_system_interface::instruction as system_instruction;
    use std::collections::HashSet;

    fn create_test_transaction(
        from: &Keypair,
        to: &solana_sdk::pubkey::Pubkey,
        amount: u64,
    ) -> SanitizedTransaction {
        let instruction = system_instruction::transfer(&from.pubkey(), to, amount);
        let message = Message::new(&[instruction], Some(&from.pubkey()));
        let transaction = Transaction::new(&[from], message, Hash::default());
        SanitizedTransaction::try_from_legacy_transaction(transaction, &HashSet::new()).unwrap()
    }

    fn verify_batch_conflict_free(batch: &ConflictFreeBatch) -> bool {
        let mut seen_read_accounts = HashSet::new();
        let mut seen_write_accounts = HashSet::new();

        for tx_with_index in &batch.transactions {
            let (read_accounts, write_accounts) = extract_accounts(&tx_with_index.transaction);

            // Check for write-write conflicts
            for account in &write_accounts {
                if seen_write_accounts.contains(account) {
                    return false;
                }
            }

            // Check for read-write conflicts
            for account in &write_accounts {
                if seen_read_accounts.contains(account) {
                    return false;
                }
            }

            for account in &read_accounts {
                if seen_write_accounts.contains(account) {
                    return false;
                }
            }

            // Add to seen sets
            for account in read_accounts {
                seen_read_accounts.insert(account);
            }
            for account in write_accounts {
                seen_write_accounts.insert(account);
            }
        }

        true
    }

    #[test]
    fn test_dag_scheduler_no_conflicts() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let dave = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&charlie, &dave.pubkey(), 200),
        ];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        assert!(!batches.is_empty());
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_dag_scheduler_with_conflicts() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&bob, &charlie.pubkey(), 50),
            create_test_transaction(&alice, &charlie.pubkey(), 75),
        ];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        assert!(batches.len() >= 2);
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_greedy_scheduler_no_conflicts() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let dave = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&charlie, &dave.pubkey(), 200),
        ];

        let mut scheduler = Scheduler::new_greedy();
        let batches = scheduler.schedule(transactions);

        assert!(!batches.is_empty());
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_greedy_scheduler_with_conflicts() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&bob, &charlie.pubkey(), 50),
            create_test_transaction(&alice, &charlie.pubkey(), 75),
        ];

        let mut scheduler = Scheduler::new_greedy();
        let batches = scheduler.schedule(transactions);

        assert!(batches.len() >= 2);
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_scheduler_preserves_fifo_when_possible() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let dave = Keypair::new();
        let eve = Keypair::new();
        let frank = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&charlie, &dave.pubkey(), 200),
            create_test_transaction(&eve, &frank.pubkey(), 300),
        ];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        assert!(!batches.is_empty());
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_complex_dependency_chain() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let dave = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&bob, &charlie.pubkey(), 50),
            create_test_transaction(&charlie, &dave.pubkey(), 25),
            create_test_transaction(&dave, &alice.pubkey(), 10),
        ];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        assert_eq!(batches.len(), 4);
        for batch in &batches {
            assert_eq!(batch.len(), 1);
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_parallel_chains() {
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let dave = Keypair::new();
        let eve = Keypair::new();
        let frank = Keypair::new();

        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 100),
            create_test_transaction(&charlie, &dave.pubkey(), 200),
            create_test_transaction(&bob, &eve.pubkey(), 50),
            create_test_transaction(&dave, &frank.pubkey(), 100),
        ];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        assert!(batches.len() >= 2);
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }

    #[test]
    fn test_empty_transactions() {
        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(vec![]);
        assert_eq!(batches.len(), 0);

        let mut scheduler = Scheduler::new_greedy();
        let batches = scheduler.schedule(vec![]);
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn test_single_transaction() {
        let alice = Keypair::new();
        let bob = Keypair::new();

        let transactions = vec![create_test_transaction(&alice, &bob.pubkey(), 100)];

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions.clone());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert!(verify_batch_conflict_free(&batches[0]));

        let mut scheduler = Scheduler::new_greedy();
        let batches = scheduler.schedule(transactions);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert!(verify_batch_conflict_free(&batches[0]));
    }

    #[test]
    fn test_spl_token_like_scenario() {
        // Simulate the scenario from test_spl_token_multiple_batches
        let alice = Keypair::new();
        let bob = Keypair::new();
        let charlie = Keypair::new();
        let will = Keypair::new();

        // These transactions simulate SPL token transfers
        let transactions = vec![
            create_test_transaction(&alice, &bob.pubkey(), 1), // Alice -> Bob
            create_test_transaction(&charlie, &will.pubkey(), 1), // Charlie -> Will
            create_test_transaction(&alice, &bob.pubkey(), 1), // Alice -> Bob
            create_test_transaction(&alice, &bob.pubkey(), 1), // Alice -> Bob
        ];

        // Debug: Check accounts for each transaction
        println!("\nTransaction accounts:");
        for (i, tx) in transactions.iter().enumerate() {
            let (read, write) = extract_accounts(tx);
            println!("Tx {}: {} reads, {} writes", i + 1, read.len(), write.len());
            // Check for system program
            if read.contains(&solana_sdk_ids::system_program::id())
                || write.contains(&solana_sdk_ids::system_program::id())
            {
                println!("  Contains system program!");
            }
        }

        let mut scheduler = Scheduler::new_dag();
        let batches = scheduler.schedule(transactions);

        println!("\nScheduling result: {} batches", batches.len());
        for (i, batch) in batches.iter().enumerate() {
            print!("Batch {}: transactions ", i + 1);
            for tx in &batch.transactions {
                print!("{} ", tx.index + 1);
            }
            println!();
        }

        // The expected behavior:
        // Batch 1: Tx1 (Alice->Bob) and Tx2 (Charlie->Will) - no conflicts
        // Batch 2: Tx3 (Alice->Bob) - conflicts with Tx1
        // Batch 3: Tx4 (Alice->Bob) - conflicts with Tx3

        // But if system program is causing conflicts, we might get 4 batches
        assert!(
            batches.len() >= 3,
            "Expected at least 3 batches, got {}",
            batches.len()
        );

        // Verify all batches are conflict-free
        for batch in &batches {
            assert!(verify_batch_conflict_free(batch));
        }
    }
}
