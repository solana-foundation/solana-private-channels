use crate::accounts::bob::BOB;
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    pubkey::Pubkey,
};
use solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback};
use std::collections::{HashMap, HashSet};

const DEFAULT_FEE_PAYER_LAMPORTS: u64 = 10;

/// A read-only, thread-safe snapshot of account state for parallel SVM execution.
///
/// Unlike [`GaslessCallback`], `SnapshotCallback` owns a plain `HashMap`
/// and is automatically safe to share across worker threads.
///
/// Built from BOB after preload: all referenced accounts are already warm in
/// the cache. `AccountSharedData` clone is cheap (`Arc<Vec<u8>>` ref bump).
pub struct SnapshotCallback {
    accounts: HashMap<Pubkey, AccountSharedData>,
    fee_payers: HashSet<Pubkey>,
}

impl SnapshotCallback {
    /// Build a snapshot from BOB's current in-memory state.
    ///
    /// Iterates `account_keys` and copies each account BOB knows about
    /// (precompiles + cached accounts). Unknown keys are skipped — they'll
    /// return `None` from `get_account_shared_data`, matching BOB's behavior.
    pub fn from_bob(bob: &BOB, account_keys: &[Pubkey], fee_payers: HashSet<Pubkey>) -> Self {
        let mut accounts = HashMap::with_capacity(account_keys.len());
        for pubkey in account_keys {
            if let Some(account) = bob.get_account_shared_data(pubkey) {
                accounts.insert(*pubkey, account);
            }
        }
        SnapshotCallback {
            accounts,
            fee_payers,
        }
    }
}

impl InvokeContextCallback for SnapshotCallback {}

impl TransactionProcessingCallback for SnapshotCallback {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.accounts.get(pubkey).cloned().or_else(|| {
            self.fee_payers.contains(pubkey).then(|| {
                AccountSharedData::new(
                    DEFAULT_FEE_PAYER_LAMPORTS,
                    0,
                    &solana_sdk_ids::system_program::ID,
                )
            })
        })
    }

    fn account_matches_owners(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        owners: &[solana_sdk::pubkey::Pubkey],
    ) -> Option<usize> {
        self.get_account_shared_data(account)
            .and_then(|account| owners.iter().position(|key| account.owner().eq(key)))
    }
}

pub struct GaslessCallback<'a> {
    bob: &'a BOB,
    fee_payers: HashSet<Pubkey>,
}

impl<'a> GaslessCallback<'a> {
    pub fn new(accounts_db: &'a BOB, fee_payers: HashSet<Pubkey>) -> Self {
        Self {
            bob: accounts_db,
            fee_payers,
        }
    }
}

impl<'a> InvokeContextCallback for GaslessCallback<'a> {}

impl<'a> TransactionProcessingCallback for GaslessCallback<'a> {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.bob.get_account_shared_data(pubkey).or_else(|| {
            self.fee_payers.contains(pubkey).then(|| {
                AccountSharedData::new(
                    DEFAULT_FEE_PAYER_LAMPORTS,
                    0,
                    &solana_sdk_ids::system_program::ID,
                )
            })
        })
    }

    fn account_matches_owners(
        &self,
        account: &solana_sdk::pubkey::Pubkey,
        owners: &[solana_sdk::pubkey::Pubkey],
    ) -> Option<usize> {
        self.get_account_shared_data(account)
            .and_then(|account| owners.iter().position(|key| account.owner().eq(key)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::create_test_bob;
    use solana_svm_callback::TransactionProcessingCallback;

    #[tokio::test]
    async fn test_fee_payer_returns_dummy_account() {
        let (bob, _tx) = create_test_bob();
        let fee_payer = Pubkey::new_unique();
        let cb = GaslessCallback::new(&bob, HashSet::from([fee_payer]));

        let account = cb.get_account_shared_data(&fee_payer).unwrap();
        assert_eq!(account.lamports(), DEFAULT_FEE_PAYER_LAMPORTS);
        assert_eq!(account.owner(), &solana_sdk_ids::system_program::ID);
    }

    #[tokio::test]
    async fn test_unknown_pubkey_returns_none() {
        let (bob, _tx) = create_test_bob();
        let cb = GaslessCallback::new(&bob, HashSet::new());

        assert!(cb.get_account_shared_data(&Pubkey::new_unique()).is_none());
    }

    #[tokio::test]
    async fn test_account_matches_owners_fee_payer() {
        let (bob, _tx) = create_test_bob();
        let fee_payer = Pubkey::new_unique();
        let cb = GaslessCallback::new(&bob, HashSet::from([fee_payer]));

        // Fee payer is owned by system program
        let system = solana_sdk_ids::system_program::ID;
        let other = Pubkey::new_unique();

        assert_eq!(
            cb.account_matches_owners(&fee_payer, &[other, system]),
            Some(1)
        );
        assert_eq!(cb.account_matches_owners(&fee_payer, &[other]), None);
    }

    #[tokio::test]
    async fn test_account_matches_owners_unknown() {
        let (bob, _tx) = create_test_bob();
        let cb = GaslessCallback::new(&bob, HashSet::new());

        let unknown = Pubkey::new_unique();
        assert_eq!(
            cb.account_matches_owners(&unknown, &[Pubkey::new_unique()]),
            None
        );
    }

    // ── SnapshotCallback tests ──

    #[tokio::test]
    async fn test_snapshot_fee_payer_returns_dummy_account() {
        let (bob, _tx) = create_test_bob();
        let fee_payer = Pubkey::new_unique();
        let snapshot = SnapshotCallback::from_bob(&bob, &[], HashSet::from([fee_payer]));

        let account = snapshot.get_account_shared_data(&fee_payer).unwrap();
        assert_eq!(account.lamports(), DEFAULT_FEE_PAYER_LAMPORTS);
        assert_eq!(account.owner(), &solana_sdk_ids::system_program::ID);
    }

    #[tokio::test]
    async fn test_snapshot_unknown_pubkey_returns_none() {
        let (bob, _tx) = create_test_bob();
        let snapshot = SnapshotCallback::from_bob(&bob, &[], HashSet::new());

        assert!(snapshot
            .get_account_shared_data(&Pubkey::new_unique())
            .is_none());
    }

    #[tokio::test]
    async fn test_snapshot_captures_bob_accounts() {
        let (mut bob, _tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(42, 0, &owner);

        // Insert an account into BOB's cache directly
        bob.insert_account_for_test(pubkey, account.clone());

        let snapshot = SnapshotCallback::from_bob(&bob, &[pubkey], HashSet::new());
        let retrieved = snapshot.get_account_shared_data(&pubkey).unwrap();
        assert_eq!(retrieved.lamports(), 42);
        assert_eq!(retrieved.owner(), &owner);
    }

    #[tokio::test]
    async fn test_snapshot_skips_unknown_keys() {
        let (bob, _tx) = create_test_bob();
        let unknown = Pubkey::new_unique();
        // Key not in BOB — snapshot should not contain it
        let snapshot = SnapshotCallback::from_bob(&bob, &[unknown], HashSet::new());
        assert!(snapshot.get_account_shared_data(&unknown).is_none());
    }

    #[tokio::test]
    async fn test_snapshot_account_matches_owners() {
        let (mut bob, _tx) = create_test_bob();
        let pubkey = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let account = AccountSharedData::new(1, 0, &owner);
        bob.insert_account_for_test(pubkey, account);

        let snapshot = SnapshotCallback::from_bob(&bob, &[pubkey], HashSet::new());
        let other = Pubkey::new_unique();
        assert_eq!(
            snapshot.account_matches_owners(&pubkey, &[other, owner]),
            Some(1)
        );
        assert_eq!(snapshot.account_matches_owners(&pubkey, &[other]), None);
    }
}
