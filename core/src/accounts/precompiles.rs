//! Shared precompile account map.
//!
//! Built once process-wide from the embedded ELF blobs and served from a
//! `LazyLock`. Both the write-side `BOB` and read-side RPC handlers consume
//! this same map so precompile construction cost is paid exactly once.

use {
    crate::accounts::bpf_loader_program_account,
    solana_sdk::{
        account::{Account, AccountSharedData},
        pubkey::Pubkey,
        rent::Rent,
    },
    std::{collections::HashMap, sync::LazyLock},
};

pub static PRECOMPILES: LazyLock<HashMap<Pubkey, AccountSharedData>> =
    LazyLock::new(build_precompiles);

/// Returns the precompile account for `pubkey`, if any.
///
/// Read-only RPC handlers should prefer this plus a direct `AccountsDB`
/// lookup over constructing a `BOB`.
pub fn get(pubkey: &Pubkey) -> Option<AccountSharedData> {
    PRECOMPILES.get(pubkey).cloned()
}

fn build_precompiles() -> HashMap<Pubkey, AccountSharedData> {
    let mut precompiles = HashMap::new();

    // Zero rent for gasless operation.
    let rent = Rent {
        lamports_per_byte_year: 0,
        exemption_threshold: 0.0,
        burn_percent: 0,
    };

    // System program. lamports=1 (not 0): the SVM's AccountLoader caches
    // loaded accounts across transactions within a batch, and a cached entry
    // with lamports=0 is treated as "previously deallocated" and returned
    // as None on subsequent loads — breaking any CPI into system_program.
    // Same sentinel applies to the rent sysvar below.
    let system_account = Account {
        lamports: 1,
        data: b"solana_system_program".to_vec(),
        owner: solana_sdk_ids::native_loader::ID,
        executable: true,
        rent_epoch: u64::MAX,
    };
    precompiles.insert(
        solana_sdk_ids::system_program::ID,
        AccountSharedData::from(system_account),
    );

    // SPL Token v8.
    let spl_token_elf = include_bytes!("../../precompiles/spl_token-8.0.0.so");
    let (spl_token_id, spl_token_account) =
        bpf_loader_program_account(&spl_token::ID, spl_token_elf, &rent);
    precompiles.insert(spl_token_id, AccountSharedData::from(spl_token_account));

    // Associated Token Account v1.1.1.
    let ata_elf = include_bytes!("../../precompiles/spl_associated_token_account-1.1.1.so");
    let (ata_id, ata_account) =
        bpf_loader_program_account(&spl_associated_token_account::ID, ata_elf, &rent);
    precompiles.insert(ata_id, AccountSharedData::from(ata_account));

    // Rent sysvar. lamports=1 sentinel for the same reason as system_program.
    let rent_account = Account {
        lamports: 1,
        data: bincode::serialize(&rent).unwrap(),
        owner: solana_sdk_ids::sysvar::ID,
        executable: false,
        rent_epoch: u64::MAX,
    };
    precompiles.insert(
        solana_sdk_ids::sysvar::rent::ID,
        AccountSharedData::from(rent_account),
    );

    // SPL Memo v3.
    let memo_v3_elf = include_bytes!("../../precompiles/spl_memo-3.0.0.so");
    let (memo_v3_id, memo_v3_account) =
        bpf_loader_program_account(&spl_memo::id(), memo_v3_elf, &rent);
    precompiles.insert(memo_v3_id, AccountSharedData::from(memo_v3_account));

    // PrivateChannel Withdraw program.
    let withdraw_elf = include_bytes!("../../precompiles/private_channel_withdraw_program.so");
    let (_, withdraw_account) = bpf_loader_program_account(
        &private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        withdraw_elf,
        &rent,
    );
    precompiles.insert(
        private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        AccountSharedData::from(withdraw_account),
    );

    precompiles
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::account::ReadableAccount;

    #[test]
    fn precompiles_contains_expected_entries() {
        assert!(PRECOMPILES.contains_key(&solana_sdk_ids::system_program::ID));
        assert!(PRECOMPILES.contains_key(&spl_token::ID));
        assert!(PRECOMPILES.contains_key(&spl_associated_token_account::ID));
        assert!(PRECOMPILES.contains_key(&solana_sdk_ids::sysvar::rent::ID));
        assert!(PRECOMPILES.contains_key(&spl_memo::id()));
        assert!(PRECOMPILES.contains_key(
            &private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID
        ));
        assert_eq!(PRECOMPILES.len(), 6);
    }

    #[test]
    fn precompile_programs_are_executable_with_elf_data() {
        for program_id in [
            spl_token::ID,
            spl_associated_token_account::ID,
            spl_memo::id(),
            private_channel_withdraw_program_client::PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
        ] {
            let account = PRECOMPILES.get(&program_id).expect("missing precompile");
            assert!(account.executable(), "{} should be executable", program_id);
            assert!(!account.data().is_empty());
        }
    }

    #[test]
    fn system_program_and_rent_use_sentinel_lamports() {
        // lamports=1 sentinel prevents the SVM AccountLoader from treating
        // these as deallocated on re-read within a batch.
        let system = PRECOMPILES
            .get(&solana_sdk_ids::system_program::ID)
            .unwrap();
        assert_eq!(system.lamports(), 1);
        assert!(system.executable());

        let rent = PRECOMPILES.get(&solana_sdk_ids::sysvar::rent::ID).unwrap();
        assert_eq!(rent.lamports(), 1);
        assert!(!rent.executable());
    }

    #[test]
    fn get_returns_none_for_unknown() {
        assert!(get(&Pubkey::new_unique()).is_none());
    }

    #[test]
    fn get_returns_cloned_precompile() {
        let account = get(&spl_token::ID).expect("spl_token missing");
        assert!(account.executable());
    }
}
