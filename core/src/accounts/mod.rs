pub mod account_matches_owners;
pub mod address_index_repair;
pub mod address_index_watermark;
pub mod bob;
pub mod constants;
pub mod error;
pub mod get_account_shared_data;
pub mod get_accounts;
pub mod get_block;
pub mod get_block_time;
pub mod get_blocks;
pub mod get_blocks_in_range;
pub mod get_epoch_info;
pub mod get_first_available_block;
pub mod get_latest_blockhash;
pub mod get_latest_slot;
pub mod get_recent_performance_samples;
pub mod get_signatures_for_address;
pub mod get_transaction;
pub mod get_transaction_count;
pub mod postgres;
pub mod precompiles;
pub mod redis;
pub mod set_account;
pub mod set_latest_slot;
pub mod store_block;
pub mod store_performance_sample;
pub mod traits;
pub mod transaction_count;
pub mod truncate;
pub mod types;
pub mod utils;
pub mod write_batch;

use {
    solana_loader_v3_interface::{get_program_data_address, state::UpgradeableLoaderState},
    solana_sdk::{account::Account, bpf_loader, pubkey::Pubkey, rent::Rent},
};

/// Precompile definition
pub struct Precompile {
    pub program_id: Pubkey,
    pub loader_id: Pubkey,
    pub elf_bytes: &'static [u8],
    pub name: &'static str,
}

/// Type alias for a list of precompiles
pub type Precompiles = Vec<Precompile>;

/// Type alias for a list of sysvars
pub type Sysvars = Vec<Pubkey>;

/// Creates a BPF Loader program account containing the ELF
pub fn bpf_loader_program_account(
    program_id: &Pubkey,
    elf: &[u8],
    rent: &Rent,
) -> (Pubkey, Account) {
    (
        *program_id,
        Account {
            lamports: rent.minimum_balance(elf.len()).max(1),
            data: elf.to_vec(),
            owner: bpf_loader::id(),
            executable: true,
            rent_epoch: 0,
        },
    )
}

/// Creates program and programdata accounts for upgradeable BPF loader
pub fn bpf_loader_upgradeable_program_accounts(
    program_id: &Pubkey,
    elf: &[u8],
    rent: &Rent,
) -> [(Pubkey, Account); 2] {
    let programdata_address = get_program_data_address(program_id);

    // Create the main program account
    let program_account = {
        let space = UpgradeableLoaderState::size_of_program();
        let lamports = rent.minimum_balance(space);
        let data = bincode::serialize(&UpgradeableLoaderState::Program {
            programdata_address,
        })
        .unwrap();
        Account {
            lamports,
            data,
            owner: solana_sdk_ids::bpf_loader_upgradeable::id(),
            executable: true,
            rent_epoch: 0,
        }
    };

    // Create the programdata account containing the actual ELF
    let programdata_account = {
        let space = UpgradeableLoaderState::size_of_programdata_metadata() + elf.len();
        let lamports = rent.minimum_balance(space);
        let mut data = bincode::serialize(&UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::default()),
        })
        .unwrap();
        data.extend_from_slice(elf);
        Account {
            lamports,
            data,
            owner: solana_sdk_ids::bpf_loader_upgradeable::id(),
            executable: false,
            rent_epoch: 0,
        }
    };

    [
        (*program_id, program_account),
        (programdata_address, programdata_account),
    ]
}

pub use {postgres::PostgresAccountsDB, redis::RedisAccountsDB, traits::AccountsDB};

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::rent::Rent;

    #[test]
    fn bpf_loader_program_account_creates_executable() {
        let program_id = Pubkey::new_unique();
        let elf = vec![0xEF; 128];
        let rent = Rent::default();

        let (key, account) = bpf_loader_program_account(&program_id, &elf, &rent);

        assert_eq!(key, program_id);
        assert!(account.executable);
        assert_eq!(account.owner, bpf_loader::id());
        assert_eq!(account.data, elf);
        assert!(account.lamports >= rent.minimum_balance(elf.len()));
    }

    #[test]
    fn bpf_loader_upgradeable_creates_two_accounts() {
        let program_id = Pubkey::new_unique();
        let elf = vec![0xAB; 256];
        let rent = Rent::default();

        let accounts = bpf_loader_upgradeable_program_accounts(&program_id, &elf, &rent);

        // First account is the main program account
        let (key, program_account) = &accounts[0];
        assert_eq!(key, &program_id);
        assert!(program_account.executable);
        assert_eq!(
            program_account.owner,
            solana_sdk_ids::bpf_loader_upgradeable::id()
        );

        // Second account is the programdata account
        let (programdata_key, programdata_account) = &accounts[1];
        let expected_programdata = get_program_data_address(&program_id);
        assert_eq!(programdata_key, &expected_programdata);
        assert!(!programdata_account.executable);
        assert_eq!(
            programdata_account.owner,
            solana_sdk_ids::bpf_loader_upgradeable::id()
        );
        // Programdata account contains the ELF at the end
        assert!(programdata_account.data.ends_with(&elf));
    }
}
