//! Token-2022 transfer-hook program used only by the escrow program's
//! integration tests.
//!
//! Two behaviours, picked by how many accounts Token-2022 passes in:
//! - Benign: logs the account count. Tests assert the log to confirm the
//!   escrow forwarded everything the mint's `ExtraAccountMetaList` declares.
//! - Malicious drain: with two or more extras, moves lamports out of the
//!   first extra into the second. The escrow strips the signer bit before
//!   forwarding, so this CPI is missing a required signature and reverts
//!   the whole deposit or release.
//!
//! Ported from <https://github.com/solana-foundation/dvp>.
#![no_std]

use pinocchio::{
    account::AccountView,
    address::declare_id,
    cpi::{Seed, Signer},
    default_allocator,
    error::ProgramError,
    nostd_panic_handler, program_entrypoint,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};
use pinocchio_log::log;
use pinocchio_system::instructions::{CreateAccount, Transfer};

declare_id!("hookEjHJAu757hfesyLchyGLxH6BeNuEbcztVEFT4K4");

// Unlike the escrow program this pulls in no std dependency, so the
// allocator and panic handler have to be declared explicitly.
program_entrypoint!(process_instruction);
default_allocator!();
nostd_panic_handler!();

/// Lamports the malicious path tries to steal from the victim extra.
const DRAIN_LAMPORTS: u64 = 100_000_000;

/// Execute layout is [source, mint, destination, authority, validation_pda],
/// so anything past index 4 is a declared extra. Two of them arm the drain.
const DRAIN_ACCOUNTS_LEN: usize = 7;

/// Seed prefix of the `ExtraAccountMetaList` account.
const EXTRA_ACCOUNT_METAS_SEED: &[u8] = b"extra-account-metas";

/// Tag selecting [`process_init_extra_account_metas`]. Dispatch cannot key off
/// the interface's Execute discriminator, because serialized
/// `ExtraAccountMetaList` bytes start with that same discriminator as their TLV
/// type. Token-2022's Execute data never starts with this byte.
const INIT_EXTRA_ACCOUNT_METAS_TAG: u8 = 0;

pub fn process_instruction(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    match instruction_data.first() {
        Some(&INIT_EXTRA_ACCOUNT_METAS_TAG) => {
            process_init_extra_account_metas(program_id, accounts, &instruction_data[1..])
        }
        _ => process_execute(accounts),
    }
}

fn process_execute(accounts: &[AccountView]) -> ProgramResult {
    log!("hook accounts: {}", accounts.len());

    if accounts.len() >= DRAIN_ACCOUNTS_LEN {
        Transfer {
            from: &accounts[5],
            to: &accounts[6],
            lamports: DRAIN_LAMPORTS,
        }
        .invoke()?;
    }

    Ok(())
}

/// Creates the validation account holding the `ExtraAccountMetaList` bytes the
/// caller serialized off-chain.
///
/// A real hook program builds those bytes on-chain; this only has to make the
/// account exist. Tests against a real validator need it because the address is
/// a PDA of this program, so nothing else can create it. LiteSVM tests write
/// the account directly and never call this.
///
/// # Account Layout
/// 0. `[signer, writable]` payer
/// 1. `[writable]` validation account, PDA of ["extra-account-metas", mint]
/// 2. `[]` mint
/// 3. `[]` system_program
fn process_init_extra_account_metas(
    program_id: &Address,
    accounts: &[AccountView],
    extra_account_metas: &[u8],
) -> ProgramResult {
    let [payer_info, validation_info, mint_info, _system_program_info] = accounts else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let (expected_validation, bump) = Address::find_program_address(
        &[EXTRA_ACCOUNT_METAS_SEED, mint_info.address().as_ref()],
        program_id,
    );
    if validation_info.address() != &expected_validation {
        return Err(ProgramError::InvalidSeeds);
    }

    let bump_seed = [bump];
    let signer_seeds = [
        Seed::from(EXTRA_ACCOUNT_METAS_SEED),
        Seed::from(mint_info.address().as_ref()),
        Seed::from(&bump_seed),
    ];

    let rent = Rent::get()?;
    CreateAccount {
        from: payer_info,
        to: validation_info,
        lamports: rent.try_minimum_balance(extra_account_metas.len())?.max(1),
        space: extra_account_metas.len() as u64,
        owner: program_id,
    }
    .invoke_signed(&[Signer::from(&signer_seeds)])?;

    validation_info
        .try_borrow_mut()?
        .copy_from_slice(extra_account_metas);

    Ok(())
}
