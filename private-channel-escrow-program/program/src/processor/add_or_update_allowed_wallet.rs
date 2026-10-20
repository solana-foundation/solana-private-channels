extern crate alloc;

use crate::{
    constants::ALLOWED_WALLET_SEED,
    error::PrivateChannelEscrowProgramError,
    events::AddOrUpdateAllowedWalletEvent,
    processor::{
        shared::{
            account_check::{
                verify_mutability, verify_signer, verify_system_account, verify_system_program,
            },
            event_utils::emit_event,
            pda_utils::create_pda_account,
        },
        verify_current_program,
    },
    require_len,
    state::{discriminator::AccountSerialize, AllowedWallet, Instance},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::Seed,
    error::ProgramError,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};

/// Processes the AddOrUpdateAllowedWallet instruction.
///
/// Creates the wallet's entry, or rewrites the flags of an existing one.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for the account creation
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA to validate admin authority
/// 3. `[]` wallet - Wallet to add or update
/// 4. `[writable]` allowed_wallet_pda - AllowedWallet PDA to be created or updated
/// 5. `[]` system_program - System program for account creation
/// 6. `[signer]` event_authority - Event authority PDA for emitting events
/// 7. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `bump` (u8) - Bump for the allowed wallet PDA
/// * `block_deposits` (bool) - Refuse this wallet on Deposit while deposits are whitelisted
/// * `block_withdrawals` (bool) - Refuse this wallet on ReleaseFunds while withdrawals are
///   whitelisted
pub fn process_add_or_update_allowed_wallet(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, wallet_info, allowed_wallet_pda_info, system_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_mutability(allowed_wallet_pda_info, true)?;
    verify_system_program(system_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    let allowed_wallet = AllowedWallet::new(args.bump, args.block_deposits, args.block_withdrawals);
    allowed_wallet
        .validate_pda(
            instance_info.address(),
            wallet_info.address(),
            allowed_wallet_pda_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedWalletPda)?;

    // An existing entry keeps its rent and just has its flags rewritten.
    if allowed_wallet_pda_info.is_data_empty() {
        verify_system_account(allowed_wallet_pda_info, true)?;

        let bump_seed = [args.bump];
        let allowed_wallet_seeds = [
            Seed::from(ALLOWED_WALLET_SEED),
            Seed::from(instance_info.address().as_ref()),
            Seed::from(wallet_info.address().as_ref()),
            Seed::from(&bump_seed),
        ];

        let rent = Rent::get()?;
        create_pda_account(
            payer_info,
            &rent,
            AllowedWallet::LEN,
            program_id,
            allowed_wallet_pda_info,
            allowed_wallet_seeds,
            None,
        )?;
    } else {
        // Rejects an account that does not decode as an entry instead of writing over it.
        let existing_data = allowed_wallet_pda_info.try_borrow()?;
        AllowedWallet::try_from_bytes(&existing_data)?;
    }

    let allowed_wallet_data = allowed_wallet.to_bytes();
    let mut data_slice = allowed_wallet_pda_info.try_borrow_mut()?;
    data_slice[..allowed_wallet_data.len()].copy_from_slice(&allowed_wallet_data);

    let event = AddOrUpdateAllowedWalletEvent::new(
        instance.instance_seed,
        *wallet_info.address(),
        allowed_wallet.deposits_blocked,
        allowed_wallet.withdrawals_blocked,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct AddOrUpdateAllowedWalletArgs {
    bump: u8,
    block_deposits: bool,
    block_withdrawals: bool,
}

fn process_instruction_data(data: &[u8]) -> Result<AddOrUpdateAllowedWalletArgs, ProgramError> {
    require_len!(data, 3);

    let bump = data[0];

    // The Borsh client refuses any bool byte but 0 or 1, so we do too.
    let block_deposits = match data[1] {
        0 => false,
        1 => true,
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    let block_withdrawals = match data[2] {
        0 => false,
        1 => true,
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    // An entry blocked both ways is the same as no entry, which RemoveAllowedWallet gives.
    if block_deposits && block_withdrawals {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(AddOrUpdateAllowedWalletArgs {
        bump,
        block_deposits,
        block_withdrawals,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A flag byte outside 0/1 must be rejected to stay in sync with the
    // Borsh-based clients. The other flag stays 0, so a loose parse cannot
    // fall into the both-blocked rejection and pass anyway.
    #[test]
    fn test_process_add_or_update_allowed_wallet_instruction_data_non_canonical() {
        assert_eq!(
            process_instruction_data(&[255, 2, 0]).err(),
            Some(ProgramError::InvalidInstructionData)
        );
        assert_eq!(
            process_instruction_data(&[255, 0, 2]).err(),
            Some(ProgramError::InvalidInstructionData)
        );
    }
}
