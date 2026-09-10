extern crate alloc;

use crate::{
    constants::ALLOWED_MINT_SEED,
    error::PrivateChannelEscrowProgramError,
    events::AllowMintEvent,
    processor::{
        shared::{
            account_check::{
                verify_mutability, verify_signer, verify_system_account, verify_system_program,
            },
            event_utils::emit_event,
            pda_utils::create_pda_account,
            token_utils::{get_mint_decimals, get_or_create_ata},
        },
        validate_mint, verify_account_owner, verify_ata_program, verify_current_program,
        verify_token_programs,
    },
    require_len,
    state::{discriminator::AccountSerialize, AllowedMint, Instance},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::Seed,
    error::ProgramError,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};

/// Processes the AllowMint instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for the account creation
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA to validate admin authority
/// 3. `[]` mint - Token mint to be allowed
/// 4. `[writable]` allowed_mint - AllowedMint PDA to be created
/// 5. `[writable]` instance_ata - Instance PDA's ATA for this mint
/// 6. `[]` system_program - System program for account creation
/// 7. `[]` token_program - Token program for the mint
/// 8. `[signer]` event_authority - Event authority PDA for emitting events
/// 9. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `bump` (u8) - Bump for the allowed mint PDA
pub fn process_allow_mint(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, mint_info, allowed_mint_info, instance_ata_info, system_program_info, token_program_info, associated_token_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_mutability(allowed_mint_info, true)?;
    verify_ata_program(associated_token_program_info)?;
    verify_system_program(system_program_info)?;
    verify_token_programs(token_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    verify_account_owner(mint_info, token_program_info.address())?;
    validate_mint(mint_info)?;

    let mint_decimals = get_mint_decimals(mint_info)?;

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    get_or_create_ata(
        instance_ata_info,
        instance_info,
        mint_info,
        payer_info,
        system_program_info,
        token_program_info,
    )?;

    let allowed_mint = AllowedMint::new(args.bump, mint_decimals, *token_program_info.address());
    allowed_mint
        .validate_pda(
            instance_info.address(),
            mint_info.address(),
            allowed_mint_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedMint)?;

    // BlockMint no longer closes the PDA, so this is also the re-allow path:
    // an existing account keeps its rent and just has both gates re-opened.
    if allowed_mint_info.is_data_empty() {
        verify_system_account(allowed_mint_info, true)?;

        let bump_seed = [args.bump];
        let allowed_mint_seeds = [
            Seed::from(ALLOWED_MINT_SEED),
            Seed::from(instance_info.address().as_ref()),
            Seed::from(mint_info.address().as_ref()),
            Seed::from(&bump_seed),
        ];

        let rent = Rent::get()?;
        create_pda_account(
            payer_info,
            &rent,
            AllowedMint::LEN,
            program_id,
            allowed_mint_info,
            allowed_mint_seeds,
            None,
        )?;
    } else {
        // Rejects an account whose layout predates the gates instead of
        // letting the write below run past the end of it.
        let existing_data = allowed_mint_info.try_borrow()?;
        AllowedMint::try_from_bytes(&existing_data)?;
    }

    let allowed_mint_data = allowed_mint.to_bytes();
    allowed_mint_info
        .try_borrow_mut()?
        .copy_from_slice(&allowed_mint_data);

    let event = AllowMintEvent::new(instance.instance_seed, *mint_info.address(), mint_decimals);
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct AllowMintArgs {
    bump: u8,
}

fn process_instruction_data(data: &[u8]) -> Result<AllowMintArgs, ProgramError> {
    require_len!(data, 1);
    Ok(AllowMintArgs { bump: data[0] })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_process_allow_mint_valid_bump() {
        // Test with valid bump
        let instruction_data = vec![123]; // bump = 123

        let result = process_instruction_data(&instruction_data);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().bump, 123);
    }

    #[test]
    fn test_process_allow_mint_empty_instruction_data() {
        let instruction_data = [];
        let accounts = [];

        let result = process_allow_mint(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.unwrap_err(), ProgramError::InvalidInstructionData);
    }
}
