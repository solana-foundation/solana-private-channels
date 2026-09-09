extern crate alloc;

use crate::{
    constants::{INSTANCE_SEED, WITHDRAWAL_BITMAP_SEED},
    events::CreateInstanceEvent,
    processor::{
        shared::{
            account_check::{verify_signer, verify_system_account, verify_system_program},
            event_utils::emit_event,
            pda_utils::create_pda_account,
        },
        validate_pda_account, verify_current_program,
    },
    require_len,
    state::{discriminator::AccountSerialize, Instance, WithdrawalBitmap},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::Seed,
    error::ProgramError,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};

/// Processes the CreateInstance instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for the account creation
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[signer]` instance_seed - Instance seed signer for PDA derivation
/// 3. `[writable]` instance - Instance PDA to be created
/// 4. `[writable]` withdrawal_bitmap - Withdrawal bitmap PDA to be created
/// 5. `[]` system_program - System program for account creation
/// 6. `[signer]` event_authority - Event authority PDA for emitting events
/// 7. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `bump` (u8) - Bump for the instance PDA
/// * `bitmap_bump` (u8) - Bump for the withdrawal bitmap PDA
pub fn process_create_instance(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_seed_info, instance_info, withdrawal_bitmap_info, system_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_signer(instance_seed_info, false)?;
    verify_system_account(instance_info, true)?;
    verify_system_account(withdrawal_bitmap_info, true)?;
    verify_system_program(system_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance = Instance::new(
        args.bump,
        *instance_seed_info.address(),
        *admin_info.address(),
    );
    instance.validate_pda(instance_info)?;

    let bump_seed = [args.bump];
    let instance_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_seed),
    ];

    let rent = Rent::get()?;
    create_pda_account(
        payer_info,
        &rent,
        Instance::LEN,
        program_id,
        instance_info,
        instance_seeds,
        None,
    )?;

    let instance_data = instance.to_bytes();
    let mut data_slice = instance_info.try_borrow_mut()?;
    data_slice[..instance_data.len()].copy_from_slice(&instance_data);
    drop(data_slice);

    validate_pda_account(
        &[WITHDRAWAL_BITMAP_SEED, instance_info.address().as_ref()],
        program_id,
        args.bitmap_bump,
        withdrawal_bitmap_info,
    )?;

    let bitmap_bump_seed = [args.bitmap_bump];
    let bitmap_seeds = [
        Seed::from(WITHDRAWAL_BITMAP_SEED),
        Seed::from(instance_info.address().as_ref()),
        Seed::from(&bitmap_bump_seed),
    ];

    create_pda_account(
        payer_info,
        &rent,
        WithdrawalBitmap::LEN,
        program_id,
        withdrawal_bitmap_info,
        bitmap_seeds,
        None,
    )?;

    WithdrawalBitmap::init(
        &mut withdrawal_bitmap_info.try_borrow_mut()?,
        args.bitmap_bump,
    )?;

    let event = CreateInstanceEvent::new(*instance_seed_info.address(), *admin_info.address());
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct CreateInstanceArgs {
    bump: u8,
    bitmap_bump: u8,
}

fn process_instruction_data(data: &[u8]) -> Result<CreateInstanceArgs, ProgramError> {
    require_len!(data, 2);
    Ok(CreateInstanceArgs {
        bump: data[0],
        bitmap_bump: data[1],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;

    #[test]
    fn test_process_instruction_data_valid() {
        let instruction_data = [1, 2]; // bump, bitmap_bump

        let result = process_instruction_data(&instruction_data);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.bump, 1);
        assert_eq!(args.bitmap_bump, 2);
    }

    #[test]
    fn test_process_instruction_data_insufficient_data() {
        let instruction_data = []; // No data
        let result = process_instruction_data(&instruction_data);
        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    // A payload carrying only the instance bump would leave bitmap_bump to be
    // read out of bounds, so the length check must reject it.
    #[test]
    fn test_process_instruction_data_missing_bitmap_bump() {
        let instruction_data = [1];
        let result = process_instruction_data(&instruction_data);
        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_create_instance_empty_instruction_data() {
        let instruction_data = [];
        let accounts = [];

        let result = process_create_instance(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        // Empty data triggers InvalidInstructionData
        assert_eq!(result.unwrap_err(), ProgramError::InvalidInstructionData);
    }
}
