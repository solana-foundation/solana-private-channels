extern crate alloc;

use crate::{
    constants::DEPOSITOR_SEED,
    error::PrivateChannelEscrowProgramError,
    events::AddDepositorEvent,
    processor::{
        shared::{
            account_check::{verify_signer, verify_system_account, verify_system_program},
            event_utils::emit_event,
            pda_utils::create_pda_account,
        },
        verify_current_program,
    },
    require_len,
    state::{discriminator::AccountSerialize, Depositor, Instance},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::Seed,
    error::ProgramError,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};

/// Processes the AddDepositor instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for the account creation
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA to validate admin authority
/// 3. `[]` depositor - Wallet to add to the whitelist
/// 4. `[writable]` depositor_pda - Depositor PDA to be created
/// 5. `[]` system_program - System program for account creation
/// 6. `[signer]` event_authority - Event authority PDA for emitting events
/// 7. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `bump` (u8) - Bump for the depositor PDA
pub fn process_add_depositor(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, depositor_info, depositor_pda_info, system_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_system_account(depositor_pda_info, true)?;
    verify_system_program(system_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    let depositor = Depositor::new(args.bump);
    depositor
        .validate_pda(
            instance_info.address(),
            depositor_info.address(),
            depositor_pda_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidDepositorPda)?;

    let bump_seed = [args.bump];
    let depositor_seeds = [
        Seed::from(DEPOSITOR_SEED),
        Seed::from(instance_info.address().as_ref()),
        Seed::from(depositor_info.address().as_ref()),
        Seed::from(&bump_seed),
    ];

    let rent = Rent::get()?;
    create_pda_account(
        payer_info,
        &rent,
        Depositor::LEN,
        program_id,
        depositor_pda_info,
        depositor_seeds,
        None,
    )?;

    let depositor_data = depositor.to_bytes();
    let mut data_slice = depositor_pda_info.try_borrow_mut()?;
    data_slice[..depositor_data.len()].copy_from_slice(&depositor_data);

    let event = AddDepositorEvent::new(instance.instance_seed, *depositor_info.address());
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct AddDepositorArgs {
    bump: u8,
}

fn process_instruction_data(data: &[u8]) -> Result<AddDepositorArgs, ProgramError> {
    require_len!(data, 1);
    Ok(AddDepositorArgs { bump: data[0] })
}
