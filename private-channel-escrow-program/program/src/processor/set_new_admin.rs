extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::SetNewAdminEvent,
    processor::{
        shared::{account_check::verify_signer, event_utils::emit_event},
        verify_current_program,
    },
    state::{discriminator::AccountSerialize, Instance},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the SetNewAdmin instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Transaction fee payer
/// 1. `[signer]` current_admin - Current admin of the instance
/// 2. `[writable]` instance - Instance PDA to be updated
/// 3. `[signer]` new_admin - New admin to be set (must sign)
/// 4. `[signer]` event_authority - Event authority PDA for emitting events
/// 5. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * None - No instruction data required
pub fn process_set_new_admin(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [payer_info, current_admin_info, instance_info, new_admin_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(current_admin_info, false)?;
    verify_signer(new_admin_info, false)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let mut instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(current_admin_info.address())?;

    let old_admin = instance.admin;
    instance.admin = *new_admin_info.address();

    let updated_instance_data = instance.to_bytes();

    drop(instance_data);

    // A 0.1.0 instance is the current layout cut short, so it keeps its length and only
    // the bytes it has are written. SetWhitelistEnabled is what grows it.
    let mut data_slice = instance_info.try_borrow_mut()?;
    let written = data_slice.len().min(updated_instance_data.len());
    data_slice[..written].copy_from_slice(&updated_instance_data[..written]);

    let event = SetNewAdminEvent::new(instance.instance_seed, old_admin, instance.admin);
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}
