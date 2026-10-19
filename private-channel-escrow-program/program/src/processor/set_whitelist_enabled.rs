extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::SetWhitelistEnabledEvent,
    processor::{
        shared::{account_check::verify_signer, event_utils::emit_event},
        verify_current_program, verify_mutability,
    },
    require_len,
    state::{discriminator::AccountSerialize, Instance},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the SetWhitelistEnabled instruction.
///
/// The flag is absolute, so passing false turns the whitelist off. Entries are
/// kept either way.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[writable]` instance - Instance PDA to update
/// 3. `[signer]` event_authority - Event authority PDA for emitting events
/// 4. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `enabled` (bool) - Require whitelist entries on Deposit and ReleaseFunds
pub fn process_set_whitelist_enabled(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, event_authority_info, program_info] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_mutability(instance_info, true)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let mut instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    instance.whitelist_enabled = args.enabled;

    let updated_instance_data = instance.to_bytes();

    drop(instance_data);

    let mut data_slice = instance_info.try_borrow_mut()?;
    data_slice[..updated_instance_data.len()].copy_from_slice(&updated_instance_data);

    let event = SetWhitelistEnabledEvent::new(instance.instance_seed, instance.whitelist_enabled);
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct SetWhitelistEnabledArgs {
    enabled: bool,
}

fn process_instruction_data(data: &[u8]) -> Result<SetWhitelistEnabledArgs, ProgramError> {
    require_len!(data, 1);

    // The Borsh client refuses any bool byte but 0 or 1, so we do too.
    let enabled = match data[0] {
        0 => false,
        1 => true,
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    Ok(SetWhitelistEnabledArgs { enabled })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A flag byte outside 0/1 must be rejected to stay in sync with the
    // Borsh-based clients, which only accept those two values.
    #[test]
    fn test_process_set_whitelist_enabled_instruction_data_non_canonical() {
        assert_eq!(
            process_instruction_data(&[2]).err(),
            Some(ProgramError::InvalidInstructionData)
        );
    }
}
