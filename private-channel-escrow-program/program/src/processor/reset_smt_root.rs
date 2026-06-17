extern crate alloc;

use crate::{
    constants::tree_constants::EMPTY_TREE_ROOT,
    error::PrivateChannelEscrowProgramError,
    events::ResetSmtRootEvent,
    processor::{
        shared::{account_check::verify_signer, event_utils::emit_event},
        verify_current_program, verify_mutability,
    },
    state::{discriminator::AccountSerialize, Instance, Operator},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the ResetSmtRoot instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator resetting the SMT root
/// 2. `[writable]` instance - Instance PDA to validate and update
/// 3. `[]` operator_pda - Operator PDA to validate operator permissions
/// 4. `[]` event_authority - Event authority PDA for emitting events
/// 5. `[]` private_channel_escrow_program - Current program for CPI
pub fn process_reset_smt_root(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let [payer_info, operator_info, instance_info, operator_pda_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let expected_current_tree_index = u64::from_le_bytes(
        instruction_data
            .get(..8)
            .ok_or(ProgramError::InvalidInstructionData)?
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;

    verify_mutability(instance_info, true)?;

    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let mut instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    let operator_pda_data = operator_pda_info.try_borrow()?;
    let operator_pda = Operator::try_from_bytes(&operator_pda_data)?;

    operator_pda
        .validate_pda(
            instance_info.address(),
            operator_info.address(),
            operator_pda_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidOperatorPda)?;

    drop(instance_data);

    // Reject a stale replay: the reset is not idempotent, so a second landing
    // would advance current_tree_index again and skip a whole tree generation.
    if instance.current_tree_index != expected_current_tree_index {
        return Err(PrivateChannelEscrowProgramError::UnexpectedTreeIndex.into());
    }

    instance.withdrawal_transactions_root = EMPTY_TREE_ROOT;
    instance.current_tree_index = instance
        .current_tree_index
        .checked_add(1)
        .ok_or(ProgramError::ArithmeticOverflow)?;

    let updated_instance_data = instance.to_bytes();
    instance_info
        .try_borrow_mut()?
        .copy_from_slice(&updated_instance_data);

    let event = ResetSmtRootEvent::new(instance.instance_seed, *operator_info.address());
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_process_reset_smt_root_empty_accounts() {
        let instruction_data = vec![];
        let accounts = [];

        let result = process_reset_smt_root(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }
}
