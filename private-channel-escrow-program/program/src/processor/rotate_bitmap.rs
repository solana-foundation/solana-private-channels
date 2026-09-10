extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::RotateBitmapEvent,
    processor::{
        shared::{account_check::verify_signer, event_utils::emit_event},
        verify_current_program, verify_mutability,
    },
    state::{Instance, Operator, WithdrawalBitmap},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the RotateBitmap instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator rotating the bitmap
/// 2. `[]` instance - Instance PDA the bitmap belongs to
/// 3. `[writable]` withdrawal_bitmap - Withdrawal bitmap PDA to rotate
/// 4. `[]` operator_pda - Operator PDA to validate operator permissions
/// 5. `[]` event_authority - Event authority PDA for emitting events
/// 6. `[]` private_channel_escrow_program - Current program for CPI
pub fn process_rotate_bitmap(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let [payer_info, operator_info, instance_info, withdrawal_bitmap_info, operator_pda_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    let expected_generation = u64::from_le_bytes(
        instruction_data
            .get(..8)
            .ok_or(ProgramError::InvalidInstructionData)?
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;

    verify_mutability(withdrawal_bitmap_info, true)?;

    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

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

    let mut bitmap_data = withdrawal_bitmap_info.try_borrow_mut()?;
    WithdrawalBitmap::validate(
        &bitmap_data,
        instance_info.address(),
        withdrawal_bitmap_info,
    )?;
    WithdrawalBitmap::rotate(&mut bitmap_data, expected_generation)?;
    let new_generation = WithdrawalBitmap::generation(&bitmap_data)?;
    drop(bitmap_data);

    let event = RotateBitmapEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        new_generation,
    );
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
    fn test_process_rotate_bitmap_empty_accounts() {
        let instruction_data = vec![];
        let accounts = [];

        let result = process_rotate_bitmap(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }
}
