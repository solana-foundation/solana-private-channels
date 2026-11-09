use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::MoveToConfidentialEvent,
    processor::{
        get_mint_decimals,
        shared::{
            account_check::{verify_signer, verify_token_2022_program},
            confidential_utils::{public_to_pending_cpi, set_confidential_credits_cpi},
            event_utils::emit_event,
            token_utils::validate_ata,
        },
        verify_account_owner, verify_current_program,
    },
    require_len,
    state::{Instance, Operator},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::{Seed, Signer},
    error::ProgramError,
    Address, ProgramResult,
};

/// Processes the MoveToConfidential instruction.
///
/// Moves escrow funds from the public balance into the pending balance, for when
/// confidential releases run short. The amount is public, and custody as a whole
/// does not change.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator moving the funds
/// 2. `[]` instance - Instance PDA, owner of the escrow account
/// 3. `[]` operator_pda - Operator PDA to validate operator permissions
/// 4. `[]` mint - Token mint of the escrow account
/// 5. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 6. `[]` token_program - Token-2022
/// 7. `[]` event_authority - Event authority PDA for emitting events
/// 8. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `amount` (u64) - Amount to move
pub fn process_move_to_confidential(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let amount = process_instruction_data(instruction_data)?;

    if amount == 0 {
        return Err(PrivateChannelEscrowProgramError::ZeroAmount.into());
    }

    let [payer_info, operator_info, instance_info, operator_pda_info, mint_info, instance_ata_info, token_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;
    verify_token_2022_program(token_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    verify_account_owner(mint_info, token_program_info.address())?;

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

    validate_ata(
        instance_ata_info,
        instance_info.address(),
        mint_info,
        token_program_info,
    )?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    drop(instance_data);

    set_confidential_credits_cpi(instance_ata_info, instance_info, true, &signers)?;
    public_to_pending_cpi(
        instance_ata_info,
        mint_info,
        instance_info,
        amount,
        get_mint_decimals(mint_info)?,
        &signers,
    )?;
    set_confidential_credits_cpi(instance_ata_info, instance_info, false, &signers)?;

    let event = MoveToConfidentialEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        *mint_info.address(),
        amount,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

fn process_instruction_data(data: &[u8]) -> Result<u64, ProgramError> {
    require_len!(data, 8);

    Ok(u64::from_le_bytes(
        data[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;

    // With no accounts, ZeroAmount proves the guard runs before any account check.
    #[test]
    fn test_process_move_to_confidential_zero_amount_rejected_before_accounts() {
        let instruction_data = 0u64.to_le_bytes();

        let result = process_move_to_confidential(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &[],
            &instruction_data,
        );

        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::ZeroAmount.into())
        );
    }

    #[test]
    fn test_process_move_to_confidential_instruction_data_too_short() {
        let result = process_instruction_data(&[1, 2, 3]);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
