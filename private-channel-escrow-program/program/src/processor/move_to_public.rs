extern crate alloc;

use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::MoveToPublicEvent,
    processor::{
        get_mint_decimals,
        shared::{
            account_check::{verify_instructions_sysvar, verify_signer, verify_token_2022_program},
            confidential_utils::{available_to_public_cpi, split_proof_contexts},
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

/// Fixed account prefix; proof contexts follow.
const FIXED_ACCOUNTS_LEN: usize = 10;

/// Processes the MoveToPublic instruction.
///
/// Moves escrow funds from the available balance back into the public balance,
/// for when public releases run short. The amount is public, and custody as a
/// whole does not change.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator moving the funds
/// 2. `[]` instance - Instance PDA, owner of the escrow account
/// 3. `[]` operator_pda - Operator PDA to validate operator permissions
/// 4. `[]` mint - Token mint of the escrow account
/// 5. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 6. `[]` token_program - Token-2022
/// 7. `[]` instructions_sysvar - Instructions sysvar, read when a proof is an instruction
/// 8. `[]` event_authority - Event authority PDA for emitting events
/// 9. `[]` private_channel_escrow_program - Current program for CPI
///
/// Trailing accounts: one proof context per zero proof offset, equality first.
///
/// # Instruction Data
/// * `amount` (u64) - Amount to move
/// * `new_decryptable_available_balance` ([u8; 36]) - Available balance after the
///   move, encrypted under the instance AES key
/// * `equality_proof_instruction_offset` (i8) - Equality proof position, 0 for a context account
/// * `range_proof_instruction_offset` (i8) - Range proof position, 0 for a context account
pub fn process_move_to_public(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;

    if args.amount == 0 {
        return Err(PrivateChannelEscrowProgramError::ZeroAmount.into());
    }

    if accounts.len() < FIXED_ACCOUNTS_LEN {
        return Err(ProgramError::NotEnoughAccountKeys);
    }
    let (fixed_accounts, remaining_accounts) = accounts.split_at(FIXED_ACCOUNTS_LEN);
    let [payer_info, operator_info, instance_info, operator_pda_info, mint_info, instance_ata_info, token_program_info, instructions_sysvar_info, event_authority_info, program_info] =
        fixed_accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;
    verify_token_2022_program(token_program_info)?;
    verify_instructions_sysvar(instructions_sysvar_info)?;
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

    let proof_offsets = [
        args.equality_proof_instruction_offset,
        args.range_proof_instruction_offset,
    ];
    let (proof_contexts, _) = split_proof_contexts(remaining_accounts, &proof_offsets)?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    drop(instance_data);

    available_to_public_cpi(
        instance_ata_info,
        mint_info,
        instructions_sysvar_info,
        proof_contexts,
        instance_info,
        args.amount,
        get_mint_decimals(mint_info)?,
        &args.new_decryptable_available_balance,
        proof_offsets,
        &signers,
    )?;

    let event = MoveToPublicEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        *mint_info.address(),
        args.amount,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct MoveToPublicArgs {
    amount: u64,
    new_decryptable_available_balance: [u8; 36],
    equality_proof_instruction_offset: i8,
    range_proof_instruction_offset: i8,
}

fn process_instruction_data(data: &[u8]) -> Result<MoveToPublicArgs, ProgramError> {
    require_len!(data, 8 + 36 + 1 + 1);

    let mut offset = 0;

    let amount = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let mut new_decryptable_available_balance = [0u8; 36];
    new_decryptable_available_balance.copy_from_slice(&data[offset..offset + 36]);
    offset += 36;

    Ok(MoveToPublicArgs {
        amount,
        new_decryptable_available_balance,
        equality_proof_instruction_offset: data[offset] as i8,
        range_proof_instruction_offset: data[offset + 1] as i8,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_process_move_to_public_instruction_data() {
        let amount = 1_234_567u64;
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&amount.to_le_bytes());
        instruction_data.extend_from_slice(&[1u8; 36]);
        instruction_data.push(1);
        instruction_data.push(2);

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.amount, amount);
        assert_eq!(args.new_decryptable_available_balance, [1u8; 36]);
        assert_eq!(args.equality_proof_instruction_offset, 1);
        assert_eq!(args.range_proof_instruction_offset, 2);
    }

    #[test]
    fn test_process_move_to_public_zero_amount_rejected_before_accounts() {
        let instruction_data = vec![0u8; 8 + 36 + 2];

        let result =
            process_move_to_public(&PRIVATE_CHANNEL_ESCROW_PROGRAM_ID, &[], &instruction_data);

        assert_eq!(
            result.err(),
            Some(PrivateChannelEscrowProgramError::ZeroAmount.into())
        );
    }
}
