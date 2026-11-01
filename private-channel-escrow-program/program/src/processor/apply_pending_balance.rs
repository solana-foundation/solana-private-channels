extern crate alloc;

use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::ApplyPendingBalanceEvent,
    processor::{
        shared::{
            account_check::{verify_signer, verify_token_2022_program},
            confidential_utils::{apply_pending_balance_cpi, read_pending_credit_counter},
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

/// Processes the ApplyPendingBalance instruction.
///
/// Moves the escrow's pending balance into its available balance. Token-2022
/// stores the new AES balance as given and only records a stale counter, so
/// this refuses one instead: a credit that arrived after the operator read the
/// account would be missing from the AES balance. The operator reads again and
/// retries.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator applying the balance
/// 2. `[]` instance - Instance PDA, owner of the escrow account
/// 3. `[]` operator_pda - Operator PDA to validate operator permissions
/// 4. `[]` mint - Token mint of the escrow account
/// 5. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 6. `[]` token_program - Token-2022
/// 7. `[]` event_authority - Event authority PDA for emitting events
/// 8. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `expected_credit_counter` (u64) - Pending credits the operator saw
/// * `new_decryptable_available_balance` ([u8; 36]) - Available balance after the
///   apply, encrypted under the instance AES key
pub fn process_apply_pending_balance(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
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

    if read_pending_credit_counter(instance_ata_info)? != args.expected_credit_counter {
        return Err(PrivateChannelEscrowProgramError::PendingCounterMismatch.into());
    }

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    drop(instance_data);

    apply_pending_balance_cpi(
        instance_ata_info,
        instance_info,
        args.expected_credit_counter,
        &args.new_decryptable_available_balance,
        &signers,
    )?;

    let event = ApplyPendingBalanceEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        *mint_info.address(),
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct ApplyPendingBalanceArgs {
    expected_credit_counter: u64,
    new_decryptable_available_balance: [u8; 36],
}

fn process_instruction_data(data: &[u8]) -> Result<ApplyPendingBalanceArgs, ProgramError> {
    require_len!(data, 8 + 36);

    let expected_credit_counter = u64::from_le_bytes(
        data[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let mut new_decryptable_available_balance = [0u8; 36];
    new_decryptable_available_balance.copy_from_slice(&data[8..8 + 36]);

    Ok(ApplyPendingBalanceArgs {
        expected_credit_counter,
        new_decryptable_available_balance,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_process_apply_pending_balance_instruction_data() {
        let expected_credit_counter = 12_345u64;
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&expected_credit_counter.to_le_bytes());
        instruction_data.extend_from_slice(&[1u8; 36]);

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.expected_credit_counter, expected_credit_counter);
        assert_eq!(args.new_decryptable_available_balance, [1u8; 36]);
    }

    #[test]
    fn test_process_apply_pending_balance_instruction_data_too_short() {
        let instruction_data = vec![0u8; 8 + 35];

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
