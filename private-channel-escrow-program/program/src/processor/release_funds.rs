extern crate alloc;

use crate::{
    constants::tree_constants::TREE_HEIGHT,
    error::PrivateChannelEscrowProgramError,
    events::ReleaseFundsEvent,
    processor::{
        get_mint_decimals, get_token_account_balance,
        shared::{
            account_check::verify_signer,
            event_utils::emit_event,
            token_utils::{validate_ata, validate_token2022_extensions},
        },
        verify_account_owner, verify_ata_program, verify_current_program, verify_mutability,
        verify_token_programs, SparseMerkleTreeUtils,
    },
    require_len,
    state::{discriminator::AccountSerialize, AllowedMint, Instance, Operator},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::{Seed, Signer},
    error::ProgramError,
    Address, ProgramResult,
};
use pinocchio_token_2022::{
    instructions::TransferChecked as TransferChecked2022, ID as TOKEN_2022_PROGRAM_ID,
};

// amount (8) + user (32) + new_root (32) + transaction_nonce (8) + sibling_proofs (TREE_HEIGHT * 32)
const INSTRUCTION_DATA_LENGTH: usize = 8 + 32 + 32 + 8 + (TREE_HEIGHT * 32);

/// Processes the ReleaseFunds instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator releasing the funds
/// 2. `[writable]` instance - Instance PDA to validate and update
/// 3. `[]` operator_pda - Operator PDA to validate operator permissions
/// 4. `[]` mint - Token mint being released
/// 5. `[]` allowed_mint - AllowedMint PDA to validate mint is allowed
/// 6. `[writable]` user_ata - User's Associated Token Account for this mint
/// 7. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 8. `[]` token_program - Token program for the mint
/// 9. `[]` associated_token_program - Associated Token program
/// 10. `[]` event_authority - Event authority PDA for emitting events
/// 11. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `amount` (u64) - Amount of tokens to release
/// * `user` (Pubkey) - User receiving the funds
/// * `new_withdrawal_root` ([u8; 32]) - New SMT root after adding this transaction nonce
/// * `transaction_nonce` (u64) - Transaction nonce to verify exclusion from current SMT
/// * `sibling_proofs` ([[u8; 32]; 16]) - SMT sibling proofs for exclusion verification
pub fn process_release_funds(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, operator_info, instance_info, operator_pda_info, mint_info, allowed_mint_info, user_ata_info, instance_ata_info, token_program_info, associated_token_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;
    verify_mutability(instance_info, true)?;
    verify_ata_program(associated_token_program_info)?;
    verify_token_programs(token_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    verify_account_owner(mint_info, token_program_info.address())?;

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

    let allowed_mint_data = allowed_mint_info.try_borrow()?;
    let allowed_mint = AllowedMint::try_from_bytes(&allowed_mint_data)?;

    allowed_mint
        .validate_pda(
            instance_info.address(),
            mint_info.address(),
            allowed_mint_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedMint)?;

    validate_ata(user_ata_info, &args.user, mint_info, token_program_info)?;
    validate_ata(
        instance_ata_info,
        instance_info.address(),
        mint_info,
        token_program_info,
    )?;

    if token_program_info.address() == &TOKEN_2022_PROGRAM_ID {
        validate_token2022_extensions(mint_info)?;
    }

    instance.validate_current_tree_index(args.transaction_nonce)?;

    SparseMerkleTreeUtils::verify_smt_exclusion_proof(
        &instance.withdrawal_transactions_root,
        args.transaction_nonce,
        &args.sibling_proofs,
    )?;

    SparseMerkleTreeUtils::verify_smt_inclusion_proof(
        &args.new_withdrawal_root,
        args.transaction_nonce,
        &args.sibling_proofs,
    )?;

    let escrow_token_balance_before = get_token_account_balance(instance_ata_info)?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(b"instance"),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signer = Signer::from(&signer_seeds);

    drop(instance_data);

    TransferChecked2022 {
        from: instance_ata_info,
        to: user_ata_info,
        authority: instance_info,
        amount: args.amount,
        token_program: token_program_info.address(),
        mint: mint_info,
        decimals: get_mint_decimals(mint_info)?,
    }
    .invoke_signed(&[signer])?;

    let escrow_token_balance_after = get_token_account_balance(instance_ata_info)?;
    let released = escrow_token_balance_before
        .checked_sub(escrow_token_balance_after)
        .ok_or(PrivateChannelEscrowProgramError::InvalidEscrowBalance)?;

    instance.withdrawal_transactions_root = args.new_withdrawal_root;
    let updated_instance_data = instance.to_bytes();
    instance_info
        .try_borrow_mut()?
        .copy_from_slice(&updated_instance_data);

    let event = ReleaseFundsEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        released,
        args.user,
        *mint_info.address(),
        args.new_withdrawal_root,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct ReleaseFundsArgs {
    amount: u64,
    user: Address,
    new_withdrawal_root: [u8; 32],
    transaction_nonce: u64,
    sibling_proofs: [[u8; 32]; TREE_HEIGHT],
}

fn process_instruction_data(data: &[u8]) -> Result<ReleaseFundsArgs, ProgramError> {
    require_len!(data, INSTRUCTION_DATA_LENGTH);

    let mut offset = 0;
    let amount = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let mut user_bytes = [0u8; 32];
    user_bytes.copy_from_slice(&data[offset..offset + 32]);
    let user = Address::new_from_array(user_bytes);
    offset += 32;

    let mut new_withdrawal_root = [0u8; 32];
    new_withdrawal_root.copy_from_slice(&data[offset..offset + 32]);
    offset += 32;

    let transaction_nonce = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let mut sibling_proofs = [[0u8; 32]; TREE_HEIGHT];
    for sibling_proof in sibling_proofs.iter_mut() {
        sibling_proof.copy_from_slice(&data[offset..offset + 32]);
        offset += 32;
    }

    Ok(ReleaseFundsArgs {
        new_withdrawal_root,
        transaction_nonce,
        amount,
        user,
        sibling_proofs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_process_release_funds_instruction_data_valid() {
        let user_key = Address::new_from_array([0u8; 32]);
        let new_root = [1u8; 32];
        let transaction_nonce = 42u64;
        let amount = 1000u64;
        let sibling_proofs = [2u8; TREE_HEIGHT * 32];

        let mut instruction_data = vec![];

        // Pack data according to new format
        instruction_data.extend_from_slice(&amount.to_le_bytes());
        instruction_data.extend_from_slice(user_key.as_ref());
        instruction_data.extend_from_slice(&new_root);
        instruction_data.extend_from_slice(&transaction_nonce.to_le_bytes());

        // Add flattened sibling proofs
        instruction_data.extend_from_slice(&sibling_proofs);

        let result = process_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.new_withdrawal_root, new_root);
        assert_eq!(args.transaction_nonce, transaction_nonce);
        assert_eq!(args.amount, amount);
        assert_eq!(args.user, user_key);
        let expected_sibling_proofs = [[2u8; 32]; TREE_HEIGHT];
        assert_eq!(args.sibling_proofs, expected_sibling_proofs);
    }

    #[test]
    fn test_process_release_funds_instruction_data_insufficient_length() {
        let instruction_data = vec![1, 2, 3];

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_release_funds_empty_accounts() {
        let instruction_data = vec![0; INSTRUCTION_DATA_LENGTH];
        let accounts = [];

        let result = process_release_funds(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }
}
