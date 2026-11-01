extern crate alloc;

use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::ConfidentialReleaseFundsEvent,
    processor::{
        shared::{
            account_check::{verify_instructions_sysvar, verify_signer, verify_token_2022_program},
            confidential_utils::{
                confidential_transfer_cpi, read_source_amount, split_proof_contexts,
                ConfidentialTransferArgs,
            },
            event_utils::emit_event,
            token_utils::validate_ata,
        },
        verify_account_owner, verify_current_program, verify_mutability,
    },
    require_len,
    state::{AllowedMint, AllowedWallet, Direction, Instance, Operator, WithdrawalBitmap},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::{Seed, Signer},
    error::ProgramError,
    Address, ProgramResult,
};
use spl_token_2022::extension::ExtensionType;

// user (32) + transaction_nonce (8) + withdrawer (32), then the transfer fields
const FIXED_DATA_LEN: usize = 32 + 8 + 32;

/// Fixed account prefix; proof contexts and then hook extras follow.
const FIXED_ACCOUNTS_LEN: usize = 16;

/// Processes the ConfidentialReleaseFunds instruction.
///
/// ReleaseFunds for a hidden amount: the escrow's available balance pays the
/// user's pending balance with a Token-2022 confidential transfer. The event
/// carries the amount encrypted under the instance key, taken from the proof
/// Token-2022 accepted.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` operator - Operator releasing the funds
/// 2. `[]` instance - Instance PDA to validate and sign the transfer
/// 3. `[writable]` withdrawal_bitmap - Withdrawal bitmap PDA to consume the nonce
/// 4. `[]` operator_pda - Operator PDA to validate operator permissions
/// 5. `[]` mint - Token mint being released
/// 6. `[]` allowed_mint - AllowedMint PDA to validate mint is allowed
/// 7. `[writable]` user_ata - User's Associated Token Account for this mint
/// 8. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 9. `[]` token_program - Token-2022
/// 10. `[]` instructions_sysvar - Instructions sysvar, read when a proof is an instruction
/// 11. `[]` event_authority - Event authority PDA for emitting events
/// 12. `[]` private_channel_escrow_program - Current program for CPI
/// 13. `[]` memo_program - SPL Memo program, used only when user_ata requires a memo
/// 14. `[]` user_allowed_wallet_pda - AllowedWallet PDA of `user`, read only while
///     withdrawals are whitelisted
/// 15. `[]` withdrawer_allowed_wallet_pda - AllowedWallet PDA of `withdrawer`, read only
///     while withdrawals are whitelisted
///
/// Trailing accounts: one proof context per zero proof offset, in Token-2022's
/// proof order, then the mint's transfer-hook extras.
///
/// # Instruction Data
/// * `user` (Pubkey) - User receiving the funds
/// * `transaction_nonce` (u64) - Transaction nonce to consume from the bitmap
/// * `withdrawer` (Pubkey) - Wallet whose channel burn this release settles
/// * Confidential transfer fields - see `ConfidentialTransferArgs`
pub fn process_confidential_release_funds(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    if accounts.len() < FIXED_ACCOUNTS_LEN {
        return Err(ProgramError::NotEnoughAccountKeys);
    }
    let (fixed_accounts, remaining_accounts) = accounts.split_at(FIXED_ACCOUNTS_LEN);
    let [payer_info, operator_info, instance_info, withdrawal_bitmap_info, operator_pda_info, mint_info, allowed_mint_info, user_ata_info, instance_ata_info, token_program_info, instructions_sysvar_info, event_authority_info, program_info, memo_program_info, user_allowed_wallet_pda_info, withdrawer_allowed_wallet_pda_info] =
        fixed_accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(operator_info, false)?;
    verify_mutability(withdrawal_bitmap_info, true)?;
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

    let allowed_mint_data = allowed_mint_info.try_borrow()?;
    let allowed_mint = AllowedMint::try_from_bytes(&allowed_mint_data)?;

    allowed_mint
        .validate_pda(
            instance_info.address(),
            mint_info.address(),
            allowed_mint_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedMint)?;

    if allowed_mint.withdrawals_blocked {
        return Err(PrivateChannelEscrowProgramError::WithdrawalsBlockedForMint.into());
    }

    if instance.withdrawals_whitelisted {
        AllowedWallet::verify_allowed(
            withdrawer_allowed_wallet_pda_info,
            instance_info.address(),
            &args.withdrawer,
            Direction::Withdraw,
        )?;
        AllowedWallet::verify_allowed(
            user_allowed_wallet_pda_info,
            instance_info.address(),
            &args.user,
            Direction::Withdraw,
        )?;
    }

    validate_ata(user_ata_info, &args.user, mint_info, token_program_info)?;
    validate_ata(
        instance_ata_info,
        instance_info.address(),
        mint_info,
        token_program_info,
    )?;

    let mut bitmap_data = withdrawal_bitmap_info.try_borrow_mut()?;
    WithdrawalBitmap::validate(
        &bitmap_data,
        instance_info.address(),
        withdrawal_bitmap_info,
    )?;
    WithdrawalBitmap::validate_generation(&bitmap_data, args.transaction_nonce)?;
    WithdrawalBitmap::consume_nonce(&mut bitmap_data, args.transaction_nonce)?;
    drop(bitmap_data);

    let with_fee = allowed_mint.has_extension(ExtensionType::TransferFeeConfig);
    let proof_offsets = args.transfer.proof_offsets.token_2022_order(with_fee);
    let (proof_contexts, hook_extras) = split_proof_contexts(remaining_accounts, &proof_offsets)?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    drop(instance_data);

    confidential_transfer_cpi(
        instance_ata_info,
        mint_info,
        user_ata_info,
        instructions_sysvar_info,
        proof_contexts,
        instance_info,
        &args.transfer,
        with_fee,
        Some(memo_program_info),
        hook_extras,
        &signers,
    )?;

    let amount = read_source_amount(
        &args.transfer.proof_offsets,
        instructions_sysvar_info,
        proof_contexts,
    )?;

    let event = ConfidentialReleaseFundsEvent::new(
        instance.instance_seed,
        *operator_info.address(),
        args.user,
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

struct ConfidentialReleaseFundsArgs {
    user: Address,
    transaction_nonce: u64,
    withdrawer: Address,
    transfer: ConfidentialTransferArgs,
}

fn process_instruction_data(data: &[u8]) -> Result<ConfidentialReleaseFundsArgs, ProgramError> {
    require_len!(data, FIXED_DATA_LEN + ConfidentialTransferArgs::LEN);

    let mut offset = 0;

    let mut user_bytes = [0u8; 32];
    user_bytes.copy_from_slice(&data[offset..offset + 32]);
    let user = Address::new_from_array(user_bytes);
    offset += 32;

    let transaction_nonce = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

    let mut withdrawer_bytes = [0u8; 32];
    withdrawer_bytes.copy_from_slice(&data[offset..offset + 32]);
    let withdrawer = Address::new_from_array(withdrawer_bytes);
    offset += 32;

    let transfer = ConfidentialTransferArgs::try_from_bytes(&data[offset..])?;

    Ok(ConfidentialReleaseFundsArgs {
        user,
        transaction_nonce,
        withdrawer,
        transfer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    const TRANSFER_ARGS_LEN: usize = ConfidentialTransferArgs::LEN;

    #[test]
    fn test_process_confidential_release_funds_instruction_data() {
        let user = Address::new_from_array([7u8; 32]);
        let transaction_nonce = 42u64;
        let withdrawer = Address::new_from_array([9u8; 32]);
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(user.as_ref());
        instruction_data.extend_from_slice(&transaction_nonce.to_le_bytes());
        instruction_data.extend_from_slice(withdrawer.as_ref());
        instruction_data.extend_from_slice(&[0u8; TRANSFER_ARGS_LEN]);

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.user, user);
        assert_eq!(args.transaction_nonce, transaction_nonce);
        assert_eq!(args.withdrawer, withdrawer);
    }

    #[test]
    fn test_process_confidential_release_funds_instruction_data_too_short() {
        let instruction_data = vec![0u8; 32 + 8 + 32 + TRANSFER_ARGS_LEN - 1];

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
