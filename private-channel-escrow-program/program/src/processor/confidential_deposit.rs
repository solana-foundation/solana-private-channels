extern crate alloc;

use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::ConfidentialDepositEvent,
    processor::{
        shared::{
            account_check::{verify_instructions_sysvar, verify_signer, verify_token_2022_program},
            confidential_utils::{
                confidential_transfer_cpi, read_pending_balance, set_confidential_credits_cpi,
                split_proof_contexts, ConfidentialTransferArgs,
            },
            event_utils::emit_event,
            token_utils::{read_mint_profile, validate_ata},
        },
        verify_account_owner, verify_current_program,
    },
    require_len,
    state::{AllowedMint, AllowedWallet, Direction, Instance},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::{Seed, Signer},
    error::ProgramError,
    Address, ProgramResult,
};
use spl_token_2022::extension::ExtensionType;

/// Fixed account prefix; proof contexts and then hook extras follow.
const FIXED_ACCOUNTS_LEN: usize = 13;

/// Processes the ConfidentialDeposit instruction.
///
/// Moves a hidden amount from the user's available balance into the escrow's
/// pending balance with a Token-2022 confidential transfer. The escrow takes
/// confidential credits only for the length of that transfer. The amount is the
/// growth of the pending balance, which the event carries for the indexer to
/// decrypt. Nothing on chain sees the amount, so neither a zero amount nor the
/// mint's minimum deposit is enforced.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` user - User depositing tokens
/// 2. `[]` instance - Instance PDA, owner of the escrow account
/// 3. `[]` mint - Token mint being deposited
/// 4. `[]` allowed_mint - AllowedMint PDA to validate mint is allowed
/// 5. `[writable]` user_ata - User's Associated Token Account for this mint
/// 6. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 7. `[]` token_program - Token-2022
/// 8. `[]` instructions_sysvar - Instructions sysvar, read when a proof is an instruction
/// 9. `[]` event_authority - Event authority PDA for emitting events
/// 10. `[]` private_channel_escrow_program - Current program for CPI
/// 11. `[]` user_allowed_wallet_pda - AllowedWallet PDA of `user`, read only while deposits
///     are whitelisted
/// 12. `[]` recipient_allowed_wallet_pda - AllowedWallet PDA of the channel recipient
///     (`recipient`, or `user` when none is given), read only while deposits are whitelisted
///
/// Trailing accounts: one proof context per zero proof offset, in Token-2022's
/// proof order, then the mint's transfer-hook extras.
///
/// # Instruction Data
/// * Confidential transfer fields - see `ConfidentialTransferArgs`
/// * `recipient` (Option<Pubkey>) - Optional recipient for private_channel tracking
pub fn process_confidential_deposit(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;

    if accounts.len() < FIXED_ACCOUNTS_LEN {
        return Err(ProgramError::NotEnoughAccountKeys);
    }
    let (fixed_accounts, remaining_accounts) = accounts.split_at(FIXED_ACCOUNTS_LEN);
    let [payer_info, user_info, instance_info, mint_info, allowed_mint_info, user_ata_info, instance_ata_info, token_program_info, instructions_sysvar_info, event_authority_info, program_info, user_allowed_wallet_pda_info, recipient_allowed_wallet_pda_info] =
        fixed_accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(user_info, false)?;
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

    let allowed_mint_data = allowed_mint_info.try_borrow()?;
    let allowed_mint = AllowedMint::try_from_bytes(&allowed_mint_data)?;

    allowed_mint
        .validate_pda(
            instance_info.address(),
            mint_info.address(),
            allowed_mint_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedMint)?;

    if allowed_mint.deposits_blocked {
        return Err(PrivateChannelEscrowProgramError::DepositsBlockedForMint.into());
    }

    let recipient = args.recipient.unwrap_or(*user_info.address());

    if instance.deposits_whitelisted {
        AllowedWallet::verify_allowed(
            user_allowed_wallet_pda_info,
            instance_info.address(),
            user_info.address(),
            Direction::Deposit,
        )?;
        AllowedWallet::verify_allowed(
            recipient_allowed_wallet_pda_info,
            instance_info.address(),
            &recipient,
            Direction::Deposit,
        )?;
    }

    // Same recreate check as Deposit.
    let mint_profile = read_mint_profile(mint_info)?;
    if allowed_mint.profile_changed(&mint_profile, token_program_info.address()) {
        return Err(PrivateChannelEscrowProgramError::MintProfileChanged.into());
    }

    validate_ata(
        user_ata_info,
        user_info.address(),
        mint_info,
        token_program_info,
    )?;

    validate_ata(
        instance_ata_info,
        instance_info.address(),
        mint_info,
        token_program_info,
    )?;

    let with_fee = allowed_mint.has_extension(ExtensionType::TransferFeeConfig);
    let proof_offsets = args.transfer.proof_offsets.token_2022_order(with_fee);
    let (proof_contexts, hook_extras) = split_proof_contexts(remaining_accounts, &proof_offsets)?;

    let pending_before = read_pending_balance(instance_ata_info)?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    // The instance signs the CPIs below, so its data can't stay borrowed.
    drop(instance_data);

    set_confidential_credits_cpi(instance_ata_info, instance_info, true, &signers)?;
    confidential_transfer_cpi(
        user_ata_info,
        mint_info,
        instance_ata_info,
        instructions_sysvar_info,
        proof_contexts,
        user_info,
        &args.transfer,
        with_fee,
        // The escrow account never requires memos, see Deposit.
        None,
        hook_extras,
        &[],
    )?;
    set_confidential_credits_cpi(instance_ata_info, instance_info, false, &signers)?;

    let pending_after = read_pending_balance(instance_ata_info)?;

    let event = ConfidentialDepositEvent::new(
        instance.instance_seed,
        *user_info.address(),
        recipient,
        *mint_info.address(),
        pending_before,
        pending_after,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct ConfidentialDepositArgs {
    transfer: ConfidentialTransferArgs,
    recipient: Option<Address>,
}

fn process_instruction_data(data: &[u8]) -> Result<ConfidentialDepositArgs, ProgramError> {
    // Transfer fields + recipient flag (1 byte)
    require_len!(data, ConfidentialTransferArgs::LEN + 1);

    let transfer = ConfidentialTransferArgs::try_from_bytes(data)?;
    let mut offset = ConfidentialTransferArgs::LEN;

    let tag = data[offset];
    offset += 1;

    let recipient = match tag {
        0 => None,
        1 => {
            require_len!(data, offset + 32);

            let mut recipient_bytes = [0u8; 32];
            recipient_bytes.copy_from_slice(&data[offset..offset + 32]);
            Some(Address::new_from_array(recipient_bytes))
        }
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    Ok(ConfidentialDepositArgs {
        transfer,
        recipient,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    const TRANSFER_ARGS_LEN: usize = ConfidentialTransferArgs::LEN;

    #[test]
    fn test_process_confidential_deposit_instruction_data_with_recipient() {
        let recipient = Address::new_from_array([7u8; 32]);
        let mut instruction_data = vec![0u8; TRANSFER_ARGS_LEN];
        instruction_data.push(1);
        instruction_data.extend_from_slice(recipient.as_ref());

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.recipient, Some(recipient));
    }

    #[test]
    fn test_process_confidential_deposit_instruction_data_without_recipient() {
        let mut instruction_data = vec![0u8; TRANSFER_ARGS_LEN];
        instruction_data.push(0);

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.recipient, None);
    }

    #[test]
    fn test_process_confidential_deposit_instruction_data_non_canonical_recipient_tag() {
        let mut instruction_data = vec![0u8; TRANSFER_ARGS_LEN];
        instruction_data.push(2);
        instruction_data.extend_from_slice(&[0u8; 32]);

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
