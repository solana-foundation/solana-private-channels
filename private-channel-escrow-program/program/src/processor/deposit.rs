extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::DepositEvent,
    processor::{
        get_mint_decimals, get_token_account_balance,
        shared::{
            account_check::{verify_signer, verify_system_program},
            event_utils::emit_event,
            token_utils::{validate_ata, validate_token2022_extensions},
        },
        verify_account_owner, verify_ata_program, verify_current_program, verify_token_programs,
    },
    require_len,
    state::{AllowedMint, Instance},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

use pinocchio_token_2022::{
    instructions::TransferChecked as TransferChecked2022, ID as TOKEN_2022_PROGRAM_ID,
};

/// Processes the Deposit instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` user - User depositing tokens
/// 2. `[]` instance - Instance PDA to validate
/// 3. `[]` mint - Token mint being deposited
/// 4. `[]` allowed_mint - AllowedMint PDA to validate mint is allowed
/// 5. `[writable]` user_ata - User's Associated Token Account for this mint
/// 6. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 7. `[]` system_program - System program
/// 8. `[]` token_program - Token program for the mint
/// 9. `[]` associated_token_program - Associated Token program
/// 10. `[]` event_authority - Event authority PDA for emitting events
/// 11. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `amount` (u64) - Amount of tokens to deposit
/// * `recipient` (Option<Pubkey>) - Optional recipient for private_channel tracking
pub fn process_deposit(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, user_info, instance_info, mint_info, allowed_mint_info, user_ata_info, instance_ata_info, system_program_info, token_program_info, associated_token_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(user_info, false)?;
    verify_ata_program(associated_token_program_info)?;
    verify_system_program(system_program_info)?;
    verify_token_programs(token_program_info)?;
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

    if token_program_info.address() == &TOKEN_2022_PROGRAM_ID {
        validate_token2022_extensions(mint_info)?;
    }

    let escrow_token_balance_before = get_token_account_balance(instance_ata_info)?;

    TransferChecked2022 {
        from: user_ata_info,
        to: instance_ata_info,
        authority: user_info,
        amount: args.amount,
        token_program: token_program_info.address(),
        mint: mint_info,
        decimals: get_mint_decimals(mint_info)?,
    }
    .invoke_signed(&[])?;

    let escrow_token_balance_after = get_token_account_balance(instance_ata_info)?;
    let received = escrow_token_balance_after
        .checked_sub(escrow_token_balance_before)
        .ok_or(PrivateChannelEscrowProgramError::InvalidEscrowBalance)?;

    let recipient = args.recipient.unwrap_or(*user_info.address());
    let event = DepositEvent::new(
        instance.instance_seed,
        *user_info.address(),
        received,
        recipient,
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

struct DepositArgs {
    amount: u64,
    recipient: Option<Address>,
}

fn process_instruction_data(data: &[u8]) -> Result<DepositArgs, ProgramError> {
    // Minimum: amount (8 bytes) + recipient flag (1 byte)
    require_len!(data, 9);

    let mut offset = 0;

    let amount = u64::from_le_bytes(
        data[offset..offset + 8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );
    offset += 8;

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

    Ok(DepositArgs { amount, recipient })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_process_deposit_instruction_data_with_recipient() {
        let recipient_key = Address::new_from_array([0u8; 32]);
        let mut instruction_data = vec![];

        instruction_data.extend_from_slice(&1000u64.to_le_bytes());
        instruction_data.push(1);
        instruction_data.extend_from_slice(recipient_key.as_ref());

        let result = process_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.amount, 1000);
        assert_eq!(args.recipient, Some(recipient_key));
    }

    #[test]
    fn test_process_deposit_instruction_data_without_recipient() {
        let mut instruction_data = vec![];

        instruction_data.extend_from_slice(&500u64.to_le_bytes());
        instruction_data.push(0);

        let result = process_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.amount, 500);
        assert_eq!(args.recipient, None);
    }

    #[test]
    fn test_process_deposit_instruction_data_insufficient_length() {
        let instruction_data = vec![1, 2, 3];

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_deposit_instruction_data_non_canonical_recipient_tag() {
        // Flag byte = 2 is not a valid Option tag. The on-chain parser must reject it
        // so it stays in sync with the Borsh-based indexer, which only accepts 0 or 1.
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&1000u64.to_le_bytes());
        instruction_data.push(2);
        instruction_data.extend_from_slice(&[0u8; 32]);

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_deposit_empty_accounts() {
        let instruction_data = vec![6, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let accounts = [];

        let result = process_deposit(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }

    // has_recipient flag = 1 signals that 32 more bytes follow for the recipient key.
    // If those bytes are absent the require_len! guard must reject the data rather
    // than reading out-of-bounds memory.
    #[test]
    fn test_process_deposit_instruction_data_has_recipient_flag_but_missing_key() {
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&1000u64.to_le_bytes()); // amount
        instruction_data.push(1); // has_recipient = true, but no 32-byte key follows

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
