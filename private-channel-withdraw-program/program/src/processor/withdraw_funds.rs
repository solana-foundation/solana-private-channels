use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};
use pinocchio_token::instructions::Burn;

use crate::{
    error::PrivateChannelWithdrawProgramError,
    events::WithdrawFundsEvent,
    processor::{
        validate_ata, verify_ata_program, verify_mint_account, verify_signer, verify_token_program,
    },
    require_len,
};

/// Processes the WithdrawFunds instruction.
///
/// # Account Layout
/// 0. `[signer]` user - User initiating the withdrawal
/// 1. `[]` mint - Token mint
/// 2. `[writable]` token_account - Source token account
/// 3. `[]` token_program - Token program
/// 4. `[]` associated_token_program - Associated token program
///
/// # Instruction Data
/// * `amount` (u64) - Amount of tokens to withdraw
/// * `destination` (Option<Pubkey>) - Destination public key
pub fn process_withdraw_funds(
    _program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = parse_instruction_data(instruction_data)?;

    if args.amount == 0 {
        return Err(PrivateChannelWithdrawProgramError::ZeroAmount.into());
    }

    let [user_info, mint_info, token_account_info, token_program_info, associated_token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(user_info, false)?;
    verify_ata_program(associated_token_program_info)?;
    verify_token_program(token_program_info)?;
    verify_mint_account(mint_info)?;

    validate_ata(
        token_account_info,
        user_info.address(),
        mint_info,
        token_program_info,
    )?;

    Burn {
        account: token_account_info,
        mint: mint_info,
        authority: user_info,
        amount: args.amount,
    }
    .invoke()?;

    let event = WithdrawFundsEvent {
        amount: args.amount,
        destination: args.destination.unwrap_or(*user_info.address()),
    };
    pinocchio_log::log!("{}", event.to_bytes().as_slice());

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithdrawFundsArgs {
    pub amount: u64,
    pub destination: Option<Address>,
}

fn parse_instruction_data(data: &[u8]) -> Result<WithdrawFundsArgs, ProgramError> {
    require_len!(data, 9);

    let amount = u64::from_le_bytes(
        data[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let destination = match data[8] {
        0 => None,
        1 => {
            require_len!(data, 9 + 32);
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data[9..41]);
            Some(Address::new_from_array(bytes))
        }
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    Ok(WithdrawFundsArgs {
        amount,
        destination,
    })
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use super::*;
    use crate::ID as PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID;
    use alloc::vec;

    #[test]
    fn test_parse_instruction_data_valid_with_destination() {
        let destination_key = Address::new_from_array([0u8; 32]);
        let mut instruction_data = vec![];

        instruction_data.extend_from_slice(&1000u64.to_le_bytes());
        instruction_data.push(1);
        instruction_data.extend_from_slice(destination_key.as_ref());

        let result = parse_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.amount, 1000);
        assert_eq!(args.destination, Some(destination_key));
    }

    #[test]
    fn test_parse_instruction_data_valid_without_destination() {
        let mut instruction_data = vec![];

        instruction_data.extend_from_slice(&500u64.to_le_bytes());
        instruction_data.push(0);

        let result = parse_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.amount, 500);
        assert_eq!(args.destination, None);
    }

    #[test]
    fn test_parse_instruction_data_insufficient_length() {
        let instruction_data = vec![1, 2, 3];

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_parse_instruction_data_empty() {
        let instruction_data = vec![];

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_parse_instruction_data_zero_amount() {
        let mut instruction_data = vec![];

        instruction_data.extend_from_slice(&0u64.to_le_bytes());
        instruction_data.push(0);

        let result = parse_instruction_data(&instruction_data);

        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.amount, 0);
        assert_eq!(args.destination, None);
    }

    #[test]
    fn test_parse_instruction_data_truncated_destination() {
        // Flag byte = 1 (destination present) but only 5 bytes of pubkey instead of 32
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&100u64.to_le_bytes());
        instruction_data.push(1);
        instruction_data.extend_from_slice(&[0u8; 5]);

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_parse_instruction_data_non_canonical_option_tag() {
        // Flag byte = 2 is not a valid Option tag. The on-chain parser must reject it
        // so it stays in sync with the Borsh-based indexer, which only accepts 0 or 1.
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&100u64.to_le_bytes());
        instruction_data.push(2);
        instruction_data.extend_from_slice(&[0u8; 32]);

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_withdraw_funds_empty_accounts() {
        let instruction_data = vec![6, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let accounts = [];

        let result = process_withdraw_funds(
            &PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }
}
