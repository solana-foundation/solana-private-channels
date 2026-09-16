extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::BlockMintEvent,
    processor::{
        shared::{account_check::verify_signer, event_utils::emit_event},
        verify_current_program, verify_mutability,
    },
    require_len,
    state::{discriminator::AccountSerialize, AllowedMint, Instance},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the BlockMint instruction.
///
/// Sets both gates to the requested state. The PDA is never closed, so a mint
/// with deposits blocked can still be withdrawn from.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for transaction fees
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA to validate admin authority
/// 3. `[]` mint - Token mint whose gates are being set
/// 4. `[writable]` allowed_mint - AllowedMint PDA to update
/// 5. `[signer]` event_authority - Event authority PDA for emitting events
/// 6. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `block_deposits` (bool) - Reject new deposits for this mint
/// * `block_withdrawals` (bool) - Reject fund releases for this mint
pub fn process_block_mint(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, mint_info, allowed_mint_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_mutability(allowed_mint_info, true)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    let allowed_mint_data = allowed_mint_info.try_borrow()?;
    let mut allowed_mint = AllowedMint::try_from_bytes(&allowed_mint_data)?;

    allowed_mint
        .validate_pda(
            instance_info.address(),
            mint_info.address(),
            allowed_mint_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedMint)?;

    drop(allowed_mint_data);

    allowed_mint.deposits_blocked = args.block_deposits;
    allowed_mint.withdrawals_blocked = args.block_withdrawals;

    let updated_allowed_mint_data = allowed_mint.to_bytes();
    allowed_mint_info
        .try_borrow_mut()?
        .copy_from_slice(&updated_allowed_mint_data);

    let event = BlockMintEvent::new(
        instance.instance_seed,
        *mint_info.address(),
        allowed_mint.deposits_blocked,
        allowed_mint.withdrawals_blocked,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct BlockMintArgs {
    block_deposits: bool,
    block_withdrawals: bool,
}

fn process_instruction_data(data: &[u8]) -> Result<BlockMintArgs, ProgramError> {
    require_len!(data, 2);

    // The Borsh client refuses any bool byte but 0 or 1, so we do too.
    let block_deposits = match data[0] {
        0 => false,
        1 => true,
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    let block_withdrawals = match data[1] {
        0 => false,
        1 => true,
        _ => return Err(ProgramError::InvalidInstructionData),
    };

    Ok(BlockMintArgs {
        block_deposits,
        block_withdrawals,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ID as PRIVATE_CHANNEL_ESCROW_PROGRAM_ID;
    use alloc::vec;

    // Opposite values confirm the two gates are not read from the same byte.
    #[test]
    fn test_process_block_mint_instruction_data_valid() {
        let args = process_instruction_data(&[1, 0]).expect("Should parse");

        assert!(args.block_deposits);
        assert!(!args.block_withdrawals);
    }

    #[test]
    fn test_process_block_mint_instruction_data_insufficient_length() {
        let result = process_instruction_data(&[1]);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    // A gate byte outside 0/1 must be rejected to stay in sync with the
    // Borsh-based clients, which only accept those two values.
    #[test]
    fn test_process_block_mint_instruction_data_non_canonical_gate() {
        assert_eq!(
            process_instruction_data(&[2, 0]).err(),
            Some(ProgramError::InvalidInstructionData)
        );
        assert_eq!(
            process_instruction_data(&[0, 2]).err(),
            Some(ProgramError::InvalidInstructionData)
        );
    }

    #[test]
    fn test_process_block_mint_empty_accounts() {
        let instruction_data = vec![0, 0];
        let accounts = [];

        let result = process_block_mint(
            &PRIVATE_CHANNEL_ESCROW_PROGRAM_ID,
            &accounts,
            &instruction_data,
        );

        assert_eq!(result.err(), Some(ProgramError::NotEnoughAccountKeys));
    }
}
