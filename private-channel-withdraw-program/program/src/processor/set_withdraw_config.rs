use pinocchio::{
    account::AccountView,
    cpi::Seed,
    error::ProgramError,
    sysvars::{rent::Rent, Sysvar},
    Address, ProgramResult,
};
use pinocchio_associated_token_account::ID as ATA_PROGRAM_ID;
use pinocchio_token::{state::Mint, ID as TOKEN_PROGRAM_ID};

use crate::{
    error::PrivateChannelWithdrawProgramError,
    processor::{create_pda_account, verify_mint_account, verify_signer},
    require_len,
    state::{WithdrawConfig, WITHDRAW_CONFIG_SEED},
};

/// Processes the SetWithdrawConfig instruction.
///
/// Creates the mint's withdraw config on first use and overwrites it on later
/// calls whose `allow_mint_slot` is at or after the stored one. The operator
/// sends it in every deposit mint transaction, and those land in any order, so
/// a deposit built before a reprice is a no-op instead of restoring older values.
///
/// Every failure is a custom error or a builtin the operator does not retry
/// on. `InvalidAccountData`, `UninitializedAccount` and `IncorrectProgramId`
/// from any instruction in a deposit transaction read to the operator as a
/// missing mint and retry forever.
///
/// # Account Layout
/// 0. `[signer, writable]` authority - Mint authority, also pays for the config
/// 1. `[]` mint - Token mint
/// 2. `[writable]` withdraw_config - Withdraw config PDA for the mint
/// 3. `[]` system_program - System program
///
/// # Instruction Data
/// * `fee` (u64) - Fee in base units charged on top of each withdrawal, 0 allowed
/// * `allow_mint_slot` (u64) - Slot of the AllowMint these values came from
/// * `treasury` (Pubkey) - Owner of the token account fees are paid to
/// * `min_withdraw_amount` (u64) - Smallest amount a withdrawal may move, 0 for none
pub fn process_set_withdraw_config(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = parse_instruction_data(instruction_data)?;

    let [authority_info, mint_info, withdraw_config_info, system_program_info] = accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    // Writable is left to the CreateAccount CPI, which fails with
    // PrivilegeEscalation. `verify_signer(.., true)` would return InvalidAccountData.
    verify_signer(authority_info, false)?;
    verify_mint_account(mint_info)?;

    {
        let mint_data = mint_info.try_borrow()?;
        let mint = unsafe { Mint::from_bytes_unchecked(&mint_data) };
        if mint.mint_authority() != Some(authority_info.address()) {
            return Err(PrivateChannelWithdrawProgramError::InvalidMintAuthority.into());
        }
    }

    if system_program_info.address() != &pinocchio_system::ID {
        return Err(PrivateChannelWithdrawProgramError::InvalidSystemProgram.into());
    }

    let (withdraw_config_address, bump) = Address::find_program_address(
        &[WITHDRAW_CONFIG_SEED, mint_info.address().as_ref()],
        program_id,
    );
    if withdraw_config_info.address() != &withdraw_config_address {
        return Err(PrivateChannelWithdrawProgramError::InvalidWithdrawConfig.into());
    }

    if withdraw_config_info.owned_by(program_id) {
        let stored = WithdrawConfig::try_from_bytes(&withdraw_config_info.try_borrow()?)?;
        // Ok, not an error: the deposit carrying this write must still mint.
        if args.allow_mint_slot < stored.allow_mint_slot {
            return Ok(());
        }
    } else {
        let bump_seed = [bump];
        let seeds = [
            Seed::from(WITHDRAW_CONFIG_SEED),
            Seed::from(mint_info.address().as_ref()),
            Seed::from(&bump_seed),
        ];
        create_pda_account(
            authority_info,
            &Rent::get()?,
            WithdrawConfig::LEN,
            program_id,
            withdraw_config_info,
            seeds,
        )?;
    }

    // Only the address. The account may not exist yet; WithdrawFunds fails
    // until it does, except for the treasury's own withdrawals.
    let treasury_token_account = Address::find_program_address(
        &[
            args.treasury.as_ref(),
            TOKEN_PROGRAM_ID.as_ref(),
            mint_info.address().as_ref(),
        ],
        &ATA_PROGRAM_ID,
    )
    .0;

    let withdraw_config = WithdrawConfig {
        bump,
        fee: args.fee,
        allow_mint_slot: args.allow_mint_slot,
        treasury: args.treasury,
        treasury_token_account,
        min_withdraw_amount: args.min_withdraw_amount,
    };
    withdraw_config_info
        .try_borrow_mut()?
        .copy_from_slice(&withdraw_config.to_bytes());

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetWithdrawConfigArgs {
    pub fee: u64,
    pub allow_mint_slot: u64,
    pub treasury: Address,
    pub min_withdraw_amount: u64,
}

fn parse_instruction_data(data: &[u8]) -> Result<SetWithdrawConfigArgs, ProgramError> {
    require_len!(data, 8 + 8 + 32 + 8);

    let fee = u64::from_le_bytes(
        data[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let allow_mint_slot = u64::from_le_bytes(
        data[8..16]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let treasury = Address::new_from_array(
        data[16..48]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let min_withdraw_amount = u64::from_le_bytes(
        data[48..56]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    Ok(SetWithdrawConfigArgs {
        fee,
        allow_mint_slot,
        treasury,
        min_withdraw_amount,
    })
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use super::*;
    use alloc::vec;

    #[test]
    fn test_parse_instruction_data_valid() {
        let fee = 1_234_567u64;
        let allow_mint_slot = 7_654_321u64;
        let treasury = Address::new_from_array([7u8; 32]);
        let min_withdraw_amount = 2_468_024u64;
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&fee.to_le_bytes());
        instruction_data.extend_from_slice(&allow_mint_slot.to_le_bytes());
        instruction_data.extend_from_slice(treasury.as_ref());
        instruction_data.extend_from_slice(&min_withdraw_amount.to_le_bytes());

        let args = parse_instruction_data(&instruction_data).unwrap();

        assert_eq!(
            args,
            SetWithdrawConfigArgs {
                fee,
                allow_mint_slot,
                treasury,
                min_withdraw_amount
            }
        );
    }

    // The pre-minimum layout ended at the treasury.
    #[test]
    fn test_parse_instruction_data_missing_minimum() {
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&1_000u64.to_le_bytes());
        instruction_data.extend_from_slice(&1u64.to_le_bytes());
        instruction_data.extend_from_slice(Address::new_from_array([7u8; 32]).as_ref());

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
