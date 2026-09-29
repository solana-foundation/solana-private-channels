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
    state::{WithdrawFeeConfig, WITHDRAW_FEE_CONFIG_SEED},
};

/// Processes the SetWithdrawFeeConfig instruction.
///
/// Creates the mint's fee config on first use and overwrites it on every later
/// call. The operator sends it in every deposit mint transaction, so the last
/// fee set at the escrow's AllowMint is always the one in force.
///
/// Every failure is a custom error or a builtin the operator does not retry
/// on. `InvalidAccountData`, `UninitializedAccount` and `IncorrectProgramId`
/// from any instruction in a deposit transaction read to the operator as a
/// missing mint and retry forever.
///
/// # Account Layout
/// 0. `[signer, writable]` authority - Mint authority, also pays for the config
/// 1. `[]` mint - Token mint
/// 2. `[writable]` withdraw_fee_config - Fee config PDA for the mint
/// 3. `[]` system_program - System program
///
/// # Instruction Data
/// * `fee` (u64) - Fee in base units charged on top of each withdrawal, 0 allowed
/// * `treasury` (Pubkey) - Owner of the token account fees are paid to
pub fn process_set_withdraw_fee_config(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = parse_instruction_data(instruction_data)?;

    let [authority_info, mint_info, withdraw_fee_config_info, system_program_info] = accounts
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

    let (withdraw_fee_config_address, bump) = Address::find_program_address(
        &[WITHDRAW_FEE_CONFIG_SEED, mint_info.address().as_ref()],
        program_id,
    );
    if withdraw_fee_config_info.address() != &withdraw_fee_config_address {
        return Err(PrivateChannelWithdrawProgramError::InvalidFeeConfig.into());
    }

    if withdraw_fee_config_info.owned_by(program_id) {
        WithdrawFeeConfig::try_from_bytes(&withdraw_fee_config_info.try_borrow()?)?;
    } else {
        let bump_seed = [bump];
        let seeds = [
            Seed::from(WITHDRAW_FEE_CONFIG_SEED),
            Seed::from(mint_info.address().as_ref()),
            Seed::from(&bump_seed),
        ];
        create_pda_account(
            authority_info,
            &Rent::get()?,
            WithdrawFeeConfig::LEN,
            program_id,
            withdraw_fee_config_info,
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

    let withdraw_fee_config = WithdrawFeeConfig {
        bump,
        fee: args.fee,
        treasury: args.treasury,
        treasury_token_account,
    };
    withdraw_fee_config_info
        .try_borrow_mut()?
        .copy_from_slice(&withdraw_fee_config.to_bytes());

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetWithdrawFeeConfigArgs {
    pub fee: u64,
    pub treasury: Address,
}

fn parse_instruction_data(data: &[u8]) -> Result<SetWithdrawFeeConfigArgs, ProgramError> {
    require_len!(data, 8 + 32);

    let fee = u64::from_le_bytes(
        data[..8]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let treasury = Address::new_from_array(
        data[8..40]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    Ok(SetWithdrawFeeConfigArgs { fee, treasury })
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    use super::*;
    use alloc::vec;

    #[test]
    fn test_parse_instruction_data_valid() {
        let fee = 1_234_567u64;
        let treasury = Address::new_from_array([7u8; 32]);
        let mut instruction_data = vec![];
        instruction_data.extend_from_slice(&fee.to_le_bytes());
        instruction_data.extend_from_slice(treasury.as_ref());

        let args = parse_instruction_data(&instruction_data).unwrap();

        assert_eq!(args, SetWithdrawFeeConfigArgs { fee, treasury });
    }

    #[test]
    fn test_parse_instruction_data_missing_treasury() {
        let instruction_data = 1_000u64.to_le_bytes();

        let result = parse_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
