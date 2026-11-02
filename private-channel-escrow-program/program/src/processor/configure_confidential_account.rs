extern crate alloc;

use crate::{
    constants::INSTANCE_SEED,
    error::PrivateChannelEscrowProgramError,
    events::ConfigureConfidentialAccountEvent,
    processor::{
        shared::{
            account_check::{
                verify_instructions_sysvar, verify_signer, verify_system_program,
                verify_token_2022_program,
            },
            confidential_utils::{
                configure_account_cpi, read_elgamal_pubkey, reallocate_cpi,
                set_confidential_credits_cpi,
            },
            event_utils::emit_event,
            token_utils::validate_ata,
        },
        verify_account_owner, verify_current_program,
    },
    require_len,
    state::{AllowedMint, Instance},
    validate_event_authority,
};
use pinocchio::{
    account::AccountView,
    cpi::{Seed, Signer},
    error::ProgramError,
    Address, ProgramResult,
};
use spl_token_2022::extension::ExtensionType;

/// Processes the ConfigureConfidentialAccount instruction.
///
/// Turns on confidential transfers for the instance's escrow account of an allowed
/// mint, then turns confidential credits off so they only arrive through
/// ConfidentialDeposit and MoveToConfidential. Public credits stay on. Token-2022
/// refuses a mint without `ConfidentialTransferMint` and an account configured
/// before.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Pays for the larger escrow account
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA, owner of the escrow account
/// 3. `[]` mint - Token mint of the escrow account
/// 4. `[]` allowed_mint - AllowedMint PDA of the mint
/// 5. `[writable]` instance_ata - Instance's Associated Token Account for this mint
/// 6. `[]` system_program - System program
/// 7. `[]` token_program - Token-2022
/// 8. `[]` instructions_sysvar - Instructions sysvar, holding the key's proof
/// 9. `[]` event_authority - Event authority PDA for emitting events
/// 10. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * `decryptable_zero_balance` ([u8; 36]) - Zero, encrypted under the instance AES key
/// * `proof_instruction_offset` (i8) - Position of the `VerifyPubkeyValidity`
///   instruction relative to this one, never 0
pub fn process_configure_confidential_account(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let args = process_instruction_data(instruction_data)?;
    let [payer_info, admin_info, instance_info, mint_info, allowed_mint_info, instance_ata_info, system_program_info, token_program_info, instructions_sysvar_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_system_program(system_program_info)?;
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

    instance.validate_admin(admin_info.address())?;

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
        instance_ata_info,
        instance_info.address(),
        mint_info,
        token_program_info,
    )?;

    let bump_slice = [instance.bump];
    let signer_seeds = [
        Seed::from(INSTANCE_SEED),
        Seed::from(instance.instance_seed.as_ref()),
        Seed::from(&bump_slice),
    ];
    let signers = [Signer::from(&signer_seeds)];

    // The instance signs the CPIs below, so its data can't stay borrowed.
    drop(instance_data);

    reallocate_cpi(
        instance_ata_info,
        payer_info,
        system_program_info,
        instance_info,
        allowed_mint.has_extension(ExtensionType::TransferFeeConfig),
        &signers,
    )?;
    configure_account_cpi(
        instance_ata_info,
        mint_info,
        instructions_sysvar_info,
        instance_info,
        &args.decryptable_zero_balance,
        args.proof_instruction_offset,
        &signers,
    )?;
    set_confidential_credits_cpi(instance_ata_info, instance_info, false, &signers)?;

    let event = ConfigureConfidentialAccountEvent::new(
        instance.instance_seed,
        *mint_info.address(),
        read_elgamal_pubkey(instance_ata_info)?,
    );
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}

struct ConfigureConfidentialAccountArgs {
    decryptable_zero_balance: [u8; 36],
    proof_instruction_offset: i8,
}

fn process_instruction_data(data: &[u8]) -> Result<ConfigureConfidentialAccountArgs, ProgramError> {
    require_len!(data, 36 + 1);

    let mut decryptable_zero_balance = [0u8; 36];
    decryptable_zero_balance.copy_from_slice(&data[..36]);

    let proof_instruction_offset = data[36] as i8;
    if proof_instruction_offset == 0 {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(ConfigureConfidentialAccountArgs {
        decryptable_zero_balance,
        proof_instruction_offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_process_configure_confidential_account_instruction_data() {
        let mut instruction_data = vec![1u8; 36];
        instruction_data.push(-1i8 as u8);

        let args = process_instruction_data(&instruction_data).unwrap();

        assert_eq!(args.decryptable_zero_balance, [1u8; 36]);
        assert_eq!(args.proof_instruction_offset, -1);
    }

    // Offset 0 means a context account, which this instruction does not take.
    #[test]
    fn test_process_configure_confidential_account_rejects_zero_offset() {
        let mut instruction_data = vec![1u8; 36];
        instruction_data.push(0);

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }

    #[test]
    fn test_process_configure_confidential_account_instruction_data_too_short() {
        let instruction_data = vec![1u8; 36];

        let result = process_instruction_data(&instruction_data);

        assert_eq!(result.err(), Some(ProgramError::InvalidInstructionData));
    }
}
