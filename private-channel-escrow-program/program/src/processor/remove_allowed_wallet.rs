extern crate alloc;

use crate::{
    error::PrivateChannelEscrowProgramError,
    events::RemoveAllowedWalletEvent,
    processor::{
        shared::{
            account_check::{verify_signer, verify_system_program},
            event_utils::emit_event,
        },
        verify_current_program,
    },
    state::{AllowedWallet, Instance},
    validate_event_authority,
};
use pinocchio::{account::AccountView, error::ProgramError, Address, ProgramResult};

/// Processes the RemoveAllowedWallet instruction.
///
/// # Account Layout
/// 0. `[signer, writable]` payer - Receives the rent reclaimed from closed account
/// 1. `[signer]` admin - Admin of the instance
/// 2. `[]` instance - Instance PDA to validate admin authority
/// 3. `[]` wallet - Wallet to remove from the whitelist
/// 4. `[writable]` allowed_wallet_pda - AllowedWallet PDA to be closed
/// 5. `[]` system_program - System program (not used but kept for consistency)
/// 6. `[signer]` event_authority - Event authority PDA for emitting events
/// 7. `[]` private_channel_escrow_program - Current program for CPI
///
/// # Instruction Data
/// * None - No instruction data required
pub fn process_remove_allowed_wallet(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [payer_info, admin_info, instance_info, wallet_info, allowed_wallet_pda_info, system_program_info, event_authority_info, program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(payer_info, true)?;
    verify_signer(admin_info, false)?;
    verify_system_program(system_program_info)?;
    verify_current_program(program_info)?;

    validate_event_authority!(event_authority_info);

    let instance_data = instance_info.try_borrow()?;
    let instance = Instance::try_from_bytes(&instance_data)?;

    instance
        .validate_pda(instance_info)
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidInstance)?;

    instance.validate_admin(admin_info.address())?;

    let allowed_wallet_data = allowed_wallet_pda_info.try_borrow()?;
    let allowed_wallet = AllowedWallet::try_from_bytes(&allowed_wallet_data)?;

    allowed_wallet
        .validate_pda(
            instance_info.address(),
            wallet_info.address(),
            allowed_wallet_pda_info,
        )
        .map_err(|_| PrivateChannelEscrowProgramError::InvalidAllowedWalletPda)?;

    drop(allowed_wallet_data);

    let payer_lamports = payer_info.lamports();
    payer_info.set_lamports(
        payer_lamports
            .checked_add(allowed_wallet_pda_info.lamports())
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );
    allowed_wallet_pda_info.set_lamports(0);
    allowed_wallet_pda_info.close()?;

    let event = RemoveAllowedWalletEvent::new(instance.instance_seed, *wallet_info.address());
    emit_event(
        program_id,
        event_authority_info,
        program_info,
        &event.to_bytes(),
    )?;

    Ok(())
}
