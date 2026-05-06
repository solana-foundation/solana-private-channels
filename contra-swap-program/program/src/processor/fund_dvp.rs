use crate::{
    error::ContraSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer, verify_token_program},
    processor::shared::token_utils::verify_canonical_ata,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{
    account::AccountView,
    address::Address,
    error::ProgramError,
    sysvars::{clock::Clock, Sysvar},
    ProgramResult,
};
use pinocchio_token::{instructions::Transfer, state::TokenAccount};

/// Processes the FundDvp instruction.
///
/// Permissioned to either depositor. The signer's identity selects the
/// leg:
/// - signer == dvp.user_a → top up to `dvp.amount_a` of `dvp.mint_a`
/// - signer == dvp.user_b → top up to `dvp.amount_b` of `dvp.mint_b`
///
/// "Top up" semantics: the signer transfers `leg_amount - escrow_balance`
/// from their ATA. This is robust to dust pre-deposited by anyone via a
/// raw SPL Transfer (escrow ATAs are derivable from public state, so any
/// holder of the leg's mint can drop tokens into them). If the escrow
/// already holds at least `leg_amount`, the leg is treated as funded and
/// the call rejects (the depositor must Reclaim first to re-fund).
///
/// The mint pubkey comes from `SwapDvp` state, so no mint account is
/// part of the layout. SPL classic Transfer doesn't take a mint either.
///
/// # Account Layout
/// 0. `[signer]`    signer            - Must equal `dvp.user_a` or `dvp.user_b`
/// 1. `[]`          swap_dvp          - SwapDvp PDA owned by this program
/// 2. `[writable]`  signer_source_ata - Signer's canonical ATA for the leg's mint
/// 3. `[writable]`  dvp_dest_ata      - DvP's escrow ATA for the leg's mint
/// 4. `[]`          token_program
pub fn process_fund_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [signer_info, swap_dvp_info, signer_source_ata_info, dvp_dest_ata_info, token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(signer_info, false)?;
    verify_token_program(token_program_info)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    // Leg selection by signer identity.
    let (leg_mint, leg_amount) = if signer_info.address() == &dvp.user_a {
        (&dvp.mint_a, dvp.amount_a)
    } else if signer_info.address() == &dvp.user_b {
        (&dvp.mint_b, dvp.amount_b)
    } else {
        return Err(ContraSwapProgramError::SignerNotParty.into());
    };

    let now = Clock::get()?.unix_timestamp;
    if now > dvp.expiry_timestamp {
        return Err(ContraSwapProgramError::DvpExpired.into());
    }

    // Both ATAs must be canonical. The mint pubkey is read from state.
    // signer_source_ata is the depositor's ATA for the leg's mint
    // (transfer source).
    verify_canonical_ata(
        signer_source_ata_info,
        signer_info.address(),
        leg_mint,
        token_program_info,
    )?;
    // dvp_dest_ata is the DvP PDA's escrow ATA for the leg's mint
    // (transfer destination).
    verify_canonical_ata(
        dvp_dest_ata_info,
        swap_dvp_info.address(),
        leg_mint,
        token_program_info,
    )?;

    // Top up to `leg_amount`. If the escrow already meets or exceeds the
    // target, the leg is funded — reject so callers must Reclaim before
    // re-funding (Reclaim drains the leg back to 0).
    let escrow_balance = TokenAccount::from_account_view(dvp_dest_ata_info)?.amount();
    if escrow_balance >= leg_amount {
        return Err(ContraSwapProgramError::LegAlreadyFunded.into());
    }
    let to_transfer = leg_amount - escrow_balance;

    Transfer {
        from: signer_source_ata_info,
        to: dvp_dest_ata_info,
        authority: signer_info,
        amount: to_transfer,
    }
    .invoke()?;

    Ok(())
}
