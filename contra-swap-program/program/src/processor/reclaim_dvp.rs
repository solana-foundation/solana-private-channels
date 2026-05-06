use crate::{
    error::ContraSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer, verify_token_program},
    processor::shared::token_utils::verify_canonical_ata,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{
    account::AccountView,
    address::Address,
    cpi::Signer,
    error::ProgramError,
    sysvars::{clock::Clock, Sysvar},
    ProgramResult,
};
use pinocchio_token::{instructions::Transfer, state::TokenAccount};

/// Processes the ReclaimDvp instruction.
///
/// Permissioned to either depositor. The signer's identity selects the
/// leg to drain:
/// - signer == dvp.user_a → drain `dvp.mint_a` escrow back to user_a
/// - signer == dvp.user_b → drain `dvp.mint_b` escrow back to user_b
///
/// Drains the escrow's *actual* balance (matches Cancel/Reject), not the
/// state-recorded leg amount. This sweeps any dust pre-deposited via a
/// raw SPL Transfer along with the legitimate funds, so the leg can be
/// cleanly re-funded afterwards. If the leg has zero balance, the call
/// is a no-op.
///
/// The DvP itself stays open after a reclaim — the caller can re-fund
/// the leg later, or either party can abort the trade. Reclaim only
/// drains a single leg.
///
/// # Account Layout
/// 0. `[signer]`    signer            - Must equal `dvp.user_a` or `dvp.user_b`
/// 1. `[]`          swap_dvp          - SwapDvp PDA (signs the transfer as authority)
/// 2. `[writable]`  dvp_source_ata    - DvP's escrow ATA for the leg's mint
/// 3. `[writable]`  signer_dest_ata   - Signer's canonical ATA for the leg's mint
/// 4. `[]`          token_program
pub fn process_reclaim_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [signer_info, swap_dvp_info, dvp_source_ata_info, signer_dest_ata_info, token_program_info] =
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
    let leg_mint = if signer_info.address() == &dvp.user_a {
        &dvp.mint_a
    } else if signer_info.address() == &dvp.user_b {
        &dvp.mint_b
    } else {
        return Err(ContraSwapProgramError::SignerNotParty.into());
    };

    // Reclaim only works pre-expiry. After expiry, Cancel or Reject
    // is the way to drain a funded leg.
    let now = Clock::get()?.unix_timestamp;
    if now > dvp.expiry_timestamp {
        return Err(ContraSwapProgramError::DvpExpired.into());
    }

    // Both ATAs must be canonical. The leg's mint pubkey is read from state.
    // dvp_source_ata is the DvP PDA's escrow for the leg's mint
    // (refund source).
    verify_canonical_ata(
        dvp_source_ata_info,
        swap_dvp_info.address(),
        leg_mint,
        token_program_info,
    )?;
    // signer_dest_ata is the depositor's ATA for the leg's mint
    // (refund destination).
    verify_canonical_ata(
        signer_dest_ata_info,
        signer_info.address(),
        leg_mint,
        token_program_info,
    )?;

    // Drain whatever the escrow holds (matches Cancel/Reject). If empty,
    // skip the CPI: a no-op call is harmless and avoids an Insufficient-
    // Funds error that would otherwise fire on the SPL Transfer.
    let escrow_balance = TokenAccount::from_account_view(dvp_source_ata_info)?.amount();
    if escrow_balance == 0 {
        return Ok(());
    }

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);

    Transfer {
        from: dvp_source_ata_info,
        to: signer_dest_ata_info,
        authority: swap_dvp_info,
        amount: escrow_balance,
    }
    .invoke_signed(&[Signer::from(&swap_dvp_seeds)])?;

    Ok(())
}
