use pinocchio::{account::AccountView, cpi::Signer, error::ProgramError, ProgramResult};
use pinocchio_token_2022::instructions::CloseAccount;

use crate::{
    processor::shared::token_utils::{
        get_mint_decimals, get_token_account_balance, transfer_checked_cpi, verify_canonical_ata,
    },
    require,
    state::swap_dvp::SwapDvp,
};

/// Shared refund-and-close path for CancelDvp and RejectDvp. Validates
/// the mints and escrow ATAs, refunds each funded leg to its depositor,
/// closes both escrow ATAs and the SwapDvp PDA, and sweeps reclaimed
/// rent to `rent_destination` (`fixed[0]`, the instruction signer).
///
/// The two instructions are otherwise identical; their only behavioural
/// difference — who is authorized to sign — is checked by the caller
/// before this runs. No extension validation here: Create is the consent
/// point, and the refund path must stay available even if a mint's
/// extension parameters change post-Create so funds are never stranded.
///
/// `fixed` is the 11-account prefix from `split_leg_remaining_accounts`;
/// `leg_a_extras`/`leg_b_extras` are the per-leg transfer-hook accounts.
#[inline(always)]
pub fn refund_and_close_dvp(
    fixed: &[AccountView],
    dvp: &SwapDvp,
    leg_a_extras: &[AccountView],
    leg_b_extras: &[AccountView],
) -> ProgramResult {
    let [rent_destination_info, swap_dvp_info, mint_a_info, mint_b_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_a_info, user_b_ata_b_info, token_program_a_info, token_program_b_info, memo_program_info] =
        fixed
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    require!(
        mint_a_info.address() == &dvp.mint_a && mint_b_info.address() == &dvp.mint_b,
        ProgramError::InvalidAccountData
    );
    // Token program from state, not the mint owner: an unfunded leg's
    // mint may have been closed.
    require!(
        token_program_a_info.address() == &dvp.token_program_a
            && token_program_b_info.address() == &dvp.token_program_b,
        ProgramError::IncorrectProgramId
    );

    // Address-only validation: an unfunded leg's user ATA can be
    // uninitialized; the canonical pubkey is well-defined regardless.
    // If a leg is funded the Transfer below fails naturally on an
    // uninitialized destination. Note: refund pairing — each user gets
    // their *own* mint back (not the cross used at Settle).
    // dvp_ata_a: DvP PDA's escrow for mint_a (asset escrow).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        &dvp.mint_a,
        token_program_a_info,
    )?;
    // dvp_ata_b: DvP PDA's escrow for mint_b (cash escrow).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        &dvp.mint_b,
        token_program_b_info,
    )?;
    // user_a_ata_a: seller's ATA for mint_a — refund destination if asset leg was funded.
    verify_canonical_ata(
        user_a_ata_a_info,
        &dvp.user_a,
        &dvp.mint_a,
        token_program_a_info,
    )?;
    // user_b_ata_b: buyer's ATA for mint_b — refund destination if cash leg was funded.
    verify_canonical_ata(
        user_b_ata_b_info,
        &dvp.user_b,
        &dvp.mint_b,
        token_program_b_info,
    )?;

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);
    let signer_seeds = [Signer::from(&swap_dvp_seeds)];

    // Snapshot both balances before any CPI runs, so a hook drain of
    // one leg during the other leg's refund forces InsufficientFunds
    // on the second transfer instead of being silently skipped.
    let leg_a_amount = get_token_account_balance(dvp_ata_a_info)?;
    let leg_b_amount = get_token_account_balance(dvp_ata_b_info)?;

    if leg_a_amount > 0 {
        transfer_checked_cpi(
            dvp_ata_a_info,
            mint_a_info,
            user_a_ata_a_info,
            swap_dvp_info,
            leg_a_amount,
            get_mint_decimals(mint_a_info)?,
            token_program_a_info.address(),
            memo_program_info,
            leg_a_extras,
            &signer_seeds,
        )?;
    }

    if leg_b_amount > 0 {
        transfer_checked_cpi(
            dvp_ata_b_info,
            mint_b_info,
            user_b_ata_b_info,
            swap_dvp_info,
            leg_b_amount,
            get_mint_decimals(mint_b_info)?,
            token_program_b_info.address(),
            memo_program_info,
            leg_b_extras,
            &signer_seeds,
        )?;
    }

    CloseAccount {
        account: dvp_ata_a_info,
        destination: rent_destination_info,
        authority: swap_dvp_info,
        token_program: token_program_a_info.address(),
    }
    .invoke_signed(&signer_seeds)?;

    CloseAccount {
        account: dvp_ata_b_info,
        destination: rent_destination_info,
        authority: swap_dvp_info,
        token_program: token_program_b_info.address(),
    }
    .invoke_signed(&signer_seeds)?;

    let rent_destination_lamports = rent_destination_info.lamports();
    rent_destination_info.set_lamports(
        rent_destination_lamports
            .checked_add(swap_dvp_info.lamports())
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );
    swap_dvp_info.set_lamports(0);
    swap_dvp_info.close()?;

    Ok(())
}
