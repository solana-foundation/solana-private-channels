use crate::{
    error::ContraSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer, verify_token_program},
    processor::shared::token_utils::verify_canonical_ata,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{
    account::AccountView, address::Address, cpi::Signer, error::ProgramError, ProgramResult,
};
use pinocchio_token::{
    instructions::{CloseAccount, Transfer},
    state::TokenAccount,
};

/// Processes the RejectDvp instruction.
///
/// Permissioned to either depositor: user_a or user_b can pull the
/// plug on the trade entirely, refunding any funded legs to their
/// respective depositors and closing the SwapDvp + both escrow ATAs.
/// No expiry check — Reject must work post-expiry too.
///
/// Closed-account rent goes to `dvp.settlement_authority`, matching the
/// rent-recipient convention used by Settle and Cancel.
///
/// # Account Layout
/// 0. `[signer, writable]` signer - Must equal `dvp.user_a` or `dvp.user_b`
/// 1. `[writable]` settlement_authority - Must equal `dvp.settlement_authority`; receives closed-account rent
/// 2. `[writable]` swap_dvp - SwapDvp PDA (signs CPIs, then closed)
/// 3. `[writable]` dvp_ata_a - Asset escrow (drained if funded, then closed)
/// 4. `[writable]` dvp_ata_b - Cash escrow (drained if funded, then closed)
/// 5. `[writable]` user_a_ata_a - user_a's ATA for mint_a; refund destination
/// 6. `[writable]` user_b_ata_b - user_b's ATA for mint_b; refund destination
/// 7. `[]` token_program
pub fn process_reject_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [signer_info, settlement_authority_info, swap_dvp_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_a_info, user_b_ata_b_info, token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(signer_info, true)?;
    verify_token_program(token_program_info)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    if signer_info.address() != &dvp.user_a && signer_info.address() != &dvp.user_b {
        return Err(ContraSwapProgramError::SignerNotParty.into());
    }

    if settlement_authority_info.address() != &dvp.settlement_authority {
        return Err(ContraSwapProgramError::SettlementAuthorityMismatch.into());
    }

    // Address-only validation: an unfunded leg's user ATA can be
    // uninitialized; the canonical pubkey is well-defined regardless.
    // Note: refund pairing — each user gets their *own* mint back (not
    // the cross used at Settle).
    // dvp_ata_a: DvP PDA's escrow for mint_a (asset escrow).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        &dvp.mint_a,
        token_program_info,
    )?;
    // dvp_ata_b: DvP PDA's escrow for mint_b (cash escrow).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        &dvp.mint_b,
        token_program_info,
    )?;
    // user_a_ata_a: seller's ATA for mint_a — refund destination if asset leg was funded.
    verify_canonical_ata(
        user_a_ata_a_info,
        &dvp.user_a,
        &dvp.mint_a,
        token_program_info,
    )?;
    // user_b_ata_b: buyer's ATA for mint_b — refund destination if cash leg was funded.
    verify_canonical_ata(
        user_b_ata_b_info,
        &dvp.user_b,
        &dvp.mint_b,
        token_program_info,
    )?;

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);
    let signer_seeds = [Signer::from(&swap_dvp_seeds)];

    // Refund each leg only if its escrow has a balance. Transfers the
    // actual balance, not `dvp.amount_x`, so an unfunded leg is a clean
    // skip rather than an InsufficientFunds failure.
    let leg_a_amount = TokenAccount::from_account_view(dvp_ata_a_info)?.amount();
    if leg_a_amount > 0 {
        Transfer {
            from: dvp_ata_a_info,
            to: user_a_ata_a_info,
            authority: swap_dvp_info,
            amount: leg_a_amount,
        }
        .invoke_signed(&signer_seeds)?;
    }

    let leg_b_amount = TokenAccount::from_account_view(dvp_ata_b_info)?.amount();
    if leg_b_amount > 0 {
        Transfer {
            from: dvp_ata_b_info,
            to: user_b_ata_b_info,
            authority: swap_dvp_info,
            amount: leg_b_amount,
        }
        .invoke_signed(&signer_seeds)?;
    }

    CloseAccount {
        account: dvp_ata_a_info,
        destination: settlement_authority_info,
        authority: swap_dvp_info,
    }
    .invoke_signed(&signer_seeds)?;

    CloseAccount {
        account: dvp_ata_b_info,
        destination: settlement_authority_info,
        authority: swap_dvp_info,
    }
    .invoke_signed(&signer_seeds)?;

    let authority_lamports = settlement_authority_info.lamports();
    settlement_authority_info.set_lamports(
        authority_lamports
            .checked_add(swap_dvp_info.lamports())
            .ok_or(ProgramError::ArithmeticOverflow)?,
    );
    swap_dvp_info.set_lamports(0);
    swap_dvp_info.close()?;

    Ok(())
}
