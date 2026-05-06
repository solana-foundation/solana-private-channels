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

/// Processes the CancelDvp instruction.
///
/// Permissioned to the settlement authority. Refunds each funded leg
/// to its depositor and closes the SwapDvp PDA + both escrow ATAs.
/// No expiry check — Cancel must work after expiry too, otherwise an
/// expired-but-funded DvP would strand funds.
///
/// Drains the escrow's actual balance (0 if the leg was never funded).
/// Transfer is skipped on a 0 balance; CloseAccount accepts an empty
/// account.
///
/// # Account Layout
/// 0. `[signer, writable]` settlement_authority - Must equal `dvp.settlement_authority`; receives closed-account rent
/// 1. `[writable]` swap_dvp - SwapDvp PDA (signs CPIs, then closed)
/// 2. `[writable]` dvp_ata_a - Asset escrow (drained if funded, then closed)
/// 3. `[writable]` dvp_ata_b - Cash escrow (drained if funded, then closed)
/// 4. `[writable]` user_a_ata_a - user_a's ATA for mint_a; refund destination
/// 5. `[writable]` user_b_ata_b - user_b's ATA for mint_b; refund destination
/// 6. `[]` token_program
pub fn process_cancel_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [settlement_authority_info, swap_dvp_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_a_info, user_b_ata_b_info, token_program_info] =
        accounts
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(settlement_authority_info, true)?;
    verify_token_program(token_program_info)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    if settlement_authority_info.address() != &dvp.settlement_authority {
        return Err(ContraSwapProgramError::SettlementAuthorityMismatch.into());
    }

    // Address validation only — initialization state of the user ATAs
    // is not checked here. If a leg is unfunded the ATA can be
    // uninitialized (caller still passes the canonical pubkey).
    // If the leg is funded the Transfer below will fail naturally on
    // an uninitialized destination. Note: refund pairing — each user
    // gets their *own* mint back (not the cross used at Settle).
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

    // Refund each leg if and only if its escrow has a balance. We
    // transfer the actual balance rather than `dvp.amount_x` so that
    // an unfunded leg is a clean skip rather than an InsufficientFunds
    // failure.
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
