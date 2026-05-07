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
use pinocchio_token::{
    instructions::{CloseAccount, Transfer},
    state::TokenAccount,
};

/// Processes the SettleDvp instruction.
///
/// Atomically delivers the asset leg to `user_b` (buyer) and the cash
/// leg to `user_a` (seller), then closes the SwapDvp PDA and both
/// escrow ATAs. Rent lamports from the three closed accounts are
/// swept to the settlement authority.
///
/// Each leg transfers exactly `dvp.amount_x` to the counterparty. Any
/// surplus held in the escrow above the leg amount (e.g. an over-deposit
/// via raw SPL Transfer — the canonical funding path) is refunded to
/// the leg's depositor on their own mint before the escrow is closed.
/// This ensures the counterparty receives exactly the agreed amount and
/// cannot capture an over-deposit.
///
/// # Account Layout
/// 0. `[signer, writable]` settlement_authority - Must equal `dvp.settlement_authority`; receives closed-account rent
/// 1. `[writable]` swap_dvp - SwapDvp PDA (signs CPIs as authority, then closed)
/// 2. `[writable]` dvp_ata_a - Escrow for the asset leg (drained, then closed)
/// 3. `[writable]` dvp_ata_b - Escrow for the cash leg (drained, then closed)
/// 4. `[writable]` user_a_ata_b - user_a's ATA for mint_b; receives the cash leg
/// 5. `[writable]` user_b_ata_a - user_b's ATA for mint_a; receives the asset leg
/// 6. `[writable]` user_a_ata_a - user_a's ATA for mint_a; receives any asset-leg surplus refund
/// 7. `[writable]` user_b_ata_b - user_b's ATA for mint_b; receives any cash-leg surplus refund
/// 8. `[]` token_program
pub fn process_settle_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    let [settlement_authority_info, swap_dvp_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_b_info, user_b_ata_a_info, user_a_ata_a_info, user_b_ata_b_info, token_program_info] =
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

    let now = Clock::get()?.unix_timestamp;
    if now > dvp.expiry_timestamp {
        return Err(ContraSwapProgramError::DvpExpired.into());
    }
    if let Some(earliest) = dvp.earliest_settlement_timestamp {
        if now < earliest {
            return Err(ContraSwapProgramError::SettlementTooEarly.into());
        }
    }

    // All six ATAs must be canonical. The mint and user pubkeys come
    // from state; the swap_dvp address is the on-chain account. Note
    // the cross: each user receives the *other* leg's mint, so user_a
    // pairs with mint_b and user_b pairs with mint_a. The surplus refund
    // ATAs follow the Cancel/Reject pairing (each user gets their own
    // mint back).
    // dvp_ata_a: DvP PDA's escrow for mint_a (asset, drained to user_b).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        &dvp.mint_a,
        token_program_info,
    )?;
    // dvp_ata_b: DvP PDA's escrow for mint_b (cash, drained to user_a).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        &dvp.mint_b,
        token_program_info,
    )?;
    // user_a_ata_b: seller's ATA for mint_b — receives the cash leg.
    verify_canonical_ata(
        user_a_ata_b_info,
        &dvp.user_a,
        &dvp.mint_b,
        token_program_info,
    )?;
    // user_b_ata_a: buyer's ATA for mint_a — receives the asset leg.
    verify_canonical_ata(
        user_b_ata_a_info,
        &dvp.user_b,
        &dvp.mint_a,
        token_program_info,
    )?;
    // user_a_ata_a: seller's ATA for mint_a — surplus refund destination
    // for the asset leg. Address-only validation: only touched if
    // surplus > 0, in which case the Transfer CPI fails naturally on an
    // uninitialized destination.
    verify_canonical_ata(
        user_a_ata_a_info,
        &dvp.user_a,
        &dvp.mint_a,
        token_program_info,
    )?;
    // user_b_ata_b: buyer's ATA for mint_b — surplus refund destination
    // for the cash leg. Same address-only treatment as above.
    verify_canonical_ata(
        user_b_ata_b_info,
        &dvp.user_b,
        &dvp.mint_b,
        token_program_info,
    )?;

    // Both legs must hold *at least* their target amount. Any balance
    // above the target is treated as an over-deposit by the leg's
    // depositor and refunded to them below, so the counterparty always
    // receives exactly the agreed `dvp.amount_x`.
    let escrow_a_balance = TokenAccount::from_account_view(dvp_ata_a_info)?.amount();
    if escrow_a_balance < dvp.amount_a {
        return Err(ContraSwapProgramError::LegNotFunded.into());
    }
    let escrow_b_balance = TokenAccount::from_account_view(dvp_ata_b_info)?.amount();
    if escrow_b_balance < dvp.amount_b {
        return Err(ContraSwapProgramError::LegNotFunded.into());
    }

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);
    let signer_seeds = [Signer::from(&swap_dvp_seeds)];

    // Cash to seller, then asset to buyer. Order is irrelevant for
    // correctness (the whole tx is atomic); spec lists cash first.
    Transfer {
        from: dvp_ata_b_info,
        to: user_a_ata_b_info,
        authority: swap_dvp_info,
        amount: dvp.amount_b,
    }
    .invoke_signed(&signer_seeds)?;

    Transfer {
        from: dvp_ata_a_info,
        to: user_b_ata_a_info,
        authority: swap_dvp_info,
        amount: dvp.amount_a,
    }
    .invoke_signed(&signer_seeds)?;

    // Refund any surplus to each leg's depositor so the escrows close
    // empty and over-deposits don't leak to the counterparty.
    let asset_surplus = escrow_a_balance - dvp.amount_a;
    if asset_surplus > 0 {
        Transfer {
            from: dvp_ata_a_info,
            to: user_a_ata_a_info,
            authority: swap_dvp_info,
            amount: asset_surplus,
        }
        .invoke_signed(&signer_seeds)?;
    }

    let cash_surplus = escrow_b_balance - dvp.amount_b;
    if cash_surplus > 0 {
        Transfer {
            from: dvp_ata_b_info,
            to: user_b_ata_b_info,
            authority: swap_dvp_info,
            amount: cash_surplus,
        }
        .invoke_signed(&signer_seeds)?;
    }

    // Close both escrow ATAs; rent lamports go to settlement_authority.
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

    // Close the SwapDvp PDA. There's no SPL-style CloseAccount for a
    // program-owned non-token account: drain its lamports manually,
    // then `close()` zeroes the data and reassigns to the system program.
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
