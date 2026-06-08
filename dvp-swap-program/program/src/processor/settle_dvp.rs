use crate::{
    error::DvpSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer},
    processor::shared::token_utils::{
        get_mint_decimals, get_token_account_balance, transfer_checked_cpi, verify_canonical_ata,
    },
    processor::shared::utils::split_leg_remaining_accounts,
    require,
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
use pinocchio_token_2022::instructions::CloseAccount;

/// Length of the fixed account prefix; anything beyond this is treated
/// as transfer-hook remaining accounts split between the two legs.
const FIXED_ACCOUNTS_LEN: usize = 13;

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
/// Extension validation is **not** performed here — Create is the
/// consent point. Settle must remain available even if a mint's
/// extension parameters change post-Create so the trade can always
/// reach its terminal state.
///
/// # Account Layout
/// 0.  `[signer, writable]` settlement_authority - Must equal `dvp.settlement_authority`; receives closed-account rent
/// 1.  `[writable]` swap_dvp - SwapDvp PDA (signs CPIs as authority, then closed)
/// 2.  `[]` mint_a - Must equal `dvp.mint_a`
/// 3.  `[]` mint_b - Must equal `dvp.mint_b`
/// 4.  `[writable]` dvp_ata_a - Escrow for the asset leg (drained, then closed)
/// 5.  `[writable]` dvp_ata_b - Escrow for the cash leg (drained, then closed)
/// 6.  `[writable]` user_a_ata_b - user_a's ATA for mint_b; receives the cash leg (caller must pre-initialize)
/// 7.  `[writable]` user_b_ata_a - user_b's ATA for mint_a; receives the asset leg (caller must pre-initialize)
/// 8.  `[writable]` user_a_ata_a - user_a's ATA for mint_a; receives any asset-leg surplus refund. Required: anyone can dust the escrow, so a surplus refund can always fire — a missing ATA reverts the whole Settle. Pre-initialize it.
/// 9.  `[writable]` user_b_ata_b - user_b's ATA for mint_b; receives any cash-leg surplus refund. Required: same as user_a_ata_a — pre-initialize it.
/// 10. `[]` token_program_a - SPL Token or Token-2022; must own mint_a
/// 11. `[]` token_program_b - SPL Token or Token-2022; must own mint_b
/// 12. `[]` memo_program - SPL Memo program; only used for destinations that require a memo
///
/// Trailing accounts (variable):
/// - First `leg_a_extras_count` go to leg A's `TransferChecked` CPI
///   (hook program, validation PDA, and any accounts resolved from
///   `ExtraAccountMetaList`).
/// - The rest go to leg B's `TransferChecked` CPI.
///
/// # Instruction Data
/// * `leg_a_extras_count` (u8) - Split point between leg A and leg B
///   trailing accounts.
pub fn process_settle_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let (fixed, leg_a_extras, leg_b_extras) =
        split_leg_remaining_accounts(accounts, instruction_data, FIXED_ACCOUNTS_LEN)?;
    let [settlement_authority_info, swap_dvp_info, mint_a_info, mint_b_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_b_info, user_b_ata_a_info, user_a_ata_a_info, user_b_ata_b_info, token_program_a_info, token_program_b_info, memo_program_info] =
        fixed
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(settlement_authority_info, true)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    require!(
        settlement_authority_info.address() == &dvp.settlement_authority,
        DvpSwapProgramError::SettlementAuthorityMismatch
    );

    // Bind mints and token programs to state.
    require!(
        mint_a_info.address() == &dvp.mint_a && mint_b_info.address() == &dvp.mint_b,
        ProgramError::InvalidAccountData
    );
    require!(
        token_program_a_info.address() == &dvp.token_program_a
            && token_program_b_info.address() == &dvp.token_program_b,
        ProgramError::IncorrectProgramId
    );

    let now = Clock::get()?.unix_timestamp;
    require!(now <= dvp.expiry_timestamp, DvpSwapProgramError::DvpExpired);
    if let Some(earliest) = dvp.earliest_settlement_timestamp {
        require!(now >= earliest, DvpSwapProgramError::SettlementTooEarly);
    }

    // All six ATAs must be canonical. Note the cross at Settle: each user
    // receives the *other* leg's mint, so user_a pairs with mint_b and
    // user_b pairs with mint_a. The surplus refund ATAs follow the
    // Cancel/Reject pairing (each user gets their own mint back).
    // dvp_ata_a: DvP PDA's escrow for mint_a (asset, drained to user_b).
    verify_canonical_ata(
        dvp_ata_a_info,
        swap_dvp_info.address(),
        &dvp.mint_a,
        token_program_a_info,
    )?;
    // dvp_ata_b: DvP PDA's escrow for mint_b (cash, drained to user_a).
    verify_canonical_ata(
        dvp_ata_b_info,
        swap_dvp_info.address(),
        &dvp.mint_b,
        token_program_b_info,
    )?;
    // user_a_ata_b: seller's ATA for mint_b — receives the cash leg.
    verify_canonical_ata(
        user_a_ata_b_info,
        &dvp.user_a,
        &dvp.mint_b,
        token_program_b_info,
    )?;
    // user_b_ata_a: buyer's ATA for mint_a — receives the asset leg.
    verify_canonical_ata(
        user_b_ata_a_info,
        &dvp.user_b,
        &dvp.mint_a,
        token_program_a_info,
    )?;
    // user_a_ata_a: seller's ATA for mint_a — surplus refund destination
    // for the asset leg. Address-only validation: only touched if
    // surplus > 0, in which case the Transfer CPI fails naturally on an
    // uninitialized destination.
    verify_canonical_ata(
        user_a_ata_a_info,
        &dvp.user_a,
        &dvp.mint_a,
        token_program_a_info,
    )?;
    // user_b_ata_b: buyer's ATA for mint_b — surplus refund destination
    // for the cash leg. Same address-only treatment as above.
    verify_canonical_ata(
        user_b_ata_b_info,
        &dvp.user_b,
        &dvp.mint_b,
        token_program_b_info,
    )?;

    // Both legs must hold *at least* their target amount. Any balance
    // above the target is treated as an over-deposit by the leg's
    // depositor and refunded to them below, so the counterparty always
    // receives exactly the agreed `dvp.amount_x`.
    let escrow_a_balance = get_token_account_balance(dvp_ata_a_info)?;
    require!(
        escrow_a_balance >= dvp.amount_a,
        DvpSwapProgramError::LegNotFunded
    );
    let escrow_b_balance = get_token_account_balance(dvp_ata_b_info)?;
    require!(
        escrow_b_balance >= dvp.amount_b,
        DvpSwapProgramError::LegNotFunded
    );

    let decimals_a = get_mint_decimals(mint_a_info)?;
    let decimals_b = get_mint_decimals(mint_b_info)?;

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);
    let signer_seeds = [Signer::from(&swap_dvp_seeds)];

    // Cash to seller, then asset to buyer. Order is irrelevant for
    // correctness (the whole tx is atomic); spec lists cash first.
    transfer_checked_cpi(
        dvp_ata_b_info,
        mint_b_info,
        user_a_ata_b_info,
        swap_dvp_info,
        dvp.amount_b,
        decimals_b,
        token_program_b_info.address(),
        memo_program_info,
        leg_b_extras,
        &signer_seeds,
    )?;

    transfer_checked_cpi(
        dvp_ata_a_info,
        mint_a_info,
        user_b_ata_a_info,
        swap_dvp_info,
        dvp.amount_a,
        decimals_a,
        token_program_a_info.address(),
        memo_program_info,
        leg_a_extras,
        &signer_seeds,
    )?;

    // Refund any surplus to each leg's depositor so the escrows close
    // empty and over-deposits don't leak to the counterparty.
    //
    // The surplus CPI reuses the leg's hook extras but transfers to a
    // different destination and amount than the delivery. Hooks whose
    // ExtraAccountMetaList resolves accounts from the transfer destination
    // or amount (rare but spec-valid) will reject this CPI and revert the
    // whole Settle. Recovery: depositor calls Reclaim, re-funds exactly
    // amount_x, retries Settle. See README "TransferHook + over-deposit
    // caveat".
    let asset_surplus = escrow_a_balance - dvp.amount_a;
    if asset_surplus > 0 {
        transfer_checked_cpi(
            dvp_ata_a_info,
            mint_a_info,
            user_a_ata_a_info,
            swap_dvp_info,
            asset_surplus,
            decimals_a,
            token_program_a_info.address(),
            memo_program_info,
            leg_a_extras,
            &signer_seeds,
        )?;
    }

    let cash_surplus = escrow_b_balance - dvp.amount_b;
    if cash_surplus > 0 {
        transfer_checked_cpi(
            dvp_ata_b_info,
            mint_b_info,
            user_b_ata_b_info,
            swap_dvp_info,
            cash_surplus,
            decimals_b,
            token_program_b_info.address(),
            memo_program_info,
            leg_b_extras,
            &signer_seeds,
        )?;
    }

    // Close both escrow ATAs; rent lamports go to settlement_authority.
    CloseAccount {
        account: dvp_ata_a_info,
        destination: settlement_authority_info,
        authority: swap_dvp_info,
        token_program: token_program_a_info.address(),
    }
    .invoke_signed(&signer_seeds)?;

    CloseAccount {
        account: dvp_ata_b_info,
        destination: settlement_authority_info,
        authority: swap_dvp_info,
        token_program: token_program_b_info.address(),
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
