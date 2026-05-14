use crate::{
    error::DvpSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer, verify_token_program},
    processor::shared::token_utils::{
        get_mint_decimals, get_token_account_balance, transfer_checked_cpi, verify_canonical_ata,
    },
    processor::shared::utils::split_leg_remaining_accounts,
    require,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{
    account::AccountView, address::Address, cpi::Signer, error::ProgramError, ProgramResult,
};
use pinocchio_token_2022::instructions::CloseAccount;

/// Length of the fixed account prefix; anything beyond this is treated
/// as transfer-hook remaining accounts split between the two legs.
const FIXED_ACCOUNTS_LEN: usize = 10;

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
/// Extension validation is **not** performed here — Create is the
/// consent point. Cancel must remain available even if a mint's
/// extension parameters change post-Create so funds are never stranded.
///
/// # Account Layout
/// 0. `[signer, writable]` settlement_authority - Must equal `dvp.settlement_authority`; receives closed-account rent
/// 1. `[writable]` swap_dvp - SwapDvp PDA (signs CPIs, then closed)
/// 2. `[]` mint_a - Must equal `dvp.mint_a`
/// 3. `[]` mint_b - Must equal `dvp.mint_b`
/// 4. `[writable]` dvp_ata_a - Asset escrow (drained if funded, then closed)
/// 5. `[writable]` dvp_ata_b - Cash escrow (drained if funded, then closed)
/// 6. `[writable]` user_a_ata_a - user_a's ATA for mint_a; refund destination (caller must pre-initialize if leg A is funded)
/// 7. `[writable]` user_b_ata_b - user_b's ATA for mint_b; refund destination (caller must pre-initialize if leg B is funded)
/// 8. `[]` token_program_a - SPL Token or Token-2022; must own mint_a
/// 9. `[]` token_program_b - SPL Token or Token-2022; must own mint_b
///
/// Trailing accounts (variable):
/// - First `leg_a_extras_count` go to leg A's refund `TransferChecked`
///   CPI (only consumed if leg A was funded).
/// - The rest go to leg B's refund `TransferChecked` CPI (only consumed
///   if leg B was funded).
///
/// # Instruction Data
/// * `leg_a_extras_count` (u8) - Split point between leg A and leg B
///   trailing accounts.
pub fn process_cancel_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let (fixed, leg_a_extras, leg_b_extras) =
        split_leg_remaining_accounts(accounts, instruction_data, FIXED_ACCOUNTS_LEN)?;
    let [settlement_authority_info, swap_dvp_info, mint_a_info, mint_b_info, dvp_ata_a_info, dvp_ata_b_info, user_a_ata_a_info, user_b_ata_b_info, token_program_a_info, token_program_b_info] =
        fixed
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(settlement_authority_info, true)?;
    verify_token_program(token_program_a_info)?;
    verify_token_program(token_program_b_info)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    require!(
        settlement_authority_info.address() == &dvp.settlement_authority,
        DvpSwapProgramError::SettlementAuthorityMismatch
    );

    require!(
        mint_a_info.address() == &dvp.mint_a && mint_b_info.address() == &dvp.mint_b,
        ProgramError::InvalidAccountData
    );
    verify_account_owner(mint_a_info, token_program_a_info.address())?;
    verify_account_owner(mint_b_info, token_program_b_info.address())?;

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
            leg_b_extras,
            &signer_seeds,
        )?;
    }

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
