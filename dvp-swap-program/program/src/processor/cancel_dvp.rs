use crate::{
    error::DvpSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer},
    processor::shared::refund::refund_and_close_dvp,
    processor::shared::utils::split_leg_remaining_accounts,
    require,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{account::AccountView, address::Address, error::ProgramError, ProgramResult};

/// Length of the fixed account prefix; anything beyond this is treated
/// as transfer-hook remaining accounts split between the two legs.
const FIXED_ACCOUNTS_LEN: usize = 11;

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
/// 10. `[]` memo_program - SPL Memo program; only used for destinations that require a memo
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
    let [settlement_authority_info, swap_dvp_info, ..] = fixed else {
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

    refund_and_close_dvp(fixed, &dvp, leg_a_extras, leg_b_extras)
}
