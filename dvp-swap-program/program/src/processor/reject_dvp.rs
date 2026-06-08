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

/// Processes the RejectDvp instruction.
///
/// Permissioned to either depositor: user_a or user_b can pull the
/// plug on the trade entirely, refunding any funded legs to their
/// respective depositors and closing the SwapDvp + both escrow ATAs.
/// No expiry check — Reject must work post-expiry too.
///
/// Closed-account rent goes to `signer`, not `dvp.settlement_authority`.
/// Reject is the safety valve and must work even if the configured
/// settlement authority is unreachable (e.g. a sysvar the runtime
/// refuses to pass as writable). Settle and Cancel still
/// pay rent to the settlement authority — it's their signer there, so
/// it's already required to be usable.
///
/// Extension validation is **not** performed here — Create is the
/// consent point. Reject must remain available even if a mint's
/// extension parameters change post-Create so funds are never stranded.
///
/// # Account Layout
/// 0. `[signer, writable]` signer - Must equal `dvp.user_a` or `dvp.user_b`; receives closed-account rent
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
pub fn process_reject_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    instruction_data: &[u8],
) -> ProgramResult {
    let (fixed, leg_a_extras, leg_b_extras) =
        split_leg_remaining_accounts(accounts, instruction_data, FIXED_ACCOUNTS_LEN)?;
    let [signer_info, swap_dvp_info, ..] = fixed else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(signer_info, true)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    require!(
        signer_info.address() == &dvp.user_a || signer_info.address() == &dvp.user_b,
        DvpSwapProgramError::SignerNotParty
    );

    // Rent is swept to `fixed[0]` (the signer here), not the settlement
    // authority — see the doc comment above for why.
    refund_and_close_dvp(fixed, &dvp, leg_a_extras, leg_b_extras)
}
