use crate::{
    error::DvpSwapProgramError,
    processor::shared::account_check::{verify_account_owner, verify_signer},
    processor::shared::token_utils::{
        get_mint_decimals, get_token_account_balance, transfer_checked_cpi, verify_canonical_ata,
    },
    require,
    state::swap_dvp::SwapDvp,
};
use pinocchio::{
    account::AccountView, address::Address, cpi::Signer, error::ProgramError, ProgramResult,
};

/// Length of the fixed account prefix; anything beyond this is treated
/// as transfer-hook remaining accounts for the single refund CPI.
const FIXED_ACCOUNTS_LEN: usize = 7;

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
/// the leg later (pre-expiry, to still settle), or either party can
/// abort the trade. Reclaim only drains a single leg, so it only takes
/// the mint and token program for that leg (not both).
///
/// Extension validation is **not** performed here — Create is the
/// consent point. Reclaim must remain available even if a mint's
/// extension parameters change post-Create so depositors can always
/// recover their funds.
///
/// # Account Layout
/// 0. `[signer]`    signer          - Must equal `dvp.user_a` or `dvp.user_b`
/// 1. `[]`          swap_dvp        - SwapDvp PDA (signs the transfer as authority)
/// 2. `[]`          mint            - Mint of the leg being reclaimed; must equal `dvp.mint_a` or `dvp.mint_b`
/// 3. `[writable]`  dvp_source_ata  - DvP's escrow ATA for the leg's mint
/// 4. `[writable]`  signer_dest_ata - Signer's canonical ATA for the leg's mint (caller must pre-initialize if the leg has a non-zero balance)
/// 5. `[]`          token_program   - SPL Token or Token-2022; must own `mint`
/// 6. `[]`          memo_program    - SPL Memo program; only used if signer_dest_ata requires a memo
///
/// Trailing accounts (variable): transfer-hook extras forwarded to the
/// refund `TransferChecked` CPI (hook program, validation PDA, and any
/// accounts resolved from `ExtraAccountMetaList`). Empty for legacy SPL
/// Token and hook-less Token-2022 mints.
pub fn process_reclaim_dvp(
    program_id: &Address,
    accounts: &[AccountView],
    _instruction_data: &[u8],
) -> ProgramResult {
    require!(
        accounts.len() >= FIXED_ACCOUNTS_LEN,
        ProgramError::NotEnoughAccountKeys
    );
    let (fixed, remaining) = accounts.split_at(FIXED_ACCOUNTS_LEN);
    let [signer_info, swap_dvp_info, mint_info, dvp_source_ata_info, signer_dest_ata_info, token_program_info, memo_program_info] =
        fixed
    else {
        return Err(ProgramError::NotEnoughAccountKeys);
    };

    verify_signer(signer_info, false)?;
    verify_account_owner(swap_dvp_info, program_id)?;

    let dvp = {
        let data = swap_dvp_info.try_borrow()?;
        SwapDvp::try_from_bytes(&data)?
    };

    // Leg selection by signer identity.
    let (leg_mint, leg_token_program) = if signer_info.address() == &dvp.user_a {
        (&dvp.mint_a, &dvp.token_program_a)
    } else if signer_info.address() == &dvp.user_b {
        (&dvp.mint_b, &dvp.token_program_b)
    } else {
        return Err(DvpSwapProgramError::SignerNotParty.into());
    };

    // Bind the passed mint and token program to state.
    require!(
        mint_info.address() == leg_mint,
        ProgramError::InvalidAccountData
    );
    require!(
        token_program_info.address() == leg_token_program,
        ProgramError::IncorrectProgramId
    );

    // No expiry gate: per-leg recovery stays open at all times. Safe
    // because Settle is gated on `now <= expiry` and both legs funded.

    // Both ATAs must be canonical for the leg's token program.
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
    let escrow_balance = get_token_account_balance(dvp_source_ata_info)?;
    if escrow_balance == 0 {
        return Ok(());
    }

    let (nonce_bytes, bump_bytes) = dvp.seed_buffers();
    let swap_dvp_seeds = dvp.signing_seeds(&nonce_bytes, &bump_bytes);

    transfer_checked_cpi(
        dvp_source_ata_info,
        mint_info,
        signer_dest_ata_info,
        swap_dvp_info,
        escrow_balance,
        get_mint_decimals(mint_info)?,
        token_program_info.address(),
        memo_program_info,
        remaining,
        &[Signer::from(&swap_dvp_seeds)],
    )?;

    Ok(())
}
