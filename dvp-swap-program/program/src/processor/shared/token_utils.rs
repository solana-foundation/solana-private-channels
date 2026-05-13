use core::mem::MaybeUninit;
use core::slice::from_raw_parts;

use pinocchio::{
    account::AccountView,
    address::Address,
    cpi::{invoke_signed_with_bounds, Signer},
    error::ProgramError,
    instruction::{InstructionAccount, InstructionView},
    ProgramResult,
};
use pinocchio_token::{state::Mint as TokenMint, state::TokenAccount, ID as TOKEN_PROGRAM_ID};
use pinocchio_token_2022::{
    state::Mint as Token2022Mint, state::TokenAccount as Token2022Account,
    ID as TOKEN_2022_PROGRAM_ID,
};
use spl_token_2022::extension::{
    confidential_transfer::ConfidentialTransferMint,
    confidential_transfer_fee::ConfidentialTransferFeeConfig,
    interest_bearing_mint::InterestBearingConfig, scaled_ui_amount::ScaledUiAmountConfig,
    transfer_fee::TransferFeeConfig, BaseStateWithExtensions, StateWithExtensions,
};
use spl_token_2022::state::Mint as Token2022MintState;

use crate::error::DvpSwapProgramError;

/// SPL Token / Token-2022 `TransferChecked` instruction discriminator.
const TRANSFER_CHECKED_DISCRIMINATOR: u8 = 12;

/// Verify that `ata_info` is the canonical Associated Token Account for
/// the given wallet/mint/token-program tuple. Address-only — does not
/// check initialization. Callers that need the ATA initialized rely on
/// the downstream Transfer/CloseAccount CPI to fail naturally on an
/// uninitialized account.
#[inline(always)]
pub fn verify_canonical_ata(
    ata_info: &AccountView,
    wallet: &Address,
    mint: &Address,
    token_program_info: &AccountView,
) -> Result<(), ProgramError> {
    let expected = Address::find_program_address(
        &[
            wallet.as_ref(),
            token_program_info.address().as_ref(),
            mint.as_ref(),
        ],
        &pinocchio_associated_token_account::ID,
    )
    .0;
    if ata_info.address() != &expected {
        return Err(ProgramError::InvalidSeeds);
    }
    Ok(())
}

/// Read the `amount` field of a token account owned by either legacy
/// SPL Token or Token-2022. The two share an identical first-165-byte
/// account layout, so the only branch is on the owner program ID.
#[inline(always)]
pub fn get_token_account_balance(info: &AccountView) -> Result<u64, ProgramError> {
    if info.owned_by(&TOKEN_PROGRAM_ID) {
        let data = info.try_borrow()?;
        let account = unsafe { TokenAccount::from_bytes_unchecked(&data) };
        return Ok(account.amount());
    }
    if info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        let data = info.try_borrow()?;
        let account = unsafe { Token2022Account::from_bytes_unchecked(&data) };
        return Ok(account.amount());
    }
    Err(ProgramError::InvalidAccountOwner)
}

/// Read the `decimals` field of a mint owned by either legacy SPL Token
/// or Token-2022. `TransferChecked` requires decimals on every call.
#[inline(always)]
pub fn get_mint_decimals(mint_info: &AccountView) -> Result<u8, ProgramError> {
    if mint_info.owned_by(&TOKEN_PROGRAM_ID) {
        let data = mint_info.try_borrow()?;
        let mint = unsafe { TokenMint::from_bytes_unchecked(&data) };
        return Ok(mint.decimals());
    }
    if mint_info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        let data = mint_info.try_borrow()?;
        let mint = unsafe { Token2022Mint::from_bytes_unchecked(&data) };
        return Ok(mint.decimals());
    }
    Err(ProgramError::InvalidAccountOwner)
}

/// Reject Token-2022 mints carrying any extension that breaks the
/// "escrow balance == sum of deposits" invariant the rest of the
/// processor relies on (settle's surplus-refund math, equality checks
/// against `dvp.amount_x`, etc.).
///
/// Blocked: `ConfidentialTransferMint`, `ConfidentialTransferFeeConfig`,
/// `TransferFeeConfig`, `InterestBearingConfig`, `ScaledUiAmountConfig`.
/// Everything else (Pausable, PermanentDelegate, NonTransferable,
/// MintCloseAuthority, MetadataPointer, GroupPointer, TransferHook, …)
/// is allowed. `TransferHook` is supported via `transfer_checked_cpi`:
/// the client resolves the hook's `ExtraAccountMetaList` off-chain and
/// passes the resulting accounts as trailing accounts to the
/// instruction, which forwards them through the `TransferChecked` CPI.
///
/// Called **only at CreateDvp**. Unwind paths (Settle/Cancel/Reject/
/// Reclaim) skip this check so funds remain recoverable even if a
/// blocked extension's parameters are activated post-Create.
///
/// Legacy SPL Token mints carry no extensions; this is a no-op for them.
#[inline(always)]
pub fn validate_mint_extensions(mint_info: &AccountView) -> ProgramResult {
    if !mint_info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        return Ok(());
    }

    let data = mint_info.try_borrow()?;
    let mint = StateWithExtensions::<Token2022MintState>::unpack(&data)
        .map_err(|_| DvpSwapProgramError::BlockedMintExtension)?;

    if mint.get_extension::<ConfidentialTransferMint>().is_ok()
        || mint
            .get_extension::<ConfidentialTransferFeeConfig>()
            .is_ok()
        || mint.get_extension::<TransferFeeConfig>().is_ok()
        || mint.get_extension::<InterestBearingConfig>().is_ok()
        || mint.get_extension::<ScaledUiAmountConfig>().is_ok()
    {
        return Err(DvpSwapProgramError::BlockedMintExtension.into());
    }

    Ok(())
}

/// Maximum number of hook extras (validation PDA, hook program ID, and
/// any accounts resolved from `ExtraAccountMetaList`) accepted per
/// `TransferChecked` CPI. Bounds the per-CPI stack frame; the call
/// returns `InvalidArgument` if the caller provides more.
pub const MAX_HOOK_REMAINING_ACCOUNTS: usize = 16;
const MAX_TRANSFER_CHECKED_ACCOUNTS: usize = 4 + MAX_HOOK_REMAINING_ACCOUNTS;

/// `TransferChecked` CPI on SPL Token or Token-2022 with a trailing
/// slice of accounts forwarded to the token program. The trailing
/// accounts are the transfer-hook extras (hook program, validation PDA,
/// and any accounts resolved from `ExtraAccountMetaList`); for legacy
/// SPL Token and hook-less Token-2022 mints, callers pass an empty
/// slice and the behaviour matches a plain 4-account `TransferChecked`.
///
/// The program does **not** validate the extras — Token-2022 itself
/// rejects the CPI if the supplied accounts don't satisfy the hook's
/// `ExtraAccountMetaList`. The client is responsible for resolving the
/// hook accounts off-chain and ordering them as the hook expects.
///
/// Wire-equivalent to `pinocchio_token_2022::instructions::TransferChecked`
/// (discriminator `12`, `amount: u64 LE`, `decimals: u8`) but built by
/// hand so the account list can carry hook extras.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub fn transfer_checked_cpi(
    from: &AccountView,
    mint: &AccountView,
    to: &AccountView,
    authority: &AccountView,
    amount: u64,
    decimals: u8,
    token_program: &Address,
    remaining: &[AccountView],
    signers: &[Signer],
) -> ProgramResult {
    if remaining.len() > MAX_HOOK_REMAINING_ACCOUNTS {
        return Err(ProgramError::InvalidArgument);
    }
    let total = 4 + remaining.len();

    // Account metas: 4 fixed + N trailing. Trailing forwards each
    // remaining account's writable/signer flags as-is so the hook
    // program receives them with the flags the client declared.
    const UNINIT_META: MaybeUninit<InstructionAccount> = MaybeUninit::uninit();
    let mut metas = [UNINIT_META; MAX_TRANSFER_CHECKED_ACCOUNTS];
    metas[0].write(InstructionAccount::writable(from.address()));
    metas[1].write(InstructionAccount::readonly(mint.address()));
    metas[2].write(InstructionAccount::writable(to.address()));
    metas[3].write(InstructionAccount::readonly_signer(authority.address()));
    for (i, acc) in remaining.iter().enumerate() {
        metas[4 + i].write(InstructionAccount::from(acc));
    }
    // SAFETY: the first `total` slots were just initialised above.
    let metas_slice: &[InstructionAccount] =
        unsafe { from_raw_parts(metas.as_ptr() as *const InstructionAccount, total) };

    let mut data = [0u8; 10];
    data[0] = TRANSFER_CHECKED_DISCRIMINATOR;
    data[1..9].copy_from_slice(&amount.to_le_bytes());
    data[9] = decimals;

    let instruction = InstructionView {
        program_id: token_program,
        accounts: metas_slice,
        data: &data,
    };

    // Account infos: same shape — 4 fixed + N trailing. `&AccountView`
    // is `Copy`, so we initialise the whole array with `from` as a
    // placeholder and overwrite the prefix; `invoke_signed_with_bounds`
    // only reads the first `instruction.accounts.len()` entries.
    let mut infos: [&AccountView; MAX_TRANSFER_CHECKED_ACCOUNTS] =
        [from; MAX_TRANSFER_CHECKED_ACCOUNTS];
    infos[1] = mint;
    infos[2] = to;
    infos[3] = authority;
    for (i, acc) in remaining.iter().enumerate() {
        infos[4 + i] = acc;
    }

    invoke_signed_with_bounds::<MAX_TRANSFER_CHECKED_ACCOUNTS>(
        &instruction,
        &infos[..total],
        signers,
    )
}
