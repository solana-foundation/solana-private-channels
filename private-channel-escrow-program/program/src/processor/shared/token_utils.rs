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
use pinocchio_associated_token_account::instructions::CreateIdempotent;
use pinocchio_token::{
    state::{Mint as TokenMint, TokenAccount},
    ID as TOKEN_PROGRAM_ID,
};
use pinocchio_token_2022::{
    state::Mint as Token2022Mint, state::TokenAccount as Token2022Account,
    ID as TOKEN_2022_PROGRAM_ID,
};
use spl_token_2022::extension::StateWithExtensions;
use spl_token_2022::state::Mint as Token2022MintState;

use crate::error::PrivateChannelEscrowProgramError;

#[inline(always)]
pub fn validate_ata(
    ata_info: &AccountView,
    wallet_key: &Address,
    mint_info: &AccountView,
    token_program_info: &AccountView,
) -> ProgramResult {
    // Validate ATA address is correct for this wallet + mint
    let expected_ata = Address::find_program_address(
        &[
            wallet_key.as_ref(),
            token_program_info.address().as_ref(),
            mint_info.address().as_ref(),
        ],
        &pinocchio_associated_token_account::ID,
    )
    .0;

    if ata_info.address() != &expected_ata || ata_info.is_data_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    Ok(())
}

#[inline(always)]
pub fn get_or_create_ata(
    ata_info: &AccountView,
    wallet_info: &AccountView,
    mint_info: &AccountView,
    payer_info: &AccountView,
    system_program_info: &AccountView,
    token_program_info: &AccountView,
) -> ProgramResult {
    // Validate ATA address is correct for this wallet + mint
    let expected_ata = Address::find_program_address(
        &[
            wallet_info.address().as_ref(),
            token_program_info.address().as_ref(),
            mint_info.address().as_ref(),
        ],
        &pinocchio_associated_token_account::ID,
    )
    .0;

    if ata_info.address() != &expected_ata {
        return Err(PrivateChannelEscrowProgramError::InvalidAta.into());
    }

    // Create ATA if it doesn't exist
    if ata_info.is_data_empty() {
        CreateIdempotent {
            funding_account: payer_info,
            account: ata_info,
            wallet: wallet_info,
            mint: mint_info,
            system_program: system_program_info,
            token_program: token_program_info,
        }
        .invoke()?;
    }

    Ok(())
}

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
    Err(PrivateChannelEscrowProgramError::InvalidTokenAccount.into())
}

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
    Err(PrivateChannelEscrowProgramError::InvalidMint.into())
}

/// Validates the account really is a mint. The decimals read above is
/// unchecked, so this is what proves the bytes behind it.
///
/// No extension is rejected. Pause and permanent delegate are handled
/// off-chain by the operator's withdrawal pre-flight, and `TransferHook`
/// mints transfer through [`transfer_checked_cpi`] with client-resolved
/// extras. Called at AllowMint, where decimals are pinned into state with
/// no transfer CPI behind them; the transfer paths rely on
/// `TransferChecked`, which re-validates the mint against both ATAs.
#[inline(always)]
pub fn validate_mint(mint_info: &AccountView) -> ProgramResult {
    let data = mint_info.try_borrow()?;

    if mint_info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        StateWithExtensions::<Token2022MintState>::unpack(&data)
            .map_err(|_| PrivateChannelEscrowProgramError::InvalidMint)?;
        return Ok(());
    }

    // Legacy mints carry no extensions, so the exact size separates one
    // from a token account (165) or a multisig (355).
    if data.len() != TokenMint::LEN {
        return Err(PrivateChannelEscrowProgramError::InvalidMint.into());
    }

    Ok(())
}

/// Max transfer-hook accounts per `TransferChecked` CPI: hook program,
/// validation PDA, and whatever the mint's `ExtraAccountMetaList` resolves
/// to. Bounded because the metas array below is stack-allocated under a
/// const generic. Well above what real hooks declare, and under the
/// 64-account transaction ceiling that v1 leaves unchanged.
pub const MAX_HOOK_REMAINING_ACCOUNTS: usize = 32;
const MAX_TRANSFER_CHECKED_ACCOUNTS: usize = 4 + MAX_HOOK_REMAINING_ACCOUNTS;

/// SPL Token / Token-2022 `TransferChecked` discriminator.
const TRANSFER_CHECKED_DISCRIMINATOR: u8 = 12;

/// `TransferChecked` CPI carrying a trailing slice of transfer-hook extras.
/// Hand-built because the pinocchio builder's account list is fixed at 4;
/// an empty slice behaves like a plain `TransferChecked`.
///
/// The extras are not validated here. Token-2022 resolves the mint's
/// `ExtraAccountMetaList` itself and rejects the CPI if they do not satisfy
/// it, so resolving them is the client's job.
///
/// Each extra keeps its writable flag but never its signer bit, so a hostile
/// `ExtraAccountMetaList` naming the payer or user cannot borrow their
/// signature. The authority is passed as a signer and so not covered by the
/// strip; `spl-tlv-account-resolution` clamps resolved extras to the
/// privileges the address holds in the hook's `Execute`, where it is readonly.
/// A mint that genuinely needs a signer extra is untransferable here.
///
/// Ported from <https://github.com/solana-foundation/dvp>.
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
    hook_extras: &[AccountView],
    signers: &[Signer],
) -> ProgramResult {
    if hook_extras.len() > MAX_HOOK_REMAINING_ACCOUNTS {
        return Err(ProgramError::InvalidArgument);
    }
    let total = 4 + hook_extras.len();

    const UNINIT_META: MaybeUninit<InstructionAccount> = MaybeUninit::uninit();
    let mut metas = [UNINIT_META; MAX_TRANSFER_CHECKED_ACCOUNTS];
    metas[0].write(InstructionAccount::writable(from.address()));
    metas[1].write(InstructionAccount::readonly(mint.address()));
    metas[2].write(InstructionAccount::writable(to.address()));
    metas[3].write(InstructionAccount::readonly_signer(authority.address()));
    for (index, account) in hook_extras.iter().enumerate() {
        let meta = if account.is_writable() {
            InstructionAccount::writable(account.address())
        } else {
            InstructionAccount::readonly(account.address())
        };
        metas[4 + index].write(meta);
    }
    // SAFETY: the first `total` slots were just initialized above.
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

    // `&AccountView` is Copy: fill with `from`, overwrite the prefix, and
    // only the first `total` entries are read.
    let mut infos: [&AccountView; MAX_TRANSFER_CHECKED_ACCOUNTS] =
        [from; MAX_TRANSFER_CHECKED_ACCOUNTS];
    infos[1] = mint;
    infos[2] = to;
    infos[3] = authority;
    for (index, account) in hook_extras.iter().enumerate() {
        infos[4 + index] = account;
    }

    invoke_signed_with_bounds::<MAX_TRANSFER_CHECKED_ACCOUNTS>(
        &instruction,
        &infos[..total],
        signers,
    )
}
