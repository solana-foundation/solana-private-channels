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
    instructions::SyncNative, state::Mint as Token2022Mint,
    state::TokenAccount as Token2022Account, ID as TOKEN_2022_PROGRAM_ID,
};
use spl_token_2022::extension::{
    confidential_transfer::ConfidentialTransferMint,
    confidential_transfer_fee::ConfidentialTransferFeeConfig,
    interest_bearing_mint::InterestBearingConfig, memo_transfer::memo_required,
    non_transferable::NonTransferable, scaled_ui_amount::ScaledUiAmountConfig,
    transfer_fee::TransferFeeConfig, BaseStateWithExtensions, StateWithExtensions,
};
use spl_token_2022::state::{Account as Token2022AccountState, Mint as Token2022MintState};

use crate::{error::DvpSwapProgramError, require};

/// SPL Token / Token-2022 `TransferChecked` instruction discriminator.
const TRANSFER_CHECKED_DISCRIMINATOR: u8 = 12;

/// SPL Memo program.
const MEMO_PROGRAM_ID: Address =
    Address::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

/// Memo emitted before a memo-required transfer. Token-2022 only checks
/// the preceding sibling is the Memo program; the content is arbitrary.
const MEMO_TEXT: &[u8] = b"memo required";

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
    require!(ata_info.address() == &expected, ProgramError::InvalidSeeds);
    Ok(())
}

/// Read the token balance, issuing a `SyncNative` CPI first if the account
/// holds wrapped SOL. `parse` returns `(amount, is_native)` from the
/// account's bytes; it is called once for non-native accounts, and a
/// second time after the sync CPI for native ones (the synced `amount`
/// must be re-read).
fn read_balance_synced(
    info: &AccountView,
    token_program: &Address,
    parse: impl Fn(&[u8]) -> (u64, bool),
) -> Result<u64, ProgramError> {
    let (amount, is_native) = {
        let data = info.try_borrow()?;
        parse(&data)
    };
    if !is_native {
        return Ok(amount);
    }
    SyncNative {
        native_token: info,
        token_program,
    }
    .invoke()?;
    let data = info.try_borrow()?;
    Ok(parse(&data).0)
}

/// Read the `amount` field of a token account owned by either legacy
/// SPL Token or Token-2022, decoding with that program's own account
/// type. Wrapped-SOL accounts are `SyncNative`d first so `amount`
/// reflects lamports sent via a raw system transfer (which don't update
/// `amount` on their own).
#[inline(always)]
pub fn get_token_account_balance(info: &AccountView) -> Result<u64, ProgramError> {
    if info.owned_by(&TOKEN_PROGRAM_ID) {
        return read_balance_synced(info, &TOKEN_PROGRAM_ID, |data| {
            let account = unsafe { TokenAccount::from_bytes_unchecked(data) };
            (account.amount(), account.is_native())
        });
    }
    if info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        return read_balance_synced(info, &TOKEN_2022_PROGRAM_ID, |data| {
            let account = unsafe { Token2022Account::from_bytes_unchecked(data) };
            (account.amount(), account.is_native())
        });
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

/// Reject Token-2022 mints carrying either:
/// - an amount-mutating extension that silently breaks the "escrow
///   balance == sum of deposits" invariant (`ConfidentialTransfer`(+Fee),
///   `TransferFee`, `InterestBearing`, `ScaledUiAmount`): the credited
///   amount drifts from the debited amount, so the program would settle
///   "successfully" while a leg comes up short; or
/// - `NonTransferable`, which permanently blocks transfers out of the
///   escrow. Unlike everything else this never recovers — if a balance
///   reaches the escrow (e.g. the authority mints straight to it), no
///   settle/refund/reclaim can drain it, stranding both legs.
///
/// Everything else is allowed, including `Pausable`, `PermanentDelegate`,
/// `DefaultAccountState`, `TransferHook`, etc. These fail *loudly*
/// (reverted CPI, atomic rollback) rather than silently — funds stay in
/// escrow and recover once the blocking condition lifts.
/// `PermanentDelegate` is a deliberate carve-out for regulated RWA tokens
/// (issuer/transfer-agent clawback is an intrinsic property of the asset).
///
/// Residual griefing/clawback surface the program does not defend
/// against on-chain: a `Pausable` authority can pause mid-trade, a
/// `PermanentDelegate` can drain or claw back, a `FreezeAuthority` can
/// freeze the escrow ATA (or a frozen `DefaultAccountState` can make
/// future ATAs start frozen), a `TransferHook` EAML can be updated to
/// exceed the per-CPI account cap (`MAX_HOOK_REMAINING_ACCOUNTS`) or
/// to error unconditionally, and a `MintCloseAuthority` can close a
/// zero-supply mint and recreate it at the same address with a
/// different extension set (e.g. a transfer fee), changing transfer
/// behavior after Create since terminal paths bind only the mint
/// address and token program, not the extension set. Traders are
/// expected to vet the mints they agree to transact in; mint-authority
/// trust is not a problem the program can solve.
///
/// Called only at CreateDvp. Unwind paths skip this check so funds
/// remain recoverable if extension parameters change post-Create.
/// Legacy SPL Token mints only get a mint-size check.
#[inline(always)]
pub fn validate_mint_extensions(mint_info: &AccountView) -> ProgramResult {
    if !mint_info.owned_by(&TOKEN_2022_PROGRAM_ID) {
        // Legacy SPL Token mint: no extensions, but confirm it's a real
        // mint (exact size) rather than trusting the owner check alone.
        let data = mint_info.try_borrow()?;
        require!(
            data.len() == TokenMint::LEN,
            ProgramError::InvalidAccountData
        );
        return Ok(());
    }

    let data = mint_info.try_borrow()?;
    let mint = StateWithExtensions::<Token2022MintState>::unpack(&data)
        .map_err(|_| ProgramError::InvalidAccountData)?;

    if mint.get_extension::<ConfidentialTransferMint>().is_ok()
        || mint
            .get_extension::<ConfidentialTransferFeeConfig>()
            .is_ok()
        || mint.get_extension::<TransferFeeConfig>().is_ok()
        || mint.get_extension::<InterestBearingConfig>().is_ok()
        || mint.get_extension::<ScaledUiAmountConfig>().is_ok()
        || mint.get_extension::<NonTransferable>().is_ok()
    {
        return Err(DvpSwapProgramError::BlockedMintExtension.into());
    }

    Ok(())
}

/// Maximum number of hook extras (validation PDA, hook program ID, and
/// `ExtraAccountMetaList`-resolved accounts) accepted per
/// `TransferChecked` CPI. The cap is an implementation constraint —
/// the metas array passed to `invoke_signed_with_bounds` is
/// stack-allocated under a const generic and must be sized at compile
/// time — not a security check. Sized to fit comfortably inside SBF's
/// stack frame budget.
///
/// Residual griefing surface: a mint authority can call
/// `UpdateExtraAccountMetaList` and push an EAML with more than
/// `MAX_HOOK_REMAINING_ACCOUNTS - 2` entries, bricking every transfer
/// CPI on that leg until the authority relents. Treated the same way
/// as the other mint-authority surfaces in `validate_mint_extensions`:
/// counterparty risk, not a program-correctness problem.
pub const MAX_HOOK_REMAINING_ACCOUNTS: usize = 32;
const MAX_TRANSFER_CHECKED_ACCOUNTS: usize = 4 + MAX_HOOK_REMAINING_ACCOUNTS;

/// True if `to` is a Token-2022 account requiring an incoming-transfer memo.
#[inline(always)]
pub fn requires_memo(to: &AccountView) -> Result<bool, ProgramError> {
    if !to.owned_by(&TOKEN_2022_PROGRAM_ID) {
        return Ok(false);
    }
    let data = to.try_borrow()?;
    let account = StateWithExtensions::<Token2022AccountState>::unpack(&data)
        .map_err(|_| ProgramError::InvalidAccountData)?;
    Ok(memo_required(&account))
}

/// CPI the Memo program so it is the immediately preceding sibling of the
/// next transfer, satisfying a memo-required destination. Validates the
/// passed account only here — callers without a memo destination can pass
/// any account in the slot.
#[inline(always)]
fn invoke_memo(memo_program: &AccountView) -> ProgramResult {
    require!(
        memo_program.address() == &MEMO_PROGRAM_ID,
        ProgramError::IncorrectProgramId
    );
    let instruction = InstructionView {
        program_id: &MEMO_PROGRAM_ID,
        accounts: &[],
        data: MEMO_TEXT,
    };
    invoke_signed_with_bounds::<0>(&instruction, &[], &[])
}

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
    memo_program: &AccountView,
    remaining: &[AccountView],
    signers: &[Signer],
) -> ProgramResult {
    // A memo-required destination needs a Memo CPI as its immediately
    // preceding sibling; emit it here so the adjacency can't be broken.
    if requires_memo(to)? {
        invoke_memo(memo_program)?;
    }

    require!(
        remaining.len() <= MAX_HOOK_REMAINING_ACCOUNTS,
        ProgramError::InvalidArgument
    );
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
