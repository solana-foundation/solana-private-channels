use dvp_swap_program_client::{DvpSwapProgramError, DVP_SWAP_PROGRAM_ID};
use litesvm::{types::TransactionMetadata, LiteSVM};
use solana_program::{clock::Clock, pubkey};
use solana_program_pack::Pack;
use solana_sdk::{
    account::Account,
    instruction::Instruction,
    program_option::COption,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_pod::optional_keys::OptionalNonZeroPubkey;
use spl_token::state::{Account as TokenAccount, Mint};
use spl_token_2022::{
    extension::{
        confidential_transfer::ConfidentialTransferMint,
        interest_bearing_mint::InterestBearingConfig,
        non_transferable::NonTransferable,
        pausable::PausableConfig,
        permanent_delegate::PermanentDelegate,
        scaled_ui_amount::ScaledUiAmountConfig,
        transfer_fee::TransferFeeConfig,
        transfer_hook::{TransferHook, TransferHookAccount},
        BaseStateWithExtensionsMut, ExtensionType, PodStateWithExtensionsMut,
    },
    pod::{PodAccount, PodMint},
    state::{Account as Token2022Account, AccountState, Mint as Token2022Mint},
};

pub use spl_token::ID as TOKEN_PROGRAM_ID;
pub use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;

pub const ATA_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
pub const SWAP_PROGRAM_ID: Pubkey = DVP_SWAP_PROGRAM_ID;

/// Program ID of the no-op transfer-hook fixture loaded into LiteSVM
/// for hook-bearing Token-2022 lifecycle tests. Matches
/// `declare_id!` in `dvp-swap-program/tests/transfer-hook-fixture/src/lib.rs`.
pub const HOOK_FIXTURE_PROGRAM_ID: Pubkey = pubkey!("HookqJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC");

pub const SWAP_DVP_SEED: &[u8] = b"dvp";
pub const NONCE_TOMBSTONE_SEED: &[u8] = b"nonce";

pub const SIGNER_NOT_PARTY: u32 = DvpSwapProgramError::SignerNotParty as u32;
pub const DVP_EXPIRED: u32 = DvpSwapProgramError::DvpExpired as u32;
pub const SETTLEMENT_AUTHORITY_MISMATCH: u32 =
    DvpSwapProgramError::SettlementAuthorityMismatch as u32;
pub const SETTLEMENT_TOO_EARLY: u32 = DvpSwapProgramError::SettlementTooEarly as u32;
pub const LEG_NOT_FUNDED: u32 = DvpSwapProgramError::LegNotFunded as u32;
pub const EXPIRY_NOT_IN_FUTURE: u32 = DvpSwapProgramError::ExpiryNotInFuture as u32;
pub const EXPIRY_TOO_FAR_IN_FUTURE: u32 = DvpSwapProgramError::ExpiryTooFarInFuture as u32;
pub const EARLIEST_AFTER_EXPIRY: u32 = DvpSwapProgramError::EarliestAfterExpiry as u32;
pub const SELF_DVP: u32 = DvpSwapProgramError::SelfDvp as u32;
pub const SAME_MINT: u32 = DvpSwapProgramError::SameMint as u32;
pub const ZERO_AMOUNT: u32 = DvpSwapProgramError::ZeroAmount as u32;
pub const BLOCKED_MINT_EXTENSION: u32 = DvpSwapProgramError::BlockedMintExtension as u32;
pub const SETTLEMENT_AUTHORITY_EXECUTABLE: u32 =
    DvpSwapProgramError::SettlementAuthorityExecutable as u32;
pub const SETTLEMENT_AUTHORITY_IS_PARTY: u32 =
    DvpSwapProgramError::SettlementAuthorityIsParty as u32;
pub const NONCE_ALREADY_USED: u32 = DvpSwapProgramError::NonceAlreadyUsed as u32;

const MIN_LAMPORTS: u64 = 500_000_000;

pub struct TestContext {
    pub svm: LiteSVM,
    pub payer: Keypair,
}

impl TestContext {
    pub fn new() -> Self {
        let mut svm = LiteSVM::new().with_sysvars().with_default_programs();

        // CreateDvp rejects expiry <= now; default LiteSVM Clock has
        // unix_timestamp = 0, which would let any future expiry of 1+
        // pass. Pin Clock to wall-clock so the tests' "expiry = now + 1h"
        // is meaningful.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        svm.set_sysvar(&Clock {
            slot: 1,
            epoch_start_timestamp: now,
            epoch: 0,
            leader_schedule_epoch: 0,
            unix_timestamp: now,
        });

        let program_data = include_bytes!("../../../../target/deploy/dvp_swap_program.so");
        let _ = svm.add_program(SWAP_PROGRAM_ID, program_data);

        let hook_data = include_bytes!("../../../../target/deploy/transfer_hook_fixture.so");
        let _ = svm.add_program(HOOK_FIXTURE_PROGRAM_ID, hook_data);

        let payer = Keypair::new();
        svm.airdrop(&payer.pubkey(), 1_000_000_000).unwrap();

        Self { svm, payer }
    }

    pub fn now(&self) -> i64 {
        self.svm.get_sysvar::<Clock>().unix_timestamp
    }

    pub fn advance_clock(&mut self, seconds: i64) {
        let clock = self.svm.get_sysvar::<Clock>();
        self.svm.set_sysvar(&Clock {
            slot: clock.slot + seconds as u64,
            unix_timestamp: clock.unix_timestamp + seconds,
            ..clock
        });
    }

    pub fn airdrop_if_required(&mut self, pubkey: &Pubkey, lamports: u64) {
        let needs = match self.svm.get_account(pubkey) {
            Some(account) => account.lamports < MIN_LAMPORTS,
            None => true,
        };
        if needs {
            self.svm.airdrop(pubkey, lamports).expect("airdrop");
        }
    }

    pub fn send(
        &mut self,
        ix: Instruction,
        signers: &[&Keypair],
    ) -> Result<TransactionMetadata, String> {
        // Advance the blockhash before signing so back-to-back identical
        // instructions (e.g. fund → reclaim → fund) don't collide on
        // signature and trip litesvm's AlreadyProcessed guard.
        self.svm.expire_blockhash();
        let mut all_signers = vec![&self.payer];
        all_signers.extend(signers);
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.payer.pubkey()),
            &all_signers,
            self.svm.latest_blockhash(),
        );
        self.svm
            .send_transaction(tx)
            .map_err(|e| format!("{:?}", e))
    }

    pub fn get_account(&self, pubkey: &Pubkey) -> Option<Account> {
        self.svm.get_account(pubkey)
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// The wrapped-SOL (native) mint, owned by legacy SPL Token. LiteSVM does
/// not seed this account, so tests must write it with `set_native_mint`.
pub const NATIVE_MINT: Pubkey = spl_token::native_mint::ID;

/// Write the wrapped-SOL mint (decimals 9, SPL Token) at its canonical
/// address so WSOL legs can be exercised.
pub fn set_native_mint(context: &mut TestContext) {
    let mint_state = Mint {
        decimals: 9,
        is_initialized: true,
        freeze_authority: COption::None,
        mint_authority: COption::None,
        supply: 0,
    };
    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_state, &mut data).unwrap();
    write_account(context, &NATIVE_MINT, data, TOKEN_PROGRAM_ID, 1_000_000_000);
}

/// Write a bare mint (no Token-2022 extensions) owned by `token_program`.
/// For Token-2022 mints with extensions use one of the
/// `set_mint_2022_with_*` builders below — they pack a proper TLV layout
/// the program will see exactly as a real on-chain mint.
pub fn set_mint(context: &mut TestContext, mint: &Pubkey, token_program: &Pubkey) {
    if *token_program == TOKEN_PROGRAM_ID {
        let mint_state = Mint {
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
            mint_authority: COption::None,
            supply: 1_000_000_000_000,
        };
        let mut data = vec![0u8; Mint::LEN];
        Mint::pack(mint_state, &mut data).unwrap();
        write_account(context, mint, data, *token_program, 1_000_000_000);
    } else if *token_program == TOKEN_2022_PROGRAM_ID {
        let mint_state = Token2022Mint {
            decimals: 6,
            is_initialized: true,
            freeze_authority: COption::None,
            mint_authority: COption::None,
            supply: 1_000_000_000_000,
        };
        let mut data = vec![0u8; Token2022Mint::LEN];
        Token2022Mint::pack_into_slice(&mint_state, &mut data);
        write_account(context, mint, data, *token_program, 1_000_000_000);
    } else {
        panic!("unknown token program: {token_program}");
    }
}

/// Write a token-account into the SVM. Dispatches the pack format on
/// `token_program`. For Token-2022 we pack the base 165 bytes — no
/// account-side extensions (ImmutableOwner / CpiGuard / etc.) — since
/// the program's transfer/close CPIs don't depend on them.
pub fn set_token_balance(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
    token_program: &Pubkey,
) {
    let data = if *token_program == TOKEN_PROGRAM_ID {
        let acct = TokenAccount {
            mint: *mint,
            owner: *owner,
            amount,
            delegate: COption::None,
            state: spl_token::state::AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };
        let mut buf = vec![0u8; TokenAccount::LEN];
        TokenAccount::pack(acct, &mut buf).unwrap();
        buf
    } else if *token_program == TOKEN_2022_PROGRAM_ID {
        let acct = Token2022Account {
            mint: *mint,
            owner: *owner,
            amount,
            delegate: COption::None,
            state: AccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };
        let mut buf = vec![0u8; Token2022Account::LEN];
        spl_token_2022::state::Account::pack(acct, &mut buf).unwrap();
        buf
    } else {
        panic!("unknown token program: {token_program}");
    };
    write_account(context, ata, data, *token_program, 2_039_280);
}

pub fn fund_wallet_ata(
    context: &mut TestContext,
    wallet: &Keypair,
    mint: &Pubkey,
    amount: u64,
    token_program: &Pubkey,
) -> Pubkey {
    context.airdrop_if_required(&wallet.pubkey(), 1_000_000_000);
    let ata = get_associated_token_address_with_program_id(&wallet.pubkey(), mint, token_program);
    let create_ata_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        &wallet.pubkey(),
        mint,
        token_program,
    );
    context.send(create_ata_ix, &[]).expect("create user ATA");
    set_token_balance(context, &ata, mint, &wallet.pubkey(), amount, token_program);
    ata
}

/// Create the ATA for `wallet` + `mint` without setting a balance. Used
/// for ATAs that the program will write into (settle recipient ATAs).
pub fn create_ata(
    context: &mut TestContext,
    wallet: &Pubkey,
    mint: &Pubkey,
    token_program: &Pubkey,
) -> Pubkey {
    let create_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        wallet,
        mint,
        token_program,
    );
    context.send(create_ix, &[]).expect("create ATA");
    get_associated_token_address_with_program_id(wallet, mint, token_program)
}

/// Read the token account `amount`. Works for both SPL Token and
/// Token-2022 since both layouts share the first 165 bytes.
pub fn get_token_balance(context: &TestContext, ata: &Pubkey) -> u64 {
    match context.get_account(ata) {
        Some(account)
            if account.owner == TOKEN_PROGRAM_ID || account.owner == TOKEN_2022_PROGRAM_ID =>
        {
            TokenAccount::unpack(&account.data[..TokenAccount::LEN])
                .unwrap()
                .amount
        }
        _ => 0,
    }
}

pub fn swap_dvp_pda(
    settlement_authority: &Pubkey,
    user_a: &Pubkey,
    user_b: &Pubkey,
    mint_a: &Pubkey,
    mint_b: &Pubkey,
    nonce: u64,
) -> (Pubkey, u8) {
    let nonce_bytes = nonce.to_le_bytes();
    Pubkey::find_program_address(
        &[
            SWAP_DVP_SEED,
            settlement_authority.as_ref(),
            user_a.as_ref(),
            user_b.as_ref(),
            mint_a.as_ref(),
            mint_b.as_ref(),
            &nonce_bytes,
        ],
        &SWAP_PROGRAM_ID,
    )
}

pub fn nonce_tombstone_pda(swap_dvp: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[NONCE_TOMBSTONE_SEED, swap_dvp.as_ref()], &SWAP_PROGRAM_ID)
}

pub fn dvp_ata(swap_dvp: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(swap_dvp, mint, token_program)
}

pub fn assert_program_error(result: Result<TransactionMetadata, String>, expected_code: u32) {
    match result {
        Err(e) => {
            let expected = format!("Custom({})", expected_code);
            assert!(
                e.contains(&expected),
                "expected Custom({}) in error, got: {}",
                expected_code,
                e
            );
        }
        Ok(_) => panic!("expected tx to fail with Custom({})", expected_code),
    }
}

/// Assert the tx failed with a built-in `ProgramError` variant (anything
/// that surfaces as `InstructionError(_, <Variant>)` rather than
/// `Custom(N)` — `InvalidAccountData`, `InvalidSeeds`, etc.). Matches
/// the full `InstructionError(0, <Variant>)` substring so it can't
/// spuriously hit on the same variant name appearing elsewhere in the
/// error string.
pub fn assert_instruction_error(
    result: Result<TransactionMetadata, String>,
    expected_variant: &str,
) {
    match result {
        Err(e) => {
            let needle = format!("InstructionError(0, {})", expected_variant);
            assert!(
                e.contains(&needle),
                "expected {} in error, got: {}",
                needle,
                e
            );
        }
        Ok(_) => panic!(
            "expected tx to fail with InstructionError(_, {})",
            expected_variant
        ),
    }
}

// ---------------------------------------------------------------------
// Token-2022 mint builders with extensions.
//
// Each helper writes a properly TLV-encoded mint owned by Token-2022
// directly into the SVM. The mint's base fields (`decimals`,
// `is_initialized`) are set up so `TransferChecked` against the mint
// works; the extension is added on top so the program's
// `validate_mint_extensions` check can see it at CreateDvp.
// ---------------------------------------------------------------------

#[inline]
fn build_mint_2022_with_extensions(
    extensions: &[ExtensionType],
    init_extension: impl FnOnce(&mut PodStateWithExtensionsMut<PodMint>),
) -> Vec<u8> {
    let space = ExtensionType::try_calculate_account_len::<Token2022Mint>(extensions).unwrap();
    let mut data = vec![0u8; space];
    let mut state = PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();
    init_extension(&mut state);
    *state.base = PodMint {
        mint_authority: COption::None.into(),
        supply: 1_000_000_000_000u64.into(),
        decimals: 6,
        is_initialized: true.into(),
        freeze_authority: COption::None.into(),
    };
    state.init_account_type().unwrap();
    data
}

#[inline]
fn write_account(
    context: &mut TestContext,
    pubkey: &Pubkey,
    data: Vec<u8>,
    owner: Pubkey,
    lamports: u64,
) {
    context
        .svm
        .set_account(
            *pubkey,
            Account {
                lamports,
                data,
                owner,
                executable: false,
                rent_epoch: 0,
            },
        )
        .unwrap();
}

pub fn set_mint_2022_with_permanent_delegate(
    context: &mut TestContext,
    mint: &Pubkey,
    delegate: &Pubkey,
) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::PermanentDelegate], |state| {
        let ext = state.init_extension::<PermanentDelegate>(true).unwrap();
        ext.delegate = OptionalNonZeroPubkey::try_from(Some(*delegate)).unwrap();
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

pub fn set_mint_2022_with_pausable(context: &mut TestContext, mint: &Pubkey, authority: &Pubkey) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::Pausable], |state| {
        let ext = state.init_extension::<PausableConfig>(true).unwrap();
        *ext = PausableConfig {
            paused: false.into(),
            authority: OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap(),
        };
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

pub fn set_mint_2022_with_transfer_hook(
    context: &mut TestContext,
    mint: &Pubkey,
    hook_program_id: &Pubkey,
    authority: &Pubkey,
) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::TransferHook], |state| {
        let ext = state.init_extension::<TransferHook>(true).unwrap();
        *ext = TransferHook {
            authority: OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap(),
            program_id: OptionalNonZeroPubkey::try_from(Some(*hook_program_id)).unwrap(),
        };
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

/// Writes a Token-2022 token account with the `TransferHookAccount`
/// extension initialized. Token-2022's `process_transfer` calls
/// `set_transferring` on both source and destination, which reads the
/// `TransferHookAccount` extension off each — a bare 165-byte account
/// would fail with `InvalidAccountData`. Real ATAs created by the SPL
/// ATA program against a hook-bearing mint already carry this
/// extension; this helper is the test-side equivalent for any user ATA
/// that was created *before* its mint gained `TransferHook` (or had its
/// data stomped by `set_token_balance`).
pub fn set_token_2022_with_hook_account(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    use spl_token_2022::pod::PodCOption;

    let space = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
        ExtensionType::TransferHookAccount,
    ])
    .unwrap();
    let mut data = vec![0u8; space];
    let mut state =
        PodStateWithExtensionsMut::<PodAccount>::unpack_uninitialized(&mut data).unwrap();
    state.init_extension::<TransferHookAccount>(true).unwrap();
    *state.base = PodAccount {
        mint: *mint,
        owner: *owner,
        amount: amount.into(),
        delegate: PodCOption::none(),
        state: AccountState::Initialized as u8,
        is_native: PodCOption::none(),
        delegated_amount: 0u64.into(),
        close_authority: PodCOption::none(),
    };
    state.init_account_type().unwrap();
    write_account(context, ata, data, TOKEN_2022_PROGRAM_ID, 2_039_280);
}

/// Writes a Token-2022 token account with MemoTransfer enabled. Incoming
/// transfers then require a preceding Memo-program instruction.
pub fn set_token_2022_with_memo_required(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    use spl_token_2022::extension::memo_transfer::MemoTransfer;
    use spl_token_2022::pod::PodCOption;

    let space = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
        ExtensionType::MemoTransfer,
    ])
    .unwrap();
    let mut data = vec![0u8; space];
    let mut state =
        PodStateWithExtensionsMut::<PodAccount>::unpack_uninitialized(&mut data).unwrap();
    let ext = state.init_extension::<MemoTransfer>(true).unwrap();
    ext.require_incoming_transfer_memos = true.into();
    *state.base = PodAccount {
        mint: *mint,
        owner: *owner,
        amount: amount.into(),
        delegate: PodCOption::none(),
        state: AccountState::Initialized as u8,
        is_native: PodCOption::none(),
        delegated_amount: 0u64.into(),
        close_authority: PodCOption::none(),
    };
    state.init_account_type().unwrap();
    write_account(context, ata, data, TOKEN_2022_PROGRAM_ID, 2_039_280);
}

/// Sets up a Token-2022 mint that delegates to the test hook fixture
/// (`HOOK_FIXTURE_PROGRAM_ID`) and creates its `ExtraAccountMetaList`
/// validation PDA declaring **one** extra account: the system program.
///
/// The hook fixture only logs the account count it receives, so the
/// tests assert `hook accounts: N` where N is the standard 5 (source,
/// mint, destination, authority, validation PDA) plus the 1 extra we
/// declared here = **6**. Use [`hook_extras_for_mint`] to get the
/// trailing accounts the client must pass to Settle/Cancel/Reject/Reclaim
/// for this mint.
pub fn setup_hook_mint(context: &mut TestContext, mint: &Pubkey) {
    use spl_tlv_account_resolution::{account::ExtraAccountMeta, state::ExtraAccountMetaList};
    use spl_transfer_hook_interface::{
        get_extra_account_metas_address, instruction::ExecuteInstruction,
    };

    set_mint_2022_with_transfer_hook(
        context,
        mint,
        &HOOK_FIXTURE_PROGRAM_ID,
        &context.payer.pubkey(),
    );

    let validation_pda = get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID);
    let extras =
        vec![
            ExtraAccountMeta::new_with_pubkey(&solana_program::system_program::ID, false, false)
                .unwrap(),
        ];
    let size = ExtraAccountMetaList::size_of(extras.len()).unwrap();
    let mut data = vec![0u8; size];
    ExtraAccountMetaList::init::<ExecuteInstruction>(&mut data, &extras).unwrap();
    write_account(
        context,
        &validation_pda,
        data,
        HOOK_FIXTURE_PROGRAM_ID,
        1_000_000_000,
    );
}

/// Returns the trailing accounts the client must append to a
/// `TransferChecked` (here surfaced as the swap program's
/// `leg_*_extras`) when transferring a mint set up via
/// [`setup_hook_mint`]. Order matches `spl_transfer_hook_interface`'s
/// offchain resolver: declared extras first, then the hook program ID,
/// then the validation PDA.
pub fn hook_extras_for_mint(mint: &Pubkey) -> Vec<solana_sdk::instruction::AccountMeta> {
    use solana_sdk::instruction::AccountMeta;
    use spl_transfer_hook_interface::get_extra_account_metas_address;

    let validation_pda = get_extra_account_metas_address(mint, &HOOK_FIXTURE_PROGRAM_ID);
    vec![
        AccountMeta::new_readonly(solana_program::system_program::ID, false),
        AccountMeta::new_readonly(HOOK_FIXTURE_PROGRAM_ID, false),
        AccountMeta::new_readonly(validation_pda, false),
    ]
}

/// TransferFeeConfig — a blocked extension; used in negative tests.
pub fn set_mint_2022_with_transfer_fee(
    context: &mut TestContext,
    mint: &Pubkey,
    authority: &Pubkey,
    basis_points: u16,
    maximum_fee: u64,
) {
    use spl_token_2022::extension::transfer_fee::TransferFee;
    use spl_token_2022::pod::PodCOption;

    let data = build_mint_2022_with_extensions(&[ExtensionType::TransferFeeConfig], |state| {
        let ext = state.init_extension::<TransferFeeConfig>(true).unwrap();
        let auth = OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap();
        ext.transfer_fee_config_authority = auth;
        ext.withdraw_withheld_authority = auth;
        ext.withheld_amount = 0u64.into();
        ext.older_transfer_fee = TransferFee {
            epoch: 0u64.into(),
            maximum_fee: maximum_fee.into(),
            transfer_fee_basis_points: basis_points.into(),
        };
        ext.newer_transfer_fee = ext.older_transfer_fee;
        // Suppress unused-import / dead-field warnings for PodCOption when
        // some downstream `TransferFee` field types change versions.
        let _ = PodCOption::<Pubkey>::default();
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

/// InterestBearingConfig — blocked; used in negative tests.
pub fn set_mint_2022_with_interest_bearing(
    context: &mut TestContext,
    mint: &Pubkey,
    authority: &Pubkey,
) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::InterestBearingConfig], |state| {
        let ext = state.init_extension::<InterestBearingConfig>(true).unwrap();
        ext.rate_authority = OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap();
        ext.initialization_timestamp = 0i64.into();
        ext.pre_update_average_rate = 0i16.into();
        ext.last_update_timestamp = 0i64.into();
        ext.current_rate = 0i16.into();
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

/// ScaledUiAmountConfig — blocked; used in negative tests.
pub fn set_mint_2022_with_scaled_ui_amount(
    context: &mut TestContext,
    mint: &Pubkey,
    authority: &Pubkey,
) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::ScaledUiAmount], |state| {
        let ext = state.init_extension::<ScaledUiAmountConfig>(true).unwrap();
        ext.authority = OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap();
        ext.multiplier = 1.0f64.into();
        ext.new_multiplier_effective_timestamp = 0i64.into();
        ext.new_multiplier = 1.0f64.into();
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

/// NonTransferable — blocked; used in negative tests. A balance reaching
/// a non-transferable escrow can never be drained, so Create rejects it.
pub fn set_mint_2022_with_non_transferable(context: &mut TestContext, mint: &Pubkey) {
    let data = build_mint_2022_with_extensions(&[ExtensionType::NonTransferable], |state| {
        state.init_extension::<NonTransferable>(true).unwrap();
    });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}

/// ConfidentialTransferMint — blocked; used in negative tests.
pub fn set_mint_2022_with_confidential_transfer(
    context: &mut TestContext,
    mint: &Pubkey,
    authority: &Pubkey,
) {
    let data =
        build_mint_2022_with_extensions(&[ExtensionType::ConfidentialTransferMint], |state| {
            let ext = state
                .init_extension::<ConfidentialTransferMint>(true)
                .unwrap();
            ext.authority = OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap();
            ext.auto_approve_new_accounts = false.into();
            ext.auditor_elgamal_pubkey = Default::default();
        });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}
