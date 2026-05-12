use contra_swap_program_client::{ContraSwapProgramError, CONTRA_SWAP_PROGRAM_ID};
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
        interest_bearing_mint::InterestBearingConfig, pausable::PausableConfig,
        permanent_delegate::PermanentDelegate, scaled_ui_amount::ScaledUiAmountConfig,
        transfer_fee::TransferFeeConfig, transfer_hook::TransferHook, BaseStateWithExtensionsMut,
        ExtensionType, PodStateWithExtensionsMut,
    },
    pod::PodMint,
    state::{Account as Token2022Account, AccountState, Mint as Token2022Mint},
};

pub use spl_token::ID as TOKEN_PROGRAM_ID;
pub use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;

pub const ATA_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const SWAP_PROGRAM_ID: Pubkey = CONTRA_SWAP_PROGRAM_ID;

pub const SWAP_DVP_SEED: &[u8] = b"dvp";

pub const SIGNER_NOT_PARTY: u32 = ContraSwapProgramError::SignerNotParty as u32;
pub const DVP_EXPIRED: u32 = ContraSwapProgramError::DvpExpired as u32;
pub const SETTLEMENT_AUTHORITY_MISMATCH: u32 =
    ContraSwapProgramError::SettlementAuthorityMismatch as u32;
pub const SETTLEMENT_TOO_EARLY: u32 = ContraSwapProgramError::SettlementTooEarly as u32;
pub const LEG_NOT_FUNDED: u32 = ContraSwapProgramError::LegNotFunded as u32;
pub const EXPIRY_NOT_IN_FUTURE: u32 = ContraSwapProgramError::ExpiryNotInFuture as u32;
pub const EARLIEST_AFTER_EXPIRY: u32 = ContraSwapProgramError::EarliestAfterExpiry as u32;
pub const SELF_DVP: u32 = ContraSwapProgramError::SelfDvp as u32;
pub const SAME_MINT: u32 = ContraSwapProgramError::SameMint as u32;
pub const ZERO_AMOUNT: u32 = ContraSwapProgramError::ZeroAmount as u32;
pub const BLOCKED_MINT_EXTENSION: u32 = ContraSwapProgramError::BlockedMintExtension as u32;

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

        let program_data = include_bytes!("../../../../target/deploy/contra_swap_program.so");
        let _ = svm.add_program(SWAP_PROGRAM_ID, program_data);

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
    let mut state =
        PodStateWithExtensionsMut::<PodMint>::unpack_uninitialized(&mut data).unwrap();
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

/// ConfidentialTransferMint — blocked; used in negative tests.
pub fn set_mint_2022_with_confidential_transfer(
    context: &mut TestContext,
    mint: &Pubkey,
    authority: &Pubkey,
) {
    let data =
        build_mint_2022_with_extensions(&[ExtensionType::ConfidentialTransferMint], |state| {
            let ext = state.init_extension::<ConfidentialTransferMint>(true).unwrap();
            ext.authority = OptionalNonZeroPubkey::try_from(Some(*authority)).unwrap();
            ext.auto_approve_new_accounts = false.into();
            ext.auditor_elgamal_pubkey = Default::default();
        });
    write_account(context, mint, data, TOKEN_2022_PROGRAM_ID, 1_000_000_000);
}
