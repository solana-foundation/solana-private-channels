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
    get_associated_token_address, instruction::create_associated_token_account_idempotent,
};
use spl_token::{
    state::{Account as TokenAccount, Mint},
    ID as TOKEN_PROGRAM_ID,
};

pub const ATA_PROGRAM_ID: Pubkey = pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const SWAP_PROGRAM_ID: Pubkey = CONTRA_SWAP_PROGRAM_ID;

pub const SWAP_DVP_SEED: &[u8] = b"dvp";

pub const SIGNER_NOT_PARTY: u32 = ContraSwapProgramError::SignerNotParty as u32;
pub const DVP_EXPIRED: u32 = ContraSwapProgramError::DvpExpired as u32;
pub const LEG_ALREADY_FUNDED: u32 = ContraSwapProgramError::LegAlreadyFunded as u32;
pub const SETTLEMENT_AUTHORITY_MISMATCH: u32 =
    ContraSwapProgramError::SettlementAuthorityMismatch as u32;
pub const SETTLEMENT_TOO_EARLY: u32 = ContraSwapProgramError::SettlementTooEarly as u32;
pub const LEG_NOT_FUNDED: u32 = ContraSwapProgramError::LegNotFunded as u32;
pub const EXPIRY_NOT_IN_FUTURE: u32 = ContraSwapProgramError::ExpiryNotInFuture as u32;
pub const EARLIEST_AFTER_EXPIRY: u32 = ContraSwapProgramError::EarliestAfterExpiry as u32;
pub const SELF_DVP: u32 = ContraSwapProgramError::SelfDvp as u32;
pub const SAME_MINT: u32 = ContraSwapProgramError::SameMint as u32;
pub const ZERO_AMOUNT: u32 = ContraSwapProgramError::ZeroAmount as u32;

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

pub fn set_mint(context: &mut TestContext, mint: &Pubkey) {
    let mint_state = Mint {
        decimals: 6,
        is_initialized: true,
        freeze_authority: COption::None,
        mint_authority: COption::None,
        supply: 1_000_000_000_000,
    };
    let mut data = vec![0u8; Mint::LEN];
    Mint::pack(mint_state, &mut data).unwrap();
    context
        .svm
        .set_account(
            *mint,
            Account {
                lamports: 1_000_000_000,
                data,
                owner: TOKEN_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .unwrap();
}

pub fn set_token_balance(
    context: &mut TestContext,
    ata: &Pubkey,
    mint: &Pubkey,
    owner: &Pubkey,
    amount: u64,
) {
    let token_account = TokenAccount {
        mint: *mint,
        owner: *owner,
        amount,
        delegate: COption::None,
        state: spl_token::state::AccountState::Initialized,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };
    let mut data = vec![0u8; TokenAccount::LEN];
    TokenAccount::pack(token_account, &mut data).unwrap();
    context
        .svm
        .set_account(
            *ata,
            Account {
                lamports: 2_039_280,
                data,
                owner: TOKEN_PROGRAM_ID,
                executable: false,
                rent_epoch: 0,
            },
        )
        .unwrap();
}

pub fn fund_wallet_ata(
    context: &mut TestContext,
    wallet: &Keypair,
    mint: &Pubkey,
    amount: u64,
) -> Pubkey {
    context.airdrop_if_required(&wallet.pubkey(), 1_000_000_000);
    let ata = get_associated_token_address(&wallet.pubkey(), mint);
    let create_ata_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        &wallet.pubkey(),
        mint,
        &TOKEN_PROGRAM_ID,
    );
    context.send(create_ata_ix, &[]).expect("create user ATA");
    set_token_balance(context, &ata, mint, &wallet.pubkey(), amount);
    ata
}

/// Create the ATA for `wallet` + `mint` without setting a balance. Used
/// for ATAs that the program will write into (settle recipient ATAs).
pub fn create_ata(context: &mut TestContext, wallet: &Pubkey, mint: &Pubkey) -> Pubkey {
    let create_ix = create_associated_token_account_idempotent(
        &context.payer.pubkey(),
        wallet,
        mint,
        &TOKEN_PROGRAM_ID,
    );
    context.send(create_ix, &[]).expect("create ATA");
    get_associated_token_address(wallet, mint)
}

pub fn get_token_balance(context: &TestContext, ata: &Pubkey) -> u64 {
    match context.get_account(ata) {
        Some(account) if account.owner == TOKEN_PROGRAM_ID => {
            TokenAccount::unpack(&account.data).unwrap().amount
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

pub fn dvp_ata(swap_dvp: &Pubkey, mint: &Pubkey) -> Pubkey {
    get_associated_token_address(swap_dvp, mint)
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
