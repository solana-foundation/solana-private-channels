use private_channel_withdraw_program_client::instructions::{
    ConfidentialWithdrawFundsBuilder, WithdrawFundsBuilder,
    CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR, WITHDRAW_FUNDS_DISCRIMINATOR,
};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use spl_token_2022::ID as TOKEN_PROGRAM_ID;

use crate::utils::{
    assert_program_error, channel_ata, find_withdraw_config_pda, get_mint_supply,
    get_token_balance, set_mint, set_mint_supply, set_mint_with_delegate, set_token_balance,
    set_withdraw_config, set_withdraw_config_with_minimum, setup_test_balances, TestContext,
    AMOUNT_ABOVE_CONFIDENTIAL_MAXIMUM_ERROR, AMOUNT_BELOW_MINIMUM_ERROR, ATA_PROGRAM_ID,
    CONFIDENTIAL_ONLY_MINT_ERROR, INCORRECT_PROGRAM_ID_ERROR, INVALID_INSTRUCTION_DATA_ERROR,
    INVALID_MINT_ERROR, INVALID_TREASURY_ACCOUNT_ERROR, INVALID_WITHDRAW_CONFIG_ERROR,
    MISSING_REQUIRED_SIGNATURE_ERROR, NOT_ENOUGH_ACCOUNT_KEYS_ERROR,
    PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID, TEST_WITHDRAW_FEE, TOKEN_INSUFFICIENT_FUNDS_ERROR,
    WITHDRAW_CONFIG_NOT_INITIALIZED_ERROR, ZERO_AMOUNT_ERROR,
};

const WITHDRAW_AMOUNT: u64 = 500_000;
const INITIAL_BALANCE: u64 = 1_000_000;
/// Largest amount the escrow's confidential release can carry (a 16-bit lo and 32-bit hi).
const CONFIDENTIAL_MAX: u64 = (1 << 48) - 1;

/// A mint with a fee-charging config and a funded user, the setup most cases share.
struct Setup {
    context: TestContext,
    user: Keypair,
    mint: Pubkey,
    user_ata: Pubkey,
    withdraw_config: Pubkey,
    treasury_token_account: Pubkey,
}

impl Setup {
    fn new(balance: u64) -> Self {
        let mut context = TestContext::new();
        let user = Keypair::new();
        let mint = Keypair::new().pubkey();
        set_mint(&mut context, &mint);
        let (withdraw_config, treasury_token_account) = set_withdraw_config(
            &mut context,
            &mint,
            TEST_WITHDRAW_FEE,
            &Pubkey::new_unique(),
        );
        setup_test_balances(&mut context, &user, &mint, balance);
        let user_ata = channel_ata(&user.pubkey(), &mint);
        Self {
            context,
            user,
            mint,
            user_ata,
            withdraw_config,
            treasury_token_account,
        }
    }

    /// A WithdrawFunds over this setup's accounts; cases change it before sending.
    fn withdraw(&self, amount: u64) -> Instruction {
        WithdrawFundsBuilder::new()
            .user(self.user.pubkey())
            .mint(self.mint)
            .token_account(self.user_ata)
            .token_program(TOKEN_PROGRAM_ID)
            .associated_token_program(ATA_PROGRAM_ID)
            .withdraw_config(self.withdraw_config)
            .treasury_token_account(self.treasury_token_account)
            .amount(amount)
            .instruction()
    }

    fn confidential(&self, amount: u64, destination: Option<Pubkey>) -> Instruction {
        let mut builder = ConfidentialWithdrawFundsBuilder::new();
        builder
            .user(self.user.pubkey())
            .mint(self.mint)
            .token_account(self.user_ata)
            .token_program(TOKEN_PROGRAM_ID)
            .associated_token_program(ATA_PROGRAM_ID)
            .withdraw_config(self.withdraw_config)
            .treasury_token_account(self.treasury_token_account)
            .amount(amount);
        if let Some(destination) = destination {
            builder.destination(destination);
        }
        builder.instruction()
    }
}

/// The same instruction sent as ConfidentialWithdrawFunds: only the discriminator differs.
fn as_confidential(mut instruction: Instruction) -> Instruction {
    assert_eq!(instruction.data[0], WITHDRAW_FUNDS_DISCRIMINATOR);
    instruction.data[0] = CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR;
    instruction
}

/// Sets the config's last byte, its `confidential_only` flag.
fn set_confidential_only(context: &mut TestContext, withdraw_config: &Pubkey) {
    let mut account = context
        .get_account(withdraw_config)
        .expect("withdraw config should exist");
    *account.data.last_mut().expect("withdraw config has data") = 1;
    context
        .svm
        .set_account(*withdraw_config, account)
        .expect("Failed to set withdraw config");
}

/// The log line pinocchio_log writes for a WithdrawFundsEvent.
fn event_log(amount: u64, destination: &Pubkey) -> String {
    let mut bytes = amount.to_le_bytes().to_vec();
    bytes.extend_from_slice(destination.as_ref());
    let parts: Vec<String> = bytes.iter().map(u8::to_string).collect();
    format!("Program log: [{}]", parts.join(", "))
}

/// A refusal case: builds its own state and returns the WithdrawFunds to send, its signers
/// and the error both instructions must fail with.
type Case = fn() -> (Setup, Instruction, Vec<Keypair>, u32);

/// Runs `case` once as WithdrawFunds and once as ConfidentialWithdrawFunds, each on fresh
/// state, and checks both fail the same way with no balance moved.
fn assert_same_refusal(name: &str, case: Case) {
    for confidential in [false, true] {
        let (mut setup, instruction, signers, expected) = case();
        let instruction = if confidential {
            as_confidential(instruction)
        } else {
            instruction
        };
        let user_before = get_token_balance(&mut setup.context, &setup.user_ata);
        let signers: Vec<&Keypair> = signers.iter().collect();
        let result = setup
            .context
            .send_transaction_with_signers(instruction, &signers);
        println!("{name} (confidential: {confidential})");
        assert_program_error(result, expected);
        assert_eq!(
            get_token_balance(&mut setup.context, &setup.user_ata),
            user_before,
            "{name}: nothing may move on a refusal"
        );
    }
}

#[test]
fn test_confidential_withdraw_funds_burns_and_pays_the_fee() {
    let mut setup = Setup::new(INITIAL_BALANCE);
    let destination = Pubkey::new_from_array([42u8; 32]);
    let supply_before = get_mint_supply(&mut setup.context, &setup.mint);

    let instruction = setup.confidential(WITHDRAW_AMOUNT, Some(destination));
    let user = setup.user.insecure_clone();
    let meta = setup
        .context
        .send_transaction_with_signers_with_transaction_result(instruction, &[&user], true, None)
        .expect("a confidential withdrawal should succeed");

    assert_eq!(
        get_token_balance(&mut setup.context, &setup.user_ata),
        INITIAL_BALANCE - WITHDRAW_AMOUNT - TEST_WITHDRAW_FEE
    );
    assert_eq!(
        get_token_balance(&mut setup.context, &setup.treasury_token_account),
        TEST_WITHDRAW_FEE
    );
    assert_eq!(
        get_mint_supply(&mut setup.context, &setup.mint),
        supply_before - WITHDRAW_AMOUNT
    );
    // Same event as WithdrawFunds; the indexer tells them apart by discriminator.
    let expected = event_log(WITHDRAW_AMOUNT, &destination);
    assert!(
        meta.logs.contains(&expected),
        "{expected} not in {:#?}",
        meta.logs
    );
}

#[test]
fn test_confidential_withdraw_funds_without_destination_releases_to_the_user() {
    let mut setup = Setup::new(INITIAL_BALANCE);
    let instruction = setup.confidential(WITHDRAW_AMOUNT, None);
    let user = setup.user.insecure_clone();
    let meta = setup
        .context
        .send_transaction_with_signers_with_transaction_result(instruction, &[&user], false, None)
        .expect("a confidential withdrawal should succeed");
    let expected = event_log(WITHDRAW_AMOUNT, &user.pubkey());
    assert!(
        meta.logs.contains(&expected),
        "{expected} not in {:#?}",
        meta.logs
    );
}

#[test]
fn test_confidential_withdraw_funds_accepts_the_maximum() {
    let mut setup = Setup::new(CONFIDENTIAL_MAX + TEST_WITHDRAW_FEE);
    set_mint_supply(&mut setup.context, &setup.mint, u64::MAX);

    let instruction = setup.confidential(CONFIDENTIAL_MAX, None);
    let user = setup.user.insecure_clone();
    setup
        .context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("2^48 - 1 is the largest confidential amount and is accepted");

    assert_eq!(get_token_balance(&mut setup.context, &setup.user_ata), 0);
    assert_eq!(
        get_mint_supply(&mut setup.context, &setup.mint),
        u64::MAX - CONFIDENTIAL_MAX
    );
}

#[test]
fn test_confidential_withdraw_funds_refuses_above_the_maximum() {
    for amount in [CONFIDENTIAL_MAX + 1, u64::MAX - TEST_WITHDRAW_FEE] {
        let mut setup = Setup::new(amount + TEST_WITHDRAW_FEE);
        set_mint_supply(&mut setup.context, &setup.mint, u64::MAX);

        let instruction = setup.confidential(amount, None);
        let user = setup.user.insecure_clone();
        let result = setup
            .context
            .send_transaction_with_signers(instruction, &[&user]);

        assert_program_error(result, AMOUNT_ABOVE_CONFIDENTIAL_MAXIMUM_ERROR);
        assert_eq!(
            get_token_balance(&mut setup.context, &setup.user_ata),
            amount + TEST_WITHDRAW_FEE
        );
        assert_eq!(
            get_token_balance(&mut setup.context, &setup.treasury_token_account),
            0
        );
    }
}

/// The cap is only for confidential releases; a public withdrawal of the same amount works.
#[test]
fn test_withdraw_funds_above_the_confidential_maximum_is_unchanged() {
    let mut setup = Setup::new(CONFIDENTIAL_MAX + 1 + TEST_WITHDRAW_FEE);
    set_mint_supply(&mut setup.context, &setup.mint, u64::MAX);

    let instruction = setup.withdraw(CONFIDENTIAL_MAX + 1);
    let user = setup.user.insecure_clone();
    setup
        .context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("WithdrawFunds has no cap");
    assert_eq!(get_token_balance(&mut setup.context, &setup.user_ata), 0);
}

/// A confidential-only mint has no public balance on Solana, so a public withdrawal could
/// never be released. It is refused before anything moves.
#[test]
fn test_withdraw_funds_refuses_a_confidential_only_mint() {
    let mut setup = Setup::new(INITIAL_BALANCE);
    set_confidential_only(&mut setup.context, &setup.withdraw_config);
    let supply_before = get_mint_supply(&mut setup.context, &setup.mint);

    let instruction = setup.withdraw(WITHDRAW_AMOUNT);
    let user = setup.user.insecure_clone();
    let result = setup
        .context
        .send_transaction_with_signers(instruction, &[&user]);

    assert_program_error(result, CONFIDENTIAL_ONLY_MINT_ERROR);
    assert_eq!(
        get_token_balance(&mut setup.context, &setup.user_ata),
        INITIAL_BALANCE
    );
    assert_eq!(
        get_token_balance(&mut setup.context, &setup.treasury_token_account),
        0
    );
    assert_eq!(
        get_mint_supply(&mut setup.context, &setup.mint),
        supply_before
    );
}

/// The treasury's exemptions do not reach past the refusal: it moves its fees out with
/// ConfidentialWithdrawFunds instead.
#[test]
fn test_withdraw_funds_refuses_the_treasury_on_a_confidential_only_mint() {
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new().pubkey();
    set_mint(&mut context, &mint);
    let (withdraw_config, _) =
        set_withdraw_config(&mut context, &mint, TEST_WITHDRAW_FEE, &admin.pubkey());
    set_confidential_only(&mut context, &withdraw_config);
    setup_test_balances(&mut context, &admin, &mint, INITIAL_BALANCE);
    let admin_ata = channel_ata(&admin.pubkey(), &mint);
    let instruction = WithdrawFundsBuilder::new()
        .user(admin.pubkey())
        .mint(mint)
        .token_account(admin_ata)
        .withdraw_config(withdraw_config)
        .treasury_token_account(Pubkey::new_unique())
        .amount(WITHDRAW_AMOUNT)
        .instruction();
    let result = context.send_transaction_with_signers(instruction, &[&admin]);
    assert_program_error(result, CONFIDENTIAL_ONLY_MINT_ERROR);
    assert_eq!(get_token_balance(&mut context, &admin_ata), INITIAL_BALANCE);
}

#[test]
fn test_confidential_withdraw_funds_accepts_a_confidential_only_mint() {
    let mut setup = Setup::new(INITIAL_BALANCE);
    set_confidential_only(&mut setup.context, &setup.withdraw_config);

    let instruction = setup.confidential(WITHDRAW_AMOUNT, None);
    let user = setup.user.insecure_clone();
    setup
        .context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("a confidential-only mint takes confidential withdrawals");

    assert_eq!(
        get_token_balance(&mut setup.context, &setup.user_ata),
        INITIAL_BALANCE - WITHDRAW_AMOUNT - TEST_WITHDRAW_FEE
    );
}

/// The treasury pays no fee and has no minimum, and a zero-fee mint never reads the
/// treasury account, exactly as for WithdrawFunds.
#[test]
fn test_confidential_withdraw_funds_keeps_the_fee_exemptions() {
    // Treasury: below the minimum, no fee, treasury account unread.
    let mut context = TestContext::new();
    let admin = Keypair::new();
    let mint = Keypair::new().pubkey();
    set_mint(&mut context, &mint);
    let (withdraw_config, _) = set_withdraw_config_with_minimum(
        &mut context,
        &mint,
        TEST_WITHDRAW_FEE,
        &admin.pubkey(),
        WITHDRAW_AMOUNT,
    );
    setup_test_balances(&mut context, &admin, &mint, INITIAL_BALANCE);
    let admin_ata = channel_ata(&admin.pubkey(), &mint);
    let instruction = ConfidentialWithdrawFundsBuilder::new()
        .user(admin.pubkey())
        .mint(mint)
        .token_account(admin_ata)
        .withdraw_config(withdraw_config)
        .treasury_token_account(Pubkey::new_unique())
        .amount(1)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&admin])
        .expect("the treasury withdraws below the minimum without a fee");
    assert_eq!(
        get_token_balance(&mut context, &admin_ata),
        INITIAL_BALANCE - 1
    );

    // Zero-fee mint: a treasury account that does not exist is never read.
    let mut context = TestContext::new();
    let user = Keypair::new();
    let mint = Keypair::new().pubkey();
    set_mint(&mut context, &mint);
    let (withdraw_config, _) = set_withdraw_config(&mut context, &mint, 0, &Pubkey::new_unique());
    setup_test_balances(&mut context, &user, &mint, INITIAL_BALANCE);
    let user_ata = channel_ata(&user.pubkey(), &mint);
    let instruction = ConfidentialWithdrawFundsBuilder::new()
        .user(user.pubkey())
        .mint(mint)
        .token_account(user_ata)
        .withdraw_config(withdraw_config)
        .treasury_token_account(Pubkey::new_unique())
        .amount(WITHDRAW_AMOUNT)
        .instruction();
    context
        .send_transaction_with_signers(instruction, &[&user])
        .expect("a zero-fee confidential withdrawal should succeed");
    assert_eq!(
        get_token_balance(&mut context, &user_ata),
        INITIAL_BALANCE - WITHDRAW_AMOUNT
    );
}

#[test]
fn test_confidential_withdraw_funds_makes_the_same_checks_as_withdraw_funds() {
    let cases: &[(&str, Case)] = &[
        ("zero amount", || {
            let s = Setup::new(INITIAL_BALANCE);
            let ix = s.withdraw(0);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], ZERO_AMOUNT_ERROR)
        }),
        ("below minimum", || {
            let mut s = Setup::new(INITIAL_BALANCE);
            let (config, treasury) = set_withdraw_config_with_minimum(
                &mut s.context,
                &s.mint,
                TEST_WITHDRAW_FEE,
                &Pubkey::new_unique(),
                WITHDRAW_AMOUNT,
            );
            s.withdraw_config = config;
            s.treasury_token_account = treasury;
            let ix = s.withdraw(WITHDRAW_AMOUNT - 1);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], AMOUNT_BELOW_MINIMUM_ERROR)
        }),
        ("insufficient funds", || {
            let s = Setup::new(WITHDRAW_AMOUNT / 2);
            let ix = s.withdraw(WITHDRAW_AMOUNT);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], TOKEN_INSUFFICIENT_FUNDS_ERROR)
        }),
        ("balance covers the amount but not the fee", || {
            let s = Setup::new(WITHDRAW_AMOUNT);
            let ix = s.withdraw(WITHDRAW_AMOUNT);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], TOKEN_INSUFFICIENT_FUNDS_ERROR)
        }),
        ("config not initialized", || {
            let mut s = Setup::new(INITIAL_BALANCE);
            let other_mint = Pubkey::new_unique();
            set_mint(&mut s.context, &other_mint);
            setup_test_balances(&mut s.context, &s.user, &other_mint, INITIAL_BALANCE);
            s.mint = other_mint;
            s.user_ata = channel_ata(&s.user.pubkey(), &other_mint);
            s.withdraw_config = find_withdraw_config_pda(&other_mint).0;
            let ix = s.withdraw(WITHDRAW_AMOUNT);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], WITHDRAW_CONFIG_NOT_INITIALIZED_ERROR)
        }),
        ("config of another mint", || {
            let mut s = Setup::new(INITIAL_BALANCE);
            let other_mint = Pubkey::new_unique();
            set_mint(&mut s.context, &other_mint);
            let (other_config, _) = set_withdraw_config(
                &mut s.context,
                &other_mint,
                TEST_WITHDRAW_FEE,
                &Pubkey::new_unique(),
            );
            s.withdraw_config = other_config;
            let ix = s.withdraw(WITHDRAW_AMOUNT);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INVALID_WITHDRAW_CONFIG_ERROR)
        }),
        ("wrong treasury account", || {
            let mut s = Setup::new(INITIAL_BALANCE);
            let attacker = Pubkey::new_unique();
            let attacker_ata = channel_ata(&attacker, &s.mint);
            set_token_balance(&mut s.context, &attacker_ata, &s.mint, &attacker, 0);
            s.treasury_token_account = attacker_ata;
            let ix = s.withdraw(WITHDRAW_AMOUNT);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INVALID_TREASURY_ACCOUNT_ERROR)
        }),
        ("wrong mint", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[1] = AccountMeta::new(Pubkey::new_unique(), false);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INVALID_MINT_ERROR)
        }),
        ("user not a signer", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[0] = AccountMeta::new_readonly(s.user.pubkey(), false);
            (s, ix, vec![], MISSING_REQUIRED_SIGNATURE_ERROR)
        }),
        ("wrong associated token program", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[4] = AccountMeta::new_readonly(Pubkey::new_unique(), false);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INCORRECT_PROGRAM_ID_ERROR)
        }),
        ("wrong token program", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[3] = AccountMeta::new_readonly(Pubkey::new_unique(), false);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INCORRECT_PROGRAM_ID_ERROR)
        }),
        ("legacy SPL Token program", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[3] = AccountMeta::new_readonly(spl_token::ID, false);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INCORRECT_PROGRAM_ID_ERROR)
        }),
        ("source is not the user's ATA", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[2] = AccountMeta::new(Pubkey::new_unique(), false);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INVALID_INSTRUCTION_DATA_ERROR)
        }),
        ("permanent delegate withdrawing a user's balance", || {
            let mut s = Setup::new(INITIAL_BALANCE);
            let delegate = Keypair::new();
            set_mint_with_delegate(&mut s.context, &s.mint, &delegate.pubkey());
            let (config, treasury) = set_withdraw_config(
                &mut s.context,
                &s.mint,
                TEST_WITHDRAW_FEE,
                &delegate.pubkey(),
            );
            s.withdraw_config = config;
            s.treasury_token_account = treasury;
            s.context
                .airdrop_if_required(&delegate.pubkey(), 1_000_000_000)
                .unwrap();
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts[0] = AccountMeta::new_readonly(delegate.pubkey(), true);
            (s, ix, vec![delegate], INVALID_INSTRUCTION_DATA_ERROR)
        }),
        ("not enough accounts", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.accounts.truncate(3);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], NOT_ENOUGH_ACCOUNT_KEYS_ERROR)
        }),
        ("instruction data too short", || {
            let s = Setup::new(INITIAL_BALANCE);
            let ix = Instruction {
                program_id: PRIVATE_CHANNEL_WITHDRAW_PROGRAM_ID,
                accounts: vec![],
                data: vec![WITHDRAW_FUNDS_DISCRIMINATOR, 1, 2],
            };
            (s, ix, vec![], INVALID_INSTRUCTION_DATA_ERROR)
        }),
        ("non-canonical destination tag", || {
            let s = Setup::new(INITIAL_BALANCE);
            let mut ix = s.withdraw(WITHDRAW_AMOUNT);
            ix.data[9] = 2;
            ix.data.extend_from_slice(&[0u8; 32]);
            let signer = s.user.insecure_clone();
            (s, ix, vec![signer], INVALID_INSTRUCTION_DATA_ERROR)
        }),
    ];

    for (name, case) in cases {
        assert_same_refusal(name, *case);
    }
}

/// The indexer and the gateway read both withdrawals by position, so the account order and
/// the data layout must match WithdrawFunds byte for byte apart from the discriminator.
#[test]
fn test_confidential_withdraw_funds_matches_the_withdraw_funds_layout() {
    let setup = Setup::new(INITIAL_BALANCE);
    let destination = Pubkey::new_unique();
    let mut public = WithdrawFundsBuilder::new();
    public
        .user(setup.user.pubkey())
        .mint(setup.mint)
        .token_account(setup.user_ata)
        .withdraw_config(setup.withdraw_config)
        .treasury_token_account(setup.treasury_token_account)
        .amount(WITHDRAW_AMOUNT)
        .destination(destination);
    let confidential = setup.confidential(WITHDRAW_AMOUNT, Some(destination));

    assert_eq!(as_confidential(public.instruction()), confidential);
    assert_eq!(CONFIDENTIAL_WITHDRAW_FUNDS_DISCRIMINATOR, 2);
}
