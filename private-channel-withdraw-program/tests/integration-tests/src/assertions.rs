use solana_sdk::pubkey::Pubkey;
use spl_token_2022::{extension::StateWithExtensions, state::Account as TokenAccount};

use crate::utils::TestContext;

pub fn assert_balance_changed(
    context: &mut TestContext,
    token_account: &Pubkey,
    initial_balance: u64,
    expected_change: i64,
) {
    let account = context
        .get_account(token_account)
        .expect("Token account should exist");
    let current_balance = StateWithExtensions::<TokenAccount>::unpack(&account.data)
        .unwrap()
        .base
        .amount;
    let expected_balance = (initial_balance as i64 + expected_change) as u64;
    assert_eq!(
        current_balance, expected_balance,
        "Token balance change should match expected. Initial: {}, Current: {}, Expected Change: {}",
        initial_balance, current_balance, expected_change
    );
}
