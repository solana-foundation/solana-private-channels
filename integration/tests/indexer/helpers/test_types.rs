#![allow(dead_code)]

use solana_sdk::pubkey::Pubkey;

pub const LOCAL_RPC_URL: &str = "http://localhost:18899";
pub const DEFAULT_INDEXER_DB_URL: &str =
    "postgres://private_channel:private_channel_password@localhost:5434/indexer";
pub const INDEXER_DB_ENV_VAR: &str = "TEST_INDEXER_DB_URL";

pub const NUM_USERS: usize = 5;
pub const DEPOSITS_PER_USER: usize = 10;
pub const BASE_AMOUNT: u64 = 10_000;
// 240 s gives sufficient headroom for parallel cargo-test runs where multiple validators
// and multiple Postgres containers compete for CPU. Under nextest (one process per test)
// the timeout is never reached.
//
// Coverage-instrumented builds are ~2-3x slower than release/debug. CI sets
// PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS=600 for the coverage target to give those runs
// enough headroom; all other invocations fall back to 240 s.
//
// Every wait helper takes its budget from here rather than from an argument. A number
// written at the call site is tuned against whatever build its author ran, so it silently
// ignores the env var above and fails the slowest run in CI while passing locally.
pub static WAIT_TIMEOUT_SECS: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("PRIVATE_CHANNEL_TEST_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(240)
});

pub const ESCROW_INSTANCE_SEEDS_PRIVATE_KEY: [u8; 64] = [
    253, 137, 127, 96, 208, 56, 227, 155, 179, 196, 123, 197, 226, 86, 137, 104, 38, 0, 15, 229,
    175, 29, 110, 195, 49, 39, 28, 16, 184, 135, 196, 49, 46, 124, 200, 66, 209, 118, 114, 166,
    209, 41, 204, 119, 179, 128, 230, 85, 76, 156, 48, 21, 16, 75, 236, 76, 216, 53, 153, 60, 41,
    227, 56, 85,
];

#[derive(Debug, Clone)]
pub struct UserTransaction {
    pub user_pubkey: Pubkey,
    pub amount: u64,
    pub signature: String,
    pub slot: u64,
    pub tx_type: TransactionType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
}
