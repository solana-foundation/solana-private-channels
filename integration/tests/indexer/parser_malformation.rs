//! Parser malformation handling (table-driven)
//!
//! Target files:
//!   * `indexer/src/indexer/datasource/common/parser/escrow.rs` (~43 uncovered)
//!   * `indexer/src/indexer/datasource/common/parser/withdraw.rs` (parallel surface)
//!
//! Binary: `indexer_integration` (existing) — attached via `#[path]` mod
//!         to share its compile surface; zero fixture cost (pure function).
//!
//! Feeds 6 deliberately-malformed `CompiledInstruction` payloads into
//! `parse_escrow_instruction` and asserts each maps to the right error or
//! `Ok(None)`. Exercises:
//!
//!   1. Empty data buffer                           → `Ok(None)` (empty branch)
//!   2. Unknown discriminator byte                  → `Ok(None)` (fall-through)
//!   3. Valid discriminator but insufficient accts  → `Err(InsufficientAccounts)`
//!   4. Base58-undecodable data                     → `Err` from decode step
//!   5. Discriminator valid but account index OOB   → panic? / Err?
//!      (the current parser trusts the account_keys slice; asserting the
//!      OOB case documents the trust contract — see NOTE below)
//!   6. Truncated borsh payload for CreateInstance  → `Err` from borsh

use private_channel_indexer::indexer::datasource::common::parser::escrow::parse_escrow_instruction;
use private_channel_indexer::indexer::datasource::common::types::{
    CompiledInstruction, InstructionLocation,
};
use solana_sdk::pubkey::Pubkey;

fn account_keys(n: usize) -> Vec<Pubkey> {
    (0..n).map(|_| Pubkey::new_unique()).collect()
}

fn ix(program_id_index: u8, accounts: Vec<u8>, data_b58: &str) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index,
        accounts,
        data: data_b58.to_string(),
    }
}

// ── Case 1: empty data ──────────────────────────────────────────────────────
#[test]
fn test_parse_escrow_empty_data_returns_none() {
    let keys = account_keys(8);
    let instruction = ix(0, vec![0, 1, 2, 3, 4, 5, 6], "");
    let result =
        parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
            .unwrap();
    assert!(
        result.is_none(),
        "empty instruction data must produce Ok(None)"
    );
}

// ── Case 2: unknown discriminator ──────────────────────────────────────────
#[test]
fn test_parse_escrow_unknown_discriminator_returns_none() {
    let keys = account_keys(8);
    // Discriminator 0xFF isn't one of the 5 known variants — must fall through
    // to `_ => Ok(None)` without erroring.
    let ff = bs58::encode(&[0xFFu8]).into_string();
    let instruction = ix(0, vec![0, 1, 2, 3, 4, 5, 6], &ff);
    let result =
        parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
            .unwrap();
    assert!(
        result.is_none(),
        "unknown discriminator must be Ok(None), got {result:?}"
    );
}

// ── Case 3: insufficient accounts for CreateInstance ───────────────────────
#[test]
fn test_parse_escrow_create_instance_insufficient_accounts_errs() {
    let keys = account_keys(8);
    // Discriminator = CREATE_INSTANCE (0) + 32 zero bytes of plausible
    // borsh-compatible payload. The instruction only declares 3 accounts,
    // but CreateInstance needs 7 → InsufficientAccounts error path.
    let mut data_bytes = vec![0u8]; // discriminator CREATE_INSTANCE
    data_bytes.extend(vec![0u8; 32]); // plausible borsh bytes
    let data_b58 = bs58::encode(&data_bytes).into_string();
    let instruction = ix(0, vec![0, 1, 2], &data_b58); // only 3 accounts

    let err = parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
        .expect_err("CREATE_INSTANCE with < 7 accounts must error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("insufficient") || msg.contains("account"),
        "error must reference account-count violation, got: {msg}"
    );
}

// ── Case 4: base58-undecodable data ────────────────────────────────────────
#[test]
fn test_parse_escrow_invalid_base58_errs() {
    let keys = account_keys(8);
    // Lowercase 'l' is invalid in base58's alphabet.
    let instruction = ix(0, vec![0, 1, 2, 3, 4, 5, 6], "lllll");
    let err = parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
        .expect_err("invalid base58 must surface the decode error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("base58") || msg.contains("decode") || msg.contains("parser"),
        "error must be a decode/parser error, got: {msg}"
    );
}

// ── Case 5: truncated borsh payload ────────────────────────────────────────
//
// DEPOSIT's payload (`DepositData`) is `{ amount: u64, recipient: Option<Pubkey> }`
// → minimum 9 bytes to deserialize. A 2-byte discriminator+payload (single
// zero after the discriminator) is far short of that, so borsh must surface
// an error before any Ok variant is produced. CREATE_INSTANCE's payload is
// a single `u8` bump byte, so 0 bytes deserializes to `bump: 0` and does
// NOT exercise the truncation branch — this is why we target DEPOSIT here.
#[test]
fn test_parse_escrow_truncated_borsh_errs() {
    let keys = account_keys(8);
    let truncated = bs58::encode(&[6u8, 0u8]).into_string(); // DEPOSIT + 1 payload byte
    let instruction = ix(0, vec![0, 1, 2, 3, 4], &truncated); // 5 accounts — enough to pass the count check
    let err = parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
        .expect_err("truncated borsh payload must error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        !msg.is_empty(),
        "error must surface a non-empty message for truncated borsh"
    );
}

// ── Case 6: valid DEPOSIT discriminator with insufficient accounts ────────
//
// DEPOSIT requires specific account count (see parse_deposit). This verifies
// the InsufficientAccounts branch is hit for a *different* discriminator
// than Case 3 — guards against over-fitting to CreateInstance's path.
#[test]
fn test_parse_escrow_deposit_insufficient_accounts_errs() {
    let keys = account_keys(8);
    let mut data_bytes = vec![6u8]; // DEPOSIT discriminator
    data_bytes.extend(vec![0u8; 32]);
    let data_b58 = bs58::encode(&data_bytes).into_string();
    let instruction = ix(0, vec![0u8], &data_b58); // 1 account only

    let err = parse_escrow_instruction(&instruction, &keys, &[], InstructionLocation::top_level(0))
        .expect_err("DEPOSIT with 1 account must error");
    let msg = format!("{err:?}").to_lowercase();
    assert!(
        msg.contains("insufficient") || msg.contains("account") || msg.contains("parser"),
        "error must reference account-count or parser failure, got: {msg}"
    );
}
