# Withdraw Program — Test Coverage Analysis

> This is a **semantic coverage estimate** produced by analyzing test assertions
> against the program's testable surface. It is not instrumented line coverage —
> Solana SBF programs do not support LLVM coverage instrumentation.

## Summary

| Category                      | Coverage     | Details                                                                                 |
| ----------------------------- | ------------ | --------------------------------------------------------------------------------------- |
| Instruction handlers          | 100% (1/1)   | WithdrawFunds tested                                                                    |
| Account validation paths      | 100% (5/5)   | Signer, ATA program, token program, mint, ATA derivation                                |
| Business logic error branches | 100% (5/5)   | Zero amount, insufficient funds, wrong mint, truncated destination, not enough accounts |
| Custom error codes exercised  | 100% (2/2)   | InvalidMint, ZeroAmount                                                                 |
| State & trait coverage (unit) | 100% (11/11) | Instruction parsing, discriminator, event serialization                                 |
| Event coverage                | 100% (2/2)   | Serialization unit-tested; on-chain emission verified in integration test               |
| Security edge cases           | 100% (3/3)   | Non-signer, wrong programs, wrong ATA address                                           |
| **Overall (risk-weighted)**   | **~90%**     |                                                                                         |

## Test Inventory

**11 unit tests** + **13 integration tests** (LiteSVM) + **7 TypeScript SDK tests**.

### Unit Tests (11 tests)

#### Instruction Data Parsing (8 tests in `withdraw_funds.rs`)

- `test_parse_instruction_data_valid_with_destination` — 41-byte data with destination
- `test_parse_instruction_data_valid_without_destination` — 9-byte data, no destination
- `test_parse_instruction_data_insufficient_length` — data too short (3 bytes)
- `test_parse_instruction_data_empty` — empty data
- `test_parse_instruction_data_zero_amount` — zero amount succeeds at parse level
- `test_parse_instruction_data_truncated_destination` — flag=1 but pubkey truncated
- `test_parse_instruction_data_non_canonical_option_tag` — Option tag byte other than 0/1 rejected
- `test_process_withdraw_funds_empty_accounts` — empty accounts returns NotEnoughAccountKeys

#### Discriminator (2 tests in `discriminator.rs`)

- `test_discriminator_valid` — byte 0 maps to WithdrawFunds
- `test_discriminator_invalid` — byte 1 returns Err

#### Event Serialization (1 test in `events.rs`)

- `test_withdraw_funds_event_to_bytes` — verifies 40-byte layout (8 amount + 32 destination)

### WithdrawFunds — Integration Tests (13 tests)

#### Happy Path

- `test_withdraw_funds_success` — basic withdrawal, balance verified
- `test_withdraw_funds_with_destination` — optional destination parameter
- `test_withdraw_funds_event_emission` — verifies the `WithdrawFundsEvent` log is actually emitted on-chain with the correct amount and destination bytes; reconstructs the expected pinocchio_log format (`[b0, b1, ..., b39]`) and matches it against the transaction logs

#### Error Paths

- `test_withdraw_funds_insufficient_funds` — SPL Token insufficient funds
- `test_withdraw_funds_zero_amount` — ZeroAmount custom error
- `test_withdraw_funds_invalid_instruction_data_too_short` — malformed data rejected
- `test_withdraw_funds_wrong_mint` — InvalidMint custom error
- `test_withdraw_funds_non_signer_user` — MissingRequiredSignature

#### Account Validation

- `test_withdraw_funds_wrong_ata_program` — wrong ATA program address (IncorrectProgramId)
- `test_withdraw_funds_wrong_token_program` — wrong token program address (IncorrectProgramId)
- `test_withdraw_funds_wrong_ata_address` — ATA PDA mismatch (InvalidInstructionData)
- `test_withdraw_funds_invalid_discriminator` — byte 255 discriminator rejected
- `test_withdraw_funds_not_enough_accounts` — only 3 of 5 required accounts

### WithdrawFunds — TypeScript SDK Tests (7 tests)

#### Instruction Data Validation (4 tests)

- Encodes discriminator, amount, and destination correctly
- Handles u64 amounts (0, 1, 1M, 1B, max safe integer, max u64)
- Handles optional destination (None/Some variants)
- Round-trip encode/decode verification

#### Account Requirements (3 tests)

- All 5 required accounts present in correct order
- Account permissions correct (READONLY_SIGNER, WRITABLE, READONLY)
- Program addresses correct (private_channel program, token program, ATA program)

## Documented Gaps

### Remaining Untested Paths

- Token-2022 rejection — the program accepts only the legacy SPL Token program, but no test asserts that a Token-2022 `token_program` is rejected

### Priority Recommendations

1. **Medium**: Add a test asserting Token-2022 is rejected with `IncorrectProgramId`
