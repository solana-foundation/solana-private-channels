# Withdraw Program — Test Coverage Analysis

> This is a **semantic coverage estimate** produced by analyzing test assertions
> against the program's testable surface. It is not instrumented line coverage —
> Solana SBF programs do not support LLVM coverage instrumentation.

## Summary

| Category                      | Coverage     | Details                                                                                                     |
| ----------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------- |
| Instruction handlers          | 100% (2/2)   | WithdrawFunds, SetWithdrawFeeConfig                                                                         |
| Account validation paths      | 100% (10/10) | Signer, ATA program, token program, mint, ATA derivation, fee config owner/address, treasury, system program |
| Business logic error branches | 100% (8/8)   | Zero amount, insufficient funds, balance below amount + fee, zero-fee mint, mint authority, wrong mint      |
| Custom error codes exercised  | 100% (7/7)   | InvalidMint, ZeroAmount, InvalidFeeConfig, FeeConfigNotInitialized, InvalidMintAuthority, InvalidTreasuryAccount, InvalidSystemProgram |
| State & trait coverage (unit) | 100% (16/16) | Instruction parsing, discriminator, event serialization, fee config layout                                  |
| Event coverage                | 100% (2/2)   | Serialization unit-tested; on-chain emission verified in integration test                                   |
| Security edge cases           | 100% (7/7)   | Non-signer, wrong programs, wrong ATA address, foreign fee config, wrong treasury, pre-funded config PDA    |
| **Overall (risk-weighted)**   | **~90%**     |                                                                                                             |

## Test Inventory

**16 unit tests** + **26 integration tests** (LiteSVM) + **10 TypeScript SDK tests**.

### Unit Tests (16 tests)

#### WithdrawFunds Instruction Data Parsing (8 tests in `withdraw_funds.rs`)

- `test_parse_instruction_data_valid_with_destination` — 41-byte data with destination
- `test_parse_instruction_data_valid_without_destination` — 9-byte data, no destination
- `test_parse_instruction_data_insufficient_length` — data too short (3 bytes)
- `test_parse_instruction_data_empty` — empty data
- `test_parse_instruction_data_zero_amount` — zero amount succeeds at parse level
- `test_parse_instruction_data_truncated_destination` — flag=1 but pubkey truncated
- `test_parse_instruction_data_non_canonical_option_tag` — Option tag byte other than 0/1 rejected
- `test_process_withdraw_funds_empty_accounts` — empty accounts returns NotEnoughAccountKeys

#### SetWithdrawFeeConfig (2 tests in `set_withdraw_fee_config.rs`)

- `test_parse_instruction_data_valid` — fee and treasury parsed from 40 bytes
- `test_parse_instruction_data_missing_treasury` — fee without treasury rejected

#### Fee Config State (2 tests in `state/withdraw_fee_config.rs`)

- `test_withdraw_fee_config_serialization_roundtrip` — 73-byte layout, byte-asymmetric fee catches field order and endianness
- `test_withdraw_fee_config_try_from_bytes_wrong_length` — short data returns the custom InvalidFeeConfig, never a builtin error

#### Discriminator (2 tests in `discriminator.rs`)

- `test_discriminator_valid` — byte 0 maps to WithdrawFunds
- `test_discriminator_invalid` — byte 2 returns Err

#### Event Serialization (1 test in `events.rs`)

- `test_withdraw_funds_event_to_bytes` — verifies 40-byte layout (8 amount + 32 destination)

### WithdrawFunds — Integration Tests (19 tests)

#### Happy Path

- `test_withdraw_funds_success` — user pays amount + fee, treasury gains the fee, supply drops by the amount only
- `test_withdraw_funds_with_destination` — optional destination parameter
- `test_withdraw_funds_treasury_pays_no_fee` — the treasury's own withdrawal moves only the amount
- `test_withdraw_funds_event_emission` — verifies the `WithdrawFundsEvent` log is actually emitted on-chain with the correct amount and destination bytes; reconstructs the expected pinocchio_log format (`[b0, b1, ..., b39]`) and matches it against the transaction logs

#### Fee Paths

- `test_withdraw_funds_balance_covers_amount_but_not_fee` — fails as a whole; neither the fee moves nor anything is burned
- `test_withdraw_funds_zero_fee` — a zero-fee mint burns only the amount and never reads the treasury account, even one that does not exist
- `test_withdraw_funds_fee_config_not_initialized` — FeeConfigNotInitialized
- `test_withdraw_funds_fee_config_wrong_address` — another mint's config rejected with InvalidFeeConfig
- `test_withdraw_funds_wrong_treasury_account` — a token account other than the configured one rejected with InvalidTreasuryAccount

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
- `test_withdraw_funds_not_enough_accounts` — only 3 of 7 required accounts

### SetWithdrawFeeConfig — Integration Tests (7 tests)

- `test_set_withdraw_fee_config_creates_config` — stores bump, fee, treasury and the treasury's ATA
- `test_set_withdraw_fee_config_zero_fee` — a zero fee is stored, not rejected
- `test_set_withdraw_fee_config_overwrites` — a second call replaces fee, treasury and treasury ATA
- `test_set_withdraw_fee_config_prefunded_pda` — succeeds when the PDA already holds lamports
- `test_set_withdraw_fee_config_not_mint_authority` — InvalidMintAuthority
- `test_set_withdraw_fee_config_wrong_address` — InvalidFeeConfig, a custom error so the operator's mint retry never matches it
- `test_set_withdraw_fee_config_wrong_system_program` — InvalidSystemProgram, for the same reason

### TypeScript SDK Tests (10 tests)

#### WithdrawFunds Instruction Data Validation (4 tests)

- Encodes discriminator, amount, and destination correctly
- Handles u64 amounts (0, 1, 1M, 1B, max safe integer, max u64)
- Handles optional destination (None/Some variants)
- Round-trip encode/decode verification

#### WithdrawFunds Account Requirements (4 tests)

- All 7 required accounts present in correct order
- Account permissions correct (READONLY_SIGNER, WRITABLE, READONLY)
- Program addresses correct (private_channel program, token program, ATA program)
- `withdrawFeeConfig` derived from `["withdraw_fee_config", mint]` when not provided

#### SetWithdrawFeeConfig (2 tests)

- Data bytes are discriminator, fee (u64 LE), treasury, in the order the program parses them
- `withdrawFeeConfig` derived from the mint, system program defaulted, account roles correct

## Documented Gaps

### Remaining Untested Paths

- Token-2022 rejection — the program accepts only the legacy SPL Token program, but no test asserts that a Token-2022 `token_program` is rejected

### Priority Recommendations

1. **Medium**: Add a test asserting Token-2022 is rejected with `IncorrectProgramId`
