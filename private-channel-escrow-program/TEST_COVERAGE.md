# Escrow Program — Test Coverage Analysis

> This is a **semantic coverage estimate** produced by analyzing test assertions
> against the program's testable surface. It is not instrumented line coverage —
> Solana SBF programs do not support LLVM coverage instrumentation.

## Summary

| Category                      | Coverage     | Details                                                        |
| ----------------------------- | ------------ | -------------------------------------------------------------- |
| Instruction handlers          | 100% (9/9)   | All handlers have success + error tests                        |
| Account validation paths      | 95% (19/20)  | Signer, PDA, owner, mutability, ATA program, system program    |
| Business logic error branches | 93% (14/15)  | Nonce replay, balance verification, Token2022 extensions       |
| Custom error codes exercised  | 100% (13/13) | All custom errors tested                                       |
| State & trait coverage (unit) | 100% (14/14) | Instruction data parsing for all handlers                      |
| Event coverage                | 100% (9/9)   | All events emitted in integration tests                        |
| Security edge cases           | 100% (14/14) | Double-spend, foreign bitmap, Token2022, generation boundaries |
| **Overall (risk-weighted)**   | **~95%**     |                                                                |

> The percentages above predate the SMT-to-bitmap migration and have not been
> re-derived against the current surface. The inventory below is accurate.

## Test Inventory

**55 unit tests** (instruction data parsing, state serialization, error ABI, event encoding, bitmap logic) + **84 integration tests** (end-to-end behavior).

### CreateInstance (6 integration tests)

- `test_create_instance_success` — happy path; also asserts the withdrawal bitmap is created on generation 0 with every bit clear
- `test_create_instance_duplicate` — duplicate creation fails
- `test_create_instance_invalid_pda` — wrong instance PDA rejected
- `test_create_instance_invalid_admin_not_signer` — unsigned admin rejected
- `test_create_instance_invalid_event_authority` — invalid event authority PDA
- `test_create_instance_invalid_system_program` — wrong system program address

### AllowMint (10 integration tests)

- `test_allow_mint_success` — SPL Token mint
- `test_allow_mint_twice_repins_instead_of_failing` — since BlockMint stopped closing the PDA, a second AllowMint re-pins the profile and re-opens both gates; expires the blockhash so the second transaction is not dropped as a replay
- `test_allow_mint_invalid_pda` — wrong PDA rejected
- `test_allow_mint_invalid_admin_not_signer` — unsigned admin rejected
- `test_allow_mint_invalid_admin` — wrong admin rejected
- `test_allow_mint_invalid_instance_account_owner` — wrong owner rejected
- `test_allow_mint_token_2022_basic_success` — Token2022 mint allowed
- `test_allow_mint_token_2022_permanent_delegate_accepted` — permanent-delegate Token-2022 mint allowed; drain detection is enforced by the operator at withdrawal time
- `test_allow_mint_token_2022_pausable_accepted` — pausable Token-2022 mint allowed; pause state is enforced by the operator at withdrawal time
- `test_allow_mint_token_2022_transfer_hook_allowed` — hook mints are allowlistable, and the escrow ATA the CPI creates carries `TransferHookAccount`

### BlockMint (11 integration tests)

- `test_block_mint_success` — happy path; the PDA survives with both gates set
- `test_block_mint_allowed_mint_not_found` — nonexistent mint fails
- `test_block_mint_invalid_pda` — wrong PDA rejected
- `test_block_mint_invalid_admin_not_signer` — unsigned admin rejected
- `test_block_mint_invalid_admin` — wrong admin rejected
- `test_block_mint_invalid_instance_account_owner` — wrong owner rejected
- `test_block_mint_mismatched_mint` — PDA/mint mismatch rejected
- `test_block_mint_prevents_deposit` — a deposit-blocked mint fails a subsequent deposit with DepositsBlockedForMint
- `test_allow_block_allow_cycle` — a mint can be re-allowed after being blocked; deposit succeeds once re-allowed
- `test_block_mint_deposits_only_still_allows_release` — blocking deposits leaves already-escrowed funds withdrawable
- `test_block_mint_withdrawals_prevents_release` — the withdrawal gate alone rejects a release with WithdrawalsBlockedForMint

### AddOperator (6 integration tests)

- `test_add_operator_success` — happy path
- `test_add_operator_duplicate` — duplicate operator fails
- `test_add_operator_invalid_pda` — wrong PDA rejected
- `test_add_operator_invalid_admin_not_signer` — unsigned admin rejected
- `test_add_operator_invalid_admin` — wrong admin rejected
- `test_add_operator_invalid_instance_account_owner` — wrong owner rejected

### RemoveOperator (6 integration tests)

- `test_remove_operator_success` — happy path with rent reclamation
- `test_remove_operator_nonexistent` — nonexistent operator fails
- `test_remove_operator_invalid_admin_not_signer` — unsigned admin rejected
- `test_remove_operator_invalid_admin` — wrong admin rejected
- `test_remove_operator_invalid_instance_account_owner` — wrong owner rejected
- `test_remove_operator_prevents_release_funds` — once an operator PDA is closed, release_funds using that PDA fails with InvalidAccountData

### SetNewAdmin (7 integration tests)

- `test_set_new_admin_success` — happy path
- `test_set_new_admin_invalid_current_admin_not_signer` — unsigned current admin
- `test_set_new_admin_invalid_current_admin` — wrong admin rejected
- `test_set_new_admin_invalid_instance_account_owner` — wrong owner rejected
- `test_set_new_admin_invalid_new_admin_not_signer` — new admin must sign
- `test_set_new_admin_old_admin_locked_out` — after transfer, old admin's allow_mint attempt is rejected with InvalidAdmin
- `test_set_new_admin_existing_operators_still_valid` — operator PDAs are keyed to the instance, not the admin; they remain valid after an admin change

### Deposit (20 integration tests)

- `test_deposit_success` — happy path
- `test_deposit_with_recipient` — optional recipient parameter
- `test_deposit_insufficient_funds` — insufficient balance error
- `test_deposit_mint_not_allowed` — unapproved mint rejected
- `test_deposit_invalid_instruction_data_too_short` — malformed data
- `test_deposit_not_enough_accounts` — missing accounts
- `test_deposit_token_2022_basic_success` — Token2022 deposit
- `test_deposit_token_2022_transfer_hook_forwards_extras` — the hook runs exactly once and sees every account its `ExtraAccountMetaList` declares
- `test_deposit_token_2022_transfer_hook_without_extras_fails` — omitting the extras fails the transfer rather than skipping the hook
- `test_deposit_rejects_signer_bearing_hook_extra` — a hostile `ExtraAccountMetaList` names the fee payer as a signer extra; the stripped signer bit leaves the hook's drain CPI unsigned, so the deposit reverts and the attacker gets nothing
- `test_deposit_token_2022_transfer_fee_success` — the escrow credits the measured balance delta, so the depositor is credited net of the fee
- `test_deposit_invalid_associated_token_program` — wrong ATA program rejected
- `test_multiple_depositors_same_instance` — three users deposit to same instance
- `test_deposit_wrong_user_ata` — passing another user's ATA as the user_ata is rejected with InvalidInstructionData
- `test_deposit_wrong_instance_ata` — passing an instance ATA for a different mint is rejected with InvalidInstructionData
- `test_deposit_rejected_after_mint_decimals_change` — MintProfileChanged; TransferChecked cannot catch this since it validates the decimals it is handed against the mint itself
- `test_deposit_rejected_after_mint_token_program_change` — MintProfileChanged; the ATAs for the new program are left uncreated, so this also pins the check running before `validate_ata`
- `test_deposit_rejected_after_mint_gains_extension` — MintProfileChanged; decimals, token program and freeze authority all still match, so only the extension bitmask can catch the recreate
- `test_deposit_rejected_after_mint_gains_freeze_authority` — MintProfileChanged; a freeze authority cannot be re-enabled once revoked, so gaining one means a recreate
- `test_deposit_succeeds_after_freeze_authority_revoked` — the other direction; revoking leaves the mint strictly safer and must not strand deposits, so tightening the check into an equality breaks this

### ReleaseFunds (21 integration tests)

- `test_release_funds_success` — happy path; asserts the nonce bit is consumed
- `test_release_funds_insufficient_funds` — insufficient balance error
- `test_release_funds_not_operator` — wrong operator rejected
- `test_release_funds_invalid_instruction_data_too_short` — malformed data
- `test_release_funds_operator_not_signer` — unsigned operator rejected
- `test_release_funds_bitmap_tracks_many_nonces` — nonces spread across many bitmap bytes, interleaved with replays; setting a later bit must not free an earlier one
- `test_release_funds_with_bitmap_rotation` — full generation lifecycle across a rotation
- `test_release_funds_nonce_zero_boundary` — nonce=0, the first bit of the first byte
- `test_release_funds_last_nonce_in_generation` — nonce 65535, the last bit of the last byte
- `test_release_funds_nonce_from_future_generation_rejected` — a fresh instance refuses nonces from later generations, before any rotation
- `test_release_funds_zero_amount_consumes_nonce` — a zero-amount release still burns its nonce
- `test_release_funds_foreign_bitmap_rejected` — another instance's bitmap is rejected; neither bitmap records the nonce and no funds move
- `test_release_funds_rotation_frees_same_bit_position` — nonce N and N+65536 map to the same bit; the second must succeed after a rotation
- `test_double_spend_same_nonce_after_bitmap_rotation` — a previous-generation nonce stays closed even though rotation cleared its bit
- `test_double_spend_bitmap_rejects_used_nonce` — replay within a generation rejected by the bit alone
- `test_double_spend_sequential_releases_then_replay` — three neighbouring nonces in one byte, then a replay of the first
- `test_release_funds_wrong_user_ata` — passing another user's ATA as user_ata while keeping the correct user pubkey in instruction data is rejected with InvalidInstructionData
- `test_release_funds_full_balance` — releasing the entire deposited balance succeeds and leaves the instance ATA at zero
- `test_release_funds_token_2022_transfer_fee_success` — escrow is debited the full release amount and the user receives it minus the fee, so the fee falls on the user, not the escrow
- `test_release_funds_token_2022_transfer_hook_forwards_extras` — the hook runs once on the way out of escrow and the nonce is consumed
- `test_release_funds_rejects_signer_bearing_hook_extra` — the release leg of the signer-strip guard, where the escrow PDA is the transfer authority; the release reverts and the nonce stays spendable

### RotateBitmap (6 integration tests)

- `test_rotate_bitmap_success` — happy path
- `test_rotate_bitmap_not_operator` — wrong operator rejected
- `test_rotate_bitmap_operator_not_signer` — unsigned operator rejected
- `test_rotate_bitmap_advances_generation` — two rotations advance the generation twice
- `test_rotate_bitmap_replay_with_stale_generation_rejected` — a replayed rotation carrying a stale expected generation cannot skip a generation
- `test_rotate_bitmap_foreign_bitmap_rejected` — another instance's bitmap is rejected and left on its own generation

### EmitEvent (2 integration tests)

- `test_emit_event_wrong_event_authority` — discriminator 228 routes to process_emit_event; any address other than the canonical event_authority PDA is rejected with InvalidEventAuthority
- `test_emit_event_no_accounts` — calling emit_event with an empty account list is rejected with NotEnoughAccountKeys

### Unit Tests (55 tests across processor and program modules)

**Instruction data parsing** (processor modules):

- `create_instance`: 4 tests (valid data, insufficient data, empty data, payload missing the bitmap bump)
- `allow_mint`: 2 tests (valid bump, empty data)
- `deposit`: 6 tests (with/without recipient, insufficient length, empty accounts, has_recipient flag set but recipient bytes absent)
- `release_funds`: 3 tests (valid data, insufficient length, empty accounts)
- `rotate_bitmap`: 1 test (empty accounts)
- `add_operator`: 2 tests (valid instruction data, empty instruction data)

**Withdrawal bitmap logic** (`state/withdrawal_bitmap.rs`):

- 9 tests covering `consume_nonce` (replay rejected, neighbouring bits in one byte do not collide), `validate_generation` (accepts the whole current window, rejects the next generation, rejects the previous one after a rotation), `rotate` (clears bits and advances, rejects a stale expected generation, rejects overflow at u64::MAX), and `init` (rejects an already-initialized account without disturbing it, rejects an undersized account)

**State serialization and validation** (`state/`):

- `allowed_mint`: 5 tests (constructor stores bump, serialize→deserialize roundtrip, wrong discriminator rejected, empty data rejected, data too short rejected)
- `operator`: 5 tests (constructor stores bump, serialize→deserialize roundtrip, wrong discriminator rejected, empty data rejected, data too short rejected)
- `instance`: 5 tests (constructor, serialization roundtrip with length check, validate_admin succeeds for correct key, validate_admin returns InvalidAdmin for wrong key, wrong discriminator rejected on deserialization)
- `discriminator`: 2 tests (all 10 valid instruction discriminator bytes accepted, unmapped bytes rejected)

**Error ABI stability** (`error.rs`):

- `test_error_codes_are_stable`: 1 test — asserts every `PrivateChannelEscrowProgramError` variant maps to its expected `Custom(N)` code; acts as an explicit lock against silent reordering that would break client SDKs and indexers

**Event encoding** (`events.rs`):

- 9 tests, one per event type (CreateInstance, AllowMint, BlockMint, AddOperator, RemoveOperator, SetNewAdmin, Deposit, ReleaseFunds, RotateBitmap) — each verifies the discriminator byte, field values, serialized byte length, and the `EVENT_IX_TAG_LE` prefix

## Documented Gaps

### Untested Edge Cases

- Generation overflow at u64::MAX is covered as a unit test on `WithdrawalBitmap::rotate`, but not end-to-end: driving an instance to that generation on-chain would take 2^64 rotations, so an integration test would need direct account state manipulation
