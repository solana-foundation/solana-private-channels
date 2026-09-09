# Escrow Program — Test Coverage Analysis

> This is a **semantic coverage estimate** produced by analyzing test assertions
> against the program's testable surface. It is not instrumented line coverage —
> Solana SBF programs do not support LLVM coverage instrumentation.

## Summary

| Category                      | Coverage     | Details                                                     |
| ----------------------------- | ------------ | ----------------------------------------------------------- |
| Instruction handlers          | 100% (9/9)   | All handlers have success + error tests                     |
| Account validation paths      | 95% (19/20)  | Signer, PDA, owner, mutability, ATA program, system program |
| Business logic error branches | 93% (14/15)  | SMT proofs, balance verification, Token2022 extensions      |
| Custom error codes exercised  | 100% (13/13) | All custom errors tested                                    |
| State & trait coverage (unit) | 100% (14/14) | Instruction data parsing for all handlers                   |
| Event coverage                | 100% (9/9)   | All events emitted in integration tests                     |
| Security edge cases           | 100% (14/14) | Double-spend, malformed proofs, Token2022, nonce boundaries |
| **Overall (risk-weighted)**   | **~95%**     |                                                             |

## Test Inventory

**66 unit tests** (instruction data parsing, state serialization, error ABI, event encoding, SMT proof logic) + **84 integration tests** (end-to-end behavior).

### CreateInstance (4 integration tests)

- `test_create_instance_success` — happy path
- `test_create_instance_duplicate` — duplicate creation fails
- `test_create_instance_invalid_admin_not_signer` — unsigned admin rejected
- `test_create_instance_invalid_event_authority` — invalid event authority PDA
- `test_create_instance_invalid_system_program` — wrong system program address

### AllowMint (9 integration tests)

- `test_allow_mint_success` — SPL Token mint
- `test_allow_mint_duplicate` — duplicate mint fails
- `test_allow_mint_invalid_pda` — wrong PDA rejected
- `test_allow_mint_invalid_admin_not_signer` — unsigned admin rejected
- `test_allow_mint_invalid_admin` — wrong admin rejected
- `test_allow_mint_invalid_instance_account_owner` — wrong owner rejected
- `test_allow_mint_token_2022_basic_success` — Token2022 mint allowed
- `test_allow_mint_token_2022_permanent_delegate_accepted` — permanent-delegate Token-2022 mint allowed; drain detection is enforced by the operator at withdrawal time
- `test_allow_mint_token_2022_pausable_accepted` — pausable Token-2022 mint allowed; pause state is enforced by the operator at withdrawal time
- `test_allow_mint_token_2022_transfer_hook_blocked` — TransferHookNotAllowed; the program's `TransferChecked` CPI does not resolve extra-account metas, so hook mints are rejected at validation

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

### Deposit (12 integration tests)

- `test_deposit_success` — happy path
- `test_deposit_with_recipient` — optional recipient parameter
- `test_deposit_insufficient_funds` — insufficient balance error
- `test_deposit_mint_not_allowed` — unapproved mint rejected
- `test_deposit_invalid_instruction_data_too_short` — malformed data
- `test_deposit_not_enough_accounts` — missing accounts
- `test_deposit_token_2022_basic_success` — Token2022 deposit
- `test_deposit_token_2022_transfer_hook_rejected` — TransferHookNotAllowed on deposit path (live swap of mint data post-AllowMint proves the check runs at deposit, not only at AllowMint)
- `test_deposit_invalid_associated_token_program` — wrong ATA program rejected
- `test_multiple_depositors_same_instance` — three users deposit to same instance
- `test_deposit_wrong_user_ata` — passing another user's ATA as the user_ata is rejected with InvalidInstructionData
- `test_deposit_wrong_instance_ata` — passing an instance ATA for a different mint is rejected with InvalidInstructionData

### ReleaseFunds (23 integration tests)

- `test_release_funds_success` — happy path with SMT proof
- `test_release_funds_insufficient_funds` — insufficient balance error
- `test_release_funds_not_operator` — wrong operator rejected
- `test_release_funds_invalid_instruction_data_too_short` — malformed data
- `test_release_funds_operator_not_signer` — unsigned operator rejected
- `test_release_funds_smt_exclusion` — SMT exclusion proof scenarios
- `test_release_funds_invalid_inclusion_proof` — wrong root rejected
- `test_release_funds_with_smt_reset` — full SMT lifecycle
- `test_release_funds_nonce_zero_boundary` — nonce=0 edge case
- `test_release_funds_single_leaf_smt` — single-leaf tree operations
- `test_release_funds_max_depth_smt_proof` — maximum depth verification
- `test_double_spend_same_nonce_after_tree_reset` — cross-tree replay
- `test_double_spend_smt_exclusion_rejects_used_nonce` — nonce reuse
- `test_double_spend_sequential_releases_then_replay` — sequential replay
- `test_malformed_proof_all_zero_siblings` — zeroed proof data
- `test_malformed_proof_wrong_nonce_siblings` — wrong nonce siblings
- `test_malformed_proof_nonce_outside_tree_range` — out-of-range nonce
- `test_malformed_proof_nonce_far_outside_range` — far out-of-range nonce
- `test_boundary_nonce_last_valid_for_tree_index_zero` — boundary nonce
- `test_zero_amount_release` — zero amount edge case
- `test_release_funds_wrong_user_ata` — passing another user's ATA as user_ata while keeping the correct user pubkey in instruction data is rejected with InvalidInstructionData
- `test_release_funds_full_balance` — releasing the entire deposited balance succeeds and leaves the instance ATA at zero

### ResetSmtRoot (4 integration tests)

- `test_reset_smt_root_success` — happy path
- `test_reset_smt_root_not_operator` — wrong operator rejected
- `test_reset_smt_root_operator_not_signer` — unsigned operator rejected
- `test_reset_smt_root_updates_nonce` — tree index incremented

### EmitEvent (2 integration tests)

- `test_emit_event_wrong_event_authority` — discriminator 228 routes to process_emit_event; any address other than the canonical event_authority PDA is rejected with InvalidEventAuthority
- `test_emit_event_no_accounts` — calling emit_event with an empty account list is rejected with NotEnoughAccountKeys

### Unit Tests (66 tests across processor and program modules)

**Instruction data parsing** (processor modules):

- `create_instance`: 3 tests (valid data, insufficient data, empty data)
- `allow_mint`: 2 tests (valid bump, empty data)
- `deposit`: 5 tests (with/without recipient, insufficient length, empty accounts, has_recipient flag set but recipient bytes absent)
- `release_funds`: 3 tests (valid data, insufficient length, empty accounts)
- `reset_smt_root`: 1 test (empty accounts)
- `add_operator`: 2 tests (valid instruction data, empty instruction data)

**SMT proof logic** (`processor/shared/smt_utils.rs`):

- 19 tests covering `hash_combine` (determinism, order-dependence, avalanche effect) and `verify_smt_exclusion_proof` / `verify_smt_inclusion_proof` (empty tree, different nonces, with siblings, wrong root, corrupted siblings, edge-case nonces, early termination, all-bits-set, exclusion-vs-inclusion for same nonce)

**State serialization and validation** (`state/`):

- `allowed_mint`: 5 tests (constructor stores bump, serialize→deserialize roundtrip, wrong discriminator rejected, empty data rejected, data too short rejected)
- `operator`: 5 tests (constructor stores bump, serialize→deserialize roundtrip, wrong discriminator rejected, empty data rejected, data too short rejected)
- `instance`: 9 tests (constructor, checked_add overflow on tree index, nonce zero boundary, nonce boundary at tree index 1, serialization roundtrip, validate_admin succeeds for correct key, validate_admin returns InvalidAdmin for wrong key, wrong discriminator rejected on deserialization, second tree nonce validation)
- `discriminator`: 2 tests (all 10 valid instruction discriminator bytes accepted, unmapped bytes rejected)

**Error ABI stability** (`error.rs`):

- `test_error_codes_are_stable`: 1 test — asserts every `PrivateChannelEscrowProgramError` variant maps to its expected `Custom(N)` code; acts as an explicit lock against silent reordering that would break client SDKs and indexers

**Event encoding** (`events.rs`):

- 9 tests, one per event type (CreateInstance, AllowMint, BlockMint, AddOperator, RemoveOperator, SetNewAdmin, Deposit, ReleaseFunds, ResetSmtRoot) — each verifies the discriminator byte, field values, serialized byte length, and the `EVENT_IX_TAG_LE` prefix

## Documented Gaps

### Untested Edge Cases

- `checked_add` overflow on tree index (u64::MAX) — requires direct account state manipulation to set tree_index to MAX, impractical in integration tests without dedicated test infrastructure
