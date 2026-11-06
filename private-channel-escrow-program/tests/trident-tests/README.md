# Trident Fuzz Tests — private-channel-escrow-program

Stateful fuzz tests for the escrow program using [Trident](https://github.com/Ackee-Blockchain/trident) v0.12.0.

## Harnesses

### `fuzz_escrow`

Tests the core escrow lifecycle in a single bitmap generation.

| Flow                | Description                                                                                                           |
| ------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `fuzz_deposit`      | Deposits a random amount. Asserts exact ATA balance movement, or rejection when the deposit gate is closed.           |
| `fuzz_release`      | 50% valid bitmap / 50% foreign bitmap. Asserts success/failure and balance invariants, including the withdrawal gate. |
| `fuzz_double_spend` | Replays a previously successful release verbatim — must always be rejected.                                           |
| `fuzz_set_gates`    | `BlockMint` with a random combination of both flags. Must always succeed and must never move escrowed tokens.         |
| `fuzz_re_allow`     | `AllowMint` on the existing PDA — the re-allow path. Re-opens both gates; escrowed balances must survive untouched.   |

**Final invariant:** `escrow_balance == total_deposited - total_released`

The two gates are independent and set as absolute values, so a mint with deposits
blocked must still release. That is the property that keeps blocking a mint from
stranding the balances already in it.

### `fuzz_rotate_bitmap`

Tests the bitmap rotation lifecycle across multiple generations.

| Flow                 | Description                                                                                |
| -------------------- | ------------------------------------------------------------------------------------------ |
| `fuzz_deposit`       | Deposits a random amount.                                                                  |
| `fuzz_release`       | Valid release within the current generation. Skipped silently if preconditions aren't met. |
| `fuzz_replay_nonce`  | Replays a nonce already consumed in this generation — must always be rejected.             |
| `fuzz_rotate_bitmap` | Clears the on-chain bitmap and advances the generation. Asserts balances are unaffected.   |
| `fuzz_stale_nonce`   | Attempts a release with a nonce from the previous generation — must always be rejected.    |

**Final invariant:** `escrow_balance == total_deposited - total_released`

### `fuzz_confidential`

Tests the confidential instructions (12-17) next to public deposits and releases on
the same escrow account. Each iteration picks a confidential Token-2022 mint: plain,
with a transfer fee, or now and then ConfidentialMintBurn, whose user holds nothing
(public mint and deposit are refused on it). The escrow starts unconfigured, as every
deployed one is, so configure can land after public deposits and fees, and every
confidential flow also runs before it. The whitelist starts in a random state.
Proofs go inline, into context accounts, or mixed.

| Flow                                | Description                                                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `fuzz_configure`                    | Lands once on a supported mint, leaving the public side alone. Repeats never change the key.                   |
| `fuzz_confidential_deposit`         | Hidden deposit for the user or a second wallet, edge amounts included. Confidential credits must be off after. |
| `fuzz_operator_read` / `fuzz_apply` | Apply from a fresh or a stale read of the pending balance, so the AES copy can lag.                            |
| `fuzz_confidential_release`         | Proofs from the operator's count, from the AES copy, or made stale by an apply in between.                     |
| `fuzz_move_to_confidential`         | Public to pending, zero and overdraws included.                                                                |
| `fuzz_move_to_public`               | Available to public, from fresh or stale proofs.                                                               |
| `fuzz_public_deposit`               | Public deposit on the same account, net of the fee.                                                            |
| `fuzz_public_release`               | Public release; shares the nonce space with confidential releases.                                             |
| `fuzz_direct_transfer_to_escrow`    | Token-2022 confidential transfer bypassing the escrow. Must always be refused.                                 |
| `fuzz_release_to_instance`          | Either release naming the instance as user. Must fail, with `InvalidRecipient` when gates and whitelist allow. |
| `fuzz_re_allow`                     | `AllowMint` again: reopens the gates and leaves the escrow account, extensions included, as it was.            |
| `fuzz_user_apply`                   | The user applies their own pending balance.                                                                    |
| gates and whitelist flows           | `BlockMint`, `SetWhitelistEnabled`, add, update and remove entries for either wallet.                          |

Every flow predicts its outcome from the model, and a refused one must leave the
accounts byte for byte as they were. Expected errors are checked with the program
that raised them, since escrow and Token-2022 error numbers overlap.

Escrow events are not checked: Trident does not expose inner instructions. The
integration tests check each event, and this harness checks the same facts from
the accounts.

**Final invariants:** every public, pending and available balance, credit count and
withheld fee matches the model; mint supply equals all balances plus all withheld
fees; confidential credits are off; the AES copy holds the value last written; the
escrow's ElGamal key is unchanged; the bitmap holds exactly the spent nonces.

## Running

Build the program first (from repo root). `cargo-build-sbf` must run from
`program/` so it sees only the on-chain package; above that it also pulls the
client crate and its host-only deps:

```bash
make -C private-channel-escrow-program build-no-clients
```

Run a harness:

```bash
cd private-channel-escrow-program/tests/trident-tests
cargo run --bin fuzz_escrow
cargo run --bin fuzz_rotate_bitmap
cargo run --bin fuzz_confidential
```

Trident prints failed assertions through its progress bar, so they only show in a
terminal. In CI, `TRIDENT_WITH_EXIT_CODE=1` turns them into exit code 99.

Debug mode — single-threaded, panics and program logs visible:

```bash
cargo build --bin fuzz_escrow
TRIDENT_FUZZ_DEBUG=0000000000000000 ./target/debug/fuzz_escrow 2>&1 | head -200
```

## Structure

```
trident-tests/
  shared.rs             # Shared constants, AccountAddresses, setup_escrow, token_amount
  confidential.rs       # Confidential mints, accounts, proofs and decryption
  fuzz_escrow.rs        # Core lifecycle harness
  fuzz_rotate_bitmap.rs # Bitmap rotation lifecycle harness
  fuzz_confidential.rs  # Confidential instructions harness
  Cargo.toml
  Trident.toml
```

## Notes

- The Pinocchio program uses `sol_get_sysvar` for `Rent::get()`, which requires patched Trident syscall stubs. See `[patch.crates-io]` in `Cargo.toml`.
- The same patch turns on the curve25519 syscalls. Token-2022 adds and subtracts confidential balances with them, and Trident leaves them off.
- `Trident.toml` loads mainnet's Token-2022 v11.1.0 from `core/precompiles/`, the build the integration tests pin, in place of the v7.0.0 Trident bundles.
- Trident runs agave 2.3, whose ZK ElGamal proof program is a builtin, so proofs come from the zk-sdk 2.3 crates, not the 7.x ones the integration tests use. Token-2022 reads only the proof contexts, whose layout is the same.
- Nonces in `fuzz_rotate_bitmap` are generation-aware: `nonce = generation * NONCES_PER_GENERATION + offset` to ensure they belong to the current generation.
