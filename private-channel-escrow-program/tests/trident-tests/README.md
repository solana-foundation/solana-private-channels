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
```

Debug mode — single-threaded, panics and program logs visible:

```bash
cargo build --bin fuzz_escrow
TRIDENT_FUZZ_DEBUG=0000000000000000 ./target/debug/fuzz_escrow 2>&1 | head -200
```

## Structure

```
trident-tests/
  shared.rs             # Shared constants, AccountAddresses, setup_escrow, token_amount
  fuzz_escrow.rs        # Core lifecycle harness
  fuzz_rotate_bitmap.rs # Bitmap rotation lifecycle harness
  Cargo.toml
  Trident.toml
```

## Notes

- The Pinocchio program uses `sol_get_sysvar` for `Rent::get()`, which requires patched Trident syscall stubs. See `[patch.crates-io]` in `Cargo.toml`.
- Nonces in `fuzz_rotate_bitmap` are generation-aware: `nonce = generation * NONCES_PER_GENERATION + offset` to ensure they belong to the current generation.
