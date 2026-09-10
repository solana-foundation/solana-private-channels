# DvP Swap Program (vendored)

The DvP swap program's canonical source now lives in
[`solana-foundation/dvp`](https://github.com/solana-foundation/dvp). This
directory is **not** the program source: it is a vendored snapshot of the two
artifacts this repo consumes at build and runtime.

- `clients/rust/` — the generated Rust client crate `dvp-swap-program-client`
  (program ID, instruction builders, `SwapDvp` account layout). `core`,
  `gateway`, and `integration` depend on it by path. The generated code under
  `clients/rust/src/generated/` is committed here (it is git-ignored in the
  upstream repo, where it is regenerated on demand).
- `../core/precompiles/dvp_swap_program.so` — the compiled program, embedded
  into the node runtime as a precompile (`core/src/accounts/precompiles.rs`).

## Program ID

`dvp34bdbcEm4f4FCUjGV4mDAkDshaQR4LkK8fdcsyZq` — the devnet deployment. The
mainnet ID will differ and is not set here yet.

**This is an override.** Upstream `solana-foundation/dvp` declares a different
`declare_id!` in `program/src/lib.rs`; this repo pins the program to its own
devnet address. So re-syncing is not a plain copy: the `declare_id!` must be
patched to `dvp34…` before building, or the vendored `.so` + client repoint to
the upstream address and break the deployment. The same ID is mirrored in
`gateway/src/auth.rs` and `gateway/tests/auth_integration.rs`; keep all four in
sync.

## Re-syncing from upstream

Vendored at `solana-foundation/dvp` commit
`9103afe2c6375cdd7c755b4a0cbfd3aa00e6d8f2`, plus the `verify.rs` test-offset fix
from dvp PR #8 (which the vendored copy carries until that merges upstream). To
refresh:

```bash
# in a checkout of solana-foundation/dvp
# 1. Pin the program to this repo's devnet address (see "Program ID" above).
sed -i '' 's/<upstream-declare-id>/dvp34bdbcEm4f4FCUjGV4mDAkDshaQR4LkK8fdcsyZq/' program/src/lib.rs
# 2. Regenerate the client from the (now dvp34) IDL, then build the .so.
make generate-clients                 # regenerate the Rust/TS clients from the IDL
(cd program && cargo-build-sbf)       # build target/deploy/dvp_swap_program.so

# then, in this repo
cp -R <dvp>/clients/rust/src/.  dvp-swap-program/clients/rust/src/
cp    <dvp>/clients/rust/Cargo.toml dvp-swap-program/clients/rust/Cargo.toml
cp    <dvp>/target/deploy/dvp_swap_program.so core/precompiles/dvp_swap_program.so

# 3. Verify: the client program ID stays dvp34 and its tests pass.
grep DVP_SWAP_PROGRAM_ID dvp-swap-program/clients/rust/src/generated/programs.rs
make dvp-client-test
```

The gateway reads the `SwapDvp` account size and owner fields straight from the
vendored client (`SwapDvp::try_from_bytes` in `gateway/src/auth.rs`), so a layout
change is picked up when you re-vendor the client above. There are no
hand-mirrored constants to keep in sync.
