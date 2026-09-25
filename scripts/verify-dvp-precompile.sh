#!/usr/bin/env bash
set -euo pipefail

# verify-dvp-precompile.sh: rebuild the vendored DvP precompile from upstream
# source and fail unless it matches the committed binary and the core pin.
# The build runs in a digest-pinned solana-verify image, so the bytes are the
# same on any host. Also checks the vendored client still targets PROGRAM_ID,
# the address the runtime installs the precompile at.
#
# Re-vendoring bumps DVP_COMMIT here (and BASE_IMAGE if the toolchain moves).

DVP_REPO="https://github.com/solana-foundation/dvp"
DVP_COMMIT="9103afe2c6375cdd7c755b4a0cbfd3aa00e6d8f2"
UPSTREAM_PROGRAM_ID="DzG1qJupt6Khm8s8jB3p93NkhPoiAg2M7vkEhkS15CtC"
PROGRAM_ID="dvp34bdbcEm4f4FCUjGV4mDAkDshaQR4LkK8fdcsyZq"
BASE_IMAGE="solanafoundation/solana-verifiable-build@sha256:695f890e620db8c39afe5112e048599f8ee395a0cab5a2e572f30a72c6366cb4"

cd "$(dirname "$0")/.."

COMMITTED_SO="core/precompiles/dvp_swap_program.so"
PIN_FILE="core/src/accounts/precompiles.rs"
CLIENT_PROGRAMS="dvp-swap-program/clients/rust/src/generated/programs.rs"

if ! command -v solana-verify >/dev/null 2>&1; then
  echo "ERROR: solana-verify not found on PATH (cargo install solana-verify --locked)" >&2
  exit 1
fi

client_id="$(grep -o 'pubkey!("[^"]*")' "$CLIENT_PROGRAMS" | cut -d'"' -f2)"
if [ "$client_id" != "$PROGRAM_ID" ]; then
  echo "ERROR: $CLIENT_PROGRAMS targets '$client_id', expected $PROGRAM_ID" >&2
  exit 1
fi

pinned="$(grep -A1 'const DVP_SWAP_PROGRAM_SHA256' "$PIN_FILE" | grep -o '[0-9a-f]\{64\}')"
if [ -z "$pinned" ]; then
  echo "ERROR: could not read DVP_SWAP_PROGRAM_SHA256 from $PIN_FILE" >&2
  exit 1
fi

workdir="$(mktemp -d)"
# The build writes target/ as root from inside the container, so remove it
# through the same image or the runner is left with files it cannot delete.
cleanup() {
  if [ -d "$workdir/dvp/target" ]; then
    docker run --rm --entrypoint rm -v "$workdir/dvp:/dvp" "$BASE_IMAGE" -rf /dvp/target
  fi
  rm -rf "$workdir"
}
trap cleanup EXIT

git init -q "$workdir/dvp"
git -C "$workdir/dvp" fetch -q --depth 1 "$DVP_REPO" "$DVP_COMMIT"
git -C "$workdir/dvp" checkout -q FETCH_HEAD

lib_rs="$workdir/dvp/program/src/lib.rs"
perl -pi -e "s/$UPSTREAM_PROGRAM_ID/$PROGRAM_ID/" "$lib_rs"
if ! grep -q "declare_id!(\"$PROGRAM_ID\")" "$lib_rs"; then
  echo "ERROR: declare_id patch did not apply to $lib_rs; upstream ID may have changed" >&2
  exit 1
fi

solana-verify build --library-name dvp_swap_program --base-image "$BASE_IMAGE" "$workdir/dvp"

built="$(sha256sum "$workdir/dvp/target/deploy/dvp_swap_program.so" | cut -d' ' -f1)"
committed="$(sha256sum "$COMMITTED_SO" | cut -d' ' -f1)"

echo "built from $DVP_COMMIT: $built"
echo "committed $COMMITTED_SO: $committed"
echo "pinned DVP_SWAP_PROGRAM_SHA256: $pinned"

if [ "$built" != "$committed" ] || [ "$built" != "$pinned" ]; then
  echo "ERROR: DvP precompile does not match its pinned upstream build" >&2
  exit 1
fi
echo "OK: DvP precompile matches $DVP_REPO@$DVP_COMMIT"
