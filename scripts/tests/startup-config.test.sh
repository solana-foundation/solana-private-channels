#!/usr/bin/env bash
set -euo pipefail

# Startup config contract: one instance variable, explicit signer backends, program IDs
# pinned to the compiled ones, operator metrics on loopback.
# The compose checks need `docker compose` and jq; they are skipped without them.
# Run from the repo root: ./scripts/tests/startup-config.test.sh

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}
# Prints ok only when the test added no failure since `start`.
pass() { [[ $failures -eq $1 ]] && echo "ok: $2" || true; }

has_key() { grep -qE "^$2=" "$1"; }

declared_id() { grep -oE 'declare_id!\("[^"]+"\)' "$1" | cut -d'"' -f2; }
ESCROW_ID="$(declared_id private-channel-escrow-program/program/src/lib.rs)"
WITHDRAW_ID="$(declared_id private-channel-withdraw-program/program/src/lib.rs)"

test_env_contract() {
  local start=$failures
  ./scripts/check-env-contract.sh >/dev/null || fail "check-env-contract.sh"
  for key in ADMIN_SIGNER OPERATOR_SIGNER COMMON_ESCROW_INSTANCE_ID; do
    has_key .env.example "$key" || fail ".env.example lacks $key"
  done
  for f in .env.example .env.local .env.devnet private-channel-deploy/templates/env.j2; do
    has_key "$f" ESCROW_INSTANCE_ID && fail "$f still renders ESCROW_INSTANCE_ID"
  done
  for f in .env.local .env.devnet; do
    grep -qx 'ADMIN_SIGNER=memory' "$f" || fail "$f lacks ADMIN_SIGNER=memory"
    grep -qx 'OPERATOR_SIGNER=memory' "$f" || fail "$f lacks OPERATOR_SIGNER=memory"
  done
  has_key .env.devnet COMMON_ESCROW_INSTANCE_ID || fail ".env.devnet lacks COMMON_ESCROW_INSTANCE_ID"
  grep -qx 'COMMON_ESCROW_INSTANCE_ID=' .env.example \
    || fail ".env.example must ship COMMON_ESCROW_INSTANCE_ID blank, not a placeholder"
  pass "$start" "env contract"
}

test_program_ids_are_the_compiled_ones() {
  local start=$failures
  for f in .env.example .env.local; do
    grep -qx "ESCROW_PROGRAM_ID=${ESCROW_ID}" "$f" || fail "$f ESCROW_PROGRAM_ID != declare_id"
    grep -qx "WITHDRAW_PROGRAM_ID=${WITHDRAW_ID}" "$f" || fail "$f WITHDRAW_PROGRAM_ID != declare_id"
  done
  local common=private-channel-deploy/vars/common.yml
  grep -qx "compiled_escrow_program_id: ${ESCROW_ID}" "$common" || fail "common.yml compiled escrow id"
  grep -qx "compiled_withdraw_program_id: ${WITHDRAW_ID}" "$common" || fail "common.yml compiled withdraw id"
  grep -qE '^(escrow|withdraw)_program_id:' private-channel-deploy/vars/dev.yml \
    && fail "vars/dev.yml still offers program IDs as config"
  pass "$start" "program IDs"
}

test_compose_has_no_literal_signer_backend() {
  local start=$failures
  for f in docker-compose.yml docker-compose.devnet.yml; do
    grep -qE '^\s*- (ADMIN|OPERATOR)_SIGNER=memory' "$f" && fail "$f hard-codes a memory signer"
  done
  devnet_maps="$(grep -c 'COMMON_ESCROW_INSTANCE_ID=\${COMMON_ESCROW_INSTANCE_ID}' docker-compose.devnet.yml || true)"
  [[ "$devnet_maps" == 3 ]] || fail "devnet compose must map all 3 workers to COMMON_ESCROW_INSTANCE_ID (got $devnet_maps)"
  pass "$start" "compose sources"
}

test_devnet_script_writes_the_one_instance_key() {
  local start=$failures
  grep -q 'COMMON_ESCROW_INSTANCE_ID' scripts/devnet/devnet-test.sh \
    || fail "devnet-test.sh does not write COMMON_ESCROW_INSTANCE_ID"
  grep -qE '(^|[^_])ESCROW_INSTANCE_ID=' scripts/devnet/devnet-test.sh \
    && fail "devnet-test.sh still writes ESCROW_INSTANCE_ID"
  pass "$start" "devnet-test.sh instance key"
}

compose_json() {
  local file="$1"
  shift
  env -u ADMIN_SIGNER -u OPERATOR_SIGNER -u COMMON_ESCROW_INSTANCE_ID -u ESCROW_INSTANCE_ID \
    DEVNET_FALLBACK_RPC_URL=https://fallback.invalid \
    docker compose -f "$file" --env-file versions.env "$@" config --format json 2>"$TMP/err"
}

test_compose_interpolation() {
  local start=$failures
  if ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null; then
    echo "skip: compose interpolation (docker compose or jq missing)"
    return
  fi
  local preserved=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$preserved" >"$TMP/instance.env"

  local local_json devnet_json
  local_json="$(compose_json docker-compose.yml --env-file .env.local)" \
    || fail "localnet compose config: $(cat "$TMP/err")"
  devnet_json="$(compose_json docker-compose.devnet.yml --env-file .env.devnet --env-file "$TMP/instance.env")" \
    || fail "devnet compose config: $(cat "$TMP/err")"

  for json in "$local_json" "$devnet_json"; do
    for svc in operator-solana operator-private-channel; do
      [[ "$(jq -r --arg s "$svc" '.services[$s].environment.ADMIN_SIGNER' <<<"$json")" == memory ]] \
        || fail "$svc ADMIN_SIGNER not taken from the env file"
      [[ "$(jq -r --arg s "$svc" '.services[$s].environment.OPERATOR_SIGNER' <<<"$json")" == memory ]] \
        || fail "$svc OPERATOR_SIGNER not taken from the env file"
      bad="$(jq -r --arg s "$svc" '[.services[$s].ports[]? | select(.host_ip != "127.0.0.1")] | length' <<<"$json")"
      [[ "$bad" == 0 ]] || fail "$svc publishes a port beyond loopback"
    done
  done

  # The preserved PDA reaches every devnet worker; no second variable can replace it.
  for svc in indexer-solana operator-solana operator-private-channel; do
    got="$(jq -r --arg s "$svc" '.services[$s].environment.COMMON_ESCROW_INSTANCE_ID' <<<"$devnet_json")"
    [[ "$got" == "$preserved" ]] || fail "devnet $svc instance is '$got', not the preserved PDA"
  done

  # No default backend: an env file without ADMIN_SIGNER is refused at interpolation.
  grep -v '^ADMIN_SIGNER=' .env.local >"$TMP/no-signer.env"
  if compose_json docker-compose.yml --env-file "$TMP/no-signer.env" >/dev/null; then
    fail "localnet compose accepted an env without ADMIN_SIGNER"
  else
    grep -q 'ADMIN_SIGNER' "$TMP/err" || fail "refusal does not name ADMIN_SIGNER: $(cat "$TMP/err")"
  fi
  pass "$start" "compose interpolation"
}

test_env_contract
test_program_ids_are_the_compiled_ones
test_compose_has_no_literal_signer_backend
test_devnet_script_writes_the_one_instance_key
test_compose_interpolation

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "startup-config tests passed"
