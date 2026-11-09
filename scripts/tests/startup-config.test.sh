#!/usr/bin/env bash
set -euo pipefail

# Startup config contract: one instance variable, explicit signer backends, program IDs
# pinned to the compiled ones, operator metrics on loopback.
# The compose checks need `docker compose` and jq; they are skipped without them, except
# in CI (CI=true), where a missing tool fails the run.
# Run from the repo root: ./scripts/tests/startup-config.test.sh

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

skip() {
  if [[ "${CI:-}" == true ]]; then
    echo "FAIL: CI cannot skip $1" >&2
    exit 1
  fi
  echo "skip: $1"
}

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
  # The hard-coded IDs the indexer and streamer watch must follow declare_id! too.
  local watched=(
    "indexer/src/indexer/datasource/common/parser/escrow.rs:${ESCROW_ID}"
    "indexer/src/indexer/datasource/common/parser/withdraw.rs:${WITHDRAW_ID}"
    "core/src/bin/streamer.rs:${ESCROW_ID}"
  )
  for entry in "${watched[@]}"; do
    grep -q "\"${entry#*:}\"" "${entry%%:*}" || fail "${entry%%:*} does not watch ${entry#*:}"
  done
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
  env -u ADMIN_SIGNER -u OPERATOR_SIGNER -u COMMON_ESCROW_INSTANCE_ID -u ESCROW_INSTANCE_ID -u ESCROW_CONFIDENTIAL_KEYS \
    DEVNET_FALLBACK_RPC_URL=https://fallback.invalid \
    docker compose -f "$file" --env-file versions.env "$@" config --format json 2>"$TMP/err"
}

test_compose_interpolation() {
  local start=$failures
  if ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null; then
    skip "compose interpolation (docker compose or jq missing)"
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
    # The deposit operator signs as the channel admin only and never holds a release key.
    jq -e '.services["operator-solana"].environment | keys | map(select(test("^OPERATOR_(SIGNER|PRIVATE_KEY|VAULT_|TURNKEY_|PRIVY_)"))) | length == 0' \
      <<<"$json" >/dev/null || fail "operator-solana carries an OPERATOR_* signer var"
    [[ "$(jq -r '.services["operator-private-channel"].environment.OPERATOR_SIGNER' <<<"$json")" == memory ]] \
      || fail "operator-private-channel OPERATOR_SIGNER not taken from the env file"
    for svc in operator-solana operator-private-channel; do
      [[ "$(jq -r --arg s "$svc" '.services[$s].environment.ADMIN_SIGNER' <<<"$json")" == memory ]] \
        || fail "$svc ADMIN_SIGNER not taken from the env file"
      bad="$(jq -r --arg s "$svc" '[.services[$s].ports[]? | select(.host_ip != "127.0.0.1")] | length' <<<"$json")"
      [[ "$bad" == 0 ]] || fail "$svc publishes a port beyond loopback"
    done
  done

  # The preserved PDA reaches every devnet worker; no second variable can replace it.
  for svc in indexer-solana operator-solana operator-private-channel; do
    got="$(jq -r --arg s "$svc" '.services[$s].environment.COMMON_ESCROW_INSTANCE_ID' <<<"$devnet_json")"
    [[ "$got" == "$preserved" ]] || fail "devnet $svc instance is '$got', not the preserved PDA"
  done

  # A single-key env: the release key falls back to the admin key. A separate one wins.
  printf 'ADMIN_PRIVATE_KEY=SNTLadmin\n' >"$TMP/admin-key.env"
  printf 'OPERATOR_PRIVATE_KEY=SNTLoperator\n' >"$TMP/operator-key.env"
  local one two
  one="$(compose_json docker-compose.yml --env-file .env.local --env-file "$TMP/admin-key.env")" || fail "single-key config"
  [[ "$(jq -r '.services["operator-private-channel"].environment.OPERATOR_PRIVATE_KEY' <<<"$one")" == SNTLadmin ]] \
    || fail "OPERATOR_PRIVATE_KEY does not fall back to ADMIN_PRIVATE_KEY"
  two="$(compose_json docker-compose.devnet.yml --env-file .env.devnet --env-file "$TMP/instance.env" \
    --env-file "$TMP/admin-key.env" --env-file "$TMP/operator-key.env")" || fail "two-key config"
  [[ "$(jq -r '.services["operator-private-channel"].environment.OPERATOR_PRIVATE_KEY' <<<"$two")" == SNTLoperator ]] \
    || fail "a separate OPERATOR_PRIVATE_KEY does not reach the withdraw operator"

  # No default backend: an env file without ADMIN_SIGNER is refused at interpolation.
  grep -v '^ADMIN_SIGNER=' .env.local >"$TMP/no-signer.env"
  if compose_json docker-compose.yml --env-file "$TMP/no-signer.env" >/dev/null; then
    fail "localnet compose accepted an env without ADMIN_SIGNER"
  else
    grep -q 'ADMIN_SIGNER' "$TMP/err" || fail "refusal does not name ADMIN_SIGNER: $(cat "$TMP/err")"
  fi
  pass "$start" "compose interpolation"
}

# Remints are channel writes, a restored primary has no replica slot, and Docker
# creates the WAL archive volumes root-owned. Each must hold in both stacks.
test_compose_release_fixes() {
  local start=$failures
  if ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null; then
    skip "compose release fixes (docker compose or jq missing)"
    return
  fi
  printf 'COMMON_ESCROW_INSTANCE_ID=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz\n' >"$TMP/instance.env"
  local name json
  for name in local devnet; do
    if [[ $name == local ]]; then
      json="$(compose_json docker-compose.yml --env-file .env.local)" \
        || { fail "localnet compose config: $(cat "$TMP/err")"; continue; }
    else
      json="$(compose_json docker-compose.devnet.yml --env-file .env.devnet --env-file "$TMP/instance.env")" \
        || { fail "devnet compose config: $(cat "$TMP/err")"; continue; }
    fi

    [[ "$(jq -r '.services["operator-private-channel"].environment.COMMON_SOURCE_RPC_URL' <<<"$json")" == http://gateway:8904 ]] \
      || fail "$name operator-private-channel source is not the gateway internal listener (remints are writes)"

    jq -r '.services["postgres-replica"].command | join(" ")' <<<"$json" | grep -q 'pg_create_physical_replication_slot' \
      || fail "$name postgres-replica does not recreate a missing replica_slot"

    local svc entry
    for svc in postgres-primary postgres-indexer; do
      entry="$(jq -r --arg s "$svc" '.services[$s].entrypoint // [] | join(" ")' <<<"$json")"
      [[ "$entry" == *"chown postgres:postgres /wal_archive"* && "$entry" == *"exec docker-entrypoint.sh"* ]] \
        || fail "$name $svc does not chown /wal_archive before the image entrypoint"
      [[ "$(jq -r --arg s "$svc" '.services[$s].command[0]' <<<"$json")" == postgres ]] \
        || fail "$name $svc command no longer starts with postgres"
    done
  done
  pass "$start" "compose release fixes"
}

# The escrow instance's confidential keys reach exactly the escrow indexer and both
# operators, ship blank in every preset and stay optional until an account is configured.
CONFIDENTIAL_KEYS='{"elgamal_secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","aes_key":"AAAAAAAAAAAAAAAAAAAAAA=="}'
CONFIDENTIAL_SERVICES="indexer-solana operator-solana operator-private-channel"
CONFIDENTIAL_PUBKEY='AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='

test_confidential_keys_wiring() {
  local start=$failures
  for f in .env.example .env.local .env.devnet; do
    grep -qx 'ESCROW_CONFIDENTIAL_KEYS=' "$f" || fail "$f must ship ESCROW_CONFIDENTIAL_KEYS blank"
    grep -qx 'ESCROW_CONFIDENTIAL_PUBKEY=' "$f" || fail "$f must ship ESCROW_CONFIDENTIAL_PUBKEY blank"
  done
  # Optional: blank passes, a well-formed value passes, a malformed one is refused by name only.
  local i=0 var
  : >"$TMP/secrets.env"
  for var in POSTGRES_PASSWORD POSTGRES_REPLICATION_PASSWORD POSTGRES_RUNTIME_PASSWORD \
    POSTGRES_AUTH_RUNTIME_PASSWORD POSTGRES_AUTH_OWNER_PASSWORD POSTGRES_GATEWAY_PASSWORD \
    POSTGRES_MONITORING_PASSWORD POSTGRES_GRAFANA_PASSWORD POSTGRES_INDEXER_PASSWORD GF_ADMIN_PASSWORD; do
    i=$((i + 1))
    printf '%s=secret%s\n' "$var" "$i" >>"$TMP/secrets.env"
  done
  env -u ESCROW_CONFIDENTIAL_KEYS ./scripts/check-required-env.sh .env.example "$TMP/secrets.env" >/dev/null 2>&1 \
    || fail "check-required-env.sh refused a blank ESCROW_CONFIDENTIAL_KEYS"
  printf "ESCROW_CONFIDENTIAL_KEYS='%s'\n" "$CONFIDENTIAL_KEYS" >"$TMP/good-keys.env"
  env -u ESCROW_CONFIDENTIAL_KEYS ./scripts/check-required-env.sh .env.example "$TMP/secrets.env" "$TMP/good-keys.env" \
    >/dev/null 2>&1 || fail "check-required-env.sh refused well-formed ESCROW_CONFIDENTIAL_KEYS"
  printf 'ESCROW_CONFIDENTIAL_KEYS=not-json-secret-material\n' >"$TMP/bad-keys.env"
  if env -u ESCROW_CONFIDENTIAL_KEYS ./scripts/check-required-env.sh .env.example "$TMP/secrets.env" "$TMP/bad-keys.env" \
    >/dev/null 2>"$TMP/bad-keys.err"; then
    fail "check-required-env.sh accepted malformed ESCROW_CONFIDENTIAL_KEYS"
  else
    grep -q 'ESCROW_CONFIDENTIAL_KEYS' "$TMP/bad-keys.err" || fail "refusal does not name ESCROW_CONFIDENTIAL_KEYS"
    grep -q 'secret-material' "$TMP/bad-keys.err" && fail "refusal leaks the ESCROW_CONFIDENTIAL_KEYS value"
  fi
  # The pinned pubkey: base64 of 32 bytes, and only together with the keys.
  printf 'ESCROW_CONFIDENTIAL_PUBKEY=%s\n' "$CONFIDENTIAL_PUBKEY" >"$TMP/pubkey.env"
  env -u ESCROW_CONFIDENTIAL_KEYS -u ESCROW_CONFIDENTIAL_PUBKEY ./scripts/check-required-env.sh .env.example \
    "$TMP/secrets.env" "$TMP/good-keys.env" "$TMP/pubkey.env" >/dev/null 2>&1 \
    || fail "check-required-env.sh refused a pinned pubkey with its keys"
  env -u ESCROW_CONFIDENTIAL_KEYS -u ESCROW_CONFIDENTIAL_PUBKEY ./scripts/check-required-env.sh .env.example \
    "$TMP/secrets.env" "$TMP/pubkey.env" >/dev/null 2>"$TMP/pin-alone.err" \
    && fail "check-required-env.sh accepted ESCROW_CONFIDENTIAL_PUBKEY without the keys"
  grep -q 'ESCROW_CONFIDENTIAL_KEYS is blank' "$TMP/pin-alone.err" || fail "pin without keys refusal does not say why"
  printf 'ESCROW_CONFIDENTIAL_PUBKEY=not-a-pubkey\n' >"$TMP/bad-pubkey.env"
  env -u ESCROW_CONFIDENTIAL_KEYS -u ESCROW_CONFIDENTIAL_PUBKEY ./scripts/check-required-env.sh .env.example \
    "$TMP/secrets.env" "$TMP/good-keys.env" "$TMP/bad-pubkey.env" >/dev/null 2>"$TMP/bad-pubkey.err" \
    && fail "check-required-env.sh accepted a malformed ESCROW_CONFIDENTIAL_PUBKEY"
  grep -q 'ESCROW_CONFIDENTIAL_PUBKEY' "$TMP/bad-pubkey.err" || fail "refusal does not name ESCROW_CONFIDENTIAL_PUBKEY"
  if ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null; then
    skip "confidential keys compose wiring (docker compose or jq missing)"
    pass "$start" "confidential keys wiring"
    return
  fi
  printf "COMMON_ESCROW_INSTANCE_ID=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz\nESCROW_CONFIDENTIAL_KEYS='%s'\nESCROW_CONFIDENTIAL_PUBKEY=%s\n" \
    "$CONFIDENTIAL_KEYS" "$CONFIDENTIAL_PUBKEY" >"$TMP/keys.env"
  local name json svc got
  for name in local devnet; do
    if [[ $name == local ]]; then
      json="$(compose_json docker-compose.yml --env-file .env.local --env-file "$TMP/keys.env")" \
        || { fail "localnet compose config: $(cat "$TMP/err")"; continue; }
    else
      json="$(compose_json docker-compose.devnet.yml --env-file .env.devnet --env-file "$TMP/keys.env")" \
        || { fail "devnet compose config: $(cat "$TMP/err")"; continue; }
    fi
    for svc in $CONFIDENTIAL_SERVICES; do
      got="$(jq -r --arg s "$svc" '.services[$s].environment.ESCROW_CONFIDENTIAL_KEYS' <<<"$json")"
      [[ "$got" == "$CONFIDENTIAL_KEYS" ]] || fail "$name $svc does not get ESCROW_CONFIDENTIAL_KEYS verbatim (got '$got')"
      got="$(jq -r --arg s "$svc" '.services[$s].environment.ESCROW_CONFIDENTIAL_PUBKEY' <<<"$json")"
      [[ "$got" == "$CONFIDENTIAL_PUBKEY" ]] || fail "$name $svc does not get ESCROW_CONFIDENTIAL_PUBKEY (got '$got')"
    done
    # The secret goes nowhere else.
    got="$(jq -r '[.services | to_entries[] | select(.value.environment.ESCROW_CONFIDENTIAL_KEYS != null) | .key] | sort | join(" ")' <<<"$json")"
    [[ "$got" == "indexer-solana operator-private-channel operator-solana" ]] \
      || fail "$name ESCROW_CONFIDENTIAL_KEYS reaches: $got"
  done
  # Blank when unset, so the services read it as not configured.
  json="$(compose_json docker-compose.yml --env-file .env.local)" || fail "localnet compose config: $(cat "$TMP/err")"
  got="$(jq -r '.services["operator-solana"].environment.ESCROW_CONFIDENTIAL_KEYS' <<<"$json")"
  [[ "$got" == "" ]] || fail "unset ESCROW_CONFIDENTIAL_KEYS must render blank (got '$got')"
  pass "$start" "confidential keys wiring"
}

test_env_contract
test_program_ids_are_the_compiled_ones
test_compose_has_no_literal_signer_backend
test_devnet_script_writes_the_one_instance_key
test_compose_interpolation
test_compose_release_fixes
test_confidential_keys_wiring

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "startup-config tests passed"
