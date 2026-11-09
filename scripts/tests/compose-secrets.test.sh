#!/usr/bin/env bash
set -euo pipefail

# Compose contract for secret files, via `docker compose config` (no daemon needed):
#   1. no secret reaches the config (env, command, labels, entrypoint);
#   2. each service mounts only secrets/<itself>, and its _FILE set is its secret_files;
#   3. operator-solana has no OPERATOR_* signer var; the withdraw operator has its own,
#      for each of the 4 backends;
#   4. raw compose (no override) still matches the config from before secret files,
#      apart from the listed, intended changes;
#   5. the replica command parses and carries no secret.
# The .env and override are rendered from the real templates with sentinel secrets.
# Needs ansible-playbook, docker compose and jq; skips without them, except in CI.
# Run from the repo root: ./scripts/tests/compose-secrets.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/tests/deploy-test-lib.sh
. "$REPO/scripts/tests/deploy-test-lib.sh"

for tool in ansible-playbook jq; do
  command -v "$tool" >/dev/null || { skip "compose-secrets ($tool missing)"; exit 0; }
done
docker compose version >/dev/null 2>&1 || { skip "compose-secrets (docker compose missing)"; exit 0; }

deploy_test_setup
sentinel_secrets "$D/secrets.yml"
FIX="$REPO/scripts/tests/fixtures/compose-golden"
INSTANCE=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz
ADMIN_PUB=AdminRemote1111111111111111111111111111111
OPERATOR_PUB=OperRemote11111111111111111111111111111111

# Renders .env, the override and the evaluated secret_files map for one setup:
# render_setup <name> <network> [ansible args].
render_setup() {
  local name="$1" network="$2"
  shift 2
  local out="$TMP/$name"
  mkdir -p "$out"
  cat >"$TMP/render-$name.yml" <<EOF
- hosts: localhost
  gather_facts: false
  vars_files: [ "$D/vars/common.yml", "$D/vars/dev.yml", "$D/secrets.yml" ]
  tasks:
  - ansible.builtin.template: { src: "$D/templates/env.j2", dest: "$out/.env" }
  - ansible.builtin.template: { src: "$D/templates/compose.yml.j2", dest: "$out/override.yml" }
  - ansible.builtin.copy: { content: "{{ secret_files | to_json }}", dest: "$out/secret_files.json" }
    no_log: true
EOF
  playbook "$out/render.log" "$TMP/render-$name.yml" -e image_tag=t -e image_registry=reg.invalid/o \
    -e network="$network" -e "host_data_dir=$TMP/host" -e escrow_instance_id=$INSTANCE \
    -e pc_admin_pubkey=$ADMIN_PUB -e pc_operator_pubkey=$OPERATOR_PUB \
    -e pc_admin_private_key=SNTLmemadminkey -e pc_operator_private_key=SNTLmemoperatorkey \
    -e "_secrets_rev={}" "$@" || { fail "render $name"; tail -20 "$out/render.log" >&2; return 1; }
  printf 'SNTLmemadminkey\nSNTLmemoperatorkey\n' >>"$TMP/sentinels.txt"
  local file=docker-compose.yml
  [[ $network != localnet ]] && file=docker-compose.devnet.yml
  (cd "$REPO" && docker compose -p pc-test --env-file versions.env --env-file "$out/.env" \
    -f "$file" -f "$out/override.yml" --profile auth config --format json) >"$out/config.json" 2>"$out/config.err" \
    || { fail "compose config $name: $(cat "$out/config.err")"; return 1; }
}

check_setup() {
  local name="$1" out="$TMP/$1"
  # 1. No sentinel anywhere in the resolved config.
  assert_no_sentinel "$name compose config" "$out/config.json"
  # 2. Mounts and _FILE sets.
  python3 - "$out/config.json" "$out/secret_files.json" "$TMP/host" <<'PY' || fail "$name: mounts or _FILE sets wrong"
import json, sys
cfg = json.load(open(sys.argv[1]))["services"]
want = json.load(open(sys.argv[2]))
host = sys.argv[3]
bad = []
for svc, c in cfg.items():
    files = sorted(i["key"] for i in want.get(svc, []) if i["value"])
    env = sorted(k[:-5] for k in (c.get("environment") or {}) if k.endswith("_FILE") and (c["environment"][k] or "").startswith("/run/secrets/"))
    if svc in want and env != files:
        bad.append(f"{svc}: _FILE {env} != secret_files {files}")
    mounts = [v for v in c.get("volumes", []) if v.get("target") == "/run/secrets"]
    srcs = [m["source"] for m in mounts]
    if svc in want and srcs != [f"{host}/secrets/{svc}"]:
        bad.append(f"{svc}: /run/secrets from {srcs}")
    if svc in want and mounts and not mounts[0].get("read_only"):
        bad.append(f"{svc}: secrets mount is writable")
    for m in c.get("volumes", []):
        if "/secrets/" in (m.get("source") or "") and m.get("source") != f"{host}/secrets/{svc}":
            bad.append(f"{svc}: mounts another service's secrets {m['source']}")
if bad:
    print("\n".join(bad), file=sys.stderr)
    sys.exit(1)
PY
  # 3. Operator signer vars.
  if jq -e '.services["operator-solana"].environment | keys | map(select(test("^OPERATOR_(SIGNER|PRIVATE_KEY|VAULT_|TURNKEY_|PRIVY_)"))) | length > 0' \
    "$out/config.json" >/dev/null; then
    fail "$name: operator-solana carries an OPERATOR_* signer var"
  fi
  # 5. Replica command parses as bash once compose's $$ escape is undone.
  jq -r '.services["postgres-replica"].command[2]' "$out/config.json" | sed 's/\$\$/$/g' >"$out/replica.sh"
  bash -n "$out/replica.sh" || fail "$name: replica command does not parse"
}

# Per backend, the withdraw operator gets exactly that backend's OPERATOR_* vars.
SIGNER_KEYS_memory="PRIVATE_KEY"
SIGNER_KEYS_vault="VAULT_ADDR VAULT_TOKEN VAULT_KEY_NAME VAULT_PUBKEY"
SIGNER_KEYS_turnkey="TURNKEY_API_PUBLIC_KEY TURNKEY_API_PRIVATE_KEY TURNKEY_ORGANIZATION_ID TURNKEY_PRIVATE_KEY_ID TURNKEY_PUBKEY"
SIGNER_KEYS_privy="PRIVY_APP_ID PRIVY_APP_SECRET PRIVY_WALLET_ID"
for backend in memory vault turnkey privy; do
  vars=(-e admin_signer=$backend -e operator_signer=$backend)
  for role in admin operator; do
    vars+=(-e ${role}_vault_addr=https://vault.invalid -e ${role}_vault_key_name=k -e ${role}_pubkey=$ADMIN_PUB
      -e ${role}_turnkey_api_public_key=pk -e ${role}_turnkey_organization_id=org -e ${role}_turnkey_private_key_id=kid
      -e ${role}_privy_app_id=app -e ${role}_privy_wallet_id=wal)
  done
  render_setup "devnet-$backend" devnet "${vars[@]}" || continue
  check_setup "devnet-$backend"
  keys="SIGNER_KEYS_$backend"
  want="$(for k in ${!keys}; do echo "OPERATOR_$k"; done | sort | tr '\n' ' ')"
  # Base compose keeps a blank OPERATOR_PRIVATE_KEY, which a remote backend never reads.
  got="$(jq -r '.services["operator-private-channel"].environment | to_entries[] | select(.value != "" and .value != null) | .key | select(test("^OPERATOR_(PRIVATE_KEY|VAULT_|TURNKEY_|PRIVY_)"))' \
    "$TMP/devnet-$backend/config.json" | sed 's/_FILE$//' | sort -u | tr '\n' ' ')"
  [[ "$want" == "$got" ]] || fail "devnet-$backend: withdraw operator OPERATOR_* '$got' != '$want'"
  [[ "$(jq -r '.services["operator-private-channel"].environment.OPERATOR_SIGNER' "$TMP/devnet-$backend/config.json")" == $backend ]] \
    || fail "devnet-$backend: OPERATOR_SIGNER is not $backend"
done
render_setup localnet localnet && check_setup localnet
echo "ok: rendered compose (1, 2, 3, 5)"

# 4. Raw compose dual mode against the golden config from before secret files.
# The intended changes are dropped from both sides and checked on their own.
# Compose versions differ on printing bind create_host_path, so it is dropped too.
normalize() {
  jq -S 'walk(if type == "object" then del(.create_host_path) else . end)
    | del(.services["postgres-replica"].command)
    | del(.services["operator-solana"].environment.OPERATOR_SIGNER, .services["operator-solana"].environment.OPERATOR_PRIVATE_KEY)
    | del(.services.gateway.environment.JWT_SECRET_PREVIOUS, .services.gateway.environment.GATEWAY_REQUIRE_JWT)
    | del(.services.auth.environment.JWT_SECRET_PREVIOUS)' "$1"
}
for name in local devnet; do
  file=docker-compose.yml
  [[ $name == devnet ]] && file=docker-compose.devnet.yml
  if ! (cd "$REPO" && env -i PATH="$PATH" HOME="$HOME" docker compose -f "$file" --env-file "$FIX/$name.env" \
    --profile auth config --format json) 2>"$TMP/golden-$name.err" | sed "s|$REPO|REPO|g" >"$TMP/now-$name.json"; then
    fail "raw $name compose config: $(cat "$TMP/golden-$name.err")"
    continue
  fi
  if ! diff <(normalize "$FIX/$name.json") <(normalize "$TMP/now-$name.json") >"$TMP/golden-$name.diff"; then
    fail "raw $name compose drifted from the golden config:"
    head -40 "$TMP/golden-$name.diff" >&2
  fi
  # The withdraw operator still signs with the admin key in a single-key env.
  [[ "$(jq -r '.services["operator-private-channel"].environment.OPERATOR_PRIVATE_KEY' "$TMP/now-$name.json")" == GOLDENadminkey ]] \
    || fail "raw $name: OPERATOR_PRIVATE_KEY does not fall back to ADMIN_PRIVATE_KEY"
  for k in JWT_SECRET_PREVIOUS GATEWAY_REQUIRE_JWT; do
    [[ "$(jq -r ".services.gateway.environment.$k" "$TMP/now-$name.json")" == "" ]] || fail "raw $name: gateway $k is not blank"
  done
  # Raw mode reads the replica passwords from env at runtime, as before.
  jq -r '.services["postgres-replica"].command[2]' "$TMP/now-$name.json" | sed 's/\$\$/$/g' >"$TMP/raw-replica.sh"
  bash -n "$TMP/raw-replica.sh" || fail "raw $name: replica command does not parse"
  grep -q 'printf %s "$POSTGRES_PASSWORD"' "$TMP/raw-replica.sh" || fail "raw $name: replica no longer falls back to POSTGRES_PASSWORD"
done
echo "ok: raw compose golden (4)"

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "compose-secrets tests passed"
