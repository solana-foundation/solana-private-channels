#!/usr/bin/env bash
set -euo pipefail

# Deploy preflight refuses a config the binaries will not honour, and env.j2 renders one
# instance variable, explicit signer backends and the compiled program IDs.
# Needs ansible-playbook (and solana-keygen for the preflight cases); skips without them,
# except in CI (CI=true), where a missing tool fails the run.
# Run from the repo root: ./scripts/tests/ansible-preflight.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v ansible-playbook >/dev/null; then
  echo "skip: the playbook tests (ansible-playbook missing)"
  [[ "${CI:-}" == true ]] && exit 1
  exit 0
fi

# shellcheck source=scripts/tests/deploy-test-lib.sh
. "$REPO/scripts/tests/deploy-test-lib.sh"
deploy_test_setup

ESCROW_ID=9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU
WITHDRAW_ID=J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi
INSTANCE=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz
PLACEHOLDER=11111111111111111111111111111111
WORKERS="indexer-solana operator-solana operator-private-channel"

cat >"$D/secrets.yml" <<'EOF'
postgres_password: p1
postgres_replication_password: p2
postgres_runtime_password: p3
postgres_auth_runtime_password: p4
postgres_auth_owner_password: p5
postgres_gateway_password: p6
postgres_monitoring_password: p7
postgres_grafana_password: p8
postgres_indexer_password: p9
jwt_secret: 0123456789abcdef0123456789abcdef
grafana_admin_password: g
ghcr_user: u
ghcr_token: t
yellowstone_token: y
EOF

# Expect the config asserts to pass; prints the resolved compose_services.
expect_services() {
  local name="$1" want_workers="$2"
  shift 2
  if ! playbook "$TMP/$name.log" "$D/deploy.yml" --tags config_asserts "$@"; then
    fail "$name: preflight refused, expected pass"
    tail -40 "$TMP/$name.log" >&2
    return
  fi
  local services
  services="$(grep -oE 'compose_services: [a-z -]+' "$TMP/$name.log" | tail -1)"
  for w in $WORKERS; do
    if [[ "$want_workers" == yes ]] && [[ " $services " != *" $w "* ]]; then
      fail "$name: $w missing from '$services'"
    elif [[ "$want_workers" == no ]] && [[ " $services " == *" $w "* ]]; then
      fail "$name: $w deployed without an instance: '$services'"
    fi
  done
  echo "ok: $name ($services)"
}

test_syntax() {
  playbook "$TMP/syntax.log" "$D/deploy.yml" --syntax-check || {
    fail "syntax-check"
    tail -20 "$TMP/syntax.log" >&2
  }
}

test_preflight_cases() {
  if ! command -v solana-keygen >/dev/null; then
    skip "preflight cases (solana-keygen missing)"
    return
  fi
  solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/admin.json" >/dev/null
  solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/operator.json" >/dev/null
  local data="$TMP/host"
  mkdir -p "$data/config"
  local base=(-e env=dev -e "admin_keypair_path=$TMP/admin.json" -e "operator_keypair_path=$TMP/operator.json"
    -e "host_data_dir=$data")
  local common=("${base[@]}" -e permanent_delegate=De1egate11111111111111111111111111111111111)
  local devnet=("${common[@]}" -e network=devnet -e fallback_rpc_url=https://fallback.invalid)

  # No fallback to the admin key, so a missing delegate stops the deploy.
  expect_refused no_permanent_delegate "permanent_delegate is not set" "${base[@]}" -e escrow_instance_id=$INSTANCE

  # An unknown name would pick the localnet compose file without its validator assets.
  expect_refused unknown_network "localnet, devnet or mainnet" "${devnet[@]}" -e network=mainnet-beta \
    -e escrow_instance_id=$INSTANCE
  # Tagged always: a single-phase rerun must not skip the config asserts.
  expect_refused custom_id_partial_run "must equal the compiled" "${devnet[@]}" -e escrow_instance_id=$INSTANCE \
    -e escrow_program_id=$INSTANCE --tags __no_such_phase__
  expect_refused custom_id_skip_preflight "must equal the compiled" "${devnet[@]}" -e escrow_instance_id=$INSTANCE \
    -e escrow_program_id=$INSTANCE --tags config_asserts --skip-tags preflight
  expect_refused devnet_no_instance "create the instance" "${devnet[@]}"
  expect_refused placeholder "all-ones placeholder" "${common[@]}" -e escrow_instance_id=$PLACEHOLDER
  expect_refused custom_program_id "must equal the compiled" "${common[@]}" -e escrow_program_id=$INSTANCE
  expect_refused custom_withdraw_id "must equal the compiled" "${common[@]}" -e withdraw_program_id=$INSTANCE
  expect_refused shared_password "share a password" "${common[@]}" -e postgres_gateway_password=p1

  expect_services localnet_no_instance no "${common[@]}"
  local kept=(-e reset_state=false -e validator_reset=false)
  expect_services localnet_instance yes "${common[@]}" "${kept[@]}" -e escrow_instance_id=$INSTANCE
  expect_services compiled_ids_accepted yes "${common[@]}" "${kept[@]}" -e escrow_instance_id=$INSTANCE \
    -e escrow_program_id=$ESCROW_ID -e withdraw_program_id=$WITHDRAW_ID
  expect_services devnet_instance yes "${devnet[@]}" -e escrow_instance_id=$INSTANCE
  # Mainnet refuses memory signers, so this case runs both roles on Vault.
  local vault=(-e admin_signer=vault -e operator_signer=vault -e admin_pubkey=AdminRemote1111111111111111111111111111111
    -e operator_pubkey=OperRemote11111111111111111111111111111111 -e admin_vault_addr=https://v.invalid
    -e operator_vault_addr=https://v.invalid -e admin_vault_key_name=a -e operator_vault_key_name=o
    -e admin_vault_token=t -e operator_vault_token=t)
  expect_services mainnet_instance yes "${devnet[@]}" -e network=mainnet -e escrow_instance_id=$INSTANCE "${vault[@]}"

  # A redeploy keeps the PDA a previous render left in .env, with no var set.
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$INSTANCE" >"$data/config/.env"
  expect_services devnet_preserved_instance yes "${devnet[@]}"
  # A validator that boots with --reset wipes the PDA a previous render kept, even when
  # Postgres is kept.
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$INSTANCE" >"$data/config/.env"
  expect_services localnet_reset_drops_preserved_instance no "${common[@]}"
  expect_services localnet_validator_reset_drops_preserved_instance no "${common[@]}" -e reset_state=false
  # reset_state wipes the validator volume whatever validator_reset says, but only on a run
  # that includes its phase (check mode skips the wipe's shell tasks).
  local wipe=(--tags config_asserts,reset_state --check)
  expect_services localnet_state_reset_drops_preserved_instance no "${common[@]}" -e validator_reset=false "${wipe[@]}"
  expect_services localnet_partial_run_keeps_instance yes "${common[@]}" -e validator_reset=false
  expect_services localnet_kept_ledger_keeps_instance yes "${common[@]}" "${kept[@]}"
  rm "$data/config/.env"
  # The reset wipes an explicitly named instance too; a create_instance seed is random.
  expect_refused localnet_reset_with_named_instance "a localnet reset wipes" "${common[@]}" -e escrow_instance_id=$INSTANCE
  expect_refused localnet_validator_reset_with_named_instance "a localnet reset wipes" "${common[@]}" \
    -e escrow_instance_id=$INSTANCE -e reset_state=false
  expect_refused localnet_state_reset_with_named_instance "a localnet reset wipes" "${common[@]}" \
    -e escrow_instance_id=$INSTANCE -e validator_reset=false "${wipe[@]}"
  expect_services localnet_partial_run_with_named_instance yes "${common[@]}" \
    -e escrow_instance_id=$INSTANCE -e validator_reset=false
  expect_services localnet_named_instance_kept_ledger yes "${common[@]}" -e escrow_instance_id=$INSTANCE \
    -e reset_state=false -e validator_reset=false
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$PLACEHOLDER" >"$data/config/.env"
  expect_refused devnet_preserved_placeholder "create the instance" "${devnet[@]}"
  rm "$data/config/.env"
}

instance_line() { grep -E '^COMMON_ESCROW_INSTANCE_ID=' "$1"; }

test_env_render() {
  render "$TMP/none.env"
  render "$TMP/var.env" -e escrow_instance_id=$INSTANCE
  render "$TMP/pda.env" -e escrow_instance_id_pda=$INSTANCE
  render "$TMP/both.env" -e escrow_instance_id=$INSTANCE -e escrow_instance_id_pda=$PLACEHOLDER
  render "$TMP/hook.env" -e notify_webhook=https://hooks.invalid/x

  [[ "$(instance_line "$TMP/none.env")" == "COMMON_ESCROW_INSTANCE_ID=" ]] || fail "unset instance must render blank"
  [[ "$(instance_line "$TMP/var.env")" == "COMMON_ESCROW_INSTANCE_ID=$INSTANCE" ]] || fail "escrow_instance_id not rendered"
  [[ "$(instance_line "$TMP/pda.env")" == "COMMON_ESCROW_INSTANCE_ID=$INSTANCE" ]] || fail "preserved PDA not rendered"
  [[ "$(instance_line "$TMP/both.env")" == "COMMON_ESCROW_INSTANCE_ID=$INSTANCE" ]] || fail "explicit var must win over the preserved PDA"
  for f in "$TMP"/{none,var,pda,both}.env; do
    [[ "$(grep -cE '^(COMMON_)?ESCROW_INSTANCE_ID=' "$f")" == 1 ]] || fail "$f: expected exactly one instance line"
    grep -qx 'ADMIN_SIGNER=memory' "$f" || fail "$f: ADMIN_SIGNER=memory missing"
    grep -qx 'OPERATOR_SIGNER=memory' "$f" || fail "$f: OPERATOR_SIGNER=memory missing"
    grep -qx "ESCROW_PROGRAM_ID=$ESCROW_ID" "$f" || fail "$f: escrow program ID is not the compiled one"
    grep -qx "WITHDRAW_PROGRAM_ID=$WITHDRAW_ID" "$f" || fail "$f: withdraw program ID is not the compiled one"
  done
  # The webhook is a secret file now; the .env keeps the keys blank so compose's
  # no-op default applies when it is unset.
  for f in "$TMP/hook.env" "$TMP/none.env"; do
    grep -qx 'RECONCILIATION_WEBHOOK_URL=' "$f" || fail "$f: RECONCILIATION_WEBHOOK_URL must render blank"
    grep -qx 'ALERT_WEBHOOK_URL=' "$f" || fail "$f: ALERT_WEBHOOK_URL must render blank"
  done
  grep -q 'hooks.invalid' "$TMP/hook.env" && fail "the webhook URL reached the .env"
  echo "ok: env.j2 render"
}

# GHCR token scope check, against a stub of the GitHub API. The token must never
# reach the output, and only a provably read-only classic PAT passes.
test_ghcr_scope() {
  if ! command -v solana-keygen >/dev/null; then
    skip "GHCR scope cases (solana-keygen missing)"
    return
  fi
  [[ -f "$TMP/admin.json" ]] || solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/admin.json" >/dev/null
  mkdir -p "$TMP/ghcr-host/config"
  local token="SNTLghcrtoken$RANDOM$RANDOM"
  local base=(-e env=dev -e "admin_keypair_path=$TMP/admin.json" -e "host_data_dir=$TMP/ghcr-host"
    -e permanent_delegate=De1egate11111111111111111111111111111111111
    -e image_registry=ghcr.io/owner -e "ghcr_token=$token" --tags remote_checks)
  local name status hdr
  ghcr_case() {
    name="$1"
    if [[ "$2" == pass ]]; then
      PC_TEST_GITHUB_API="http://127.0.0.1:$STUB_PORT" expect_pass "$name" "${base[@]}" "${@:3}"
    else
      PC_TEST_GITHUB_API="http://127.0.0.1:$STUB_PORT" expect_refused "$name" "$2" "${base[@]}" "${@:3}"
    fi
    grep -qF "$token" "$TMP/$name.log" "$TMP/ansible.log" && fail "$name: GHCR token leaked"
    return 0
  }

  stub_http 200 "" "X-OAuth-Scopes: read:packages"
  ghcr_case ghcr_read_only pass
  stub_http 200 "" "X-OAuth-Scopes: write:packages, read:packages"
  ghcr_case ghcr_write_scope "write:packages, read:packages"
  stub_http 200 "" "X-OAuth-Scopes: repo"
  ghcr_case ghcr_repo_scope "scopes: repo"
  stub_http 200 ""
  ghcr_case ghcr_no_scope_header "read-only cannot be proven"
  stub_http 401 ""
  ghcr_case ghcr_invalid_token "rejected by GitHub (401)"
  stub_http 200 "" "X-OAuth-Scopes: read:packages"
  ghcr_case ghcr_fine_grained "classic PAT" -e "ghcr_token=github_pat_$token"
  stub_http 500 ""
  ghcr_case ghcr_server_error pass
  grep -q "could not verify the GHCR token scopes" "$TMP/ghcr_server_error.log" || fail "ghcr_server_error: no warning"
  STUB_PORT="$(free_port)"
  ghcr_case ghcr_unreachable pass
  grep -q "could not verify the GHCR token scopes" "$TMP/ghcr_unreachable.log" || fail "ghcr_unreachable: no warning"
  # Another registry never gets the PAT, so the check is skipped even for a bad token.
  stub_http 401 ""
  ghcr_case ghcr_other_registry pass -e image_registry=127.0.0.1:5000/owner
}

# Signer backends: every var a backend needs is required and named when missing;
# mainnet refuses keys held in memory; admin and operator differ off localnet.
SIGNER_VARS_vault="vault_addr vault_token vault_key_name pubkey"
SIGNER_VARS_turnkey="turnkey_api_public_key turnkey_api_private_key turnkey_organization_id turnkey_private_key_id pubkey"
SIGNER_VARS_privy="privy_app_id privy_app_secret privy_wallet_id pubkey"
ADMIN_PUB=AdminRemote1111111111111111111111111111111
OPERATOR_PUB=OperRemote11111111111111111111111111111111

# Writes an extra-vars file with every var `role` needs for `backend`, minus `drop`.
signer_vars() {
  local out="$1" role="$2" backend="$3" drop="${4:-}" v vars pub
  vars="SIGNER_VARS_$backend"
  pub=$ADMIN_PUB
  [[ $role == operator ]] && pub=$OPERATOR_PUB
  {
    echo "${role}_signer: $backend"
    for v in ${!vars}; do
      [[ "$v" == "$drop" ]] && continue
      if [[ $v == pubkey ]]; then echo "${role}_pubkey: $pub"
      elif [[ $v == vault_addr ]]; then echo "${role}_vault_addr: https://vault.invalid:8200"
      else echo "${role}_$v: SNTLsigner${role}${v}"; fi
    done
  } >"$out"
}

signer_setup() {
  [[ -f "$TMP/admin.json" ]] || solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/admin.json" >/dev/null
  [[ -f "$TMP/operator.json" ]] || solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/operator.json" >/dev/null
  mkdir -p "$TMP/sig-host/config"
  SIG_BASE=(-e env=dev -e "admin_keypair_path=$TMP/admin.json" -e "operator_keypair_path=$TMP/operator.json"
    -e "host_data_dir=$TMP/sig-host" -e permanent_delegate=De1egate11111111111111111111111111111111111)
  SIG_DEVNET=("${SIG_BASE[@]}" -e network=devnet -e fallback_rpc_url=https://fallback.invalid
    -e escrow_instance_id=$INSTANCE)
}

test_signers() {
  if ! command -v solana-keygen >/dev/null; then
    skip "signer cases (solana-keygen missing)"
    return
  fi
  signer_setup
  local role backend v vars
  for role in admin operator; do
    for backend in vault turnkey privy; do
      vars="SIGNER_VARS_$backend"
      signer_vars "$TMP/sv.yml" "$role" "$backend"
      expect_pass "signer_${role}_${backend}" "${SIG_DEVNET[@]}" -e "@$TMP/sv.yml"
      grep -q SNTLsigner "$TMP/signer_${role}_${backend}.log" "$TMP/ansible.log" && fail "signer_${role}_${backend}: signer secret leaked"
      for v in ${!vars}; do
        signer_vars "$TMP/sv.yml" "$role" "$backend" "$v"
        expect_refused "signer_${role}_${backend}_no_$v" "${role}_$v" "${SIG_DEVNET[@]}" -e "@$TMP/sv.yml"
      done
    done
  done
  signer_vars "$TMP/sv.yml" operator vault
  expect_refused signer_vault_http "must be an https:// URL" "${SIG_DEVNET[@]}" -e "@$TMP/sv.yml" \
    -e operator_vault_addr=http://vault.invalid:8200
  expect_refused signer_unknown_backend "memory, vault, turnkey or privy" "${SIG_DEVNET[@]}" -e admin_signer=kms

  # The same key in both roles buys no separation; localnet keeps the single-key dev path.
  expect_refused signer_equal_keys "must differ" "${SIG_DEVNET[@]}" -e "operator_keypair_path=$TMP/admin.json"
  expect_pass signer_equal_keys_localnet "${SIG_BASE[@]}" -e "operator_keypair_path=$TMP/admin.json"

  signer_vars "$TMP/av.yml" admin vault
  signer_vars "$TMP/ov.yml" operator vault
  local mainnet=("${SIG_DEVNET[@]}" -e network=mainnet)
  expect_refused signer_mainnet_memory "mainnet refuses the memory signer" "${mainnet[@]}"
  expect_refused signer_mainnet_operator_memory "mainnet refuses the memory signer" "${mainnet[@]}" -e "@$TMP/av.yml"
  expect_pass signer_mainnet_vault "${mainnet[@]}" -e "@$TMP/av.yml" -e "@$TMP/ov.yml"
  # The asserts that used to sit behind admin_keypair_path still run for remote signers.
  expect_refused signer_mainnet_vault_no_delegate "permanent_delegate is not set" "${mainnet[@]}" \
    -e "@$TMP/av.yml" -e "@$TMP/ov.yml" -e permanent_delegate=
  expect_refused signer_mainnet_vault_no_fallback "fallback_rpc_url is empty" "${mainnet[@]}" \
    -e "@$TMP/av.yml" -e "@$TMP/ov.yml" -e fallback_rpc_url=

  printf '[1,2,3]\n' >"$TMP/short.json"
  expect_refused signer_short_keypair "64-byte" "${SIG_DEVNET[@]}" -e "operator_keypair_path=$TMP/short.json"
  expect_refused signer_pubkey_mismatch "does not match" "${SIG_DEVNET[@]}" -e admin_pubkey=$ADMIN_PUB

  # The operator key alone can release funds, so a memory operator beside a remote admin warns.
  expect_pass signer_devnet_mixed "${SIG_DEVNET[@]}" -e "@$TMP/av.yml"
  grep -q "WARNING: operator_signer is memory" "$TMP/signer_devnet_mixed.log" || fail "signer_devnet_mixed: no warning"

  # Doppler: a keypair held in Doppler is refused on mainnet.
  mkdir -p "$TMP/dbin"
  cat >"$TMP/dbin/doppler" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == --version ]] && { echo v3; exit 0; }
cat "$DOPPLER_FAKE_JSON"
EOF
  chmod +x "$TMP/dbin/doppler"
  python3 - "$D/secrets.yml" "$TMP/admin.json" >"$TMP/doppler.json" <<'PY'
import json, sys, yaml
d = {k.upper(): str(v) for k, v in yaml.safe_load(open(sys.argv[1])).items()}
d["ADMIN_KEYPAIR"] = open(sys.argv[2]).read()
print(json.dumps(d))
PY
  PATH="$TMP/dbin:$PATH" DOPPLER_TOKEN=fake DOPPLER_FAKE_JSON="$TMP/doppler.json" \
    expect_refused signer_mainnet_doppler_keypair "ADMIN_KEYPAIR" "${mainnet[@]}" -e "@$TMP/av.yml" -e "@$TMP/ov.yml"
  # On devnet a Doppler keypair is the memory key source, and its pubkey is derived from it.
  PATH="$TMP/dbin:$PATH" DOPPLER_TOKEN=fake DOPPLER_FAKE_JSON="$TMP/doppler.json" \
    expect_pass signer_devnet_doppler_keypair "${SIG_DEVNET[@]}" -e admin_keypair_path=/nonexistent
}

# The Operator PDA must exist before the withdraw operator starts, or every release fails.
test_operator_pda() {
  if ! command -v solana >/dev/null; then
    skip "operator PDA cases (solana CLI missing)"
    return
  fi
  signer_setup
  local op owner rpc_tag="SNTLpdarpc$RANDOM"
  op="$(solana-keygen pubkey "$TMP/operator.json")"
  pda_case() {
    local name="$1" want="$2"
    shift 2
    local args=("${SIG_DEVNET[@]}" -e "rpc_url=http://127.0.0.1:$STUB_PORT/$rpc_tag" -e image_registry=127.0.0.1:5000/o
      --tags remote_checks "$@")
    if [[ $want == pass ]]; then expect_pass "$name" "${args[@]}"; else expect_refused "$name" "$want" "${args[@]}"; fi
    grep -q "$rpc_tag" "$TMP/$name.log" "$TMP/ansible.log" && fail "$name: rpc_url leaked"
    return 0
  }
  printf '{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":null}}' >"$TMP/pda-null.json"
  stub_http 200 "$TMP/pda-null.json" "Content-Type: application/json"
  pda_case pda_missing "$op"
  owner="$ESCROW_ID"
  printf '{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"owner":"%s","lamports":1,"data":["","base64"],"executable":false}}}' "$owner" >"$TMP/pda-ok.json"
  stub_http 200 "$TMP/pda-ok.json" "Content-Type: application/json"
  pda_case pda_present pass
  printf '{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":{"owner":"11111111111111111111111111111111","lamports":1,"data":["","base64"],"executable":false}}}' >"$TMP/pda-bad.json"
  stub_http 200 "$TMP/pda-bad.json" "Content-Type: application/json"
  pda_case pda_wrong_owner "owned by"
  STUB_PORT="$(free_port)"
  pda_case pda_unreachable pass
  grep -q "could not check the Operator PDA" "$TMP/pda_unreachable.log" || fail "pda_unreachable: no warning"

  # Localnet runs the same check whenever an instance is set, explicit or carried over.
  local lnet=("${SIG_BASE[@]}" -e reset_state=false -e validator_reset=false -e image_registry=127.0.0.1:5000/o
    --tags remote_checks)
  stub_http 200 "$TMP/pda-null.json" "Content-Type: application/json"
  lnet+=(-e "rpc_url=http://127.0.0.1:$STUB_PORT/$rpc_tag")
  expect_refused pda_localnet_missing "$op" "${lnet[@]}" -e escrow_instance_id=$INSTANCE
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$INSTANCE" >"$TMP/sig-host/config/.env"
  expect_refused pda_localnet_preserved_missing "$op" "${lnet[@]}"
  rm "$TMP/sig-host/config/.env"
  # A first boot has no instance, so nothing is checked (the stub would refuse).
  expect_pass pda_localnet_first_boot "${SIG_BASE[@]}" -e "rpc_url=http://127.0.0.1:$STUB_PORT/$rpc_tag" \
    -e image_registry=127.0.0.1:5000/o --tags remote_checks
  stub_http 200 "$TMP/pda-ok.json" "Content-Type: application/json"
  lnet[-1]="rpc_url=http://127.0.0.1:$STUB_PORT/$rpc_tag"
  expect_pass pda_localnet_present "${lnet[@]}" -e escrow_instance_id=$INSTANCE
  grep -q "$rpc_tag" "$TMP"/pda_localnet_*.log "$TMP/ansible.log" && fail "pda_localnet: rpc_url leaked"
  return 0
}

# The .env carries exactly the signer_util vars of the selected backend, pubkeys come
# from one place, and no private key is written for a remote signer.
test_render_signers() {
  if ! command -v solana-keygen >/dev/null; then
    skip "signer render (solana-keygen missing)"
    return
  fi
  signer_setup
  local su="$REPO/indexer/src/operator/utils/signer_util.rs" backend role want got
  local fake=(-e pc_admin_pubkey=$ADMIN_PUB -e pc_operator_pubkey=$OPERATOR_PUB)
  for backend in memory vault turnkey privy; do
    for role in admin operator; do
      if [[ $backend == memory ]]; then
        printf '%s_signer: memory\n' "$role" >"$TMP/rv.yml"
      else
        signer_vars "$TMP/rv.yml" "$role" "$backend"
      fi
      render "$TMP/sig-$backend-$role.env" "${fake[@]}" -e "@$TMP/rv.yml" -e '{"extra_admin_keys": ["Extra1111111111111111111111111111111111111"]}'
      local up="${role^^}"
      local bk="${backend^^}"
      [[ $backend == memory ]] && bk=PRIVATE_KEY
      want="$(grep -oE "\"${up}_${bk}[A-Z_]*\"" "$su" | tr -d '"' | sort -u)"
      # The private key line is always present (blank for a remote signer), so compose
      # does not warn; only the selected backend's own vars are compared.
      local keys="(VAULT|TURNKEY|PRIVY)"
      [[ $backend == memory ]] && keys="(PRIVATE_KEY|VAULT|TURNKEY|PRIVY)"
      got="$(grep -oE "^${up}_${keys}[A-Z_]*=" "$TMP/sig-$backend-$role.env" | tr -d '=' | sort -u)"
      [[ "$want" == "$got" ]] || fail "render $backend/$role: .env keys '$(echo $got)' != signer_util '$(echo $want)'"
      grep -qx "${up}_SIGNER=$backend" "$TMP/sig-$backend-$role.env" || fail "render $backend/$role: ${up}_SIGNER not $backend"
      if [[ $backend != memory ]]; then
        grep -qx "${up}_PRIVATE_KEY=" "$TMP/sig-$backend-$role.env" || fail "render $backend/$role: ${up}_PRIVATE_KEY missing or not blank"
      fi
    done
  done
  grep -qx "PRIVATE_CHANNEL_ADMIN_KEYS=$ADMIN_PUB,Extra1111111111111111111111111111111111111" "$TMP/sig-vault-admin.env" \
    || fail "PRIVATE_CHANNEL_ADMIN_KEYS is not admin_pubkey plus extra_admin_keys"
  grep -qx "ADMIN_VAULT_PUBKEY=$ADMIN_PUB" "$TMP/sig-vault-admin.env" || fail "ADMIN_VAULT_PUBKEY not from admin_pubkey"
  echo "ok: signer render"
}

# JWT_SECRET_PREVIOUS follows the same rules as gateway and auth, and stays blank
# whenever RBAC is off.
test_jwt_previous() {
  if ! command -v solana-keygen >/dev/null; then
    skip "JWT previous cases (solana-keygen missing)"
    return
  fi
  signer_setup
  local cur=0123456789abcdef0123456789abcdef
  expect_refused jwt_previous_short "jwt_secret_previous" "${SIG_BASE[@]}" -e jwt_secret_previous=short
  # Equal after trimming, so the surrounding spaces must not make it pass.
  expect_refused jwt_previous_equal "jwt_secret_previous" "${SIG_BASE[@]}" -e "{\"jwt_secret_previous\": \" $cur \"}"
  expect_refused jwt_previous_blank_current "jwt_secret_previous" "${SIG_BASE[@]}" -e jwt_secret= \
    -e jwt_secret_previous=fedcba9876543210fedcba9876543210
  expect_pass jwt_previous_unset "${SIG_BASE[@]}"
  expect_pass jwt_previous_ok "${SIG_BASE[@]}" -e jwt_secret_previous=fedcba9876543210fedcba9876543210
  grep -q fedcba9876543210 "$TMP/jwt_previous_ok.log" "$TMP/jwt_previous_short.log" && fail "jwt previous leaked"
  render "$TMP/jwt-off.env" -e gateway_enforce_jwt=false -e jwt_secret_previous=fedcba9876543210fedcba9876543210
  grep -qx 'JWT_SECRET_PREVIOUS=' "$TMP/jwt-off.env" || fail "JWT_SECRET_PREVIOUS must render blank with RBAC off"
  grep -qx 'GATEWAY_REQUIRE_JWT=' "$TMP/jwt-off.env" || fail "GATEWAY_REQUIRE_JWT must render blank with RBAC off"
  render "$TMP/jwt-on.env" -e jwt_secret_previous=fedcba9876543210fedcba9876543210
  grep -qx 'GATEWAY_REQUIRE_JWT=1' "$TMP/jwt-on.env" || fail "GATEWAY_REQUIRE_JWT=1 not rendered with RBAC on"
  echo "ok: jwt previous render"
}

# Secret files: right value, right owner and mode, exact set, stale files removed,
# secrets-rev changes only where values changed, nothing leaks into logs, and the
# Doppler backend renders the same tree.
secrets_snapshot() {
  local host="$1" out="$2"
  (cd "$host" && find secrets -type f ! -name .salt | sort | while read -r f; do printf '%s=%s\n' "$f" "$(cat "$f")"; done) >"$out.tree"
  grep -v '^#' "$host/config/.env" >"$out.env"
  cp "$host/config/compose.override.yml" "$out.override"
}

labels_of() {
  python3 -c 'import sys, yaml
d = yaml.safe_load(open(sys.argv[1]))["services"]
for k, v in sorted(d.items()):
    print(k, (v.get("labels") or {}).get("secrets-rev", "-"))' "$1"
}

test_secrets_render() {
  if ! command -v solana-keygen >/dev/null; then
    skip "secrets render (solana-keygen missing)"
    return
  fi
  if [[ $(id -u) != 0 ]] && ! sudo -n true 2>/dev/null; then
    skip "secrets render (needs root or passwordless sudo)"
    return
  fi
  signer_setup
  local host="$TMP/sec-host" saved="$TMP/secrets.yml.saved"
  mkdir -p "$host/config"
  cp "$D/secrets.yml" "$saved"
  sentinel_secrets "$D/secrets.yml"
  local args=(-e env=dev -e "admin_keypair_path=$TMP/admin.json" -e "operator_keypair_path=$TMP/operator.json"
    -e "host_data_dir=$host" -e permanent_delegate=De1egate11111111111111111111111111111111111
    -e network=devnet -e escrow_instance_id=$INSTANCE -e yellowstone_endpoint=https://ys.invalid -e "repo_root=$REPO")

  if ! playbook "$TMP/sec1.log" "$D/deploy.yml" --tags render,secrets_render -vvv --diff "${args[@]}"; then
    fail "secrets render failed"
    tail -40 "$TMP/sec1.log" >&2
    cp "$saved" "$D/secrets.yml"
    return
  fi
  assert_no_sentinel "secrets render -vvv --diff output" "$TMP/sec1.log" "$TMP/ansible.log"
  assert_no_sentinel "rendered .env and override" "$host/config/.env" "$host/config/compose.override.yml"

  local sv
  sv() { grep -E "^$1:" "$D/secrets.yml" | sed -E "s/^$1: '?([^']*)'?$/\1/"; }
  [[ "$(cat "$host/secrets/postgres-primary/POSTGRES_PASSWORD")" == "$(sv postgres_password)" ]] || fail "postgres-primary POSTGRES_PASSWORD value"
  [[ "$(cat "$host/secrets/gateway/JWT_SECRET")" == "$(sv jwt_secret)" ]] || fail "gateway JWT_SECRET value"
  [[ "$(cat "$host/secrets/operator-solana/COMMON_SOURCE_RPC_URL")" == "$(sv rpc_url)" ]] || fail "operator-solana rpc value"
  [[ "$(cat "$host/secrets/write-node/PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL")" == \
     "postgres://private_channel_runtime:$(sv postgres_runtime_password)@postgres-primary:5432/private_channel" ]] || fail "write-node DB URL"
  [[ -s "$host/secrets/operator-private-channel/OPERATOR_PRIVATE_KEY" ]] || fail "operator key file missing"
  [[ -e "$host/secrets/operator-solana/OPERATOR_PRIVATE_KEY" ]] && fail "operator-solana got the release key"

  [[ "$(stat -c '%a %U' "$host/secrets")" == "700 root" ]] || fail "secrets root mode/owner: $(stat -c '%a %U' "$host/secrets")"
  local bad
  bad="$(find "$host/secrets" -mindepth 1 -maxdepth 1 -type d ! -perm 0755 -o -mindepth 1 -maxdepth 1 -type d ! -user root)"
  [[ -z "$bad" ]] || fail "service dirs not root 0755: $bad"
  bad="$(find "$host/secrets" -mindepth 2 -type f ! -perm 0444 -o -mindepth 2 -type f ! -user root)"
  [[ -z "$bad" ]] || fail "secret files not root 0444: $bad"
  [[ "$(stat -c '%a' "$host/secrets/.salt")" == 400 ]] || fail "salt not 0400"

  # Completeness: every secret the base compose interpolates for a service has a file.
  python3 - "$REPO/docker-compose.devnet.yml" "$host" <<'PY' || fail "secret_files is incomplete (see above)"
import os, re, sys, yaml
SECRET = re.compile(r'\$\{(POSTGRES_PASSWORD|POSTGRES_\w*_PASSWORD|JWT_SECRET\w*|ADMIN_PRIVATE_KEY|OPERATOR_PRIVATE_KEY|INDEXER_YELLOWSTONE_TOKEN|ALERT_WEBHOOK_URL|RECONCILIATION_WEBHOOK_URL|GF_ADMIN_PASSWORD|LOCAL_VALIDATOR_RPC_URL|DEVNET_RPC_URL|DEVNET_FALLBACK_RPC_URL|GATEWAY_WHITELIST_RPC_URL)')
compose, host = sys.argv[1], sys.argv[2]
missing = []
for svc, c in yaml.safe_load(open(compose))["services"].items():
    if svc in ("grafana", "prometheus"):
        continue
    for e in c.get("environment", []):
        key = e.split("=", 1)[0]
        if SECRET.search(e) and not os.path.exists(f"{host}/secrets/{svc}/{key}"):
            missing.append(f"{svc}/{key}")
if missing:
    print("missing secret files: " + " ".join(missing), file=sys.stderr)
    sys.exit(1)
PY

  # The override names exactly the files that exist, per service.
  python3 - "$host" <<'PY' || fail "override _FILE set differs from the tree"
import os, sys, yaml
host = sys.argv[1]
svcs = yaml.safe_load(open(f"{host}/config/compose.override.yml"))["services"]
bad = []
for svc, c in svcs.items():
    d = f"{host}/secrets/{svc}"
    files = set(os.listdir(d)) if os.path.isdir(d) else set()
    env = {k[:-5] for k in (c.get("environment") or {}) if k.endswith("_FILE")}
    if env != files:
        bad.append(f"{svc}: env {sorted(env)} files {sorted(files)}")
    vols = c.get("volumes") or []
    if files and vols != [f"{d}:/run/secrets:ro"]:
        bad.append(f"{svc}: volumes {vols}")
if bad:
    print("\n".join(bad), file=sys.stderr)
    sys.exit(1)
PY
  secrets_snapshot "$host" "$TMP/snap-file"
  labels_of "$host/config/compose.override.yml" >"$TMP/labels1"

  # Doppler parity: the same values from a fake Doppler render the same files.
  python3 - "$D/secrets.yml" >"$TMP/doppler-sec.json" <<'PY'
import json, sys, yaml
y = yaml.safe_load(open(sys.argv[1]))
d = {k.upper(): str(v) for k, v in y.items() if k not in ("notify_webhook", "alert_webhook_url")}
d["ALERT_WEBHOOK_URL"] = y["notify_webhook"]
d["GRAFANA_ALERT_WEBHOOK_URL"] = y["alert_webhook_url"]
print(json.dumps(d))
PY
  mkdir -p "$TMP/dbin"
  cat >"$TMP/dbin/doppler" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == --version ]] && { echo v3; exit 0; }
cat "$DOPPLER_FAKE_JSON"
EOF
  chmod +x "$TMP/dbin/doppler"
  if PATH="$TMP/dbin:$PATH" DOPPLER_TOKEN=fake DOPPLER_FAKE_JSON="$TMP/doppler-sec.json" \
    playbook "$TMP/sec-doppler.log" "$D/deploy.yml" --tags render,secrets_render "${args[@]}"; then
    secrets_snapshot "$host" "$TMP/snap-doppler"
    for part in tree env override; do
      diff -q "$TMP/snap-file.$part" "$TMP/snap-doppler.$part" >/dev/null || fail "doppler parity: $part differs"
    done
    assert_no_sentinel "doppler render output" "$TMP/sec-doppler.log"
  else
    fail "doppler render failed"
    tail -30 "$TMP/sec-doppler.log" >&2
  fi

  # Clear the webhook and rotate one password: stale files go, and only the
  # services whose values changed get a new secrets-rev.
  sed -i -E "s/^notify_webhook: .*/notify_webhook: ''/; s/^postgres_runtime_password: (.*)/postgres_runtime_password: \1new/" "$D/secrets.yml"
  if playbook "$TMP/sec2.log" "$D/deploy.yml" --tags render,secrets_render "${args[@]}"; then
    [[ -e "$host/secrets/indexer-solana/ALERT_WEBHOOK_URL" ]] && fail "a cleared webhook left its file"
    grep -q 'ALERT_WEBHOOK_URL_FILE' "$host/config/compose.override.yml" && fail "a cleared webhook kept its _FILE var"
    labels_of "$host/config/compose.override.yml" >"$TMP/labels2"
    local changed
    changed="$( (diff "$TMP/labels1" "$TMP/labels2" || true) | { grep '^>' || true; } | awk '{print $2}' | sort | tr '\n' ' ')"
    # Postgres reads only its superuser password after initdb, so a role password leaves it alone.
    [[ "$changed" == "indexer-private-channel indexer-solana operator-private-channel operator-solana read-node write-node " ]] \
      || fail "secrets-rev changed for: '$changed'"
  else
    fail "second secrets render failed"
    tail -30 "$TMP/sec2.log" >&2
  fi
  rev_case() {
    local name="$1" want="$2" changed
    if ! playbook "$TMP/$name.log" "$D/deploy.yml" --tags render,secrets_render "${args[@]}"; then
      fail "$name: render failed"
      tail -30 "$TMP/$name.log" >&2
      return
    fi
    labels_of "$host/config/compose.override.yml" >"$TMP/labels-$name"
    changed="$( (diff "$TMP/labels2" "$TMP/labels-$name" || true) | { grep '^>' || true; } | awk '{print $2}' | sort | tr '\n' ' ')"
    [[ "$changed" == "$want" ]] || fail "$name: secrets-rev changed for '$changed', want '$want'"
    cp "$TMP/labels-$name" "$TMP/labels2"
  }
  sed -i -E "s/^(postgres_(gateway|monitoring|auth_owner|auth_runtime|grafana)_password): (.*)/\1: \3new/" "$D/secrets.yml"
  rev_case rev_role_passwords "auth auth-migrate gateway "
  sed -i -E "s/^postgres_replication_password: (.*)/postgres_replication_password: \1new/" "$D/secrets.yml"
  # The databases take password changes live (ALTER ROLE, replica passfile), so they never restart.
  rev_case rev_replication "pg-backup-primary "
  sed -i -E "s/^postgres_password: (.*)/postgres_password: \1new/" "$D/secrets.yml"
  rev_case rev_superuser ""
  sed -i -E "s/^postgres_indexer_password: (.*)/postgres_indexer_password: \1new/" "$D/secrets.yml"
  rev_case rev_indexer_superuser "indexer-private-channel indexer-solana operator-private-channel operator-solana pg-backup-indexer "

  # A standalone --tags deploy renders the tree, labels and override from the same tasks.
  playbook "$TMP/deploy-tasks.log" "$D/deploy.yml" --list-tasks --tags deploy "${args[@]}"
  for t in "Write the secret files" "Compute the secrets-rev label per service" "Render compose override"; do
    grep -q "$t" "$TMP/deploy-tasks.log" || fail "--tags deploy does not run '$t'"
  done
  cp "$saved" "$D/secrets.yml"
  echo "ok: secrets render"
}

# PC_TEST_ONLY=<regex> runs only the matching test functions.
for t in test_syntax test_preflight_cases test_env_render test_ghcr_scope test_signers test_operator_pda test_render_signers test_jwt_previous test_secrets_render; do
  if [[ -z "${PC_TEST_ONLY:-}" || "$t" =~ $PC_TEST_ONLY ]]; then "$t"; fi
done

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "ansible preflight tests passed"
