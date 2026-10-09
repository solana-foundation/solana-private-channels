#!/usr/bin/env bash
set -euo pipefail

# Deploy preflight refuses a config the binaries will not honour, and env.j2 renders one
# instance variable, explicit signer backends and the compiled program IDs.
# Needs ansible-playbook (and solana-keygen for the preflight cases); skips without them.
# Run from the repo root: ./scripts/tests/ansible-preflight.test.sh

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v ansible-playbook >/dev/null; then
  echo "skip: ansible-playbook not installed"
  exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
# A private copy, so the stub secrets.yml and ansible.log never land in the repo.
cp -r "$REPO/private-channel-deploy" "$TMP/deploy"
D="$TMP/deploy"
export ANSIBLE_CONFIG="$D/ansible.cfg" ANSIBLE_LOG_PATH="$TMP/ansible.log"
# The stub secrets.yml is the file backend; a caller's DOPPLER_TOKEN would switch it.
unset DOPPLER_TOKEN

ESCROW_ID=9tgHa1DcnaSSUtmMsst8ovKTe1Gfxzezn27KnH9xXYeU
WITHDRAW_ID=J231K9UEpS4y4KAPwGc4gsMNCjKFRMYcQBcjVW7vBhVi
INSTANCE=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz
PLACEHOLDER=11111111111111111111111111111111
WORKERS="indexer-solana operator-solana operator-private-channel"

failures=0
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

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

# Ansible refuses non-blocking stdio, so its output always goes to a file.
playbook() {
  local out="$1"
  shift
  ansible-playbook -i localhost, -c local "$@" >"$out" 2>&1 </dev/null
}

# Expect the config asserts to fail with `msg`.
expect_refused() {
  local name="$1" msg="$2"
  shift 2
  local tags=(--tags config_asserts)
  [[ " $* " == *" --tags "* ]] && tags=()
  if playbook "$TMP/$name.log" "$D/deploy.yml" "${tags[@]}" "$@"; then
    fail "$name: preflight passed, expected refusal"
  elif ! grep -q "$msg" "$TMP/$name.log"; then
    fail "$name: refused without '$msg'; see output below"
    tail -40 "$TMP/$name.log" >&2
  else
    echo "ok: $name refused"
  fi
}

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
    echo "skip: preflight cases (solana-keygen missing)"
    return
  fi
  solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/admin.json" >/dev/null
  local data="$TMP/host"
  mkdir -p "$data/config"
  local common=(-e env=dev -e "admin_keypair_path=$TMP/admin.json" -e "host_data_dir=$data")
  local devnet=("${common[@]}" -e network=devnet -e fallback_rpc_url=https://fallback.invalid)

  expect_refused mainnet "renders an in-memory hot key" "${devnet[@]}" -e network=mainnet -e escrow_instance_id=$INSTANCE
  expect_refused other_network "renders an in-memory hot key" "${devnet[@]}" -e network=prod -e escrow_instance_id=$INSTANCE
  # Tagged always: a single-phase rerun must not skip the memory-signer refusal.
  expect_refused mainnet_partial_run "renders an in-memory hot key" "${devnet[@]}" -e network=mainnet \
    -e escrow_instance_id=$INSTANCE --tags __no_such_phase__
  expect_refused mainnet_skip_preflight "renders an in-memory hot key" "${devnet[@]}" -e network=mainnet \
    -e escrow_instance_id=$INSTANCE --tags config_asserts --skip-tags preflight
  expect_refused devnet_no_instance "create the instance" "${devnet[@]}"
  expect_refused placeholder "all-ones placeholder" "${common[@]}" -e escrow_instance_id=$PLACEHOLDER
  expect_refused custom_program_id "must equal the compiled" "${common[@]}" -e escrow_program_id=$INSTANCE
  expect_refused custom_withdraw_id "must equal the compiled" "${common[@]}" -e withdraw_program_id=$INSTANCE

  expect_services localnet_no_instance no "${common[@]}"
  local kept=(-e reset_state=false -e validator_reset=false)
  expect_services localnet_instance yes "${common[@]}" "${kept[@]}" -e escrow_instance_id=$INSTANCE
  expect_services compiled_ids_accepted yes "${common[@]}" "${kept[@]}" -e escrow_instance_id=$INSTANCE \
    -e escrow_program_id=$ESCROW_ID -e withdraw_program_id=$WITHDRAW_ID
  expect_services devnet_instance yes "${devnet[@]}" -e escrow_instance_id=$INSTANCE

  # A redeploy keeps the PDA a previous render left in .env, with no var set (SOLA13-67).
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$INSTANCE" >"$data/config/.env"
  expect_services devnet_preserved_instance yes "${devnet[@]}"
  # A validator that boots with --reset wipes the PDA a previous render kept, even when
  # Postgres is kept.
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$INSTANCE" >"$data/config/.env"
  expect_services localnet_reset_drops_preserved_instance no "${common[@]}"
  expect_services localnet_validator_reset_drops_preserved_instance no "${common[@]}" -e reset_state=false
  expect_services localnet_kept_ledger_keeps_instance yes "${common[@]}" "${kept[@]}"
  rm "$data/config/.env"
  # The reset wipes an explicitly named instance too; a create_instance seed is random.
  expect_refused localnet_reset_with_named_instance "a localnet reset wipes" "${common[@]}" -e escrow_instance_id=$INSTANCE
  expect_refused localnet_validator_reset_with_named_instance "a localnet reset wipes" "${common[@]}" \
    -e escrow_instance_id=$INSTANCE -e reset_state=false
  expect_services localnet_named_instance_kept_ledger yes "${common[@]}" -e escrow_instance_id=$INSTANCE \
    -e reset_state=false -e validator_reset=false
  printf 'COMMON_ESCROW_INSTANCE_ID=%s\n' "$PLACEHOLDER" >"$data/config/.env"
  expect_refused devnet_preserved_placeholder "create the instance" "${devnet[@]}"
  rm "$data/config/.env"
}

render() {
  local out="$1"
  shift
  cat >"$TMP/render.yml" <<EOF
- hosts: localhost
  gather_facts: false
  vars_files: [ "$D/vars/common.yml", "$D/vars/dev.yml", "$D/secrets.yml" ]
  tasks:
  - ansible.builtin.template: { src: "$D/templates/env.j2", dest: "$out" }
EOF
  playbook "$out.log" "$TMP/render.yml" -e image_tag=t "$@" || {
    fail "render $out"
    tail -20 "$out.log" >&2
  }
}

instance_line() { grep -E '^COMMON_ESCROW_INSTANCE_ID=' "$1"; }

test_env_render() {
  render "$TMP/none.env"
  render "$TMP/var.env" -e escrow_instance_id=$INSTANCE
  render "$TMP/pda.env" -e escrow_instance_id_pda=$INSTANCE
  render "$TMP/both.env" -e escrow_instance_id=$INSTANCE -e escrow_instance_id_pda=$PLACEHOLDER

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
  echo "ok: env.j2 render"
}

test_syntax
test_preflight_cases
test_env_render

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "ansible preflight tests passed"
