#!/usr/bin/env bash
set -euo pipefail

# k3s.yml preflight refuses before connect: wrong k3s version, wrong or missing tools, low
# disk, no io_uring, reset_state off localnet, a wrong program id. The host probe is the
# _k3s_host_override test hook and the tools are stubs. Run from the repo root.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
for t in ansible-playbook solana-keygen; do
  if ! command -v "$t" >/dev/null; then
    echo "skip: k3s preflight tests ($t missing)"
    [[ "${CI:-}" == true ]] && exit 1
    exit 0
  fi
done

# shellcheck source=scripts/tests/deploy-test-lib.sh
. "$REPO/scripts/tests/deploy-test-lib.sh"
deploy_test_setup

cat >"$D/secrets.yml" <<'S'
postgres_password: p1
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
S
solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/admin.json" >/dev/null
solana-keygen new --no-bip39-passphrase --silent --outfile "$TMP/operator.json" >/dev/null

# Stub helm, kubectl and crane that report the pinned versions (or K3S_STUB_<tool>).
mkdir -p "$TMP/bin"
pin() { grep -E "^$1=" "$REPO/versions.env" | cut -d= -f2; }
cat >"$TMP/bin/helm" <<H
#!/bin/sh
case "\$1" in version) echo "\${K3S_STUB_HELM:-$(pin HELM_VERSION)}" ;; lint) exit 0 ;; esac
H
cat >"$TMP/bin/kubectl" <<K
#!/bin/sh
echo '{"clientVersion": {"gitVersion": "'"\${K3S_STUB_KUBECTL:-$(pin KUBECTL_VERSION)}"'"}}'
K
cat >"$TMP/bin/crane" <<C
#!/bin/sh
[ "\${K3S_STUB_CRANE:-}" = missing ] && exit 127
echo "\${K3S_STUB_CRANE:-$(pin CRANE_VERSION)}"
C
chmod +x "$TMP/bin/"*
export PATH="$TMP/bin:$PATH"

k3s_play() {
  local out="$1"
  shift
  playbook "$out" "$D/k3s.yml" "$@"
}
host_ok='{"k3s_version": "", "free_gb": 100, "io_uring_disabled": 0, "kernel": "6.8"}'
base=(-e env=dev -e "repo_root=$REPO" -e "admin_keypair_path=$TMP/admin.json" -e "operator_keypair_path=$TMP/operator.json"
  -e permanent_delegate=De1egate11111111111111111111111111111111111 -e reset_state=false -e validator_reset=false)
host() { echo "{\"_k3s_host_override\": $1}"; }

refused() {
  local name="$1" msg="$2"
  shift 2
  if k3s_play "$TMP/$name.log" "${base[@]}" "$@"; then
    fail "$name: passed, expected refusal"
  elif ! grep -q -- "$msg" "$TMP/$name.log"; then
    fail "$name: refused without '$msg'"
    tail -30 "$TMP/$name.log" >&2
  elif grep -q 'CONNECT\|Delete per-run Secrets\|Open the SSH tunnel' "$TMP/$name.log"; then
    fail "$name: refused only after connect"
  else
    echo "ok: $name refused before connect"
  fi
}
passes() {
  local name="$1"
  shift
  if k3s_play "$TMP/$name.log" "${base[@]}" "$@"; then echo "ok: $name passed"; else
    fail "$name: refused, expected pass"
    tail -30 "$TMP/$name.log" >&2
  fi
}

test_syntax() {
  for p in k3s.yml down-k3s.yml teardown-k3s.yml k3s-connect.yml; do
    playbook "$TMP/syntax-$p.log" "$D/$p" --syntax-check || fail "syntax-check $p"
  done
}

test_host_checks() {
  local tags=(--tags "k3s_host,config_asserts")
  passes host_ok "${tags[@]}" -e "$(host "$host_ok")"
  refused newer_k3s "newer than the pinned" "${tags[@]}" \
    -e "$(host '{"k3s_version": "v9.99.0+k3s1", "free_gb": 100, "io_uring_disabled": 0, "kernel": "6.8"}')"
  local pin; pin="$(sed -n 's/^K3S_VERSION=//p' "$REPO/versions.env")"
  refused newer_k3s_suffix "newer than the pinned" "${tags[@]}" \
    -e "$(host '{"k3s_version": "'"${pin%+k3s*}+k3s9"'", "free_gb": 100, "io_uring_disabled": 0, "kernel": "6.8"}')"
  passes older_k3s_upgrades "${tags[@]}" \
    -e "$(host '{"k3s_version": "v1.30.0+k3s1", "free_gb": 100, "io_uring_disabled": 0, "kernel": "6.8"}')"
  refused low_disk "GB free under /var/lib/rancher" "${tags[@]}" \
    -e "$(host '{"k3s_version": "", "free_gb": 3, "io_uring_disabled": 0, "kernel": "6.8"}')"
  refused no_io_uring "needs io_uring" "${tags[@]}" \
    -e "$(host '{"k3s_version": "", "free_gb": 100, "io_uring_disabled": 2, "kernel": "6.8"}')"
  passes io_uring_off_devnet_ok "${tags[@]}" -e network=devnet -e fallback_rpc_url=https://f.invalid \
    -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz \
    -e "$(host '{"k3s_version": "", "free_gb": 100, "io_uring_disabled": 2, "kernel": "6.8"}')"
  refused reset_off_localnet "reset_state=true is refused for network=devnet" "${tags[@]}" -e network=devnet \
    -e fallback_rpc_url=https://f.invalid -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz \
    -e reset_state=true -e "$(host "$host_ok")"
  refused long_token "at most 4h" "${tags[@]}" -e k3s_token_duration=8h -e "$(host "$host_ok")"
  # The guard runs even when only its action's tag is selected.
  refused reset_tag_only "reset_state=true is refused for network=devnet" --tags reset_state -e network=devnet \
    -e fallback_rpc_url=https://f.invalid -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz \
    -e reset_state=true -e "$(host "$host_ok")"
  # Skipping the preflight guard still leaves the one next to the wipe.
  refused reset_skip_preflight "wipes the ledger and indexer Clusters" --tags reset_state --skip-tags k3s_host \
    -e network=devnet -e fallback_rpc_url=https://f.invalid \
    -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz -e reset_state=true -e "$(host "$host_ok")"
  refused long_token_deploy_tag "at most 4h" --tags deploy -e k3s_token_duration=8h -e "$(host "$host_ok")"
  refused backup_no_creds "backup_s3_access_key_id" "${tags[@]}" -e k3s_backup_enabled=true \
    -e k3s_backup_destination_path=s3://b/p -e "$(host "$host_ok")"
  refused wrong_program_id "must equal the compiled" "${tags[@]}" \
    -e escrow_program_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz -e "$(host "$host_ok")"
}

test_tools() {
  local tags=(--tags k3s_tools)
  passes tools_pinned "${tags[@]}"
  K3S_STUB_HELM=v3.10.0 refused helm_wrong "helm is v3.10.0" "${tags[@]}"
  K3S_STUB_KUBECTL=v1.29.0 refused kubectl_wrong "kubectl is v1.29.0" "${tags[@]}"
  K3S_STUB_CRANE=0.1.0 refused crane_wrong "crane is 0.1.0" "${tags[@]}"
  # A stub that fails like a missing binary, so a real crane later on PATH cannot mask it.
  K3S_STUB_CRANE=missing refused crane_missing "crane is missing" "${tags[@]}"
}

for t in test_syntax test_host_checks test_tools; do
  if [[ -z "${PC_TEST_ONLY:-}" || "$t" =~ $PC_TEST_ONLY ]]; then "$t"; fi
done
if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "k3s preflight tests passed"
