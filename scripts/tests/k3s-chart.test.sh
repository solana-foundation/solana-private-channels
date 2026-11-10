#!/usr/bin/env bash
set -euo pipefail

# k3s chart contract: unittest, lint, kubeconform, the chart rendered from the real deploy
# vars (secret mounts, no secret in env, waits vs compose, NetworkPolicy table) and the
# committed secrets-rev table (UPDATE_REV_TABLE=1 rewrites it). No cluster needed.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
K8S_VERSION="${K8S_VERSION:-1.35.0}"

for t in helm kubeconform ansible-playbook; do
  if ! command -v "$t" >/dev/null; then
    echo "skip: k3s chart tests ($t missing)"
    [[ "${CI:-}" == true ]] && exit 1
    exit 0
  fi
done

# shellcheck source=scripts/tests/deploy-test-lib.sh
. "$REPO/scripts/tests/deploy-test-lib.sh"
deploy_test_setup

render_values() {
  local out="$1" secrets="$2"
  shift 2
  playbook "$out.log" "$REPO/scripts/tests/k3s/render-values.yml" -e "deploy_dir=$D" -e "out=$out" \
    -e "secrets_file=$secrets" "$@" || { fail "render values $out"; tail -20 "$out.log" >&2; return 1; }
}

net_args() {
  case "$1" in
    localnet) echo "-e network=localnet" ;;
    devnet) echo "-e network=devnet -e escrow_instance_id=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz -e yellowstone_endpoint=https://ys.invalid" ;;
  esac
}

test_unittest() {
  local c
  for c in private-channel pc-db; do
    if helm unittest "$REPO/charts/$c" >"$TMP/unittest-$c.log" 2>&1; then
      echo "ok: helm unittest $c ($(grep -E '^Tests:' "$TMP/unittest-$c.log" | tr -s ' '))"
    else
      fail "helm unittest $c"
      tail -30 "$TMP/unittest-$c.log" >&2
    fi
    helm lint "$REPO/charts/$c" >"$TMP/lint-$c.log" 2>&1 || { fail "helm lint $c"; tail -10 "$TMP/lint-$c.log" >&2; }
  done
}

test_rendered_contract() {
  sentinel_secrets "$TMP/secrets.yml"
  # The signer keys the deploy derives on the control node are secrets too.
  printf 'SNTLadminkey\nSNTLoperatorkey\n' >>"$TMP/sentinels.txt"
  local net compose
  for net in localnet devnet; do
    compose=docker-compose.yml
    [[ $net == devnet ]] && compose=docker-compose.devnet.yml
    # shellcheck disable=SC2046
    render_values "$TMP/values-$net.yaml" "$TMP/secrets.yml" $(net_args $net) -e streamer_test=1 || continue
    assert_no_sentinel "values.generated.yaml ($net)" "$TMP/values-$net.yaml"
    # streamer renders on localnet only when enabled; turn it on to check its contract too.
    local extra=()
    [[ $net == localnet ]] && extra=(--set streamer.enabled=true)
    if ! helm template private-channel "$REPO/charts/private-channel" -n private-channel \
      -f "$TMP/values-$net.yaml" "${extra[@]}" >"$TMP/rendered-$net.yaml" 2>"$TMP/rendered-$net.err"; then
      fail "helm template ($net)"
      cat "$TMP/rendered-$net.err" >&2
      continue
    fi
    assert_no_sentinel "rendered manifests ($net)" "$TMP/rendered-$net.yaml"
    if python3 "$REPO/scripts/tests/k3s/chart_checks.py" "$TMP/rendered-$net.yaml" "$TMP/values-$net.yaml" \
      "$REPO/$compose" "$TMP/sentinels.txt" >"$TMP/checks-$net.log" 2>&1; then
      echo "ok: chart contract ($net)"
    else
      fail "chart contract ($net)"
      cat "$TMP/checks-$net.log" >&2
    fi
    if kubeconform -strict -summary -kubernetes-version "$K8S_VERSION" "$TMP/rendered-$net.yaml" \
      >"$TMP/kubeconform-$net.log" 2>&1; then
      echo "ok: kubeconform app chart ($net): $(tail -1 "$TMP/kubeconform-$net.log")"
    else
      fail "kubeconform app chart ($net)"
      cat "$TMP/kubeconform-$net.log" >&2
    fi
  done
  # pc-db renders CNPG and Barman CRDs, which have no built-in schema.
  helm template pc-db "$REPO/charts/pc-db" -n private-channel --set backup.enabled=true \
    --set backup.destinationPath=s3://b/p >"$TMP/rendered-pcdb.yaml"
  if kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version "$K8S_VERSION" \
    "$TMP/rendered-pcdb.yaml" >"$TMP/kubeconform-pcdb.log" 2>&1; then
    echo "ok: kubeconform pc-db: $(tail -1 "$TMP/kubeconform-pcdb.log")"
  else
    fail "kubeconform pc-db"
    cat "$TMP/kubeconform-pcdb.log" >&2
  fi
}

# The grants SQL on k3s is the compose file's grant half, unchanged.
test_grants_sql() {
  local a b
  a="$(sed -n '/^GRANT CONNECT ON DATABASE :"app_db" TO private_channel_runtime/,$p' "$REPO/init-auth-roles.sql")"
  b="$(sed -n '/^GRANT CONNECT ON DATABASE :"app_db" TO private_channel_runtime/,$p' "$REPO/charts/pc-db/files/ledger-grants.sql")"
  [[ "$a" == "$b" ]] || fail "ledger-grants.sql drifted from the grant half of init-auth-roles.sql"
  a="$(sed -n '/^GRANT CONNECT ON DATABASE :"app_db" TO private_channel_grafana/,$p' "$REPO/init-indexer-roles.sql")"
  b="$(sed -n '/^GRANT CONNECT ON DATABASE :"app_db" TO private_channel_grafana/,$p' "$REPO/charts/pc-db/files/indexer-grants.sql")"
  [[ "$a" == "$b" ]] || fail "indexer-grants.sql drifted from the grant half of init-indexer-roles.sql"
  echo "ok: grants SQL matches the compose grant halves"
}

# secret var -> the workloads whose pods must roll when it changes, per env.
REV_VARS="postgres_password postgres_replication_password postgres_runtime_password postgres_auth_runtime_password
postgres_auth_owner_password postgres_gateway_password postgres_monitoring_password postgres_grafana_password
postgres_indexer_password jwt_secret jwt_secret_previous grafana_admin_password ghcr_token yellowstone_token
escrow_confidential_keys notify_webhook alert_webhook_url rpc_url fallback_rpc_url gateway_whitelist_rpc_url
pc_admin_private_key pc_operator_private_key"

test_rev_table() {
  sentinel_secrets "$TMP/rev-base.yml"
  printf 'pc_admin_private_key: SNTLadminkey\npc_operator_private_key: SNTLoperatorkey\n' >>"$TMP/rev-base.yml"
  local net v
  : >"$TMP/rev-table.yaml"
  for net in localnet devnet; do
    # shellcheck disable=SC2046
    render_values "$TMP/rev-$net-base" "$TMP/rev-base.yml" $(net_args $net) || return
    echo "$net:" >>"$TMP/rev-table.yaml"
    for v in $REV_VARS; do
      sed -E "s/^($v: '?)([^']*)/\1\2changed/" "$TMP/rev-base.yml" >"$TMP/rev-$v.yml"
      # shellcheck disable=SC2046
      render_values "$TMP/rev-$net-$v" "$TMP/rev-$v.yml" $(net_args $net) || return
      python3 - "$TMP/rev-$net-base.rev.json" "$TMP/rev-$net-$v.rev.json" "$v" >>"$TMP/rev-table.yaml" <<'PY'
import json, sys
a, b = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
rolls = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
print(f"  {sys.argv[3]}: [{', '.join(rolls)}]")
PY
    done
  done
  local table="$REPO/charts/private-channel/secrets-rev-table.yaml"
  if [[ "${UPDATE_REV_TABLE:-}" == 1 ]]; then
    { echo "# Generated by scripts/tests/k3s-chart.test.sh (UPDATE_REV_TABLE=1): the workloads whose pods"
      echo "# roll when each secret changes. CNPG pods never roll; CNPG applies passwords live."
      cat "$TMP/rev-table.yaml"; } >"$table"
    echo "ok: secrets-rev table written"
  elif diff <(grep -v '^#' "$table") "$TMP/rev-table.yaml" >"$TMP/rev-diff" 2>&1; then
    echo "ok: secrets-rev table matches secrets_rev_keys and secret_files"
  else
    fail "secrets-rev table is stale (rerun with UPDATE_REV_TABLE=1):"
    cat "$TMP/rev-diff" >&2
  fi
}

for t in test_unittest test_rendered_contract test_grants_sql test_rev_table; do
  if [[ -z "${PC_TEST_ONLY:-}" || "$t" =~ $PC_TEST_ONLY ]]; then "$t"; fi
done

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "k3s chart tests passed"
