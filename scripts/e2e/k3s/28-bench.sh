#!/usr/bin/env bash
# Step 28 (approximated): bench-tps transfer as an in-cluster Job against gateway-internal
# 8904 (a test-only allow row), with the baseline's parameters (200 accounts, 4 threads,
# batch 14, 60 s); once with synchronous replication on and once off on the same host.
# The binary is served from the control node; the Job runs the control node's image.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) bench"
bin="$REPO/target/release/private-channel-bench-tps"
[[ -x "$bin" ]] || blocked "bench binary not built"
mkdir -p "$RUN/bench-bin"; cp "$bin" "$RUN/bench-bin/bench"
ctl "cd /work/run/bench-bin && (setsid nohup python3 -m http.server 18080 >/dev/null 2>&1 &) ; sleep 1"
img=172.30.77.5:5000/pc-e2e/ctl:1
if ! "$RUN/bin/crane" manifest --insecure "$img" >/dev/null 2>&1; then
  docker save -o /tmp/k3s-ctl.tar k3s-e2e/ctl:1 && "$RUN/bin/crane" push --insecure /tmp/k3s-ctl.tar "$img" >/dev/null; rm -f /tmp/k3s-ctl.tar
fi
ctl "cat $(key admin)" | hk -n $NS create secret generic pc-e2e-bench-admin --from-file=admin.json=/dev/stdin --dry-run=client -o yaml | hk apply -f - >/dev/null
rule='{"k3s_np_extra_rules": [{"name": "e2e-bench", "to": "gateway", "ports": [8904], "from": ["pc-e2e-bench"]}]}'
echo "$rule" | ctl "cat > /tmp/bench-rule.json"
run_bench() {  # run_bench <label> [extra ansible args]
  local label="$1"; shift
  sync_repo
  ctl "cp /tmp/bench-rule.json $CTL_REPO/private-channel-deploy/bench-rule.json"
  playbook "$STEP-deploy-$label" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE" -e @bench-rule.json "$@" \
    || die "deploy for $label failed"
  hk -n $NS delete job pc-e2e-bench --ignore-not-found --wait >/dev/null
  hk -n $NS apply -f - >/dev/null <<Y
apiVersion: batch/v1
kind: Job
metadata: { name: pc-e2e-bench }
spec:
  backoffLimit: 0
  template:
    metadata: { labels: { app.kubernetes.io/name: pc-e2e-bench } }
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
      - name: bench
        image: $img
        command: [ /bin/bash, -c ]
        args: [ "curl -fsS -o /tmp/bench http://172.30.77.20:18080/bench && chmod +x /tmp/bench && /tmp/bench transfer --admin-keypair /keys/admin.json --rpc-url http://gateway-internal:8904 --accounts 200 --threads 4 --batch-size 14 --duration 60" ]
        volumeMounts: [ { name: k, mountPath: /keys, readOnly: true } ]
      volumes: [ { name: k, secret: { secretName: pc-e2e-bench-admin } } ]
Y
  hk -n $NS wait --for=condition=complete job/pc-e2e-bench --timeout=900s >/dev/null 2>&1 || true
  hk -n $NS logs job/pc-e2e-bench >"$RUN/logs/$STEP-$label.log" 2>&1
  local tps p
  tps="$(grep -iE 'tps|landed|dropped' "$RUN/logs/$STEP-$label.log" | tail -4 | tr '\n' ' ' | cut -c1-300)"
  p="$(hk -n pc-monitoring exec prometheus-pc-kps-prometheus-0 -c prometheus -- wget -qO- "http://localhost:9090/api/v1/query?query=$(python3 -c 'import urllib.parse;print(urllib.parse.quote("histogram_quantile(0.5, sum by (le) (increase(private_channel_gateway_request_duration_seconds_bucket{method=\"sendTransaction\"}[2m]))) * 1000"))')" | jq -r '.data.result[0].value[1] // "n/a"')"
  say "bench $label: $tps; gateway sendTransaction p50 ms: $p"
  finding "Step 28 bench ($label, localnet test VM, not comparable with the devnet baseline 4737.8 TPS): $tps p50 ms $p"
}
run_bench sync-on
run_bench sync-off -e k3s_sync_replication=false
run_bench restore
hk -n $NS delete job pc-e2e-bench --ignore-not-found >/dev/null
hk -n $NS delete secret pc-e2e-bench-admin --ignore-not-found >/dev/null
pass
