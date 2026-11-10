#!/usr/bin/env bash
# Step 1: fresh host to running stack, then the host proofs (API from a pod, kubelet scrape,
# add-ons, Secrets encrypted at rest, off-host ports, nofile, idempotent rerun, version
# bump with BUMP_TO=<version>, reboot unless SKIP_REBOOT=1).
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) bootstrap"
prepare_host
finding "APPROXIMATED: e2e hosts are containers with a private / mount; a unit makes it rshared (node-exporter needs it, as on a real VM)."
sync_repo
playbook "$STEP-deploy1" k3s.yml || die "first k3s.yml run failed (logs/$STEP-deploy1.log)"
ok "first deploy"

hk get nodes -o wide | tee -a "$LOG"
for ns_dep in cert-manager/cert-manager cnpg-system/cnpg-cloudnative-pg pc-monitoring/pc-kps-operator pc-monitoring/pc-kps-grafana; do
  hk -n "${ns_dep%%/*}" rollout status "deploy/${ns_dep#*/}" --timeout=60s >>"$LOG" 2>&1 || die "$ns_dep not Ready"
done
hk -n cnpg-system get deploy -o name | grep -q barman || die "barman plugin missing"
hk -n cnpg-system rollout status deploy -l app.kubernetes.io/name=plugin-barman-cloud --timeout=60s >>"$LOG" 2>&1 || die "barman plugin not Ready"
ok "cert-manager, CNPG, Barman plugin, kube-prometheus-stack Ready"

# A pod reaches the API through kubernetes.default despite the 6443 drop on eth0.
code=$(hk -n $NS exec deploy/gateway -c gateway -- curl -sk -o /dev/null -w '%{http_code}' https://kubernetes.default/version)
[[ "$code" =~ ^(200|401|403)$ ]] || die "pod cannot reach kubernetes.default (http $code)"
ok "pod reaches https://kubernetes.default (http $code)"

prom() { hk -n pc-monitoring exec prometheus-pc-kps-prometheus-0 -c prometheus -- wget -qO- "http://localhost:9090/api/v1/query?query=$1"; }
wait_for 180 bash -c "$(declare -f prom hk); E2E_HOST='$E2E_HOST'; prom 'up%7Bjob%3D%22kubelet%22%7D' | grep -q '\"1\"'" \
  || die "up{job=kubelet} is not 1"
ok "up{job=kubelet} == 1"

onhost "k3s secrets-encrypt status" | tee -a "$LOG" | grep -q 'Encryption Status: Enabled' || die "secrets encryption off"
known="$(e2e_secret postgres_runtime_password)"
hits=$(onhost "cat /var/lib/rancher/k3s/server/db/state.db* | grep -a -c -F '$known' || true")
[[ "$hits" == 0 ]] || die "a Secret value is in the datastore in clear ($hits hits)"
ok "secrets encrypted in state.db (0 clear hits)"

for p in 6443 10250; do
  ctl "nc -z -w 3 $E2E_HOST_IP $p" && die "port $p open from the control node"
done
for p in 8899 8903; do
  ctl "nc -z -w 3 $E2E_HOST_IP $p" || die "port $p closed from the control node"
done
open=$(ctl "nmap -Pn -p 1-65535 --min-rate 2000 -oG - $E2E_HOST_IP | grep -o '[0-9]*/open' | cut -d/ -f1 | sort -n | tr '\n' ' '")
say "open ports from control node: $open"
for p in $open; do [[ "$p" =~ ^(22|8899|8903)$ ]] || die "unexpected open port $p"; done
ok "only 22, 8899, 8903 reachable off-host"

nofile=$(hk -n $NS exec deploy/gateway -c gateway -- sh -c 'ulimit -n')
(( nofile >= 65536 )) || die "nofile in a pod is $nofile"
ok "nofile in a pod = $nofile"

# Long-running pods only: hook Jobs (auth-migrate, grants) are recreated by design.
uids() {
  { hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}'
    hk -n $NS get pods -l 'private-channel.io/db,!job-name' -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}'
    hk -n pc-monitoring get pods -l app.kubernetes.io/name=grafana -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.uid}{"\n"}{end}'; } | sort
}
before="$(uids)"
playbook "$STEP-deploy2" k3s.yml -e reset_state=false || die "second run failed"
after="$(uids)"
[[ "$before" == "$after" ]] || { diff <(echo "$before") <(echo "$after") | tee -a "$LOG"; die "a rerun with no change rolled pods"; }
changed_boot=$(grep -A1 -E 'TASK \[(Install|Download|Render the k3s|Restart k3s)' "$RUN/logs/$STEP-deploy2.log" | grep -c '^changed' || true)
ok "second run rolled nothing (bootstrap changed tasks: $changed_boot)"

if [[ -n "${BUMP_TO:-}" ]]; then
  playbook "$STEP-bump" k3s.yml -e reset_state=false -e "k3s_version=$BUMP_TO" || die "bump run failed"
  v=$(hk get nodes -o jsonpath='{.items[0].status.nodeInfo.kubeletVersion}')
  [[ "$v" == "${BUMP_TO%%+*}"* ]] || die "node runs $v after the bump to $BUMP_TO"
  ok "k3s bump applied: $v"
fi

if [[ -z "${SKIP_REBOOT:-}" ]]; then
  say "reboot (docker restart stands in for a VM reboot)"
  docker restart "k3s-e2e-$E2E_PROFILE" >/dev/null
  sleep 20
wait_for 300 hk wait --for=condition=Ready node --all --timeout=10s || die "node not Ready after reboot"
  wait_for 600 bash -c "$(declare -f hk); E2E_HOST='$E2E_HOST'; hk -n $NS wait --for=condition=Ready pod -l app.kubernetes.io/part-of=private-channel --timeout=10s" \
    || die "app pods not Ready after reboot"
  onhost "nft list table inet pc_k3s_guard" | tee -a "$LOG" | grep -q 8472 || die "guard table (with the VXLAN drop) missing after reboot"
  ctl "nc -z -w 3 $E2E_HOST_IP 6443" && die "6443 open after reboot"
  wait_for 300 bash -c "$E2E_CTL bash -lc 'nc -z -w 3 $E2E_HOST_IP 8899'" || die "8899 closed after reboot"
  playbook "$STEP-health-after-reboot" k3s.yml -e reset_state=false --tags health || die "health after reboot failed"
  ok "reboot: pods, CNPG, ServiceLB and the firewall rule came back; health passes"
  finding "APPROXIMATED: step 1 reboot is docker restart of the host container (no kernel reboot)."
fi
pass
