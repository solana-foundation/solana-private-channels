#!/usr/bin/env bash
# Step 5: no secret value and no deployer token outside pc-* Secret data. Counts matches
# per surface (never prints a value). /proc/*/environ of app processes is excluded:
# with-secrets exports values into the process env by design.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) no-leak sweep"
sync_repo
vals="$RUN/e2e-$E2E_PROFILE-leakvals.txt"
( umask 077
  sed -E 's/^[a-z_]+: //' "$RUN/e2e-$E2E_PROFILE-secrets.yml" | awk 'length($0) >= 8' | grep -v '^unused$'
  # The memory signer keys as the containers get them (base58).
  for k in admin withdraw-operator; do
    ctl "python3 -c 'import json,sys; A=b\"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz\"; b=bytes(json.load(open(sys.argv[1]))); n=int.from_bytes(b,\"big\"); s=b\"\"
while n: n,r=divmod(n,58); s=A[r:r+1]+s
print(s.decode())' $CTL_REPO/keypairs/$k.json"
  done ) >"$vals"

# A verbose deploy whose token we capture mid-run, so it is in the sweep too.
say "deploy at -vvv --diff"
ctl "cd $CTL_REPO/private-channel-deploy && ANSIBLE_LOG_PATH=/work/run/logs/$STEP-vvv.ansible.log ansible-playbook -vvv --diff \
  -l $E2E_INV_HOST -e @e2e-vars.yml k3s.yml -e reset_state=false >/work/run/logs/$STEP-vvv.log 2>&1 & echo \$! >/tmp/$STEP.pid"
token=""
for _ in $(seq 1 300); do
  token="$(ctl "cat /tmp/pc-k3s-deploy-$E2E_INV_HOST.*/kubeconfig 2>/dev/null | awk '/token:/ {print \$2}'" || true)"
  [[ -n "$token" ]] && break
  sleep 2
done
[[ -n "$token" ]] || die "could not capture the deploy token"
echo "$token" >>"$vals"
ps_ctl="$(ctl "ps -eo args")"; ps_host="$(onhost "ps -eo args")"
# The play's own log, not the pid: the pid can linger as a zombie on the control node.
ctl "until grep -q 'PLAY RECAP' /work/run/logs/$STEP-vvv.log; do sleep 3; done"
grep -q 'failed=0' "$RUN/logs/$STEP-vvv.log" || die "the -vvv deploy failed (logs/$STEP-vvv.log)"
ps_ctl+="$(ctl "ps -eo args")"; ps_host+="$(onhost "ps -eo args")"

D="$RUN/logs/$STEP-surfaces"; rm -rf "$D"; mkdir -p "$D"
for ns in $NS pc-monitoring cnpg-system cert-manager; do
  hk -n "$ns" get pods,deploy,sts,jobs,cm,events,svc,networkpolicy -o yaml >"$D/objects-$ns.yaml" 2>/dev/null || true
  for p in $(hk -n "$ns" get pods -o name); do
    hk -n "$ns" logs "$p" --all-containers --prefix --tail=-1 >>"$D/logs-$ns.txt" 2>/dev/null || true
  done
done
for r in private-channel pc-db; do
  onhost "KUBECONFIG=/etc/rancher/k3s/k3s.yaml; export KUBECONFIG; for v in \$(helm -n $NS history $r -o json | jq -r '.[].revision'); do
    helm -n $NS get values $r --all --revision \$v; helm -n $NS get manifest $r --revision \$v; helm -n $NS get hooks $r --revision \$v; done" >"$D/helm-$r.txt"
done
# The release records themselves (base64 + gzip JSON inside sh.helm.release.v1 Secrets).
onhost "k3s kubectl -n $NS get secret -l owner=helm -o json | jq -r '.items[].data.release' | while read -r d; do echo \"\$d\" | base64 -d | base64 -d | gunzip; echo; done" >"$D/helm-release-records.json"
onhost "journalctl -u k3s --no-pager" >"$D/journal-k3s.txt"
onhost "cat /var/lib/rancher/k3s/server/db/state.db*" >"$D/state.db.raw" 2>/dev/null
cp "$RUN/logs/$STEP-vvv.log" "$RUN/logs/$STEP-vvv.ansible.log" "$D/" 2>/dev/null || true
echo "$ps_ctl" >"$D/ps-ctl.txt"; echo "$ps_host" >"$D/ps-host.txt"
ctl "ls -la /tmp ~/.cache/pc-k3s 2>/dev/null; cat /tmp/pc-k3s-* 2>/dev/null; find /tmp -maxdepth 2 -name 'pc-*' 2>/dev/null" >"$D/ctl-tmp.txt" 2>&1

total=0
for f in "$D"/*; do
  n=$(grep -a -c -F -f "$vals" "$f" || true)
  say "surface $(basename "$f"): $n matching lines"
  total=$((total + n))
done
(( total == 0 )) || die "$total lines with a secret value or the deploy token (see per-surface counts)"
ok "0 matches across $(ls "$D" | wc -l) surfaces"
rm -f "$D/state.db.raw"
pass
