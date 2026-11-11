#!/usr/bin/env bash
# Step 4: the deployer token can do exactly what the deploy needs and nothing that
# escalates; after disconnect it is rejected; a killed run's token is revoked by the
# next connect; k3s-connect.sh gives the same scope.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) deployer RBAC"
sync_repo

# A token like a deploy's, via k3s-connect (its own owner label, connect-<random>).
out="$(ctl "cd $CTL_REPO && scripts/k3s-connect.sh $E2E_INV_HOST -e @$CTL_REPO/private-channel-deploy/e2e-vars.yml")" || die "k3s-connect.sh failed"
KC="$(sed -n 's/^export KUBECONFIG=//p' <<<"$out")"
SOCK="$(sed -n 's/.*ssh -S \([^ ]*\) -O exit.*/\1/p' <<<"$out")"
[[ -n "$KC" ]] || die "no kubeconfig from k3s-connect.sh"
k() { ctl "KUBECONFIG=$KC kubectl $*"; }
ok "k3s-connect.sh gave a kubeconfig"
# A second session must not revoke or close the first.
out2="$(ctl "cd $CTL_REPO && scripts/k3s-connect.sh $E2E_INV_HOST -e @$CTL_REPO/private-channel-deploy/e2e-vars.yml")" || die "second k3s-connect.sh failed"
KC2="$(sed -n 's/^export KUBECONFIG=//p' <<<"$out2")"; SOCK2="$(sed -n 's/.*ssh -S \([^ ]*\) -O exit.*/\1/p' <<<"$out2")"
k "-n $NS get cm -o name" >/dev/null || die "a second k3s-connect session revoked the first"
ctl "KUBECONFIG=$KC2 kubectl -n $NS get cm -o name" >/dev/null || die "second session has no access"
ctl "ssh -S $SOCK2 -O exit pc-k3s; rm -rf $(dirname "$KC2")" || true
ok "two k3s-connect sessions stay open side by side"

k "auth can-i --list -n $NS" >"$RUN/logs/$STEP-can-i-$NS.txt"
k "auth can-i --list -n pc-monitoring" >"$RUN/logs/$STEP-can-i-mon.txt"
k "auth can-i --list -n kube-system" >"$RUN/logs/$STEP-can-i-kube-system.txt"
# The namespaced grants must be the Role's; nothing beyond self-review in other namespaces.
grep -qE '^secrets +\[\] +\[\] +\[get list watch create update patch delete\]' "$RUN/logs/$STEP-can-i-$NS.txt" || die "Secret verbs in $NS differ"
if grep -E '^(secrets|pods|deployments.apps) ' "$RUN/logs/$STEP-can-i-kube-system.txt"; then die "rights in kube-system"; fi
ok "can-i --list matches the Roles (logs/$STEP-can-i-*.txt)"

refuse() {
  local what="$1"
  shift
  if k "$*" >>"$LOG" 2>&1; then die "allowed: $what"; fi
  ok "refused: $what"
}
refuse "create a ClusterRole" "create clusterrole pc-e2e-x --verb=get --resource=pods"
refuse "create a StorageClass" "apply -f - <<<'{\"apiVersion\":\"storage.k8s.io/v1\",\"kind\":\"StorageClass\",\"metadata\":{\"name\":\"pc-e2e-x\"},\"provisioner\":\"x\"}'"
refuse "create a namespace" "create namespace pc-e2e-x"
refuse "create a PV" "apply -f - <<<'{\"apiVersion\":\"v1\",\"kind\":\"PersistentVolume\",\"metadata\":{\"name\":\"pc-e2e-x\"},\"spec\":{\"capacity\":{\"storage\":\"1Gi\"},\"accessModes\":[\"ReadWriteOnce\"],\"hostPath\":{\"path\":\"/tmp/x\"}}}'"
refuse "get secrets -A" "get secrets -A"
refuse "a privileged pod in $NS" "-n $NS run pc-e2e-priv --image=busybox --restart=Never --overrides='{\"spec\":{\"containers\":[{\"name\":\"x\",\"image\":\"busybox\",\"securityContext\":{\"privileged\":true}}]}}'"
refuse "a hostPath pod in $NS" "-n $NS run pc-e2e-hp --image=busybox --restart=Never --overrides='{\"spec\":{\"volumes\":[{\"name\":\"h\",\"hostPath\":{\"path\":\"/\"}}],\"containers\":[{\"name\":\"x\",\"image\":\"busybox\",\"volumeMounts\":[{\"name\":\"h\",\"mountPath\":\"/h\"}]}]}}'"
for ns in $NS pc-monitoring; do
  refuse "a service-account-token Secret in $ns" "-n $ns apply -f - <<<'{\"apiVersion\":\"v1\",\"kind\":\"Secret\",\"type\":\"kubernetes.io/service-account-token\",\"metadata\":{\"name\":\"pc-e2e-sat\",\"annotations\":{\"kubernetes.io/service-account.name\":\"default\"}}}'"
done
refuse "a LoadBalancer Service other than gateway and auth" "-n $NS create service loadbalancer pc-e2e-lb --tcp=18080:18080"
refuse "a NodePort Service" "-n $NS create service nodeport pc-e2e-np --tcp=18081:18081"
refuse "a Secret in pc-monitoring" "-n pc-monitoring create secret generic pc-e2e-x --from-literal=a=b"
refuse "a pod in pc-monitoring" "-n pc-monitoring run pc-e2e-x --image=busybox --restart=Never"
refuse "a Deployment in pc-monitoring" "-n pc-monitoring create deployment pc-e2e-x --image=busybox"
refuse "exec in pc-monitoring" "-n pc-monitoring exec deploy/pc-kps-grafana -c grafana -- true"
k "-n $NS get secrets -o name" >/dev/null || die "deployer cannot list Secrets in $NS"
ok "deployer can list Secrets in $NS (needed for the secrets phase)"

# Revocation: delete the bound Secret, the token must be rejected at once.
hk -n $NS delete secret -l private-channel.io/token-session=true >>"$LOG"
wait_for 30 bash -c "! $E2E_CTL bash -lc 'KUBECONFIG=$KC kubectl -n $NS get cm' >/dev/null 2>&1" || die "token still valid after its Secret was deleted"
ok "token rejected once its per-run Secret is deleted"
ctl "ssh -S $SOCK -O exit pc-k3s; rm -rf $(dirname "$KC")" || true

# A deploy run killed hard: its per-run Secret and tunnel are left; the next connect cleans them.
say "kill -9 a deploy mid-run"
# By process group, never pkill -f: that pattern would also match this shell.
ctl "cd $CTL_REPO/private-channel-deploy && setsid ansible-playbook -l $E2E_INV_HOST -e @e2e-vars.yml k3s.yml -e reset_state=false \
  --tags health >/work/run/logs/$STEP-killed.log 2>&1 & pg=\$!; for _ in \$(seq 1 120); do grep -q 'Prove the token works' /work/run/logs/$STEP-killed.log && break; sleep 1; done; \
  kill -9 -- -\$pg; sleep 2"
left="$(hk -n $NS get secret -l private-channel.io/token-owner=deploy -o name)"
[[ -n "$left" ]] || die "the killed run left no per-run Secret (could not test)"
stale_kc="$(ctl "ls -d /tmp/pc-k3s-deploy-$E2E_INV_HOST.* 2>/dev/null | head -1")"
stale_token="$(ctl "grep 'token:' $stale_kc/kubeconfig | awk '{print \$2}'")"
say "killed run left $left and $stale_kc"
playbook "$STEP-next-run" k3s.yml -e reset_state=false --tags health || die "the run after the kill failed"
hk -n $NS get "${left#secret/}" -o name 2>/dev/null | grep -q . && die "stale per-run Secret survived the next connect"
ctl "test -e $stale_kc" && die "stale kubeconfig dir survived"
wait_for 20 bash -c "[[ \$($E2E_HOST bash -lc \"curl -sk -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer $stale_token' https://127.0.0.1:6443/api\") == 401 ]]" \
  || die "the killed run's token is still accepted"
ok "the next connect deleted the stale Secret and kubeconfig; the killed run's token gets 401"
# This host's deploy tunnel only: ad hoc k3s-connect sessions are meant to stay open.
leftover="$(ctl "pgrep -fa '[s]sh .*tunnel-deploy-$E2E_INV_HOST' || true")"
[[ -z "$leftover" ]] || die "a tunnel survived: $leftover"
ok "no tunnel or kubeconfig remains"
pass
