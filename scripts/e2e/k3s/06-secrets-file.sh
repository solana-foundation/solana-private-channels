#!/usr/bin/env bash
# Step 6: file backend, exact set. A removed key disappears from the Secret and the pod;
# an empty value means no key and no KEY_FILE. /run/secrets files are 0440, group 1000,
# and another uid cannot read them.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) file-backend secrets"
x="$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
keys() { hk -n $NS get secret "pc-$1" -o json | jq -r '.data | keys[]' | sort | tr '\n' ' '; }
podenv() { hk -n $NS exec "deploy/$1" -c "$1" -- sh -c 'env | cut -d= -f1 | sort | tr "\n" " "'; }

# Add an optional key (the gateway's whitelist RPC), then clear it again.
echo "gateway_whitelist_rpc_url: 'https://whitelist-e2e.invalid/x'" >"$x"
sync_repo
playbook "$STEP-add" k3s.yml -e reset_state=false -e gateway_whitelist_instance=7HgtQ4VcSZJDtzUBQbjqMWcSr5dauigbqRFqpQMK8pwz || die "deploy with the key failed"
[[ " $(keys gateway) " == *" GATEWAY_WHITELIST_RPC_URL "* ]] || die "key not in pc-gateway"
wait_for 120 bash -c "$(declare -f hk); E2E_HOST='$E2E_HOST'; hk -n $NS get deploy gateway -o json | jq -e '.spec.template.spec.containers[0].env[] | select(.name==\"GATEWAY_WHITELIST_RPC_URL_FILE\")'" || die "no KEY_FILE"
ok "added key is in the Secret and the pod (KEY_FILE)"
rm -f "$x"
sync_repo
playbook "$STEP-remove" k3s.yml -e reset_state=false || die "deploy without the key failed"
[[ " $(keys gateway) " != *" GATEWAY_WHITELIST_RPC_URL "* ]] || die "removed key still in pc-gateway"
hk -n $NS rollout status deploy/gateway --timeout=120s >>"$LOG"
e="$(podenv gateway)"
[[ " $e " != *" GATEWAY_WHITELIST_RPC_URL_FILE "* && " $e " != *" GATEWAY_WHITELIST_RPC_URL "* ]] || die "removed key still in the pod env"
hk -n $NS exec deploy/gateway -c gateway -- test ! -e /run/secrets/GATEWAY_WHITELIST_RPC_URL || die "file still mounted"
ok "removed key gone from Secret, env and /run/secrets; empty value renders no key"

for svc in write-node gateway; do
  st="$(hk -n $NS exec "deploy/$svc" -c "$svc" -- sh -c 'for f in /run/secrets/*; do stat -L -c "%a %g %n" "$f"; done')"
  say "$svc: $st"
  while read -r mode grp _; do
    [[ "$mode" == 440 && "$grp" == 1000 ]] || die "$svc: secret file mode $mode group $grp"
  done <<<"$st"
done
# Another uid on the same pod cannot read them (an ephemeral debug container as uid 2000).
f="$(hk -n $NS exec deploy/write-node -c write-node -- sh -c 'ls /run/secrets | head -1')"
pod="$(hk -n $NS get pod -l app.kubernetes.io/name=write-node -o jsonpath='{.items[0].metadata.name}')"
onhost "echo '{\"securityContext\":{\"runAsUser\":2000,\"runAsGroup\":2000}}' > /root/pc-e2e-reader.json"
hk -n $NS debug "$pod" --profile=restricted --image="$(hk -n $NS get pod "$pod" -o jsonpath='{.spec.containers[0].image}')" \
  --target=write-node -c pc-e2e-reader --custom=/root/pc-e2e-reader.json \
  -- sh -c "id -u; cat /proc/1/root/run/secrets/$f >/dev/null 2>&1 && echo READ || echo DENIED; sleep 5" >>"$LOG" 2>&1 || die "debug container failed"
wait_for 60 bash -c "$E2E_HOST k3s kubectl -n $NS logs $pod -c pc-e2e-reader | grep -qE 'READ|DENIED'" || die "reader produced no result"
r="$(hk -n $NS logs "$pod" -c pc-e2e-reader)"
say "uid 2000 reading $f: $(tr '\n' ' ' <<<"$r")"
grep -q '^2000$' <<<"$r" || die "reader did not run as uid 2000"
grep -qx DENIED <<<"$r" || die "uid 2000 read a secret file"
ok "files are 0440 group 1000, unreadable to another uid"
pass
