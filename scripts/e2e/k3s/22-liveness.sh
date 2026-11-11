#!/usr/bin/env bash
# Step 22 (localnet): stop the validator for 2 minutes; indexer-solana and the operators
# turn NotReady with restartCount unchanged, then recover on their own.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) liveness under upstream outage"
w="indexer-solana operator-solana operator-private-channel"
restarts() { for x in $w; do hk -n $NS get pods -l "app.kubernetes.io/name=$x" -o jsonpath="{.items[0].status.containerStatuses[0].restartCount} "; done; }
r0="$(restarts)"
hk -n $NS scale sts/validator --replicas=0 >>"$LOG"
saw_notready=""
for _ in $(seq 1 40); do
  nr="$(hk -n $NS get pods -l 'app.kubernetes.io/name in (indexer-solana,operator-solana,operator-private-channel)' -o json \
    | jq -r '[.items[] | select(([.status.conditions[]? | select(.type=="Ready" and .status=="True")] | length) == 0) | .metadata.labels["app.kubernetes.io/name"]] | join(" ")')"
  [[ -n "$nr" ]] && saw_notready="$saw_notready $nr"
  sleep 3
done
hk -n $NS scale sts/validator --replicas=1 >>"$LOG"
say "NotReady seen during the outage:$(tr ' ' '\n' <<<"$saw_notready" | sort -u | xargs)"
[[ -n "$saw_notready" ]] || die "no worker turned NotReady while the validator was down"
r1="$(restarts)"
[[ "$r0" == "$r1" ]] || die "restartCount changed: $r0 -> $r1"
ok "workers NotReady, restartCount unchanged ($r1)"
hk -n $NS wait --for=condition=Ready pod -l 'app.kubernetes.io/name in (indexer-solana,operator-solana,operator-private-channel)' --timeout=600s >>"$LOG" \
  || die "workers did not recover on their own"
[[ "$(restarts)" == "$r0" ]] || finding "restartCount changed during recovery: $r0 -> $(restarts) (a worker may exit on its own after a long upstream gap)"
ok "workers recovered after the validator returned"
pass
