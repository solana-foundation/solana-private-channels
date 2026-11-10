#!/usr/bin/env bash
# Step 18 (subset): kill -9 write-node (one writer); indexer replacement after the old pod
# is gone; ledger primary deleted under a write loop (promotion, Services follow, no
# acknowledged write lost); replica deleted (writes continue, it returns as sync).
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) writer lease, failover, replica"
primary() { hk -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}'; }
replica() { hk -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=replica -o jsonpath='{.items[0].metadata.name}'; }
sql() { hk -n $NS exec "$(primary)" -c postgres -- psql -U postgres -d private_channel -tAc "$1"; }

# Writer lease.
old="$(hk -n $NS get pod -l app.kubernetes.io/name=write-node -o jsonpath='{.items[0].metadata.name}')"
hk -n $NS exec "$old" -c write-node -- sh -c 'kill -9 1' || true
running_max=0
for _ in $(seq 1 60); do
  n="$(hk -n $NS get pods -l app.kubernetes.io/name=write-node --field-selector=status.phase=Running -o name | wc -l)"
  (( n > running_max )) && running_max=$n
  sleep 2
done
hk -n $NS rollout status deploy/write-node --timeout=300s >>"$LOG" || die "write-node did not come back"
(( running_max <= 1 )) || die "$running_max write-node pods Running at once"
hk -n $NS logs deploy/write-node --tail=200 | grep -iE 'lease|writer' | tail -3 | tee -a "$LOG"
ok "kill -9 write-node: one writer at a time, replacement running"

# Indexer StatefulSet: the replacement waits for the old pod to be gone.
uid0="$(hk -n $NS get pod indexer-private-channel-0 -o jsonpath='{.metadata.uid}')"
hk -n $NS delete pod indexer-private-channel-0 --wait=false >>"$LOG"
overlap=0
for _ in $(seq 1 60); do
  n="$(hk -n $NS get pods -l app.kubernetes.io/name=indexer-private-channel -o name | wc -l)"
  (( n > 1 )) && overlap=1
  sleep 1
done
(( overlap == 0 )) || die "two indexer pods existed at once"
[[ "$(hk -n $NS get pod indexer-private-channel-0 -o jsonpath='{.metadata.uid}')" != "$uid0" ]] || die "indexer not replaced"
ok "indexer replacement started only after the old pod was gone"

# Primary failover under a write loop recorded on the client side.
sql "CREATE TABLE IF NOT EXISTS e2e_writes(id bigserial primary key, at timestamptz default now())" >/dev/null
pod0="$(primary)"
: >"$RUN/logs/$STEP-writes.txt"
( for _ in $(seq 1 400); do
    if v="$($E2E_HOST k3s kubectl -n $NS exec "$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}')" -c postgres -- \
              psql -U postgres -d private_channel -qtAc 'INSERT INTO e2e_writes DEFAULT VALUES RETURNING id' 2>/dev/null)"; then
      echo "$v" >>"$RUN/logs/$STEP-writes.txt"
    fi
    sleep 0.5
  done ) &
loop=$!
sleep 10
t0=$(date +%s)
say "deleting the ledger primary $pod0"
hk -n $NS delete pod "$pod0" --wait=false >>"$LOG"
until [[ -n "$(primary)" && "$(primary)" != "$pod0" ]]; do sleep 2; (( $(date +%s) - t0 < 400 )) || die "no promotion within 400s"; done
t1=$(date +%s)
say "promoted $(primary) after $((t1 - t0))s (failoverDelay 60)"
svc_ep="$(hk -n $NS get endpointslice -l kubernetes.io/service-name=postgres-primary -o jsonpath='{.items[0].endpoints[0].targetRef.name}')"
[[ "$svc_ep" == "$(primary)" ]] || die "postgres-primary points at $svc_ep, not $(primary)"
ok "postgres-primary follows the new primary"
wait $loop || true
recorded="$(grep -E '^[0-9]+$' "$RUN/logs/$STEP-writes.txt" | sort -n | tr '\n' ',' | sed 's/,$//')"
n_rec="$(grep -cE '^[0-9]+$' "$RUN/logs/$STEP-writes.txt")"
missing="$(sql "SELECT count(*) FROM unnest(ARRAY[$recorded]::bigint[]) r WHERE NOT EXISTS (SELECT 1 FROM e2e_writes w WHERE w.id = r)")"
[[ "$missing" == 0 ]] || die "$missing acknowledged writes lost in the failover"
ok "all $n_rec acknowledged writes present after failover"
hk -n $NS rollout status deploy/write-node --timeout=300s >>"$LOG" || die "write-node not healthy after failover"
hk -n $NS wait --for=condition=Ready pod -l app.kubernetes.io/name=read-node --timeout=300s >>"$LOG" || die "read-node not Ready"
hk -n $NS wait --for=condition=Ready cluster/pc-ledger --timeout=600s >>"$LOG" 2>&1 || true
ok "write-node and read-node healthy again; old primary rejoins as replica"

# Replica down under writes: preferred durability keeps accepting, then it returns as sync.
wait_for 600 bash -c "[[ -n \"\$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=replica -o name)\" ]]" || die "no replica back"
rp="$(replica)"
hk -n $NS delete pod "$rp" --wait=false >>"$LOG"
sleep 5
t="$(date +%s)"
sql "INSERT INTO e2e_writes DEFAULT VALUES" >/dev/null || die "a write blocked or failed with the replica down"
(( $(date +%s) - t < 10 )) || die "a write took >10s with the replica down"
ssn="$(sql "SHOW synchronous_standby_names")"
say "with the replica down: write ok, synchronous_standby_names='$ssn'"
wait_for 600 bash -c "$E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -tAc \"select sync_state from pg_stat_replication\" | grep -q '^sync$\|^quorum$'" \
  || die "replica did not return as synchronous"
ok "replica returned as synchronous standby"
pass
