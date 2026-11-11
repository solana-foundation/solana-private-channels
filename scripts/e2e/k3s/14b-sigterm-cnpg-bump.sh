#!/usr/bin/env bash
# Step 14 remainder: deleting each worker's pod stops the binary with SIGTERM well
# within its grace period (no SIGKILL), then a CNPG chart bump (one patch, under a
# write loop) with no acknowledged write lost and write-node healthy after.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) SIGTERM and CNPG bump"
fails=0
for w in deploy/write-node sts/indexer-solana sts/indexer-private-channel deploy/operator-solana deploy/operator-private-channel deploy/gateway; do
  name="${w#*/}"
  p="$(hk -n $NS get pods -l "app.kubernetes.io/name=$name" -o jsonpath='{.items[0].metadata.name}')"
  pid1="$(hk -n $NS exec "$p" -c "$name" -- sh -c 'cat /proc/1/cmdline | tr "\0" " "')"
  # Follow the log through the stop, since the pod and its logs go away with it.
  hk -n $NS logs -f "$p" -c "$name" --tail=0 >"$RUN/logs/$STEP-$name-stop.log" 2>&1 &
  lf=$!
  t0=$(date +%s)
  hk -n $NS delete pod "$p" --wait=true --timeout=120s >>"$LOG" 2>&1
  t=$(( $(date +%s) - t0 ))
  kill $lf 2>/dev/null || true
  last="$( (grep -v DEBUG "$RUN/logs/$STEP-$name-stop.log" || true) | tail -3 | cut -c1-160 | tr '\n' '|')"
  say "$name: PID 1 was '$pid1'; deleted in ${t}s; last log: $last"
  # With a shared process namespace PID 1 is the pause container, never the binary.
  [[ "$pid1" == /pause* ]] || say "note: $name PID 1 is '$pid1' (no shared process namespace)"
  grace="$(hk -n $NS get "$w" -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
  (( t < grace )) || { say "WRONG $name took ${t}s >= grace ${grace}s (SIGKILL)"; fails=$((fails + 1)); }
done
(( fails == 0 )) && ok "every worker exits on SIGTERM before its grace period"

# CNPG chart bump under a write loop.
primary() { hk -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}'; }
hk -n $NS exec "$(primary)" -c postgres -- psql -U postgres -d private_channel -tAc "CREATE TABLE IF NOT EXISTS e2e_writes(id bigserial primary key, at timestamptz default now())" >/dev/null
: >"$RUN/logs/$STEP-writes.txt"
( for _ in $(seq 1 600); do
    v="$($E2E_HOST k3s kubectl -n $NS exec "$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}')" -c postgres -- \
      psql -U postgres -d private_channel -qtAc 'INSERT INTO e2e_writes DEFAULT VALUES RETURNING id' 2>/dev/null)" && echo "$v" >>"$RUN/logs/$STEP-writes.txt"
    sleep 0.5
  done ) &
loop=$!
op0="$(hk -n cnpg-system get deploy -l app.kubernetes.io/name=cloudnative-pg -o jsonpath='{.items[0].spec.template.spec.containers[0].image}')"
inst_args=(-e reset_state=false -e validator_reset=false -e k3s_cnpg_chart_version=0.29.1)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
sync_repo
playbook "$STEP-cnpg-bump" k3s.yml "${inst_args[@]}" || { kill $loop; die "deploy with the CNPG bump failed"; }
op1="$(hk -n cnpg-system get deploy -l app.kubernetes.io/name=cloudnative-pg -o jsonpath='{.items[0].spec.template.spec.containers[0].image}')"
say "CNPG operator image $op0 -> $op1"
[[ "$op0" != "$op1" ]] || { say "WRONG the CNPG operator did not change"; fails=$((fails + 1)); }
hk -n $NS wait --for=condition=Ready cluster/pc-ledger --timeout=600s >>"$LOG" 2>&1
sleep 30
kill $loop 2>/dev/null || true
wait $loop 2>/dev/null || true
ids="$(grep -E '^[0-9]+$' "$RUN/logs/$STEP-writes.txt" | sort -n | tr '\n' ',' | sed 's/,$//')"
n="$(grep -cE '^[0-9]+$' "$RUN/logs/$STEP-writes.txt")"
lost="$(hk -n $NS exec "$(primary)" -c postgres -- psql -U postgres -d private_channel -tAc "SELECT count(*) FROM unnest(ARRAY[$ids]::bigint[]) r WHERE NOT EXISTS (SELECT 1 FROM e2e_writes w WHERE w.id = r)")"
say "CNPG bump: $n acknowledged writes, $lost lost; instance images: $(hk -n $NS get pods -l private-channel.io/db=ledger -o jsonpath='{.items[*].spec.containers[0].image}')"
[[ "$lost" == 0 ]] || { say "WRONG $lost writes lost"; fails=$((fails + 1)); }
hk -n $NS rollout status deploy/write-node --timeout=300s >>"$LOG" || { say "WRONG write-node not healthy"; fails=$((fails + 1)); }
(( fails == 0 )) || die "$fails checks wrong"
pass
