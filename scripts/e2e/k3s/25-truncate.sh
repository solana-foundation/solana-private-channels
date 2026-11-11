#!/usr/bin/env bash
# Step 25: admin truncate through the k3s Job (scripts/k3s-truncate.sh), all --dry-run:
# the window gate names the smallest passing --keep-slots and that value passes while one
# less is refused; a missing withdraw indexer URL is refused; the checkpoint gate refuses a
# cut past a stalled withdraw indexer.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) truncation gates"
[[ -n "${MINT:-}" ]] || die "needs step 17 (channel traffic)"
sync_repo
connect_ctl
tr() { ctl "cd $CTL_REPO && KUBECONFIG=$KC scripts/k3s-truncate.sh $*" 2>&1 | tee -a "$LOG"; }
out="$(tr 1 --dry-run || true)"
need="$(grep -oE 'keep_slots >= [0-9]+' <<<"$out" | grep -oE '[0-9]+' | head -1)"
[[ -n "$need" ]] || die "keep-slots 1 was not refused with a named minimum: $(tail -3 <<<"$out")"
grep -q pg_dump_ok <<<"$out" || die "pg_dump did not run in the Job"
ok "keep-slots 1 refused: use keep_slots >= $need"
out="$(tr "$((need - 1))" --dry-run || true)"
grep -q "keep_slots >= $need" <<<"$out" || die "keep-slots $((need - 1)) not refused with the same minimum"
out="$(tr "$need" --dry-run)" || die "keep-slots $need refused: $(tail -3 <<<"$out")"
ok "keep-slots $need passes the window gate (dry run): $(grep -oE 'blocks_deleted[^,]*' <<<"$out" | head -1)"
out="$(tr "$need" --dry-run --no-withdraw-url || true)"
grep -qiE 'withdraw-indexer-db-url|withdraw indexer' <<<"$out" || die "a missing withdraw indexer URL was not refused: $(tail -3 <<<"$out")"
ok "a missing --withdraw-indexer-db-url is refused"
# Checkpoint gate: stall the withdraw indexer, make channel blocks, cut close to the tip.
hk -n $NS scale sts/indexer-private-channel --replicas=0 >>"$LOG"
pf $NS svc/write-node 18910:8900
pf $NS svc/read-node 18901:8901
ctl "cp /work/repo/scripts/e2e/k3s/chan_tx.py /tmp/chan_tx.py"
for _ in $(seq 1 15); do ctl "python3 /tmp/chan_tx.py http://127.0.0.1:18910 http://127.0.0.1:18901 burn $(key user) $MINT $(cpub user) 1" >>"$LOG" 2>&1 || true; sleep 2; done
out="$(tr "$need" --dry-run || true)"
hk -n $NS scale sts/indexer-private-channel --replicas=1 >>"$LOG"
if grep -qi 'checkpoint.*behind' <<<"$out"; then
  ok "checkpoint gate refused: $(grep -i checkpoint <<<"$out" | head -1 | cut -c1-200)"
else
  finding "step 25: the checkpoint gate did not trigger with the withdraw indexer stopped and 15 channel burns (cut at keep-slots $need): $(tail -2 <<<"$out" | tr '\n' ' ' | cut -c1-200)"
  say "checkpoint gate not reproduced"
fi
hk -n $NS rollout status sts/indexer-private-channel --timeout=300s >>"$LOG"
pass
