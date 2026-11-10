#!/usr/bin/env bash
# Step 21 (localnet, short tick override): over-issue on the channel as the channel admin;
# after the confirm ticks the halt row reads halted=t insolvency=t, the /recon webhook
# arrives, the Grafana fence alert reaches the receiver and both operators turn NotReady.
# Recovery: burn the excess, clear the flag, rollout restart: the fence goes 0 to 1.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) reconciliation halt"
[[ -n "${INSTANCE:-}" && -n "${MINT:-}" ]] || die "needs steps 2 and 17 (instance and mint)"
hk create ns pc-e2e-hook --dry-run=client -o yaml | hk apply -f - >/dev/null
hk apply -f - <"$(dirname "$0")/hook_receiver.yaml" >>"$LOG"
hk -n pc-e2e-hook rollout status deploy/hook --timeout=300s >>"$LOG" || die "receiver not up"
HOOK=http://hook.pc-e2e-hook:8080
printf "notify_webhook: '%s/recon'\nalert_webhook_url: '%s/grafana'\n" "$HOOK" "$HOOK" >"$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
sync_repo
playbook "$STEP-hooks" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE" || die "deploy with the receiver failed"
# Short tick: test-only, reverted by the closing redeploy.
for o in operator-solana operator-private-channel; do hk -n $NS set env "deploy/$o" OPERATOR_RECONCILIATION_INTERVAL_SECS=20 >>"$LOG"; done
hk -n $NS rollout status deploy/operator-solana --timeout=300s >>"$LOG"
hk -n $NS rollout status deploy/operator-private-channel --timeout=300s >>"$LOG"
connect_ctl
pf $NS svc/write-node 18910:8900
pf $NS svc/read-node 18901:8901
ctl "cp /work/repo/scripts/e2e/k3s/chan_tx.py /tmp/chan_tx.py"
over="$(ctl "python3 /tmp/chan_tx.py http://127.0.0.1:18910 http://127.0.0.1:18901 mint $(key admin) $MINT $(cpub user) 10000")" || die "over-issue failed: $over"
sig over_issue "$over"
idx() { hk -n $NS exec "$(hk -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name)" -c postgres -- psql -U postgres -d indexer -tAc "$1"; }
wait_for 600 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -d indexer -tAc 'select halted::text from reconciliation_halt') == true ]]" \
  || die "no halt within 10 min"
row="$(idx "select halted, insolvency, left(reason, 160) from reconciliation_halt")"
say "halt row: $row"
[[ "$row" == t\|t\|* ]] || die "halt row is not halted=t insolvency=t"
ok "halt row halted=t insolvency=t"
wait_for 180 bash -c "$E2E_HOST k3s kubectl -n pc-e2e-hook logs deploy/hook | grep -q 'POST /recon'" || die "no /recon webhook"
ok "/recon webhook arrived: $(hk -n pc-e2e-hook logs deploy/hook | grep 'POST /recon' | head -1 | cut -c1-160)"
nr=""
for o in operator-solana operator-private-channel; do
  wait_for 120 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS get pods -l app.kubernetes.io/name=$o -o jsonpath='{.items[0].status.conditions[?(@.type==\"Ready\")].status}') == False ]]" && nr+="$o "
done
[[ "$nr" == "operator-solana operator-private-channel " ]] || die "operators NotReady: '$nr'"
ok "both operators NotReady (503)"
if wait_for 300 bash -c "$E2E_HOST k3s kubectl -n pc-e2e-hook logs deploy/hook | grep 'POST /grafana' | grep -q 'solvency fence tripped'"; then
  ok "Grafana 'Escrow solvency fence tripped' alert reached the receiver"
else
  finding "step 21: the Grafana fence alert did not reach the receiver within 5 min of the halt"
  say "WRONG no Grafana alert"; graf_fail=1
fi
burn="$(ctl "python3 /tmp/chan_tx.py http://127.0.0.1:18910 http://127.0.0.1:18901 burn $(key user) $MINT $(cpub user) 10000")" || die "burn failed: $burn"
sig burn_excess "$burn"
sleep 30
idx "UPDATE reconciliation_halt SET halted = FALSE, halted_at = NOW() WHERE id = TRUE" >>"$LOG"
hk -n $NS rollout restart deploy/operator-solana deploy/operator-private-channel >>"$LOG"
hk -n $NS rollout status deploy/operator-solana --timeout=300s >>"$LOG"
hk -n $NS rollout status deploy/operator-private-channel --timeout=300s >>"$LOG"
fence() { hk -n $NS exec deploy/operator-solana -c operator-solana -- curl -s localhost:9102/metrics | awk '/^private_channel_operator_solvency_fence_state\{.*escrow/ {print $2}'; }
wait_for 300 bash -c "$(declare -f fence hk); E2E_HOST='$E2E_HOST'; NS=$NS; [[ \$(fence) == 1 ]]" || die "fence did not open after recovery (now $(fence))"
ok "recovery: burned, cleared, restarted; fence 1, both operators Ready"
# helm skips an unchanged release, so a redeploy alone would keep the short tick.
for o in operator-solana operator-private-channel; do hk -n $NS set env "deploy/$o" OPERATOR_RECONCILIATION_INTERVAL_SECS- >>"$LOG"; done
rm -f "$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
sync_repo
playbook "$STEP-revert" k3s.yml -e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE" || die "closing redeploy failed"
[[ -z "${graf_fail:-}" ]] || die "Grafana alert missing (everything else passed)"
pass
