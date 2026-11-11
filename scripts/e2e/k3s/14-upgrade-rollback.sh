#!/usr/bin/env bash
# Step 14 (subset): A to B never runs two write-nodes, one auth-migrate per revision; helm
# rollback to A restores service; a redeploy replaces an indexer pod stuck on a bad revision;
# a pc-db change (shared_buffers) applies with write-node healthy after the switchover.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) upgrade, rollback, pc-db change"
inst_args=(-e reset_state=false -e validator_reset=false)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
primary() { hk -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o jsonpath='{.items[0].metadata.name}'; }
rows() { hk -n $NS exec "$(primary)" -c postgres -- psql -U postgres -d private_channel -tAc "select string_agg(relname||'='||n_live_tup, ',' order by relname) from pg_stat_user_tables"; }
# B: the same image under a second tag, so the pod template changes.
crane_cp="crane copy --insecure 172.30.77.5:5000/pc-e2e/private-channel-app:head 172.30.77.5:5000/pc-e2e/private-channel-app:head-b"
ctl "$crane_cp" >>"$LOG" 2>&1 || die "could not tag B"
rev_a="$(helm_revision)"; rows_a="$(rows)"
sync_repo
( for _ in $(seq 1 150); do $E2E_HOST k3s kubectl -n $NS get pods -l app.kubernetes.io/name=write-node --field-selector=status.phase=Running -o name | wc -l; sleep 2; done ) >"$RUN/logs/$STEP-writers.txt" &
mon=$!
playbook "$STEP-to-b" k3s.yml "${inst_args[@]}" -e image_tag=head-b || die "upgrade to B failed"
kill $mon 2>/dev/null || true
maxw="$(sort -n "$RUN/logs/$STEP-writers.txt" | tail -1)"
(( maxw <= 1 )) || die "$maxw write-node pods Running at once during the upgrade"
rev_b="$(helm_revision)"
am="$(hk -n $NS get job auth-migrate -o jsonpath='{.status.succeeded}')"
[[ "$am" == 1 ]] || die "auth-migrate did not run exactly once for revision $rev_b"
hook_fresh auth-migrate private-channel || die "auth-migrate Job predates revision $rev_b (the hook did not run on upgrade)"
ok "A ($rev_a) to B ($rev_b): one writer at a time, one auth-migrate run"
hk -n $NS logs deploy/write-node --previous 2>/dev/null | tail -5 >>"$LOG" || true
onhost "KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm -n $NS rollback private-channel $rev_a --wait --timeout 10m" >>"$LOG" 2>&1 || die "helm rollback failed"
hk -n $NS rollout status deploy/gateway --timeout=300s >>"$LOG" || die "gateway not back after rollback"
img="$(hk -n $NS get deploy write-node -o jsonpath='{.spec.template.spec.containers[0].image}')"
[[ "$img" == *:head ]] || die "rollback did not restore A ($img)"
ok "helm rollback to A restored service ($img)"
# A bad release leaves an indexer pod not Ready on its revision; the next deploy must replace it.
hk -n $NS set image statefulset/indexer-private-channel indexer-private-channel=172.30.77.5:5000/pc-e2e/private-channel-app:missing >>"$LOG"
wait_for 120 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS get pod indexer-private-channel-0 -o jsonpath='{.spec.containers[0].image}') == *:missing ]]" \
  || die "the bad revision never reached indexer-private-channel-0"
playbook "$STEP-after-bad" k3s.yml "${inst_args[@]}" -e image_tag=head-b || die "redeploy after a bad indexer revision failed"
[[ "$(hk -n $NS get pod indexer-private-channel-0 -o jsonpath='{.spec.containers[0].image}')" == *:head-b ]] \
  || die "indexer-private-channel-0 still on the bad revision"
ok "a redeploy replaced the indexer pod stuck on a bad revision"
say "row estimates before: $rows_a"; say "row estimates after:  $(rows)"
# pc-db parameter change: CNPG restarts the instances and switches the primary.
p0="$(primary)"
playbook "$STEP-pcdb" k3s.yml "${inst_args[@]}" -e k3s_pg_shared_buffers=192MB || die "pc-db change deploy failed"
wait_for 900 bash -c "[[ \"\$($E2E_HOST k3s kubectl -n $NS exec \$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db=ledger,cnpg.io/instanceRole=primary -o name) -c postgres -- psql -U postgres -tAc 'show shared_buffers')\" == 192MB ]]" \
  || die "shared_buffers not applied"
say "primary before $p0, after $(primary)"
hk -n $NS rollout status deploy/write-node --timeout=300s >>"$LOG" || die "write-node not healthy after the pc-db change"
ok "pc-db change applied (shared_buffers=192MB), write-node healthy"
pass
