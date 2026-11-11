#!/usr/bin/env bash
# Step 27 (localnet): down stops the app and hibernates Postgres, Clusters and PVCs stay,
# the next deploy resumes with data. A deleted pc-ledger PVC leaves a Released PV with its
# data dir. teardown refuses a wrong confirm and a non-e2e host, then leaves nothing.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) down and teardown"
sync_repo
inst_args=(-e reset_state=false -e validator_reset=false)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
marker="e2e-$(date +%s)"
pod() { hk -n $NS get pods -l "private-channel.io/db=$1,cnpg.io/instanceRole=primary" -o name | head -1; }
hk -n $NS exec "$(pod ledger)" -c postgres -- psql -U postgres -d private_channel -c "create table if not exists e2e_marker(v text); insert into e2e_marker values ('$marker')" >>"$LOG"
playbook "$STEP-down" down-k3s.yml || die "down failed"
[[ -z "$(hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o name)" ]] || die "app pods still running"
wait_for 300 bash -c "[[ -z \"\$($E2E_HOST k3s kubectl -n $NS get pods -l private-channel.io/db -o name)\" ]]" || die "Postgres pods still running"
hk -n $NS get cluster pc-ledger pc-indexer >>"$LOG" || die "Clusters gone after down"
[[ "$(hk -n $NS get pvc -l cnpg.io/cluster -o name | wc -l)" -ge 3 ]] || die "PVCs gone after down"
ok "down: app stopped, Postgres hibernated, Clusters and PVCs kept"
playbook "$STEP-resume" k3s.yml "${inst_args[@]}" || die "resume deploy failed"
got="$(hk -n $NS exec "$(pod ledger)" -c postgres -- psql -U postgres -d private_channel -tAc "select count(*) from e2e_marker where v='$marker'")"
[[ "$got" == 1 ]] || die "data lost across down"
ok "next deploy resumed with data"

# Retain: a deleted claim keeps its volume and directory.
playbook "$STEP-down2" down-k3s.yml || die "second down failed"
pvc="$(hk -n $NS get pvc -l cnpg.io/cluster=pc-ledger -o name | head -1)"
pv="$(hk -n $NS get "$pvc" -o jsonpath='{.spec.volumeName}')"
path="$(hk get pv "$pv" -o jsonpath='{.spec.hostPath.path}{.spec.local.path}')"
hk -n $NS delete "$pvc" --wait=false >>"$LOG"
wait_for 120 bash -c "[[ \"\$($E2E_HOST k3s kubectl get pv $pv -o jsonpath='{.status.phase}')\" == Released ]]" || die "PV $pv not Released"
onhost "test -d '$path' && ls '$path' | head -3" >>"$LOG" || die "data dir $path gone"
ok "deleted pc-ledger PVC: PV $pv Released, data dir kept"

if playbook "$STEP-td-wrong" teardown-k3s.yml -e confirm_teardown=not-this-host; then die "teardown with a wrong confirm passed"; fi
grep -q 'Confirm with -e confirm_teardown' "$RUN/logs/$STEP-td-wrong.log" || die "wrong-confirm refusal message"
if playbook "$STEP-td-nonE2E" teardown-k3s.yml -e "confirm_teardown=$E2E_INV_HOST" -e e2e_host=false; then die "teardown of a non-e2e host passed"; fi
grep -q 'is not an e2e host' "$RUN/logs/$STEP-td-nonE2E.log" || die "non-e2e refusal message"
hk -n $NS get cluster pc-ledger >/dev/null || die "a refused teardown deleted something"
ok "teardown refuses a wrong confirm and a non-e2e host"
playbook "$STEP-teardown" teardown-k3s.yml -e "confirm_teardown=$E2E_INV_HOST" || die "teardown failed"
for ns in $NS pc-monitoring cnpg-system cert-manager; do hk get ns "$ns" >/dev/null 2>&1 && die "namespace $ns left"; done
[[ -z "$(hk get pv -o name)" ]] || die "PVs left: $(hk get pv -o name | xargs)"
left="$(onhost "ls /var/lib/rancher/k3s/storage 2>/dev/null | wc -l")"
[[ "$left" == 0 ]] || die "$left local-path dirs left"
ok "teardown left no namespace, PV or data dir"
: >"$STATE"
pass
