#!/usr/bin/env bash
# Step 19 remainder (localnet, after 19): PITR per docs/PITR.md "PITR on k3s", timed per
# phase against the compose drill (112.6 s): recover both databases into new Clusters (the
# indexer target 70 s before the ledger target), the post-target row is gone, the role
# Services move, the app boots (fence and bitmap checks), archiving resumes under a new
# serverName. Then a Cluster reusing a used serverName is refused by Barman.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) PITR and timed drill"
S3=(-e k3s_backup_enabled=true -e k3s_backup_destination_path=s3://pc-backups/local -e k3s_backup_endpoint_url=http://s3.pc-e2e-s3:9000)
inst=(-e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE")
pod() { hk -n $NS get pods -l "private-channel.io/db=$1,cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}'; }
q() { hk -n $NS exec "$(pod "$1")" -c postgres -- psql -U postgres -d "$2" -tAc "$3"; }
sl="$(hk -n $NS get cluster pc-ledger -o jsonpath='{.spec.plugins[0].parameters.serverName}')"
si="$(hk -n $NS get cluster pc-indexer -o jsonpath='{.spec.plugins[0].parameters.serverName}')"
[[ -n "$sl" && -n "$si" ]] || die "backups are not on (run step 19 first)"
q ledger private_channel "CREATE TABLE IF NOT EXISTS e2e_pitr(v text, at timestamptz default now())" >/dev/null
q ledger private_channel "INSERT INTO e2e_pitr(v) VALUES ('before')" >/dev/null
for c in pc-ledger pc-indexer; do
  hk -n $NS apply -f - >>"$LOG" <<Y
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: { name: $c-pitr-base-$(date +%s) }
spec: { cluster: { name: $c }, method: plugin, pluginConfiguration: { name: barman-cloud.cloudnative-pg.io } }
Y
done
sleep 60
t_idx="$(date -u +%FT%TZ)"; sleep 70; t_led="$(date -u +%FT%TZ)"; sleep 5
q ledger private_channel "INSERT INTO e2e_pitr(v) VALUES ('after-target')" >/dev/null
q ledger private_channel "SELECT pg_switch_wal()" >/dev/null; q indexer indexer "SELECT pg_switch_wal()" >/dev/null
sleep 60
say "targets: indexer $t_idx, ledger $t_led; source serverNames $sl, $si"
sync_repo
T0=$(date +%s)
playbook "$STEP-down" down-k3s.yml || die "down failed"
T1=$(date +%s)
ov="{\"k3s_ledger_overrides\": {\"name\": \"pc-ledger-r1\", \"services\": false, \"recovery\": {\"serverName\": \"$sl\", \"targetTime\": \"$t_led\"}},
     \"k3s_indexer_overrides\": {\"name\": \"pc-indexer-r1\", \"services\": false, \"recovery\": {\"serverName\": \"$si\", \"targetTime\": \"$t_idx\"}}}"
echo "$ov" | ctl "cat > $CTL_REPO/private-channel-deploy/pitr-overrides.json"
playbook "$STEP-recover" k3s.yml --tags database -e k3s_stop_after=database "${S3[@]}" "${inst[@]}" -e @pitr-overrides.json \
  || die "recovery deploy failed"
T2=$(date +%s)
rq() { hk -n $NS exec "$1-1" -c postgres -- psql -U postgres -d "$2" -tAc "$3"; }
after="$(rq pc-ledger-r1 private_channel "select count(*) from e2e_pitr where v = 'after-target'")"
before="$(rq pc-ledger-r1 private_channel "select count(*) from e2e_pitr where v = 'before'")"
[[ "$after" == 0 && "$before" -ge 1 ]] || die "recovered ledger: before=$before after-target=$after"
ok "recovered ledger to $t_led: the pre-target row is there, the post-target row is gone"
hk -n $NS delete cluster pc-ledger pc-indexer --wait --timeout=5m >>"$LOG" || die "could not delete the old Clusters"
echo "$ov" | sed 's/"services": false/"services": true/g' | ctl "cat > $CTL_REPO/private-channel-deploy/pitr-overrides.json"
playbook "$STEP-services" k3s.yml --tags database -e k3s_stop_after=database "${S3[@]}" "${inst[@]}" -e @pitr-overrides.json \
  || die "moving the role Services failed"
ep="$(hk -n $NS get endpointslice -l kubernetes.io/service-name=postgres-primary -o jsonpath='{.items[0].endpoints[0].targetRef.name}')"
[[ "$ep" == pc-ledger-r1-* ]] || die "postgres-primary points at $ep"
ok "role Services moved to the recovered Clusters ($ep)"
T4=$(date +%s)
playbook "$STEP-app" k3s.yml "${S3[@]}" "${inst[@]}" -e @pitr-overrides.json || die "app deploy after the restore failed"
T5=$(date +%s)
for o in operator-solana operator-private-channel; do
  hk -n $NS logs "deploy/$o" --since=10m | grep -iE 'fence|bitmap' | grep -v DEBUG | tail -2 | cut -c1-200 | tee -a "$LOG"
done
hk -n $NS wait --for=condition=Ready pod -l 'app.kubernetes.io/part-of=private-channel,!job-name' --timeout=600s >>"$LOG" || die "app not Ready after the restore"
ok "app Ready on the restored databases"
wait_for 600 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS get cluster pc-ledger-r1 -o jsonpath='{.status.conditions[?(@.type==\"ContinuousArchiving\")].status}') == True ]]" \
  || die "archiving did not resume on pc-ledger-r1"
nsn="$(hk -n $NS get cluster pc-ledger-r1 -o jsonpath='{.spec.plugins[0].parameters.serverName}')"
[[ "$nsn" != "$sl" ]] || die "the recovered Cluster reuses the old serverName"
ok "archiving resumed under a new serverName ($nsn)"
say "drill timings: stop $((T1 - T0))s, recover $((T2 - T1))s, swap old to new $((T4 - T2))s, app $((T5 - T4))s, total $((T5 - T0))s (compose drill 112.6 s)"
finding "Step 19 PITR drill (k3s, localnet): stop $((T1 - T0)) s, recover both Clusters $((T2 - T1)) s, delete old and move Services $((T4 - T2)) s, app back $((T5 - T4)) s, total $((T5 - T0)) s (compose drill 112.6 s). The k3s total includes the full k3s.yml phases (health, monitoring, sanity)."
echo "$ov" | sed 's/"services": false/"services": true/g' | python3 -c 'import json,sys,yaml; print(yaml.safe_dump(json.load(sys.stdin)))' >"$RUN/e2e-$E2E_PROFILE-pitr.yml"
# The recovered Clusters reference the object store, so later deploys keep backups on.
printf 'k3s_backup_enabled: true\nk3s_backup_destination_path: s3://pc-backups/local\nk3s_backup_endpoint_url: http://s3.pc-e2e-s3:9000\n' >>"$RUN/e2e-$E2E_PROFILE-pitr.yml"
# A Cluster pointed at a used archive path must not start archiving into it.
hk -n $NS apply -f - >>"$LOG" <<Y
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: { name: pc-e2e-dup }
spec:
  instances: 1
  imageName: $(hk -n $NS get cluster pc-ledger-r1 -o jsonpath='{.spec.imageName}')
  storage: { storageClass: pc-retain, size: 1Gi }
  plugins:
  - { name: barman-cloud.cloudnative-pg.io, isWALArchiver: true, parameters: { barmanObjectName: pc-ledger-store, serverName: $nsn } }
Y
dup=""
for _ in $(seq 1 60); do
  dup="$(hk -n $NS get cluster pc-e2e-dup -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status} {.status.conditions[?(@.type=="ContinuousArchiving")].message}')"
  [[ "$dup" == False* || "$dup" == True* ]] && break
  sleep 10
done
say "duplicate serverName Cluster: $dup"
hk -n $NS delete cluster pc-e2e-dup --wait=false >>"$LOG"
[[ "$dup" == False* ]] || die "a Cluster reusing serverName $nsn was not refused ($dup)"
ok "a Cluster reusing an existing serverName is refused by Barman"
pass
