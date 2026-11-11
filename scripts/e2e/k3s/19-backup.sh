#!/usr/bin/env bash
# Step 19 (subset, localnet): an S3 server in its own namespace as the bucket; with backups on,
# both Clusters archive WAL through the Barman plugin (ContinuousArchiving), an
# on-demand Backup completes, the 6h ScheduledBackups exist, objects land in the bucket.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) backups (S3 test server)"
ak="e2e$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"; sk="e2e$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
# SeaweedFS stands in for the bucket (MinIO images are no longer anonymously pullable).
# Without an identities config its S3 gateway accepts any key pair.
ak="e2e$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"; sk="e2e$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
hk delete ns pc-e2e-minio --ignore-not-found --wait >>"$LOG" 2>&1
if ! hk get ns pc-e2e-s3 >/dev/null 2>&1; then
  hk create ns pc-e2e-s3 >>"$LOG"
  hk -n pc-e2e-s3 apply -f - >>"$LOG" <<'Y'
apiVersion: apps/v1
kind: Deployment
metadata: { name: s3 }
spec:
  selector: { matchLabels: { app: s3 } }
  template:
    metadata: { labels: { app: s3 } }
    spec:
      containers:
      - name: s3
        image: chrislusf/seaweedfs:4.53
        args: [ server, -dir=/data, -s3, -s3.port=9000 ]
        ports: [ { containerPort: 9000 } ]
        readinessProbe: { tcpSocket: { port: 9000 }, periodSeconds: 5 }
        volumeMounts: [ { name: d, mountPath: /data } ]
      volumes: [ { name: d, emptyDir: {} } ]
---
apiVersion: v1
kind: Service
metadata: { name: s3 }
spec: { selector: { app: s3 }, ports: [ { port: 9000 } ] }
Y
  hk -n pc-e2e-s3 rollout status deploy/s3 --timeout=300s >>"$LOG" || die "S3 server not ready"
  printf 'backup_s3_access_key_id: %s\nbackup_s3_secret_access_key: %s\n' "$ak" "$sk" >"$RUN/e2e-$E2E_PROFILE-secrets.rpc.yml"
  chmod 600 "$RUN/e2e-$E2E_PROFILE-secrets.rpc.yml"
fi
sleep 10
# Create only if absent, so a rerun of this step works.
hk -n pc-e2e-s3 exec deploy/s3 -- sh -c 'echo s3.bucket.list | weed shell | grep -qw pc-backups || echo "s3.bucket.create -name pc-backups" | weed shell' >>"$LOG" 2>&1 || die "bucket"
sync_repo
inst_args=(-e reset_state=false -e validator_reset=false -e k3s_backup_enabled=true
  -e k3s_backup_destination_path=s3://pc-backups/local -e k3s_backup_endpoint_url=http://s3.pc-e2e-s3:9000)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
playbook "$STEP-on" k3s.yml "${inst_args[@]}" || die "deploy with backups on failed"
for c in pc-ledger pc-indexer; do
  wait_for 600 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS get cluster $c -o jsonpath='{.status.conditions[?(@.type==\"ContinuousArchiving\")].status}') == True ]]" \
    || die "$c: ContinuousArchiving not True"
  sn="$(hk -n $NS get cluster $c -o jsonpath='{.spec.plugins[0].parameters.serverName}')"
  say "$c archives under serverName $sn"
  hk -n $NS get scheduledbackup "$c-every-6h" -o jsonpath='{.spec.schedule}' | grep -q '0 0 \*/6' || die "$c: no 6h ScheduledBackup"
  hk -n $NS apply -f - >>"$LOG" <<Y
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: { name: $c-e2e-ondemand }
spec: { cluster: { name: $c }, method: plugin, pluginConfiguration: { name: barman-cloud.cloudnative-pg.io } }
Y
  wait_for 600 bash -c "[[ \$($E2E_HOST k3s kubectl -n $NS get backup $c-e2e-ondemand -o jsonpath='{.status.phase}') == completed ]]" \
    || die "$c: on-demand Backup did not complete ($(hk -n $NS get backup "$c-e2e-ondemand" -o jsonpath='{.status.phase} {.status.error}'))"
  ok "$c: WAL archiving on, on-demand backup completed"
done
objs="$(hk -n pc-e2e-s3 exec deploy/s3 -- sh -c 'echo "fs.ls -l /buckets/pc-backups/local" | weed shell' 2>/dev/null | grep -c . || true)"
(( objs > 0 )) || die "no objects in the bucket"
ok "$objs objects in the bucket"
# Same serverName across a pc-db upgrade (a retention change forces one): the chart reuses the live Cluster's.
s1="$(hk -n $NS get cluster pc-ledger -o jsonpath='{.spec.plugins[0].parameters.serverName}')"
r1="$(helm_revision pc-db)"
playbook "$STEP-again" k3s.yml "${inst_args[@]}" -e k3s_backup_retention=6d || die "second deploy failed"
[[ "$(helm_revision pc-db)" -gt "$r1" ]] || die "pc-db was not upgraded, so the serverName lookup went untested"
[[ "$(hk -n $NS get cluster pc-ledger -o jsonpath='{.spec.plugins[0].parameters.serverName}')" == "$s1" ]] || die "serverName changed across a redeploy"
ok "serverName kept across a pc-db upgrade ($s1)"
finding "Step 19 ran against a SeaweedFS S3 server only: PITR recovery, the reused-serverName refusal, SSE on a real bucket and the timed drill were not run."
pass
