#!/usr/bin/env bash
# Runs `admin truncate` on k3s as a Job: pg_dump 16 (postgres:16-bookworm) next to the CLI copied from
# the app image. DB URLs are built inside the pod from the CNPG superuser Secrets, never argv.
#   KUBECONFIG=... scripts/k3s-truncate.sh <keep-slots> [--dry-run] [--no-withdraw-url]
set -euo pipefail
keep="${1:?usage: k3s-truncate.sh <keep-slots> [--dry-run] [--no-withdraw-url]}"
shift
dry="" withdraw=1
for a in "$@"; do
  case "$a" in
    --dry-run) dry=--dry-run ;;
    --no-withdraw-url) withdraw=0 ;;
    *) echo "unknown flag $a" >&2; exit 2 ;;
  esac
done
ns="${K8S_NAMESPACE:-private-channel}"
img="$(kubectl -n "$ns" get deploy write-node -o jsonpath='{.spec.template.spec.containers[0].image}')"
pull="$(kubectl -n "$ns" get deploy write-node -o jsonpath='{.spec.template.spec.imagePullSecrets[0].name}')"
# Debian bookworm like the app image: the CLI needs glibc 2.34, the CNPG image is bullseye.
pg="${PG_IMAGE:-postgres:16-bookworm}"
job="pc-truncate-$(date +%s)"
pullyaml=""
[[ -n "$pull" ]] && pullyaml="imagePullSecrets: [ { name: $pull } ]"
# A real cut keeps its dump on a pc-retain claim, which outlives the Job and its TTL.
workvol="emptyDir: {}"
if [[ -z "$dry" ]]; then
  kubectl -n "$ns" apply -f - >/dev/null <<Y
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: $job, labels: { app.kubernetes.io/name: pc-truncate } }
spec: { accessModes: [ ReadWriteOnce ], storageClassName: pc-retain, resources: { requests: { storage: 10Gi } } }
Y
  workvol="persistentVolumeClaim: { claimName: $job }"
fi
kubectl -n "$ns" apply -f - >/dev/null <<Y
apiVersion: batch/v1
kind: Job
metadata: { name: $job, labels: { app.kubernetes.io/name: pc-truncate } }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata: { labels: { app.kubernetes.io/name: pc-truncate } }
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext: { runAsNonRoot: true, runAsUser: 999, runAsGroup: 999 }
      $pullyaml
      initContainers:
      - name: copy-admin
        image: $img
        args: [ /bin/sh, -c, "cp /usr/local/bin/admin /tools/admin" ]
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: [ ALL ] } }
        volumeMounts: [ { name: tools, mountPath: /tools } ]
      containers:
      - name: truncate
        image: $pg
        command: [ /bin/bash, -c ]
        args:
        - |
          set -euo pipefail
          enc() { local s="\$1" o="" c i; for ((i = 0; i < \${#s}; i++)); do c="\${s:i:1}"
            case "\$c" in [A-Za-z0-9._~-]) o+="\$c" ;; *) printf -v c '%%%02X' "'\$c"; o+="\$c" ;; esac; done; echo "\$o"; }
          PGPASSWORD="\$LEDGER_PW" pg_dump -Fc -h postgres-primary -U postgres -d private_channel -f /work/core.dump
          echo pg_dump_ok
          export PRIVATE_CHANNEL_ACCOUNTSDB_CONNECTION_URL="postgres://postgres:\$(enc "\$LEDGER_PW")@postgres-primary:5432/private_channel"
          if [ "$withdraw" = 1 ]; then
            export PRIVATE_CHANNEL_WITHDRAW_INDEXER_DB_URL="postgres://postgres:\$(enc "\$INDEXER_PW")@postgres-indexer:5432/indexer"
          fi
          unset LEDGER_PW INDEXER_PW
          /tools/admin truncate --keep-slots $keep --pg-dump-path /work/core.dump $dry
        env:
        - name: LEDGER_PW
          valueFrom: { secretKeyRef: { name: pc-db-superuser-ledger, key: password } }
        - name: INDEXER_PW
          valueFrom: { secretKeyRef: { name: pc-db-superuser-indexer, key: password } }
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: [ ALL ] } }
        volumeMounts: [ { name: tools, mountPath: /tools }, { name: work, mountPath: /work } ]
      volumes: [ { name: tools, emptyDir: {} }, { name: work, $workvol } ]
Y
# Poll: a refused cut fails the Job, and kubectl wait cannot wait for either outcome.
for _ in $(seq 1 240); do
  st="$(kubectl -n "$ns" get job "$job" -o jsonpath='{.status.succeeded}{.status.failed}')"
  [[ -n "$st" ]] && break
  sleep 5
done
kubectl -n "$ns" logs "job/$job" -c truncate 2>&1 | sed -E 's#postgres://[^ "]*#<db-url>#g'
[[ -z "$dry" ]] && echo "dump kept on PVC $job (core.dump); copy it off-host, then delete the PVC"
[[ "$(kubectl -n "$ns" get job "$job" -o jsonpath='{.status.succeeded}')" == 1 ]]
