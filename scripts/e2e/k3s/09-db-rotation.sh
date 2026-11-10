#!/usr/bin/env bash
# Step 9: rotate-secret.sh then k3s.yml for each DB login and both CNPG superusers: the
# old password is refused and the new one logs in (psql over TCP in the CNPG pod), the
# replica keeps streaming, the grants hook completes, exporters and the Grafana
# datasource stay healthy. write-node's NotReady window per rotation is recorded.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) DB password rotation"
sec="$RUN/e2e-$E2E_PROFILE-secrets.yml"
inst_args=(-e reset_state=false -e validator_reset=false)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
pod() { hk -n $NS get pods -l "private-channel.io/db=$1,cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}'; }
# login <db-label> <role> <database> <password-file>: 0 if a password login over TCP works.
login() {
  hk -n $NS exec -i "$(pod "$1")" -c postgres -- sh -c \
    "PGPASSWORD=\$(cat) psql -h 127.0.0.1 -U $2 -d $3 -tAc 'select 1' >/dev/null 2>&1" <"$4"
}
fails=0
for spec in ledger:private_channel_runtime:private_channel:postgres_runtime_password \
            ledger:private_channel_auth_runtime:private_channel:postgres_auth_runtime_password \
            ledger:private_channel_auth_owner:private_channel:postgres_auth_owner_password \
            ledger:private_channel_gateway:private_channel:postgres_gateway_password \
            ledger:private_channel_monitoring:private_channel:postgres_monitoring_password \
            indexer:private_channel_grafana:indexer:postgres_grafana_password \
            indexer:private_channel_indexer:indexer:postgres_indexer_password \
            ledger:postgres:private_channel:postgres_password; do
  IFS=: read -r db role dbname var <<<"$spec"
  old="$(mktemp)"; new="$(mktemp)"
  e2e_secret "$var" >"$old"
  bash "$REPO/scripts/rotate-secret.sh" "$var" --secrets-file "$sec" >>"$LOG" 2>&1 || die "rotate-secret.sh $var failed"
  e2e_secret "$var" >"$new"
  cmp -s "$old" "$new" && die "rotate-secret.sh did not change $var"
  sync_repo
  ( for _ in $(seq 1 400); do
      r="$($E2E_HOST k3s kubectl -n $NS get pods -l app.kubernetes.io/name=write-node -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
      echo "$(date +%s) ${r:-none}"; sleep 1
    done ) >"$RUN/logs/$STEP-$var-ready.txt" &
  mon=$!
  t0=$(date +%s)
  playbook "$STEP-$var" k3s.yml "${inst_args[@]}" || { kill $mon; die "deploy after rotating $var failed"; }
  kill $mon 2>/dev/null || true
  down="$(awk -v t0="$t0" '$1 >= t0 && $2 != "True" {n++} END {print n+0}' "$RUN/logs/$STEP-$var-ready.txt")"
  if login "$db" "$role" "$dbname" "$new" && ! login "$db" "$role" "$dbname" "$old"; then
    say "ok $var: new password logs in as $role, the old one is refused; write-node NotReady ${down}s"
  else
    say "WRONG $var: login new=$(login "$db" "$role" "$dbname" "$new" && echo ok || echo refused) old=$(login "$db" "$role" "$dbname" "$old" && echo ok || echo refused)"
    fails=$((fails + 1))
  fi
  # The indexer cluster's superuser follows the indexer password; check it too.
  [[ $var == postgres_indexer_password ]] && { login indexer postgres indexer "$new" || { say "WRONG pc-indexer postgres superuser"; fails=$((fails + 1)); }; }
  rm -f "$old" "$new"
done
st="$(hk -n $NS exec "$(pod ledger)" -c postgres -- psql -U postgres -tAc "select state||'/'||sync_state from pg_stat_replication")"
[[ "$st" == streaming/* ]] || { say "WRONG replica: $st"; fails=$((fails + 1)); }
say "replica after the rotations: $st"
[[ "$(hk -n $NS get job pc-db-grants -o jsonpath='{.status.succeeded}')" == 1 ]] || { say "WRONG grants hook"; fails=$((fails + 1)); }
for x in postgres-exporter-primary postgres-exporter-indexer; do
  v="$(hk -n $NS exec "deploy/$x" -- wget -qO- localhost:9187/metrics 2>/dev/null | awk '/^pg_up / {print $2}')"
  [[ "$v" == 1 ]] || { say "WRONG $x pg_up=$v"; fails=$((fails + 1)); }
done
connect_ctl
pf pc-monitoring svc/pc-kps-grafana 13000:80
ds="$(ctl "curl -s -u 'admin:$(e2e_secret grafana_admin_password)' http://127.0.0.1:13000/api/datasources/uid/postgres-indexer/health")"
say "Grafana Postgres datasource health: $(jq -r .status <<<"$ds" 2>/dev/null)"
[[ "$(jq -r .status <<<"$ds")" == OK ]] || { say "WRONG datasource health"; fails=$((fails + 1)); }
(( fails == 0 )) || die "$fails rotation checks wrong"
pass
