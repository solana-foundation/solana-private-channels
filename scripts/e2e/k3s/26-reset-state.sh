#!/usr/bin/env bash
# Step 26. localnet: reset_state wipes validator, indexer, ledger, Prometheus and Grafana
# in lockstep; no MismatchExceedsThreshold; no old PV or local-path dir is left.
# devnet: reset_state=true is refused before connect and nothing is deleted.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) reset_state"
sync_repo
if [[ $E2E_PROFILE != local ]]; then
  before="$(hk get pv -o name | sort)"
  if playbook "$STEP-refused" k3s.yml -e reset_state=true; then die "reset_state passed off localnet"; fi
  grep -q 'reset_state=true is refused' "$RUN/logs/$STEP-refused.log" || die "refused for another reason"
  grep -q 'Delete per-run Secrets' "$RUN/logs/$STEP-refused.log" && die "refused only after connect"
  [[ "$(hk get pv -o name | sort)" == "$before" ]] || die "volumes changed"
  ok "reset_state refused before connect on $E2E_PROFILE; nothing deleted"
  pass; exit 0
fi
old_pvs="$(hk get pv -o jsonpath='{range .items[*]}{.metadata.name} {.spec.hostPath.path}{.spec.local.path}{"\n"}{end}')"
old_uid="$(hk -n $NS get cluster pc-ledger -o jsonpath='{.metadata.uid}')"
say "volumes before: $(wc -l <<<"$old_pvs")"
playbook "$STEP-reset" k3s.yml -e reset_state=true || die "reset deploy failed"
new_uid="$(hk -n $NS get cluster pc-ledger -o jsonpath='{.metadata.uid}')"
[[ "$new_uid" != "$old_uid" ]] || die "pc-ledger was not recreated"
while read -r pv path; do
  [[ -z "$pv" ]] && continue
  hk get pv "$pv" >/dev/null 2>&1 && die "old PV $pv left"
  [[ -n "$path" ]] && onhost "test -e '$path'" && die "old data dir $path left"
done <<<"$old_pvs"
ok "every old PV and local-path dir is gone; Clusters recreated"
n="$(hk -n $NS exec "$(hk -n $NS get pods -l private-channel.io/db=indexer,cnpg.io/instanceRole=primary -o name)" -c postgres -- \
  psql -U postgres -d indexer -tAc "select count(*) from transactions" 2>/dev/null || echo 0)"
[[ "${n:-0}" == 0 ]] || die "indexer DB not empty after reset ($n rows)"
sleep 30
hk -n $NS logs sts/indexer-private-channel --tail=-1 2>/dev/null | grep -q MismatchExceedsThreshold && die "MismatchExceedsThreshold after reset"
ok "indexer DB empty, no MismatchExceedsThreshold"
: >"$STATE"
pass
