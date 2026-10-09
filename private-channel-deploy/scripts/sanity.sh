#!/usr/bin/env bash
# Passive sanity check after a Solana Private Channels deploy.
#
# Inspects healthchecks, Prometheus metrics, and recent logs without sending
# any new transactions. Each check prints a `==== CHECK N: <name> ====` banner
# followed by PASS / FAIL / SKIP lines explaining what was verified.
#
# Exit code: 0 when no FAIL, 1 otherwise. SKIPs are honest and do not fail —
# they record what couldn't be verified (typically: no traffic on a fresh deploy).
#
# For deeper, transaction-driven validation, see private-channel-deploy/SMOKE_TEST.md
# (parked but available for post-deploy testing).

set -uo pipefail

# ---------------------------------------------------------------------------
# Tunables (overridable from the Ansible play)
# ---------------------------------------------------------------------------
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9090}"
INDEXER_LAG_MAX="${INDEXER_LAG_MAX:-50}"            # slots
OPERATOR_BACKLOG_MAX="${OPERATOR_BACKLOG_MAX:-100}" # tx
GATEWAY_ERROR_RATIO_MAX="${GATEWAY_ERROR_RATIO_MAX:-0.05}"
RECONCILE_LOG_WINDOW="${RECONCILE_LOG_WINDOW:-10m}"
PIPELINE_OBSERVE_SECS="${PIPELINE_OBSERVE_SECS:-10}"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
pass=0; fail=0; skip=0
banner() { echo; echo "==== CHECK $1: $2 ===="; }
ok()     { echo "  PASS  $*"; pass=$((pass+1)); }
no()     { echo "  FAIL  $*"; fail=$((fail+1)); }
sk()     { echo "  SKIP  $*"; skip=$((skip+1)); }

# Query Prometheus instant value. Returns the first series' value, or empty if
# no series. Sums multiple series when summing makes sense (caller decides).
prom() {
  local query="$1"
  curl -fsS --get --data-urlencode "query=$query" "$PROMETHEUS_URL/api/v1/query" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    r = d.get('data', {}).get('result', [])
    if not r:
        sys.exit(0)
    print(r[0]['value'][1])
except Exception:
    sys.exit(0)
"
}

# Query Prometheus and return all (label_value, value) pairs separated by tab.
prom_by_label() {
  local query="$1" label="$2"
  curl -fsS --get --data-urlencode "query=$query" "$PROMETHEUS_URL/api/v1/query" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for r in d.get('data', {}).get('result', []):
        lbl = r['metric'].get('$label', '<none>')
        val = r['value'][1]
        print(f'{lbl}\t{val}')
except Exception:
    pass
"
}

# Float compare: returns 0 if $1 <= $2.
le() { python3 -c "import sys; sys.exit(0 if float('$1') <= float('$2') else 1)"; }
lt() { python3 -c "import sys; sys.exit(0 if float('$1') <  float('$2') else 1)"; }

# ---------------------------------------------------------------------------
# CHECK 1: HTTP /health on every Solana Private Channels service
# ---------------------------------------------------------------------------
banner 1 "service /health endpoints"

probe_http() {
  local name="$1" url="$2"
  if curl -fsS -m 5 "$url" >/dev/null 2>&1; then ok "$name → 200 ($url)"; else no "$name → unreachable ($url)"; fi
}

probe_http "gateway"    "${GATEWAY_URL}/health"
probe_http "write-node" "${WRITE_URL}/health"
probe_http "read-node"  "${READ_URL}/health"

# Indexers default to :9100; operators are given an explicit METRICS_PORT in
# docker-compose.yml (operator-solana=9102, operator-private-channel=9103, host
# loopback only). Read it from the container env so this stays correct if compose changes.
for svc in indexer-solana indexer-private-channel operator-solana operator-private-channel; do
  # A localnet first boot without an escrow instance does not deploy these three.
  if [[ "${SANITY_WORKERS_EXPECTED:-true}" == false && "$svc" != indexer-private-channel ]] \
    && ! docker inspect "private-channel-$svc" >/dev/null 2>&1; then
    sk "$svc not deployed (no escrow instance yet)"
    continue
  fi
  port=$(docker exec "private-channel-$svc" sh -c 'echo "${METRICS_PORT:-9100}"' 2>/dev/null || echo 9100)
  if docker exec "private-channel-$svc" curl -fsS -m 3 "http://localhost:${port}/health" >/dev/null 2>&1; then
    ok "$svc → 200 (in-container :${port}/health)"
  else
    no "$svc → /health not reachable on :${port}"
  fi
done

# Postgres pg_isready
for pg in postgres-primary postgres-indexer; do
  if docker exec "private-channel-$pg" pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; then
    ok "$pg → pg_isready OK"
  else
    no "$pg → pg_isready failed"
  fi
done

# ---------------------------------------------------------------------------
# CHECK 2: Indexer lag (chain_tip_slot - current_slot per program_type)
# ---------------------------------------------------------------------------
banner 2 "indexer lag (chain_tip − current_slot, threshold ≤ ${INDEXER_LAG_MAX})"

lag_lines=$(prom_by_label "private_channel_indexer_chain_tip_slot - private_channel_indexer_current_slot" "program_type")
if [ -z "$lag_lines" ]; then
  sk "no indexer lag series yet (metrics not populated; indexer may still be initializing)"
else
  while IFS=$'\t' read -r prog lag; do
    if le "$lag" "$INDEXER_LAG_MAX"; then
      ok "indexer ($prog) lag = $lag slots"
    else
      no "indexer ($prog) lag = $lag slots — exceeds threshold $INDEXER_LAG_MAX"
    fi
  done <<<"$lag_lines"
fi

# ---------------------------------------------------------------------------
# CHECK 3: Operator backlog depth (per program_type)
# ---------------------------------------------------------------------------
banner 3 "operator backlog depth (≤ ${OPERATOR_BACKLOG_MAX})"

backlog_lines=$(prom_by_label "private_channel_operator_backlog_depth" "program_type")
if [ -z "$backlog_lines" ]; then
  sk "no operator backlog series yet (operator may still be initializing)"
else
  while IFS=$'\t' read -r prog depth; do
    if le "$depth" "$OPERATOR_BACKLOG_MAX"; then
      ok "operator ($prog) backlog = $depth"
    else
      no "operator ($prog) backlog = $depth — exceeds threshold $OPERATOR_BACKLOG_MAX"
    fi
  done <<<"$backlog_lines"
fi

# ---------------------------------------------------------------------------
# CHECK 4: Yellowstone gRPC stability (snapshot, wait, re-snapshot)
# ---------------------------------------------------------------------------
banner 4 "Yellowstone gRPC stability (no reconnects in ${PIPELINE_OBSERVE_SECS}s)"

r1=$(prom "sum(private_channel_indexer_datasource_reconnects_total)")
sleep "$PIPELINE_OBSERVE_SECS"
r2=$(prom "sum(private_channel_indexer_datasource_reconnects_total)")

if [ -z "$r1" ] || [ -z "$r2" ]; then
  sk "reconnect counter not yet emitted (indexer may still be initializing)"
elif [ "$r1" = "$r2" ]; then
  ok "0 reconnects in ${PIPELINE_OBSERVE_SECS}s window (cumulative count = $r2)"
else
  diff=$(python3 -c "print(float('$r2') - float('$r1'))")
  no "$diff reconnect(s) in ${PIPELINE_OBSERVE_SECS}s — Yellowstone connection unstable"
fi

# ---------------------------------------------------------------------------
# CHECK 5: Gateway error ratio
# ---------------------------------------------------------------------------
banner 5 "gateway error ratio (< ${GATEWAY_ERROR_RATIO_MAX})"

reqs=$(prom "sum(private_channel_gateway_requests_total)")
errs=$(prom "sum(private_channel_gateway_errors_total)")

if [ -z "$reqs" ] || [ "${reqs%.*}" = "0" ]; then
  sk "no gateway requests yet (fresh deploy; cannot compute error ratio)"
elif [ -z "$errs" ] || [ "${errs%.*}" = "0" ]; then
  ok "$reqs requests, 0 errors"
else
  ratio=$(python3 -c "print(round(float('$errs')/float('$reqs'), 4))")
  if lt "$ratio" "$GATEWAY_ERROR_RATIO_MAX"; then
    ok "$errs / $reqs = $ratio (< $GATEWAY_ERROR_RATIO_MAX)"
  else
    no "$errs / $reqs = $ratio — exceeds threshold $GATEWAY_ERROR_RATIO_MAX"
  fi
fi

# ---------------------------------------------------------------------------
# CHECK 6: Pipeline counters incrementing (sample twice over a window)
# ---------------------------------------------------------------------------
banner 6 "pipeline movement (dedup_received_total over ${PIPELINE_OBSERVE_SECS}s)"

c1=$(prom "sum(private_channel_dedup_received_total)")
sleep "$PIPELINE_OBSERVE_SECS"
c2=$(prom "sum(private_channel_dedup_received_total)")

if [ -z "$c1" ] || [ -z "$c2" ]; then
  sk "dedup counter not yet emitted (write-node may not have processed any tx)"
elif [ "$c1" = "$c2" ]; then
  if [ "${c1%.*}" = "0" ]; then
    sk "pipeline idle (0 → 0 in ${PIPELINE_OBSERVE_SECS}s; expected on a fresh deploy with no traffic)"
  else
    sk "pipeline static ($c1 → $c2 in ${PIPELINE_OBSERVE_SECS}s; no recent traffic — verify by replaying a tx)"
  fi
else
  ok "pipeline moving (dedup_received_total: $c1 → $c2 in ${PIPELINE_OBSERVE_SECS}s)"
fi

# ---------------------------------------------------------------------------
# CHECK 7: Reconciliation log signals
# ---------------------------------------------------------------------------
banner 7 "operator-solana reconciliation logs (last ${RECONCILE_LOG_WINDOW})"

logs=$(docker logs --since "$RECONCILE_LOG_WINDOW" private-channel-operator-solana 2>&1 || true)
recon_err=$(echo "$logs" | grep -c "MismatchExceedsThreshold" || true)
recon_ok=$(echo "$logs"  | grep -cE "Balance reconciliation OK|reconciliation succeeded|Reconciliation passed" || true)

if [ "$recon_err" -gt 0 ]; then
  no "$recon_err MismatchExceedsThreshold error(s) in last $RECONCILE_LOG_WINDOW — state desync between validator and indexer DB"
elif [ "$recon_ok" -gt 0 ]; then
  ok "$recon_ok reconciliation-OK line(s) in last $RECONCILE_LOG_WINDOW"
else
  sk "no reconciliation lines in last $RECONCILE_LOG_WINDOW (interval may not have fired since deploy)"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo
echo "==== SANITY SUMMARY: ${pass} PASS · ${fail} FAIL · ${skip} SKIP ===="
echo "Threshold knobs: INDEXER_LAG_MAX=${INDEXER_LAG_MAX}  OPERATOR_BACKLOG_MAX=${OPERATOR_BACKLOG_MAX}"
echo "                 GATEWAY_ERROR_RATIO_MAX=${GATEWAY_ERROR_RATIO_MAX}"
echo

if [ "$fail" -gt 0 ]; then
  echo "Sanity FAILED. See FAIL lines above for actionable detail."
  exit 1
fi

if [ "$skip" -gt 0 ]; then
  echo "Sanity PASSED with ${skip} SKIPs (no traffic to observe yet — see SMOKE_TEST.md for active validation)."
fi
exit 0
