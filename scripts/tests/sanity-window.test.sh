#!/usr/bin/env bash
set -euo pipefail

# Check 5 of sanity.sh judges only gateway errors since SANITY_WINDOW_START, so lifetime
# 502s from an earlier read-node recreate pass, while 502s inside the window still fail.
# deploy.yml must set the window on the compose path. Run from the repo root.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP:?}"' EXIT
failures=0
fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Stub curl answers Prometheus queries from LIFE_* (plain sums) or WIN_* (increase()).
mkdir "$TMP/bin"
cat >"$TMP/bin/curl" <<'STUB'
#!/usr/bin/env bash
q=""
for a in "$@"; do [[ "$a" == query=* ]] && q="${a#query=}"; done
[[ -z "$q" ]] && exit 7
if [[ "$q" == *increase\(* ]]; then r="$WIN_REQS" e="$WIN_ERRS"; else r="$LIFE_REQS" e="$LIFE_ERRS"; fi
case "$q" in
  *gateway_errors_total*) v="$e" ;;
  *gateway_requests_total*) v="$r" ;;
  *) echo '{"data":{"result":[]}}'; exit 0 ;;
esac
printf '{"data":{"result":[{"metric":{},"value":[0,"%s"]}]}}\n' "$v"
STUB
printf '#!/bin/sh\nexit 1\n' >"$TMP/bin/docker"
# Stub sleep records how long check 5 waits instead of waiting.
printf '#!/bin/sh\necho "$1" >>"%s/sleeps"\n' "$TMP" >"$TMP/bin/sleep"
chmod +x "$TMP/bin/curl" "$TMP/bin/docker" "$TMP/bin/sleep"

# Prints check 5's verdict line (PASS, FAIL or SKIP).
check5() {
  env PATH="$TMP/bin:$PATH" PIPELINE_OBSERVE_SECS=0 PROMETHEUS_URL=http://prom GATEWAY_URL=http://g \
    WRITE_URL=http://w READ_URL=http://r POSTGRES_USER=u "$@" \
    bash "$REPO/private-channel-deploy/scripts/sanity.sh" 2>/dev/null \
    | awk '/==== CHECK 5/{f=1; next} /==== CHECK/{f=0} f && NF {print $1; exit}' || true
}
now=$(date +%s)
life=(LIFE_REQS=1000 LIFE_ERRS=300)

got=$(check5 "${life[@]}" WIN_REQS=0 WIN_ERRS=0)
[[ "$got" == FAIL ]] || fail "no window: lifetime 502s should fail check 5, got '$got'"
got=$(check5 "${life[@]}" WIN_REQS=50 WIN_ERRS=0 SANITY_WINDOW_START=$((now - 120)))
[[ "$got" == PASS ]] || fail "window: lifetime 502s outside the window should pass, got '$got'"
got=$(check5 "${life[@]}" WIN_REQS=50 WIN_ERRS=10 SANITY_WINDOW_START=$((now - 120)))
[[ "$got" == FAIL ]] || fail "window: 502s inside the window should fail, got '$got'"
# A window under 60 s is never widened back past its start: check 5 waits until it is 60 s.
: >"$TMP/sleeps"
check5 "${life[@]}" WIN_REQS=50 WIN_ERRS=0 SANITY_WINDOW_START=$((now - 10)) >/dev/null
awk '$1 >= 45 {f=1} END {exit !f}' "$TMP/sleeps" || fail "window: a 10 s window was widened instead of waited out"

# The compose window starts once the health probes pass, after this deploy's own recreates.
python3 - "$REPO/private-channel-deploy/deploy.yml" <<'PY' || fail "deploy.yml does not start the check 5 window after the health probes"
import sys, yaml
tasks = yaml.safe_load(open(sys.argv[1]))[0]["tasks"]
flat = [t for x in tasks for t in (x.get("block") or [x])]
names = [t.get("name") for t in flat]
probe = names.index("HTTP probe")
sets = [i for i, t in enumerate(flat) if "_sanity_window_start" in str(t.get("register", "")) + str(t.get("ansible.builtin.set_fact", ""))]
env = str(next(t for t in flat if t.get("name") == "Run sanity script").get("environment", {}).get("SANITY_WINDOW_START", ""))
sys.exit(0 if sets and min(sets) > probe and "_sanity_window_start" in env and "deploy_start_epoch" in env else 1)
PY
[[ $failures -gt 0 ]] && { echo "$failures failure(s)" >&2; exit 1; }
echo "sanity window tests passed"
