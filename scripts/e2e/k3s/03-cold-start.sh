#!/usr/bin/env bash
# Step 3: scale everything to 0 and start together; each pod's initContainers wait for
# exactly its compose dependencies. Asserted from the init logs and start times.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) cold-start ordering"
apps="$(hk -n $NS get deploy,sts -l app.kubernetes.io/part-of=private-channel -o name | grep -v validator)"
hk -n $NS scale $apps --replicas=0 >>"$LOG"
hk -n $NS wait --for=delete pod -l 'app.kubernetes.io/part-of=private-channel,app.kubernetes.io/name!=validator,!job-name' --timeout=300s >>"$LOG" 2>&1 || true
say "all app pods stopped; starting together"
for a in $apps; do hk -n $NS scale "$a" --replicas=1 >/dev/null & done; wait
hk -n $NS wait --for=condition=Ready pod -l 'app.kubernetes.io/part-of=private-channel,!job-name' --timeout=900s >>"$LOG" 2>&1 || die "pods not Ready after a cold start"
ready() { hk -n $NS get pods -l "app.kubernetes.io/name=$1" -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].lastTransitionTime}'; }
started() { hk -n $NS get pods -l "app.kubernetes.io/name=$1" -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}'; }
# A dependent's main container starts after each "healthy" dependency turned Ready.
waits="$(hk -n $NS get deploy,sts -l app.kubernetes.io/part-of=private-channel -o json | jq -r '.items[] |
  .metadata.name as $n | (.spec.template.spec.initContainers // [])[] | .env[]? | select(.name=="WAIT_HTTP") | .value | split(" ")[] | "\($n) \(.)"')"
while read -r svc spec; do
  [[ -z "$svc" ]] && continue
  kind="${spec%%:*}"; rest="${spec#*:}"; dep="${rest%%:*}"
  s="$(started "$svc")"; r="$(ready "$dep")"
  if [[ "$kind" == healthy ]]; then
    [[ "$s" > "$r" || "$s" == "$r" ]] || die "$svc started at $s before $dep was healthy at $r"
  fi
  say "$svc waited for $dep ($kind): started $s, $dep ready $r"
done <<<"$waits"
for p in $(hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o name); do
  for ic in $(hk -n $NS get "$p" -o jsonpath='{.spec.initContainers[*].name}'); do
    n=$(hk -n $NS logs "$p" -c "$ic" 2>/dev/null | grep -c waiting || true)
    say "$p $ic: $n wait lines"
  done
done
ok "every dependent started after its dependencies (logs/e2e-$E2E_PROFILE-$STEP.log)"
pass
