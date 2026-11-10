#!/usr/bin/env bash
# Step 12: an image without the with-secrets entrypoint stops k3s.yml before helm upgrade
# and the release revision is unchanged; the gateway with GATEWAY_REQUIRE_JWT=1 and a
# blank JWT secret exits non-zero.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) fail closed"
sync_repo
before="$(helm_revision)"
if playbook "$STEP-nowrapper" k3s.yml -e reset_state=false -e image_tag=e2e-nowrapper; then die "deploy of a no-wrapper image passed"; fi
grep -q 'not\s*/usr/local/bin/with-secrets\|without the with-secrets entrypoint' "$RUN/logs/$STEP-nowrapper.log" || die "refused for another reason (logs/$STEP-nowrapper.log)"
grep -q 'Install or upgrade the app release' "$RUN/logs/$STEP-nowrapper.log" && die "reached helm upgrade"
[[ "$(helm_revision)" == "$before" ]] || die "release revision moved"
ok "no-wrapper image refused before helm upgrade (revision $before unchanged)"
img="$(hk -n $NS get deploy gateway -o jsonpath='{.spec.template.spec.containers[0].image}')"
hk -n $NS delete pod pc-e2e-nojwt --ignore-not-found >/dev/null
hk -n $NS run pc-e2e-nojwt --image="$img" --restart=Never --labels=app.kubernetes.io/name=pc-e2e-nojwt \
  --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":1000},"automountServiceAccountToken":false}}' \
  --env=GATEWAY_REQUIRE_JWT=1 --env=JWT_SECRET= --env=GATEWAY_WRITE_URL=http://write-node:8900 --env=GATEWAY_READ_URL=http://read-node:8901 \
  -- /usr/local/bin/gateway >>"$LOG" 2>&1
wait_for 120 bash -c "$(declare -f hk); E2E_HOST='$E2E_HOST'; hk -n $NS get pod pc-e2e-nojwt -o jsonpath='{.status.phase}' | grep -qx Failed" \
  || die "gateway with GATEWAY_REQUIRE_JWT=1 and blank JWT_SECRET did not exit non-zero"
hk -n $NS logs pc-e2e-nojwt | tail -3 | tee -a "$LOG"
hk -n $NS delete pod pc-e2e-nojwt >/dev/null
ok "gateway refuses to start on a blank JWT_SECRET"
pass
