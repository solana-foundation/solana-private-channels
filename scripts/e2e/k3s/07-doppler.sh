#!/usr/bin/env bash
# Step 7: the Doppler backend on k3s. A throwaway config in the test project holds the same
# values as the file backend; the deploy reads Doppler on the control node, every pc-*
# Secret is byte-identical to the file-backend render and nothing rolls. A value edited in
# Doppler changes nothing in the cluster until the next deploy. The config (and with it the
# service token) is deleted at the end. No value is printed.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) Doppler backend"
creds="${E2E_CREDS_FILE:-}"
[[ -n "$creds" && -f "$creds" ]] || blocked "set E2E_CREDS_FILE to the throwaway test creds env file"
project="$(grep -E '^DOPPLER_PROJECT=' "$creds" | cut -d= -f2)"
command -v doppler >/dev/null || blocked "no doppler CLI on this machine"
docker cp -L "$(command -v doppler)" k3s-e2e-ctl:/usr/local/bin/doppler
cfg="dev_k3s_$(date +%s)"
cleanup() { env -u DOPPLER_TOKEN doppler configs delete "$cfg" --project "$project" --yes --silent >/dev/null 2>&1 || true; ctl "rm -f /root/.pc-e2e-doppler-token" || true; }
trap cleanup EXIT
inst_args=(-e reset_state=false -e validator_reset=false -e "escrow_instance_id=$INSTANCE")
hashes() { for s in $(hk -n $NS get secret -o name | grep -E 'secret/pc-' | grep -v pc-deploy-run | sort); do
  echo "$s $(hk -n $NS get "$s" -o json | jq -cS .data | sha256sum | cut -c1-16)"; done; }
uids() { hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort; }
sync_repo
playbook "$STEP-file" k3s.yml "${inst_args[@]}" || die "file-backend baseline deploy failed"
h0="$(hashes)"; u0="$(uids)"
env -u DOPPLER_TOKEN doppler configs create "$cfg" --project "$project" --environment dev --silent || die "config create failed"
bash "$REPO/scripts/doppler-import.sh" --project "$project" --config "$cfg" "$RUN/e2e-$E2E_PROFILE-secrets.yml" >>"$LOG" 2>&1 || die "import failed"
env -u DOPPLER_TOKEN doppler configs tokens create e2e --project "$project" --config "$cfg" --plain | ctl "umask 077; cat > /root/.pc-e2e-doppler-token"
# The file backend's secrets.yml stays on disk; DOPPLER_TOKEN selects the Doppler backend.
PB_ENV='DOPPLER_TOKEN=$(cat /root/.pc-e2e-doppler-token)' playbook "$STEP-doppler" k3s.yml "${inst_args[@]}" || die "Doppler-backend deploy failed"
grep -q 'Secrets backend: doppler' "$RUN/logs/$STEP-doppler.log" || die "the deploy did not use the Doppler backend"
h1="$(hashes)"; u1="$(uids)"
if [[ "$h0" == "$h1" ]]; then ok "every pc-* Secret is identical to the file-backend render ($(wc -l <<<"$h1") Secrets)"; else
  diff <(echo "$h0") <(echo "$h1") | tee -a "$LOG"; die "Doppler render differs from the file backend"; fi
[[ "$u0" == "$u1" ]] || die "switching the backend rolled pods"
ok "no pod rolled; optional keys absent from Doppler (webhooks, whitelist RPC) stopped nothing"
# Edit a value in Doppler (stdin, never argv), then check the cluster did not move.
od -An -N16 -tx1 /dev/urandom | tr -d ' \n' | env -u DOPPLER_TOKEN doppler secrets set POSTGRES_GRAFANA_PASSWORD --project "$project" --config "$cfg" --silent >/dev/null || die "doppler set failed"
sleep 90
h2="$(hashes)"
[[ "$h2" == "$h1" ]] || die "a Doppler edit changed the cluster without a deploy"
ok "a Doppler edit changed nothing in the cluster before the next deploy"
playbook "$STEP-back-to-file" k3s.yml "${inst_args[@]}" || die "deploy back on the file backend failed"
[[ "$(hashes)" == "$h0" ]] || die "file backend render changed"
pass
