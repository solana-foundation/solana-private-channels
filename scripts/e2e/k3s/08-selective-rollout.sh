#!/usr/bin/env bash
# Step 8: each secret kind rolls exactly its row of the committed secrets-rev table; CNPG
# never rolls; a no-change deploy rolls nothing. Then the new Grafana admin logs in, the old
# is refused, a dashboard edit loads and the contact point holds the new webhook.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) selective rollout"
table="$REPO/charts/private-channel/secrets-rev-table.yaml"
envkey=$([[ $E2E_PROFILE == local ]] && echo localnet || echo devnet)
sec="$RUN/e2e-$E2E_PROFILE-secrets.yml"
inst_args=(-e reset_state=false -e validator_reset=false)
[[ -n "${INSTANCE:-}" ]] && inst_args+=(-e "escrow_instance_id=$INSTANCE")
uids() {
  { hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o json
    hk -n $NS get pods -l 'private-channel.io/db,!job-name' -o json
    hk -n pc-monitoring get pods -l app.kubernetes.io/name=grafana -o json; } \
  | jq -r '.items[] | "\(.metadata.labels["app.kubernetes.io/name"] // .metadata.labels["cnpg.io/cluster"]) \(.metadata.uid)"' | sort
}
# Workloads with a pod (name uid) present before and gone after.
rolled() { comm -23 <(echo "$1") <(echo "$2") | awk '{print $1}' | sort -u | tr '\n' ' '; }
deploy() { sync_repo; playbook "$STEP-$1" k3s.yml "${inst_args[@]}" || die "deploy $1 failed"; sleep 20; }

deploy baseline
u0="$(uids)"; deploy nochange; u1="$(uids)"
[[ -z "$(rolled "$u0" "$u1")" ]] || die "a no-change deploy rolled: $(rolled "$u0" "$u1")"
ok "no-change deploy rolled nothing (salt stable)"

old_admin="$(e2e_secret grafana_admin_password)"
fails=0
for var in ${E2E_REV_VARS:-postgres_runtime_password postgres_gateway_password postgres_monitoring_password postgres_indexer_password \
  postgres_password jwt_secret grafana_admin_password postgres_grafana_password}; do
  want="$(python3 -c 'import sys,yaml; print(" ".join(yaml.safe_load(open(sys.argv[1]))[sys.argv[2]][sys.argv[3]]))' "$table" "$envkey" "$var")"
  u0="$(uids)"
  sed -i -E "s/^($var: )(.*)$/\1\2x/" "$sec"
  deploy "$var"
  hk -n $NS wait --for=condition=Ready pod -l 'app.kubernetes.io/part-of=private-channel,!job-name' --timeout=300s >/dev/null 2>&1 || true
  u1="$(uids)"
  got="$(rolled "$u0" "$u1")"
  # Workloads the table lists but this env does not run (streamer off by default) cannot roll.
  want_live=""; for w in $want; do grep -q "^$w " <<<"$u0" && want_live+="$w "; done
  if [[ "$(tr ' ' '\n' <<<"$got" | sort | xargs)" == "$(tr ' ' '\n' <<<"$want_live" | sort | xargs)" ]]; then
    say "ok $var rolled [$got]"
  else
    say "WRONG $var rolled [$got], table says [$want_live]"; fails=$((fails + 1))
  fi
  grep -q '^pc-ledger\|^pc-indexer' <<<"$(comm -23 <(echo "$u0") <(echo "$u1"))" && { say "WRONG: a CNPG pod rolled on $var"; fails=$((fails + 1)); }
done
(( fails == 0 )) || die "$fails secret kinds rolled the wrong set"
ok "every secret kind rolled exactly its table row; CNPG pods never rolled"

# Grafana admin login after the rotation, through a port-forward.
connect_ctl
pf pc-monitoring svc/pc-kps-grafana 13000:80
new_admin="$(e2e_secret grafana_admin_password)"
c_new="$(ctl "curl -s -o /dev/null -w '%{http_code}' -u 'admin:$new_admin' http://127.0.0.1:13000/api/user")"
c_old="$(ctl "curl -s -o /dev/null -w '%{http_code}' -u 'admin:$old_admin' http://127.0.0.1:13000/api/user")"
[[ "$c_new" == 200 && "$c_old" == 401 ]] || die "Grafana admin: new $c_new old $c_old"
ok "new Grafana admin password logs in (200), the old one is refused (401)"
# A dashboard ConfigMap edit still loads (the sidecars hold the new admin login).
hk -n pc-monitoring patch cm pc-dash-health --type merge -p "$(hk -n pc-monitoring get cm pc-dash-health -o json \
  | jq -c '{data: (.data | map_values(sub("\"title\": *\"Solana Private Channels Health Overview\""; "\"title\": \"Solana Private Channels Health Overview e2e\"")))}')" >/dev/null
wait_for 120 bash -c "$E2E_CTL bash -lc \"curl -s -u 'admin:$new_admin' 'http://127.0.0.1:13000/api/search?query=Overview%20e2e'\" | grep -q 'Overview e2e'" \
  || die "dashboard edit did not load (sidecar 401?)"
ok "dashboard ConfigMap edit loaded after the admin change"
hook="https://grafana-hook-e2e-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n').invalid/x"
echo "alert_webhook_url: '$hook'" >"$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
deploy webhook
pf pc-monitoring svc/pc-kps-grafana 13000:80
cp="$(ctl "curl -s -u 'admin:$new_admin' http://127.0.0.1:13000/api/v1/provisioning/contact-points")"
grep -qF "${hook%/x}" <<<"$cp" || [[ "$cp" == *"[REDACTED]"* ]] || die "contact point does not hold the new webhook"
ok "contact point updated with the new webhook (secure field: $(grep -c REDACTED <<<"$cp" || true) redacted)"
rm -f "$RUN/e2e-$E2E_PROFILE-secrets.extra.yml"
pass
