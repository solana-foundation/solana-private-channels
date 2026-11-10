#!/usr/bin/env bash
# Step 20: blackbox and every scrape job up, exactly 9 dashboards, compose labels rebuilt
# without double counting, node-exporter target, pinned versions, and Grafana's service
# account cannot read Secrets outside pc-monitoring.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) monitoring"
q() { hk -n pc-monitoring exec prometheus-pc-kps-prometheus-0 -c prometheus -- wget -qO- "http://localhost:9090/api/v1/query?query=$(python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$1")"; }
fails=0
chk() { if [[ "$2" == ok ]]; then say "ok: $1"; else say "WRONG: $1"; fails=$((fails + 1)); fi; }
jobs="$(hk -n pc-monitoring get secret pc-prometheus-scrape -o jsonpath='{.data.scrape\.yaml}' | base64 -d | python3 -c 'import sys,yaml; print(" ".join(j["job_name"] for j in yaml.safe_load(sys.stdin)))')"
wait_for 120 bash -c "$(declare -f q hk); E2E_HOST='$E2E_HOST'; q 'probe_success == 0' | grep -q '\"result\":\[\]'" || true
down="$(q 'probe_success == 0' | jq -r '.data.result[].metric.instance' | xargs)"
chk "all blackbox probes succeed (failing: ${down:-none})" "$([[ -z "$down" ]] && echo ok)"
for j in $jobs; do
  [[ "$j" == blackbox-http ]] && continue
  v="$(q "min(up{job=\"$j\"})" | jq -r '.data.result[0].value[1] // "absent"')"
  chk "job $j up=$v" "$([[ "$v" == 1 ]] && echo ok)"
done
v="$(q 'count(up{job="node-exporter"} == 1)' | jq -r '.data.result[0].value[1] // 0')"
chk "node-exporter target up ($v)" "$([[ "$v" -ge 1 ]] && echo ok)"
for s in indexer-solana operator-solana gateway write-node; do
  n="$(q "count(container_memory_working_set_bytes{container_label_com_docker_compose_service=\"$s\"})" | jq -r '.data.result[0].value[1] // 0')"
  chk "compose label for $s present ($n series)" "$([[ "$n" -ge 1 ]] && echo ok)"
  n="$(q "count by (name) (container_memory_working_set_bytes{name=\"private-channel-$s\"})" | jq -r '.data.result[0].value[1] // 0')"
  chk "one memory series per name for $s (got $n)" "$([[ "$n" == 1 ]] && echo ok)"
done
n="$(q 'count(container_network_receive_bytes_total{container_label_com_docker_compose_service="gateway"})' | jq -r '.data.result[0].value[1] // 0')"
chk "network series carry the compose label ($n)" "$([[ "$n" -ge 1 ]] && echo ok)"
n="$(q 'count(container_memory_working_set_bytes{container_label_com_docker_compose_service=~"pc-ledger.*"})' | jq -r '.data.result[0].value[1] // 0')"
chk "ledger panels select pc-ledger ($n series)" "$([[ "$n" -ge 1 ]] && echo ok)"
fs="$(q 'count(container_fs_usage_bytes{container_label_com_docker_compose_service!=""})' | jq -r '.data.result[0].value[1] // 0')"
say "containers dashboard fs series with a compose label: $fs"
[[ "$fs" -ge 1 ]] || finding "the containers dashboard filesystem panels (group by id) are empty on kubelet cAdvisor: container_fs_usage_bytes has no per-container series on containerd here"
gimg="$(hk -n pc-monitoring get deploy pc-kps-grafana -o jsonpath='{.spec.template.spec.containers[?(@.name=="grafana")].image}')"
pimg="$(hk -n pc-monitoring get prometheus pc-kps-prometheus -o jsonpath='{.spec.image}{.spec.version}')"
nimg="$(hk -n pc-monitoring get ds -l app.kubernetes.io/name=prometheus-node-exporter -o jsonpath='{.items[0].spec.template.spec.containers[0].image}')"
say "images: grafana $gimg, prometheus $pimg, node-exporter $nimg"
chk "pinned Grafana 11.3.0" "$([[ "$gimg" == *:11.3.0 ]] && echo ok)"
chk "pinned node-exporter v1.8.2" "$([[ "$nimg" == *:v1.8.2 ]] && echo ok)"
chk "pinned Prometheus v2.55.1" "$([[ "$pimg" == *v2.55.1* ]] && echo ok)"
sa="$(hk -n pc-monitoring get deploy pc-kps-grafana -o jsonpath='{.spec.template.spec.serviceAccountName}')"
for ns in $NS kube-system default; do
  a="$(hk auth can-i get secrets -n "$ns" --as="system:serviceaccount:pc-monitoring:$sa" || true)"
  chk "Grafana SA get secrets in $ns: $a" "$([[ "$a" == no ]] && echo ok)"
done
connect_ctl
pf pc-monitoring svc/pc-kps-grafana 13000:80
n="$(ctl "curl -s -u 'admin:$(e2e_secret grafana_admin_password)' 'http://127.0.0.1:13000/api/search?type=dash-db' | jq length")"
chk "exactly 9 dashboards ($n)" "$([[ "$n" == 9 ]] && echo ok)"
(( fails == 0 )) || die "$fails monitoring checks wrong"
pass
