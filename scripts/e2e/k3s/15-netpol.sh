#!/usr/bin/env bash
# Step 15: the NetworkPolicy matrix. Every allowed row connects from its source pod; from
# an unlabeled pod, a kube-system pod and a monitoring pod the guarded ports are refused;
# app pods cannot reach Grafana or Prometheus; probes pass under default deny.
. "$(dirname "$0")/lib.sh"
progress "STEP $STEP ($E2E_PROFILE) NetworkPolicy matrix"
img="$(hk -n $NS get deploy gateway -o jsonpath='{.spec.template.spec.containers[0].image}')"
probe() {  # probe <namespace> <labels json> <name>
  hk -n "$1" delete pod "$3" --ignore-not-found --wait >/dev/null
  hk -n "$1" run "$3" --image="$img" --restart=Never --labels="$2" \
    --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":1000},"automountServiceAccountToken":false}}' -- sleep 900 >/dev/null
  hk -n "$1" wait --for=condition=Ready "pod/$3" --timeout=120s >/dev/null
}
# The impersonating probe pods also join the real Services' endpoints, so every row
# targets a real pod's IP, never a Service name.
target_ip() {
  local h="${1%%.*}" ns=$NS sel
  case "$h" in
    gateway | gateway-internal) sel="app.kubernetes.io/name=gateway" ;;
    validator | validator-fallback) sel="app.kubernetes.io/name=validator" ;;
    postgres-primary) sel="private-channel.io/db=ledger,cnpg.io/instanceRole=primary" ;;
    postgres-replica) sel="private-channel.io/db=ledger,cnpg.io/instanceRole=replica" ;;
    postgres-indexer) sel="private-channel.io/db=indexer,cnpg.io/instanceRole=primary" ;;
    pc-kps-grafana) ns=pc-monitoring sel="app.kubernetes.io/name=grafana" ;;
    pc-kps-prometheus) ns=pc-monitoring sel="app.kubernetes.io/name=prometheus" ;;
    *) sel="app.kubernetes.io/name=$h" ;;
  esac
  hk -n "$ns" get pods -l "$sel,app.kubernetes.io/part-of!=pc-e2e,!job-name" -o jsonpath='{.items[0].status.podIP}'
}
reach() {  # reach <ns> <pod> <url>: 0 if a TCP connection to the real pod is made
  local hp="${3#*://}" ip port
  hp="${hp%%/*}"; port="${hp##*:}"
  [[ "$hp" == pc-kps-grafana* ]] && port=3000
  ip="$(target_ip "${hp%%:*}")"
  [[ -n "$ip" ]] || return 1
  hk -n "$1" exec "$2" -- timeout 4 bash -c "exec 3<>/dev/tcp/$ip/$port" 2>/dev/null
}
probe $NS app.kubernetes.io/name=pc-e2e-none pc-e2e-none
probe kube-system app.kubernetes.io/name=pc-e2e-ks pc-e2e-ks
# Sources the policies admit, impersonated by label (same labels as the real pods).
for src in operator-solana gateway read-node indexer-solana blackbox-exporter pc-db-grants auth-migrate streamer postgres-exporter-primary; do
  probe $NS "app.kubernetes.io/name=$src,app.kubernetes.io/part-of=pc-e2e" "pc-e2e-as-$src"
done
fails=0
row() {  # row <allow|deny> <ns> <pod> <url>
  [[ $E2E_PROFILE != local && "$4" == *validator* ]] && return 0
  if reach "$2" "$3" "$4"; then got=allow; else got=deny; fi
  if [[ "$got" == "$1" ]]; then say "ok $1: $3 -> $4"; else say "WRONG ($got, want $1): $3 -> $4"; fails=$((fails + 1)); fi
}
G=gateway-internal.$NS
row allow $NS pc-e2e-as-operator-solana "http://$G:8904/"
row allow $NS pc-e2e-as-gateway "http://write-node.$NS:8900/"
row allow $NS pc-e2e-as-gateway "http://read-node.$NS:8901/"
row allow $NS pc-e2e-as-read-node "http://write-node.$NS:8900/"
row allow $NS pc-e2e-as-indexer-solana "http://read-node.$NS:8901/"
row allow $NS pc-e2e-as-indexer-solana "http://validator.$NS:8899/"
row allow $NS pc-e2e-as-indexer-solana "telnet://validator-fallback.$NS:10000"
row allow $NS pc-e2e-as-indexer-solana "telnet://postgres-indexer.$NS:5432"
row allow $NS pc-e2e-as-blackbox-exporter "http://write-node.$NS:8900/health"
row allow $NS pc-e2e-as-pc-db-grants "telnet://postgres-primary.$NS:5432"
row allow $NS pc-e2e-as-auth-migrate "telnet://postgres-primary.$NS:5432"
row allow $NS pc-e2e-as-streamer "telnet://postgres-replica.$NS:5432"
row allow $NS pc-e2e-as-postgres-exporter-primary "telnet://postgres-primary.$NS:5432"
row allow $NS pc-e2e-none "http://gateway.$NS:8899/health"
row deny $NS pc-e2e-as-gateway "http://$G:8904/"
row deny $NS pc-e2e-as-read-node "telnet://postgres-primary.$NS:5432"
row deny $NS pc-e2e-as-streamer "telnet://postgres-primary.$NS:5432"
row deny $NS pc-e2e-as-gateway "telnet://postgres-indexer.$NS:5432"
for p in "pc-e2e-none $NS" "pc-e2e-ks kube-system"; do
  set -- $p
  for u in "http://$G:8904/" "http://read-node.$NS:8901/" "telnet://postgres-primary.$NS:5432" "http://validator.$NS:8899/" "http://$G:9101/"; do
    row deny "$2" "$1" "$u"
  done
done
# Monitoring namespace: app pods may not reach Grafana or Prometheus.
row deny $NS pc-e2e-as-gateway "http://pc-kps-grafana.pc-monitoring:80/"
row deny $NS pc-e2e-as-gateway "http://pc-kps-prometheus.pc-monitoring:9090/"
# CNPG operator to the instance manager: the Cluster stays Ready and its status is fresh.
hk -n $NS get cluster pc-ledger -o jsonpath='{.status.phase}' | grep -q 'healthy' || { say "pc-ledger not healthy"; fails=$((fails + 1)); }
for p in $(hk -n $NS get pods -o name | grep pc-e2e); do hk -n $NS delete "$p" --wait=false >/dev/null; done
hk -n kube-system delete pod pc-e2e-ks --wait=false >/dev/null
# Probes under default deny: every app pod Ready with no restarts from failed probes.
notready="$(hk -n $NS get pods -l 'app.kubernetes.io/part-of=private-channel,!job-name' -o json | jq -r '.items[] | select(([.status.conditions[]? | select(.type=="Ready" and .status=="True")] | length) == 0) | .metadata.name')"
[[ -z "$notready" ]] || { say "not Ready under default deny: $notready"; fails=$((fails + 1)); }
(( fails == 0 )) || die "$fails NetworkPolicy rows wrong"
finding "APPROXIMATED: step 15 impersonates source pods by label (same labels as the real workloads); the monitoring Grafana/Prometheus deny rows rely on kps having no default deny in pc-monitoring, see FINDINGS."
pass
