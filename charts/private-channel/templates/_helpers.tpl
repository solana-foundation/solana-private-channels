{{/* App image, pinned to the verified digest when one is given. */}}
{{- define "pc.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
{{- end -}}

{{- define "pc.isLocalnet" -}}{{ if eq .Values.network "localnet" }}true{{ end }}{{- end -}}

{{/* Compose shape: localnet uses docker-compose.yml, devnet and mainnet docker-compose.devnet.yml. */}}
{{- define "pc.profile" -}}{{ if eq .Values.network "localnet" }}localnet{{ else }}devnet{{ end }}{{- end -}}

{{- define "pc.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: private-channel
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end -}}

{{/*
Static shape of each app workload. single: one copy only (schema-enforced).
health: the /health port; extraPorts are exposed but not probed.
*/}}
{{- define "pc.apps" -}}
write-node: { kind: Deployment, single: true, recreate: true, port: {{ .Values.ports.write }}, extraPorts: { metrics: {{ .Values.ports.nodeMetrics }} }, binary: /usr/local/bin/private-channel-node }
read-node: { kind: Deployment, port: {{ .Values.ports.read }}, extraPorts: { metrics: {{ .Values.ports.nodeMetrics }} }, binary: /usr/local/bin/private-channel-node }
gateway: { kind: Deployment, port: {{ .Values.ports.gateway }}, extraPorts: { internal: {{ .Values.ports.gatewayInternal }}, metrics: {{ .Values.ports.gatewayMetrics }} }, binary: /usr/local/bin/gateway }
auth: { kind: Deployment, port: {{ .Values.ports.auth }}, extraPorts: {}, binary: /usr/local/bin/auth, gate: auth }
streamer: { kind: Deployment, single: true, recreate: true, port: {{ .Values.ports.streamer }}, extraPorts: {}, binary: /usr/local/bin/streamer, gate: streamer }
indexer-solana: { kind: StatefulSet, single: true, port: 9100, extraPorts: {}, worker: indexer, gate: workers }
indexer-private-channel: { kind: StatefulSet, single: true, port: 9100, extraPorts: {}, worker: indexer }
operator-solana: { kind: Deployment, single: true, recreate: true, port: 9102, extraPorts: {}, worker: operator, gate: workers }
operator-private-channel: { kind: Deployment, single: true, recreate: true, port: 9103, extraPorts: {}, worker: operator, gate: workers }
{{- end -}}

{{/* Whether a gated workload renders. */}}
{{- define "pc.enabled" -}}
{{- $gate := .gate | default "" -}}
{{- if eq $gate "" -}}true
{{- else if eq $gate "workers" -}}{{ if .root.Values.workers.enabled }}true{{ end }}
{{- else if eq $gate "auth" -}}{{ if .root.Values.auth.enabled }}true{{ end }}
{{- else if eq $gate "streamer" -}}{{ if and .root.Values.streamer.enabled (eq .root.Values.network "localnet") }}true{{ end }}
{{- end -}}
{{- end -}}

{{/* Port a wait target answers on. */}}
{{- define "pc.waitPort" -}}
{{- $p := dict "write-node" .root.Values.ports.write "read-node" .root.Values.ports.read "gateway" .root.Values.ports.gateway "gateway-internal" .root.Values.ports.gatewayInternal "validator" 8899 -}}
{{- get $p .target -}}
{{- end -}}

{{/* initContainers that hold the pod until its compose depends_on are met. */}}
{{- define "pc.waits" -}}
{{- $root := .root -}}
{{- $w := get (get $root.Values.waits (include "pc.profile" $root)) .name | default dict -}}
{{- $http := list -}}
{{- range $t := ($w.healthy | default list) }}{{ $http = append $http (printf "healthy:%s:%s" $t (include "pc.waitPort" (dict "root" $root "target" $t))) }}{{ end -}}
{{- range $t := ($w.started | default list) }}{{ $http = append $http (printf "started:%s:%s" $t (include "pc.waitPort" (dict "root" $root "target" $t))) }}{{ end -}}
{{- if $w.postgres }}
- name: wait-postgres
  image: {{ $root.Values.waitImages.postgres }}
  command: [ /bin/sh, -c ]
  args:
    - 'for h in $WAIT_POSTGRES; do until pg_isready -q -h "$h" -p 5432 -U postgres -d postgres; do echo "waiting for $h"; sleep 2; done; done'
  env:
    - { name: WAIT_POSTGRES, value: {{ join " " $w.postgres | quote }} }
  securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: [ ALL ] } }
  resources: { requests: { cpu: 10m, memory: 16Mi }, limits: { memory: 64Mi } }
{{- end }}
{{- if $http }}
- name: wait-services
  image: {{ include "pc.image" $root }}
  command: [ /bin/sh, -c ]
  args:
    - |
      for spec in $WAIT_HTTP; do
        kind=${spec%%:*}; rest=${spec#*:}; host=${rest%%:*}; port=${rest#*:}
        if [ "$kind" = healthy ]; then
          until curl -fsS -m 3 -o /dev/null "http://$host:$port/health"; do echo "waiting for $host healthy"; sleep 2; done
        else
          until curl -s -m 3 -o /dev/null "http://$host:$port/"; do echo "waiting for $host started"; sleep 2; done
        fi
      done
  env:
    - { name: WAIT_HTTP, value: {{ join " " $http | quote }} }
  securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: [ ALL ] } }
  resources: { requests: { cpu: 10m, memory: 16Mi }, limits: { memory: 64Mi } }
{{- end }}
{{- end -}}

{{/* KEY_FILE env for each key of the service's Secret. */}}
{{- define "pc.secretEnv" -}}
{{- range $k := (get .root.Values.secretKeys .name | default list) }}
- { name: {{ printf "%s_FILE" $k }}, value: {{ printf "/run/secrets/%s" $k }} }
{{- end }}
{{- end -}}

{{- define "pc.hasSecrets" -}}{{ if (get .root.Values.secretKeys .name) }}true{{ end }}{{- end -}}

{{/* Pod-level security shared by every app pod. */}}
{{- define "pc.podSecurity" -}}
automountServiceAccountToken: false
{{- /* The pause container is PID 1, so a binary without a SIGTERM handler still stops on SIGTERM (as with compose init: true). */}}
shareProcessNamespace: true
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
{{- end -}}

{{- define "pc.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  capabilities: { drop: [ ALL ] }
{{- end -}}

{{- define "pc.pullSecrets" -}}
{{- if .Values.imagePullSecret }}
imagePullSecrets:
  - name: {{ .Values.imagePullSecret }}
{{- end }}
{{- end -}}

{{/* Signer env (non-secret) for the roles a service signs as. */}}
{{- define "pc.signerEnv" -}}
{{- $c := .root.Values.config -}}
- { name: ADMIN_SIGNER, value: {{ $c.adminSigner | quote }} }
- { name: ADMIN_PRIVATE_KEY, value: "" }
{{- range $k, $v := $c.adminSignerEnv }}
- { name: {{ $k }}, value: {{ $v | toString | quote }} }
{{- end }}
{{- if .operator }}
- { name: OPERATOR_SIGNER, value: {{ $c.operatorSigner | quote }} }
- { name: OPERATOR_PRIVATE_KEY, value: "" }
{{- range $k, $v := $c.operatorSignerEnv }}
- { name: {{ $k }}, value: {{ $v | toString | quote }} }
{{- end }}
{{- end }}
{{- end -}}

{{- define "pc.nodeEnv" -}}
{{- $c := .Values.config -}}
- { name: PRIVATE_CHANNEL_ADMIN_KEYS, value: {{ $c.adminKeys | quote }} }
- { name: PRIVATE_CHANNEL_PERMANENT_DELEGATE, value: {{ $c.permanentDelegate | quote }} }
{{- if $c.complianceOwner }}
- { name: PRIVATE_CHANNEL_COMPLIANCE_OWNER, value: {{ $c.complianceOwner | quote }} }
{{- end }}
- { name: PRIVATE_CHANNEL_PG_MAX_CONNECTIONS, value: "32" }
- { name: PRIVATE_CHANNEL_LOG_LEVEL, value: info }
- { name: PRIVATE_CHANNEL_JSON_LOGS, value: "false" }
- { name: PRIVATE_CHANNEL_METRICS, value: "true" }
- { name: PRIVATE_CHANNEL_METRICS_PORT, value: {{ .Values.ports.nodeMetrics | quote }} }
{{- end -}}

{{/* Plain env per service, the same values each compose file sets. Secrets are KEY_FILE only. */}}
{{- define "pc.env" -}}
{{- $root := .root -}}
{{- $v := $root.Values -}}
{{- $local := eq $v.network "localnet" -}}
{{- $noop := "http://localhost:9/no-op" -}}
{{- $readUrl := printf "http://read-node:%v" $v.ports.read -}}
{{- $internalUrl := printf "http://gateway-internal:%v" $v.ports.gatewayInternal -}}
{{- if eq .name "write-node" }}
- { name: RUST_LOG, value: info }
- { name: PRIVATE_CHANNEL_PORT, value: {{ $v.ports.write | quote }} }
- { name: PRIVATE_CHANNEL_MODE, value: write }
- { name: PRIVATE_CHANNEL_SIGVERIFY_QUEUE_SIZE, value: "10000000" }
- { name: PRIVATE_CHANNEL_SIGVERIFY_WORKERS, value: "32" }
- { name: PRIVATE_CHANNEL_MAX_CONNECTIONS, value: "1000000" }
- { name: PRIVATE_CHANNEL_MAX_TX_PER_BATCH, value: "64" }
- { name: PRIVATE_CHANNEL_BATCH_DEADLINE_MS, value: "10" }
- { name: PRIVATE_CHANNEL_BATCH_CHANNEL_CAPACITY, value: "16" }
- { name: PRIVATE_CHANNEL_MAX_SVM_WORKERS, value: "8" }
{{ include "pc.nodeEnv" $root }}
{{- else if eq .name "read-node" }}
- { name: RUST_LOG, value: info }
- { name: PRIVATE_CHANNEL_PORT, value: {{ $v.ports.read | quote }} }
- { name: PRIVATE_CHANNEL_MODE, value: read }
- { name: PRIVATE_CHANNEL_MAX_CONNECTIONS, value: "100000" }
{{ include "pc.nodeEnv" $root }}
{{- else if eq .name "gateway" }}
- { name: RUST_LOG, value: info }
- { name: GATEWAY_PORT, value: {{ $v.ports.gateway | quote }} }
- { name: GATEWAY_INTERNAL_PORT, value: {{ $v.ports.gatewayInternal | quote }} }
- { name: METRICS_PORT, value: {{ $v.ports.gatewayMetrics | quote }} }
- { name: GATEWAY_WRITE_URL, value: {{ printf "http://write-node:%v" $v.ports.write | quote }} }
- { name: GATEWAY_READ_URL, value: {{ $readUrl | quote }} }
- { name: GATEWAY_UPSTREAM_TIMEOUT_SECS, value: "30" }
- { name: GATEWAY_MAX_FORWARDED_READS, value: "768" }
- { name: GATEWAY_MAX_CONNECTIONS, value: "1024" }
- { name: GATEWAY_MAX_CONNECTIONS_PER_IP, value: "64" }
- { name: GATEWAY_RATE_LIMIT_PER_SECOND, value: "50" }
- { name: GATEWAY_RATE_LIMIT_BURST, value: "100" }
- { name: PRIVATE_CHANNEL_PG_MAX_CONNECTIONS, value: "32" }
- { name: GATEWAY_REQUIRE_JWT, value: {{ ternary "1" "" $v.auth.enabled | quote }} }
- { name: AUTH_DATABASE_MAX_CONNECTIONS, value: "10" }
- { name: GATEWAY_WHITELIST_INSTANCE, value: {{ $v.config.gatewayWhitelistInstance | quote }} }
- { name: GATEWAY_WHITELIST_READS_PER_SECOND, value: "20" }
{{- else if eq .name "auth" }}
- { name: RUST_LOG, value: info }
- { name: AUTH_PORT, value: {{ $v.ports.auth | quote }} }
- { name: CORS_ALLOWED_ORIGIN, value: {{ $v.auth.corsAllowedOrigin | quote }} }
- { name: AUTH_DATABASE_MAX_CONNECTIONS, value: "10" }
- { name: AUTH_ARGON2_MAX_CONCURRENCY, value: "4" }
- { name: AUTH_RATE_LIMIT_PER_SECOND, value: "5" }
- { name: AUTH_RATE_LIMIT_BURST, value: "10" }
- { name: AUTH_USERNAME_FAILED_LOGINS_PER_MINUTE, value: "5" }
- { name: AUTH_MAX_CONNECTIONS, value: "1024" }
- { name: AUTH_MAX_CONNECTIONS_PER_IP, value: "64" }
- { name: AUTH_HEADER_READ_TIMEOUT_SECS, value: "10" }
- { name: AUTH_REQUEST_TIMEOUT_SECS, value: "15" }
- { name: AUTH_TCP_KEEPALIVE_IDLE_SECS, value: "60" }
- { name: AUTH_TCP_KEEPALIVE_INTERVAL_SECS, value: "15" }
{{- else if eq .name "auth-migrate" }}
- { name: RUST_LOG, value: info }
{{- else if eq .name "streamer" }}
- { name: RUST_LOG, value: info }
- { name: STREAMER_PORT, value: {{ $v.ports.streamer | quote }} }
- { name: STREAMER_POLL_INTERVAL_MS, value: "200" }
- { name: STREAMER_LOG_LEVEL, value: info }
{{- else if eq .name "indexer-solana" }}
- { name: ESCROW_CONFIDENTIAL_PUBKEY, value: {{ $v.escrow.confidentialPubkey | quote }} }
- { name: RUST_LOG, value: "info,private_channel_indexer=debug" }
- { name: COMMON_SOURCE_RPC_URL, value: {{ $readUrl | quote }} }
- { name: METRICS_PORT, value: "9100" }
- { name: COMMON_ESCROW_INSTANCE_ID, value: {{ $v.escrow.instanceId | quote }} }
- { name: ALERT_WEBHOOK_URL, value: {{ $noop }} }
{{- if $local }}
- { name: COMMON_RPC_URL, value: "http://validator:8899" }
- { name: INDEXER_YELLOWSTONE_ENDPOINT, value: "http://validator:10000" }
{{- else }}
- { name: INDEXER_YELLOWSTONE_ENDPOINT, value: {{ $v.config.yellowstoneEndpoint | quote }} }
{{- end }}
{{- else if eq .name "indexer-private-channel" }}
- { name: RUST_LOG, value: "info,private_channel_indexer=debug" }
- { name: COMMON_RPC_URL, value: {{ $readUrl | quote }} }
{{- if not $local }}
- { name: INDEXER_BACKFILL_RPC_URL, value: {{ $readUrl | quote }} }
{{- end }}
- { name: METRICS_PORT, value: "9100" }
- { name: ALERT_WEBHOOK_URL, value: {{ $noop }} }
{{- else if eq .name "operator-solana" }}
- { name: ESCROW_CONFIDENTIAL_PUBKEY, value: {{ $v.escrow.confidentialPubkey | quote }} }
- { name: RUST_LOG, value: "info,private_channel_indexer=debug" }
- { name: COMMON_RPC_URL, value: {{ $internalUrl | quote }} }
{{- if $local }}
- { name: COMMON_SOURCE_RPC_URL, value: "http://validator:8899" }
{{- end }}
- { name: COMMON_ESCROW_INSTANCE_ID, value: {{ $v.escrow.instanceId | quote }} }
{{ include "pc.signerEnv" (dict "root" $root "operator" false) }}
- { name: OPERATOR_PERMANENT_DELEGATE, value: {{ $v.config.permanentDelegate | quote }} }
- { name: METRICS_PORT, value: "9102" }
- { name: ALERT_WEBHOOK_URL, value: {{ $noop }} }
- { name: OPERATOR_RECONCILIATION_WEBHOOK_URL, value: {{ $noop }} }
{{- else if eq .name "operator-private-channel" }}
- { name: ESCROW_CONFIDENTIAL_PUBKEY, value: {{ $v.escrow.confidentialPubkey | quote }} }
- { name: RUST_LOG, value: "info,private_channel_indexer=debug" }
{{- if $local }}
- { name: COMMON_RPC_URL, value: "http://validator:8899" }
- { name: COMMON_FALLBACK_RPC_URL, value: "http://validator-fallback:8899" }
{{- end }}
- { name: COMMON_SOURCE_RPC_URL, value: {{ $internalUrl | quote }} }
{{ include "pc.signerEnv" (dict "root" $root "operator" true) }}
- { name: OPERATOR_PERMANENT_DELEGATE, value: {{ $v.config.permanentDelegate | quote }} }
- { name: COMMON_ESCROW_INSTANCE_ID, value: {{ $v.escrow.instanceId | quote }} }
- { name: METRICS_PORT, value: "9103" }
- { name: ALERT_WEBHOOK_URL, value: {{ $noop }} }
{{- end }}
{{- end -}}

{{/* Pod labels for a NetworkPolicy endpoint: an app name or db:<ledger|indexer>[:role]. */}}
{{- define "pc.npSelector" -}}
{{- if hasPrefix "db:" . -}}
{{- $p := splitList ":" . -}}
{{- /* pc-db stamps this label on every instance pod, so it survives a PITR Cluster rename. */}}
private-channel.io/db: {{ index $p 1 }}
{{- if gt (len $p) 2 }}
cnpg.io/instanceRole: {{ index $p 2 }}
{{- end }}
{{- else -}}
app.kubernetes.io/name: {{ . }}
{{- end -}}
{{- end -}}

{{/* Builds a 0600 libpq passfile from the Secret (escaping : and \), then starts the exporter. */}}
{{- define "pc.pgpassWrapper" -}}
umask 077; p=$(sed -e 's/\\/\\\\/g' -e 's/:/\\:/g' /run/secrets/DATA_SOURCE_PASS); printf '*:*:*:*:%s\n' "$p" > /pgpass/pgpass; unset p; exec /bin/postgres_exporter
{{- end -}}

