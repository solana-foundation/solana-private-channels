{{- define "pcdb.roleSecret" -}}pc-db-role-{{ . | replace "_" "-" }}{{- end -}}

{{/* Backup path per Cluster lifetime: reuse the live Cluster's, else stamp a new one. */}}
{{- define "pcdb.serverName" -}}
{{- $c := .c -}}
{{- $server := $c.serverName -}}
{{- if not $server -}}
{{- $live := lookup "postgresql.cnpg.io/v1" "Cluster" .root.Release.Namespace $c.name -}}
{{- if $live -}}
{{- range $p := ($live.spec.plugins | default list) -}}
{{- if eq $p.name "barman-cloud.cloudnative-pg.io" -}}{{- $server = (get ($p.parameters | default dict) "serverName") -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not $server -}}{{- $server = printf "%s-%s" $c.name (now | date "20060102t150405") -}}{{- end -}}
{{ $server }}
{{- end -}}
