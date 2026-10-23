{{/* Name of the chart's objects: <release>-cronoq, or the release name if it contains "cronoq". */}}
{{- define "cronoq.fullname" -}}
{{- if contains "cronoq" .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-cronoq" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "cronoq.labels" -}}
app.kubernetes.io/name: cronoq
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "cronoq.selector" -}}
app.kubernetes.io/name: cronoq
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cronoq.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cronoq.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The server groups to deploy: one per shard, or a single unnamed one. */}}
{{- define "cronoq.groups" -}}
{{- if .Values.shards -}}
{{- range .Values.shards }}
- {{ .name | quote }}
{{- end }}
{{- else -}}
- ""
{{- end -}}
{{- end -}}

{{/* Scheme of the shard endpoints. */}}
{{- define "cronoq.scheme" -}}
{{- if .Values.tls.secretName }}https{{ else }}http{{ end -}}
{{- end -}}

{{/* Checks that cannot be expressed in the values schema. */}}
{{- define "cronoq.validate" -}}
{{- if and (not .Values.auth.existingSecret) (not .Values.auth.insecureNoAuth) -}}
{{- fail "auth.existingSecret is required (a Secret with keys client, worker, metrics, admin), or set auth.insecureNoAuth=true on a trusted network" -}}
{{- end -}}
{{- if and (eq .Values.store.kind "memory") (gt (int .Values.replicas) 1) -}}
{{- fail "store.kind=memory keeps state in each pod: every replica would lead on its own. Use replicas: 1, or store.kind=postgres for standbys" -}}
{{- end -}}
{{- if and (eq .Values.store.kind "postgres") (not .Values.store.postgres.urlSecret.name) -}}
{{- fail "store.postgres.urlSecret.name is required with store.kind=postgres" -}}
{{- end -}}
{{- if and .Values.shards (not .Values.tls.secretName) (not .Values.insecurePeerHttp) -}}
{{- fail "shards without TLS send callers' tokens between servers over plain http: set tls.secretName, or insecurePeerHttp=true on a trusted cluster network" -}}
{{- end -}}
{{- if and .Values.shards (eq .Values.store.kind "postgres") (not (contains "{shard}" .Values.store.postgres.schema)) -}}
{{- fail "with shards, store.postgres.schema must contain {shard}, so that each shard has its own tables" -}}
{{- end -}}
{{- if and .Values.serviceMonitor.enabled (not .Values.auth.existingSecret) -}}
{{- fail "serviceMonitor needs auth.existingSecret for the metrics token" -}}
{{- end -}}
{{- end -}}
