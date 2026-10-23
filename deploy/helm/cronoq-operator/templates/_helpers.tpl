{{- define "op.fullname" -}}
{{- if contains "cronoq-operator" .Release.Name -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-cronoq-operator" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "op.labels" -}}
app.kubernetes.io/name: cronoq-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "op.selector" -}}
app.kubernetes.io/name: cronoq-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "op.serviceAccount" -}}
{{- default (include "op.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}
