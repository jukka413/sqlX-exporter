{{/*
Expand the name of the chart.
*/}}
{{- define "sqlx-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "sqlx-exporter.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label.
*/}}
{{- define "sqlx-exporter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "sqlx-exporter.labels" -}}
helm.sh/chart: {{ include "sqlx-exporter.chart" . }}
{{ include "sqlx-exporter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "sqlx-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sqlx-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Config volume source.
Приоритет:
  1. existingSecret — явно указанный готовый Secret (создан вручную или через ESO)
  2. По умолчанию — ConfigMap из values.config
*/}}
{{- define "sqlx-exporter.configVolume" -}}
{{- if .Values.existingSecret }}
secret:
  secretName: {{ .Values.existingSecret }}
{{- else }}
configMap:
  name: {{ include "sqlx-exporter.fullname" . }}
{{- end }}
{{- end }}
