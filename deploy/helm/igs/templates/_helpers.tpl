{{- define "igs.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "igs.labels" -}}
app.kubernetes.io/name: igs
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "igs.selector" -}}
app.kubernetes.io/name: igs
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/*
igs.proxies is a pod's use of the proxies file (secretFiles): "required"
for the components that reach Instagram through the proxy tiers (the
crawlers, registry-sync), "optional" for the scheduler (it counts each
tier's exits for discovery, and starts without them), "" for the rest and
the hook Jobs, which neither mount it nor wait for its Secret. Args: dict
root, proxies (the component's proxies value).
*/}}
{{- define "igs.proxies" -}}
{{- if and .root.Values.secretFiles.existingSecret (has .proxies (list "required" "optional")) -}}
{{ .proxies }}
{{- end -}}
{{- end -}}

{{/* Args: dict root, proxies. */}}
{{- define "igs.configFiles" -}}
{{- $px := include "igs.proxies" . -}}
/etc/igs/config.yaml
{{- if eq $px "required" -}}
,/etc/igs-secret/{{ .root.Values.secretFiles.key }}
{{- else if eq $px "optional" -}}
,?/etc/igs-secret/{{ .root.Values.secretFiles.key }}
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts. Args: dict root, proxies. */}}
{{- define "igs.env" -}}
- name: IGS_CONFIG
  value: {{ include "igs.configFiles" . | quote }}
{{- if .root.Values.platformCredentials.existingSecret }}
# The platform's credentials Secret arrives under its own key names
# (POSTGRES_*, KAFKA_*, VALKEY_*, S3_*, ...); igs reads them by those names.
- name: IGS_PLATFORM_CREDENTIALS
  value: "true"
{{- end }}
{{- end -}}

{{/*
igs.envFrom: platform-credentials, then the component's own credentials
Secret (REQ-008 fix 22b; a key it holds replaces the shared one for this
component only), then secretEnv. Args: dict root, credentials (a Secret
name or "").
*/}}
{{- define "igs.envFrom" -}}
{{- $root := .root -}}
{{- with $root }}
{{- if or .Values.secretEnv.existingSecret .Values.platformCredentials.existingSecret $.credentials }}
envFrom:
  {{- if .Values.platformCredentials.existingSecret }}
  - secretRef:
      name: {{ .Values.platformCredentials.existingSecret }}
  {{- end }}
  {{- with $.credentials }}
  {{- range (kindIs "slice" . | ternary . (list .)) }}
  - secretRef:
      name: {{ . }}
  {{- end }}
  {{- end }}
  {{- if .Values.secretEnv.existingSecret }}
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
  {{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* Args: dict root, proxies. */}}
{{- define "igs.volumeMounts" -}}
{{- $root := .root -}}
- name: config
  mountPath: /etc/igs
  readOnly: true
{{- if include "igs.proxies" . }}
- name: secret-files
  mountPath: /etc/igs-secret
  readOnly: true
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  mountPath: {{ $root.Values.tlsFiles.mountPath }}
  readOnly: true
{{- end }}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{/* Args: dict root, proxies. */}}
{{- define "igs.volumes" -}}
{{- include "igs.volumesFor" (dict "root" .root "proxies" .proxies "configMap" (printf "%s-config" (include "igs.fullname" .root))) -}}
{{- end -}}

{{/* The hook Jobs never mount the proxies file. Args: root. */}}
{{- define "igs.hookVolumes" -}}
{{- include "igs.volumesFor" (dict "root" . "proxies" "" "configMap" (printf "%s-hook-config" (include "igs.fullname" .))) -}}
{{- end -}}

{{- define "igs.volumesFor" -}}
{{- $root := .root -}}
{{- $px := include "igs.proxies" . -}}
- name: config
  configMap:
    name: {{ .configMap }}
{{- if $px }}
- name: secret-files
  secret:
    secretName: {{ $root.Values.secretFiles.existingSecret }}
    {{- if eq $px "optional" }}
    optional: true
    {{- end }}
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  secret:
    secretName: {{ $root.Values.tlsFiles.existingSecret }}
{{- end }}
- name: tmp
  emptyDir: {}
{{- end -}}

{{/*
igs.egressPorts renders a NetworkPolicy's ports from [{port, endPort}]
(TCP; endPort makes a range).
*/}}
{{- define "igs.egressPorts" -}}
{{- range . }}
- protocol: TCP
  port: {{ required "a port" .port }}
  {{- with .endPort }}
  endPort: {{ . }}
  {{- end }}
{{- end }}
{{- end -}}
