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

{{- define "igs.configFiles" -}}
{{- if .Values.secretFiles.existingSecret -}}
/etc/igs/config.yaml,/etc/igs-secret/{{ .Values.secretFiles.key }}
{{- else -}}
/etc/igs/config.yaml
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts. */}}
{{- define "igs.env" -}}
- name: IGS_CONFIG
  value: {{ include "igs.configFiles" . | quote }}
{{- end -}}

{{- define "igs.envFrom" -}}
{{- if .Values.secretEnv.existingSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
{{- end }}
{{- end -}}

{{- define "igs.volumeMounts" -}}
- name: config
  mountPath: /etc/igs
  readOnly: true
{{- if .Values.secretFiles.existingSecret }}
- name: secret-files
  mountPath: /etc/igs-secret
  readOnly: true
{{- end }}
{{- if .Values.tlsFiles.existingSecret }}
- name: tls-files
  mountPath: {{ .Values.tlsFiles.mountPath }}
  readOnly: true
{{- end }}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "igs.volumes" -}}
{{- include "igs.volumesFor" (dict "root" . "configMap" (printf "%s-config" (include "igs.fullname" .))) -}}
{{- end -}}

{{- define "igs.hookVolumes" -}}
{{- include "igs.volumesFor" (dict "root" . "configMap" (printf "%s-hook-config" (include "igs.fullname" .))) -}}
{{- end -}}

{{- define "igs.volumesFor" -}}
{{- $root := .root -}}
- name: config
  configMap:
    name: {{ .configMap }}
{{- if $root.Values.secretFiles.existingSecret }}
- name: secret-files
  secret:
    secretName: {{ $root.Values.secretFiles.existingSecret }}
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  secret:
    secretName: {{ $root.Values.tlsFiles.existingSecret }}
{{- end }}
- name: tmp
  emptyDir: {}
{{- end -}}
