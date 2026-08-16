{{- define "dsh-cloud.labels" -}}
app.kubernetes.io/name: dsh-cloud
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "dsh-cloud.namespace" -}}
{{- if not (regexMatch "^[A-Za-z0-9_.-]{1,64}$" .Values.namespace) -}}
{{- fail "namespace must be a 1-64 character SQL-safe deployment identifier" -}}
{{- end -}}
{{- .Values.namespace -}}
{{- end -}}
{{- define "dsh-cloud.gatewayImage" -}}{{ .Values.images.gateway.repository }}:{{ .Values.images.gateway.tag }}{{- end -}}
{{- define "dsh-cloud.workerImage" -}}{{ .Values.images.worker.repository }}:{{ .Values.images.worker.tag }}{{- end -}}
{{- define "dsh-cloud.toolBrokerImage" -}}{{ .Values.images.toolBroker.repository }}:{{ .Values.images.toolBroker.tag }}{{- end -}}
