{{/*
Shared naming and the two derived values worth explaining.
*/}}

{{- define "faas.name" -}}
{{- default .Chart.Name .Values.nameOverride -}}
{{- end -}}

{{- define "faas.labels" -}}
app.kubernetes.io/name: {{ include "faas.name" $ }}
app.kubernetes.io/managed-by: {{ $.Release.Service }}
app.kubernetes.io/part-of: faas
helm.sh/chart: {{ printf "%s-%s" $.Chart.Name $.Chart.Version }}
{{- end -}}

{{/*
The image, with the declaration's placeholder host replaced by the real
registry. The declarations say `registry/faas-duration-rms:1.0.0`; `registry` is
a placeholder, and everything after the first slash is the part under review.
*/}}
{{- define "faas.image" -}}
{{- $image := .function.image -}}
{{- if .root.Values.registry -}}
{{- $tail := (splitList "/" $image) | last -}}
{{- printf "%s/%s" (trimSuffix "/" .root.Values.registry) $tail -}}
{{- else -}}
{{- $image -}}
{{- end -}}
{{- end -}}

{{/*
How long the pod gets to drain on SIGTERM.

Not a constant, and this is the detail most likely to be got wrong: the runner
stops polling, lets in-flight work finish, commits, and exits. A file may still
be running for up to per_file_timeout_seconds, and a rebalance may take up to
rebalance_drain_seconds on top. OpenShift's default grace period is 30s, so
anything with a two-minute timeout -- the hydrator, for one -- would be SIGKILLed
mid-file on every rollout, and every rollout would produce a burst of redelivered
work.
*/}}
{{- define "faas.terminationGrace" -}}
{{- $timeout := .function.perFileTimeoutSeconds | float64 -}}
{{- $drain := .function.rebalanceDrainSeconds | default 30.0 | float64 -}}
{{- add (int (ceil (add $timeout $drain))) 15 -}}
{{- end -}}
