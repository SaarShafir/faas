# Deploying FaaS to OpenShift — a guide for a coding agent

You are deploying an audio function-as-a-service platform onto an OpenShift
cluster in a closed network. This document assumes you have the repository, a
shell, and `oc` logged in. It is written to be executed in order.

If you are receiving this as a transferred bundle rather than a clone, read
[`TRANSFER.md`](TRANSFER.md) first — it covers what did and did not come with it,
and how the container images get in.

Read [`faas-spec.md`](../faas-spec.md) once before starting. Section numbers
(§4.2, §5.2) appear throughout the code and this guide, and they are the
shortest path to understanding why something is built the way it is.

---

## 1. What you are deploying

Eleven Kafka consumer groups and a web console.

```
Audio API ──► hydrator ──► faas.audio.internal ──► 10 functions ──► faas.results
                 │              (references)            │
                 └──► object store (FLAC)               └──► faas.dlq.<function>
```

- **The hydrator** fetches source audio, transcodes it to canonical FLAC
  (16 kHz mono), puts it in the object store, and publishes a *reference* —
  never the audio — to an internal topic (§4.1, §4.2).
- **Ten functions** each consume that topic as an independent consumer group,
  fetch the FLAC, compute something, and publish a `Result` (§4.3, §6).
- **The console** is a read-only web UI over all of it, optionally with
  operator actions.

Two things are **not** built and you should not invent them: autoscaling
(build-order step 6) and a results sink. Kafka is the sink; another service
consumes `faas.results` later.

---

## 2. Rules that must not be broken

These are not style preferences. Each one has a failure mode that is hard to
diagnose after the fact.

**Never hand-write a Deployment or edit `values-functions.yaml`.**
That file is generated from `functions/*/function.yaml` by
`scripts/generate_values.py`. §8 of the spec promises that adding a function is
one PR and zero infra tickets; the first hand-written manifest ends that. If you
need a per-function change, change the declaration and regenerate.

**Never change a topic's partition count without changing the declarations, or
vice versa.** The SDK computes each result's partition itself — murmur2 over
`call_id`, so every result for a call is colocated for the aggregator (§6) — and
passes it to librdkafka explicitly. A topic with fewer partitions than
`results_topic_partitions` fails **when the first result is produced**, not at
startup. Pods will look healthy for minutes before anything goes wrong.

**Never change the `Status` enum numbering** in `proto/faas/v1/result.proto`.
`SUCCESS = 1`, not §6's `0`, deliberately: proto3 has no field presence for
enums, so a zero value of `SUCCESS` would make an unset status read as "this
call succeeded". It was changed while the topic was empty; it is a wire-breaking
migration now.

**Do not enable the console's write actions or sandbox without a decision.**
The console has no authentication. `console.sandbox=true` executes arbitrary
Python inside the console pod. In a closed network the Route may be an
acceptable boundary — that is a decision for a human, not a default.

**Do not remove `fsGroup: 0`.** OpenShift assigns a random UID in group 0. A
mounted volume arrives root-owned at 755 regardless of what the image did. This
already broke the local stack once.

---

## 3. Gather these before you start

Do not guess any of them. If a value is unknown, stop and ask.

| What | Where it goes | Notes |
|---|---|---|
| Namespace / project | `oc project` | One namespace holds everything. |
| Image registry | `registry` in values | The declarations say `registry/faas-*`; `registry` is a placeholder host. |
| Registry pull secret | `imagePullSecrets` | Needed if the registry is authenticated. |
| Kafka bootstrap | `kafka.bootstrapServers` | e.g. `kafka-kafka-bootstrap:9092`. |
| Kafka auth | `kafka.secretName` | Omit for a plaintext internal listener. |
| Is Kafka Strimzi-managed? | `topics.create` | If yes, the chart creates `KafkaTopic` resources. If no, someone must create topics with the exact partition counts. |
| Object store endpoint | `objectStore.endpoint` | Empty for real AWS. Set for MinIO/ODF — and keep `addressingStyle: path`, since a non-AWS endpoint has no per-bucket DNS. |
| Object store credentials | Secret named in `objectStore.secretName` | Must contain `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. **The chart does not create it.** |
| Bucket lifecycle rule | the bucket itself | §11 gives the store the 24h TTL and there is deliberately no reaper in the SDK. Without it, audio accumulates forever. |
| Audio API URL | `audioApi.url` | §4.1's source of recordings. Only the hydrator uses it. |
| OTel collector endpoint | `events.endpoint` | Where per-call events go. Set `events.enabled=false` if there is no collector. |
| Prometheus Operator present? | `monitoring.*` | OpenShift user-workload monitoring provides it. If absent, set both to `false` or the install fails on unknown CRDs. |

---

## 4. Deploy

### 4.1 Verify the repo state first

```bash
python -m pytest
python scripts/generate_values.py --check
```

Expected: all tests pass, and `values-functions.yaml matches 11 declarations`.

If `--check` fails, someone edited a declaration without regenerating. Run
`python scripts/generate_values.py` and review the diff before continuing — the
difference is what the cluster would otherwise have run.

### 4.2 Build and push images

```bash
python scripts/build_images.py --registry <registry>/<org> --push
```

This reads the image name and tag out of each declaration, so the tag in git and
the tag in the registry cannot drift. Twelve images: ten functions, the
hydrator, and the console.

To check what it will do without doing it: add `--dry-run`.

### 4.3 Write a cluster values file

Create `my-cluster.yaml` from the table in section 3. Start from
`deploy/chart/values.yaml`, which documents every key. Minimum:

```yaml
registry: registry.internal/faas
kafka:
  bootstrapServers: kafka-kafka-bootstrap:9092
objectStore:
  bucket: faas-audio
  endpoint: http://minio.storage.svc:9000   # omit for real AWS
  secretName: faas-object-store
audioApi:
  url: http://audio-api.audio.svc:8080
events:
  endpoint: http://otel-collector.observability.svc:4318
```

### 4.4 Create the credentials Secret

```bash
oc create secret generic faas-object-store \
  --from-literal=AWS_ACCESS_KEY_ID=... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

### 4.5 Topics

If Strimzi manages Kafka, set `topics.create: true` and `topics.cluster: <name>`,
and add the topic list printed by `generate_values.py`.

If not, someone must create these **with exactly these partition counts**:

| Topic | Partitions | Why |
|---|---|---|
| `faas.calls.raw` | 12 | §4.1 input |
| `faas.audio.internal` | 200 | must equal `results_topic_partitions` in `hydrator.yaml` |
| `faas.results` | 200 | must equal `results_topic_partitions` in every function declaration |
| `faas.dlq.<function>` × 11 | 3 | one per consumer group (§5.4) |

Run `python scripts/generate_values.py` to print the authoritative list.

### 4.6 Render, review, install

```bash
helm template faas deploy/chart \
  -f deploy/chart/values-functions.yaml -f my-cluster.yaml > /tmp/rendered.yaml
```

Read `/tmp/rendered.yaml` before applying. Confirm:

- 12 Deployments, 12 Services, 1 ConfigMap, 1 ServiceAccount (+1 for the console)
- every container image starts with your registry, **including the console's**
- `terminationGracePeriodSeconds` differs per function — 165 for the hydrator,
  75 for the 30-second functions. A uniform value means the helper is broken.
- `fsGroup: 0` on every pod

Then:

```bash
helm install faas deploy/chart \
  -f deploy/chart/values-functions.yaml -f my-cluster.yaml
```

---

## 5. Verify

Work through these in order. Do not skip to the last one.

### 5.1 Pods reach Ready

```bash
oc get pods -l app.kubernetes.io/part-of=faas
```

Expected: 12 pods `Running` and `READY 1/1`.

**Ready means something specific here**: the pod holds Kafka partitions.
A pod that is `Running` but never `Ready` has started but not joined its
consumer group — check the broker address and credentials first.

### 5.2 Probes answer

```bash
oc exec deploy/faas-duration-rms -- \
  python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8080/readyz').read())"
```

Expected: `{"ready": true, "partitions": N, ...}` with `N > 0`.

### 5.3 Metrics are being scraped

```bash
oc exec deploy/faas-duration-rms -- \
  python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:9108/metrics').read()[:400])"
```

Expected: `faas_in_flight` at minimum. Counters like `faas_processed_total` do
not exist until the first file is processed — their absence before any traffic
is normal, not a fault.

### 5.4 Config lint

```bash
oc port-forward svc/faas-console 8000:8000 &
curl -s localhost:8000/api/lint
```

Expected: `{"ok": true, "findings": []}`.

Any `error` finding here means a declaration disagrees with a topic, and it will
fail at run time rather than at startup. Fix before sending traffic.

### 5.5 One call end to end

Publish a single record to `faas.calls.raw`. Its shape is §4.1's, and the field
names the hydrator reads are `call_id` and `audio_id`:

```json
{"call_id": "smoke-001", "audio_id": "<an id the Audio API will serve>", "ingested_at": "2026-01-01T00:00:00Z"}
```

Then trace it:

```bash
curl -s localhost:8000/api/call/smoke-001 | python -m json.tool
```

Expected: `"hydrated": true`, `"complete": true`, and ten results.

If `hydrated` is false, the failure is upstream of every function — look at
`faas.dlq.hydrator`, not at a function's DLQ.

---

## 6. When something is wrong

| Symptom | Most likely cause |
|---|---|
| Pod `Running`, never `Ready` | Cannot reach Kafka, or joined no partitions. Check `bootstrapServers` and any SASL secret. |
| `CreateContainerConfigError` | The object-store Secret named in `objectStore.secretName` does not exist. |
| Pod starts, dies, restarts | Read the logs. A declaration the SDK cannot parse crash-loops at boot; so does a missing `FAAS_AUDIO_BUCKET`. |
| Permission denied writing `/scratch` | `fsGroup: 0` was removed, or a volume was added without it. |
| Works, then fails on the first result | Partition-count mismatch. `/api/lint` names it. |
| Every rollout produces duplicate results | `terminationGracePeriodSeconds` too short: work is being SIGKILLed mid-file. It must exceed `per_file_timeout_seconds` plus the drain. |
| Lag alerts never fire | The recording rule needs `clamp_min`. kafka-exporter reports `-1` for partitions a group has never committed on, and an unclamped sum over 200 partitions goes negative — a negative backlog never crosses 3600. |
| `faas_max_poll_exceeded_total` above zero | The §5.2 failure: the poll loop blocked past `max.poll.interval.ms`, the consumer was evicted, and in-flight work is being reprocessed. Serious. Investigate rather than restart. |
| Console pause returns 403 | `console.allowWrites` is false, or the Role/RoleBinding was not created. |
| Console traces are empty | `FAAS_EVENTS_URL` points nowhere, or the collector is not delivering. Fall back by unsetting it — the console then reads Kafka directly, which is slower but needs no log pipeline. |

**Diagnosis order for "a call did not produce results":**
1. Console trace (`/api/call/<id>`) — is it hydrated?
2. If not hydrated → `faas.dlq.hydrator`. The Audio API or a decode failed.
3. If hydrated but functions are missing → check whether those functions are
   `Ready`, then their lag. A function that is simply behind looks identical to
   one that lost the call, which is §7's completeness gap and why the aggregator
   is still to be built.
4. If a function dead-lettered it → the DLQ record's headers carry the error
   code, the attempt count, and the topic/partition/offset a replay would read.

---

## 7. Changing things afterwards

**Adding a function** (this is the §8 path, and the whole point):

1. `functions/<name>/function.py` and `functions/<name>/function.yaml`
2. `python scripts/generate_values.py`
3. `python scripts/build_images.py --registry ... --push --only <name>`
4. Create its DLQ topic (3 partitions)
5. `helm upgrade` — one PR, no hand-written manifest

**Changing a timeout, retry budget or in-flight depth:** edit the declaration,
regenerate, `helm upgrade`. No image rebuild — the declaration is mounted from a
ConfigMap and the pods read it from there, and the checksum annotation restarts
them.

**Scaling a function:** `replicas` in the generated values. §5.2 is explicit that
pod count is the thing to scale while `in_flight` stays shallow — do not raise
`in_flight` to get throughput.

**Rollback:** `helm rollback faas`. Safe at any time: consumer offsets are
committed in Kafka and survive pod replacement, so a rolled-back function
resumes where it stopped. In-flight files are redelivered, which is at-least-once
working as designed (§5.3).

---

## 8. Deliberately absent — do not improvise these

If any of these is needed, raise it rather than building it ad hoc.

- **Autoscaling.** Build-order step 6, and the reason the lag metrics exist.
  A KEDA `ScaledObject` on consumer lag is the intended shape and drops in
  without touching the functions.
- **CI.** Nothing runs the tests or `generate_values.py --check` automatically,
  so generated values can drift from the declarations silently.
- **NetworkPolicy.** Nothing restricts egress.
- **Console authentication.** None. The Route is the only boundary.
- **The aggregator and `call_complete` (§7).** Nothing tells a consumer that all
  functions have finished for a call. The console's "complete" is its own
  opinion, computed from the declarations, not a marker on a topic.
- **The deletion path (§10).** Erasure by `call_id` needs a registry of every
  `(function, version)` that ever ran. The console derives one from live
  consumer groups, which expire — that is a stopgap and does not satisfy §10.

---

## 9. Escalate rather than guess

Stop and ask a human when:

- A verification step in section 5 fails and the cause is not in section 6.
- Any change would alter a topic's partition count, the `Status` enum, or the
  §6 record key — all three are wire-breaking with data on the topics.
- You are about to enable `console.allowWrites` or `console.sandbox`.
- A declaration and the running cluster disagree and it is not obvious which is
  correct.
- The object store has no lifecycle rule. Deploying without one is a decision
  about unbounded storage growth, not an oversight to paper over.
