# FaaS — Audio Function-as-a-Service Platform

Implementation spec for a coding agent. Read this end-to-end before writing code.

---

## 1. Purpose

A pluggable processing layer that runs many independent audio algorithms ("functions") over a
high-volume audio stream. Adding a new function must require no infrastructure work. A function
that falls behind must not affect any other function.

**Non-goal:** this is not FaaS in the ephemeral-invocation sense. Functions are long-lived
consumer processes. The name refers to the plugin model, not the runtime.

---

## 2. Scale parameters

Design to these numbers. They are load-bearing.

| Parameter | Value |
|---|---|
| Ingest rate | ~5,000 audio-hours per wall-clock hour |
| Average file length | 5 minutes |
| Message rate | ~17 files/sec |
| Max tolerable lag (slowest function) | a few hours |
| Object store retention | 24h |
| Internal topic retention | 48h (must exceed object TTL) |
| Steady-state audio storage | ~7 TB (FLAC) |
| Aggregate read bandwidth | ~800 MB/s at 10 functions |
| Per-file size (FLAC 16kHz/16-bit mono) | ~5 MB |

Kafka throughput is a non-issue at 17 msg/sec. Every sizing decision below is about
**parallelism and lag isolation**, not throughput.

---

## 3. Architecture

```
  input topic (metadata)
         │
         ▼
   ┌───────────┐   get-by-id    ┌──────────┐
   │ Hydrator  │───────────────▶│ Audio API│  (serves canonical FLAC)
   └───────────┘                └──────────┘
         │  PUT {call_id}.flac, unchanged
         ▼
   ┌───────────────┐
   │  S3 (24h TTL) │◀──────────┐
   └───────────────┘           │ GET
         │                     │
         │ publish reference   │
         ▼                     │
   internal topic (48h) ───────┼──── consumer group per (function, version)
         │                     │
         ▼                     │
   ┌──────────┐  ┌──────────┐  │
   │ func A   │  │ func B   │──┘   ... N functions
   └──────────┘  └──────────┘
         │             │
         └──────┬──────┘
                ▼
         results topic ──▶ sink service ──▶ result store
```

**Claim-check pattern.** Audio never travels through Kafka. The hydrator reads metadata once,
fetches audio once, and publishes a *reference*. Functions read audio from S3 independently.
This is what allows independent lag while preserving read-once semantics.

---

## 4. Components

### 4.1 Hydrator

Single consumer group on the input topic. Stateless. Few replicas — at 17 files/sec it will
never be the bottleneck. Keep it dumb.

Per message:
1. Parse metadata, extract audio id.
2. `GET` audio by id from the Audio API, which serves canonical FLAC —
   **16 kHz, 16-bit, mono** — already. Encoding happens upstream of this
   platform; the hydrator does not transcode.
3. Verify the fetched bytes are canonical FLAC by reading the STREAMINFO
   header (sample rate, channels, bit depth, a known duration). Anything else
   is poison, not a retry: the Audio API will return the same bytes on the
   third attempt, and this is the one place an upstream encoding regression is
   caught before it reaches every function at once.
4. `PUT` those bytes, unchanged, to S3 at deterministic key `{call_id}.flac`.
5. Publish reference to the internal topic, with sample rate/channels/duration
   read from the same STREAMINFO header rather than assumed.
6. Commit offset.

**Do not embed metadata in FLAC tags.** Metadata lives in the Kafka reference only. One source
of truth; the metadata schema can evolve without rewriting objects.

The SDK only ever decodes canonical FLAC via `soundfile`/libsndfile, and now
nothing in this platform produces any other form: whatever zoo of input
formats a call started as, upstream of the Audio API is where it stops.

### 4.2 Internal topic

- Partitions: **200–500**. This bounds *process* count, not concurrency (see §5.2).
- Retention: **48h**, strictly longer than the S3 TTL. This is deliberate: a lagging consumer
  should hit a live offset with a dead object key (recoverable — re-fetch) rather than an
  evicted offset (silent skip).
- Key: `call_id`.

Reference message:

```protobuf
message AudioReference {
  int32  envelope_version = 1;
  string call_id          = 2;
  string object_key       = 3;   // {call_id}.flac
  int32  sample_rate      = 4;
  int32  channels         = 5;
  double duration_seconds = 6;
  google.protobuf.Timestamp ingested_at  = 7;  // from source metadata
  google.protobuf.Timestamp hydrated_at  = 8;
  bytes  source_metadata  = 9;   // opaque passthrough of original metadata
}
```

### 4.3 Functions

One consumer group per function, **group id = `{function_id}:{function_version}`**. This gives
independent offsets, independent lag, independent scaling, and independent DLQ — natively, with
no custom lag bookkeeping. It also makes shadow deploys fall out for free: v2 runs against live
traffic alongside v1, writing to its own results namespace, and you diff before cutover.

### 4.4 Results topic and sink

Functions do not own database connections. They emit to a single results topic; a sink service
lands the data. See §6.

---

## 5. SDK — build this first

Every function inherits the SDK. The poll/work semantics and commit correctness are the parts
that are miserable to change later. **Build the SDK before the hydrator.**

### 5.1 Function contract

```python
class Function(Protocol):
    function_id: str
    function_version: str

    def process(self, ref: AudioReference, audio: AudioHandle) -> Result: ...
```

`AudioHandle` lazily fetches from S3 and decodes FLAC. Function authors never see Kafka, S3,
offsets, retries, or serialization.

### 5.2 Poll/work decoupling — CRITICAL

Per-file processing takes seconds to minutes. A naive poll loop exceeds
`max.poll.interval.ms`, the broker evicts the consumer mid-file, a rebalance fires, and another
consumer reprocesses the same file — forever, if the function is slow enough. This is the single
most likely way this project fails in production.

Required design:
- Poll thread only polls and heartbeats. It never blocks on work.
- Work goes to a bounded in-flight pool.
- Offsets commit **only on completion**.

**In-flight concurrency is independent of partition count.** 200 partitions × 20 in-flight =
4,000 concurrent files without repartitioning. Partitions bound process count; the pool bounds
concurrency.

**The pool must not be threads.** Decode plus inference is CPU-bound and GIL-blocked. Use a
process pool, or — preferred default — **one consumer per pod with small in-flight depth, and
let the autoscaler scale pod count.** This makes the failure unit a pod and makes commit
reasoning far simpler.

### 5.3 Commit semantics

With N files in flight they complete out of order. **Commit the low-water mark, never the
latest offset** — committing the latest silently skips unfinished files on crash.

This yields at-least-once delivery, so duplicates on restart are expected and fine: results are
idempotent on `(call_id, function_id, function_version)`.

### 5.4 Failure handling

| Failure | Behavior |
|---|---|
| Transient error | Bounded retries with backoff, then DLQ |
| Poison message | Straight to per-function DLQ, **commit the offset** |
| Dead object key (lag > TTL) | Re-fetch from Audio API, rate-limited separately from live hydration |
| Timeout | Per-file timeout from config; treat as failure |

A poison message must never accrue unbounded lag while every other function looks healthy.
DLQ is per function.

Note the distinction from §6: the DLQ holds the **input message** for replay. A `FAILED`
record on the results topic records that the call was **attempted and produced no output**.
Emit both. Without the latter, downstream cannot distinguish "no result yet" from "no result
ever," and every consumer invents its own timeout heuristic.

### 5.5 Observability — emitted by the SDK, not by function authors

If authors instrument, metrics will be inconsistent and there will be no cross-function view.
The SDK emits, for every function, for free:

- consumer lag
- per-file latency histogram
- throughput multiple vs realtime
- DLQ rate
- in-flight depth
- retry count

OpenTelemetry → Prometheus. Consumer lag additionally via `kafka-exporter`, which also feeds
the autoscaler.

---

## 6. Results envelope

Envelope is stable and strongly typed. Payload is opaque and function-owned. This split is the
whole design: a function can ship a new output shape without a topic-wide schema migration.

```protobuf
message Result {
  int32  envelope_version  = 1;
  string call_id           = 2;
  string function_id       = 3;
  string function_version  = 4;

  enum Status { SUCCESS = 0; FAILED = 1; SKIPPED = 2; }
  Status status            = 5;
  Error  error             = 6;   // null unless FAILED

  string payload_schema_version = 7;
  string payload_content_type   = 8;
  oneof payload_body {
    bytes  payload      = 9;   // inline, < 256 KB
    string payload_ref  = 10;  // S3 key, >= 256 KB
  }

  string input_object_key = 11;
  int64  input_offset     = 12;
  int32  attempt          = 13;
  google.protobuf.Timestamp ingested_at  = 14;
  google.protobuf.Timestamp started_at   = 15;
  google.protobuf.Timestamp completed_at = 16;
}

message Error {
  string code      = 1;
  string message   = 2;
  bool   retryable = 3;
}
```

**Key = `{call_id}:{function_id}:{function_version}`.** Not `call_id` alone — if log compaction
is ever enabled with `call_id` as the key, the last writer wins and silently destroys every
other function's result for that call. The composite key also gives idempotent dedup for free.

**Partition on `call_id` alone**, even though the key is composite, so all results for a call
land on one partition and the aggregator (§7) needs no shuffle.

**Claim-check large payloads.** Transcripts and embedding sets exceed Kafka's ~1 MB default.
SDK rule: inline under 256 KB, else write to S3 and set `payload_ref`. Function authors call
the same API either way and never think about it.

**Payload schemas live in a registry keyed by `function_id`.** The envelope is validated
topic-wide; payloads per function. Otherwise the envelope becomes a union type that every
function must ship a change to.

---

## 7. Completeness

Nothing in §6 tells a consumer that all functions have finished for a call. **Do not leave this
to consumers** — they will each build a different timeout and disagree about coverage.

Build an aggregator that reads the active-function registry, waits for the expected set (with
timeout), and emits an explicit `call_complete` marker.

---

## 8. Function declaration

"Add a function" must be one PR and zero infra tickets. Config as code, one repo.

```yaml
function_id: speaker_diarization
function_version: "2.1.0"
image: registry/faas-speaker-diarization:2.1.0
resources:
  cpu: 4
  memory: 8Gi
  gpu: 1            # omit for CPU-only
in_flight: 4
per_file_timeout_seconds: 120
retry_budget: 3
dlq_topic: faas.dlq.speaker_diarization
payload_schema: schemas/speaker_diarization/v2.proto
```

**Onboarding contract:** a function must sustain **≥25x realtime per core**. "Faster than
realtime" is not sufficient — concurrency required is `5000 / speedup`, so a 2x function needs
~2,500 concurrent files while a 25x function needs ~200. Anything below the floor requires a
capacity review before it ships.

---

## 9. Chaining

Flat and independent by default. Someone will eventually want function B to consume A's output
(diarization → transcription → analytics).

Rules if enabled:
- A function may subscribe to the results topic as an input.
- Dependencies are **declared explicitly**; the DAG is never implicit.
- Depth is capped.
- A chained function's lag is its own **plus** its upstream's. This reintroduces coupled lag —
  it is a deliberate exception to the isolation guarantee, not a loophole.

---

## 10. Data deletion

Audio at this volume is PII-bearing. Erasure must reach S3, the results topic, and every
derived output. **This is genuinely awful to retrofit — design it in now.**

- S3: the 24h lifecycle TTL handles raw audio almost entirely.
- Kafka cannot delete a record in place. Use log compaction with tombstones on the composite
  key, or a deletion-marker topic that every sink honors.
- Erasure by `call_id` requires tombstoning every `(function, version)` pair that ever ran —
  so maintain a registry of all historical function versions.
- Derived results are the hard part; they live indefinitely.

---

## 11. Stack

| Concern | Choice |
|---|---|
| Language | Python 3.12+ |
| Kafka client | `confluent-kafka-python` (librdkafka) — better rebalance handling than aiokafka/kafka-python |
| Orchestration | OpenShift (on-prem) |
| Autoscaling | Custom Metrics Autoscaler operator (KEDA), scaling on Kafka consumer-group lag |
| Kafka | AMQ Streams (Strimzi) |
| Object store | S3-compatible; lifecycle rule for 24h TTL — do not write a reaper |
| Schema | Protobuf + Buf |
| Audio decode (SDK) | `soundfile` / libsndfile |
| Audio encode (upstream of the Audio API — not this platform) | ffmpeg or equivalent |
| Metrics | OpenTelemetry → Prometheus + Grafana (enable OpenShift user workload monitoring; off by default) |
| Packaging | One base image with SDK preinstalled; `uv` for deps. Function image = base + algorithm |

Autoscaling on lag is the load-bearing platform choice. It is exactly the stated requirement:
each function scales independently, a slow one grows its own replica count, nothing else
notices. Without it, replica counts are hand-tuned per function forever.

### OpenShift gotchas — these will bite on first deploy

1. **Random UID.** The default SCC runs containers as an arbitrary UID, not the Dockerfile's.
   Anything writing a scratch path — the console sandbox's temp files, model caches, HuggingFace's `~/.cache` —
   fails unless the directory is group-writable by GID 0. Set `HF_HOME` explicitly to an
   emptyDir, and in the image: `chgrp -R 0 /path && chmod -R g=u /path`. This breaks
   approximately every ML container.
2. **GPU** requires the NVIDIA GPU Operator plus Node Feature Discovery, and on RHCOS the
   driver toolkit path. Budget real time; it is not a `helm install`. Use a separate node pool
   with taints.
3. **Shared models:** if several functions call the same model, put it behind Triton rather
   than loading weights into every worker. Otherwise GPU memory becomes the scaling limit long
   before lag does.
4. **No managed anything.** You own Kafka upgrades, broker disks, etcd. This is a standing
   argument against adding a second stateful system.

---

## 12. Operations

- Alert at **1h** lag, page at **3h**. With a 24h object TTL this is a real remediation window,
  not a cliff.
- Backfill (new function version over history) re-fetches from the Audio API. **Rate-limit the
  backfill path separately from live hydration** so a backfill cannot starve the live stream.
- Capacity is finite and on-prem: ~7 TB steady-state and ~800 MB/s aggregate east-west read.
  Size storage node NICs accordingly — this is the most likely place the design meets a wall.

---

## 13. Build order

1. **SDK** — poll/work decoupling, in-flight pool, low-water-mark commits, DLQ, metrics.
   Everything inherits this and it is the hardest thing to change later.
2. Protobuf schemas — `AudioReference`, `Result`, Buf setup.
3. Hydrator.
4. One trivial reference function (e.g. duration + RMS) end-to-end.
5. Results sink service.
6. Autoscaling on lag.
7. Aggregator + `call_complete`.
8. Deletion path.

Do not build functions 2..N until step 4 is stable. The reference function is the contract test.
