# faas-sdk

Steps 1–4 of the build order in [`faas-spec.md`](faas-spec.md): the SDK every
audio function inherits, the protobuf schemas it speaks, the hydrator that
feeds it, and the reference function that proves the path end to end. The
results sink, autoscaling, the aggregator and the deletion path come after this.

Requires `ffmpeg` on PATH. Without it the transcode and contract tests skip
rather than fail — but they are the ones worth having, so install it.

```bash
python -m venv .venv && ./.venv/Scripts/python -m pip install -e ".[dev]"
./.venv/Scripts/python -m pytest
./.venv/Scripts/python scripts/gen_proto.py   # only after editing proto/
```

## What a function author writes

```python
from faas_sdk import FunctionResult, run

class DurationRms:
    function_id = "duration_rms"
    function_version = "1.0.0"

    def process(self, ref, audio):
        samples = audio.samples()
        rms = float((samples ** 2).mean() ** 0.5)
        return FunctionResult(payload=json.dumps({"rms": rms}).encode(),
                              schema_version="1",
                              content_type="application/json")

run(DurationRms())
```

Kafka, offsets, retries, the DLQ, the results envelope, the 256 KB claim check
and the metrics are all on the other side of that line.

## Layout

| Module | Spec | What it owns |
|---|---|---|
| `runner.py` | §5.2 §5.3 §5.4 | the poll loop, backpressure, retry/DLQ routing |
| `codec_protobuf.py` | §4.2 §6 | the wire format; `codec.py` keeps JSON for local dev |
| `offsets.py` | §5.3 | low-water-mark commit ledger |
| `pool.py` | §5.2 | bounded in-flight pool (process-backed or inline) |
| `dlq.py` | §5.4 | per-function DLQ, input preserved for replay |
| `results.py` | §6 | envelope, composite key, claim check |
| `partitioner.py` | §6 | murmur2, so partitioning is on `call_id` alone |
| `config.py` | §8 | the function declaration |
| `metrics.py` | §5.5 | the six metrics, emitted by the SDK not by authors |
| `audio.py` | §5.1 | lazy S3 fetch + FLAC decode |
| `kafka.py` `objectstore.py` | §11 | the only places librdkafka and boto3 appear |
| `testing.py` | — | fakes, shipped so functions can be tested without a broker |

`ports.py` holds the protocols the runner talks to. The core logic has no
dependency on a broker, an object store or a subprocess, which is why the
suite runs in under a second.

## The hydrator

`src/faas_hydrator/` — §4.1. Parse metadata, GET audio by id, transcode to
canonical FLAC, PUT to `{call_id}.flac`, publish the reference, commit.

It runs on the SDK's runner rather than its own loop. A transcode is seconds of
work between polls, so it has the same §5.2 problem a function does, and
growing a second copy of the hardest code in the system to solve it twice would
be the wrong trade. Only the two ends differ: `JsonSourceDecoder` replaces the
reference decoder, and `ReferenceEmitter` publishes to the internal topic
instead of the results topic. That took one new constructor argument on
`FunctionRunner` (`decoder=`) and nothing else.

Two ordering rules hold the pipeline together, both tested:

- **The object lands before the reference is published.** The PUT happens inside
  `process()`, and the emitter only ever publishes a reference the runner got
  back from a completed call — so a failed PUT publishes nothing. A reference
  ahead of its object is a dead key for any function fast enough to read it,
  surfacing as unexplained transient failures across unrelated functions.
- **The offset commits after the reference is published**, via the SDK's ledger.
  A crash in between replays the call, and the deterministic key makes that
  harmless.

`Transcoder` verifies what ffmpeg actually produced by reading the FLAC
STREAMINFO header, rather than trusting the flags it passed. That catches the
non-seekable-output bug for free: ffmpeg writing to a pipe cannot seek back to
patch `total_samples`, so the reference would claim every call is 0 seconds
long. Both ends therefore go through files, not pipes — which is also what makes
formats needing a header seek (MP4's moov atom) work at all.

## The reference function, and the contract test

`functions/duration_rms/` — §13 step 4. Trivial by design: decode, measure RMS
and peak, emit JSON. Adding a function is this directory — `function.yaml` plus
`function.py`, one PR and zero infra tickets (§8). It contains no Kafka, no
offsets, no retries, no S3 and no serialization, which is the §5.1 claim made
literal.

`tests/functions/test_contract.py` is the part that matters. The spec says not
to build functions 2..N until step 4 is stable and calls the reference function
the contract test, so it runs the real thing: real ffmpeg transcoding real
audio, a real object-store round trip, real libsndfile decoding, the real
protobuf wire format, and both runners with their real ledgers and DLQ routing.
Only Kafka and S3 are fakes, and both are fakes of interfaces the SDK owns. The
hydrator's output bytes are fed to the function verbatim, so any disagreement
about the wire format surfaces there.

A call goes in as an Audio API response and comes out as a `Result`, and the
measurements match what went in — checked twice over: against a signal of
stated amplitude (exact), and against the decoded input itself (fidelity across
44.1 kHz stereo WAV → 16 kHz mono FLAC → S3 → libsndfile). Dropping `-ar` from
the transcoder fails ten of these.

## The three things this exists to get right

**Poll never blocks on work (§5.2).** Every loop iteration is bounded by the
poll timeout no matter what the pool is doing. Backpressure is `pause`/`resume`
on the assignment, never "stop calling poll" — skipping the poll is precisely
what gets the consumer evicted mid-file and starts the reprocess-forever loop.

**Commits are the low-water mark (§5.3).** With N files in flight completing out
of order, the committable offset is the lowest *incomplete* one. Committing the
highest completed offset silently drops everything unfinished below it on crash.
`test_offsets.py` pins this; mutating `min(pending)` out of `offsets.py` fails
four tests.

**A poison message cannot accrue lag (§5.4).** Bounded retries with exponential
backoff, then the input goes to a per-function DLQ *and* a `FAILED` result goes
to the results topic, and the offset is committed. Both records, always —
without the second, downstream cannot distinguish "no result yet" from "no
result ever" and every consumer invents its own timeout heuristic.

## Schemas

`proto/faas/v1/` holds `AudioReference` (§4.2) and `Result` (§6), field numbers
matching the spec exactly and pinned by tests — field numbers *are* the wire
format, and §10 freezes them once published. Generated Python is committed to
`src/faas/`, so a function image build never needs protoc.

`buf generate` is the declared toolchain (§11); `scripts/gen_proto.py` uses it
when the binary is present and falls back to protoc from `grpcio-tools`
otherwise. Buf lint runs `DEFAULT` minus `ENUM_VALUE_PREFIX`, and
breaking-change detection is `WIRE_JSON` — renames are allowed, wire changes are
not.

**The one deliberate deviation from §6: `SUCCESS` is 1, not 0.** proto3 has no
field presence for enums, so with the spec's numbering an unset `status` decodes
as `SUCCESS` — a partial write, a producer bug or a mis-built message reads as
"this call succeeded", the wrong failure direction for the one field that gates
whether downstream trusts a payload. The zero value is `STATUS_UNSPECIFIED` and
the named states start at 1. Both codecs reject it in either direction: an
unspecified or unrecognised status decodes to `DecodeError` (poison, straight to
the DLQ) rather than a guess, and the SDK will not encode one. Done while the
topic was empty; it is a wire-breaking migration now.

## Broker tests

`tests/kafka/` — 12 tests against a real Apache Kafka broker in KRaft mode,
excluded from the default run:

```bash
pytest -m kafka
```

Apache Kafka rather than Redpanda because the behaviour under test *is* the
group coordinator, which is where a reimplementation could differ; the
deployment target is AMQ Streams, which is Apache Kafka. The container is
managed directly rather than through testcontainers, which raced its own start
script here — doing it by hand also removes the chicken-and-egg on advertised
listeners, since the host port is chosen before the broker boots.

**Every assertion is paired with a negative control.** "The consumer was not
evicted" passes trivially if the scenario never stressed the poll interval, so
each case runs twice — once with the design, once with the naive version — and
the naive one must actually fail. The naive poll loop really is evicted and
reprocesses its file; committing the highest completed offset really does
destroy three files.

These found three things the fakes could not:

1. **`ConfluentConsumer.poll` crashed the pod on eviction.** It raised
   `RuntimeError` for any non-EOF error, but `_MAX_POLL_EXCEEDED` is an event,
   not a failure — librdkafka rejoins by itself. Turning a recoverable eviction
   into a crash drops in-flight work and makes a bad situation worse. Now
   counted (`max_poll_exceeded`) and logged loudly, since a non-zero count means
   §5.2 is happening.
2. **`bootstrap.py` defaulted to a pool that reintroduces the §5.2 bug.**
   `InlineWorkerPool` runs `process()` synchronously inside `submit()`, so work
   happens on the poll thread — structurally the naive loop, wearing the SDK's
   clothes. Against a real broker it is evicted. The default is now
   `ProcessWorkerPool`, which is why `run()` takes the function *class* rather
   than an instance: the worker constructs its own, so weights and clients are
   never pickled across. The hydrator keeps an inline pool deliberately — its
   work is bounded by ffmpeg's own subprocess timeout, so the block is provably
   inside `max.poll.interval.ms`, whereas a function's timeout is enforced
   *between* poll iterations and so cannot fire while inline work blocks.
3. **The murmur2 test was comparing against the wrong hash.** The implementation
   was right; the test used librdkafka's `consistent` partitioner, which is
   CRC32. Only `murmur2`/`murmur2_random` are Java-Producer-compatible, and Java
   is the ecosystem this has to interop with. It now agrees on all 69 keys.

## Deliberate deviations from the spec

- **`process()` returns `FunctionResult`, not §5.1's `Result`.** §6's `Result` is
  the wire envelope, which the SDK stamps with offsets, attempts and timestamps.
  Returning it from `process` would leak the envelope into every function and
  erode the §6 split between stable envelope and opaque payload.
- **Source is 3.10-compatible** though `requires-python` is `>=3.12`, so the
  suite runs on whatever interpreter is to hand.

## Known limits

- **Timeout cancellation is best-effort for a running job.** `ProcessWorkerPool`
  cannot interrupt a worker mid-file without killing the process; it abandons the
  outcome so the runner can retry or DLQ, but the worker keeps burning CPU until
  it returns. One more reason the spec prefers shallow in-flight depth with a pod
  as the failure unit (§5.2).
- **A retry in backoff holds its partition's low-water mark.** Correct — the file
  is unfinished — but a long backoff on one file delays commits for everything
  behind it on that partition. Worth watching once real retry rates exist.
- **`AudioHandle` decode is untested end-to-end.** It needs libsndfile; tests use
  `testing.StubAudioHandle`. The first real coverage lands with the reference
  function (build order step 4).
- **Consumer lag is not emitted yet.** `kafka-exporter` feeds the autoscaler and
  is the source of truth (§5.5, §11); the SDK-side gauge is a cross-check that
  needs a broker to be worth writing.
- **Decoding is lossy for unknown fields.** Messages land in dataclasses, so a
  field added by a newer producer survives the read but not a re-encode. Fine
  for the SDK — the runner only decodes references and only encodes results it
  built itself — but anything that proxies a message through must work on the
  protobuf object, not the dataclass.
- **Buf lint and breaking-change detection do not run automatically.** Both pass
  locally against buf 1.72, and generated code now comes from `buf generate`
  rather than the protoc fallback, but nothing enforces either on a push. They
  need to run in CI to be worth anything.
- **A call that fails hydration is invisible downstream.** No reference is
  published, so no function ever sees it and the aggregator (§7) never expects
  it. The DLQ holds the input for replay — the spec's answer — but nothing
  distinguishes "not hydrated yet" from "never will be", and a dead-letter topic
  is not something downstream can reasonably watch. Closing it means a
  hydration-failure topic the aggregator honours; that is a design decision, not
  something to invent here. See `ReferenceEmitter.emit_failure`.
- **Rebalance behaviour is still unproven.** The broker suite covers eviction and
  commits, but not a real cooperative-sticky rebalance: two consumers in a group,
  partitions moving, `_on_revoke` draining in-flight work under a live
  coordinator. That is the largest remaining gap.
- **The input topic's schema is guessed.** §4.1 says "parse metadata, extract
  audio id" and defines no format. `JsonSourceDecoder` takes the field names as
  arguments for that reason — it will need pointing at whatever upstream
  actually sends.
