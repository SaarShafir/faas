# FaaS — build status

Working notes against [`faas-spec.md`](faas-spec.md). [`README.md`](README.md)
explains how the thing works; this is what is done, what was decided, and what
comes next.

Last updated: 2026-08-13.

The P0 rebalance gap is closed: `tests/kafka/test_rebalance.py` moves
partitions between two consumers under a live coordinator with committed and
uncommitted work in flight, and proves the §5.2 drain commits what finished,
cancels what did not, and redelivers exactly the unfinished file to the new
owner -- nothing lost, nothing duplicated.

---

## Where we are in the build order (§13)

| # | Step | State |
|---|---|---|
| 1 | SDK — poll/work decoupling, in-flight pool, low-water-mark commits, DLQ, metrics | **done** |
| 2 | Protobuf schemas — `AudioReference`, `Result`, Buf setup | **done** |
| 3 | Hydrator | **done** |
| 4 | One trivial reference function, end to end | **done** |
| 5 | Results sink service | not started |
| 6 | Autoscaling on lag | not started |
| 7 | Aggregator + `call_complete` | not started |
| 8 | Deletion path | not started |

The spec's gate — "do not build functions 2..N until step 4 is stable" — is met:
`tests/functions/test_contract.py` runs a call end to end through real ffmpeg,
a real object-store round trip, real libsndfile decoding and the real protobuf
wire format.

## Tests

210 total. The unit suite runs in ~7s and is the default; the broker and object
store suites need Docker and run in ~4min combined.

```bash
pytest                 # 191 unit tests
pytest -m kafka        # 13 broker tests, needs Docker
pytest -m minio        # 6 object store tests, needs Docker
```

| Area | Tests |
|---|---|
| SDK core (offsets, pool, runner, failure handling, results, config, codecs) | 95 |
| Hydrator (flac, transcode, metadata, hydrator, pipeline) | 72 |
| Reference function + contract | 24 |
| Broker (poll interval, commits, partitioner, cooperative rebalance) | 13 |
| Object store (MinIO: round trip, error mapping, claim check, dead key, real-store contract) | 6 |

## Environment

Everything below is installed and working on this machine.

- `.venv` — pytest, ruff, PyYAML, protobuf, grpcio-tools, soundfile, numpy,
  confluent-kafka, boto3, testcontainers (unused, see below).
- **ffmpeg 9.0** via winget (`Gyan.FFmpeg`). On PATH for new shells; older shells
  need a restart. Without it the transcode and contract tests skip rather than
  fail — but they are the ones worth having.
- **Docker** required only for `pytest -m kafka` and `pytest -m minio`. Pulls
  `apache/kafka:latest` and `minio/minio:latest`.
- Source is 3.10-compatible although `requires-python` says `>=3.12`, so the
  suite runs on whatever interpreter is to hand. Local Python is 3.10.

---

## Decisions worth knowing about

**The hydrator runs on the SDK's runner, not its own loop.** A transcode is
seconds of work between polls — the same §5.2 problem a function has. Only the
two ends differ: `JsonSourceDecoder` replaces the reference decoder and
`ReferenceEmitter` publishes to the internal topic. Cost: one constructor
argument (`decoder=`) on `FunctionRunner`.

**`process()` returns `FunctionResult`, not §5.1's `Result`.** §6's `Result` is
the wire envelope the SDK stamps with offsets, attempts and timestamps.
Returning it from `process` would leak the envelope into every function and
erode the §6 envelope/payload split.

**Wire types are dataclasses behind a `Codec` seam.** `ProtobufCodec` is the
production path and `bootstrap.py` defaults to it; `JsonCodec` stays for local
dev. `tests/test_codec_swap.py` runs the whole runner over both.

**`run()` takes the function class, not an instance** — see bug 2 below.

**The object store got the Kafka treatment.** The contract test's S3 is a fake
of an interface the SDK owns, so the wire protocol had no real coverage until
`tests/objectstore/` (MinIO in Docker, same manual-container pattern as the
broker suite). It pins the two mappings that route production behaviour:
real `NoSuchKey` → `ObjectMissingError` (dead-key re-fetch path), everything
else → `TransientError`, plus the 256 KB claim check and the dead-key
re-fetch over real bytes.

## Bugs found by testing, and fixed

Three came out of the broker suite, one out of the hydrator work. Recorded
because the reasoning matters more than the diffs.

1. **`ConfluentConsumer.poll` crashed the pod on eviction.** It raised
   `RuntimeError` for any non-EOF error, but `_MAX_POLL_EXCEEDED` is an event,
   not a failure — librdkafka rejoins by itself. Turning a recoverable eviction
   into a crash drops in-flight work. Now counted (`max_poll_exceeded`) and
   logged loudly.
2. **`bootstrap.py` defaulted to a pool that reintroduced the §5.2 bug.**
   `InlineWorkerPool` runs `process()` synchronously inside `submit()`, so work
   happens on the poll thread. Against a real broker it is evicted. Default is
   now `ProcessWorkerPool`. The hydrator keeps an inline pool deliberately: its
   work is bounded by ffmpeg's own subprocess timeout, so the block is provably
   inside `max.poll.interval.ms`, whereas a function's timeout is enforced
   *between* poll iterations and cannot fire while inline work blocks.
3. **The murmur2 test compared against the wrong hash.** Implementation was
   right; librdkafka's `consistent` partitioner is CRC32, and only
   `murmur2`/`murmur2_random` are Java-compatible. Agrees on all 69 keys now.
4. **The hydrator's store-before-publish invariant had no real test.** The one
   that existed could not fail — moving the PUT inside `process()` does not
   break anything, because the guarantee is structural. The meaningful version
   (a *failed* PUT publishes nothing) was missing and is now in
   `tests/hydrator/test_pipeline.py`.

Load-bearing logic was mutation-checked rather than trusted: removing
`min(pending)` from the ledger fails 4 tests, disabling backpressure fails 3,
dropping `-ar` from the transcoder fails 10.

---

## Next

### Decide before anything else ships

**The `Status` enum zero value.** §6 specifies `SUCCESS = 0`, which is what is
implemented so the wire matches the document. But proto3 has no presence for
enums: an unset `status` decodes as `SUCCESS`. A partial write or a producer bug
therefore reads as "this call succeeded" — the wrong failure direction for the
one field gating whether downstream trusts a payload. `STATUS_UNSPECIFIED = 0`
exists for exactly this. **Free to change now, wire-breaking migration once the
topic has data.** See `proto/faas/v1/result.proto`.

### P0 — close before step 5

- **CI.** Nothing runs automatically. Needs: unit suite on every push, `buf lint`
  and `buf breaking` (neither has ever run — no buf binary locally), broker
  suite on changes to `kafka.py`/`runner.py`, object store suite on changes to
  `objectstore.py`, or all three nightly.

*The real rebalance test is done* — see the note at the top. Two consumers in a
group, partitions moving, `_on_revoke` draining in-flight work under a live
coordinator, with the committed/uncommitted split asserted on both sides.

### P1 — the next build-order step

- **Step 5, results sink.** Functions do not own database connections (§4.4).
- **Decide the hydration-failure gap.** A call that fails hydration is invisible
  downstream: no reference, so no function sees it and the aggregator never
  expects it. The DLQ holds the input for replay — the spec's answer — but
  nothing distinguishes "not hydrated yet" from "never will be", and a
  dead-letter topic is not something downstream can watch. Closing it means a
  hydration-failure topic the aggregator honours. Design decision, deliberately
  not invented. See `ReferenceEmitter.emit_failure`.
- **Point `JsonSourceDecoder` at the real input schema.** §4.1 defines no format,
  so the field names are constructor arguments and currently guessed.

### P2 — known limits, none blocking

- **Timeout cancellation is best-effort for a running job.** `ProcessWorkerPool`
  cannot interrupt a worker mid-file without killing the process; it abandons the
  outcome so the runner can retry or DLQ, but the worker burns CPU until it
  returns.
- **A retry in backoff holds its partition's low-water mark.** Correct — the file
  is unfinished — but a long backoff delays commits for everything behind it on
  that partition. Watch once real retry rates exist.
- **Consumer lag is not emitted by the SDK.** `kafka-exporter` is the source of
  truth and feeds the autoscaler (§5.5, §11); the SDK-side gauge is a cross-check
  that needs a broker to be worth writing.
- **Decoding is lossy for unknown fields.** Messages land in dataclasses, so a
  field added by a newer producer survives the read but not a re-encode. Fine for
  the SDK — the runner only decodes references and only encodes results it built
  — but anything proxying a message must work on the protobuf object.
- **The broker suite takes ~4min**, dominated by idle-detection waits in the
  redelivery helpers. Worth tightening if it becomes a CI annoyance.
- **OpenShift gotchas (§11) are unaddressed.** No Dockerfiles yet. The random-UID
  scratch-path problem will bite the hydrator first: ffmpeg temp files need a
  directory group-writable by GID 0.
