# The local stack

Production's shape, on one machine: Kafka, an object store, an Audio API, two
hydrators and ten functions — each function its own consumer group with its own
DLQ, exactly as §11 deploys them. What is missing is missing on purpose: no
autoscaler (out of scope here) and no results sink (build-order step 5, not
written yet — the monitor reads the results topic directly instead).

It exists to break things here rather than there. The first run of it found a
bug that crash-looped a pod on any per-file timeout; see STATUS.md.

## Run it

```bash
docker compose --profile stack up -d --build      # ~3 min the first time
docker compose --profile seed  run --rm seeder  --calls 300 --rate 25
docker compose --profile watch run --rm monitor --idle-seconds 60
```

The monitor prints a table and writes `runs/report-<run>.json`. It exits
non-zero if any call is missing or duplicated, so it can gate a CI job later.

To make it interesting:

```bash
# Kill a function mid-flight; its in-flight files are redelivered.
python -m stress.chaos --service spectral-centroid --signal SIGKILL

# Graceful restart -- the drain path should produce no duplicates at all.
python -m stress.chaos --service hydrator --signal SIGTERM
```

## Watch it

| | |
|---|---|
| Console | <http://localhost:8000> — trace calls, live feed, per-function pages |
| Grafana | <http://localhost:3000> — fleet and per-function dashboards |
| Prometheus | <http://localhost:9090> — targets, rules, raw queries |
| OpenSearch | <http://localhost:9200> — the raw events, one per call transition |

The console is read-only by design. Call tracing reads the event log, which
answers questions partition reads cannot ("every call this tenant failed"); set
`FAAS_EVENTS_URL=` empty to fall back to Kafka, where a lookup is two targeted
partition reads rather than a topic scan, because results and references are both
partitioned on `call_id` alone (§6, §4.2). Fleet, topics and lint always come
from the broker — consumer lag is broker state and no log line can carry it. `GET /api/lint` returns non-zero
`ok` when a declaration disagrees with the topics it points at, which makes it a
usable smoke test after any topic change.

Tear down with `docker compose --profile stack down -v`.

## Your own audio

Drop canonical FLAC files into `samples/` and re-run the corpus service. They
join the corpus verbatim — no re-encoding, no normalising, because whatever is
odd about a real recording is the reason it is worth having, and the Audio API
would have handed it over untouched too.

```bash
docker compose run --rm corpus
docker compose restart audio-api
```

## What the console can do to things

Two powers, both **off by default** and both on in `.env` because this is a
laptop stack. The console has **no authentication**: anyone who can reach port
8000 gets whatever is switched on.

| | |
|---|---|
| `CONSOLE_SANDBOX` | Run a function — edited in the browser or as committed — against real corpus audio. This executes arbitrary Python in the console container. |
| `CONSOLE_ALLOW_WRITES` | Replay a dead letter, pause/resume a function, and commit edits to a branch. Pause needs the Docker socket, which is equivalent to root on the host. |

Every write is appended to `runs/console-audit.jsonl` **before** it runs, so an
action that hangs or crashes still leaves a trace.

**Edits never reach a running pod.** Saving commits to a new branch using git
plumbing — the working tree and HEAD are untouched, so it is safe to use against
a checkout with uncommitted work. Push the branch and open a PR; the pods keep
running the image they were built from until a merged change is rolled out.
That is what keeps §8's "one PR, zero infra tickets" literally true.

**Pause is local-stack mechanics.** Kafka has no server-side pause; a group is
only paused in the sense that nobody is polling it. Here that means stopping the
container. On OpenShift the honest equivalent is scaling the deployment to zero
through the API server with a scoped service account.

## Knobs

Everything lives in `.env` (start from `.env.example`; the compose file has
defaults for all of it, so the stack runs without one). The ones that change
what the run actually proves:

| | |
|---|---|
| `AUDIO_API_ERROR_RATE` | share of fetches answered 503, to drive the retry ladder |
| `AUDIO_API_LATENCY_MS` | added latency, jittered over `[0, 2x]` |
| `BURN_SECONDS_PER_MINUTE` | `slow_burner`'s cost per audio minute, against its 15s timeout |
| `FLAKY_TRANSIENT_RATE` / `FLAKY_POISON_RATE` | `flaky_analyzer`'s failure buckets |
| `HYDRATOR_REPLICAS` | more than one makes every restart a real rebalance |

## Two things that will bite you

**Partition counts are load-bearing.** The SDK computes a result's partition
itself — murmur2 over `call_id`, so every result for a call lands together for
the aggregator — and passes it to librdkafka explicitly. If a topic has fewer
partitions than the declaration's `results_topic_partitions` (200 by default),
the producer targets a partition that does not exist, and it fails at run time
rather than at startup. `RESULTS_PARTITIONS` and `INTERNAL_PARTITIONS` in `.env`
must move together with the declarations, never alone.

**Writable mounts need group 0.** The images run as a non-root user in group 0,
the way OpenShift runs them with a random UID. A Docker named volume is created
root-owned at 755, so the container cannot write to it — which is why the corpus
is a bind mount here. The same applies to a PVC in a pod unless it sets
`fsGroup: 0`.

## What the pieces are

| | |
|---|---|
| `stress/corpus.py` | generates the corpus with ffmpeg as canonical FLAC; picks up `samples/` |
| `stress/audio_api.py` | serves it over the §4.1 contract, with fault injection |
| `stress/seed.py` | publishes call records at a target rate, records what it sent |
| `stress/monitor.py` | consumes results and every DLQ; accounting, latency, lag |
| `stress/chaos.py` | kills and restarts containers, and timestamps what it did |
