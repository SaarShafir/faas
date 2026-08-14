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

Tear down with `docker compose --profile stack down -v`.

## Your own audio

Drop files into `samples/` and re-run the corpus service. They join the corpus
at whatever rate and format they already are — no transcoding, no normalising,
because whatever is odd about a real recording is the reason it is worth having.

```bash
docker compose run --rm corpus
docker compose restart audio-api
```

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
| `stress/corpus.py` | generates the corpus with ffmpeg; picks up `samples/` |
| `stress/audio_api.py` | serves it over the §4.1 contract, with fault injection |
| `stress/seed.py` | publishes call records at a target rate, records what it sent |
| `stress/monitor.py` | consumes results and every DLQ; accounting, latency, lag |
| `stress/chaos.py` | kills and restarts containers, and timestamps what it did |
