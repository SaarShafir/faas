# Deploying to OpenShift

Eleven consumer groups — the hydrator and ten functions — plus the console.

**Deploying it for the first time, or handing it to an agent?** Read
[`AGENT_GUIDE.md`](AGENT_GUIDE.md) instead: same material, ordered as a
procedure, with the verification steps and failure modes spelled out. This file
is the reference.

**Nothing here is written per function.** `deploy/chart/values-functions.yaml` is
generated from the declarations in `functions/`, and the chart turns each entry
into a Deployment, Service, ServiceMonitor and PDB. That is not tidiness: §8
says adding a function is one PR and zero infra tickets, and the first
hand-written Deployment ends that. Adding a function is a directory, a
regenerate, and a PR.

## Install

```bash
python scripts/generate_values.py
python scripts/build_images.py --registry registry.internal/faas --push

helm install faas deploy/chart \
  -f deploy/chart/values-functions.yaml \
  -f my-cluster.yaml
```

`my-cluster.yaml` carries what belongs to the cluster rather than to the repo —
registry, Kafka bootstrap, object store, whether topics are yours to create.
Start from `values.yaml`, which documents each one.

## What you have to provide

| | |
|---|---|
| **Kafka** | Set `kafka.bootstrapServers`. With Strimzi, set `topics.create=true` and topics become `KafkaTopic` resources reviewed like code. |
| **Object store** | A Secret with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, named in `objectStore.secretName`. The chart does not create it — credentials belong to whoever owns the cluster. |
| **The 24h lifecycle rule** | §11 gives the object store the TTL and there is deliberately no reaper in the SDK. If the bucket has no expiry rule, hydrated audio accumulates for ever. |
| **Audio API** | `audioApi.url`, §4.1's source of recordings. Only the hydrator uses it. |
| **A log backend** | `events.endpoint` points at an OTel collector; the collector decides where events land, which is what makes the backend swappable. |

## Partition counts are load-bearing

The SDK computes each result's partition itself — murmur2 over `call_id`, so
every result for a call is colocated for the aggregator (§6) — and passes it to
librdkafka explicitly. A topic with **fewer** partitions than a declaration's
`results_topic_partitions` means producing to a partition that does not exist,
and it fails when the first result is produced rather than at startup.

`generate_values.py` prints the counts the declarations require. The console's
`/api/lint` checks the running cluster against them and is a usable post-deploy
smoke test.

## Things the chart gets right that are easy to get wrong

**`terminationGracePeriodSeconds` is derived per function**, not a constant. The
runner drains on SIGTERM — stop polling, finish in-flight work, commit — and a
file may still be running for up to `per_file_timeout_seconds` with a rebalance
drain on top. OpenShift's default is 30s, which would SIGKILL the hydrator
mid-file on every rollout and produce a burst of redelivered work. The hydrator
gets 165s; the cheap analyzers get 75s.

**`fsGroup: 0` on every pod.** OpenShift assigns a random UID in group 0, so the
image makes everything it owns group-writable — but a mounted volume arrives
root-owned at 755 regardless. This is not theoretical: it is exactly how corpus
generation failed the first time the local stack ran.

**Probes distinguish stuck from busy.** Liveness asks whether the poll loop is
turning; readiness asks whether the pod holds partitions. Conflating them
restarts a pod that is legitimately grinding through a 5-minute file, which
turns one slow call into a redelivered one — the §5.2 failure by another route.
A draining pod fails readiness immediately and stays alive, so a rollout is not
cut short by a restart.

**Rollouts are one pod at a time** (`maxUnavailable: 0`). Every membership
change rebalances the group, so replacing several pods at once means several
rebalances and redelivering whatever was in flight for each.

**No CPU limit, only a memory limit.** Throttling a function mid-file makes it
miss `per_file_timeout_seconds` and dead-letter work that would otherwise have
finished. Memory is limited because there the failure mode is the node.

## The console

Read-only by default. `console.allowWrites=true` enables replay, pause and
committing edits to a branch; `console.sandbox=true` lets it run a function
against corpus audio, which is arbitrary code execution and needs a writable
root filesystem.

Pause scales the Deployment to zero through the API server, using a Role that
grants `get`, `list` and `patch` on deployments **in one namespace and nothing
else**. The previous replica count is recorded in an annotation in the same
patch, so resume restores three replicas rather than guessing one.

There is no authentication in the console. In a closed cluster the Route is the
boundary; anywhere else, put something in front of it.

## Still missing

- **Autoscaling.** Build-order step 6, and the reason the lag metrics exist.
  Replicas are fixed at 1 unless overridden. §5.2 is explicit that pod count is
  the thing to scale while `in_flight` stays shallow, so a KEDA `ScaledObject`
  on lag drops in without touching the functions.
- **CI.** Nothing runs `generate_values.py --check`, so the generated values can
  drift from the declarations silently — which is the one failure mode that
  makes the whole generate-don't-write approach untrustworthy.
- **NetworkPolicy.** Nothing restricts egress today.
