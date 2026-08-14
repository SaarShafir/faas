#!/usr/bin/env bash
# Topics, created explicitly because auto-creation is off on the broker.
#
# The partition counts are not decoration. The SDK computes a result's partition
# itself -- murmur2 over call_id, so every result for a call lands together for
# the aggregator (§6) -- and passes it to librdkafka explicitly. A topic with
# fewer partitions than the declaration's `results_topic_partitions` means
# producing to a partition that does not exist, which fails at run time and not
# at startup. Both the results topic and the internal topic are produced to that
# way, so both have to match.
set -euo pipefail

BROKER="${BROKER:-kafka:9092}"
CLI=/opt/kafka/bin/kafka-topics.sh

create() {
  local name="$1" partitions="$2"
  if $CLI --bootstrap-server "$BROKER" --list | grep -qx "$name"; then
    echo "  = $name (exists)"
    return
  fi
  $CLI --bootstrap-server "$BROKER" --create \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=172800000 >/dev/null
  echo "  + $name ($partitions partitions)"
}

echo "creating topics on $BROKER"

# §4.1 input: whoever owns the call metadata writes here.
create faas.calls.raw "${RAW_PARTITIONS:-12}"

# §4.2 internal: the hydrator's output, and what every function consumes.
create faas.audio.internal "${INTERNAL_PARTITIONS:-200}"

# §6 results: one topic for every function, keyed by call+function+version.
create faas.results "${RESULTS_PARTITIONS:-200}"

# One DLQ per consumer group (§5.4). Small -- if these are big, something is
# wrong that more partitions will not fix.
for dlq in hydrator duration_rms silence_ratio clipping_detect zero_crossing_rate \
           spectral_centroid spectral_rolloff snr_estimate energy_vad slow_burner \
           flaky_analyzer; do
  create "faas.dlq.${dlq}" 3
done

echo "topics ready"
