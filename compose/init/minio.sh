#!/bin/sh
# The audio bucket, plus the lifecycle rule that owns the 24h TTL.
#
# There is no reaper in the SDK and there should never be one (§11): the object
# store expires hydrated audio by itself, and a missing key is a recoverable
# condition the SDK already handles as ObjectMissingError. Setting the rule here
# means the local stack has the same shape, even though a stress run never lives
# long enough to see an expiry.
set -eu

mc alias set local http://minio:9000 "$MINIO_USER" "$MINIO_PASSWORD" >/dev/null

if mc ls "local/${FAAS_AUDIO_BUCKET}" >/dev/null 2>&1; then
  echo "  = bucket ${FAAS_AUDIO_BUCKET} (exists)"
else
  mc mb "local/${FAAS_AUDIO_BUCKET}" >/dev/null
  echo "  + bucket ${FAAS_AUDIO_BUCKET}"
fi

mc ilm rule add "local/${FAAS_AUDIO_BUCKET}" --expire-days 1 >/dev/null 2>&1 \
  && echo "  + lifecycle: expire after 1 day" \
  || echo "  = lifecycle already set"

echo "object store ready"
