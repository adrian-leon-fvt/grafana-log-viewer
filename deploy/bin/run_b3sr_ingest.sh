#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/ubuntu/ingest/current}"
STATE_DIR="${STATE_DIR:-/var/lib/ingest}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
ENV_FILE="${ENV_FILE:-/etc/ingest/ingest.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 2
fi
source "$ENV_FILE"

mkdir -p "$STATE_DIR"
cd "$ROOT_DIR"

exec "$PYTHON_BIN" deploy/bin/run_with_cursor.py \
  --state-file "$STATE_DIR/b3sr_cursor.json" \
  --default-lookback-seconds "${B3SR_DEFAULT_LOOKBACK_SECONDS:-600}" \
  --overlap-seconds "${B3SR_OVERLAP_SECONDS:-120}" \
  -- \
  "$PYTHON_BIN" -m decoder.B3SR.send_b3sr \
  --s3-streaming \
  --s3-bucket "${B3SR_S3_BUCKET:-b3sr-telematics}" \
  --start "{start}" \
  --end "{end}" \
  --streaming-strategy "${B3SR_S3_STREAMING_STRATEGY:-auto}"
