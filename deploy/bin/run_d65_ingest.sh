#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/ubuntu/ingest/current}"
STATE_DIR="${STATE_DIR:-/home/ubuntu/ingest/cursor}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
ENV_FILE="${ENV_FILE:-/etc/ingest/ingest.env}"

if [[ -f "$ENV_FILE" ]]; then
  source "$ENV_FILE"
fi

mkdir -p "$STATE_DIR"
cd "$ROOT_DIR"

exec "$PYTHON_BIN" deploy/bin/run_with_cursor.py \
  --state-file "$STATE_DIR/d65_cursor.json" \
  --cursor-output-file "$STATE_DIR/d65_cursor.out.json" \
  --default-lookback-seconds "${D65_DEFAULT_LOOKBACK_SECONDS:-600}" \
  --overlap-seconds "${D65_OVERLAP_SECONDS:-120}" \
  -- \
  "$PYTHON_BIN" -m decoder.D65.send_d65_data \
  --server "${D65_SERVER:-http://localhost:8428}" \
  --s3-streaming \
  --start "{start}" \
  --end "{end}" \
  --cursor-ts "{cursor_ts}" \
  --cursor-key "{cursor_key}" \
  --cursor-out "{cursor_out}" \
  --s3-streaming-strategy "${D65_S3_STREAMING_STRATEGY:-auto}"
