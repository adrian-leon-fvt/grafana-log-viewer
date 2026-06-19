#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/ubuntu/ingest/current}"
STATE_DIR="${STATE_DIR:-/home/ubuntu/ingest/cursor}"
RUNNER_IMAGE="${RUNNER_IMAGE:-ingest-runner:current}"
ENV_FILE="${ENV_FILE:-/etc/ingest/ingest.env}"

mkdir -p "$STATE_DIR"
cd "$ROOT_DIR"
docker_args=(--rm --network host -v "$ROOT_DIR:/app" -v "$STATE_DIR:/state" -w /app)
if [[ -f "$ENV_FILE" ]]; then
  docker_args+=(--env-file "$ENV_FILE")
fi
if [[ -d "${HOME}/.aws" ]]; then
  docker_args+=(-v "${HOME}/.aws:/root/.aws:ro")
fi
for var in AWS_S3_TLS_INSECURE AWS_CA_BUNDLE AWS_S3_CA_BUNDLE REQUESTS_CA_BUNDLE SSL_CERT_FILE CURL_CA_BUNDLE; do
  if [[ -n "${!var:-}" ]]; then
    docker_args+=(-e "$var=${!var}")
    if [[ "$var" == *_BUNDLE ]] && [[ -f "${!var}" ]]; then
      docker_args+=(-v "${!var}:${!var}:ro")
    fi
  fi
done
exec docker run "${docker_args[@]}" \
  "$RUNNER_IMAGE" \
  python deploy/bin/run_with_cursor.py \
  --state-file "/state/d65_cursor.json" \
  --cursor-output-file "/state/d65_cursor.out.json" \
  --default-lookback-seconds "${D65_DEFAULT_LOOKBACK_SECONDS:-600}" \
  --overlap-seconds "${D65_OVERLAP_SECONDS:-120}" \
  -- \
  python -m decoder.D65.send_d65_data \
  --server "${D65_SERVER:-http://localhost:8428}" \
  --s3-streaming \
  --start "{start}" \
  --end "{end}" \
  --cursor-ts "{cursor_ts}" \
  --cursor-key "{cursor_key}" \
  --cursor-out "{cursor_out}" \
  --verbosity "minimal" \
  --s3-streaming-strategy "${D65_S3_STREAMING_STRATEGY:-auto}"
