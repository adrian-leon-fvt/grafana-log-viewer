#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-ubuntu@victoriametrics}"
REMOTE_BASE="${REMOTE_BASE:-/home/ubuntu/ingest}"
TARGET_RELEASE="${1:-}"

if [[ -z "$TARGET_RELEASE" ]]; then
  echo "Usage: $0 <release-id>" >&2
  exit 2
fi

ssh "$REMOTE_HOST" "
  set -euo pipefail
  target='$REMOTE_BASE/releases/$TARGET_RELEASE'
  test -d \"\$target\"
  ln -sfn \"\$target\" '$REMOTE_BASE/current'
  systemctl --user restart d65-ingest.service b3sr-ingest.service
"

echo "Rolled back to release $TARGET_RELEASE on $REMOTE_HOST"
