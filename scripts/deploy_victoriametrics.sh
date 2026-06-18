#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE_HOST="${REMOTE_HOST:-ubuntu@victoriametrics}"
REMOTE_BASE="${REMOTE_BASE:-/home/ubuntu/ingest}"
RELEASE_ID="${RELEASE_ID:-$(cd "$REPO_ROOT" && git rev-parse --short HEAD)}"
REMOTE_RELEASE="$REMOTE_BASE/releases/$RELEASE_ID"
TAR_NAME="ingest-$RELEASE_ID.tar.gz"

cd "$REPO_ROOT"

TMP_TAR="$(mktemp "/tmp/$TAR_NAME.XXXX")"
trap 'rm -f "$TMP_TAR"' EXIT

tar \
  --exclude='.git' \
  --exclude='venv' \
  --exclude='data' \
  --exclude='__pycache__' \
  -czf "$TMP_TAR" \
  decoder deploy requirements.txt scripts

ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_BASE/releases' '$REMOTE_BASE/shared' /var/log/ingest /var/lib/ingest"
scp "$TMP_TAR" "$REMOTE_HOST:$REMOTE_RELEASE.tar.gz"
ssh "$REMOTE_HOST" "
  set -euo pipefail
  mkdir -p '$REMOTE_RELEASE'
  tar -xzf '$REMOTE_RELEASE.tar.gz' -C '$REMOTE_RELEASE'
  rm -f '$REMOTE_RELEASE.tar.gz'
  python3 -m venv '$REMOTE_RELEASE/.venv'
  '$REMOTE_RELEASE/.venv/bin/pip' install --upgrade pip >/dev/null
  '$REMOTE_RELEASE/.venv/bin/pip' install -r '$REMOTE_RELEASE/requirements.txt' >/dev/null
  ln -sfn '$REMOTE_RELEASE' '$REMOTE_BASE/current'
  chmod +x '$REMOTE_BASE/current/deploy/bin/'*.sh
  sudo cp '$REMOTE_BASE/current/deploy/systemd/'*.service /etc/systemd/system/
  sudo cp '$REMOTE_BASE/current/deploy/systemd/'*.timer /etc/systemd/system/
  sudo systemctl daemon-reload
  sudo systemctl enable --now d65-ingest.timer b3sr-ingest.timer
  sudo systemctl restart d65-ingest.service b3sr-ingest.service
"

echo "Deployed release $RELEASE_ID to $REMOTE_HOST:$REMOTE_RELEASE"
