# Ingest automation

This runs existing Python ingestion scripts on the `victoriametrics` host with
systemd user timers.
Runtime dependencies are installed in Docker image `ingest-runner:current` at
deploy time (no host Python/venv dependency).

Polling uses S3 `LastModified` upload time plus persisted cursor state
(`/home/ubuntu/ingest/cursor/*.json`) and an overlap buffer. This catches files
uploaded now even when their embedded recording timestamps are old. First run
after migration from a recording-time cursor rescans the previous 24 hours.
Cursor files live under `/home/ubuntu/ingest/cursor` so they survive release
switches and can be copied to new server.

Set `D65_SERVER` and `B3SR_SERVER` in `/etc/ingest/ingest.env` to local/container
addresses so ingestion avoids tailnet routing on-host.

## First-time server setup

The deploy script creates the release/cursor/log directories, installs the
systemd user units, and enables the timers automatically. The only manual
first-time step is providing the env file:

- `sudo mkdir -p /etc/ingest`
- `sudo cp /home/ubuntu/ingest/current/deploy/env/ingest.env.example /etc/ingest/ingest.env`
- Edit `/etc/ingest/ingest.env` and fill in private values.

`/etc/ingest/ingest.env` is optional; wrappers run with built-in defaults if it
is missing.

## Deploy from local machine

```bash
./scripts/deploy_victoriametrics.sh
```

## Send D65 files from a local folder

```bash
python -m decoder.D65.send_d65_data --input-folder /mnt/d/d65files
```

`--input-folder` (alias `--folder`) scans MF4 files recursively and skips S3
download. Existing `--start`/`--end` filters still apply (default: last 7 days).
Files need an `Upper`/`Lower` folder or device MAC in their path for job
selection. This local mode cannot be combined with `--s3-streaming`; scheduled
server ingestion continues using S3 streaming.

## Status and logs

```bash
tailscale ssh ubuntu@victoriametrics
systemctl --user list-timers | grep ingest
systemctl --user status d65-ingest.service b3sr-ingest.service
tail -n 200 /home/ubuntu/ingest/logs/d65.log
tail -n 200 /home/ubuntu/ingest/logs/b3sr.log
```

## Rollback

```bash
./scripts/rollback_victoriametrics.sh <release-id>
```
