# Ingest automation (Option 1)

This runs existing Python ingestion scripts on the `victoriametrics` host with
systemd timers.

Polling uses persisted cursor state (`/var/lib/ingest/*.json`) with overlap
buffer to avoid timer-drift gaps.

Set `D65_SERVER` and `B3SR_SERVER` in `/etc/ingest/ingest.env` to local/container
addresses so ingestion avoids tailnet routing on-host.

## First-time server setup

1. Copy env template and fill private values:
   - `sudo mkdir -p /etc/ingest`
   - `sudo cp /home/ubuntu/ingest/current/deploy/env/ingest.env.example /etc/ingest/ingest.env`
2. Ensure log/state dirs exist:
   - `sudo mkdir -p /var/log/ingest /var/lib/ingest`

## Deploy from local machine

```bash
./scripts/deploy_victoriametrics.sh
```

## Status and logs

```bash
tailscale ssh ubuntu@victoriametrics
sudo systemctl list-timers | grep ingest
sudo systemctl status d65-ingest.service b3sr-ingest.service
tail -n 200 /var/log/ingest/d65.log
tail -n 200 /var/log/ingest/b3sr.log
```

## Rollback

```bash
./scripts/rollback_victoriametrics.sh <release-id>
```
