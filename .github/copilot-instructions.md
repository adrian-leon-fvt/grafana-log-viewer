# grafana-log-viewer Copilot Instructions

## Build, test, and lint commands

- Install dependencies: `python -m pip install -r requirements.txt`
- No project-level build step is defined.
- No repository test runner is defined (`decoder/tests/` is currently empty).
- No enforced lint command is defined. Local editor formatting is configured for Black with line length 80 in `.vscode/settings.json`.

## High-level architecture

- Core pipeline lives in `decoder/`:
  - `decoder/sending.py` contains shared decode/send logic (`decode_and_send`, `send_decoded`, `send_signal_using_json_lines`).
  - `decoder/utils.py` and `decoder/s3_helper.py` provide shared time handling, file discovery, VM helpers, and S3 download/list helpers.
- Ingestion scripts are split by data source and mostly orchestrate shared functions:
  - `decoder/D65/send_d65_data.py`
  - `decoder/B3SR/send_b3sr.py`
  - `decoder/SnowLeopardTMS/send_snow_leopard_tms.py`
  - These scripts resolve source files (local or S3), decode MF4 with DBCs via `asammdf`, and post to VictoriaMetrics.
  - D65 also supports direct S3 streaming mode (`--s3-streaming`) with preflight strategy selection (`auto|memory|tempfile`).
- Live paths:
  - `decoder/livelogger/` handles live CAN decode (`CANReader`, `DBCDecoder`, `livestream`).
  - `decoder/GUI/` provides PySide6 UIs for live/file workflows and buffered metric sending (`metrics_manager.py`).
- Grafana provisioning is in `provisioning/`:
  - datasource points to local VictoriaMetrics (`provisioning/datasources/victoriametrics.yaml`)
  - dashboards auto-load from provisioning YAML/JSON.

## Key conventions in this repo

- Metric shape convention is stable across send paths:
  - metric name is signal name
  - labels include `job`, `message`, and `unit`
  - Prometheus text and VictoriaMetrics JSON-line imports are both used (`/api/v1/import/prometheus` and `/api/v1/import`).
- Time handling is intentionally timezone-aware:
  - CLI offsets (`--start`, `--end`) are interpreted relative to `America/Vancouver` in D65/B3SR scripts.
  - Avoid introducing naive datetimes in ingestion paths.
- D65/B3SR scripts assume workstation-specific data roots and DBC locations (via `get_windows_home_path()` and D65 DBC path helpers). Keep path logic centralized instead of scattering new absolute paths.
- Signal filtering convention:
  - known non-telemetry signals (for example multiplexer/checksum fields) are excluded via `skip_signal` helpers before send.
- Network convention for tailnet/local VM:
  - `decoder/sending.py` extends `NO_PROXY` for `localhost`, loopback, and `.ts.net`/`.tailnet`; preserve this behavior when changing HTTP calls.
