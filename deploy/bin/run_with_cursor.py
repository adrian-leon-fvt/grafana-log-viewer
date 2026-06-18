#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _parse_iso(ts: str) -> datetime:
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run command with persisted timestamp cursor."
    )
    parser.add_argument("--state-file", required=True, type=Path)
    parser.add_argument("--default-lookback-seconds", type=int, default=600)
    parser.add_argument("--overlap-seconds", type=int, default=120)
    parser.add_argument(
        "--",
        dest="separator",
        action="store_true",
        help="Separator before command",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    cmd = args.command
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        raise SystemExit("Missing command after --")

    now = datetime.now(timezone.utc)
    state = _load_state(args.state_file)
    last_ts_raw = state.get("last_timestamp")

    if last_ts_raw:
        start_ts = _parse_iso(last_ts_raw) - timedelta(seconds=args.overlap_seconds)
    else:
        start_ts = now - timedelta(seconds=args.default_lookback_seconds)

    replacements = {
        "{start}": start_ts.isoformat(),
        "{end}": now.isoformat(),
    }
    rendered = [replacements.get(token, token) for token in cmd]

    proc = subprocess.run(rendered, check=False)
    if proc.returncode != 0:
        return proc.returncode

    state["last_timestamp"] = now.isoformat()
    if "last_key" not in state:
        state["last_key"] = ""
    _save_state(args.state_file, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
