import time
from datetime import datetime
from pathlib import Path
from asammdf.blocks.types import StrPath
import requests
import os
from config import *


def get_time_str(start_time: float) -> str:
    elapsed = time.time() - start_time
    days, rem = divmod(elapsed, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)

    parts = []
    if days:
        parts.append(f"{int(days)}d")
    if hours or parts:
        parts.append(f"{int(hours)}h")
    if mins or parts:
        parts.append(f"{int(mins)}m")
    if secs or parts:
        parts.append(f"{secs:.3f}s")

    return "".join(parts)


def get_dbc_files(directory: Path | str) -> list[StrPath]:
    """
    Get all DBC files in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    return list(directory.rglob("*.[dD][bB][cC]"))


def make_metric_line(
    metric_name: str,
    message: str,
    unit: str,
    value: float,
    timestamp: datetime | float,
    job: str = "",
) -> str:
    # Format the metric line for Prometheus
    job_underscored = job.replace(" ", "_")
    return f'{metric_name}{{job="{job_underscored}",message="{message}",unit="{unit}"}} {value} {timestamp.timestamp() if type(timestamp) is datetime else timestamp}\n'


def is_victoriametrics_online(timeout: float = 3.0) -> bool:
    resp_status_code = 404

    try:
        resp = requests.get(vm_query_url, params={"query": "up"}, timeout=timeout)
        resp_status_code = resp.status_code
        if resp.status_code != 200:
            print(
                f"⚠️ Could not connect to VictoriaMetrics server. Status code: {resp.status_code}"
            )
            return False
    except Exception as e:
        print(f"⚠️ Error connecting to VictoriaMetrics server: {e}")
        return False

    return resp_status_code == 200


def get_windows_home_path() -> Path:
    """
    Try to get the windows home path on both Windows and WSL/Linux.
    Looks for a suitable path in the PATH environment variable or defaults to a common location.
    """
    return Path(
        os.environ["USERPROFILE"]
        if os.name == "nt"
        else f'/mnt/c/Users/{subprocess.run(["powershell.exe", "Write-Host $env:USERNAME"], capture_output=True, text=True).stdout.strip()}'
    )
