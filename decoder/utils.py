import time
from datetime import datetime, timedelta
from pathlib import Path
from asammdf import MDF
from asammdf.blocks.types import StrPath
import requests
import os
import json
from itertools import chain
from can import LogReader, Logger
from config import *
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


def get_time_str(start_time: float, end_ts: float | None = None) -> str:
    _end_ts = time.time() if end_ts is None else end_ts
    elapsed = _end_ts - start_time
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


def get_files(directory: Path | str, extension: str | list[str]) -> list[StrPath]:
    """
    Get all files with the specified extensions in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    patterns: list[str] = []

    if isinstance(extension, str):
        patterns = (
            [f"*{extension}"] if extension.startswith(".") else [f"*.{extension}"]
        )
    elif isinstance(extension, list) and all(isinstance(ext, str) for ext in extension):
        patterns = [
            f"*{ext}" if ext.startswith(".") else f"*.{ext}" for ext in extension
        ]
    else:
        raise ValueError("Extension must be a string or a list of strings.")

    return list(chain.from_iterable(directory.rglob(pattern) for pattern in patterns))


def get_dbc_files(directory: Path | str) -> list[StrPath]:
    """
    Get all DBC files in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    return get_files(directory, [".dbc", ".DBC"])


def make_list_of_vm_json_line_format(
    metric_name: str,
    message: str,
    unit: str,
    values: list[float],
    timestamps: list[float | datetime],
    job: str,
    batch_size: int = 250_000,
) -> list[str]:
    """
    Create a list of JSON lines in the VictoriaMetrics format for batch uploading.
    The JSON format is as follows:
    {
        "metric": {
            "__name__": "metric_name",
            "job": "job_name",
            "message": "message",
            "unit": "unit"
        },
        "values": [value1, value2, ...],
        "timestamps": [timestamp1, timestamp2, ...]
    }

    The total samples in each JSON line should not exceed the batch_size.
    """

    if len(values) != len(timestamps):
        raise ValueError("Values and timestamps must have the same length.")
    if batch_size <= 0:
        raise ValueError("Batch size must be a positive integer.")

    if len(values) == 0:
        return []

    def make_line(start_idx: int, end_idx: int) -> str:
        json_line = {
            "metric": {
                "__name__": metric_name,
                "job": job.replace(" ", "_"),
                "message": message,
                "unit": unit,
            },
            "values": values[start_idx:end_idx],
            "timestamps": [
                ts.timestamp() if isinstance(ts, datetime) else ts
                for ts in timestamps[start_idx:end_idx]
            ],
        }
        return json.dumps(json_line)

    lines: list[str] = []
    for i in range(0, len(values), batch_size):
        lines.append(make_line(i, min(i + batch_size, len(values))))

    return lines


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


def get_metrics_from_vm(
    match: str, start_date: datetime, end_date: datetime, timeout: float = 10.0
) -> dict:
    if "job" not in match:
        raise ValueError("Job name must be provided.")

    ret: dict = {}

    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    def query_vm(single_hour: int):
        day_start = start_date + timedelta(hours=single_hour)
        day_end = min(day_start + timedelta(hours=1), end_date)

        logging.info(
            f"🔍 Querying from VictoriaMetrics for {match} from {day_start.isoformat()} to {day_end.isoformat()}..."
        )

        try:
            resp = requests.get(
                vm_query_range_url,
                params={
                    "query": f"{{{match}}}",
                    "start": day_start.timestamp(),
                    "end": day_end.timestamp(),
                    "step": "1s",
                },
                timeout=timeout,
            )

            if resp.status_code == 400:
                logging.warning(
                    f"⚠️ Could not connect to VictoriaMetrics server. Status code: {resp.status_code}"
                )
                return day_start.isoformat(), None

            elif resp.status_code == 422:
                logging.warning(
                    f"⚠️ Query error from VictoriaMetrics server. Status code: {resp.status_code}. Message: {resp.text}"
                )
                return day_start.isoformat(), None

            return day_start.isoformat(), resp.json()
        except Exception as e:
            logging.error(f"⚠️ Error connecting to VictoriaMetrics server: {e}")
            return day_start.isoformat(), None

    total_hours = (end_date - start_date).days * 24 + 1
    ret_lock = Lock()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(query_vm, h) for h in range(total_hours)]
        for future in as_completed(futures):
            key, value = future.result()
            if value is not None:
                with ret_lock:
                    ret[key] = value
    return ret


def convert_mf4_to_trc(paths: list[Path | str], output_name: str | Path) -> None:
    if not isinstance(paths, list):
        paths = [paths]

    if len(paths) == 0:
        raise ValueError("At least one input path must be provided.")

    if output_name == "":
        raise ValueError("Output name must be provided.")

    try:
        if len(paths) > 1:
            print(f"🔄 Concatenating {len(paths)} MF4 files...")
        mdf = MDF().concatenate(paths)
        saved_path = mdf.save("temp.mf4", overwrite=True)
        print(f"🔄 Converting to TRC...")
        log = LogReader(saved_path)
        logger = Logger(filename=Path(output_name).with_suffix(".trc"))
        for msg in log:
            logger(msg)

        print(f"✅ Saved at {Path(output_name).with_suffix('.trc')}")

        # Delete the temp file
        os.remove(saved_path)
    except KeyboardInterrupt:
        print("❌ Conversion cancelled by user.")
        return
    except Exception as e:
        print(f"❌ Error during conversion: {e}")


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


def convert_to_eng(value: int | float) -> str:
    if value > 1e9:
        return f"{value / 1e9:.3f}B"
    elif value > 1e6:
        return f"{value / 1e6:.3f}M"
    elif value > 1e3:
        return f"{value / 1e3:.3f}k"
    else:
        return str(value)
