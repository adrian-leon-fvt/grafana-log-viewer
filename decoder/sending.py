from asammdf import MDF, Signal
from asammdf.blocks.types import DbcFileType, BusType, StrPath
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time
from collections.abc import Iterable
from collections import defaultdict

vm_import_url = "http://localhost:8428/api/v1/import/prometheus"
vm_export_url = "http://localhost:8428/api/v1/export"
vm_query_url = "http://localhost:8428/api/v1/query"
vm_query_range_url = "http://localhost:8428/api/v1/query_range"


def get_time_str(start_time: float) -> str:
    elapsed = time.time() - start_time
    mins, secs = divmod(elapsed, 60)
    return f"{mins:.0f}m{secs:.3f}s" if mins else f"{secs:.3f}s"


def get_mf4_files(directory: Path | str):
    """
    Get all MDF4 files in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    return list(directory.rglob("*.[mM][fF]4"))


def get_dbc_files(directory: Path | str) -> list[StrPath]:
    """
    Get all DBC files in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    return list(directory.rglob("*.[dD][bB][cC]"))


def get_dbc_dict(directory: Path | str) -> dict[BusType, Iterable[DbcFileType]]:
    """
    Get a dictionary of DBC files in the specified directory.
    This dictionary can be passed directly to extract_bus_logging() in asammdf.
    """

    dbc_files = get_dbc_files(directory)
    return {"CAN": [(file, 0) for file in dbc_files]}


def get_channel_data(signal: Signal) -> tuple[str, int]:
    display_names = list(signal.display_names.keys())
    message = display_names[1].split(".")[0]
    can_id = display_names[2].split(" ")[0].split("ID=")[1]
    return message, int(can_id, 16)


def make_metric_line(
    metric_name: str,
    message: str,
    can_id: int,
    unit: str,
    value: float,
    timestamp: datetime | float,
    job: str = "",
) -> str:
    # Format the metric line for Prometheus
    return f'{metric_name}{{job="{job}",message="{message}",can_id="{can_id:X}",unit="{unit}"}} {value} {timestamp.timestamp() if type(timestamp) is datetime else timestamp}\n'


def send_signal(signal: Signal, start_time: datetime, job: str):
    message, can_id = get_channel_data(signal)
    metric_name = signal.name.replace(" ", "_")
    unit = signal.unit if signal.unit else ""

    print(f"  => Sending {metric_name} ...", end="\r", flush=True)
    start = time.time()
    batch: list[str] = []
    batch_size = 10000
    for sample, ts in zip(signal.samples, signal.timestamps):
        data = make_metric_line(
            metric_name,
            message,
            can_id,
            unit,
            sample,
            start_time + timedelta(seconds=ts),
        )
        batch.append(data)
        if len(batch) >= batch_size:
            try:
                requests.post(vm_import_url, data="".join(batch))
            except Exception as e:
                print(f"\nError sending batch: {e}", flush=True)
            batch = []
            time.sleep(0.1)
    if batch:
        try:
            requests.post(vm_import_url, data="".join(batch))
        except Exception as e:
            print(f"\nError sending final batch: {e}", flush=True)

    time_str = get_time_str(start)
    print(f"  => Sending {metric_name} ... sent in {time_str}   ", flush=True)


def send_file(filename: Path, job: str | None = None):
    print(f"Sending {filename}")
    if not filename.exists():
        print(f"File {filename} does not exist.")
        return

    if not filename.is_file():
        print(f"{filename} is not a file.")
        return

    if not filename.suffix.lower() == ".mf4":
        print(f"{filename} is not a valid MDF4 file.")
        return

    with MDF(filename) as mdf:
        for sig in mdf.iter_channels():
            send_signal(sig, mdf.start_time, job=job if job else filename.stem)
