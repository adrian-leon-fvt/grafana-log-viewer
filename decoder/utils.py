import time
from datetime import datetime
from pathlib import Path
from typing import Iterable
from asammdf.blocks.types import DbcFileType, BusType, StrPath


def get_time_str(start_time: float) -> str:
    elapsed = time.time() - start_time
    mins, secs = divmod(elapsed, 60)
    return f"{mins:.0f}m{secs:.3f}s" if mins else f"{secs:.3f}s"


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
    return f'{metric_name}{{job="{job}",message="{message}",unit="{unit}"}} {value} {timestamp.timestamp() if type(timestamp) is datetime else timestamp}\n'
