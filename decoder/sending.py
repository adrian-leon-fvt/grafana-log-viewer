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


def get_channel_data(signal: Signal) -> tuple[str, int, str]:
    display_names = list(signal.display_names.keys())
    message = display_names[1].split(".")[0]
    can_id = display_names[2].split(" ")[0].split("ID=")[1]
    name = signal.name.replace(" ", "_")
    return message, int(can_id, 16), name


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


def check_signal_range(signal: Signal, start_time: datetime) -> Signal | None:
    """
    Checks if the signal timestamps already exist in the database, returns a Signal object only with timestamps not already there, if the whole range is already in the metrics database, returns None.
    """
    # Query VictoriaMetrics to check if data for this signal exists in the given time range
    metric_name = signal.name.replace(" ", "_")
    start_ts = start_time.timestamp()
    end_ts = (start_time + timedelta(seconds=signal.timestamps[-1])).timestamp()

    params: dict[str, str] = {
        "query": f"{metric_name}",
        "start": str(start_ts),
        "end": str(end_ts),
        "step": "60",  # 1 minute step, adjust as needed
    }
    try:
        resp = requests.get(vm_query_url, params=params, timeout=10)
        if resp.status_code == 200 and resp.text.strip():
            # Data exists for this signal in the range
            return None
    except Exception as e:
        print(f"Warning: Could not check VictoriaMetrics for {metric_name}: {e}")
    return signal


def send_signal(signal: Signal, start_time: datetime, job: str):
    message, can_id, metric_name = get_channel_data(signal)
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


def concat_and_decode(directory: Path | str):
    """
    Decode all MDF4 files in the specified directory and send their data to VictoriaMetrics.
    """
    files = get_mf4_files(directory)
    database_files = get_dbc_dict(directory)

    if not files:
        print(f"No MDF4 files found in {directory}.")
        return

    for file in files:
        # Group files by local date (using MDF start_time)

        # First, collect all files and their local dates
        file_dates = defaultdict(list)
        local_tz = datetime.now().astimezone().tzinfo

        for mf4_file in files:
            mdf = MDF(mf4_file, process_bus_logging=False)
            local_date = mdf.start_time.astimezone(local_tz).date()
            file_dates[local_date.strftime("%Y_%m_%d")].append(mdf)

        # For each date, concatenate and decode all files for that day
        for date, day_files in file_dates.items():
            print(f"Combining files for {date} ... [{0:>3.1f}%]", end="", flush=True, end="\r")
            concatenated = MDF().concatenate(day_files, process_bus_logging=False, )
            # Determine if 'upper' or 'lower' is in the directory path (case-insensitive)
            dir_str = str(directory).lower()
            prefix = (
                "upper"
                if "upper" in dir_str
                else "lower" if "lower" in dir_str else "unknown"
            )
            # Find the git repo root
            repo_root = Path(__file__).resolve().parent
            while not (repo_root / ".git").exists() and repo_root != repo_root.parent:
                repo_root = repo_root.parent
            out_dir = repo_root / "files" / "concatenated"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{prefix}_{date}_concat.MF4"
            concatenated.save(out_file)
            # Decode with DBC
            start = time.time()
            print(f"Decoding {date} ... [{0:>3.1f}%]", end="", flush=True)
            decoded = concatenated.extract_bus_logging(database_files=database_files)
            print(
                f"Decoding {date} ... [{100:>3.1f}%] in {get_time_str(start)}",
                end="",
                flush=True,
            )
            # Save decoded file with date as name
            out_path = repo_root / "files" / "decoded"
            out_path.mkdir(parents=True, exist_ok=True)
            decoded.save(out_path / f"{prefix}_{date}_dec.MF4")
            print(f"Saved decoded file for {date} to {out_path}")
            # Clean up
            for m in day_files:
                m.close()
            decoded.close()
            concatenated.close()
