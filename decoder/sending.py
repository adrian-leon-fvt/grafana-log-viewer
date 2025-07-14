from asammdf import MDF, Signal
from asammdf.blocks.types import DbcFileType, BusType, StrPath
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time
from collections.abc import Iterable
from typing import Literal, TypedDict, Any, Sequence
from can import ThreadSafeBus, Message, BufferedReader, Notifier, Printer
import cantools
import cantools.database
import json
import argparse
from multiprocessing.pool import ThreadPool

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


def get_channel_data(signal: Signal) -> tuple[str, str]:
    display_names = list(signal.display_names.keys())
    message = display_names[1].split(".")[0]
    name = signal.name.replace(" ", "_")
    return message, name


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


def check_signal_range(signal: Signal, start_time: datetime) -> Signal | None:
    """
    Checks if the signal timestamps already exist in the database, returns a Signal object only with timestamps not already there,
    """
    # Query VictoriaMetrics to check if data for this signal exists in the given time range
    message, metric_name = get_channel_data(signal)
    start_ts = (start_time + timedelta(seconds=signal.timestamps[0])).timestamp()
    end_ts = (start_time + timedelta(seconds=signal.timestamps[-1])).timestamp()

    params: dict[str, str] = {
        "match[]": f'{metric_name}{{message="{message}"}}',
        "start": str(start_ts),
        "end": str(end_ts),
        "step": "1s",
    }
    try:
        resp = requests.get(vm_export_url, params=params, timeout=10)
        if resp.status_code != 200:
            return signal
        elif resp.text == "":
            return signal
        else:
            # Data exists for this signal in the range
            # Cut the data that already exists
            _json = json.loads(resp.text)
            respstart_ts = datetime.fromtimestamp(
                1e-3 * _json["timestamps"][0], tz=start_time.tzinfo
            )
            respend_ts = datetime.fromtimestamp(
                1e-3 * _json["timestamps"][-1], tz=start_time.tzinfo
            )

            cutstart = (respstart_ts - start_time).total_seconds()
            cutend = (respend_ts - start_time).total_seconds()

            eps = 1e-3  # Adjusts for precision, acceptable to lose 1ms of data
            older_data = signal.cut(signal.timestamps[0] + eps, cutstart - eps)
            newer_data = signal.cut(cutend + eps, signal.timestamps[-1] - eps)

            newsig = older_data.extend(newer_data)
            return newsig if len(newsig.timestamps) > 0 else None
    except Exception as e:
        print(f"⚠️ Warning: Could not check Signal range for {metric_name}: {e}")
    return signal


def send_signal(signal: Signal, start_time: datetime, job: str | None):
    _signal = check_signal_range(signal, start_time)
    if _signal is None or len(_signal.timestamps) == 0:
        print(f"  ☑️ No new data for {signal.name}, skipping ...", flush=True)
        return

    message, metric_name = get_channel_data(_signal)
    unit = _signal.unit if _signal.unit else ""

    print(f"  📨 Sending {metric_name} ...", end="\r", flush=True)
    start = time.time()
    batch: list[str] = []
    batch_size = 10000
    for sample, ts in zip(_signal.samples, _signal.timestamps):
        data = make_metric_line(
            metric_name,
            message,
            unit,
            sample,
            start_time + timedelta(seconds=ts),
            job=job if job else "",
        )
        batch.append(data)
        if len(batch) >= batch_size:
            try:
                requests.post(vm_import_url, data="".join(batch))
            except Exception as e:
                print(f"\n ‼️ Error sending batch: {e}", flush=True)
            batch = []
            time.sleep(0.1)
    if batch:
        try:
            requests.post(vm_import_url, data="".join(batch))
        except Exception as e:
            print(f"\n ‼️ Error sending final batch: {e}", flush=True)

    time_str = get_time_str(start)
    print(f"  📨 Sending {metric_name} ... sent in {time_str}   ", flush=True)


def send_file(filename: Path, job: str | None = None):
    print(f"Sending {filename}")
    if not filename.exists():
        print(f"📃 File {filename} does not exist.")
        return

    if not filename.is_file():
        print(f"📃 {filename} is not a file.")
        return

    if not filename.suffix.lower() == ".mf4":
        print(f"📃 {filename} is not a valid MDF4 file.")
        return

    with MDF(filename) as mdf:
        for sig in mdf.iter_channels():
            send_signal(sig, mdf.start_time, job=job if job else filename.stem)


def send_decoded(decoded: Path | MDF, job: str | None = None) -> None:
    """
    Send a decoded MDF4 file to VictoriaMetrics.
    """
    if isinstance(decoded, Path):
        send_file(decoded, job)
    elif isinstance(decoded, MDF):
        for sig in decoded.iter_channels():
            _job = job if job else "-".join(decoded.name.parts)
            send_signal(sig, decoded.start_time, _job)
    else:
        print("⚠️ Invalid decoded input type. Must be Path or MDF instance.")


def decode_and_send(
    directory: Path | str,
    job: str | None = None,
    dbc_files: list[DbcFileType] | None = None,
    dbc_directory: Path | str | None = None,
):
    """
    Decode all MDF4 files in the specified directory and send their data to VictoriaMetrics.
    """
    files = get_mf4_files(directory)

    database_files: dict[BusType, Iterable[DbcFileType]] = {}

    if not dbc_files and not dbc_directory:
        database_files = get_dbc_dict(directory)
    else:
        if dbc_files:
            database_files = {"CAN": dbc_files}

        if dbc_directory:
            _dbc_dict = get_dbc_dict(dbc_directory)
            if dbc_files:
                database_files["CAN"] = list(database_files["CAN"]) + list(
                    _dbc_dict["CAN"]
                )

    if not files:
        print(f"🤷‍♂️ No MDF4 files found in {directory}.")

    for file in files:
        mdf = MDF(file, process_bus_logging=False)
        try:
            start = time.time()
            print(f" ⏳ Decoding {file} ...", end="\r", flush=True)
            decoded = mdf.extract_bus_logging(
                database_files, ignore_value2text_conversion=True
            )
            print(f" ✅ Decoded {file} in {time.time() - start:.3f}s", flush=True)
            send_decoded(decoded, job)
        except Exception as e:
            print(f"❌ Error decoding {file}: {e}")
            continue


class PortConfig(TypedDict):
    """
    Configuration for a CAN port to livestream data.
    Each port should have a dictionary with the following keys:
      - 'bus': dict of ThreadSafeBus arguments
      - 'database': list of paths to DBC files
    """

    bus: dict[
        str, Any
    ]  # Arguments for ThreadSafeBus, e.g. {"channel": "can0", "bustype": "socketcan"}
    database: str | Path | Sequence[str | Path]  # Path to DBC file
    job: str | None  # Job name for the metrics coming from this port


def livestream(ports: PortConfig):
    """
    Livestream data from CAN ports and send to VictoriaMetrics.

    Each port config should be a dict with:
      - 'bus': dict of ThreadSafeBus arguments
      - 'database': path to DBC file
    """
    bus = ThreadSafeBus(**ports["bus"])
    _reader = BufferedReader()
    notifier = Notifier(bus, [_reader])
    databases = [cantools.database.load_file(f) for f in Path('../files/upper/dbc').rglob("*.dbc")]

    def printer(reader: BufferedReader):
        while True:
            msg = reader.get_message(0.5)
            if msg is None:
                time.sleep(0.1)
                continue

            if isinstance(msg, Message):
                databases[0].

    while True:
        try:
            time.sleep(1)  # Keep the bus alive
        except KeyboardInterrupt:
            print("\n ‼️ Livestream interrupted by user.")
            notifier.stop()
            bus.shutdown()
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Decode MDF4 files and send to VictoriaMetrics."
    )
    parser.add_argument("directory", type=str, help="Directory containing MDF4 files.")
    parser.add_argument("job", type=str, default=None, help="Job name for the metrics.")

    args = parser.parse_args()

    decode_and_send(args.directory, args.job)
    print("👍 Decoding and sending completed 👍")
