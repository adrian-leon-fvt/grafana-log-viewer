from asammdf import MDF, Signal
from asammdf.blocks.types import DbcFileType, BusType
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
import time
from collections.abc import Iterable
from typing import TypedDict, Any, Sequence
from can import ThreadSafeBus, BufferedReader, Notifier
import cantools

import cantools.database
import json
import os
import logging

from config import *
from utils import *
from CANReader import CANReader
from DBCDecoder import DBCDecoder

os.environ["NO_PROXY"] = "localhost"  # Bypass proxy for VictoriaMetrics


def setup_logging():
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)


def skip_signal(name: str) -> bool:
    SIGNALS_TO_SKIP = []

    if name in SIGNALS_TO_SKIP:
        return True

    if "mux" in name.lower():
        return True

    if "crc" in name.lower():
        return True

    return False


def get_mf4_files(
    directory: Path | str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> list[Path]:
    """
    Get all MDF4 files in the specified directory.
    If start_date is provided, only return files modified after that datetime.
    If end_date is provided, only return files modified before that datetime.
    """
    if not isinstance(directory, Path):
        directory = Path(directory)

    files = list(directory.rglob("*.[mM][fF]4"))
    if start_date is not None:
        files = [
            f
            for f in files
            if datetime.fromtimestamp(f.stat().st_mtime, tz=start_date.tzinfo)
            > start_date
        ]

    if end_date is not None:
        files = [
            f
            for f in files
            if datetime.fromtimestamp(f.stat().st_mtime, tz=end_date.tzinfo) < end_date
        ]
    return files


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


def is_valid_sample(sample):
    """Check if sample can be converted to a numeric value"""
    try:
        float(sample)  # Try converting to float
        return True
    except (ValueError, TypeError):
        return False


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


def send_signal(
    signal: Signal,
    start_time: datetime,
    job: str | None,
    print_metric_line: bool = False,
    send_signal: bool = True,
    skip_signal_range_check: bool = False,
    batch_size: int = 50_000,
):
    message, metric_name = get_channel_data(signal)

    if skip_signal(signal.name):
        return

    _signal: Signal | None = signal
    if not skip_signal_range_check:
        _signal = check_signal_range(signal, start_time)

    if _signal is None or len(_signal.timestamps) < 1:
        print(f"  ☑️ No new data for {signal.name}, skipping ...", flush=True)
        return

    unit = _signal.unit if _signal.unit else ""
    _sig_start_str = start_time + timedelta(seconds=_signal.timestamps[0])
    _sig_end_str = start_time + timedelta(seconds=_signal.timestamps[-1])
    _time_str = f"{_sig_start_str.isoformat()} - {_sig_end_str.isoformat()}, {len(_signal.timestamps)} samples"

    print(f"  📨 Sending {metric_name} [{_time_str}] ...", end="\r", flush=True)
    start = time.time()
    batch: list[str] = []
    for sample, ts in zip(_signal.samples, _signal.timestamps):
        if not is_valid_sample(sample):  # Check if sample is not float (e.g. string)
            continue  # Skip this sample
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
                if print_metric_line:
                    print("".join(batch))
                if send_signal:
                    requests.post(vm_import_url, data="".join(batch))
            except Exception as e:
                print(f"\n ‼️ Error sending batch: {e}", flush=True)
            batch = []
            time.sleep(0.01)  # Avoid overwhelming the server
    if batch:
        try:
            requests.post(vm_import_url, data="".join(batch))
        except Exception as e:
            print(f"\n ‼️ Error sending final batch: {e}", flush=True)

    time_str = get_time_str(start)
    print(
        f"  📨 Sending {metric_name} [{_time_str}] ... sent in {time_str}   ",
        flush=True,
    )


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
    dbc_files: Sequence[DbcFileType] | None = None,
    dbc_directory: Path | str | None = None,
    concat_first: bool = True,
    datetime_after: datetime | None = None,
):
    """
    Decode all MDF4 files in the specified directory and send their data to VictoriaMetrics.
    """

    files = get_mf4_files(directory, start_date=datetime_after)

    database_files: dict[BusType, Iterable[DbcFileType]] = {}

    if not dbc_files and not dbc_directory:
        database_files = get_dbc_dict(directory)
    else:
        database_files["CAN"] = []

        if dbc_files:
            database_files["CAN"].extend(list(dbc_files))

        if dbc_directory:
            _dbc_dict = get_dbc_dict(dbc_directory)
            database_files["CAN"].extend(list(_dbc_dict["CAN"]))

    if not files:
        print(f"  🤷‍♂️ No MDF4 files found in {directory}.")
        return

    if not database_files:
        print(f"  🤷‍♂️ No DBC files found in {dbc_directory or directory}.")
        return

    if concat_first and len(files) > 1:
        mdf = MDF()
        try:
            print(f" ⏳ Concatenating {len(files)} files ...", end="\r", flush=True)
            start = time.time()
            mdf = MDF().concatenate(files)
            print(f" ✅ Concatenated in {time.time() - start:.3f}s", flush=True)

            try:
                print(f" ⏳ Decoding concatenated files ...", end="\r", flush=True)
                start = time.time()
                decoded = mdf.extract_bus_logging(
                    database_files, ignore_value2text_conversion=True
                )
                print(f" ✅ Decoded in {time.time() - start:.3f}s", flush=True)
                if list(decoded.iter_channels()):
                    send_decoded(decoded, job)
                else:
                    print("⚠️ No signals found, skipping sending.")

            except Exception as e:
                print(f"❌ Error decoding concatenated files: {e}")
        except Exception as e:
            print(f"❌ Error concatenating files: {e}")

    else:
        for file in files:
            mdf = MDF(file)
            if job is None:
                job = file.stem
            _file = str(file.as_posix())
            _split = _file.split("/")
            _dispname = "/".join(_split[_split.index(job) :])
            try:
                start = time.time()
                print(f" ⏳ Decoding ../{_dispname} ...", end="\r", flush=True)
                decoded = mdf.extract_bus_logging(
                    database_files, ignore_value2text_conversion=True
                )
                print(
                    f" ✅ Decoded ../{_dispname} in {time.time() - start:.3f}s",
                    flush=True,
                )
                if list(decoded.iter_channels()):
                    send_decoded(decoded, job)
                else:
                    print(f"⚠️ No signals found in {_dispname}, skipping sending.")
            except Exception as e:
                print(f"❌ Error decoding {_dispname}: {e}")
                continue


def send_d65_onedrive():
    d65_onedrive_folder = Path.joinpath(
        get_windows_home_path(),
        r"Epiroc/Rig Crew - Private - General/5. Testing/CANEdge",
    )

    if not d65_onedrive_folder.exists():
        print(
            "⚠️ D65 OneDrive path not found. Please check your PATH environment variable."
        )
        return
    else:

        # ‼️‼️‼️ Point these to where the D65 DBC files are located ‼️‼️‼️
        _d65_loc = Path.joinpath(
            Path.home(), "ttc500_shell/apps/ttc_590_d65_ctrl_app/dbc"
        )
        if os.name == "nt":  # Override if on windows
            _d65_loc = Path(
                r"\\wsl$\Ubuntu-22.04-fvt-v5\home\default\ttc500_shell\apps\ttc_590_d65_ctrl_app\dbc"
            )

        d65_dbc_files = {
            "Lower": [
                "D65_CH0_NV.dbc",
                "D65_CH1_LV_PDU.dbc",
                # "D65_CH2_RCS_J1939.dbc",
                "D65_CH3_RCS_Module.dbc",
                "D65_CH4_Main.dbc",
            ],
            "Upper": [
                "D65_CH5_CM.dbc",
                "D65_CH6_EVCC.dbc",
            ],
        }

        upper_dbc_files: list[DbcFileType] = []
        upper_dbc_files += [
            (Path.joinpath(_d65_loc, "busses", dbc), 0)
            for dbc in d65_dbc_files["Upper"]
        ]
        upper_dbc_files += [
            (Path.joinpath(_d65_loc, "brightloop", "d65_brightloops.dbc"), 0)
        ]

        lower_dbc_files: list[DbcFileType] = []
        lower_dbc_files += [
            (Path.joinpath(_d65_loc, "busses", dbc), 0)
            for dbc in d65_dbc_files["Lower"]
        ]
        lower_dbc_files += []

        cutoff = datetime.now() - timedelta(hours=8)

        decode_and_send(
            d65_onedrive_folder / "Upper",
            dbc_files=upper_dbc_files,
            job="Upper",
            datetime_after=cutoff,
            concat_first=True,
        )
        print("=> Upper 👍")
        decode_and_send(
            d65_onedrive_folder / "Lower",
            dbc_files=lower_dbc_files,
            job="Lower",
            datetime_after=cutoff,
            concat_first=True,
        )
        print("=> Lower 👍")


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
    database: str | Path | list[str | Path]  # Path to DBC file
    job: str | None  # Job name for the metrics coming from this port


def get_cantools_databases(
    files: str | Path | list[str | Path],
) -> list[cantools.database.Database]:
    """
    Load and return a list of DBC databases from the configured paths.
    """
    db_paths: list[Path] = []
    if isinstance(files, str | Path):
        if Path(files).is_dir():
            db_paths.extend(list(Path(files).rglob("*.[dD][bB][cC]")))
        elif Path(files).is_file() and Path(files).suffix.lower() == ".dbc":
            db_paths.append(Path(files))
    elif isinstance(files, list):
        for db in files:
            if Path(db).is_dir():
                db_paths.extend(list(Path(db).rglob("*.[dD][bB][cC]")))
            else:
                db_paths.append(Path(db))

    databases: list[cantools.database.Database] = []
    for db in db_paths:
        if not db.exists():
            print(f"⚠️ DBC file {db} does not exist, skipping.")
            continue
        try:
            _db = cantools.database.load_file(db)
            if isinstance(_db, cantools.database.Database):
                databases.append(_db)
        except Exception as e:
            print(f"⚠️ Error loading DBC file {db}: {e}")
    return databases


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

    # Load the DBC file(s) for this port
    db_paths: list[Path] = []
    if isinstance(ports["database"], str | Path):
        db_paths.extend([Path(ports["database"])])
    elif isinstance(ports["database"], list):
        for db in ports["database"]:
            if Path(db).is_dir():
                db_paths.extend(list(Path(db).rglob("*.[dD][bB][cC]")))
            else:
                db_paths.append(Path(db))

    databases: list[cantools.database.Database] = []

    for db in db_paths:
        if not db.exists():
            print(f"⚠️ DBC file {db} does not exist, skipping.")
            continue
        try:
            _db = cantools.database.load_file(db)
            if isinstance(_db, cantools.database.Database):
                databases.append(_db)
        except Exception as e:
            print(f"⚠️ Error loading DBC file {db}: {e}")

    while True:
        try:
            time.sleep(1)  # Keep the bus alive
        except KeyboardInterrupt:
            print("\n ‼️ Livestream interrupted by user.")
            notifier.stop()
            bus.shutdown()
            break


def main():
    setup_logging()
    logger = logging.getLogger("main")

    if is_victoriametrics_online():
        if LIVE_STREAMING:  # Streaming live CAN data
            dbc_decoder = None
            if DBC_FILE_PATHS:
                logger.info("Initializing DBC decoder...")
                dbc_decoder = DBCDecoder(DBC_FILE_PATHS)

            # Initialize CAN reader
            logger.info("Initializing CAN reader...")
            can_reader = CANReader(
                interface=CAN_INTERFACE, channel=CAN_CHANNEL, dbc_decoder=dbc_decoder
            )

            if not can_reader.connect():
                logger.error("Failed to initialize CAN interface")
                return

            logger.info("Starting CAN monitoring...")
            while True:
                try:
                    result = can_reader.read_decoded_message()
                    if result is None:
                        continue
                    timestamp, message_data = result
                    if not message_data:
                        continue
                    message = message_data["message"]
                    decoded_signals = message_data["decoded_signals"]
                    if message:
                        for signal in decoded_signals.keys():
                            value, unit = decoded_signals[signal]
                            data = make_metric_line(
                                message.name,
                                signal,
                                unit,
                                value,
                                timestamp,
                                job="d65_livestream",
                            )
                            try:
                                requests.post(vm_import_url, data="".join(data))
                            except Exception as e:
                                logging.error(f"\n ‼️ Error sending data: {e}")

                except KeyboardInterrupt:  # Shutting down properly
                    break

            logger.info("Shutting down...")
            can_reader.shutdown()
        else:  # Sending MF4 files
            send_d65_onedrive()


if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description="Decode MDF4 files and send to VictoriaMetrics."
    # )
    # parser.add_argument("directory", type=str, help="Directory containing MDF4 files.")
    # parser.add_argument("job", type=str, default=None, help="Job name for the metrics.")

    # args = parser.parse_args()

    # decode_and_send(args.directory, args.job)
    # print("👍 Decoding and sending completed 👍")
    main()
