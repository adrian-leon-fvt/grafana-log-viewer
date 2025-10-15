### Usage
# This script is meant to post decoded signals to the VictoriaMetrics TSDB from
# the Snow Leopard TMS tests CAN logs in the Epiroc OneDrive folder.
#
## Running:
#   1. Setup the script with the start/end or folder filters.
#   2. Run the script.
#   * Note: if the snow_leopard_paths.csv file is not found, it will make one
#           this may take a while depending on how many of the files are downloaded
#           from the cloud. CAN_LOGS folder must be accessible.
#
## The following functions are included:
# - read_filtered_paths_file:
#    Reads a CSV file (separated by ; instead of comma) with preprocessed data,
#    it includes the path to the file, start and end timestamps
# - save_filtered_paths_file:
#    Saves a CSV file (separated by ; instead of comma), will remove
#    the home directory from the path to make it agnostic
# - get_unique_filepaths:
#    Scans the base directory recursively for .mf4 files, will make an attempt
#    to filter out duplicates and non-raw files. Will also extract the start and end
#    timestamps from the files.
# - filter_by_date:
#    Filters the list of files by a given start and end datetime
# - filter_by_folder:
#    Filters the list of files by a given folder name
# - process_files:
#    Processes the list of files in batches, concatenates them, decodes them
#    using the provided DBC file, and sends the decoded signals to the TSDB


from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from itertools import chain
import time
import re
import logging
from asammdf import MDF
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import sys
import os

from ..sending import send_signal_using_json_lines
from ..config import *
from ..utils import *

CSVContent = tuple[Path, datetime]

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def get_unique_filepaths(
    base_dir: Path,
    start: datetime | None = None,
    end: datetime | None = None,
    max_workers: int = 10,
) -> list[CSVContent]:
    start_ts = time.time()

    logging.info(f"📂 Scanning {base_dir} for .mf4 files ...")

    filtered: list[CSVContent] = list()

    def is_duplicate(file: Path) -> bool:
        _fparts = "_".join(file.parts[-3:])
        _fparts = re.sub(r" \(\d+\)", "", _fparts)  # Remove (1), (2), etc.
        for f, _ in filtered:
            if _fparts == "_".join(f.parts[-3:]):
                return True

        return False

    def name_is_decoded(file: Path) -> bool:
        return (
            ("decoded" in str(file).lower())
            or ("deocded" in str(file).lower())
            or ("merged" in str(file).lower())
        )

    def timestamp_already_there(file: Path) -> bool:
        if not file.suffix.lower() == ".mf4":
            # Not a valid file
            return True

        try:
            ts = get_mdf_start_time(file)
            for _, t in filtered:
                if t == ts:
                    # Found a duplicate timestamp
                    return True
        except:
            # Error reading the file, skip it
            return True

        # File is valid and timestamp not found
        return False

    if base_dir.exists() and base_dir.is_dir():

        def _process_file(file: Path) -> CSVContent | None:
            if not (
                is_duplicate(file)
                or name_is_decoded(file)
                or timestamp_already_there(file)
            ):
                try:
                    start_time = get_mdf_start_time(file)
                    if (
                        start_time
                        and ((start <= start_time) if start else True)
                        and ((start_time <= end) if end else True)
                    ):
                        logging.info(
                            f"    • Adding file: ../{'/'.join(file.parts[-3:])} with range {start_time.isoformat()}"
                        )
                        return (file, start_time)
                except Exception as e:
                    logging.error(f"    ‼️ Error in {file}: {e}")
                    return None
            return None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = (
                executor.submit(_process_file, file)
                for file in chain(base_dir.rglob("*.MF4"), base_dir.rglob("*.mf4"))
            )

            for future in futures:
                result = future.result()
                if result:
                    filtered.append(result)

    logging.info(
        f"📂 Found {len(filtered)} files to process in {get_time_str(start_ts)}"
    )

    return filtered


def filter_by_date(
    files: list[tuple[Path, datetime, datetime]],
    start_time: datetime,
    end_time: datetime,
) -> list[tuple[Path, datetime, datetime]]:
    return [
        (f, start, end)
        for f, start, end in files
        if (start <= end_time) and (start >= start_time)
    ]


def filter_by_folder(
    files: list[tuple[Path, datetime, datetime]], folder: str
) -> list[tuple[Path, datetime, datetime]]:
    return [f for f in files if folder in str(f)]


def process_files(server: str, files: list, dbc_file: Path, batch_size: int = 1) -> int:
    total_signals = 0
    if not files:
        print(" ☹️  No files to process")
        return total_signals

    if not dbc_file.exists():
        print(f" ☹️  Cannot find DBC file at {dbc_file}")
        return total_signals
    database_files = {"CAN": [(dbc_file, 0)]}

    for i in range(0, len(files), batch_size):

        _files = []

        if i + batch_size < len(files):
            _files = files[i : i + batch_size]
        else:
            _files = files[i:]

        for j, (file, _) in enumerate(_files):
            print(
                f'=> [{i + j + 1} of {len(files)}] Processing ../{"/".join(file.parts[-3:])}'
            )

        try:
            ts = time.time()
            print(f"  ⌛ Concatenating ... ", end="\r", flush=True)
            cc = MDF().concatenate([f for f, _ in _files])
            print(f"  ☑️ Concatenated in {get_time_str(ts)}")

            try:
                ts = time.time()
                print(f"  ⌛ Decoding ... ", end="\r", flush=True)
                decoded = cc.extract_bus_logging(database_files=database_files)  # type: ignore
                print(f"  ☑️ Decoded in {get_time_str(ts)}")

                def skip_signal(name: str) -> bool:
                    SIGNALS_TO_SKIP = [
                        "NSerial",
                        "NChecksum",
                        "NMultiplexer",
                    ]

                    if name in SIGNALS_TO_SKIP:
                        return True

                    if "mux" in name.lower():
                        return True

                    if "crc" in name.lower():
                        return True

                    return False

                ts = time.time()
                num_of_samples = 0

                def process_signal(sig):
                    if skip_signal(sig.name):
                        return 0
                    _n = send_signal_using_json_lines(
                        signal=sig,
                        start_time=decoded.start_time,
                        job="SnowLeopardTMS",
                        print_metric_line=False,
                        send_signal=True,
                        skip_signal_range_check=True,
                        batch_size=10_000,
                        server=server,
                    )
                    return _n

                with ThreadPoolExecutor() as executor:
                    results = list(
                        executor.map(process_signal, decoded.iter_channels())
                    )
                    num_of_samples += sum(results)
                print(
                    f"  ☑️ Sent batch of {convert_to_eng(num_of_samples)} samples in {get_time_str(ts)} ({convert_to_eng(num_of_samples/(time.time() - ts))} samples/sec)"
                )

                total_signals += num_of_samples
            except Exception as e:
                print(f"  ❌ Error decoding and sending signals: {e}")
                continue
        except Exception as e:
            print(f"  ❌ Error concatenating files: {e}")
            continue

    return total_signals


def get_can_logs_path() -> Path:
    win_home = get_windows_home_path()
    can_logs = Path.joinpath(
        win_home,
        "Epiroc",
        "O365 UMR R&D Battery TMS - General",
        "05_Design",
        "Prototype",
        "Testing",
        "4 Mine Testing (Lalor, Hudbay, Snow Lake) - Prototype V2",
        "CAN LOGS",
    )
    return can_logs


if __name__ == "__main__":
    logger = logging.getLogger("main")
    can_logs = get_can_logs_path()

    # Select which folder to process
    FOLDERS: list[str] = [
        "April 8 Return from Snow Lake CANEdge SD Backup",  # 0
        "April 23 CANEdge SD Backup",  # 1
        "Archive",  # 2
        "Duplicate Uploads",  # 3
        "Field Testing",  # 4
        "Field Testing Nov 10-18 Decoded",  # 5
        "Field Testing Upload Jan 24 2025",  # 6
        "Jan28 2025",  # 7
        "Prior Testing",  # 8
        "TMS LOGS Mar 3 2025",  # 9
        "TMS Trial logs feb 25 2025",  # 10
    ]

    dbc_file = can_logs.parent.joinpath(
        "dbc_for_grafana_tools", "snow_leopard_gen2_windows_no_value_tables.dbc"
    )

    server = server_vm_sltms
    logger.info(f" -> 🛜 Testing server: {server}")
    if not is_victoriametrics_online(server):
        logger.error(f" -> ❌ {server} not available. Exiting...")
        exit(1)
    else:
        logger.info(f" -> ✅ {server} is online")

    start_time = datetime(2024, 6, 1, 0, 0, 0, tzinfo=ZoneInfo("America/Vancouver"))

    filtered: list[CSVContent] = get_unique_filepaths(can_logs)

    filtered.sort(key=lambda x: x[1])  # Sort by start time

    processed: list[CSVContent] = []

    total_ts = time.time()
    total_samples = 0

    while len(filtered) > 0:  # Process 12 months in 1-month chunks

        for f, ts in filtered:
            if start_time <= ts < (start_time + relativedelta(months=1)):
                processed.append((f, ts))

        if len(processed) > 0:
            ts = time.time()
            total_sent = process_files(
                server=server,
                files=processed,
                dbc_file=dbc_file,
                batch_size=10,
            )

            total_samples += total_sent

            logger.info(
                f"🏁 {start_time.date()}-{start_time.date()} done in {get_time_str(time.time())}, sent {convert_to_eng(total_sent)} samples in ({convert_to_eng(total_sent / (time.time() - ts))} samples/s)"
            )

            for p in processed:
                filtered.remove(p)

            processed = []

        start_time += relativedelta(months=1)

    if len(processed) > 0:
        logger.info(
            f"🎉 All done in {get_time_str(total_ts)} ({convert_to_eng(total_samples)} | {convert_to_eng(total_samples / (time.time() - total_ts))})"
        )
