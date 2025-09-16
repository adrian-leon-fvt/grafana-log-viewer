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
from itertools import chain
import time
import re
from config import *
from sending import (
    send_signal,
)
from utils import *
from asammdf import MDF
from zoneinfo import ZoneInfo


def read_filtered_paths_file(
    filepath: Path | str,
) -> list[tuple[Path, datetime, datetime]]:
    ts = time.time()
    print(f"📃 Reading filtered paths from {filepath} ... ")
    win_home = get_windows_home_path()
    try:
        with open(filepath, "r") as f:
            filtered = []
            for line in f.readlines():
                if line.strip():
                    path, start, end = line.strip().split(";")

                    filtered.append(
                        (
                            Path.joinpath(win_home, path),
                            datetime.fromisoformat(start),
                            datetime.fromisoformat(end),
                        )
                    )
            print(f"✅ Recovered {len(filtered)} filtered paths in {get_time_str(ts)}")
            return filtered
    except Exception as e:
        print(f"❌ Error reading filtered_paths.txt: {e}")
        return []


def save_filtered_paths_file(filtered: list, filepath: Path | str):
    ts = time.time()
    print(f"💾 Saving {len(filtered)} filtered paths to {filepath} ... ")
    try:
        with open(filepath, "w") as f:
            for file, start, end in filtered:
                _file = re.sub(r"^/mnt/[a-zA-Z]+/Users/.+/", "", str(file.as_posix()))
                _file = re.sub(r"[a-zA-Z]:/Users/.+/", "", _file)
                f.write(f"{_file};{start.isoformat()};{end.isoformat()}\n")

        print(f"✅ Saved filtered paths in {get_time_str(ts)}")
    except Exception as e:
        print(f"☹️ Error writing {filepath}: {e}")


def get_unique_filepaths(base_dir: Path) -> list:
    start_ts = time.time()

    print(f"📂 Scanning {base_dir} for .mf4 files ...")

    filtered: list[tuple[Path, datetime, datetime]] = list()

    def is_duplicate(file: Path) -> bool:
        _fparts = "_".join(file.parts[-3:])
        _fparts = re.sub(r" \(\d+\)", "", _fparts)  # Remove (1), (2), etc.
        for f, _, _ in filtered:
            if _fparts == "_".join(f.parts[-3:]):
                return True

        return False

    def name_is_decoded(file: Path) -> bool:
        return ("decoded" in str(file).lower()) or ("deocded" in str(file).lower())

    def is_raw(mdf: MDF) -> bool:
        return "CAN_DataFrame" in mdf.channels_db.keys()

    def get_range(mdf: MDF) -> tuple[datetime, datetime] | None:
        channels_with_data = [
            ch for ch in mdf.iter_channels() if len(ch.timestamps) > 0
        ]
        if len(channels_with_data) == 0:
            return None

        max_timestamp = max([ch.timestamps[-1] for ch in channels_with_data])
        return (
            mdf.start_time,
            mdf.start_time + timedelta(seconds=max_timestamp),
        )

    if base_dir.exists() and base_dir.is_dir():
        last_dir = None
        for file in chain(base_dir.rglob("*.MF4"), base_dir.rglob("*.mf4")):
            if last_dir is None or file.relative_to(base_dir).parts[0] != last_dir:
                last_dir = file.relative_to(base_dir).parts[0]
                print(f"  - Scanning folder: {last_dir}")

            if not (is_duplicate(file) or name_is_decoded(file)):
                try:
                    with MDF(file, process_bus_logging=False) as mdf:  # type: ignore
                        if is_raw(mdf):
                            _range = get_range(mdf)
                            if _range is not None:
                                start, end = _range
                                print(
                                    f"    • Adding file: ../{'/'.join(file.parts[-3:])} with range {start.isoformat()} to {end.isoformat()}"
                                )

                                filtered.append((file, start, end))
                except Exception as e:
                    print(f"    ‼️ Error reading {file}: {e}")
                    continue

    print(f"📂 Found {len(filtered)} files to process in {get_time_str(start_ts)}")

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


def process_files(files: list, dbc_file: Path, batch_size: int = 1):
    if not files:
        print("☹️ No files to process")
        return

    if not dbc_file.exists():
        print(f"☹️ Cannot find DBC file at {dbc_file}")
        return
    database_files = {"CAN": [(dbc_file, 0)]}

    for i in range(0, len(files), batch_size):

        _files = []

        if i + batch_size < len(files):
            _files = files[i : i + batch_size]
        else:
            _files = files[i:]

        for file, _, _ in _files:
            print(f'=> Processing ../{"/".join(file.parts[-3:])}')

        try:
            ts = time.time()
            print(f"  ⌛ Concatenating ... ", end="\r", flush=True)
            cc = MDF().concatenate([f for f, _, _ in _files])
            print(f"  ☑️ Concatenated in {get_time_str(ts)}")

            try:
                ts = time.time()
                print(f"  ⌛ Decoding ... ", end="\r", flush=True)
                decoded = cc.extract_bus_logging(database_files=database_files)  # type: ignore
                print(f"  ☑️ Decoded in {get_time_str(ts)}")

                ts = time.time()
                for sig in decoded.iter_channels():
                    send_signal(
                        signal=sig,
                        start_time=decoded.start_time,
                        job="SnowLeopardTMS",
                        print_metric_line=False,
                        send_signal=True,
                        skip_signal_range_check=True,
                    )
                print(f"  ☑️ Sent batch of signals in {get_time_str(ts)}")
            except Exception as e:
                print(f"  ❌ Error decoding and sending signals: {e}")
                continue
        except Exception as e:
            print(f"  ❌ Error concatenating files: {e}")
            continue


if __name__ == "__main__":
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

    filtered_filepath = r"D:/utils/grafana-log-viewer/decoder/snow_leopard_paths.csv"
    dbc_file = can_logs.parent.joinpath(
        "dbc", "snow_leopard_gen2_windows_no_value_tables.dbc"
    )

    filtered = read_filtered_paths_file(filtered_filepath)
    if not filtered:
        filtered = get_unique_filepaths(can_logs)
        save_filtered_paths_file(filtered, filtered_filepath)
    else:
        print("ℹ️ Using previously filtered paths")

    _min_ts = min([start for _, start, _ in filtered])
    _max_ts = max([end for _, _, end in filtered])
    print(
        f" -> ℹ️ There are {len(filtered)} files to process, from {_min_ts.isoformat()} to {_max_ts.isoformat()}"
    )

    month_offset = 3
    start_time = _min_ts + timedelta(days=month_offset * 30)
    end_time = start_time + timedelta(days=30)

    filtered_by_date = filter_by_date(
        filtered, start_time=start_time, end_time=end_time
    )

    print(
        f" -> ℹ️ Processing {len(filtered_by_date)} files from {start_time.isoformat()} to {end_time.isoformat()}"
    )

    # for folder in [7, 10, 4]:
    #     # for folder in [7, 8, 6, 5, 4, 9, 10]:
    #     selected_folder = FOLDERS[folder]
    #     folder_files = [f for f in filtered if selected_folder in str(f)]
    #     print(f"Found {len(folder_files)} files in folder {selected_folder} to process")

    if filtered_by_date:
        total_ts = time.time()
        process_files(filtered_by_date, dbc_file, batch_size=5)
        print(f"🏁 All done in {get_time_str(total_ts)}")
