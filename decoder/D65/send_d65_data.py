import os
import sys
import time
import logging
from pathlib import Path

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from asammdf import MDF
from asammdf.blocks.types import DbcFileType, BusType
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, Iterable

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from decoder.utils import (
    get_windows_home_path,
    get_time_str,
    convert_to_eng,
    is_victoriametrics_online,
)
from decoder.sending import send_decoded
from decoder.config import (
    LOG_FORMAT,
    server_vm_d65,
    server_vm_localhost,
    vmapi_import_prometheus,
)
from decoder.s3_helper import *

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

CSVContent = tuple[Path, Literal["Upper", "Lower"], datetime]

MAC_UPPER = "6C1D6B77"
MAC_LOWER = "5A72CE4C"


canedge_folder = Path.joinpath(
    get_windows_home_path(),
    "Epiroc",
    "Rig Crew - Private - General",
    "5. Testing",
    "CANEdge",
)


def shortpath(p: Path) -> str:
    if len(p.parts) < 4:
        return p.as_posix()
    return "../" + "/".join(p.parts[-3:])


def upper_or_lower(p: Path) -> Literal["Upper", "Lower"] | None:
    parts = [part.lower() for part in p.parts]
    if "upper" in parts or MAC_UPPER in str(p):
        return "Upper"
    elif "lower" in parts or MAC_LOWER in str(p):
        return "Lower"
    else:
        return None


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

    if "nmultiplexer" in name.lower():
        return True

    if "crc" in name.lower():
        return True

    return False


def get_d65_dbc_files() -> dict[Literal["Upper", "Lower"], list[Path]]:
    # ‼️‼️‼️ Point these to where the D65 DBC files are located ‼️‼️‼️
    _d65_loc = Path.joinpath(
        Path.home(), "ttc500_shell/apps/ttc_590_d65_ctrl_app/dbc")
    if os.name == "nt":  # Override if on windows
        _d65_loc = Path(
            r"\\wsl$\Ubuntu-22.04-fvt-v5\home\default\ttc500_shell\apps\ttc_590_d65_ctrl_app\dbc"
        )

    d65_dbc_files = {
        "Lower": [
            "D65_CH0_NV.dbc",
            # "D65_CH1_LV_PDU.dbc",
            # "D65_CH2_RCS_J1939.dbc",
            "D65_CH3_RCS_Module.dbc",
            "D65_CH4_Main.dbc",
        ],
        "Upper": [
            "D65_CH5_CM.dbc",
            "D65_CH6_EVCC.dbc",
        ],
    }

    upper_dbc_files: list[Path] = []
    upper_dbc_files += [
        Path.joinpath(_d65_loc, "busses", dbc) for dbc in d65_dbc_files["Upper"]
    ]
    upper_dbc_files += [Path.joinpath(_d65_loc,
                                      "brightloop", "d65_brightloops.dbc")]

    lower_dbc_files: list[Path] = []
    lower_dbc_files += [
        Path.joinpath(_d65_loc, "busses", dbc) for dbc in d65_dbc_files["Lower"]
    ]
    # lower_dbc_files += [Path.joinpath(_d65_loc, "one_shot_updates.dbc")]

    return {
        "Upper": upper_dbc_files,
        "Lower": lower_dbc_files,
    }


def get_upper_dbc_files() -> Iterable[DbcFileType]:
    dbc_files = get_d65_dbc_files()
    return [(dbc, 0) for dbc in dbc_files["Upper"]]


def get_lower_dbc_files() -> Iterable[DbcFileType]:
    dbc_files = get_d65_dbc_files()
    return [(dbc, 0) for dbc in dbc_files["Lower"]]


def get_d65_canedge_folder() -> Path:

    whp = get_windows_home_path()
    canedge_folder = Path.joinpath(
        whp, "Epiroc", "Rig Crew - Private - General", "5. Testing", "CANEdge"
    )

    if not canedge_folder.exists():
        logging.error(f"❌ CANEdge folder does not exist: {canedge_folder}")

    return canedge_folder


def get_d65_cancloud_folder() -> Path:
    cancloud_folder = Path("D:/d65files")

    if not cancloud_folder.exists():
        logging.error(f"❌ CANCloud folder does not exist: {cancloud_folder}")

    return cancloud_folder


def send_files_to_victoriametrics(
    server: str,
    files: list[CSVContent],
    stack_size: int = 10,
    max_batch_size: int = 10_000,
    skip_signal_range_check: bool = True,
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Sends the provided list of files to VictoriaMetrics in batches.
    Each file is a tuple of (Path, "Upper"|"Lower", start_time).
    Returns a pair of dictionaries with the total counts of signals sent per device.
    Uses ThreadPoolExecutor to send batches in parallel.
    """

    if not files:
        logging.warning("⚠️ No files to send.")
        return {}

    if stack_size < 1:
        stack_size = 1
        logging.warning("⚠️ concat_size must be at least 1. Setting to 1.")

    dbc_files = get_d65_dbc_files()
    upper_dbc_files: list[DbcFileType] = [
        (dbc, 0) for dbc in dbc_files["Upper"]]
    lower_dbc_files: list[DbcFileType] = [
        (dbc, 0) for dbc in dbc_files["Lower"]]

    total_upper_counts: dict[str, int] = {}
    total_lower_counts: dict[str, int] = {}
    total_files = len(files)

    if stack_size == 1:

        for i, (f, k, _) in enumerate(files):
            count_str = f"[{i} of {total_files}]"
            try:
                mdf = MDF(f)

                try:
                    start = time.time()
                    logging.info(
                        f" ⏳ {count_str} Decoding file {shortpath(f)} ...")
                    if k not in ["Upper", "Lower"]:
                        logging.error(
                            f"❌ Unknown job type '{k}' for file {shortpath(f)}, skipping."
                        )
                    else:
                        dbc: list[DbcFileType] = (
                            upper_dbc_files if k == "Upper" else lower_dbc_files
                        )
                        decoded = mdf.extract_bus_logging(
                            database_files={"CAN": dbc},
                            ignore_value2text_conversion=True,
                        )
                        logging.info(
                            f" ✅ {count_str} Decoded file {shortpath(f)} in {get_time_str(start)}"
                        )

                        if not list(decoded.iter_channels()):
                            logging.warning(
                                f"⚠️ No signals found in file {shortpath(f)}, skipping sending."
                            )
                        else:
                            result = send_decoded(
                                decoded=decoded,
                                server=server,
                                job=k,
                                skip_signal_fn=skip_signal,
                                skip_signal_range_check=skip_signal_range_check,
                                batch_size=max_batch_size,
                            )

                            for s, v in result.items():
                                if k == "Upper":
                                    total_upper_counts[s] = total_upper_counts.get(
                                        s, 0) + v
                                else:
                                    total_lower_counts[s] = total_lower_counts.get(
                                        s, 0) + v
                except Exception as e:
                    logging.error(f"❌ Error decoding file {shortpath(f)}: {e}")
                    continue

            except Exception as e:
                logging.error(f"❌ Error reading file {shortpath(f)}: {e}")
                continue
    else:

        def batch(lst, n):
            for i in range(0, len(lst), n):
                yield i, lst[i: i + n]

        def process_batch(files, dbc_files, job, stack_msg):
            logging.info(f" ⏳ {stack_msg}: Stacking {len(files)} files ...")
            start = time.time()
            try:
                mdf = MDF().stack(files)
                logging.info(
                    f" ✅ {stack_msg}: Stacked {len(files)} files in {get_time_str(start)}"
                )

                logging.info(f" ⏳ {stack_msg}: Decoding stacked files ...")
                start = time.time()
                try:
                    decoded = mdf.extract_bus_logging(
                        database_files={"CAN": dbc_files},
                        ignore_value2text_conversion=True,
                    )
                    logging.info(
                        f" ✅ {stack_msg}: Decoded stacked files in {get_time_str(start)}"
                    )
                    if not list(decoded.iter_channels()):
                        logging.warning(
                            f"⚠️ No signals found in stacked files {stack_msg}, skipping sending."
                        )
                    else:
                        result = send_decoded(
                            decoded=decoded,
                            server=server,
                            job=job,
                            skip_signal_fn=skip_signal,
                            skip_signal_range_check=True,
                            batch_size=max_batch_size,
                        )

                        for s, v in result.items():
                            if job == "Upper":
                                total_upper_counts[s] = total_upper_counts.get(
                                    s, 0) + v
                            else:
                                total_lower_counts[s] = total_lower_counts.get(
                                    s, 0) + v

                except Exception as e:
                    logging.error(
                        f"❌ Error decoding stacked files {stack_msg}: {e}")
            except Exception as e:
                logging.error(f"❌ Error stacking files {stack_msg}: {e}")

        for batch_idx, batch_files in batch(files, stack_size):
            upper_tuples = [item for item in batch_files if item[1] == "Upper"]
            upper_tuples.sort(key=lambda x: x[2])  # Sort by start time
            upper_files = [file for file, _, _ in upper_tuples]

            lower_tuples = [item for item in batch_files if item[1] == "Lower"]
            lower_tuples.sort(key=lambda x: x[2])  # Sort by start time
            lower_files = [file for file, _, _ in lower_tuples]

            start_idx = batch_idx + 1
            end_idx = batch_idx + len(batch_files)

            if len(upper_files) > 0:
                process_batch(
                    upper_files,
                    upper_dbc_files,
                    "Upper",
                    f"[Upper ({len(upper_files)}/{len(batch_files)}) in {start_idx}-{end_idx} of {total_files}]",
                )
            if len(lower_files) > 0:
                process_batch(
                    lower_files,
                    lower_dbc_files,
                    "Lower",
                    f"[Lower ({len(lower_files)}/{len(batch_files)}) in {start_idx}-{end_idx} of {total_files}]",
                )

    return total_lower_counts, total_upper_counts


def read_s3_file(
    file_path: Path | str,
    start: datetime | str = "",
    end: datetime | str = "",
) -> list[dict]:

    if isinstance(file_path, str):
        file_path = Path(file_path)

    if not file_path.exists():
        logging.error(f"❌ File does not exist: {file_path}")
        return []

    with open(file_path, "r") as f:
        lines = f.readlines()
        files = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for line in lines[1:]:  # Skip header
                parts = line.strip().split(",")
                if len(parts) != 5:
                    continue
                key, _, last_modified, size, timestamp = parts
                _ts = datetime.fromisoformat(timestamp.strip())
                if isinstance(start, str) and start:
                    start = datetime.fromisoformat(
                        start).astimezone(timezone.utc)
                if isinstance(end, str) and end:
                    end = datetime.fromisoformat(end).astimezone(timezone.utc)

                if start and _ts < start:
                    continue
                if end and _ts > end:
                    continue

                futures.append(
                    executor.submit(
                        lambda k, lm, s, ts: {
                            "Key": k,
                            "LastModified": lm,
                            "Size": int(s),
                            "Timestamp": datetime.fromisoformat(ts).astimezone(
                                timezone.utc
                            ),
                        },
                        key,
                        last_modified,
                        size,
                        timestamp,
                    )
                )

            for future in as_completed(futures):
                result = future.result()
                if result:
                    with Lock():
                        files.append(result)

        return files

    return []


def get_d65_file_list_from_s3(
    start: datetime | str = "",
    end: datetime | str = "",
    max_workers: int = 10,
    save_to_csv: bool = True,
    output_file: Path | str = "",
) -> list[dict]:
    files = get_mf4_files_list_from_s3(
        bucket_name=EESBuckets.S3_BUCKET_D65,
        start_time=start,
        end_time=end,
        max_workers=max_workers,
    )

    logging.info(f" 🪣 Found {len(files)} .mf4 files in D65 S3 bucket.")

    if save_to_csv:
        if not output_file:
            output_file = Path(
                r"D:/utils/grafana-log-viewer/decoder/d65_s3_files.csv")

        with open(output_file, "w") as f:
            f.write("Key,LastModified,Size,Timestamp\n")
            for file in files:
                key: str = file["Key"]
                k_seg: str = ""
                if key.startswith(MAC_UPPER):
                    k_seg = "Upper"
                elif key.startswith(MAC_LOWER):
                    k_seg = "Lower"

                if k_seg == "Upper" or k_seg == "Lower":
                    last_modified: datetime = (
                        file["LastModified"].astimezone(
                            timezone.utc).isoformat()
                    )
                    size: int = file["Size"]
                    timestamp: datetime = (
                        file["Timestamp"].astimezone(timezone.utc).isoformat()
                    )
                    f.write(
                        f"{key},{k_seg},{last_modified},{size},{timestamp}\n")

    return files


def download_d65_files_from_s3(
    download_path: Path,
    start: datetime | str = "",
    end: datetime | str = "",
    s3_csv_file: Path | str = "",
    s3_keys: list[str] = [],
    s3_info_list: list[dict] = [],
) -> None:
    """
    Downloads D65 .mf4 files from S3 within the specified date range to the given download path.
    The download_path should be a Path object pointing to the directory where files will be saved.
    This function creates the directory if it does not exist.

    :param start: Start datetime for filtering files.
    :param end: End datetime for filtering files.
    :param download_path: Path object for the download directory.
    :param s3_csv_file: Optional Path or str to a CSV file containing S3 file info.
    :param s3_keys: Optional list of S3 keys to download directly.
    :param s3_info_list: Optional list of dictionaries with S3 file info.
    """

    keys: list[str] = []

    if s3_csv_file and Path(s3_csv_file).exists():
        s3_csv_file = Path(s3_csv_file)
        if s3_csv_file.suffix.lower() == ".csv":
            logging.info(f"📃 Reading S3 file list from {s3_csv_file} ...")
            start_ts = time.time()
            s3_files = read_s3_file(s3_csv_file, start=start, end=end)
            logging.info(
                f"✅ Read {len(s3_files)} files from {s3_csv_file} in {get_time_str(start_ts)}"
            )
            keys.extend(
                [
                    item["Key"]
                    for item in s3_files
                    if isinstance(item, dict) and "Key" in item
                ]
            )
        else:
            logging.error(f"❌ Unsupported file format: {s3_csv_file.suffix}")
            return

    if s3_keys:
        keys.extend(s3_keys)

    if s3_info_list:
        keys.extend(
            [
                item["Key"]
                for item in s3_info_list
                if isinstance(item, dict) and "Key" in item
            ]
        )

    if not keys:
        logging.info("⚠️ No D65 files found in the provided list.")
        return

    if not download_path.exists():
        logging.info(f"📁 Creating download directory: {download_path}")
        download_path.mkdir(parents=True, exist_ok=True)

    start_ts = time.time()
    logging.info(f"⬇️ Downloading {len(keys)} files to {download_path} ...")
    count = download_files_from_s3(
        bucket_name=EESBuckets.S3_BUCKET_D65,
        keys=keys,
        download_path=download_path,
        max_workers=9,
    )
    logging.info(
        f"🏁 [D65] Downloaded {count}/{len(keys)} files in {get_time_str(start_ts)}."
    )


def get_files_in_range(
    dir_path: Path, start: datetime, end: datetime
) -> list[CSVContent]:
    if not dir_path.exists() and not dir_path.is_dir():
        logging.error(f"❌ {dir_path} does not exist or is not a directory.")
        return []

    files: list[CSVContent] = []

    globit = chain(dir_path.rglob("*.mf4"), dir_path.rglob("*.MF4"))
    with ThreadPoolExecutor() as executor:
        futures = []
        for p in globit:
            k_seg = upper_or_lower(p)
            if not k_seg:
                continue
            futures.append(
                executor.submit(
                    lambda path, k: (path, k, get_mdf_start_time(path)),
                    p,
                    k_seg,
                )
            )

        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    p, k_seg, start_time = result
                    if p in [_p for _p, _, _ in files]:
                        continue
                    if start_time.tzinfo is None:
                        start_time = start_time.replace(tzinfo=timezone.utc)
                    if start_time and (start_time >= start and start_time <= end):
                        files.append((p, k_seg, start_time))

            except Exception as e:
                logging.error(f"❌ Error processing file: {e}")

    return files


def filter_by_date(
    files: list[CSVContent],
    start_time: datetime,
    end_time: datetime,
) -> list[CSVContent]:
    return [
        (f, k_seg, start)
        for f, k_seg, start in files
        if (start <= end_time) and (start >= start_time)
    ]


def filter_by_job(
    files: list[CSVContent], job: Literal["Upper", "Lower"]
) -> list[CSVContent]:
    return [(f, k_seg, start) for f, k_seg, start in files if k_seg == job]


def get_all_d65_canedge_files(
    start: datetime, end: datetime, ignore_dchv_files: bool = True
) -> list[CSVContent]:
    canedge_folder = get_d65_canedge_folder()
    logging.info(f" 📁 Reading CANEdge files from {canedge_folder} ...")
    start_ts = time.time()
    canedge_files: list[CSVContent] = get_files_in_range(
        dir_path=canedge_folder,
        start=start,
        end=end,
    )

    if ignore_dchv_files:
        modded: list[CSVContent] = [
            (f, k_seg, start)
            for f, k_seg, start in canedge_files
            if "DCHV" not in str(f) and "logCan" not in str(f)
        ]
        canedge_files = modded

    logging.info(
        f" ✔️  [Rig Crew] Found {len(canedge_files)} files in {get_time_str(start_ts)}"
    )

    return canedge_files


def get_all_d65_cancloud_files(start: datetime, end: datetime) -> list[CSVContent]:
    cancloud_folder = get_d65_cancloud_folder()

    logging.info(f" 📁 Reading CANCloud files from {cancloud_folder} ...")
    start_ts = time.time()
    cancloud_files: list[CSVContent] = get_files_in_range(
        dir_path=cancloud_folder,
        start=start,
        end=end,
    )

    logging.info(
        f" ✔️  [CANCloud] Found {len(cancloud_files)} files in {get_time_str(start_ts)}"
    )

    return cancloud_files


def get_all_unique_d65_files(
    start: datetime,
    end: datetime,
    sorted: bool = True,
    reverse_sort: bool = False,
    ignore_dchv_files: bool = True,
) -> list[CSVContent]:
    canedge_folder = get_d65_canedge_folder()
    logging.info(f" 📁 Reading CANCloud files from {canedge_folder} ...")
    start_ts = time.time()
    canedge_files = get_all_d65_canedge_files(
        start=start, end=end, ignore_dchv_files=ignore_dchv_files
    )

    cancloud_folder = get_d65_cancloud_folder()

    logging.info(f" 📁 Reading CANCloud files from {cancloud_folder} ...")
    start_ts = time.time()
    cancloud_files: list[CSVContent] = get_files_in_range(
        dir_path=cancloud_folder,
        start=start,
        end=end,
    )

    logging.info(
        f" ✔️  [CANCloud] Found {len(cancloud_files)} files in {get_time_str(start_ts)}"
    )

    # CANEdge are files that were taken directly from the SD-card before they could be uploaded to CANCloud
    # Find just the unique files that are not in CANCloud

    def normalize_cancloud_filename(p: Path) -> str:
        # The name when downloaded replaces the '/' with '_' and adds a suffix:
        # e.g., 5A72CE4C_00001105_00000003-6853309D.MF4
        # we need to remove the suffix
        name = p.name.split("-")[0] + p.suffix  # Remove hex suffix
        # This gives a name of the form:
        # MACADDRESS_FOLDER_FILENAME.MF4
        # e.g., 5A72CE4C_00001105_00000003.MF4

        return name

    def normalize_canedge_filename(p: Path) -> str:
        # The names are of the form:
        # e.g., 5A72CE4C/00001105/00000003.MF4
        parts = p.parts[-3:]  # Get last 3 parts
        name = "_".join(parts)
        return name

    normalized_cancloud_files = [
        normalize_cancloud_filename(f) for f, _, _ in cancloud_files
    ]
    unique_canedge_files: list[CSVContent] = [
        (f, k_seg, start)
        for f, k_seg, start in canedge_files
        if normalize_canedge_filename(f) not in normalized_cancloud_files
    ]

    unique_canedge_files.sort(
        key=lambda x: x[2], reverse=reverse_sort
    )  # Sort by start time

    logging.info(
        f" ✔️  Found {len(unique_canedge_files)} unique CANEdge files not in CANCloud."
    )

    full_list = cancloud_files + unique_canedge_files

    if sorted:
        full_list.sort(key=lambda x: x[2])  # Sort by start time

    return full_list


def main_read_all_files():
    start = datetime(2025, 1, 1, tzinfo=ZoneInfo("America/Vancouver"))
    end = datetime.now().astimezone()
    files = get_all_unique_d65_files(
        sorted=True, start=start, end=end, ignore_dchv_files=True
    )


def main_post_to_victoriametrics(server: str, start_date: datetime | None = None, end_date: datetime | None = None):
    start_ts = time.time()

    if start_date is None:
        start_date = datetime(
            year=2025,
            month=11,
            day=5,
            tzinfo=ZoneInfo("America/Vancouver"),
        )

    if end_date is None:
        # end_date = datetime(
        #     year=2025,
        #     month=11,
        #     day=5,
        #     tzinfo=ZoneInfo("America/Vancouver"),
        # )

        end_date = datetime.now().astimezone()

    files: list[CSVContent] = get_all_unique_d65_files(
        sorted=True, start=start_date, end=end_date, ignore_dchv_files=True
    )

    files.sort(key=lambda x: x[2], reverse=False)  # Sort by start time

    if len(files) == 0:
        logging.warning("⚠️ No files found to send. Exiting...")
        return

    for _files, _name in [([u for u in files if u[1] == "Upper"], "Upper"), ([l for l in files if l[1] == "Lower"], "Lower")]:
        if len(_files) == 1:
            logging.info(
                f" ✔️  [{_name}] Found 1 file starting at {files[0][2].astimezone().isoformat()}."
            )
        else:
            logging.info(
                f" ✔️  [{_name}] Found {len(files)} files from {files[0][2].astimezone().isoformat()} to {files[-1][2].astimezone().isoformat()}."
            )

    logging.info(
        f" 🌐 Checking for server availability to VictoriaMetrics at {server} ..."
    )
    if not is_victoriametrics_online(server):
        logging.error(f" -> ❌ {server} not available. Exiting...")
        exit(1)

    logging.info(f" -> ✅ {server} is online. Sending files...")
    total_lower_counts, total_upper_counts = send_files_to_victoriametrics(
        server=server,
        files=files,
        stack_size=20,
        skip_signal_range_check=False,
    )
    end_ts = time.time()

    for device, total_counts in [("Upper", total_upper_counts), ("Lower", total_lower_counts)]:
        total_signals_sent = len(total_counts.keys())
        total_samples_sent = sum(total_counts.values())

        logging.info(
            f" ✔️  [{device}] Sent {total_signals_sent} signals {get_time_str(start_ts, end_ts)} ({convert_to_eng(total_samples_sent)} samples | {convert_to_eng(total_samples_sent / (end_ts - start_ts))} samples/s)."
        )

    total_signals_sent = len(total_lower_counts.keys()) + \
        len(total_upper_counts.keys())
    total_samples_sent = sum(total_lower_counts.values()) + \
        sum(total_upper_counts.values())

    logging.info(
        f" ✔️  Sent {total_signals_sent} signals {get_time_str(start_ts, end_ts)} ({convert_to_eng(total_samples_sent)} samples | {convert_to_eng(total_samples_sent / (end_ts - start_ts))} samples/s)."
    )


def main_download_files(start_date: datetime | None = None, end_date: datetime | None = None):
    download_path = Path(r"D:/d65files")
    if start_date is None:
        start_date = datetime(
            year=2025,
            month=11,
            day=5,
            hour=0,
            minute=0,
            tzinfo=ZoneInfo("America/Vancouver"),
        )

        # start_date = datetime.today().astimezone(ZoneInfo("America/Vancouver"))

    if end_date is None:
        # end_date = datetime(
        #     year=2025,
        #     month=11,
        #     day=5,
        #     tzinfo=ZoneInfo("America/Vancouver"),
        # )

        end_date = datetime.now().astimezone(start_date.tzinfo)

    # output_csv = Path(r"D:/utils/grafana-log-viewer/decoder/d65_s3_files.csv")

    s3_info_list = get_d65_file_list_from_s3(
        start=start_date,
        end=end_date,
        save_to_csv=True,
        # output_file=output_csv,
    )

    download_d65_files_from_s3(
        download_path=download_path,
        start=start_date,
        end=end_date,
        s3_info_list=s3_info_list,
        # s3_csv_file=output_csv,
    )


def main_delete_all_series(server: str):
    resp = delete_series_from_vm(server=server, match='{message=~".+"}')

    if resp:
        logging.info(
            f" ✅ Deleted series response: {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    # server = server_vm_test_dump
    server = server_vm_d65
    # server = server_vm_localhost

    start_date: datetime = datetime.today().astimezone(
        ZoneInfo("America/Vancouver")) - timedelta(days=5)
    end_date: datetime = datetime.now().astimezone(start_date.tzinfo)

    main_download_files(start_date=start_date, end_date=end_date)
    # main_delete_all_series(server)
    main_post_to_victoriametrics(
        server=server, start_date=start_date, end_date=end_date)
