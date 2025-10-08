import logging
from utils import (
    get_windows_home_path,
    get_time_str,
    convert_to_eng,
    make_metric_line,
    is_victoriametrics_online,
)
from pathlib import Path
from datetime import datetime, timedelta, timezone
from asammdf import MDF
from asammdf.blocks.v4_blocks import HeaderBlock
from asammdf.blocks.types import DbcFileType, BusType, StrPath
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Optional, Literal
import time
from can import LogReader
from cantools.database.can import Message, Signal, Database
from cantools.typechecking import DecodeResultType, SignalDictType
from sending import decode_and_send
from zoneinfo import ZoneInfo
from config import (
    LOG_FORMAT,
    server_vm_d65,
    server_vm_localhost,
    vmapi_import_prometheus,
)
import requests
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from s3_helper import *

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

CSVContent = tuple[Path, Literal["Upper", "Lower"], datetime]

MAC_UPPER = "6C1D6B77"
MAC_LOWER = "5A72CE4C"


def shortpath(p: Path) -> str:
    if len(p.parts) < 4:
        return p.as_posix()
    return "../" + "/".join(p.parts[-3:])


def get_range(mdf: MDF) -> tuple[datetime, datetime] | None:
    channels_with_data = [ch for ch in mdf.iter_channels() if len(ch.timestamps) > 0]
    if len(channels_with_data) == 0:
        return None

    max_timestamp = max([ch.timestamps[-1] for ch in channels_with_data])
    return (
        mdf.start_time,
        mdf.start_time + timedelta(seconds=max_timestamp),
    )


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


def get_d65_dbc_files() -> dict[Literal["Upper", "Lower"], list[StrPath]]:
    # ‼️‼️‼️ Point these to where the D65 DBC files are located ‼️‼️‼️
    _d65_loc = Path.joinpath(Path.home(), "ttc500_shell/apps/ttc_590_d65_ctrl_app/dbc")
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

    upper_dbc_files: list[StrPath] = []
    upper_dbc_files += [
        Path.joinpath(_d65_loc, "busses", dbc) for dbc in d65_dbc_files["Upper"]
    ]
    upper_dbc_files += [Path.joinpath(_d65_loc, "brightloop", "d65_brightloops.dbc")]

    lower_dbc_files: list[StrPath] = []
    # lower_dbc_files += [
    #     Path.joinpath(_d65_loc, "busses", dbc) for dbc in d65_dbc_files["Lower"]
    # ]
    lower_dbc_files += [Path.joinpath(_d65_loc, "one_shot_updates.dbc")]

    return {
        "Upper": upper_dbc_files,
        "Lower": lower_dbc_files,
    }


def send_files_to_victoriametrics(
    server: str,
    files: list[CSVContent],
    max_batch_count: int = 10,
    threaded: bool = True,
    max_batch_size: int = 250_000,
) -> dict[str, int]:
    """
    Sends the provided list of files to VictoriaMetrics in batches.
    Each file is a tuple of (Path, "Upper"|"Lower", start_time, end_time).
    Returns a  with the total counts of signals sent.
    Uses ThreadPoolExecutor to send batches in parallel.
    """

    if not files:
        logging.warning("⚠️ No files to send.")
        return {}

    dbc_files = get_d65_dbc_files()
    upper_dbc_files: list[DbcFileType] = [(dbc, 0) for dbc in dbc_files["Upper"]]
    lower_dbc_files: list[DbcFileType] = [(dbc, 0) for dbc in dbc_files["Lower"]]

    upper_tuples = [item for item in files if item[1] == "Upper"]
    upper_tuples.sort(key=lambda x: x[2])  # Sort by start time
    upper_files = [file for file, _, _ in upper_tuples]

    lower_tuples = [item for item in files if item[1] == "Lower"]
    lower_tuples.sort(key=lambda x: x[2])  # Sort by start time
    lower_files = [file for file, _, _ in lower_tuples]

    def batch(lst, n):
        for i in range(0, len(lst), n):
            yield i, lst[i : i + n]

    total_counts: dict[str, int] = {}

    if not threaded:
        for idx, batch_files in batch(upper_files, max_batch_count):
            start_idx = idx + 1
            end_idx = idx + len(batch_files)
            concat_msg = f"[{start_idx}-{end_idx}]"
            result = decode_and_send(
                files=batch_files,
                dbc_files=upper_dbc_files,
                job="Upper",
                concat_first=True,
                concat_msg=concat_msg,
                skip_signal_fn=skip_signal,
                skip_signal_range_check=True,
                batch_size=max_batch_size,
                server=server,
            )
            if isinstance(result, dict):
                for k, v in result.items():
                    total_counts[k] = total_counts.get(k, 0) + v

        for idx, batch_files in batch(lower_files, max_batch_count):
            start_idx = idx + 1
            end_idx = idx + len(batch_files)
            concat_msg = f"[{start_idx}-{end_idx}]"
            result = decode_and_send(
                files=batch_files,
                dbc_files=lower_dbc_files,
                job="Lower",
                concat_first=True,
                concat_msg=concat_msg,
                skip_signal_fn=skip_signal,
                skip_signal_range_check=True,
                batch_size=max_batch_size,
                server=server,
            )
            if isinstance(result, dict):
                for k, v in result.items():
                    total_counts[k] = total_counts.get(k, 0) + v

        return total_counts

    else:
        with ThreadPoolExecutor() as executor:
            futures = []

            for idx, batch_files in batch(upper_files, max_batch_count):
                start_idx = idx + 1
                end_idx = idx + len(batch_files)
                concat_msg = f"[{start_idx}-{end_idx}]"
                futures.append(
                    executor.submit(
                        decode_and_send,
                        files=batch_files,
                        dbc_files=upper_dbc_files,
                        job="Upper",
                        concat_first=True,
                        concat_msg=concat_msg,
                        skip_signal_fn=skip_signal,
                        skip_signal_range_check=True,
                        batch_size=max_batch_size,
                        server=server,
                    )
                )

            for idx, batch_files in batch(lower_files, max_batch_count):
                start_idx = idx + 1
                end_idx = idx + len(batch_files)
                concat_msg = f"[{start_idx}-{end_idx}]"
                futures.append(
                    executor.submit(
                        decode_and_send,
                        files=batch_files,
                        dbc_files=lower_dbc_files,
                        job="Lower",
                        concat_first=True,
                        concat_msg=concat_msg,
                        skip_signal_fn=skip_signal,
                        skip_signal_range_check=True,
                        batch_size=max_batch_size,
                        server=server,
                    )
                )

            for future in futures:
                try:
                    result = future.result()
                    if isinstance(result, dict):
                        with Lock():
                            for k, v in result.items():
                                total_counts[k] = total_counts.get(k, 0) + v
                except Exception as e:
                    logging.error(f"❌ Error processing batch: {e}")

    return total_counts


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
                    start = datetime.fromisoformat(start).astimezone(timezone.utc)
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
            output_file = Path(r"D:/utils/grafana-log-viewer/decoder/d65_s3_files.csv")

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
                        file["LastModified"].astimezone(timezone.utc).isoformat()
                    )
                    size: int = file["Size"]
                    timestamp: datetime = (
                        file["Timestamp"].astimezone(timezone.utc).isoformat()
                    )
                    f.write(f"{key},{k_seg},{last_modified},{size},{timestamp}\n")

    return files


def download_d65_files_from_s3(
    download_path: Path,
    end: datetime | str = "",
    start: datetime | str = "",
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


def main_download_files():
    download_path = Path(r"D:/d65files")
    start_date = datetime(
        year=2025,
        month=6,
        day=1,
        tzinfo=ZoneInfo("America/Vancouver"),
    )
    end_date = datetime.now().astimezone(start_date.tzinfo)

    download_d65_files_from_s3(
        download_path=download_path,
        start=start_date,
        end=end_date,
        s3_csv_file=Path(r"D:/utils/grafana-log-viewer/decoder/d65_s3_files.csv"),
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


def main_read_files():
    start_ts = time.time()
    whp = get_windows_home_path()
    canedge_folder = Path.joinpath(
        whp, "Epiroc", "Rig Crew - Private - General", "5. Testing", "CANEdge"
    )
    files = get_files_in_range(
        dir_path=canedge_folder,
        start=datetime(
            year=2025,
            month=7,
            day=15,
            tzinfo=ZoneInfo("America/Vancouver"),
        ),
        end=datetime.now().astimezone(),
    )

    logging.info(f" ✔️  Found {len(files)} files in {get_time_str(start_ts)}")


def main_post_to_victoriametrics():
    start_ts = time.time()

    start_date = datetime(
        year=2025,
        month=7,
        day=15,
        tzinfo=ZoneInfo("America/Vancouver"),
    )

    end_date = start_date + timedelta(days=1)

    whp = get_windows_home_path()
    canedge_folder = Path.joinpath(
        whp, "Epiroc", "Rig Crew - Private - General", "5. Testing", "CANEdge"
    )

    _files = get_files_in_range(canedge_folder, start_date, end_date)
    # _files = filter_by_date(files, start_date, end_date)
    _files = filter_by_job(_files, "Lower")
    # _files = filter_by_job(_files, "Upper")

    logging.info(f" ✔️  Found to {len(_files)} files from {start_date} to {end_date}.")

    server = server_vm_localhost
    if not is_victoriametrics_online(server):
        logging.error(f" -> ❌ {server} not available. Exiting...")
        exit(1)

    total_counts = send_files_to_victoriametrics(
        server=server,
        files=_files,
        max_batch_count=10,
    )
    end_ts = time.time()
    total_signals_sent = len(total_counts.keys())
    total_samples_sent = sum(total_counts.values())

    logging.info(
        f" ✔️  Sent {total_signals_sent} signals {get_time_str(start_ts, end_ts)} ({convert_to_eng(total_samples_sent)} samples | {convert_to_eng(total_samples_sent / (end_ts - start_ts))} samples/s)."
    )


if __name__ == "__main__":
    main_read_files()
    # main_download_files()
    # main_post_to_victoriametrics()
