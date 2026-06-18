import os
import sys
import time
import logging
import io
import tempfile
import ctypes
import subprocess
from pathlib import Path

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from asammdf import MDF
from asammdf.blocks.types import DbcFileType, BusType
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, Iterable, Any

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from decoder.utils import (
    get_windows_home_path,
    get_time_str,
    convert_to_eng,
    is_victoriametrics_online,
    parse_time_arg,
)
from decoder.sending import send_decoded, normalize_dbc_entries
from decoder.config import (
    LOG_FORMAT,
    server_vm_b3sr,
    server_vm_test_dump,
)
from decoder.s3_helper import *
import argparse
import re

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

CSVContent = tuple[Path, datetime]

B3SR_JOB = "B3SR"
DBC_FOLDER_OVERRIDE: str | None = None

canedge_folder = Path.joinpath(
    get_windows_home_path(),
    "Epiroc",
    "O365 Epiroc R&D B3SR for D65 Electrification - Documents",
    "General",
    "06_Design",
    "9. Testing",
    "13. TMS Testing",
    "CANEdge",
)


def shortpath(p: Path) -> str:
    if len(p.parts) < 4:
        return p.as_posix()
    return "../" + "/".join(p.parts[-3:])


def resolve_dbc_folder(dbc_folder: str | None = None) -> Path:
    value = DBC_FOLDER_OVERRIDE if dbc_folder is None else dbc_folder
    default_folder = Path(__file__).resolve().parent / "dbc"
    legacy_folder = canedge_folder

    if value is None or not str(value).strip():
        return default_folder

    if str(value).strip().lower() in {"old", "compatibility"}:
        return legacy_folder

    resolved = Path(os.path.expanduser(os.path.expandvars(str(value).strip())))
    if not resolved.is_absolute():
        resolved = Path.cwd() / resolved
    return resolved


def get_dbc_file_path(dbc_folder: str | None = None) -> Path:
    return resolve_dbc_folder(dbc_folder).joinpath("b3sr_base.dbc")


def send_files_to_victoriametrics(
    server: str,
    files: list[CSVContent],
    stack_size: int = 10,
    max_batch_size: int = 10_000,
    skip_signal_range_check: bool = True,
    **kwargs,
) -> dict[str, int]:
    """
    Sends the provided listed files to the given victoriametrics server, will first stack data by stack_size, and will send signals in batches of max_batch_size

    Returns the amnount of data sent for each signal

    :param server: Must be a victoriametrics server
    :type server: str
    :param files: The paths for the files to send
    :type files: list[CSVContent]
    :param stack_size: The number of files to stack on top of one another using MDF stack
    :type stack_size: int
    :param max_batch_size: The maximum amount of samples in each json string sent to victoria metrics
    :type max_batch_size: int
    :param skip_signal_range_check: If set, it will just send all signals without checking if the signal is already there. If always using this same function, it is safe to do so, as it will always send the same data and will not result in any overwriting glitches. If we've backfilled in a different way, it is probably a good idea to either ignore the files altogether, or check for existing data to ensure they won't generate conflicts in the database.
    :type skip_signal_range_check: bool
    :param kwargs: Additional arguments passed to ThreadPoolExecutor or the send_decoded function
    :return: {signal_name: amount_of_data_sent, ...}
    :rtype: dict[str, int]
    """

    sent_stats: dict[str, int] = {}

    dbc_file = get_dbc_file_path()

    if not dbc_file.exists():
        logging.error(f" ☹️ DBC file {dbc_file} does not exist, cannot proceed.")
        return sent_stats

    if stack_size < 1:
        stack_size = 1
        logging.warning(" ⚠️ stack_size cannot be less than 1, setting to 1.")

    can_dbc_files = normalize_dbc_entries([dbc_file])

    total_files = len(files)
    if stack_size == 1:
        for i, (f, d) in enumerate(files):
            count_str = f"[{i+1} of {total_files}]"

            try:
                mdf = MDF(f)

                try:
                    start = time.time()
                    logging.info(
                        f" ⏳ {count_str} Decoding file {shortpath(f)} ..."
                    )
                    decoded = mdf.extract_bus_logging(
                        database_files={"CAN": can_dbc_files},
                        ignore_value2text_conversion=True,
                    )

                    logging.info(
                        f" ✅ {count_str} Decoded file {shortpath(f)} in {get_time_str(start)}"
                    )

                    if not list(decoded.iter_channels()):
                        logging.warning(
                            f" ⚠️ {count_str} No signals found in file {shortpath(f)} after decoding with DBC {shortpath(dbc_file)}"
                        )
                        continue
                    else:
                        result = send_decoded(
                            decoded=decoded,
                            server=server,
                            job=B3SR_JOB,
                            skip_signal_fn=None,
                            skip_signal_range_check=skip_signal_range_check,
                            batch_size=max_batch_size,
                        )

                        for s, v in result.items():
                            sent_stats[s] = sent_stats.get(s, 0) + v
                except Exception as e:
                    logging.error(
                        f" ❌ {count_str} Failed to convert file {shortpath(f)} to engineering values: {e}"
                    )
                    continue

            except Exception as e:
                logging.error(
                    f" ❌ {count_str} Failed to read file {shortpath(f)}: {e}"
                )
                continue
    else:

        def batch(lst, n):
            for i in range(0, len(lst), n):
                yield i, lst[i : i + n]

        def process_batch(files, stack_msg):
            logging.info(f" ⏳ {stack_msg} Stacking {len(files)} files ...")
            start = time.time()
            try:
                mdf = MDF().stack(files)
                logging.info(
                    f" ✅ {stack_msg} Stacked {len(files)} files in {get_time_str(start)}"
                )
                logging.info(f" ⏳ {stack_msg} Decoding stacked MDF ...")
                start = time.time()

                try:
                    decoded = mdf.extract_bus_logging(
                        database_files={"CAN": can_dbc_files},
                        ignore_value2text_conversion=True,
                    )

                    logging.info(
                        f" ✅ {stack_msg} Decoded stacked MDF in {get_time_str(start)}"
                    )

                    if not list(decoded.iter_channels()):
                        logging.warning(
                            f" ⚠️ {stack_msg} No signals found in stacked MDF after decoding with DBC {shortpath(dbc_file)}"
                        )
                        return
                    else:
                        result = send_decoded(
                            decoded=decoded,
                            server=server,
                            job=B3SR_JOB,
                            skip_signal_fn=None,
                            skip_signal_range_check=skip_signal_range_check,
                            batch_size=max_batch_size,
                        )

                        for s, v in result.items():
                            sent_stats[s] = sent_stats.get(s, 0) + v
                except Exception as e:
                    logging.error(
                        f" ❌ {stack_msg} Failed to decode stacked MDF: {e}"
                    )
                    return
            except Exception as e:
                logging.error(f" ❌ {stack_msg} Failed to stack files: {e}")
                return

        _files = [f for f, d in files]
        for batch_idx, batch_files in batch(_files, stack_size):
            stack_msg = f"[{batch_idx}..{batch_idx + len(batch_files)} of {total_files}]"
            process_batch(files=batch_files, stack_msg=stack_msg)

    return sent_stats


def main_post_s3_streaming_to_victoriametrics(
    server: str,
    s3_bucket: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    s3_prefix: str = "",
    newest_first: bool = True,
    s3_info_list: list[dict] | None = None,
    streaming_strategy: Literal["auto", "memory", "tempfile"] = "auto",
    memory_fraction: float = 0.35,
    decode_overhead_factor: float = 2.5,
    max_active_files: int = 1,
    max_batch_size: int = 10_000,
    skip_signal_range_check: bool = False,
    posted_after: datetime | None = None,
):
    start_ts = time.time()

    if start_date is None:
        start_date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    if end_date is None:
        end_date = datetime.now(tz=timezone.utc)

    if s3_info_list is None:
        summary = get_new_mf4_files_summary_from_s3(
            bucket_names=s3_bucket,
            start_time=start_date,
            end_time=end_date,
            Prefix=s3_prefix,
            posted_after=posted_after,
        )
        s3_info_list = summary["buckets"].get(s3_bucket, {}).get("files", [])

    if not s3_info_list:
        logging.warning("⚠️ No B3SR S3 files found to stream.")
        return {}

    s3_info_list = [
        item
        for item in s3_info_list
        if isinstance(item, dict)
        and isinstance(item.get("Key", None), str)
        and item["Key"].lower().endswith(".mf4")
    ]

    if not s3_info_list:
        logging.warning("⚠️ No B3SR MF4 S3 files left after filtering.")
        return {}

    s3_info_list.sort(
        key=lambda x: x.get("Timestamp", datetime.min.replace(tzinfo=timezone.utc)),
        reverse=newest_first,
    )

    dbc_files = normalize_dbc_entries([get_dbc_file_path()])
    if not dbc_files:
        logging.error("❌ B3SR DBC files could not be normalized.")
        return {}

    selected_strategy, profile = _select_s3_streaming_strategy(
        s3_info_list=s3_info_list,
        requested_strategy=streaming_strategy,
        memory_fraction=memory_fraction,
        decode_overhead_factor=decode_overhead_factor,
        max_active_files=max_active_files,
    )
    logging.info(
        "🧠 B3SR S3 streaming preflight | files=%s largest=%sB "
        "worst_parallel=%sB projected_peak=%sB available_ram=%sB "
        "ram_budget=%sB strategy=%s",
        profile["file_count"],
        profile["largest_file_bytes"],
        profile["worst_parallel_bytes"],
        profile["projected_peak_bytes"],
        profile["available_ram_bytes"],
        profile["ram_budget_bytes"],
        selected_strategy,
    )

    max_retries = 10
    retry_interval_seconds = 60
    for attempt in range(1, max_retries + 1):
        if is_victoriametrics_online(server):
            break
        if attempt < max_retries:
            logging.warning(
                f" -> ⚠️ {server} not available (attempt {attempt}/{max_retries}). Retrying in 1 minute..."
            )
            time.sleep(retry_interval_seconds)
        else:
            logging.error(
                f" -> ❌ {server} not available after {max_retries} attempts. Exiting..."
            )
            exit(1)

    s3c = create_s3_client(max_pool_connections=max(1, max_active_files))
    total_counts: dict[str, int] = {}
    total = len(s3_info_list)

    for idx, item in enumerate(s3_info_list, start=1):
        key = item["Key"]
        count_str = f"[{idx} of {total}]"
        start_ts_single = time.time()
        result: dict[str, int] = {}

        if selected_strategy == "memory":
            blob = download_file_bytes_from_s3(
                bucket_name=s3_bucket,
                key=key,
                s3_client=s3c,
            )
            if blob is None:
                continue
            try:
                with MDF(io.BytesIO(blob)) as mdf:
                    result = _decode_and_send_b3sr_mdf(
                        mdf=mdf,
                        server=server,
                        dbc_files=dbc_files,
                        skip_signal_range_check=skip_signal_range_check,
                        max_batch_size=max_batch_size,
                    )
            except Exception as e:
                logging.error(f"❌ {count_str} Error processing in memory {key}: {e}")
            del blob
        else:
            tmp_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".mf4", delete=False) as tmp_file:
                    tmp_path = Path(tmp_file.name)
                ok = download_file_to_path_from_s3(
                    bucket_name=s3_bucket,
                    key=key,
                    local_path=tmp_path,
                    s3_client=s3c,
                )
                if not ok:
                    continue
                with MDF(tmp_path) as mdf:
                    result = _decode_and_send_b3sr_mdf(
                        mdf=mdf,
                        server=server,
                        dbc_files=dbc_files,
                        skip_signal_range_check=skip_signal_range_check,
                        max_batch_size=max_batch_size,
                    )
            except Exception as e:
                logging.error(f"❌ {count_str} Error processing temp file {key}: {e}")
            finally:
                if tmp_path and tmp_path.exists():
                    tmp_path.unlink()

        sent = sum(result.values())
        logging.info(
            f"✅ {count_str} streamed {shortpath(Path(key))} in {get_time_str(start_ts_single)} ({convert_to_eng(sent)} samples)"
        )
        for signal_name, count in result.items():
            total_counts[signal_name] = total_counts.get(signal_name, 0) + count

    end_ts = time.time()
    total_signals_sent = len(total_counts.keys())
    total_samples_sent = sum(total_counts.values())
    logging.info(
        f"🏁 Streamed {total} B3SR S3 files in {get_time_str(start_ts, end_ts)} "
        f"({total_signals_sent} signals | {convert_to_eng(total_samples_sent)} samples | "
        f"{convert_to_eng(total_samples_sent / max(end_ts - start_ts, 1e-9))} samples/s)."
    )

    return total_counts


def get_files_in_range(
    dir_path: Path, start: datetime, end: datetime
) -> list[CSVContent]:
    """
    Get all MF4 files in the given directory whose timestamps are within the specified range.

    :param dir_path: The directory to search for MF4 files.
    :type dir_path: Path
    :param start: The start datetime (inclusive).
    :type start: datetime
    :param end: The end datetime (exclusive).
    :type end: datetime
    :return: A list of tuples containing the file path and its corresponding timestamp.
    :rtype: list[CSVContent]
    """
    files_in_range: list[CSVContent] = []

    mf4_files_iterator = dir_path.rglob("*.MF4")

    with ThreadPoolExecutor() as executor:
        futures = []
        for p in mf4_files_iterator:
            futures.append(
                executor.submit(
                    lambda path: (path, get_mdf_start_time(path)), p
                )
            )

        for future in as_completed(futures):
            result = future.result()

            if result:
                p, start_time = result

                if p in [_p for _p, _ in files_in_range]:
                    continue

                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                if start_time and (start_time >= start and start_time <= end):
                    files_in_range.append((p, start_time))

    return files_in_range


def main_post_to_victoriametrics(
    server: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    newest_first: bool = True,
    skip_signal_range_check: bool = False,
):
    start_ts = time.time()

    if start_date is None:
        start_date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    if end_date is None:
        end_date = datetime.now(tz=timezone.utc)

    logging.info(
        f" 📂 Scanning {shortpath(canedge_folder)} for B3SR MF4 files ..."
    )

    files: list[CSVContent] = get_files_in_range(
        dir_path=canedge_folder, start=start_date, end=end_date
    )

    if newest_first:
        files.sort(key=lambda x: x[1], reverse=True)

    logging.info(
        f" ✅ Found {len(files)} B3SR MF4 files between {start_date} and {end_date} in {get_time_str(start_ts)}"
    )

    logging.info(
        f" 🌐 Checking for server availability to VictoriaMetrics at {server} ..."
    )
    if not is_victoriametrics_online(server):
        logging.error(f" -> ❌ {server} not available. Exiting...")
        exit(1)

    logging.info(f" -> ✅ {server} is online. Sending files...")
    total_counts = send_files_to_victoriametrics(
        server=server,
        files=files,
        stack_size=20,
        skip_signal_range_check=skip_signal_range_check,
    )
    total_signals_sent = len(total_counts.keys())
    total_samples_sent = sum(total_counts.values())
    end_ts = time.time()

    logging.info(
        f" ✔️  Sent {total_signals_sent} signals {get_time_str(start_ts, end_ts)} ({convert_to_eng(total_samples_sent)} samples | {convert_to_eng(total_samples_sent / (end_ts - start_ts))} samples/s)."
    )


def _get_available_ram_bytes() -> int | None:
    if os.name == "nt":
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return int(stat.ullAvailPhys)
        return None

    if sys.platform == "darwin":
        try:
            out = subprocess.check_output(
                ["vm_stat"], text=True, stderr=subprocess.DEVNULL
            )
            page_size = 4096
            m = re.search(r"page size of (\d+) bytes", out)
            if m:
                page_size = int(m.group(1))

            pages = 0
            for key in ("Pages free", "Pages inactive", "Pages speculative"):
                mm = re.search(rf"{key}:\s+(\d+)\.", out)
                if mm:
                    pages += int(mm.group(1))
            if pages > 0:
                return int(pages * page_size)
        except Exception:
            return None

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        avail_pages = os.sysconf("SC_AVPHYS_PAGES")
        return int(page_size * avail_pages)
    except Exception:
        return None


def _select_s3_streaming_strategy(
    s3_info_list: list[dict],
    requested_strategy: Literal["auto", "memory", "tempfile"],
    memory_fraction: float,
    decode_overhead_factor: float,
    max_active_files: int,
) -> tuple[Literal["memory", "tempfile"], dict[str, int | float | None]]:
    sizes = [
        int(item["Size"])
        for item in s3_info_list
        if isinstance(item, dict) and "Size" in item
    ]
    sizes.sort(reverse=True)

    max_active = max(1, max_active_files)
    worst_parallel_bytes = sum(sizes[:max_active]) if sizes else 0
    projected_peak_bytes = int(worst_parallel_bytes * decode_overhead_factor)
    available_ram_bytes = _get_available_ram_bytes()
    ram_budget_bytes = (
        int(available_ram_bytes * memory_fraction)
        if available_ram_bytes is not None
        else None
    )

    if requested_strategy == "memory":
        selected: Literal["memory", "tempfile"] = "memory"
    elif requested_strategy == "tempfile":
        selected = "tempfile"
    else:
        if ram_budget_bytes is not None and projected_peak_bytes <= ram_budget_bytes:
            selected = "memory"
        else:
            selected = "tempfile"

    profile: dict[str, int | float | None] = {
        "file_count": len(sizes),
        "largest_file_bytes": sizes[0] if sizes else 0,
        "worst_parallel_bytes": worst_parallel_bytes,
        "projected_peak_bytes": projected_peak_bytes,
        "available_ram_bytes": available_ram_bytes,
        "ram_budget_bytes": ram_budget_bytes,
        "max_active_files": max_active,
        "decode_overhead_factor": decode_overhead_factor,
    }
    return selected, profile


def _decode_and_send_b3sr_mdf(
    mdf: MDF,
    server: str,
    dbc_files: list[DbcFileType],
    skip_signal_range_check: bool,
    max_batch_size: int,
) -> dict[str, int]:
    decoded = mdf.extract_bus_logging(
        database_files={"CAN": dbc_files},
        ignore_value2text_conversion=True,
    )
    if not list(decoded.iter_channels()):
        return {}
    return send_decoded(
        decoded=decoded,
        server=server,
        job=B3SR_JOB,
        skip_signal_fn=None,
        skip_signal_range_check=skip_signal_range_check,
        batch_size=max_batch_size,
    )


def parse_time_offset(offset_str: str) -> timedelta:
    """
    Parses a time offset string like '10m', '2h', '1d' and returns a timedelta.
    Only supports negative offsets.
    """
    match = re.match(r"(\d+)([smhd])", offset_str)
    if not match:
        raise ValueError(f"Invalid offset format: {offset_str}")
    value, unit = match.groups()
    value = int(value)
    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown time unit: {unit}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Send B3SR MF4 files from CANEdge folder to VictoriaMetrics server."
    )
    parser.add_argument(
        "--server",
        type=str,
        required=False,
        default=server_vm_b3sr,
        help="The VictoriaMetrics server URL.",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="today",
        help="Start time offset (e.g. '10m', '2h', '1d') from now",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="now",
        help="End time offset (e.g. '10m', '2h', '1d') from now or 'now'",
    )
    parser.add_argument(
        "--oldest-first",
        action="store_true",
        help="Process oldest files first.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Send to test node (server_vm_test_dump) instead of main B3SR node.",
    )
    parser.add_argument(
        "--s3-streaming",
        action="store_true",
        help="Stream MF4 files directly from S3 instead of scanning local disk.",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default="",
        help="S3 bucket for streaming mode.",
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="",
        help="Optional S3 prefix for streaming mode.",
    )
    parser.add_argument(
        "--streaming-strategy",
        type=str,
        choices=["auto", "memory", "tempfile"],
        default="auto",
        help="Streaming strategy to use for S3 mode.",
    )
    parser.add_argument(
        "--memory-fraction",
        type=float,
        default=0.35,
        help="Fraction of available RAM allowed for streaming mode.",
    )
    parser.add_argument(
        "--decode-overhead-factor",
        type=float,
        default=2.5,
        help="Decode RAM overhead multiplier for streaming mode.",
    )
    parser.add_argument(
        "--max-active-files",
        type=int,
        default=1,
        help="Max S3 objects to consider in preflight memory estimate.",
    )
    parser.add_argument(
        "--skip-signal-range-check",
        action="store_true",
        help="Skip signal range checks while sending decoded data.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Send all data without checking VictoriaMetrics for existing samples.",
    )
    parser.add_argument(
        "--dbc-folder",
        type=str,
        default="",
        help="DBC folder path, or 'old'/'compatibility' for workstation lookup. Defaults to decoder/B3SR/dbc.",
    )

    args = parser.parse_args()
    DBC_FOLDER_OVERRIDE = args.dbc_folder
    skip_signal_range_check = args.backfill or args.skip_signal_range_check

    server = server_vm_test_dump if args.test else server_vm_b3sr

    # Parse start time
    now = datetime.now().astimezone(ZoneInfo("America/Vancouver"))
    start_date = parse_time_arg(args.start, now)

    # Parse end time
    end_date = parse_time_arg(args.end, now, allow_today=False)

    if args.s3_streaming:
        if not args.s3_bucket.strip():
            parser.error("--s3-bucket is required when --s3-streaming is set")
        main_post_s3_streaming_to_victoriametrics(
            server=server,
            s3_bucket=args.s3_bucket,
            start_date=start_date,
            end_date=end_date,
            s3_prefix=args.s3_prefix,
            newest_first=args.oldest_first is False,
            streaming_strategy=args.streaming_strategy,
            memory_fraction=args.memory_fraction,
            decode_overhead_factor=args.decode_overhead_factor,
            max_active_files=args.max_active_files,
            skip_signal_range_check=skip_signal_range_check,
        )
    else:
        main_post_to_victoriametrics(
            server=server,
            start_date=start_date,
            end_date=end_date,
            newest_first=args.oldest_first is False,
            skip_signal_range_check=skip_signal_range_check,
        )
