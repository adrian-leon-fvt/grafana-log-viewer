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
    server_vm_b3sr,
)
from decoder.s3_helper import *
import argparse
import re

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

CSVContent = tuple[Path, datetime]

B3SR_JOB = "B3SR"

canedge_folder = Path.joinpath(
    get_windows_home_path(),
    "Epiroc",
    "O365 Epiroc R&D B3SR for D65 Electrification - General",
    "06_Design",
    "9. Testing",
    "13. TMS Testing",
    "CANEdge",
)


def shortpath(p: Path) -> str:
    if len(p.parts) < 4:
        return p.as_posix()
    return "../" + "/".join(p.parts[-3:])


def get_dbc_file_path() -> Path:
    return canedge_folder.joinpath("b3sr_base.dbc")


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
                        database_files={"CAN": [(dbc_file, 0)]},
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
                        database_files={"CAN": [(dbc_file, 0)]},
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
        skip_signal_range_check=False,
    )
    total_signals_sent = len(total_counts.keys())
    total_samples_sent = sum(total_counts.values())
    end_ts = time.time()

    logging.info(
        f" ✔️  Sent {total_signals_sent} signals {get_time_str(start_ts, end_ts)} ({convert_to_eng(total_samples_sent)} samples | {convert_to_eng(total_samples_sent / (end_ts - start_ts))} samples/s)."
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

    args = parser.parse_args()

    # Parse start time
    now = datetime.now().astimezone(ZoneInfo("America/Vancouver"))
    if args.start == "today":
        start_date = datetime.today().astimezone(now.tzinfo)
    else:
        start_date = now - parse_time_offset(args.start)

    # Parse end time
    if args.end == "now":
        end_date = now
    else:
        end_date = now - parse_time_offset(args.end)

    main_post_to_victoriametrics(
        server=args.server,
        start_date=start_date,
        end_date=end_date,
        newest_first=args.oldest_first is False,
    )
