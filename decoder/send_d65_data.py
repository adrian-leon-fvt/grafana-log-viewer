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

CSVContent = tuple[Path, Literal["Upper", "Lower"], datetime, datetime]


def shortpath(p: Path) -> str:
    return "../" + "/".join(p.parts[-3:])


def read_filtered_paths_file(
    filepath: Path | str,
) -> list[CSVContent]:
    ts = time.time()
    logging.info(f"📃 Reading filtered paths from {filepath} ... ")
    win_home = get_windows_home_path()
    try:
        with open(filepath, "r") as f:
            filtered = []
            for line in f.readlines():
                if line.strip():
                    path, seg_k, start, end = line.strip().split(";")

                    if seg_k in ["Upper", "Lower"]:
                        filtered.append(
                            (
                                Path.joinpath(win_home, path),
                                seg_k,
                                datetime.fromisoformat(start).astimezone(timezone.utc),
                                datetime.fromisoformat(end).astimezone(timezone.utc),
                            )
                        )
            logging.info(
                f"✅ Recovered {len(filtered)} filtered paths in {get_time_str(ts)}"
            )
            return filtered
    except Exception as e:
        logging.error(f"❌ Error reading filtered_paths.txt: {e}")
        return []


def save_preprocessed_paths_file(
    filtered: list[CSVContent],
    filepath: Path | str,
):
    ts = time.time()
    logging.info(f"💾 Saving {len(filtered)} filtered paths to {filepath} ... ")
    try:
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for file, seg_k, start, end in filtered:
                _file = re.sub(
                    r"^/mnt/[a-zA-Z]+/Users/[a-zA-Z0-9 _-]+/", "", str(file.as_posix())
                )
                _file = re.sub(r"[a-zA-Z]:/Users/[a-zA-Z0-9 _-]+/", "", _file)
                f.write(
                    f"{_file};{seg_k};{start.astimezone(timezone.utc).isoformat()};{end.astimezone(timezone.utc).isoformat()}\n"
                )

        logging.info(f"✅ Saved filtered paths in {get_time_str(ts)}")
    except Exception as e:
        logging.error(f"☹️ Error writing {filepath}: {e}")


def get_range(mdf: MDF) -> tuple[datetime, datetime] | None:
    channels_with_data = [ch for ch in mdf.iter_channels() if len(ch.timestamps) > 0]
    if len(channels_with_data) == 0:
        return None

    max_timestamp = max([ch.timestamps[-1] for ch in channels_with_data])
    return (
        mdf.start_time,
        mdf.start_time + timedelta(seconds=max_timestamp),
    )


def upper_or_lower(p: Path) -> Literal["Upper", "Lower", "None"]:
    parts = [part.lower() for part in p.parts]
    if "upper" in parts:
        return "Upper"
    elif "lower" in parts:
        return "Lower"
    else:
        return "None"


def preprocess_files() -> list[CSVContent]:
    """
    Will read the entire OneDrive directory and send all new files to VictoriaMetrics.
    """

    files: list[CSVContent] = []

    whp = get_windows_home_path()

    canedge_folder = Path.joinpath(
        whp, "Epiroc", "Rig Crew - Private - General", "5. Testing", "CANEdge"
    )

    if not canedge_folder.exists():
        logging.error(
            f" ❌ CANEdge folder NOT found: {canedge_folder} exists {canedge_folder.exists()}"
        )
        return files

    logging.info(f" ✅ CANEdge folder found: {canedge_folder}")

    def get_data(file: Path) -> Optional[CSVContent]:
        logging.info(f" 📃  Reading {shortpath(file)} ")
        if file.suffix.lower() == ".mf4":
            try:
                mdf = MDF(file)
                _range = get_range(mdf)
                k_seg = upper_or_lower(file)
                if _range is not None and k_seg != "None":
                    start_time, stop_time = _range
                    return (file, k_seg, start_time, stop_time)
                else:
                    return None
            except Exception as e:
                logging.warning(f" ⚠️  Error reading {shortpath(file)}: {e}")
                return None

        elif file.suffix.lower() == ".trc":
            try:
                log = LogReader(file)
                timestamps = [msg.timestamp for msg in log]
                k_seg = upper_or_lower(file)
                if timestamps and k_seg != "None":
                    start_time = datetime.fromtimestamp(min(timestamps))
                    stop_time = datetime.fromtimestamp(max(timestamps))
                    return (file, k_seg, start_time, stop_time)
                else:
                    return None
            except Exception as e:
                logging.warning(f" ⚠️  Error reading {shortpath(file)}: {e}")
                return None

    files_to_process = list(
        chain(
            canedge_folder.rglob("**/*.MF4"),
            canedge_folder.rglob("**/*.mf4"),
            # canedge_folder.rglob("**/*.trc"),
        )
    )

    with ThreadPoolExecutor() as executor:
        results = executor.map(get_data, files_to_process)
        for result in results:
            if result:
                f, seg_k, start_time, stop_time = result
                logging.info(
                    f" ✅  [{seg_k}] {shortpath(f)}: {start_time} - {stop_time}"
                )
                files.append((f, seg_k, start_time, stop_time))

    return files


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
    upper_files = [file for file, _, _, _ in upper_tuples]

    lower_tuples = [item for item in files if item[1] == "Lower"]
    lower_tuples.sort(key=lambda x: x[2])  # Sort by start time
    lower_files = [file for file, _, _, _ in lower_tuples]

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


def send_trace(server: str, file: Path, job: str, batch_size: int = 50_000):
    if file.suffix.lower() != ".trc":
        logging.error(f"❌ File is not a .trc file: {file}")
        return

    dbc_files = get_d65_dbc_files()
    upper_dbc_files = dbc_files["Upper"]
    lower_dbc_files = dbc_files["Lower"]

    db = Database()

    if job == "Upper":
        for dbc in upper_dbc_files:
            db.add_dbc_file(dbc)
    elif job == "Lower":
        for dbc in lower_dbc_files:
            db.add_dbc_file(dbc)

    log = LogReader(file)
    metrics: list[str] = []
    metrics_lock = Lock()
    metrics: list[str] = []

    def process_msg(msg):
        try:
            message: Message = db.get_message_by_frame_id(msg.arbitration_id)
            if message is None:
                return []

            signals: DecodeResultType = message.decode(msg.data)
            timestamp = datetime.fromtimestamp(msg.timestamp, tz=timezone.utc)

            if not isinstance(signals, dict):
                return []

            local_metrics = []
            for signal_name, value in signals.items():
                if skip_signal(signal_name):
                    continue

                if not isinstance(value, (int, float)):
                    continue

                signal: Signal = message.get_signal_by_name(signal_name)
                if signal is None:
                    continue

                unit = signal.unit if signal.unit else ""
                metric_line = make_metric_line(
                    metric_name=signal_name,
                    message=message.name,
                    unit=unit,
                    value=value,
                    timestamp=timestamp,
                    job=job,
                )
                local_metrics.append(metric_line)
            return local_metrics
        except Exception as e:
            logging.error(
                f"❌ Error processing message ID {getattr(msg, 'arbitration_id', 'unknown')}: {e}"
            )
            return []

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_msg, msg) for msg in log]

        for future in as_completed(futures):
            local_metrics = future.result()
            if local_metrics:
                with metrics_lock:
                    metrics.extend(local_metrics)
                    if len(metrics) >= batch_size:
                        batch_data = "".join(metrics)
                        try:
                            requests.post(
                                server + vmapi_import_prometheus, data=batch_data
                            )
                            metrics.clear()
                        except Exception as e:
                            logging.error(f"❌ Exception sending batch: {e}")

    # Send any remaining metrics
    if metrics:
        batch_data = "".join(metrics)
        try:
            requests.post(server + vmapi_import_prometheus, data=batch_data)
        except Exception as e:
            logging.error(f"❌ Exception sending final batch: {e}")


def read_s3_file(
    file_path: Path | str,
    start: datetime | str = "",
    end: datetime | str = "",
) -> list[dict]:
    logger = logging.getLogger("read_s3_file")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)

    if isinstance(file_path, str):
        file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"❌ File does not exist: {file_path}")
        return []

    with open(file_path, "r") as f:
        lines = f.readlines()
        files = []
        with ThreadPoolExecutor() as executor:
            futures = []
            for line in lines[1:]:  # Skip header
                parts = line.strip().split(",")
                if len(parts) != 4:
                    continue
                key, last_modified, size, timestamp = parts
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
                    files.append(result)

        return files

    return []


def get_d65_file_list_from_s3(
    start: datetime | str = "",
    end: datetime | str = "",
):
    logger = logging.getLogger("get_d65_file_list_from_s3,")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)

    files = get_mf4_files_from_s3(
        bucket_name=EESBuckets.S3_BUCKET_D65,
        start_time=start,
        end_time=end,
        max_workers=20,
    )

    logger.info(f" 🪣 Found {len(files)} .mf4 files in D65 S3 bucket.")

    save_csv(files, Path(r"D:/utils/grafana-log-viewer/decoder/d65_s3_files.csv"))


def main():
    preprocessed_path = r"D:/utils/grafana-log-viewer/decoder/d65_files.csv"
    files = read_filtered_paths_file(preprocessed_path)

    if files:
        start_ts = time.time()
        logging.info(
            f" ✔️  Using {len(files)} preprocessed files from {preprocessed_path}."
        )

        def filter_by_date(
            files: list[CSVContent],
            start_time: datetime,
            end_time: datetime,
        ) -> list[CSVContent]:
            return [
                (f, k_seg, start, end)
                for f, k_seg, start, end in files
                if (start <= end_time) and (start >= start_time)
            ]

        def filter_by_job(
            files: list[CSVContent], job: Literal["Upper", "Lower"]
        ) -> list[CSVContent]:
            return [
                (f, k_seg, start, end) for f, k_seg, start, end in files if k_seg == job
            ]

        start_date = datetime(
            year=2025,
            month=7,
            day=15,
            tzinfo=ZoneInfo("America/Vancouver"),
        )
        end_date = start_date + timedelta(days=1)

        _files = files
        # _files = filter_by_date(files, start_date, end_date)
        _files = filter_by_job(_files, "Lower")
        # _files = filter_by_job(_files, "Upper")

        logging.info(
            f" ✔️  Found to {len(_files)} files from {start_date} to {end_date}."
        )

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
    else:
        ans = (
            input(
                " ❓ No preprocessed files found. Do you want to preprocess now? (y/n): "
            )
            .strip()
            .lower()
        )

        start_ts = time.time()
        if ans[0] != "y":
            logging.info(" 👋  OK Bye.")
            exit(0)

        files = preprocess_files()
        save_preprocessed_paths_file(files, Path())

        logging.info(f" ✔️  Processed {len(files)} files in {get_time_str(start_ts)}.")


if __name__ == "__main__":
    main()
