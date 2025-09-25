import logging
from utils import get_windows_home_path, get_time_str
from pathlib import Path
from datetime import datetime, timedelta, timezone
from asammdf import MDF
from asammdf.blocks.types import DbcFileType, BusType, StrPath
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Literal
from time import time
from can import LogReader
from sending import decode_and_send
from zoneinfo import ZoneInfo
from config import LOG_FORMAT
import re
import os

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

CSVContent = tuple[Path, Literal["Upper", "Lower"], datetime, datetime]


def shortpath(p: Path) -> str:
    return "../" + "/".join(p.parts[-3:])


def read_filtered_paths_file(
    filepath: Path | str,
) -> list[CSVContent]:
    ts = time()
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
    ts = time()
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


def send_files_to_victoriametrics(
    files: list[CSVContent],
):
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

    upper_dbc_files: list[DbcFileType] = []
    upper_dbc_files += [
        (Path.joinpath(_d65_loc, "busses", dbc), 0) for dbc in d65_dbc_files["Upper"]
    ]
    upper_dbc_files += [
        (Path.joinpath(_d65_loc, "brightloop", "d65_brightloops.dbc"), 0)
    ]

    lower_dbc_files: list[DbcFileType] = []
    lower_dbc_files += [
        (Path.joinpath(_d65_loc, "busses", dbc), 0) for dbc in d65_dbc_files["Lower"]
    ]
    lower_dbc_files += []

    MAX_BATCH_COUNT = 10

    upper_tuples = [item for item in files if item[1] == "Upper"]
    upper_tuples.sort(key=lambda x: x[2])  # Sort by start time
    upper_files = [file for file, _, _, _ in upper_tuples]

    lower_tuples = [item for item in files if item[1] == "Lower"]
    lower_tuples.sort(key=lambda x: x[2])  # Sort by start time
    lower_files = [file for file, _, _, _ in lower_tuples]

    def batch(lst, n):
        for i in range(0, len(lst), n):
            yield i, lst[i : i + n]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        for idx, batch_files in batch(upper_files, MAX_BATCH_COUNT):
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
                    skip_signal_range_check=True,
                )
            )

        for idx, batch_files in batch(lower_files, MAX_BATCH_COUNT):
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
                    skip_signal_range_check=True,
                )
            )

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Error processing batch: {e}")


def main():
    preprocessed_path = r"D:/utils/grafana-log-viewer/decoder/d65_files.csv"
    files = read_filtered_paths_file(preprocessed_path)

    if files:
        start_ts = time()
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

        start_date = datetime(2025, 1, 1, tzinfo=ZoneInfo("America/Vancouver"))
        end_date = datetime(2025, 8, 1, tzinfo=ZoneInfo("America/Vancouver"))

        _files = filter_by_date(files, start_date, end_date)

        logging.info(
            f" ✔️  Found to {len(_files)} files from {start_date} to {end_date}."
        )

        send_files_to_victoriametrics(_files)

        logging.info(f" ✔️  All done in {get_time_str(start_ts)}.")
    else:
        ans = (
            input(
                " No preprocessed files found. Do you want to preprocess now? (y/n): "
            )
            .strip()
            .lower()
        )

        start_ts = time()
        if ans[0] != "y":
            logging.info(" Exiting.")
            exit(0)

        files = preprocess_files()
        save_preprocessed_paths_file(files, Path())

        logging.info(f" ✔️  Processed {len(files)} files in {get_time_str(start_ts)}.")


if __name__ == "__main__":
    main()
