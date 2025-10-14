from asammdf import MDF, Signal
from asammdf.blocks.types import DbcFileType, BusType
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time
from collections.abc import Iterable
from typing import Sequence, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import json
import os
import logging

from .config import *
from .utils import *
from decoder.livelogger.CANReader import CANReader
from decoder.GUI.DBCDecoder import DBCDecoder

os.environ["NO_PROXY"] = "localhost"  # Bypass proxy for VictoriaMetrics


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


def check_signal_range(
    signal: Signal, start_time: datetime, server: str
) -> Signal | None:
    """
    Checks if the signal timestamps already exist in the database, returns a Signal object only with timestamps not already there,
    """

    logger = logging.getLogger("check_signal_range")
    setup_simple_logger(logger, format=LOG_FORMAT)

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
        resp = requests.get(server + vmapi_export, params=params, timeout=10)
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
        logger.warning(
            f"⚠️ Warning: Could not check Signal range for {metric_name}: {e}"
        )
    return signal


def send_signal(
    signal: Signal,
    start_time: datetime,
    job: str,
    server: str,
    print_metric_line: bool = False,
    send_signal: bool = True,
    skip_signal_range_check: bool = False,
    batch_size: int = 250_000,
) -> int:
    """
    Send a single signal to VictoriaMetrics.
    Options:
    - signal: The Signal object to send.
    - start_time: The datetime representing the start time of the MDF file.
    - job: Label to recognize what machine this signal belongs to.
    - print_metric_line: If True, prints the metric lines before sending (default: False).
    - send_signal: If True, actually sends the data to VictoriaMetrics (default: True).
    - skip_signal_range_check: If True, skips checking if the signal data already exists in the database (default: False).
    - batch_size: Number of samples to send in each HTTP POST batch (default: 250,000).
    """

    logger = logging.getLogger("send_signal")
    setup_simple_logger(logger)

    message, metric_name = get_channel_data(signal)

    num_of_samples_sent = 0

    _signal: Signal | None = signal
    if not skip_signal_range_check:
        _signal = check_signal_range(
            signal=signal,
            start_time=start_time,
            server=server,
        )

    if _signal is None or len(_signal.timestamps) < 1:
        logger.info(f"  ☑️ No new data for {signal.name}, skipping ...")
        return num_of_samples_sent

    unit = _signal.unit if _signal.unit else ""
    _sig_start_str = start_time + timedelta(seconds=_signal.timestamps[0])
    _sig_end_str = start_time + timedelta(seconds=_signal.timestamps[-1])
    _time_str = f"{_sig_start_str.isoformat()} - {_sig_end_str.isoformat()}, {len(_signal.timestamps)} samples"

    logger.info(f"  📨 Sending {metric_name} [{_time_str}] ...")
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
        num_of_samples_sent += 1
        if len(batch) >= batch_size:
            try:
                if print_metric_line:
                    logger.info("".join(batch))
                if send_signal:
                    requests.post(server + vmapi_import_prometheus, data="".join(batch))
            except Exception as e:
                logger.error(f"‼️ Error sending batch: {e}")
            batch = []
            time.sleep(0.01)  # Avoid overwhelming the server
    if batch:
        try:
            requests.post(server + vmapi_import_prometheus, data="".join(batch))
        except Exception as e:
            logger.error(f"‼️ Error sending final batch: {e}")

    time_str = get_time_str(start)
    end_ts = time.time()
    logger.info(
        f"  📨 Sent {metric_name} in {time_str} ({convert_to_eng(num_of_samples_sent)} samples | {convert_to_eng(num_of_samples_sent / (end_ts - start))} samples/s)"
    )
    return num_of_samples_sent


def send_signal_using_json_lines(
    signal: Signal,
    start_time: datetime,
    job: str,
    server: str,
    print_metric_line: bool = False,
    send_signal: bool = True,
    skip_signal_range_check: bool = False,
    batch_size: int = 250_000,
) -> int:
    """
    Send a single signal to VictoriaMetrics using JSON lines.
    Options:
    - signal: The Signal object to send.
    - start_time: The datetime representing the start time of the MDF file.
    - job: Label to recognize what machine this signal belongs to.
    - print_metric_line: If True, prints the metric lines before sending (default: False).
    - send_signal: If True, actually sends the data to VictoriaMetrics (default: True).
    - skip_signal_range_check: If True, skips checking if the signal data already exists in the database (default: False).
    - batch_size: Number of samples to send in each HTTP POST batch (default: 250,000).
    """

    logger = logging.getLogger("send_signal_using_json_lines")
    setup_simple_logger(logger)

    message, metric_name = get_channel_data(signal)

    num_of_samples_sent = 0

    _signal: Signal | None = signal
    if not skip_signal_range_check:
        _signal = check_signal_range(
            signal=signal,
            start_time=start_time,
            server=server,
        )

    if _signal is None or len(_signal.timestamps) < 1:
        logger.info(f"  ☑️ No new data for {metric_name}, skipping ...")
        return num_of_samples_sent

    unit = _signal.unit if _signal.unit else ""
    _sig_start_str = start_time + timedelta(seconds=_signal.timestamps[0])
    _sig_end_str = start_time + timedelta(seconds=_signal.timestamps[-1])
    values: list[float] = []
    timestamps: list[float | datetime] = []
    for sample, ts in zip(_signal.samples, _signal.timestamps):
        if not is_valid_sample(sample):  # Check if sample is not float (e.g. string)
            continue  # Skip this sample
        values.append(float(sample))
        timestamps.append((start_time + timedelta(seconds=ts)).timestamp())
    _time_str = f"{_sig_start_str.isoformat()} - {_sig_end_str.isoformat()}, {len(timestamps)} samples"

    if len(values) < 1 or len(timestamps) < 1:
        logger.info(f"  ℹ️ No valid numeric data for {metric_name}, skipping ...")
        return num_of_samples_sent

    logger.info(f"  📨 Sending {metric_name} [{_time_str}] ...")
    lines = make_list_of_vm_json_line_format(
        metric_name=metric_name,
        message=message,
        unit=unit,
        values=values,
        timestamps=timestamps,
        job=job if job else "",
        batch_size=batch_size,
    )

    start = time.time()
    for line in lines:
        try:
            if print_metric_line:
                logger.info(line)
            if send_signal:
                requests.post(server + vmapi_import_prometheus, data=line)
            num_of_samples_sent += line.count("values")
            time.sleep(0.01)  # Avoid overwhelming the server
        except Exception as e:
            logger.error(f"‼️ Error sending batch: {e}")

    time_str = get_time_str(start)
    end_ts = time.time()
    logger.info(
        f"  📨 Sent {metric_name} in {time_str} ({convert_to_eng(num_of_samples_sent)} samples | {convert_to_eng(num_of_samples_sent / (end_ts - start))} samples/s)"
    )
    return num_of_samples_sent


def send_file(
    filename: Path,
    server: str,
    job: str | None = None,
    skip_signal_range_check: bool = True,
    skip_signal_fn: Optional[Callable[[str], bool]] = None,
    batch_size: int = 250_000,
) -> dict[str, int]:
    logger = logging.getLogger("send_file")
    setup_simple_logger(logger, format=LOG_FORMAT)

    logger.info(f"Sending {filename}")
    signals_sample_count: dict[str, int] = {}
    if not filename.exists():
        logger.warning(f"📃 File {filename} does not exist.")
        return signals_sample_count

    if not filename.is_file():
        logger.warning(f"📃 {filename} is not a file.")
        return signals_sample_count

    if not filename.suffix.lower() == ".mf4":
        logger.warning(f"📃 {filename} is not a valid MDF4 file.")
        return signals_sample_count

    try:
        with MDF(filename) as mdf:
            for sig in mdf.iter_channels():
                if skip_signal_fn is not None and skip_signal_fn(sig.name):
                    continue

                samples_sent = send_signal(
                    signal=sig,
                    start_time=mdf.start_time,
                    job=job if job else filename.stem,
                    skip_signal_range_check=skip_signal_range_check,
                    batch_size=batch_size,
                    server=server,
                )

                if samples_sent > 0:
                    signals_sample_count[sig.name] = samples_sent

    except Exception as e:
        logger.error(f"❌ Error processing {filename}: {e}")

    return signals_sample_count


def send_decoded(
    decoded: Path | MDF,
    server: str,
    job: str | None = None,
    skip_signal_range_check: bool = True,
    skip_signal_fn: Optional[Callable[[str], bool]] = None,
    batch_size: int = 250_000,
) -> dict[str, int]:
    """
    Send a decoded MDF4 file to VictoriaMetrics.
    """
    logger = logging.getLogger("send_decoded")
    setup_simple_logger(logger, format=LOG_FORMAT)

    signals_sample_count: dict[str, int] = {}

    if isinstance(decoded, Path):
        send_file(
            filename=decoded,
            job=job,
            server=server,
        )
    elif isinstance(decoded, MDF):
        for sig in decoded.iter_channels():
            _job = job if job else "-".join(decoded.name.parts)
            if skip_signal_fn is not None and skip_signal_fn(sig.name):
                continue

            signals_sent = send_signal(
                signal=sig,
                start_time=decoded.start_time,
                job=_job,
                skip_signal_range_check=skip_signal_range_check,
                batch_size=batch_size,
                server=server,
            )

            if signals_sent > 0:
                signals_sample_count[sig.name] = signals_sent

    else:
        logger.warning("⚠️ Invalid decoded input type. Must be Path or MDF instance.")

    return signals_sample_count


def send_decoded_threded(
    decoded: MDF,
    server: str,
    job: str | None = None,
    skip_signal_range_check: bool = True,
    skip_signal_fn: Optional[Callable[[str], bool]] = None,
    batch_size: int = 250_000,
) -> dict[str, int]:
    """
    Send a decoded MDF4 file to VictoriaMetrics using threading.
    """

    logger = logging.getLogger("send_decoded_threaded")
    setup_simple_logger(logger, format=LOG_FORMAT)

    signals_sample_count: dict[str, int] = {}

    if not isinstance(decoded, MDF):
        logger.warning("⚠️ Invalid decoded input type. Must be MDF instance.")
        return signals_sample_count

    with ThreadPoolExecutor() as executor:
        future_to_signal = {
            executor.submit(
                send_signal,
                signal=sig,
                start_time=decoded.start_time,
                job=job if job else "-".join(decoded.name.parts),
                skip_signal_range_check=skip_signal_range_check,
                batch_size=batch_size,
                server=server,
            ): sig
            for sig in decoded.iter_channels()
            if skip_signal_fn is None or not skip_signal_fn(sig.name)
        }

        for future in as_completed(future_to_signal):
            sig = future_to_signal[future]
            try:
                samples_sent = future.result()
                if samples_sent > 0:
                    signals_sample_count[sig.name] = samples_sent
            except Exception as e:
                logger.error(f"❌ Error sending signal {sig.name}: {e}")

    return signals_sample_count


def decode_and_send(
    files: list[Path],
    dbc_files: Sequence[DbcFileType],
    server: str,
    job: str = "test_job",
    concat_first: bool = True,
    concat_msg: str = "Concat",
    skip_signal_range_check: bool = True,
    skip_signal_fn: Optional[Callable[[str], bool]] = None,
    batch_size=250_000,
) -> dict[str, int]:
    """
    Decode all MDF4 files in the specified directory and send their data to VictoriaMetrics.
    """
    logger = logging.getLogger("decode_and_send")
    setup_simple_logger(logger, format=LOG_FORMAT)

    signals_sample_count: dict[str, int] = {}

    if not files:
        logger.warning("⚠️ No directory or files specified.")
        return signals_sample_count

    database_files: dict[BusType, Iterable[DbcFileType]] = {"CAN": dbc_files}

    if not dbc_files:
        logger.error("⚠️ No DBC files specified.")
        return signals_sample_count

    if concat_first and len(files) > 1:
        mdf = MDF()
        try:
            logger.info(f" ⏳ {concat_msg}: Concatenating {len(files)} files")
            start = time.time()
            mdf = MDF().concatenate(files)
            logger.info(f" ✅ {concat_msg}: Concatenated in {time.time() - start:.3f}s")

            try:
                logger.info(f" ⏳ {concat_msg}: Decoding concatenated files")
                start = time.time()
                decoded = mdf.extract_bus_logging(
                    database_files, ignore_value2text_conversion=True
                )
                logger.info(f" ✅ {concat_msg}: Decoded in {time.time() - start:.3f}s")
                if list(decoded.iter_channels()):
                    result = send_decoded(
                        decoded=decoded,
                        job=job,
                        skip_signal_fn=skip_signal_fn,
                        skip_signal_range_check=skip_signal_range_check,
                        batch_size=batch_size,
                        server=server,
                    )
                    for k, v in result.items():
                        signals_sample_count[k] = signals_sample_count.get(k, 0) + v
                else:
                    logger.warning("⚠️ No signals found, skipping sending.")

            except Exception as e:
                logger.error(f"❌ Error decoding concatenated files: {e}")
        except Exception as e:
            logger.error(f"❌ Error concatenating files: {e}")

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
                logger.info(f" ⏳ Decoding ../{_dispname} ...")
                decoded = mdf.extract_bus_logging(
                    database_files, ignore_value2text_conversion=True
                )
                logger.info(f" ✅ Decoded ../{_dispname} in {time.time() - start:.3f}s")
                if list(decoded.iter_channels()):
                    result = send_decoded(
                        decoded=decoded,
                        job=job,
                        skip_signal_fn=skip_signal_fn,
                        skip_signal_range_check=skip_signal_range_check,
                        batch_size=batch_size,
                        server=server,
                    )

                    for k, v in result.items():
                        signals_sample_count[k] = signals_sample_count.get(k, 0) + v
                else:
                    logger.warning(
                        f"⚠️ No signals found in {_dispname}, skipping sending."
                    )
            except Exception as e:
                logger.error(f"❌ Error decoding {_dispname}: {e}")
                continue

    return signals_sample_count


def livestream(server:str):
    logger = logging.getLogger("livestream")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)

    if is_victoriametrics_online(server):
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
                            requests.post(server + vmapi_import_prometheus, data="".join(data))
                        except Exception as e:
                            logging.error(f"\n ‼️ Error sending data: {e}")

            except KeyboardInterrupt:  # Shutting down properly
                break

        logger.info("Shutting down...")
        can_reader.shutdown()


def main():
    if LIVE_STREAMING:  # Streaming live CAN data
        livestream(LIVE_STREAMING_SERVER)


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
