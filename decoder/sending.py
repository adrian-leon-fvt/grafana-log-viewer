from asammdf import MDF, Signal
import requests
from pathlib import Path
from datetime import datetime, timedelta
import time

victoriametrics_url = "http://localhost:8428/api/v1/import/prometheus"


def get_mf4_files(directory: Path | str):
    """
    Get all MDF4 files in the specified directory.
    """

    if not isinstance(directory, Path):
        directory = Path(directory)

    return list(directory.rglob("*.[mM][fF]4"))


def get_channel_data(signal: Signal) -> tuple[str, int]:
    display_names = list(signal.display_names.keys())
    message = display_names[1].split(".")[0]
    can_id = display_names[2].split(" ")[0].split("ID=")[1]
    return message, int(can_id, 16)


def make_metric_line(
    metric_name: str,
    message: str,
    can_id: int,
    unit: str,
    value: float,
    timestamp: datetime | float,
    job: str = "",
) -> str:
    # Format the metric line for Prometheus
    return f'{metric_name}{{job="{job}",message="{message}",can_id="{can_id:X}",unit="{unit}"}} {value} {timestamp.timestamp() if type(timestamp) is datetime else timestamp}\n'


def send_signal(signal: Signal, start_time: datetime, job: str):
    message, can_id = get_channel_data(signal)
    metric_name = signal.name.replace(" ", "_")
    unit = signal.unit if signal.unit else ""

    print(f"  => Sending {metric_name} ...", end="", flush=True)
    start = time.time()
    batch: list[str] = []
    batch_size = 10000
    for sample, ts in zip(signal.samples, signal.timestamps):
        data = make_metric_line(
            metric_name,
            message,
            can_id,
            unit,
            sample,
            start_time + timedelta(seconds=ts),
        )
        batch.append(data)
        if len(batch) >= batch_size:
            try:
                requests.post(victoriametrics_url, data="".join(batch))
            except Exception as e:
                print(f"\nError sending batch: {e}")
            batch = []
            time.sleep(0.1)
    if batch:
        try:
            requests.post(victoriametrics_url, data="".join(batch))
        except Exception as e:
            print(f"\nError sending final batch: {e}")
    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    time_str = f"{mins}m{secs}s" if mins else f"{secs}s"
    print(f"\r  => Sending {metric_name} ... sent in {time_str}   ")


def send_file(filename: Path, job: str | None = None):
    print(f"Sending {filename}")
    if not filename.exists():
        print(f"File {filename} does not exist.")
        return

    if not filename.is_file():
        print(f"{filename} is not a file.")
        return

    if not filename.suffix.lower() == ".mf4":
        print(f"{filename} is not a valid MDF4 file.")
        return

    with MDF(filename) as mdf:
        for sig in mdf.iter_channels():
            send_signal(sig, mdf.start_time, job=job if job else filename.stem)
