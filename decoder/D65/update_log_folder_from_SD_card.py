import sys
import os
import shutil
from pathlib import Path

import logging

from typing import Literal

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent.parent))

from decoder.config import LOG_FORMAT
from decoder.D65.send_d65_data import upper_or_lower, MAC_LOWER, MAC_UPPER
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def get_d65_log_path() -> Path:
    """
    Get the path to the D65 log folder.
    """
    return Path(r"/mnt/d/d65files") if os.name != "nt" else Path(r"d:\d65files")


if __name__ == "__main__":
    if os.name == "nt":
        sd_card_root = Path(r"E:/")
        # sd_card_root = Path(
        #     r"C:\Users\CARAL\Epiroc\Rig Crew - Private - General\5. Testing\4. Lafarge Field Trial\2025-11-26 Machine Logs\Lower"
        # )
        # sd_card_root = Path(
        #     r"C:\Users\CARAL\Epiroc\Rig Crew - Private - General\5. Testing\4. Lafarge Field Trial\2025-11-26 Machine Logs"
        # )
    else:
        sd_card_root = Path("/mnt/f/")

    if not sd_card_root.exists():
        logging.error(f"SD card root path {sd_card_root} does not exist.")
        sys.exit(1)

    # CANEdge loggers have a structure like
    #       <root> / LOG / <mac_id> / <numbered folders> / <file_name>.MF4
    # We will crawl through the log folder, figure out if the MAC ID belongs
    # to Upper/Lower, and copy the files such that they match the following naming structure:
    #       D65files / <mac_id>_<folder_number>_<filename>.MF4

    d65_log_path = get_d65_log_path()

    # When downloading from S3, CANEdge appends a unique identifier to the filenames,
    # we wish to remove that because we cannot know what it will be
    filepaths_wo_identifier: list[Path] = [
        Path(str(f).split("-")[0] + ".MF4") for f in d65_log_path.rglob("*.MF4")
    ]

    def process_filename(
        filename: Path,
    ) -> str:
        return "_".join(filename.parts[-3:])

    mf4_files_iterator = sd_card_root.rglob("*.MF4")

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                shutil.copy, mf4_file, d65_log_path / process_filename(mf4_file)
            )
            for mf4_file in mf4_files_iterator
            if process_filename(mf4_file) not in filepaths_wo_identifier
        ]

        for future in as_completed(futures):
            processed_name = future.result()
            logging.info(f" 📄 {processed_name}")
