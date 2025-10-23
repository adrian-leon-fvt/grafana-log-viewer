import sys
import time
import logging

from boto3 import client
from botocore.config import Config
from botocore.exceptions import ClientError
from enum import Enum
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent))

from decoder.utils import *
from decoder.config import *

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

class EESBuckets(Enum):
    S3_BUCKET_LOCO = ("fvt-telematics", 0)
    S3_BUCKET_D65 = ("d65-telematics", 1)
    S3_BUCKET_GARLAND = ("garland-telematics", 2)
    S3_BUCKET_TMS = ("tms-telematics", 3)
    S3_BUCKET_DYNO1 = ("dyno1-telematics", 4)


def get_bucket_names() -> list[str]:
    """
    Get a list of S3 buckets using the provided AWS credentials.
    """

    try:
        s3 = client("s3")
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
        return buckets
    except ClientError as e:
        logging.error(f"Error fetching buckets: {e}")
        return []


def download_files_from_s3(
    bucket_name: EESBuckets | str,
    keys: list[str],
    download_path: Path,
    max_workers: int = 10,
    progress_callable=None,
    max_retries: int = 2,
) -> int:
    """
    Download specified files from the given S3 bucket to the local download path.

    :param bucket_name: Name of the S3 bucket or an EESBuckets enum member.
    :param keys: List of S3 object keys to download.
    :param download_path: Local directory path to save the downloaded files.
    :param max_retries: Number of times to retry a failed download.
    """

    count = 0

    if isinstance(bucket_name, EESBuckets):
        bucket_name = bucket_name.value[0]
    elif isinstance(bucket_name, str) and bucket_name in [
        b.value[0] for b in EESBuckets
    ]:
        bucket_name = bucket_name
    else:
        logging.error(f"❌ Invalid bucket name: {bucket_name}")
        return count

    s3c = client("s3", config=Config(max_pool_connections=max_workers))
    total_keys = len(keys)

    def processed_path(key: str) -> Path:
        return download_path / Path(key.replace("/", "_"))

    def download_with_retry(key: str) -> bool:
        local_path = processed_path(key)
        for attempt in range(1, max_retries + 2):
            try:
                start_ts = time.time()
                s3c.download_file(bucket_name, key, str(local_path))
                logging.info(
                    f"✅ Downloaded {key} successfully in {get_time_str(start_ts)}."
                )
                return True
            except ClientError as e:
                logging.error(
                    f"❌ Error downloading {key} from bucket '{bucket_name}' (attempt {attempt}): {e}"
                )
            except Exception as e:
                logging.error(f"❌ Unexpected error downloading {key} (attempt {attempt}): {e}")
            if attempt <= max_retries:
                logging.info(f"🔄 Retrying download for {key} (attempt {attempt + 1})...")
                time.sleep(1)
        return False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            future_to_key = {
                executor.submit(download_with_retry, key): key
                for key in keys if not processed_path(key).exists()
            }
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    success = future.result()
                    if success:
                        count += 1
                        if progress_callable and callable(progress_callable):
                            progress_callable(count, total_keys)
                except Exception as e:
                    logging.error(f"❌ Unexpected error in future for {key}: {e}")
        except KeyboardInterrupt:
            logging.warning("⚠️ Download interrupted by user.")
            executor.shutdown(wait=False)
            raise

    return count


def get_mf4_files_list_from_s3(
    bucket_name: EESBuckets | str,
    start_time: datetime | str = "",
    end_time: datetime | str = "",
    **kwargs,
) -> list[dict]:
    """
    Get a list of .mf4 files from the specified S3 bucket within the given time range.

    :param bucket_name: Name of the S3 bucket or an EESBuckets enum member.
    :param prefix: Prefix to filter the files in the bucket.
    :param start_time: Start time for filtering files (datetime or ISO 8601 string).
    :param end_time: End time for filtering files (datetime or ISO 8601 string).
    :param max_workers: Maximum number of threads to use for concurrent processing.

    :return: List of dictionaries containing file information.
    """

    # Validate the bucket name

    if isinstance(bucket_name, EESBuckets):
        bucket_name = bucket_name.value[0]
    elif isinstance(bucket_name, str) and bucket_name in [
        b.value[0] for b in EESBuckets
    ]:
        bucket_name = bucket_name
    else:
        logging.error(f"❌ Invalid bucket name: {bucket_name}")
        return []

    # Convert start_time and end_time to datetime objects if they are strings
    if isinstance(start_time, str) and start_time != "":
        try:
            start_time = datetime.fromisoformat(start_time)
        except ValueError:
            logging.error(f"❌ Invalid start_time format: {start_time}")
            return []
    if isinstance(end_time, str) and end_time != "":
        try:
            end_time = datetime.fromisoformat(end_time)
        except ValueError:
            logging.error(f"❌ Invalid end_time format: {end_time}")
            return []

    def get_timestamp(key: str) -> datetime | None:
        resp = s3c.head_object(Bucket=bucket_name, Key=key)
        try:
            if "timestamp" not in resp["Metadata"]:
                logging.warning(f"⚠️ No timestamp metadata for {key}")
                return None

            timestamp = resp["Metadata"]["timestamp"]
            timestamp = (
                datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
                .replace(tzinfo=timezone.utc)
                .astimezone(ZoneInfo("America/Vancouver"))
            )
            return timestamp
        except KeyError:
            logging.warning(f"⚠️ No metadata for {key}")
            return None

    try:
        s3c = client("s3")
        paginator = s3c.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name)

        def process_object(obj, idx: int, total: int) -> dict:
            logging.info(f"🔍 [{idx:4d}/{total:4d}]: {obj['Key']}")
            key = obj["Key"]
            if key.lower().endswith(".mf4"):
                timestamp: datetime | None = get_timestamp(key)
                if not timestamp:
                    return {}

                last_modified = obj["LastModified"]
                if (not start_time or timestamp >= start_time) and (
                    not end_time or timestamp <= end_time
                ):
                    return {
                        "Key": key,
                        "LastModified": last_modified,
                        "Size": obj["Size"],
                        "Timestamp": timestamp,
                    }
            return {}

        mf4_files: list[dict] = []
        total_ts = time.time()
        count = 0
        for page in page_iterator:
            count = 0
            start_ts = time.time()
            logging.info(
                f"➡️ Processing page with {len(page.get('Contents', []))} items..."
            )

            with ThreadPoolExecutor(
                max_workers=kwargs.get("max_workers", 20)
            ) as executor:
                contents = page.get("Contents", [])
                futures = [
                    executor.submit(process_object, obj, idx, len(contents))
                    for idx, obj in enumerate(contents)
                ]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        mf4_files.append(result)
                        count += 1

            logging.info(
                f"✅ Processed {count} items in {get_time_str(start_ts)} seconds."
            )
        logging.info(
            f"🏁 Total time to process all pages: {get_time_str(total_ts)} seconds."
        )

        return mf4_files
    except ClientError as e:
        logging.error(f"❌ Error fetching files from bucket '{bucket_name}': {e}")

    return []


def main():
    buckets = get_bucket_names()
    for bucket in buckets:
        logging.info(f"🪣  {bucket}")


if __name__ == "__main__":
    main()
