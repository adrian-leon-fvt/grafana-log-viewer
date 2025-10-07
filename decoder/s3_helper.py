from boto3 import client
from botocore.exceptions import ClientError
import logging
from utils import *
from enum import Enum
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


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
    logger = logging.getLogger("get_buckets")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)

    try:
        s3 = client("s3")
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
        return buckets
    except ClientError as e:
        logger.error(f"Error fetching buckets: {e}")
        return []


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
    logger = logging.getLogger("get_mf4_files")
    setup_simple_logger(logger, level=logging.DEBUG, format=LOG_FORMAT)

    if isinstance(bucket_name, EESBuckets):
        bucket_name = bucket_name.value[0]
    elif isinstance(bucket_name, str) and bucket_name in [
        b.value[0] for b in EESBuckets
    ]:
        bucket_name = bucket_name
    else:
        logger.error(f"❌ Invalid bucket name: {bucket_name}")
        return []

    # Convert start_time and end_time to datetime objects if they are strings
    if isinstance(start_time, str) and start_time != "":
        try:
            start_time = datetime.fromisoformat(start_time)
        except ValueError:
            logger.error(f"❌ Invalid start_time format: {start_time}")
            return []
    if isinstance(end_time, str) and end_time != "":
        try:
            end_time = datetime.fromisoformat(end_time)
        except ValueError:
            logger.error(f"❌ Invalid end_time format: {end_time}")
            return []

    def get_timestamp(key: str) -> datetime | None:
        resp = s3c.head_object(Bucket=bucket_name, Key=key)
        try:
            if "timestamp" not in resp["Metadata"]:
                logger.warning(f"⚠️ No timestamp metadata for {key}")
                return None

            timestamp = resp["Metadata"]["timestamp"]
            timestamp = (
                datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
                .replace(tzinfo=timezone.utc)
                .astimezone(ZoneInfo("America/Vancouver"))
            )
            return timestamp
        except KeyError:
            logger.warning(f"⚠️ No metadata for {key}")
            return None

    try:
        s3c = client("s3")
        paginator = s3c.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name)

        def process_object(obj, idx: int, total: int) -> dict:
            logger.info(f"🔍 [{idx:4d}/{total:4d}]: {obj['Key']}")
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
            logger.info(
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

            logger.info(
                f"✅ Processed {count} items in {get_time_str(start_ts)} seconds."
            )
        logger.info(
            f"🏁 Total time to process all pages: {get_time_str(total_ts)} seconds."
        )

        return mf4_files
    except ClientError as e:
        logger.error(f"❌ Error fetching files from bucket '{bucket_name}': {e}")

    return []


def main():
    logger = logging.getLogger("main")
    setup_simple_logger(logger, level=logging.INFO, format=LOG_FORMAT)
    buckets = get_bucket_names()
    for bucket in buckets:
        logger.info(f" 🪣 {bucket}")


if __name__ == "__main__":
    main()
