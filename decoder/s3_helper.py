import sys
import time
import logging
import os

from boto3 import client
from botocore.config import Config
from botocore.exceptions import ClientError
from enum import Enum
from zoneinfo import ZoneInfo
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent))

from decoder.utils import *
from decoder.config import *

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def _resolve_s3_verify_setting() -> bool | str:
    """
    Resolve boto3/botocore 'verify' behavior from environment:
    - AWS_S3_TLS_INSECURE=true  -> verify=False
    - AWS_CA_BUNDLE=/path       -> verify=/path (if file exists)
    - AWS_S3_CA_BUNDLE=/path    -> verify=/path (if file exists)
    - default                   -> verify=True (botocore/certifi defaults)
    """
    insecure = os.getenv("AWS_S3_TLS_INSECURE", "").strip().lower()
    if insecure in {"1", "true", "yes", "on"}:
        logging.warning(
            "⚠️ AWS_S3_TLS_INSECURE enabled; TLS certificate validation is disabled"
        )
        return False

    for env_name in ("AWS_CA_BUNDLE", "AWS_S3_CA_BUNDLE"):
        ca_bundle = os.getenv(env_name, "").strip()
        if not ca_bundle:
            continue
        if Path(ca_bundle).exists():
            return ca_bundle
        logging.warning(
            f"⚠️ {env_name} was set but file not found: {ca_bundle}. Falling back to default CA trust."
        )

    return True


def create_s3_client(max_pool_connections: int | None = None):
    verify = _resolve_s3_verify_setting()
    config = (
        Config(max_pool_connections=max_pool_connections)
        if max_pool_connections is not None
        else None
    )
    if config is None:
        return client("s3", verify=verify)
    return client("s3", config=config, verify=verify)


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
        s3 = create_s3_client()
        response = s3.list_buckets()
        buckets = [bucket["Name"] for bucket in response.get("Buckets", [])]
        return buckets
    except ClientError as e:
        logging.error(f"Error fetching buckets: {e}")
        return []


def _normalize_bucket_names(bucket_names: EESBuckets | str | Iterable[EESBuckets | str]) -> list[EESBuckets | str]:
    if isinstance(bucket_names, (str, EESBuckets)):
        return [bucket_names]
    return list(bucket_names)


def _parse_s3_timestamp(timestamp: str) -> datetime:
    normalized = timestamp.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized).astimezone(
            ZoneInfo("America/Vancouver")
        )
    except (ValueError, TypeError):
        parsed = datetime.strptime(timestamp.strip("Z"), "%Y%m%dT%H%M%S")
        return parsed.replace(tzinfo=timezone.utc).astimezone(
            ZoneInfo("America/Vancouver")
        )


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
    else:
        if not isinstance(bucket_name, str) or not bucket_name.strip():
            logging.error(f"❌ Invalid bucket name: {bucket_name}")
            return count

    s3c = create_s3_client(max_pool_connections=max_workers)
    total_keys = len(keys)

    def processed_path(key: str) -> Path:
        return download_path / Path(key.replace("/", "_"))

    def download_with_retry(key: str) -> bool:
        local_path = processed_path(key)
        for attempt in range(1, max_retries + 2):
            try:
                start_ts = time.time()
                s3c.download_file(bucket_name, key, str(local_path))
                logging.debug(
                    f"✅ Downloaded {key} successfully in {get_time_str(start_ts)}."
                )
                return True
            except ClientError as e:
                logging.error(
                    f"❌ Error downloading {key} from bucket '{bucket_name}' (attempt {attempt}): {e}"
                )
            except Exception as e:
                logging.error(
                    f"❌ Unexpected error downloading {key} (attempt {attempt}): {e}"
                )
            if attempt <= max_retries:
                logging.debug(
                    f"🔄 Retrying download for {key} (attempt {attempt + 1})..."
                )
                time.sleep(1)
        return False

    skipped_existing_keys: list[str] = []
    keys_to_download: list[str] = []
    for key in keys:
        if processed_path(key).exists():
            skipped_existing_keys.append(key)
        else:
            keys_to_download.append(key)

    if skipped_existing_keys:
        logging.debug(
            f"⏭️ Skipping {len(skipped_existing_keys)}/{total_keys} files that already exist locally."
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            future_to_key = {
                executor.submit(download_with_retry, key): key
                for key in keys_to_download
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
                    logging.error(
                        f"❌ Unexpected error in future for {key}: {e}"
                    )
        except KeyboardInterrupt:
            logging.warning("⚠️ Download interrupted by user.")
            executor.shutdown(wait=False)
            raise

    return count


def download_file_to_path_from_s3(
    bucket_name: EESBuckets | str,
    key: str,
    local_path: Path,
    max_retries: int = 2,
    s3_client=None,
) -> bool:
    """
    Download one S3 object key to a local path with retries.
    """
    if isinstance(bucket_name, EESBuckets):
        bucket_name = bucket_name.value[0]
    elif not isinstance(bucket_name, str) or not bucket_name.strip():
        logging.error(f"❌ Invalid bucket name: {bucket_name}")
        return False

    s3c = s3_client if s3_client is not None else create_s3_client()
    for attempt in range(1, max_retries + 2):
        try:
            s3c.download_file(bucket_name, key, str(local_path))
            return True
        except ClientError as e:
            logging.error(
                f"❌ Error downloading {key} from bucket '{bucket_name}' (attempt {attempt}): {e}"
            )
        except Exception as e:
            logging.error(
                f"❌ Unexpected error downloading {key} (attempt {attempt}): {e}"
            )
        if attempt <= max_retries:
            time.sleep(1)
    return False


def download_file_bytes_from_s3(
    bucket_name: EESBuckets | str,
    key: str,
    max_retries: int = 2,
    s3_client=None,
) -> bytes | None:
    """
    Download one S3 object and return its bytes in memory.
    """
    if isinstance(bucket_name, EESBuckets):
        bucket_name = bucket_name.value[0]
    elif not isinstance(bucket_name, str) or not bucket_name.strip():
        logging.error(f"❌ Invalid bucket name: {bucket_name}")
        return None

    s3c = s3_client if s3_client is not None else create_s3_client()
    for attempt in range(1, max_retries + 2):
        try:
            resp = s3c.get_object(Bucket=bucket_name, Key=key)
            body = resp.get("Body", None)
            if body is None:
                logging.error(f"❌ Empty body for key {key}")
                return None
            return body.read()
        except ClientError as e:
            logging.error(
                f"❌ Error reading {key} from bucket '{bucket_name}' (attempt {attempt}): {e}"
            )
        except Exception as e:
            logging.error(
                f"❌ Unexpected error reading {key} (attempt {attempt}): {e}"
            )
        if attempt <= max_retries:
            time.sleep(1)
    return None


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
    elif not isinstance(bucket_name, str) or not bucket_name.strip():
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

    posted_after: datetime | None = kwargs.get("posted_after", None)
    if isinstance(posted_after, datetime) and posted_after.tzinfo is None:
        posted_after = posted_after.replace(tzinfo=timezone.utc)

    def get_timestamp(key: str) -> datetime | None:
        resp = s3c.head_object(Bucket=bucket_name, Key=key)
        try:
            if "timestamp" not in resp["Metadata"]:
                logging.warning(f"⚠️ No timestamp metadata for {key}")
                return None

            return _parse_s3_timestamp(resp["Metadata"]["timestamp"])
        except KeyError:
            logging.warning(f"⚠️ No metadata for {key}")
            return None
        except ValueError as e:
            logging.error(f"❌ Failed to parse timestamp '{resp.get('Metadata', {}).get('timestamp')}': {e}")
            return None

    try:
        s3c = create_s3_client()
        paginator = s3c.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Delimiter=kwargs.get("Delimiter", ""),
            EncodingType=kwargs.get("EncodingType", "url"),
            Prefix=kwargs.get("Prefix", ""),
            FetchOwner=kwargs.get("FetchOwner", False),
            StartAfter=kwargs.get("StartAfter", ""),
            RequestPayer=kwargs.get("RequestPayer", ""),
            PaginationConfig=kwargs.get("PaginationConfig", {"PageSize": 1000}),
        )

        def process_object(obj, idx: int, total: int) -> dict:
            logging.debug(f"🔍 [{idx+1:4d}/{total:4d}]: {obj['Key']}")
            key = obj["Key"]
            if key.lower().endswith(".mf4"):
                last_modified = obj["LastModified"]
                if posted_after and last_modified < posted_after:
                    return {}

                timestamp: datetime | None = get_timestamp(key)
                if not timestamp:
                    return {}

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
            logging.debug(
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

            logging.debug(
                f"✅ Processed {count} items in {get_time_str(start_ts)} seconds."
            )
        logging.debug(
            f"🏁 Total time to process all pages: {get_time_str(total_ts)} seconds."
        )

        return mf4_files
    except ClientError as e:
        logging.error(
            f"❌ Error fetching files from bucket '{bucket_name}': {e}"
        )

    return []


def get_new_mf4_files_summary_from_s3(
    bucket_names: EESBuckets | str | Iterable[EESBuckets | str],
    start_time: datetime | str = "",
    end_time: datetime | str = "",
    **kwargs,
) -> dict:
    """
    Check one or more buckets for new MF4 files in a window.
    """
    summary: dict = {
        "has_new_files": False,
        "total_count": 0,
        "buckets": {},
    }

    for bucket_name in _normalize_bucket_names(bucket_names):
        files = get_mf4_files_list_from_s3(
            bucket_name=bucket_name,
            start_time=start_time,
            end_time=end_time,
            **kwargs,
        )
        bucket_key = (
            bucket_name.value[0]
            if isinstance(bucket_name, EESBuckets)
            else str(bucket_name)
        )
        summary["buckets"][bucket_key] = {
            "count": len(files),
            "keys": [
                item["Key"]
                for item in files
                if isinstance(item, dict) and "Key" in item
            ],
            "files": files,
        }
        summary["total_count"] += len(files)

    summary["has_new_files"] = summary["total_count"] > 0
    return summary


def main():
    buckets = get_bucket_names()
    for bucket in buckets:
        logging.debug(f"🪣  {bucket}")


if __name__ == "__main__":
    main()
