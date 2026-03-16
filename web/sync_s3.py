"""
S3 Sync Module for District Court Judgments
Handles syncing with S3 and incremental downloads
"""

import json
import logging
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from src.utils.court_utils import CourtComplex

logger = logging.getLogger(__name__)


def get_latest_index_date(s3_bucket: str, state_code: str) -> Optional[datetime]:
    """
    Get the latest updated_at date from index files for a state.
    Returns the most recent date across all districts/complexes.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    latest_date = None
    prefix = f"metadata/tar/"

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith(".index.json"):
                continue

            # Check if this is for the requested state
            if f"/state={state_code}/" not in key:
                continue

            try:
                response = s3.get_object(Bucket=s3_bucket, Key=key)
                index_data = json.loads(response["Body"].read().decode("utf-8"))

                if "updated_at" in index_data:
                    updated_at = datetime.fromisoformat(
                        index_data["updated_at"].replace("Z", "+00:00")
                    )
                    if latest_date is None or updated_at > latest_date:
                        latest_date = updated_at

            except Exception as e:
                logger.debug(f"Could not read index {key}: {e}")

    return latest_date


def get_latest_metadata_date_from_tar(
    s3_bucket: str, state_code: str
) -> Optional[datetime]:
    """
    Fall back to parsing metadata TAR files to find the latest order date.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    latest_date = None
    current_year = datetime.now().year
    prefix = f"metadata/tar/year={current_year}/state={state_code}/"

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith("metadata.tar"):
                continue

            # Download and parse
            import tempfile

            with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            try:
                s3.download_file(s3_bucket, key, str(tmp_path))

                with tarfile.open(tmp_path, "r") as tf:
                    for member in tf.getmembers():
                        if not member.name.endswith(".json"):
                            continue

                        f = tf.extractfile(member)
                        if not f:
                            continue

                        try:
                            data = json.load(f)
                            scraped_at = data.get("scraped_at", "")
                            if scraped_at:
                                dt = datetime.fromisoformat(
                                    scraped_at.replace("Z", "+00:00")
                                )
                                if latest_date is None or dt > latest_date:
                                    latest_date = dt
                        except Exception:
                            pass

            except Exception as e:
                logger.debug(f"Could not process {key}: {e}")
            finally:
                tmp_path.unlink(missing_ok=True)

    return latest_date


def run_sync_s3(
    s3_bucket: str,
    s3_prefix: str,
    local_dir: Path,
    courts: List[CourtComplex],
    start_date: Optional[str],
    end_date: Optional[str],
    day_step: int,
    max_workers: int,
    compress_pdfs: bool = True,
):
    """
    Run the sync-s3 operation: check latest date in S3 and download new data.

    If start_date is provided, uses it directly (skips S3 auto-detection).
    If start_date is not provided, auto-detects from S3 metadata.
    """
    from archive_manager import S3ArchiveManager
    from download import run
    from process_metadata import DistrictCourtMetadataProcessor

    # Get unique states from courts
    states = set(c.state_code for c in courts)
    today = datetime.now().date()

    # Determine date range
    if start_date:
        # User provided explicit dates - use them directly
        actual_start = start_date
        actual_end = end_date if end_date else today.strftime("%Y-%m-%d")
        logger.info(f"Using provided date range: {actual_start} to {actual_end}")
    else:
        # Auto-detect from S3
        logger.info("Checking latest date from S3 metadata...")

        overall_latest = None
        for state_code in states:
            state_latest = get_latest_index_date(s3_bucket, state_code)
            if state_latest is None:
                state_latest = get_latest_metadata_date_from_tar(s3_bucket, state_code)

            if state_latest:
                if overall_latest is None or state_latest < overall_latest:
                    overall_latest = state_latest

        if overall_latest is None:
            # No existing data, start from beginning of current year
            overall_latest = datetime(datetime.now().year, 1, 1)
            logger.info("No existing data found, starting from beginning of year")
        else:
            logger.info(f"Latest date in S3: {overall_latest.date()}")

        # Check if we're up to date
        if overall_latest.date() >= today:
            logger.info("Data is up-to-date. No new downloads needed.")
            actual_start = None  # Signal no download needed
            actual_end = None
        else:
            actual_start = (overall_latest.date() + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            actual_end = today.strftime("%Y-%m-%d")
            logger.info(f"New data available from {actual_start} to {actual_end}")

    # Track changes
    changes_made = False
    all_changes = {}

    with S3ArchiveManager(
        s3_bucket, s3_prefix, local_dir, immediate_upload=True
    ) as archive_manager:
        if actual_start and actual_end:
            # Run the downloader
            run(
                courts=courts,
                start_date=actual_start,
                end_date=actual_end,
                day_step=day_step,
                max_workers=max_workers,
                archive_manager=archive_manager,
                compress_pdfs=compress_pdfs,
            )
            changes_made = True

        if changes_made:
            all_changes = archive_manager.get_all_changes()

    # Log summary
    if changes_made and all_changes:
        logger.info("\nSync Summary:")
        logger.info(f"  Date range: {actual_start} to {actual_end}")
        for location, archives in all_changes.items():
            logger.info(f"  {location}:")
            for archive_type, files in archives.items():
                logger.info(f"    {archive_type}: {len(files)} files")

    # Process metadata to parquet
    if changes_made:
        logger.info("Processing newly downloaded metadata to parquet format...")

        try:
            # Parse years from actual dates
            start_year = int(actual_start.split("-")[0])
            end_year = int(actual_end.split("-")[0])
            years_to_process = [str(year) for year in range(start_year, end_year + 1)]
            states_to_process = list(states)

            processor = DistrictCourtMetadataProcessor(
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                years_to_process=years_to_process,
                states_to_process=states_to_process,
            )

            processed_years, total_records = processor.process_bucket_metadata()

            if total_records > 0:
                logger.info(
                    f"Successfully processed {total_records} records to parquet"
                )
            else:
                logger.warning("No new records were processed to parquet")

        except Exception as e:
            logger.error(f"Error processing metadata to parquet: {e}")
            import traceback

            traceback.print_exc()
    else:
        logger.info("No new data to process to parquet format")

    # Clean up local directory only if we made changes (uploaded data)
    import shutil

    if changes_made and local_dir.exists():
        shutil.rmtree(local_dir)
        logger.info(f"Cleaned up local directory: {local_dir}")
    elif local_dir.exists():
        logger.warning(
            f"Local directory NOT cleaned up (no sync performed): {local_dir}"
        )
