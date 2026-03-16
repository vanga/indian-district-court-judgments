"""
Upload existing local TAR files to S3 without re-downloading.

Usage:
    python download.py --upload-local
    python download.py --upload-local --state_code 29 --district_code 22
"""

import json
import logging
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


def human_readable_size(size_bytes: int) -> str:
    """Convert bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def create_index_for_tar(
    tar_path: Path,
    archive_type: str,
    year: str,
    state_code: str,
    district_code: str,
    complex_code: str,
) -> dict:
    """Create an index file for a TAR archive."""
    files = []
    total_size = 0

    with tarfile.open(tar_path, "r") as tf:
        for member in tf.getmembers():
            if member.isfile():
                # Extract just the filename from the path
                filename = Path(member.name).name
                files.append(filename)
                total_size += member.size

    now = datetime.now(IST).isoformat()
    tar_size = tar_path.stat().st_size

    index = {
        "year": int(year),
        "state_code": state_code,
        "district_code": district_code,
        "complex_code": complex_code,
        "archive_type": archive_type,
        "file_count": len(files),
        "total_size": tar_size,
        "total_size_human": human_readable_size(tar_size),
        "created_at": now,
        "updated_at": now,
        "parts": [
            {
                "name": tar_path.name,
                "files": files,
                "file_count": len(files),
                "size": tar_size,
                "size_human": human_readable_size(tar_size),
                "created_at": now,
            }
        ],
    }

    return index


def upload_local_files(
    s3_bucket: str,
    s3_prefix: str,
    local_dir: Path,
    state_filter: Optional[str] = None,
    district_filter: Optional[str] = None,
    complex_filter: Optional[str] = None,
    dry_run: bool = False,
):
    """
    Upload existing local TAR files to S3.

    Scans local_dir for TAR files in the format:
    local_dir/{year}/{state}/{district}/{complex}/*.tar

    Creates index files and uploads both to S3.
    """
    if not local_dir.exists():
        logger.error(f"Local directory does not exist: {local_dir}")
        return

    s3 = boto3.client("s3")

    # Find all TAR files
    tar_files = list(local_dir.glob("*/*/*/*/*.tar"))

    if not tar_files:
        logger.warning(f"No TAR files found in {local_dir}")
        return

    logger.info(f"Found {len(tar_files)} TAR files to process")

    uploaded_count = 0
    skipped_count = 0

    for tar_path in tar_files:
        # Parse path: local_dir/year/state/district/complex/archive.tar
        parts = tar_path.relative_to(local_dir).parts
        if len(parts) != 5:
            logger.warning(f"Skipping unexpected path structure: {tar_path}")
            continue

        year, state_code, district_code, complex_code, tar_name = parts

        # Apply filters
        if state_filter and state_code != state_filter:
            continue
        if district_filter and district_code != district_filter:
            continue
        if complex_filter and complex_code != complex_filter:
            continue

        # Determine archive type from filename
        if tar_name.startswith("orders"):
            archive_type = "orders"
            s3_path_prefix = "data/tar"
        elif tar_name.startswith("metadata"):
            archive_type = "metadata"
            s3_path_prefix = "metadata/tar"
        elif tar_name.startswith("part-"):
            # Part files - determine type from directory contents
            sibling_files = list(tar_path.parent.glob("*.tar"))
            if any("orders" in f.name for f in sibling_files):
                archive_type = "orders"
                s3_path_prefix = "data/tar"
            else:
                archive_type = "metadata"
                s3_path_prefix = "metadata/tar"
        else:
            logger.warning(f"Unknown archive type for {tar_path}")
            continue

        # S3 key
        s3_key = f"{s3_prefix}{s3_path_prefix}/year={year}/state={state_code}/district={district_code}/complex={complex_code}/{tar_name}"

        # Check if already exists in S3
        try:
            s3.head_object(Bucket=s3_bucket, Key=s3_key)
            logger.debug(f"Already exists in S3: {s3_key}")
            skipped_count += 1
            continue
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        tar_size = tar_path.stat().st_size
        logger.info(
            f"Uploading {tar_name} ({human_readable_size(tar_size)}) for {year}/{state_code}/{district_code}/{complex_code}..."
        )

        if dry_run:
            logger.info(f"  [DRY RUN] Would upload to: {s3_key}")
            continue

        # Upload TAR file
        s3.upload_file(str(tar_path), s3_bucket, s3_key)
        logger.info(f"  ✓ Uploaded {tar_name}")

        # Create and upload index file (only for main archive, not parts)
        if tar_name in ["orders.tar", "metadata.tar"]:
            index = create_index_for_tar(
                tar_path, archive_type, year, state_code, district_code, complex_code
            )
            index_key = s3_key.replace(".tar", ".index.json")

            s3.put_object(
                Bucket=s3_bucket,
                Key=index_key,
                Body=json.dumps(index, indent=2),
                ContentType="application/json",
            )
            logger.info(f"  ✓ Uploaded index ({index['file_count']} files)")

            # Also save index locally
            index_path = tar_path.with_suffix(".index.json")
            index_path.write_text(json.dumps(index, indent=2))

        uploaded_count += 1

    logger.info(
        f"\nUpload complete: {uploaded_count} uploaded, {skipped_count} skipped (already in S3)"
    )


def run_upload_local(
    s3_bucket: str,
    s3_prefix: str,
    local_dir: Path,
    state_code: Optional[str] = None,
    district_code: Optional[str] = None,
    complex_code: Optional[str] = None,
    dry_run: bool = False,
):
    """Entry point for upload-local command."""
    logger.info(
        f"Uploading local files from {local_dir} to s3://{s3_bucket}/{s3_prefix}"
    )

    if state_code:
        logger.info(f"  Filtering by state: {state_code}")
    if district_code:
        logger.info(f"  Filtering by district: {district_code}")
    if complex_code:
        logger.info(f"  Filtering by complex: {complex_code}")

    # Get years and states from local files for parquet processing
    years_found = set()
    states_found = set()

    for tar_path in local_dir.glob("*/*/*/*/*.tar"):
        parts = tar_path.relative_to(local_dir).parts
        if len(parts) == 5:
            year, state, district, complex_c, _ = parts
            if state_code and state != state_code:
                continue
            if district_code and district != district_code:
                continue
            if complex_code and complex_c != complex_code:
                continue
            years_found.add(year)
            states_found.add(state)

    upload_local_files(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        local_dir=local_dir,
        state_filter=state_code,
        district_filter=district_code,
        complex_filter=complex_code,
        dry_run=dry_run,
    )

    # Generate parquet files after upload (if not dry run)
    if not dry_run and years_found and states_found:
        logger.info("\nProcessing metadata to parquet format...")

        try:
            from process_metadata import DistrictCourtMetadataProcessor

            processor = DistrictCourtMetadataProcessor(
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                years_to_process=list(years_found),
                states_to_process=list(states_found),
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
