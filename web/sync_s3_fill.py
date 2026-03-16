"""
S3 Gap-Fill Module for District Court Judgments
Handles historical backfill with chunked processing

Processes ONE chunk per run and automatically resumes from where it left off.
Run repeatedly to complete all chunks from 1950 to present.
"""

import json
import logging
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from src.utils.court_utils import CourtComplex

logger = logging.getLogger(__name__)

# Tracking file for progress
TRACKING_FILE = Path("./dc_fill_track.json")

# Chunk size in years
CHUNK_SIZE_YEARS = 5

# Start date for historical data
START_YEAR = 1950


def load_tracking_data() -> dict:
    """Load tracking data from file"""
    if TRACKING_FILE.exists():
        try:
            with open(TRACKING_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load tracking file: {e}")
    return {}


def save_tracking_data(data: dict):
    """Save tracking data to file"""
    with open(TRACKING_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_next_chunk(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Optional[tuple[str, str]]:
    """
    Determine the next chunk to process based on tracking data.
    Returns (chunk_start, chunk_end) in YYYY-MM-DD format, or None if complete.
    """
    tracking = load_tracking_data()

    # If specific dates provided, use those
    if start_date and end_date:
        return (start_date, end_date)

    # Get last processed date
    last_processed = tracking.get("last_chunk_end")

    if last_processed:
        last_date = datetime.strptime(last_processed, "%Y-%m-%d")
        chunk_start = last_date + timedelta(days=1)
    else:
        # Start from the beginning
        chunk_start = datetime(START_YEAR, 1, 1)

    # Calculate chunk end (5 years from start)
    chunk_end = datetime(chunk_start.year + CHUNK_SIZE_YEARS - 1, 12, 31)

    # Cap at today
    today = datetime.now()
    if chunk_end > today:
        chunk_end = today

    # Check if we've gone past today
    if chunk_start.date() > today.date():
        logger.info("All historical data has been processed!")
        return None

    return (
        chunk_start.strftime("%Y-%m-%d"),
        chunk_end.strftime("%Y-%m-%d"),
    )


def update_tracking(chunk_end: str):
    """Update tracking after successful chunk processing"""
    tracking = load_tracking_data()
    tracking["last_chunk_end"] = chunk_end
    tracking["last_updated"] = datetime.now().isoformat()
    save_tracking_data(tracking)


class GracefulExit:
    """Handle graceful exit on timeout"""

    def __init__(self, timeout_hours: float):
        self.should_exit = False
        self.start_time = time.time()
        self.timeout_seconds = timeout_hours * 3600

        # Set up signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful exit...")
        self.should_exit = True

    def check_timeout(self) -> bool:
        """Check if timeout has been exceeded"""
        elapsed = time.time() - self.start_time
        if elapsed >= self.timeout_seconds:
            logger.info(f"Timeout of {self.timeout_seconds / 3600:.1f} hours exceeded")
            self.should_exit = True
        return self.should_exit


def sync_s3_fill_gaps(
    s3_bucket: str,
    s3_prefix: str,
    local_dir: Path,
    courts: List[CourtComplex],
    start_date: Optional[str],
    end_date: Optional[str],
    day_step: int,
    max_workers: int,
    timeout_hours: float = 5.5,
    compress_pdfs: bool = True,
):
    """
    Fill historical gaps in S3 data.

    Processes chunks (default 5 years each) continuously until timeout.
    Skips empty chunks automatically and moves to the next.
    Automatically resumes from where it left off.
    """
    from archive_manager import S3ArchiveManager
    from download import run
    from process_metadata import DistrictCourtMetadataProcessor

    # Set up graceful exit handler
    graceful_exit = GracefulExit(timeout_hours)

    # Track all changes across chunks for final parquet processing
    all_years_processed = set()
    states_to_process = list(set(c.state_code for c in courts))
    total_files_uploaded = 0

    # Process chunks until timeout or complete
    while True:
        # Check for timeout
        if graceful_exit.check_timeout():
            logger.info("Timeout reached, stopping...")
            break

        # Get next chunk to process
        chunk = get_next_chunk(start_date, end_date)
        if chunk is None:
            logger.info("Historical backfill complete!")
            break

        # Only use provided dates for first chunk
        start_date = None
        end_date = None

        chunk_start, chunk_end = chunk
        logger.info(f"Processing chunk: {chunk_start} to {chunk_end}")

        chunk_has_data = False

        try:
            with S3ArchiveManager(
                s3_bucket, s3_prefix, local_dir, immediate_upload=True
            ) as archive_manager:
                # Run the downloader
                logger.info(f"Downloading data for {chunk_start} to {chunk_end}...")

                run(
                    courts=courts,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    day_step=day_step,
                    max_workers=max_workers,
                    archive_manager=archive_manager,
                    compress_pdfs=compress_pdfs,
                )

                all_changes = archive_manager.get_all_changes()

                if all_changes:
                    chunk_has_data = True
                    for location, archives in all_changes.items():
                        for archive_type, files in archives.items():
                            total_files_uploaded += len(files)
                            logger.info(
                                f"  {location}/{archive_type}: {len(files)} files"
                            )

                    # Track years for parquet processing
                    start_year = datetime.strptime(chunk_start, "%Y-%m-%d").year
                    end_year = datetime.strptime(chunk_end, "%Y-%m-%d").year
                    for year in range(start_year, end_year + 1):
                        all_years_processed.add(str(year))

            # Update tracking on success
            update_tracking(chunk_end)

            if chunk_has_data:
                logger.info(f"Chunk complete with data: {chunk_start} to {chunk_end}")
            else:
                logger.info(
                    f"Chunk empty (no data): {chunk_start} to {chunk_end}, moving to next..."
                )

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            break
        except Exception as e:
            logger.error(f"Error processing chunk: {e}")
            import traceback

            traceback.print_exc()
            break

    # Process metadata to parquet for all years that had data
    if all_years_processed:
        logger.info(
            f"\nProcessing metadata to parquet for years: {sorted(all_years_processed)}"
        )

        try:
            processor = DistrictCourtMetadataProcessor(
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                years_to_process=list(all_years_processed),
                states_to_process=states_to_process,
            )

            processed_years, total_records = processor.process_bucket_metadata()

            if total_records > 0:
                logger.info(
                    f"Successfully processed {total_records} records to parquet"
                )

        except Exception as e:
            logger.error(f"Error processing metadata to parquet: {e}")
            import traceback

            traceback.print_exc()
    else:
        logger.info("No data was uploaded, skipping parquet processing")

    # Clean up local directory
    import shutil

    if local_dir.exists():
        shutil.rmtree(local_dir)
        logger.info(f"Cleaned up local directory: {local_dir}")

    # Summary
    logger.info(f"\n=== Fill Summary ===")
    logger.info(f"Total files uploaded: {total_files_uploaded}")

    # Show next chunk info
    next_chunk = get_next_chunk()
    if next_chunk:
        logger.info(f"Next run will continue from: {next_chunk[0]} to {next_chunk[1]}")
    else:
        logger.info("All historical data has been processed!")
