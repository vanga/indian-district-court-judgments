"""
Shared infrastructure for the two-stage mobile scraper pipeline.

Stage 1 (scraper.py) and Stage 2 (pdf_stage.py) share logging setup,
constants, signal handling, session management, and CLI argument builders.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
import urllib3
import colorlog

# Suppress SSL warnings — eCourts API uses certificate that doesn't verify
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# Configuration
S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")
LOCAL_DIR = Path("./local_mobile_data")
DEFAULT_DELAY = 0.3  # Seconds between API calls
DEFAULT_MAX_WORKERS = 10


def setup_logging():
    """Configure colored console logging. Call once at module load."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )
    )
    root_logger.addHandler(console_handler)


def add_common_args(parser):
    """Add CLI arguments shared by both stages."""
    parser.add_argument(
        "--state", type=int, action="append", dest="states",
        help="State code (can be specified multiple times)",
    )
    parser.add_argument(
        "--district", type=int, action="append", dest="districts",
        help="District code (can be specified multiple times)",
    )
    parser.add_argument(
        "--complex", type=str, action="append", dest="complexes",
        help="Complex code (can be specified multiple times)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2020,
        help="Start year (default: 2020)",
    )
    parser.add_argument(
        "--end-year", type=int, default=2025,
        help="End year (default: 2025)",
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Delay between API calls in seconds (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--local-only", action="store_true",
        help="Don't upload to S3, keep files locally",
    )
    parser.add_argument(
        "--s3-bucket", type=str, default=S3_BUCKET,
        help=f"S3 bucket name (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--local-dir", type=str, default=str(LOCAL_DIR),
        help=f"Local directory for temp files (default: {LOCAL_DIR})",
    )


class ScraperBase:
    """Shared behavior for both pipeline stages."""

    def __init__(self, client, s3_bucket, local_dir, delay, max_retries=5):
        self.client = client
        self.s3_bucket = s3_bucket
        self.local_dir = Path(local_dir)
        self.delay = delay
        self.max_retries = max_retries

        self._interrupted = False
        self._stats_lock = threading.Lock()
        self.stats = {}  # Subclasses define their own stats keys

        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _handle_interrupt(self, signum, frame):
        if self._interrupted:
            logging.getLogger(__name__).warning("Forced exit - archives may be incomplete!")
            sys.exit(1)
        logging.getLogger(__name__).info(
            "\nInterrupt received - finishing current operation and saving..."
        )
        self._interrupted = True

    def _update_stats(self, **kwargs):
        with self._stats_lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value

    def _ensure_session(self) -> bool:
        if not self.client._initialized:
            logger = logging.getLogger(__name__)
            logger.info("Initializing API session...")
            for attempt in range(self.max_retries):
                try:
                    if self.client.initialize_session():
                        logger.info("API session initialized successfully")
                        return True
                except Exception as e:
                    logger.warning(
                        f"Session init attempt {attempt + 1}/{self.max_retries} failed: {e}"
                    )
                if attempt < self.max_retries - 1:
                    wait_time = (2**attempt) + 1
                    time.sleep(wait_time)
            logger.error("All session init attempts failed")
            return False
        return True


class SearchCheckpoint:
    """
    Tracks which search API calls have already been completed for a complex.

    Stores a single JSON file per complex at:
      s3://bucket/metadata/checkpoints/state=XX/district=YY/complex=ZZ/searches.json

    Each entry is keyed by "case_type_code/year/status" and records:
      {"found": N, "at": "ISO timestamp"}

    Thread-safe: multiple workers can record results concurrently.
    """

    FILENAME = "searches.json"

    def __init__(
        self,
        s3_bucket: str,
        local_dir: Path,
        state_code: str,
        district_code: str,
        complex_code: str,
        s3_client=None,
        local_only: bool = False,
    ):
        self.s3_bucket = s3_bucket
        self.local_dir = local_dir
        self.state_code = state_code
        self.district_code = district_code
        self.complex_code = complex_code
        self.local_only = local_only
        self.s3 = s3_client
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)
        self._data: dict[str, dict] = {}
        self._dirty = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

    @staticmethod
    def search_key(case_type_code: str, year: int, status: str) -> str:
        return f"{case_type_code}/{year}/{status}"

    def _s3_key(self) -> str:
        return (
            f"metadata/checkpoints/state={self.state_code}"
            f"/district={self.district_code}/complex={self.complex_code}"
            f"/{self.FILENAME}"
        )

    def _local_path(self) -> Path:
        return (
            self.local_dir / "checkpoints" / self.state_code
            / self.district_code / self.complex_code / self.FILENAME
        )

    def load(self) -> None:
        """Load checkpoint data from local disk or S3. Single file per complex."""
        self._data = self._load()
        if self._data:
            self._logger.info(f"    Loaded {len(self._data)} search checkpoints")

    def _load(self) -> dict[str, dict]:
        """Load searches.json from local disk or S3."""
        # Try local first
        local_path = self._local_path()
        if local_path.exists():
            try:
                with open(local_path) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data
            except (json.JSONDecodeError, OSError):
                pass

        # Try S3
        if self.s3:
            try:
                response = self.s3.get_object(
                    Bucket=self.s3_bucket, Key=self._s3_key()
                )
                data = json.loads(response["Body"].read())
                if isinstance(data, dict):
                    return data
            except self.s3.exceptions.ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code != "NoSuchKey":
                    self._logger.warning(f"    S3 error loading checkpoint: {e}")

        return {}

    def is_completed(self, case_type_code: str, year: int, status: str) -> bool:
        """Check if a search has already been recorded. Thread-safe."""
        key = self.search_key(case_type_code, year, status)
        with self._lock:
            return key in self._data

    def record(self, case_type_code: str, year: int, status: str, found: int) -> None:
        """Record a completed search result. Thread-safe."""
        key = self.search_key(case_type_code, year, status)
        entry = {"found": found, "at": datetime.now(IST).isoformat()}
        with self._lock:
            self._data[key] = entry
            self._dirty = True

    def flush(self) -> None:
        """Write checkpoint to local disk and S3 if dirty."""
        with self._lock:
            if not self._dirty:
                return
            snapshot = dict(self._data)
            self._dirty = False

        local_path = self._local_path()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "w") as f:
            json.dump(snapshot, f, separators=(",", ":"))

        if self.s3:
            self.s3.upload_file(str(local_path), self.s3_bucket, self._s3_key())
            self._logger.debug(f"    Flushed {len(snapshot)} search checkpoints")
