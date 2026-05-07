"""
eCourts Mobile API Scraper.

Searches cases, collects metadata (case history, order info), and downloads
PDFs. Use --no-pdfs for metadata-only scraping.

FEATURES:
- No CAPTCHA required (mobile API advantage)
- Downloads PDFs with Ghostscript compression
- Syncs to S3 using archive_manager
- Robust retry logic with exponential backoff
- Resume capability via S3 index files
- Search checkpointing to avoid redundant API calls

USAGE:
    # Scrape everything (metadata + PDFs)
    uv run python scraper.py --state 29 --district 22 --start-year 2020 --end-year 2025

    # Metadata only (no PDF downloads)
    uv run python scraper.py --state 29 --district 22 --no-pdfs

    # Dry run (no S3 upload)
    uv run python scraper.py --state 29 --district 22 --local-only
"""

import argparse
import concurrent.futures
import json
import logging
import random
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import urlparse, parse_qs

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from archive_manager import S3ArchiveManager
from api_client import (
    MobileAPIClient,
    State,
    District,
    CourtComplex,
    Case,
    Order,
)
from common import (
    IST, S3_BUCKET, LOCAL_DIR, DEFAULT_DELAY, DEFAULT_MAX_WORKERS,
    setup_logging, add_common_args, ScraperBase, SearchCheckpoint,
)
from crypto import decrypt_url_param
from gs import check_ghostscript_available, compress_pdf_bytes

setup_logging()
logger = logging.getLogger(__name__)

MAX_PDF_RETRIES = 3
COMPRESSION_AVAILABLE = check_ghostscript_available()
if COMPRESSION_AVAILABLE:
    logger.info("PDF compression enabled (Ghostscript found)")
else:
    logger.warning("PDF compression not available (Ghostscript not found)")
@dataclass
class CaseTypeTask:
    """A task for processing a single case type in a complex."""

    case_type_code: str
    case_type_name: str
    year: int
    status: str  # "Pending" or "Disposed"


class MobileScraper(ScraperBase):
    """
    Stage 1: Metadata scraper for eCourts Mobile API with S3 sync.
    """

    def __init__(
        self,
        s3_bucket: str = S3_BUCKET,
        local_dir: Path = LOCAL_DIR,
        delay: float = DEFAULT_DELAY,
        max_retries: int = 5,
        max_workers: int = DEFAULT_MAX_WORKERS,
        local_only: bool = False,
        immediate_upload: bool = True,
        verify: bool = False,
        download_pdfs: bool = True,
        compress_pdfs: bool = True,
    ):
        super().__init__(
            client=MobileAPIClient(),
            s3_bucket=s3_bucket,
            local_dir=local_dir,
            delay=delay,
            max_retries=max_retries,
        )
        self.max_workers = max_workers
        self.local_only = local_only
        self.immediate_upload = immediate_upload
        self.verify = verify
        self.download_pdfs = download_pdfs
        self.compress_pdfs = compress_pdfs and COMPRESSION_AVAILABLE
        self.parallel_complexes = 1  # Set via scrape() or CLI
        # Shared JWT: one appReleaseWebService call for all parallel complex threads
        self._shared_jwt_lock = threading.Lock()
        self._shared_jwt_initialized = False  # True once init was attempted
        self._shared_jwt_token = ""           # Non-empty if init succeeded

        # Legacy fallbacks — only used if methods are called without explicit params
        self._checkpoint: Optional[SearchCheckpoint] = None
        self.archive_manager: Optional[S3ArchiveManager] = None
        self.stats = {
            "states_processed": 0,
            "districts_processed": 0,
            "complexes_processed": 0,
            "case_types_processed": 0,
            "searches_skipped": 0,
            "searches_empty": 0,
            "searches_failed": 0,
            "cases_found": 0,
            "cases_processed": 0,
            "cases_skipped": 0,
            "history_failed": 0,
            "pdfs_downloaded": 0,
            "pdfs_compressed": 0,
            "pdfs_skipped": 0,
            "pdfs_failed": 0,
            "bytes_saved": 0,
            "errors": 0,
            "api_retries": 0,
            "api_failures": 0,
            "connection_errors": 0,
            "start_time": None,
            "end_time": None,
        }
        self._failed_operations: list[dict] = []
        self._failed_lock = self._stats_lock  # reuse the same lock
        self._last_status_time = time.time()
        self._thread_local = threading.local()
        self._complex_thread_local = threading.local()  # One client per outer complex-worker thread

    def _get_shared_jwt(self) -> str:
        """
        Get a shared JWT token for parallel complex workers, initializing it once.
        Only one appReleaseWebService.php call is made for all N parallel threads.
        Returns the token string, or "" if init failed.
        """
        with self._shared_jwt_lock:
            if self._shared_jwt_initialized:
                return self._shared_jwt_token
            # First thread to arrive — initialize the shared session
            bootstrap = MobileAPIClient()
            for attempt in range(self.max_retries):
                try:
                    if bootstrap.initialize_session():
                        self._shared_jwt_token = bootstrap.jwt_token
                        break
                except Exception as e:
                    logger.warning(f"Shared session init attempt {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep((2 ** attempt) + 1)
            else:
                logger.error("Shared session init failed — all attempts exhausted")
            self._shared_jwt_initialized = True
            return self._shared_jwt_token

    def _get_complex_client(self) -> MobileAPIClient:
        """
        Get or create a persistent MobileAPIClient for the current outer complex-worker thread.
        In parallel mode: all threads share one JWT token (one appReleaseWebService.php call).
        If session init fails (e.g. rate-limited), client proceeds without JWT — API endpoints
        generally work without it. Reuses the per-thread client across all complexes on the thread.
        """
        client = getattr(self._complex_thread_local, "client", None)
        if client is not None:
            # Refresh JWT if cleared (token expiry), best-effort
            if not client.jwt_token:
                jwt = self._get_shared_jwt()
                if jwt:
                    client.jwt_token = jwt
                    client._initialized = True
            return client

        # First time on this thread — get the shared JWT (best-effort)
        jwt = self._get_shared_jwt()
        client = MobileAPIClient()
        if jwt:
            client.jwt_token = jwt
            client._initialized = True
            logger.debug(f"Complex client ready on {threading.current_thread().name} (shared JWT)")
        else:
            logger.warning(f"Complex client on {threading.current_thread().name} — proceeding without JWT")
        self._complex_thread_local.client = client
        return client

    def _get_worker_client(self) -> MobileAPIClient:
        """Get or create a per-thread MobileAPIClient with its own JWT session."""
        client = getattr(self._thread_local, "client", None)
        if client is None:
            client = MobileAPIClient()
            if self.parallel_complexes > 1:
                # Reuse the shared JWT — avoids a separate appReleaseWebService call per worker thread
                jwt = self._get_shared_jwt()
                if jwt:
                    client.jwt_token = jwt
                    client._initialized = True
                # else: proceed without JWT; endpoints may still work
            else:
                client.initialize_session()
            self._thread_local.client = client
            logger.debug(f"Initialized worker client on thread {threading.current_thread().name}")
        elif not client.jwt_token:
            # JWT was cleared (e.g. after repeated session errors) — try to refresh from shared
            client._reset_http_session()
            client._initialized = False
            if self.parallel_complexes > 1:
                jwt = self._get_shared_jwt()
                if jwt:
                    client.jwt_token = jwt
                    client._initialized = True
            else:
                client.initialize_session()
            logger.debug(f"Re-initialized stale worker client on thread {threading.current_thread().name}")
        return client

    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry and jitter."""
        for attempt in range(self.max_retries):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    if attempt > 0:
                        self._update_stats(api_retries=attempt)
                    return result
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if "ConnectionError" in type(e).__name__ or "RemoteDisconnected" in str(e):
                    self._update_stats(connection_errors=1)

            if attempt < self.max_retries - 1:
                # Jitter spreads retry waves across concurrent workers
                base = (2**attempt) + (0.1 * attempt)
                wait_time = base + random.uniform(0, base * 0.5)
                time.sleep(wait_time)

        self._update_stats(api_failures=1)
        return None

    def _log_periodic_status(self):
        """Log a status summary every 5 minutes."""
        now = time.time()
        if now - self._last_status_time < 300:
            return
        self._last_status_time = now

        total_api_calls = (
            self.stats["cases_processed"]
            + self.stats["cases_skipped"]
            + self.stats["api_failures"]
        )
        failure_rate = (
            (self.stats["api_failures"] / total_api_calls * 100)
            if total_api_calls > 0
            else 0
        )
        conn_rate = (
            (self.stats["connection_errors"] / max(total_api_calls, 1) * 100)
        )

        elapsed = ""
        if self.stats["start_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            elapsed = f" | elapsed: {datetime.now(IST) - start}"

        logger.info(
            f"\n{'=' * 70}\n"
            f"STATUS | states: {self.stats['states_processed']} | "
            f"districts: {self.stats['districts_processed']} | "
            f"complexes: {self.stats['complexes_processed']}\n"
            f"CASES  | found: {self.stats['cases_found']} | "
            f"processed: {self.stats['cases_processed']} | "
            f"skipped: {self.stats['cases_skipped']}\n"
            f"FAILS  | searches: {self.stats['searches_failed']} | "
            f"history: {self.stats['history_failed']} | "
            f"api_failures: {self.stats['api_failures']} | "
            f"conn_errors: {self.stats['connection_errors']} | "
            f"failure_rate: {failure_rate:.1f}% | "
            f"conn_error_rate: {conn_rate:.1f}%{elapsed}\n"
            f"{'=' * 70}"
        )

    def _extract_pdf_filename(self, pdf_url: str) -> Optional[str]:
        """Extract PDF filename from encrypted URL params."""
        try:
            parsed = urlparse(pdf_url)
            query = parse_qs(parsed.query)
            params_enc = query.get("params", [""])[0]
            if params_enc:
                params = decrypt_url_param(params_enc)
                if isinstance(params, dict):
                    return params.get("filename", "")
        except Exception:
            pass
        return None

    def _build_case_metadata(
        self,
        case: Case,
        state: State,
        district: District,
        complex_: CourtComplex,
        history: dict,
        final_orders: list,
        interim_orders: list,
    ) -> dict:
        """Build metadata dictionary for a case."""

        def order_to_dict(o: Order) -> dict:
            data = {
                "order_number": o.order_number,
                "order_date": o.order_date,
                "order_type": o.order_type,
                "pdf_url": o.pdf_url,
                "is_final": o.is_final,
            }
            if o.pdf_url:
                filename = self._extract_pdf_filename(o.pdf_url)
                if filename:
                    data["pdf_filename"] = filename
            return data

        now = datetime.now(IST).isoformat()
        h = history or {}

        # Map raw history fields → normalised case_summary fields
        def _hval(key: str):
            v = h.get(key)
            return v if v and v not in ("None", "0", "") else None

        court_no = _hval("court_no") or _hval("courtno") or ""
        judge = _hval("desgname") or ""
        court_and_judge = f"{court_no} {judge}".strip() or None

        case_summary = {
            "case_no": case.case_no,
            "cino": case.cino,
            "case_type": case.case_type,
            "case_number": case.case_number,
            "reg_year": case.reg_year,
            "petitioner": case.petitioner,
            "court_code": case.court_code,
            # Phase-2 equivalent fields from history
            "filing_date": _hval("date_of_filing"),
            "registration_date": _hval("dt_regis"),
            "first_hearing_date": _hval("date_first_list"),
            "next_hearing_date": _hval("date_next_list"),
            "court_number_and_judge": court_and_judge,
            "petitioners_with_advocates": _hval("petNameAdd"),
            "respondents_with_advocates": _hval("resNameAdd"),
        }
        # Drop None values to keep records lean
        case_summary = {k: v for k, v in case_summary.items() if v is not None}

        has_data = bool(_hval("date_of_filing") or _hval("historyOfCaseHearing"))

        return {
            "case_summary": case_summary,
            "location": {
                "state_code": state.code,
                "state_name": state.name,
                "district_code": district.code,
                "district_name": district.name,
                "complex_code": complex_.code,
                "complex_name": complex_.name,
            },
            "orders": {
                "final_orders": [order_to_dict(o) for o in final_orders],
                "interim_orders": [order_to_dict(o) for o in interim_orders],
                "has_pdf": bool(final_orders or interim_orders),
            },
            "history": history,       # raw API response preserved
            "acts": [],               # structured acts not parsed from HTML
            "case_history": [],       # structured history not parsed from HTML
            "phase1_scraped_at": now,
            "phase2_scraped_at": now, # mobile fetches everything in one pass
            "phase2_status": "enriched" if has_data else "no_data",
            "source": "mobile_api",
        }

    def _download_pdf(
        self,
        order: Order,
        year: int,
        state: State,
        district: District,
        complex_: CourtComplex,
        archive_manager: S3ArchiveManager = None,
    ) -> bool:
        """Download a PDF from an order using the original pdf_url."""
        am = archive_manager or self.archive_manager
        pdf_filename = self._extract_pdf_filename(order.pdf_url)
        if not pdf_filename:
            return False
        clean_filename = pdf_filename.lstrip("/").replace("/", "_")

        # Check if already downloaded
        if am.file_exists(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="data",
            filename=clean_filename,
        ):
            self._update_stats(pdfs_skipped=1)
            return True

        temp_path = (
            self.local_dir
            / "temp"
            / f"{threading.current_thread().name}_{clean_filename}"
        )
        temp_path.parent.mkdir(parents=True, exist_ok=True)

        worker_client = self._get_worker_client()
        success = False
        try:
            result = worker_client.download_pdf(
                pdf_url=order.pdf_url,
                output_path=str(temp_path),
            )
            if result and temp_path.exists():
                with open(temp_path, "rb") as f:
                    header = f.read(4)
                if header == b"%PDF":
                    success = True
                else:
                    if temp_path.exists():
                        temp_path.unlink()
        except Exception as e:
            logger.debug(f"PDF download error: {pdf_filename} - {e}")

        if success and temp_path.exists():
            with open(temp_path, "rb") as f:
                pdf_content = f.read()
            original_size = len(pdf_content)

            if self.compress_pdfs:
                try:
                    compressed = compress_pdf_bytes(pdf_content, self.local_dir / "temp")
                    if len(compressed) < original_size:
                        self._update_stats(bytes_saved=original_size - len(compressed), pdfs_compressed=1)
                        pdf_content = compressed
                except Exception:
                    pass

            am.add_to_archive(
                year=year,
                state_code=str(state.code),
                district_code=str(district.code),
                complex_code=str(complex_.code),
                archive_type="data",
                filename=clean_filename,
                content=pdf_content,
            )
            self._update_stats(pdfs_downloaded=1)
            try:
                temp_path.unlink()
            except Exception:
                pass
            return True
        else:
            logger.warning(f"Failed to download PDF after {MAX_PDF_RETRIES} attempts: {pdf_filename}")
            self._update_stats(pdfs_failed=1)
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass
            return False

    def _get_year_from_case(self, case: Case) -> int:
        """Extract year from case data."""
        try:
            if case.reg_year:
                return int(case.reg_year)
        except (ValueError, TypeError):
            pass
        return datetime.now().year

    def scrape_case(
        self,
        case: Case,
        state: State,
        district: District,
        complex_: CourtComplex,
        year: int,
        archive_manager: S3ArchiveManager = None,
    ) -> bool:
        """
        Scrape a single case: get history, save metadata, download PDFs.

        Returns True if case was processed successfully.
        """
        am = archive_manager or self.archive_manager
        # Check if already processed
        metadata_filename = f"{case.case_no}.json"
        if am.file_exists(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="metadata",
            filename=metadata_filename,
        ):
            self._update_stats(cases_skipped=1)
            return True

        time.sleep(self.delay)

        # Get case history (uses per-thread client)
        worker_client = self._get_worker_client()
        history = self._retry_with_backoff(
            worker_client.get_case_history,
            state_code=state.code,
            dist_code=district.code,
            court_code=case.court_code,
            case_no=case.case_no,
        )

        if not history:
            logger.warning(f"Failed to get history for case {case.case_no} (state={state.code} district={district.code})")
            self._update_stats(history_failed=1)
            with self._failed_lock:
                self._failed_operations.append({
                    "type": "history",
                    "state": state.code,
                    "district": district.code,
                    "complex": complex_.code,
                    "court_code": case.court_code,
                    "case_no": case.case_no,
                    "year": year,
                })
            return False

        # Extract orders
        final_orders, interim_orders = worker_client.get_orders_from_history(history)

        # Build metadata
        metadata = self._build_case_metadata(
            case, state, district, complex_, history, final_orders, interim_orders
        )

        # Save metadata to archive
        metadata_json = json.dumps(metadata, ensure_ascii=False, indent=2)
        am.add_to_archive(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="metadata",
            filename=metadata_filename,
            content=metadata_json,
        )

        # Download PDFs (uses original pdf_url from case history)
        if self.download_pdfs:
            all_orders = final_orders + interim_orders
            for order in all_orders:
                if order.pdf_url:
                    self._download_pdf(order, year, state, district, complex_, archive_manager=am)

        self._update_stats(cases_processed=1)
        return True

    def _process_case_type_task(
        self,
        task: CaseTypeTask,
        state: State,
        district: District,
        complex_: CourtComplex,
        archive_manager: S3ArchiveManager = None,
        checkpoint: 'SearchCheckpoint' = None,
    ) -> Tuple[int, int]:
        """
        Process a single case type task (one case type + year + status combination).

        Returns tuple of (cases_found, cases_processed).
        """
        if self._interrupted:
            return (0, 0)

        cp = checkpoint or self._checkpoint

        # Skip if this search was already completed (unless --verify)
        if (
            not self.verify
            and cp
            and cp.is_completed(task.case_type_code, task.year, task.status)
        ):
            self._update_stats(searches_skipped=1)
            return (0, 0)

        time.sleep(self.delay)

        worker_client = self._get_worker_client()
        cases = self._retry_with_backoff(
            worker_client.search_cases_by_type,
            state_code=state.code,
            dist_code=district.code,
            court_code=complex_.njdg_est_code,
            case_type=task.case_type_code,
            year=task.year,
            pending_disposed=task.status,
        )

        if cases is None:
            # API failure after retries — record for later retry (don't checkpoint)
            self._update_stats(searches_failed=1)
            with self._failed_lock:
                self._failed_operations.append({
                    "type": "search",
                    "state": state.code,
                    "district": district.code,
                    "complex": complex_.code,
                    "court_code": complex_.njdg_est_code,
                    "case_type": task.case_type_code,
                    "case_type_name": task.case_type_name,
                    "year": task.year,
                    "status": task.status,
                })
            return (0, 0)

        if not cases:
            self._update_stats(searches_empty=1)
            # Checkpoint the empty result so we skip it next time
            if cp:
                cp.record(task.case_type_code, task.year, task.status, 0)
            return (0, 0)

        cases_found = len(cases)
        self._update_stats(cases_found=cases_found)
        processed = 0

        for case in cases:
            if self._interrupted:
                break
            try:
                if self.scrape_case(case, state, district, complex_, task.year, archive_manager=archive_manager):
                    processed += 1
            except Exception as e:
                logger.debug(f"Error processing case {case.case_no}: {e}")
                self._update_stats(errors=1)

        # Checkpoint only if all cases were processed (no interrupt)
        if not self._interrupted and cp:
            cp.record(task.case_type_code, task.year, task.status, cases_found)

        return (cases_found, processed)

    def scrape_complex(
        self,
        state: State,
        district: District,
        complex_: CourtComplex,
        years: list[int],
        pending_disposed: str = "Both",
    ) -> int:
        """
        Scrape all cases from a court complex using concurrent workers.

        Each complex creates its own S3ArchiveManager, SearchCheckpoint, and
        MobileAPIClient — no shared mutable state with other complexes. This
        allows multiple complexes to be scraped in parallel.

        Returns number of cases processed.
        """
        # Create per-complex archive manager (fully independent)
        archive_manager = S3ArchiveManager(
            s3_bucket=self.s3_bucket,
            s3_prefix="",
            local_dir=self.local_dir,
            immediate_upload=self.immediate_upload,
            local_only=self.local_only,
        )

        with archive_manager:
            # Load search checkpoints for this complex
            checkpoint = SearchCheckpoint(
                s3_bucket=self.s3_bucket,
                local_dir=self.local_dir,
                state_code=str(state.code),
                district_code=str(district.code),
                complex_code=str(complex_.code),
                s3_client=archive_manager.s3,
                local_only=self.local_only,
            )
            checkpoint.load()

            # Skip complexes previously confirmed to have no case types
            _NO_CASE_TYPES_KEY = "__no_case_types__/0/"
            if _NO_CASE_TYPES_KEY in checkpoint._data:
                return 0

            # Get (or reuse) the per-thread client for this complex worker.
            # In parallel mode: 5 threads → 5 sessions, reused across all complexes on that thread.
            # In sequential mode: creates a fresh client per complex (original behavior).
            if self.parallel_complexes > 1:
                client = self._get_complex_client()
            else:
                client = MobileAPIClient()
                for attempt in range(self.max_retries):
                    try:
                        if client.initialize_session():
                            break
                    except Exception as e:
                        logger.warning(f"Session init attempt {attempt + 1}/{self.max_retries} for {complex_.name}: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep((2 ** attempt) + 1)
                else:
                    logger.error(f"All session init attempts failed for {complex_.name}")
                    return 0

            time.sleep(self.delay)
            case_types = self._retry_with_backoff(
                client.get_case_types,
                state_code=state.code,
                dist_code=district.code,
                court_code=complex_.njdg_est_code,
            )

            if case_types is None:
                # API failure after retries — do not checkpoint, retry next run
                logger.warning(f"No case types found for {complex_.name} (state={state.code} district={district.code})")
                return 0

            if not case_types:
                # API confirmed empty — checkpoint so we don't re-attempt
                logger.warning(f"No case types found for {complex_.name} (state={state.code} district={district.code}), checkpointing")
                checkpoint.record("__no_case_types__", 0, "")
                checkpoint.flush()
                return 0

            # Generate tasks grouped by year
            statuses = (
                ["Pending", "Disposed"]
                if pending_disposed == "Both"
                else [pending_disposed]
            )
            tasks_per_year: dict[int, List[CaseTypeTask]] = {}
            total_task_count = 0

            for year in years:
                year_tasks = []
                for ct in case_types:
                    for status in statuses:
                        year_tasks.append(
                            CaseTypeTask(
                                case_type_code=ct.code,
                                case_type_name=ct.name,
                                year=year,
                                status=status,
                            )
                        )
                tasks_per_year[year] = year_tasks
                total_task_count += len(year_tasks)

            self._update_stats(case_types_processed=len(case_types))

            logger.info(
                f"    Found {len(case_types)} case types, {total_task_count} tasks across {len(years)} years (workers: {self.max_workers})"
            )

            # Process tasks year by year, flushing to S3 after each year
            total_processed = 0
            total_found = 0

            with tqdm(
                total=total_task_count,
                desc=f"    {complex_.name[:25]}",
                unit="task",
                leave=False,
                ncols=100,
            ) as pbar:
                for year in years:
                    if self._interrupted:
                        break

                    year_tasks = tasks_per_year[year]
                    year_found = 0
                    year_processed = 0

                    # Adaptive concurrency: more workers at night/weekends (IST)
                    workers, self.delay = self.get_adaptive_settings()

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=workers
                    ) as executor:
                        # Submit all tasks for this year
                        future_to_task = {
                            executor.submit(
                                self._process_case_type_task,
                                task,
                                state,
                                district,
                                complex_,
                                archive_manager=archive_manager,
                                checkpoint=checkpoint,
                            ): task
                            for task in year_tasks
                        }

                        for future in concurrent.futures.as_completed(future_to_task):
                            task = future_to_task[future]
                            try:
                                found, processed = future.result()
                                year_found += found
                                year_processed += processed
                                total_found += found
                                total_processed += processed
                                if found > 0:
                                    pbar.set_postfix(
                                        {
                                            "found": total_found,
                                            "processed": total_processed,
                                            "year": year,
                                        }
                                    )
                            except Exception as e:
                                logger.debug(
                                    f"Task failed ({task.case_type_name}/{task.year}/{task.status}): {e}"
                                )
                                self._update_stats(errors=1)
                            finally:
                                pbar.update(1)

                            if self._interrupted:
                                logger.info(
                                    "Stopping due to interrupt - cancelling remaining tasks..."
                                )
                                executor.shutdown(wait=False, cancel_futures=True)
                                break

                    # Flush archives for this year+complex to S3
                    if year_found > 0 or year_processed > 0:
                        archive_manager.flush_complex_year(
                            year=year,
                            state_code=str(state.code),
                            district_code=str(district.code),
                            complex_code=str(complex_.code),
                        )

                    # Flush search checkpoints after each year
                    checkpoint.flush()

            # Final flush (e.g. from interrupt mid-year)
            checkpoint.flush()

        logger.info(
            f"    Complex done: {total_found} cases found, {total_processed} processed"
        )
        return total_processed

    def _load_complexes_interleaved(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        complex_codes: Optional[list[str]] = None,
    ) -> list[tuple]:
        """
        Load all court complexes from courts.csv, interleaved by state.

        Returns list of (State, District, CourtComplex) tuples ordered so
        consecutive entries come from different states, spreading load across
        potential backend database shards.
        """
        import csv
        from collections import defaultdict
        from itertools import zip_longest

        csv_path = Path(__file__).parent.parent / "courts.csv"
        if not csv_path.exists():
            logger.error(f"courts.csv not found at {csv_path}")
            return []

        # Group complexes by state
        by_state: dict[int, list[tuple]] = defaultdict(list)

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    sc = int(row["state_code"])
                    dc = int(row["district_code"])
                    cc = row["complex_code"].strip()
                    court_numbers = row.get("court_numbers", "").strip()
                except (KeyError, ValueError):
                    continue

                if state_codes and sc not in state_codes:
                    continue
                if district_codes and dc not in district_codes:
                    continue
                if complex_codes and cc not in complex_codes:
                    continue

                state = State(code=sc, name=row.get("state_name", ""))
                district = District(code=dc, name=row.get("district_name", ""), state_code=sc)
                complex_ = CourtComplex(
                    code=cc,
                    name=row.get("complex_name", ""),
                    njdg_est_code=court_numbers,
                    state_code=sc,
                    district_code=dc,
                )
                by_state[sc].append((state, district, complex_))

        # Sort each state's complexes by court count descending (more courts = larger complex)
        for sc in by_state:
            by_state[sc].sort(
                key=lambda t: len(t[2].njdg_est_code.split(",")) if t[2].njdg_est_code else 0,
                reverse=True,
            )

        # Interleave: round-robin across states
        result = []
        state_lists = list(by_state.values())
        for items in zip_longest(*state_lists):
            for item in items:
                if item is not None:
                    result.append(item)

        logger.info(f"Loaded {len(result)} complexes across {len(by_state)} states")
        return result

    def _scrape_complex_worker(
        self,
        state: State,
        district: District,
        complex_: CourtComplex,
        years: list[int],
        pending_disposed: str,
        pbar: Optional[tqdm] = None,
    ):
        """Worker function for scraping a single complex. Called from scrape()."""
        if self._interrupted:
            return

        if pbar:
            pbar.set_description(
                f"s={state.code} d={district.code} {complex_.name[:20]}"
            )
        self._update_stats(complexes_processed=1)

        try:
            self.scrape_complex(
                state=state,
                district=district,
                complex_=complex_,
                years=years,
                pending_disposed=pending_disposed,
            )
            total_calls = (
                self.stats["cases_processed"]
                + self.stats["cases_skipped"]
                + self.stats["api_failures"]
            )
            fail_pct = (
                f"{self.stats['api_failures'] / total_calls * 100:.0f}%"
                if total_calls > 0
                else "0%"
            )
            if pbar:
                pbar.set_postfix(
                    {
                        "cases": self.stats["cases_processed"],
                        "pdfs": self.stats["pdfs_downloaded"],
                        "fail": fail_pct,
                    }
                )
            self._log_periodic_status()
        except Exception as e:
            logger.error(
                f"Error processing complex {complex_.name}: {e}"
            )
            self._update_stats(errors=1)

    def scrape(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        complex_codes: Optional[list[str]] = None,
        start_year: int = 2020,
        end_year: int = 2025,
        pending_disposed: str = "Both",
    ) -> dict:
        """
        Main scraping function.

        Args:
            state_codes: Filter by state codes
            district_codes: Filter by district codes
            complex_codes: Filter by complex codes
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
            pending_disposed: "Pending", "Disposed", or "Both"

        Returns:
            Statistics dictionary
        """
        self.stats["start_time"] = datetime.now(IST).isoformat()
        years = list(range(end_year, start_year - 1, -1))  # Recent years first

        print("\n" + "=" * 60)
        print("eCOURTS MOBILE API SCRAPER")
        print("=" * 60)
        print(f"  Years: {start_year} to {end_year}")
        print(f"  Filter: {pending_disposed}")
        print(f"  Workers per complex: {self.max_workers}")
        print(f"  Parallel complexes: {self.parallel_complexes}")
        print(f"  S3 Bucket: {self.s3_bucket}")
        print(f"  Local Only: {self.local_only}")
        print(f"  Verify Mode: {self.verify}")
        print("=" * 60 + "\n")

        # Build flat list of all complexes from courts.csv, interleaved by state
        all_complexes = self._load_complexes_interleaved(
            state_codes=state_codes,
            district_codes=district_codes,
            complex_codes=complex_codes,
        )

        if not all_complexes:
            logger.error("No complexes to process")
            return self.stats

        logger.info(
            f"Loaded {len(all_complexes)} complexes (interleaved across states), "
            f"parallel_complexes={self.parallel_complexes}"
        )

        # In parallel mode, get the shared JWT upfront. If it fails, exit so
        # run_scraper.sh restarts cleanly (avoids cycling through all complexes with no auth).
        if self.parallel_complexes > 1:
            jwt = self._get_shared_jwt()
            if not jwt:
                logger.error("Cannot start parallel scraping: shared session init failed. Will retry on next restart.")
                return self.stats

        pbar = tqdm(
            total=len(all_complexes),
            desc="Complexes",
            unit="complex",
            position=0,
            leave=True,
            ncols=100,
        )

        if self.parallel_complexes <= 1:
            # Sequential mode (original behavior)
            for state, district, complex_ in all_complexes:
                if self._interrupted:
                    logger.info("Stopping due to interrupt - finalizing archives...")
                    break
                self._scrape_complex_worker(
                    state, district, complex_, years, pending_disposed, pbar
                )
                pbar.update(1)
        else:
            # Parallel mode — N complexes at a time
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.parallel_complexes
            ) as executor:
                future_to_complex = {
                    executor.submit(
                        self._scrape_complex_worker,
                        state, district, complex_, years, pending_disposed, pbar
                    ): (state, district, complex_)
                    for state, district, complex_ in all_complexes
                }

                for future in concurrent.futures.as_completed(future_to_complex):
                    state, district, complex_ = future_to_complex[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Complex worker failed for {complex_.name}: {e}")
                        self._update_stats(errors=1)
                    finally:
                        pbar.update(1)

                    if self._interrupted:
                        logger.info("Stopping due to interrupt - cancelling remaining complexes...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

        pbar.close()

        self.stats["end_time"] = datetime.now(IST).isoformat()

        # Print summary
        self._print_summary()

        return self.stats

    def _print_summary(self):
        """Print scraping summary."""
        print("\n" + "=" * 60)
        if self._interrupted:
            print("SCRAPING INTERRUPTED - Archives saved properly")
        else:
            print("SCRAPING COMPLETE")
        print("=" * 60)
        print(f"States processed:     {self.stats['states_processed']}")
        print(f"Districts processed:  {self.stats['districts_processed']}")
        print(f"Complexes processed:  {self.stats['complexes_processed']}")
        print(f"Case types processed: {self.stats['case_types_processed']}")
        print(f"Searches skipped:     {self.stats['searches_skipped']}")
        print(f"Searches empty:       {self.stats['searches_empty']}")
        print(f"Searches failed:      {self.stats['searches_failed']}")
        print(f"Cases found:          {self.stats['cases_found']}")
        print(f"Cases processed:      {self.stats['cases_processed']}")
        print(f"Cases skipped:        {self.stats['cases_skipped']}")
        print(f"History failed:       {self.stats['history_failed']}")
        print(f"API retries:          {self.stats['api_retries']}")
        print(f"API failures:         {self.stats['api_failures']}")
        print(f"Connection errors:    {self.stats['connection_errors']}")
        print(f"Errors:               {self.stats['errors']}")

        if self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            print(f"Duration:             {end - start}")

        failed_count = len(self._failed_operations)
        if failed_count > 0:
            print(f"\n⚠ {failed_count} failed operations logged to failures file — re-run to retry")
        print("\nRun pdf_stage.py to download PDFs from the collected metadata.")


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Stage 1: Metadata Scraper (eCourts Mobile API)"
    )
    add_common_args(parser)
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Number of concurrent workers for case type processing (default: {DEFAULT_MAX_WORKERS})",
    )
    parser.add_argument(
        "--filter",
        choices=["Pending", "Disposed", "Both"],
        default="Both",
        help="Case status filter (default: Both)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Re-run all searches ignoring checkpoints (catches new cases filed since last run)",
    )
    parser.add_argument(
        "--no-pdfs",
        action="store_true",
        help="Skip PDF downloads (metadata only)",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable PDF compression (Ghostscript)",
    )
    parser.add_argument(
        "--parallel-complexes",
        type=int,
        default=1,
        help="Number of court complexes to scrape in parallel (default: 1)",
    )
    args = parser.parse_args()

    scraper = MobileScraper(
        s3_bucket=args.s3_bucket,
        local_dir=Path(args.local_dir),
        delay=args.delay,
        max_workers=args.max_workers,
        local_only=args.local_only,
        verify=args.verify,
        download_pdfs=not args.no_pdfs,
        compress_pdfs=not args.no_compress,
    )
    scraper.parallel_complexes = args.parallel_complexes

    stats = scraper.scrape(
        state_codes=args.states,
        district_codes=args.districts,
        complex_codes=args.complexes,
        start_year=args.start_year,
        end_year=args.end_year,
        pending_disposed=args.filter,
    )

    # Save stats
    stats_file = Path(args.local_dir) / "scrape_stats.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nStats saved to {stats_file}")

    # Save failures for retry
    if scraper._failed_operations:
        failures_file = Path(args.local_dir) / "scrape_failures.json"
        with open(failures_file, "w") as f:
            json.dump(scraper._failed_operations, f, indent=2)
        print(f"Failures saved to {failures_file} ({len(scraper._failed_operations)} operations)")


if __name__ == "__main__":
    main()
