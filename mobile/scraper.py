"""
Stage 1: Metadata Scraper (eCourts Mobile API).

Searches cases and collects metadata (case history, order info) without
downloading PDFs. PDF downloads are handled by Stage 2 (pdf_stage.py).

FEATURES:
- No CAPTCHA required (mobile API advantage)
- Syncs metadata to S3 using archive_manager
- Robust retry logic with exponential backoff
- Resume capability via S3 index files

USAGE:
    # Scrape metadata for a specific district
    uv run python scraper.py --state 29 --district 22 --start-year 1950 --end-year 2025

    # Dry run (no S3 upload)
    uv run python scraper.py --state 29 --district 22 --local-only

    # Then download PDFs (Stage 2)
    uv run python pdf_stage.py --state 29 --district 22 --start-year 1950 --end-year 2025
"""

import argparse
import concurrent.futures
import json
import logging
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

setup_logging()
logger = logging.getLogger(__name__)
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

    def _get_worker_client(self) -> MobileAPIClient:
        """Get or create a per-thread MobileAPIClient with its own JWT session."""
        client = getattr(self._thread_local, "client", None)
        if client is None:
            client = MobileAPIClient()
            client.initialize_session()
            self._thread_local.client = client
            logger.debug(f"Initialized worker client on thread {threading.current_thread().name}")
        return client

    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry."""
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
                wait_time = (2**attempt) + (0.1 * attempt)
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

        return {
            "case_summary": {
                "case_no": case.case_no,
                "cino": case.cino,
                "case_type": case.case_type,
                "case_number": case.case_number,
                "reg_year": case.reg_year,
                "petitioner": case.petitioner,
                "court_code": case.court_code,
            },
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
            "history": history,
            "scraped_at": datetime.now(IST).isoformat(),
            "source": "mobile_api",
        }

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
    ) -> bool:
        """
        Scrape a single case: get history, save metadata.

        PDF downloads are handled separately by Stage 2 (pdf_stage.py).
        Returns True if case was processed successfully.
        """
        # Check if already processed
        metadata_filename = f"{case.case_no}.json"
        if self.archive_manager.file_exists(
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
        self.archive_manager.add_to_archive(
            year=year,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            archive_type="metadata",
            filename=metadata_filename,
            content=metadata_json,
        )

        self._update_stats(cases_processed=1)
        return True

    def _process_case_type_task(
        self,
        task: CaseTypeTask,
        state: State,
        district: District,
        complex_: CourtComplex,
    ) -> Tuple[int, int]:
        """
        Process a single case type task (one case type + year + status combination).

        Returns tuple of (cases_found, cases_processed).
        """
        if self._interrupted:
            return (0, 0)

        # Skip if this search was already completed (unless --verify)
        if (
            not self.verify
            and self._checkpoint
            and self._checkpoint.is_completed(task.case_type_code, task.year, task.status)
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
            if self._checkpoint:
                self._checkpoint.record(task.case_type_code, task.year, task.status, 0)
            return (0, 0)

        cases_found = len(cases)
        self._update_stats(cases_found=cases_found)
        processed = 0

        for case in cases:
            if self._interrupted:
                break
            try:
                if self.scrape_case(case, state, district, complex_, task.year):
                    processed += 1
            except Exception as e:
                logger.debug(f"Error processing case {case.case_no}: {e}")
                self._update_stats(errors=1)

        # Checkpoint only if all cases were processed (no interrupt)
        if not self._interrupted and self._checkpoint:
            self._checkpoint.record(task.case_type_code, task.year, task.status, cases_found)

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

        Returns number of cases processed.
        """
        # Load search checkpoints for this complex
        self._checkpoint = SearchCheckpoint(
            s3_bucket=self.s3_bucket,
            local_dir=self.local_dir,
            state_code=str(state.code),
            district_code=str(district.code),
            complex_code=str(complex_.code),
            s3_client=self.archive_manager.s3 if self.archive_manager else None,
            local_only=self.local_only,
        )
        self._checkpoint.load()

        # Get case types
        time.sleep(self.delay)
        case_types = self._retry_with_backoff(
            self.client.get_case_types,
            state_code=state.code,
            dist_code=district.code,
            court_code=complex_.njdg_est_code,
        )

        if not case_types:
            logger.warning(f"No case types found for {complex_.name}")
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

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers
                ) as executor:
                    # Submit all tasks for this year
                    future_to_task = {
                        executor.submit(
                            self._process_case_type_task,
                            task,
                            state,
                            district,
                            complex_,
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
                if self.archive_manager and (year_found > 0 or year_processed > 0):
                    self.archive_manager.flush_complex_year(
                        year=year,
                        state_code=str(state.code),
                        district_code=str(district.code),
                        complex_code=str(complex_.code),
                    )

                # Flush search checkpoints after each year
                if self._checkpoint:
                    self._checkpoint.flush()

        # Final flush (e.g. from interrupt mid-year)
        if self._checkpoint:
            self._checkpoint.flush()

        logger.info(
            f"    Complex done: {total_found} cases found, {total_processed} processed"
        )
        return total_processed

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
        print("METADATA STAGE (Stage 1)")
        print("=" * 60)
        print(f"  Years: {start_year} to {end_year}")
        print(f"  Filter: {pending_disposed}")
        print(f"  Workers: {self.max_workers}")
        print(f"  S3 Bucket: {self.s3_bucket}")
        print(f"  Local Only: {self.local_only}")
        print(f"  Verify Mode: {self.verify}")
        print("=" * 60 + "\n")

        # Initialize session
        if not self._ensure_session():
            logger.error("Failed to initialize API session")
            return self.stats

        # Initialize archive manager
        self.archive_manager = S3ArchiveManager(
            s3_bucket=self.s3_bucket,
            s3_prefix="",
            local_dir=self.local_dir,
            immediate_upload=self.immediate_upload,
            local_only=self.local_only,
        )

        with self.archive_manager:
            # Get states
            states = self._retry_with_backoff(self.client.get_states)
            if not states:
                logger.error("Failed to get states")
                return self.stats

            if state_codes:
                states = [s for s in states if s.code in state_codes]

            # States progress bar
            states_pbar = tqdm(
                states,
                desc="States",
                unit="state",
                position=0,
                leave=True,
                ncols=100,
            )

            for state in states_pbar:
                if self._interrupted:
                    logger.info("Stopping due to interrupt - finalizing archives...")
                    break

                states_pbar.set_description(f"State: {state.name[:20]}")
                self._update_stats(states_processed=1)

                # Get districts
                time.sleep(self.delay)
                districts = self._retry_with_backoff(
                    self.client.get_districts,
                    state_code=state.code,
                )

                if not districts:
                    continue

                if district_codes:
                    districts = [d for d in districts if d.code in district_codes]

                # Districts progress bar
                districts_pbar = tqdm(
                    districts,
                    desc="  Districts",
                    unit="dist",
                    position=1,
                    leave=False,
                    ncols=100,
                )

                for district in districts_pbar:
                    if self._interrupted:
                        break

                    districts_pbar.set_description(f"  District: {district.name[:18]}")
                    self._update_stats(districts_processed=1)

                    # Get court complexes
                    time.sleep(self.delay)
                    complexes = self._retry_with_backoff(
                        self.client.get_court_complexes,
                        state_code=state.code,
                        dist_code=district.code,
                    )

                    if not complexes:
                        continue

                    if complex_codes:
                        complexes = [c for c in complexes if c.code in complex_codes]

                    # Complexes progress bar
                    complexes_pbar = tqdm(
                        complexes,
                        desc="    Complexes",
                        unit="complex",
                        position=2,
                        leave=False,
                        ncols=100,
                    )

                    for complex_ in complexes_pbar:
                        if self._interrupted:
                            break

                        complexes_pbar.set_description(
                            f"    Complex: {complex_.name[:16]}"
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
                            # Flushing now happens inside scrape_complex after each year completes.
                            # Flush any remaining archives for this complex (e.g. from interrupt or edge cases).
                            self.archive_manager.flush_complex(
                                state_code=str(state.code),
                                district_code=str(district.code),
                                complex_code=str(complex_.code),
                            )
                            # Update postfix with current stats
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
                            complexes_pbar.set_postfix(
                                {
                                    "cases": self.stats["cases_processed"],
                                    "skip": self.stats["cases_skipped"],
                                    "fail": fail_pct,
                                    "conn_err": self.stats["connection_errors"],
                                }
                            )
                            self._log_periodic_status()
                        except Exception as e:
                            logger.error(
                                f"Error processing complex {complex_.name}: {e}"
                            )
                            self._update_stats(errors=1)

                    complexes_pbar.close()
                districts_pbar.close()
            states_pbar.close()

        self.stats["end_time"] = datetime.now(IST).isoformat()

        # Print summary
        self._print_summary()

        return self.stats

    def _print_summary(self):
        """Print scraping summary."""
        print("\n" + "=" * 60)
        if self._interrupted:
            print("METADATA STAGE INTERRUPTED - Archives saved properly")
        else:
            print("METADATA STAGE COMPLETE")
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
    args = parser.parse_args()

    scraper = MobileScraper(
        s3_bucket=args.s3_bucket,
        local_dir=Path(args.local_dir),
        delay=args.delay,
        max_workers=args.max_workers,
        local_only=args.local_only,
        verify=args.verify,
    )

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
