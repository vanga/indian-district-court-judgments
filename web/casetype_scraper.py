"""
web/casetype_scraper.py — Production Phase 1 case-type web scraper.

Iterates every court complex in courts.csv over all years (start_year to current),
finds every registered case (Pending + Disposed) for each case type via the
submit_case_type endpoint. Achieves full docket coverage equivalent to the mobile
API's searchByCaseType — including cases that have never had orders uploaded.

Reads mobile SearchCheckpoints from S3 to skip already-done searches.
Writes metadata to the same S3 paths as the mobile scraper
(metadata/tar/year=YYYY/state=XX/district=YY/complex=ZZ/metadata.tar).

NOTE: Records written here are sparse — they have case registry info (CNR, parties,
case type, status) but no order PDFs or case history. If the mobile API IP unblocks
before this scraper touches a complex, mobile writes rich records and this scraper
skips those CNRs. For complexes touched first by this scraper, a future enrichment
pass (Phase 2) can add history via viewHistory (CAPTCHA-free) and Phase 3 can add
PDFs via the order-date search.

Usage (from repo root):
    uv run --python 3.13 python web/casetype_scraper.py [options]

Run via tmux:
    tmux new-session -d -s web-casetype 'web/run_casetype_scraper.sh'
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import colorlog

# Path setup — must happen before local imports
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mobile"))

from archive_manager import S3ArchiveManager  # noqa: E402
from common import SearchCheckpoint  # noqa: E402  (from mobile/)
from download import BASE_URL, Downloader, DistrictCourtTask  # noqa: E402
from scrape_by_case_type import get_case_types_full, submit_case_type  # noqa: E402
from src.utils.court_utils import CourtComplex, load_courts_csv  # noqa: E402

# ─── Constants ────────────────────────────────────────────────────────────────

S3_BUCKET = "indian-district-court-judgments-test"
LOCAL_DATA_DIR = (
    Path.home()
    / "judgments"
    / "indian-district-court-judgments"
    / "local_casetype_data"
)
START_YEAR = 2020
STATUSES = ["Pending", "Disposed"]
IST = timezone(timedelta(hours=5, minutes=30))

# ─── Logging ──────────────────────────────────────────────────────────────────


def setup_logging(level: str = "INFO") -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


logger = logging.getLogger(__name__)

# ─── Graceful shutdown ────────────────────────────────────────────────────────

_shutdown = threading.Event()


def _on_signal(sig, frame):
    logger.info("Shutdown requested — finishing current tasks...")
    _shutdown.set()


signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)

# ─── Session helpers ──────────────────────────────────────────────────────────


def _make_task(court: CourtComplex) -> DistrictCourtTask:
    return DistrictCourtTask(
        id=str(uuid.uuid4()),
        state_code=court.state_code,
        state_name=court.state_name,
        district_code=court.district_code,
        district_name=court.district_name,
        complex_code=court.complex_code,
        complex_name=court.complex_name,
        court_numbers=court.court_numbers,
        from_date="01-01-2024",
        to_date="31-12-2024",
        order_type="both",
    )


def _init_session(d: Downloader) -> bool:
    """Initialize Downloader session. Returns True on success."""
    try:
        d.init_session()
        return d.set_court_data()
    except Exception as e:
        logger.warning(f"Session init error: {e}")
        return False


# ─── Metadata builder ─────────────────────────────────────────────────────────


def _build_metadata(
    row: Dict,
    case_type_code: str,
    case_type_label: str,
    case_status: str,
    year: int,
    court: CourtComplex,
    details: Optional[Dict] = None,
) -> Dict:
    """Build a metadata record. Merges viewHistory details if provided."""
    cnr = row.get("cnr", "")
    cs: Dict = {
        "case_no": cnr,
        "cino": cnr,
        "case_number": row.get("case_number", ""),
        "case_type": case_type_label,
        "case_type_code": case_type_code,
        "reg_year": year,
        "petitioner": row.get("petitioner", ""),
        "respondent": row.get("respondent", ""),
        "parties": row.get("parties", ""),
        "case_status": case_status,
        "court_code": row.get("court_code", ""),
        "establishment_name": row.get("establishment_name"),
        "internal_case_no": row.get("internal_case_no", ""),
        "search_flag": row.get("search_flag", "CScaseType"),
        "hideparty": row.get("hideparty", ""),
    }
    if details:
        for field in (
            "case_type_full", "filing_number", "filing_date",
            "registration_number", "registration_date",
            "first_hearing_date", "next_hearing_date",
            "case_stage", "court_number_and_judge",
            "petitioners_with_advocates", "respondents_with_advocates",
        ):
            if details.get(field):
                cs[field] = details[field]

    now = datetime.now(IST).isoformat()
    record: Dict = {
        "case_summary": cs,
        "location": {
            "state_code": court.state_code,
            "state_name": court.state_name,
            "district_code": court.district_code,
            "district_name": court.district_name,
            "complex_code": court.complex_code,
            "complex_name": court.complex_name,
        },
        "orders": {"final_orders": [], "interim_orders": [], "has_pdf": False},
        "acts": details.get("acts") or [] if details else [],
        "case_history": details.get("case_history") or [] if details else [],
        "phase1_scraped_at": now,
        "source": "web_casetype",
    }
    if details is not None:
        record["phase2_scraped_at"] = now
        has_content = bool(details.get("case_history") or details.get("acts"))
        record["phase2_status"] = "enriched" if has_content else "no_data"
    else:
        record["phase2_status"] = "skipped"
    return record


# ─── Per-complex worker ───────────────────────────────────────────────────────


def _process_complex(
    court: CourtComplex,
    archive_manager: S3ArchiveManager,
    args: argparse.Namespace,
) -> Dict:
    """
    Process all years for one court complex. Returns stats dict.
    Runs in its own thread; each call creates its own Downloader and S3 client.
    """
    label = f"{court.state_code}/{court.district_code}/{court.complex_code} ({court.complex_name})"
    stats = {"cases_saved": 0, "searches_done": 0, "searches_skipped": 0, "errors": 0}

    s3 = None if args.local_only else boto3.client("s3")
    checkpoint = SearchCheckpoint(
        s3_bucket=S3_BUCKET,
        local_dir=LOCAL_DATA_DIR / "checkpoints",
        state_code=court.state_code,
        district_code=court.district_code,
        complex_code=court.complex_code,
        s3_client=s3,
        local_only=args.local_only,
    )
    checkpoint.load()

    task = _make_task(court)
    d = Downloader(task, archive_manager, compress_pdfs=False, fetch_case_details=False)

    if not _init_session(d):
        logger.error(f"[{label}] session init failed, skipping")
        return stats

    # Session age tracking — reinit every 200 queries to guard against silent expiry
    queries_since_init = 0
    REINIT_INTERVAL = 200

    current_year = datetime.now(IST).year

    # Fetch case types once; they're stable across years for a given complex
    try:
        case_types = get_case_types_full(d)
    except Exception as e:
        logger.error(f"[{label}] fillCaseType failed: {e}")
        return stats

    if not case_types:
        logger.warning(f"[{label}] no case types returned")
        return stats

    total_searches = len(case_types) * len(STATUSES) * (current_year - args.start_year + 1)
    logger.info(
        f"[{label}] {len(case_types)} case types, "
        f"{current_year - args.start_year + 1} years, "
        f"~{total_searches} searches"
    )

    for year in range(args.start_year, current_year + 1):
        if _shutdown.is_set():
            break

        year_cases = 0

        for status in STATUSES:
            if _shutdown.is_set():
                break

            for code, label_ct in case_types:
                if _shutdown.is_set():
                    break

                if checkpoint.is_completed(code, year, status):
                    stats["searches_skipped"] += 1
                    continue

                # Periodic session reinit
                if queries_since_init >= REINIT_INTERVAL:
                    logger.debug(f"[{label}] reiniting session after {queries_since_init} queries")
                    if not _init_session(d):
                        logger.warning(f"[{label}] session reinit failed, continuing")
                    queries_since_init = 0

                payload, _ = submit_case_type(d, code, str(year), status)
                queries_since_init += 1

                # Retry once on null payload (session may have reset)
                if payload is None:
                    logger.warning(f"[{label}] null payload {code}/{year}/{status}, reiniting")
                    if _init_session(d):
                        queries_since_init = 0
                        payload, _ = submit_case_type(d, code, str(year), status)
                        queries_since_init += 1

                if payload is None:
                    stats["errors"] += 1
                    logger.warning(f"[{label}] giving up {code}/{year}/{status}")
                    continue

                rows = payload.get("rows", [])
                saved = 0
                for row in rows:
                    cnr = row.get("cnr")
                    if not cnr:
                        continue

                    details = None
                    if not args.skip_viewhistory:
                        case_info = {
                            "internal_case_no": row.get("internal_case_no", ""),
                            "cino": cnr,
                            "court_code": row.get("court_code", ""),
                            "hideparty": row.get("hideparty", ""),
                            "search_flag": row.get("search_flag", "CScaseType"),
                            "state_code": court.state_code,
                            "dist_code": court.district_code,
                            "court_complex_code": court.complex_code,
                            "search_by": row.get("search_flag", "CScaseType"),
                        }
                        vh_html = d.view_case_history(case_info)
                        queries_since_init += 1
                        if vh_html:
                            details = d.parse_case_details(vh_html)
                        time.sleep(0.05)

                    metadata = _build_metadata(row, code, label_ct, status, year, court, details)
                    archive_manager.add_to_archive(
                        year=year,
                        state_code=court.state_code,
                        district_code=court.district_code,
                        complex_code=court.complex_code,
                        archive_type="metadata",
                        filename=f"{cnr}.json",
                        content=json.dumps(metadata, ensure_ascii=False),
                    )
                    saved += 1

                total_reported = payload.get("total") or len(rows)
                checkpoint.record(code, year, status, found=total_reported)
                checkpoint.flush()

                stats["cases_saved"] += saved
                stats["searches_done"] += 1
                year_cases += saved

                if total_reported > 0:
                    logger.info(
                        f"[{label}] {status[:3]} {code} {year}: "
                        f"{total_reported} found, {saved} saved"
                    )

                time.sleep(0.3)

        # Flush archive after each year to upload incrementally
        archive_manager.flush_complex_year(
            year, court.state_code, court.district_code, court.complex_code
        )
        if year_cases > 0:
            logger.info(f"[{label}] year {year}: {year_cases} cases, archive flushed")

    logger.info(
        f"[{label}] DONE — "
        f"{stats['cases_saved']} saved, "
        f"{stats['searches_done']} done, "
        f"{stats['searches_skipped']} skipped, "
        f"{stats['errors']} errors"
    )
    return stats


# ─── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start_year", type=int, default=START_YEAR,
        help=f"First year to scrape (default: {START_YEAR})"
    )
    parser.add_argument(
        "--max_workers", type=int, default=4,
        help="Parallel court complexes (default: 4)"
    )
    parser.add_argument(
        "--courts_csv", default="courts.csv",
        help="Path to courts.csv (default: courts.csv)"
    )
    parser.add_argument(
        "--state_code", action="append", dest="state_codes", metavar="STATE",
        help="Restrict to state code(s) — can be repeated: --state_code 9 --state_code 11"
    )
    parser.add_argument(
        "--district_code",
        help="Restrict to one district code (requires a single --state_code)"
    )
    parser.add_argument("--complex_code", help="Restrict to one complex code")
    parser.add_argument(
        "--local_only", action="store_true",
        help="Skip S3 uploads (data stays in local_casetype_data/)"
    )
    parser.add_argument(
        "--skip_viewhistory", action="store_true",
        help="Skip inline viewHistory calls — Phase 1 only (faster, sparse records)"
    )
    parser.add_argument("--log_level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    courts: List[CourtComplex] = load_courts_csv(Path(args.courts_csv))

    if args.complex_code:
        courts = [c for c in courts if c.complex_code == args.complex_code]
    elif args.district_code and args.state_codes and len(args.state_codes) == 1:
        courts = [
            c for c in courts
            if c.state_code == args.state_codes[0] and c.district_code == args.district_code
        ]
    elif args.state_codes:
        state_set = set(args.state_codes)
        courts = [c for c in courts if c.state_code in state_set]

    if not courts:
        logger.error("No courts matched the given filters")
        sys.exit(1)

    logger.info(
        f"Starting case-type scraper: {len(courts)} complexes, "
        f"{args.max_workers} workers, years {args.start_year}–{datetime.now(IST).year}"
    )

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    with S3ArchiveManager(
        s3_bucket=S3_BUCKET,
        s3_prefix="",
        local_dir=LOCAL_DATA_DIR,
        local_only=args.local_only,
    ) as archive_manager:

        total = {"cases_saved": 0, "searches_done": 0, "searches_skipped": 0, "errors": 0}
        completed = 0

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(_process_complex, court, archive_manager, args): court
                for court in courts
            }

            for future in as_completed(futures):
                court = futures[future]
                try:
                    stats = future.result()
                    for k in total:
                        total[k] += stats.get(k, 0)
                except Exception as e:
                    logger.error(
                        f"Complex {court.complex_code} raised unhandled exception: {e}",
                        exc_info=True,
                    )

                completed += 1
                logger.info(
                    f"Progress: {completed}/{len(courts)} complexes — "
                    f"{total['cases_saved']} cases total"
                )

    logger.info(
        f"Finished: {total['cases_saved']} cases saved, "
        f"{total['searches_done']} searches done, "
        f"{total['searches_skipped']} skipped, "
        f"{total['errors']} errors"
    )


if __name__ == "__main__":
    main()
