"""
web/casetype_enrich.py — Backfill: enrich existing Phase 1-only records via viewHistory.

Reads sparse Phase 1 records (source="web_casetype", no enrichment_source) from local
archives and enriches them via viewHistory (no CAPTCHA). Used to backfill complexes that
were scraped before the main casetype_scraper gained inline viewHistory support.

Writes enriched records back to archive_type="metadata" (same metadata/tar/ S3 path).
Skips CNRs that already have enrichment_source set (already enriched inline).

The enriched record merges Phase 1 data with:
  - filing_date, registration_date, first/next hearing dates
  - case_stage, court_number_and_judge, case_type_full
  - petitioners_with_advocates, respondents_with_advocates
  - acts, case_history
  - enriched_at timestamp, enrichment_source marker

Writes to archive_type="enriched" (enriched.tar) — never touches metadata.tar,
so it is safe to run concurrently with casetype_scraper.py.

Usage (from repo root):
    uv run --python 3.13 python web/casetype_enrich.py [options]

Run via tmux:
    tmux new-session -d -s web-enrich 'web/run_casetype_enrich.sh'
"""

import argparse
import json
import logging
import signal
import sys
import tarfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import colorlog

# Path setup
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mobile"))

from archive_manager import S3ArchiveManager  # noqa: E402
from download import Downloader, DistrictCourtTask  # noqa: E402
from src.utils.court_utils import CourtComplex, load_courts_csv  # noqa: E402

# ─── Constants ────────────────────────────────────────────────────────────────

S3_BUCKET = "indian-district-court-judgments-test"
METADATA_DIR = (
    Path.home()
    / "judgments"
    / "indian-district-court-judgments"
    / "local_casetype_data"
)
ENRICH_CHECKPOINT_DIR = Path.home() / ".scraper-operator" / "enrich_checkpoints"
IST = timezone(timedelta(hours=5, minutes=30))

# ─── Logging ──────────────────────────────────────────────────────────────────


def setup_logging(level: str = "INFO") -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
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

# ─── Enrichment checkpoint ────────────────────────────────────────────────────


class EnrichCheckpoint:
    """Tracks per-complex which CNRs have been enriched via viewHistory."""

    def __init__(self, complex_code: str):
        self._path = ENRICH_CHECKPOINT_DIR / f"{complex_code}.json"
        self._lock = threading.Lock()
        self._data: dict = {}

    def load(self) -> None:
        ENRICH_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text())
            except Exception:
                self._data = {}

    def is_done(self, cnr: str) -> bool:
        with self._lock:
            return cnr in self._data

    def record(self, cnr: str, result: str) -> None:
        with self._lock:
            self._data[cnr] = result
            self._path.write_text(json.dumps(self._data, separators=(",", ":")))


# ─── Tar iteration ────────────────────────────────────────────────────────────


def iter_metadata_from_tar(tar_path: Path):
    """Yield (filename, metadata_dict) for every JSON in a tar archive."""
    try:
        with tarfile.open(tar_path, "r") as tf:
            for member in tf.getmembers():
                if not member.name.endswith(".json"):
                    continue
                f = tf.extractfile(member)
                if f is None:
                    continue
                try:
                    yield member.name, json.load(f)
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"Failed to open {tar_path}: {e}")


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
    try:
        d.init_session()
        return d.set_court_data()
    except Exception as e:
        logger.warning(f"Session init error: {e}")
        return False


# ─── Enrichment record builder ────────────────────────────────────────────────


def _build_enriched_record(phase1: Dict, details: Dict) -> Dict:
    """Merge Phase 1 metadata with viewHistory details into one record."""
    record = json.loads(json.dumps(phase1))  # deep copy

    # Overlay viewHistory fields onto case_summary
    cs = record.setdefault("case_summary", {})
    for field in (
        "case_type_full", "filing_number", "filing_date",
        "registration_number", "registration_date",
        "first_hearing_date", "next_hearing_date",
        "case_stage", "court_number_and_judge",
    ):
        if details.get(field):
            cs[field] = details[field]

    if details.get("petitioners_with_advocates"):
        cs["petitioners_with_advocates"] = details["petitioners_with_advocates"]
    if details.get("respondents_with_advocates"):
        cs["respondents_with_advocates"] = details["respondents_with_advocates"]

    record["acts"] = details.get("acts") or []
    record["case_history"] = details.get("case_history") or []
    record["enriched_at"] = datetime.now(IST).isoformat()
    record["enrichment_source"] = "web_viewHistory"

    return record


# ─── Per-complex worker ───────────────────────────────────────────────────────


def _enrich_complex(
    court: CourtComplex,
    enrich_am: S3ArchiveManager,
    args: argparse.Namespace,
) -> Dict:
    """
    For one court complex: read Phase 1 metadata → viewHistory → write enriched record.
    """
    label = f"{court.state_code}/{court.district_code}/{court.complex_code} ({court.complex_name})"
    stats = {"view_history_calls": 0, "enriched": 0, "no_history": 0, "errors": 0, "skipped": 0}

    # Collect all (year, CNR, case_info) from local Phase 1 tars
    cases: List[Tuple[int, str, Dict, Dict]] = []  # (year, cnr, case_info, phase1_record)
    for tar_path in sorted(METADATA_DIR.glob(
        f"*/{court.state_code}/{court.district_code}/{court.complex_code}/metadata.tar"
    )):
        try:
            year = int(tar_path.parts[-5])
        except (ValueError, IndexError):
            continue
        for _, record in iter_metadata_from_tar(tar_path):
            if record.get("source") != "web_casetype":
                continue
            if record.get("enrichment_source"):
                continue  # already enriched inline by casetype_scraper
            cs = record.get("case_summary", {})
            loc = record.get("location", {})
            cnr = cs.get("cino") or cs.get("case_no")
            if not cnr:
                continue
            case_info = {
                "internal_case_no": cs.get("internal_case_no", ""),
                "cino": cnr,
                "court_code": cs.get("court_code", ""),
                "hideparty": cs.get("hideparty", ""),
                "search_flag": cs.get("search_flag", "CScaseType"),
                "state_code": loc.get("state_code", court.state_code),
                "dist_code": loc.get("district_code", court.district_code),
                "court_complex_code": loc.get("complex_code", court.complex_code),
                "search_by": cs.get("search_flag", "CScaseType"),
            }
            cases.append((year, cnr, case_info, record))

    if not cases:
        return stats

    ckpt = EnrichCheckpoint(court.complex_code)
    ckpt.load()

    pending = [(y, cnr, ci, r) for y, cnr, ci, r in cases if not ckpt.is_done(cnr)]
    if not pending:
        logger.debug(f"[{label}] all {len(cases)} cases already enriched")
        return stats

    stats["skipped"] = len(cases) - len(pending)
    logger.info(f"[{label}] {len(pending)} to enrich ({len(cases)} total, {stats['skipped']} already done)")

    task = _make_task(court)
    d = Downloader(task, enrich_am, compress_pdfs=False, fetch_case_details=False)
    if not _init_session(d):
        logger.error(f"[{label}] session init failed")
        return stats

    queries_since_init = 0
    REINIT_INTERVAL = 300

    for year, cnr, case_info, phase1_record in pending:
        if _shutdown.is_set():
            break

        if queries_since_init >= REINIT_INTERVAL:
            _init_session(d)
            queries_since_init = 0

        html = d.view_case_history(case_info)
        queries_since_init += 1
        stats["view_history_calls"] += 1

        if not html:
            stats["errors"] += 1
            logger.debug(f"[{label}] viewHistory empty for {cnr}")
            ckpt.record(cnr, "error")
            continue

        details = d.parse_case_details(html)

        if not details.get("case_history") and not details.get("acts"):
            stats["no_history"] += 1
            ckpt.record(cnr, "no_history")
            time.sleep(0.1)
            continue

        enriched = _build_enriched_record(phase1_record, details)
        content = json.dumps(enriched, ensure_ascii=False)

        enrich_am.add_to_archive(
            year=year,
            state_code=court.state_code,
            district_code=court.district_code,
            complex_code=court.complex_code,
            archive_type="enriched",
            filename=f"{cnr}.json",
            content=content,
        )
        stats["enriched"] += 1
        ckpt.record(cnr, "enriched")
        logger.debug(f"[{label}] enriched {cnr}")
        time.sleep(0.1)

    # Flush enriched archives
    years_seen = {y for y, _, _, _ in pending}
    for year in years_seen:
        enrich_am.flush_complex_year(
            year, court.state_code, court.district_code, court.complex_code
        )

    logger.info(
        f"[{label}] DONE — "
        f"{stats['view_history_calls']} viewHistory, "
        f"{stats['enriched']} enriched, "
        f"{stats['no_history']} no-history, "
        f"{stats['skipped']} skipped, "
        f"{stats['errors']} errors"
    )
    return stats


# ─── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max_workers", type=int, default=8,
                        help="Parallel court complexes (default: 8)")
    parser.add_argument("--courts_csv", default="courts.csv")
    parser.add_argument("--state_code")
    parser.add_argument("--district_code")
    parser.add_argument("--complex_code")
    parser.add_argument("--local_only", action="store_true")
    parser.add_argument("--log_level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    courts: List[CourtComplex] = load_courts_csv(Path(args.courts_csv))
    if args.complex_code:
        courts = [c for c in courts if c.complex_code == args.complex_code]
    elif args.district_code and args.state_code:
        courts = [c for c in courts
                  if c.state_code == args.state_code and c.district_code == args.district_code]
    elif args.state_code:
        courts = [c for c in courts if c.state_code == args.state_code]

    # Only process complexes that have local Phase 1 data
    courts_with_data = [
        c for c in courts
        if list(METADATA_DIR.glob(
            f"*/{c.state_code}/{c.district_code}/{c.complex_code}/metadata.tar"
        ))
    ]

    if not courts_with_data:
        logger.info("No local Phase 1 metadata found yet — Phase 1 may still be running")
        return

    logger.info(
        f"Phase 2 enrichment: {len(courts_with_data)} complexes with data, "
        f"{args.max_workers} workers (no CAPTCHA)"
    )

    ENRICHED_DIR = METADATA_DIR.parent / "local_casetype_enriched"
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)

    with S3ArchiveManager(
        s3_bucket=S3_BUCKET,
        s3_prefix="",
        local_dir=ENRICHED_DIR,
        local_only=args.local_only,
    ) as enrich_am:

        total = {"view_history_calls": 0, "enriched": 0, "no_history": 0,
                 "skipped": 0, "errors": 0}
        completed = 0

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(_enrich_complex, court, enrich_am, args): court
                for court in courts_with_data
            }
            for future in as_completed(futures):
                court = futures[future]
                try:
                    stats = future.result()
                    for k in total:
                        total[k] += stats.get(k, 0)
                except Exception as e:
                    logger.error(f"Complex {court.complex_code}: {e}", exc_info=True)
                completed += 1
                logger.info(
                    f"Progress: {completed}/{len(courts_with_data)} — "
                    f"{total['enriched']} enriched, {total['view_history_calls']} viewHistory"
                )

    logger.info(
        f"Done: {total['enriched']} enriched, "
        f"{total['view_history_calls']} viewHistory calls, "
        f"{total['no_history']} no-history, "
        f"{total['skipped']} already done, "
        f"{total['errors']} errors"
    )


if __name__ == "__main__":
    main()
