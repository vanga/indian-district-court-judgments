"""
web/casetype_enrich.py — Phase 2+3: CAPTCHA-free viewHistory enrichment + PDF download.

Reads Phase 1 metadata records (source="web_casetype") from local archives.
For each case:
  1. Calls viewHistory (NO CAPTCHA) → extracts displayPdf() onclick params
  2. Calls display_pdf for each order → downloads the actual PDF
  3. Saves PDFs to data/tar/year=YYYY/state=XX/district=YY/complex=ZZ/data.tar

Designed to run concurrently with casetype_scraper.py (Phase 1):
  Phase 1: web-casetype  (2 workers, CAPTCHA-gated, finding CNRs)
  Phase 2+3: web-enrich  (8 workers, CAPTCHA-free, downloading PDFs)

Skip logic:
  - A CNR is skipped if any PDF for it is already in data.tar (archive_manager.file_exists)
  - A viewHistory checkpoint (enriched.json per complex) tracks CNRs with no orders,
    so we don't re-call viewHistory for cases confirmed to have no PDFs.

Usage (from repo root):
    uv run --python 3.13 python web/casetype_enrich.py [options]

Run via tmux:
    tmux new-session -d -s web-enrich 'web/run_casetype_enrich.sh'
"""

import argparse
import json
import logging
import re
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

import boto3
import colorlog
from bs4 import BeautifulSoup

# Path setup
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mobile"))

from archive_manager import S3ArchiveManager  # noqa: E402
from download import BASE_URL, Downloader, DistrictCourtTask  # noqa: E402
from src.utils.court_utils import CourtComplex, load_courts_csv  # noqa: E402

# ─── Constants ────────────────────────────────────────────────────────────────

S3_BUCKET = "indian-district-court-judgments-test"
METADATA_DIR = (
    Path.home()
    / "judgments"
    / "indian-district-court-judgments"
    / "local_casetype_data"
)
PDF_DIR = (
    Path.home()
    / "judgments"
    / "indian-district-court-judgments"
    / "local_casetype_pdfs"
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

# ─── displayPdf extraction ────────────────────────────────────────────────────

_DISPLAY_PDF_RE = re.compile(
    r"displayPdf\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,"
    r"\s*'([^']+)'\s*,\s*'([^']*)'\s*\)"
)


def extract_display_pdf_params(html: str) -> List[Dict]:
    """Extract all unique displayPdf() params from viewHistory HTML."""
    results = []
    seen = set()
    for m in _DISPLAY_PDF_RE.finditer(html):
        filename = m.group(4)
        if filename in seen:
            continue
        seen.add(filename)
        results.append({
            "normal_v": m.group(1),
            "case_val": m.group(2),
            "court_code": m.group(3),
            "filename": filename,
            "app_flag": m.group(5),
        })
    return results


# ─── Enrichment checkpoint ────────────────────────────────────────────────────


class EnrichCheckpoint:
    """
    Tracks per-complex which CNRs have been processed by viewHistory.
    Stores "no_orders" CNRs so we don't re-call viewHistory for empty cases.
    """

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

    def record_no_orders(self, cnr: str) -> None:
        with self._lock:
            self._data[cnr] = "no_orders"
            self._flush_locked()

    def record_has_orders(self, cnr: str, count: int) -> None:
        with self._lock:
            self._data[cnr] = count
            self._flush_locked()

    def _flush_locked(self) -> None:
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


# ─── Per-complex worker ───────────────────────────────────────────────────────


def _enrich_complex(
    court: CourtComplex,
    pdf_am: S3ArchiveManager,
    args: argparse.Namespace,
) -> Dict:
    """
    For one court complex: read Phase 1 metadata → viewHistory → download PDFs.
    """
    label = f"{court.state_code}/{court.district_code}/{court.complex_code} ({court.complex_name})"
    stats = {"view_history_calls": 0, "pdfs_downloaded": 0, "pdfs_skipped": 0,
             "no_orders": 0, "errors": 0}

    # Collect all (year, CNR, viewHistory params) from local Phase 1 tars
    cases: List[Tuple[int, str, Dict]] = []  # (year, cnr, params)
    for tar_path in sorted(METADATA_DIR.glob(
        f"*/{court.state_code}/{court.district_code}/{court.complex_code}/metadata.tar"
    )):
        try:
            year = int(tar_path.parts[-5])
        except (ValueError, IndexError):
            continue
        for _, record in iter_metadata_from_tar(tar_path):
            if record.get("source") != "web_casetype":
                continue  # skip mobile records (already have PDFs via Stage 2)
            cs = record.get("case_summary", {})
            loc = record.get("location", {})
            cnr = cs.get("cino") or cs.get("case_no")
            if not cnr:
                continue
            # internal_case_no is optional — viewHistory works with just cino
            internal_case_no = cs.get("internal_case_no", "")
            cases.append((year, cnr, {
                "internal_case_no": internal_case_no,
                "cino": cnr,
                "court_code": cs.get("court_code", ""),
                "hideparty": cs.get("hideparty", ""),
                "search_flag": cs.get("search_flag", "CScaseType"),
                "state_code": loc.get("state_code", court.state_code),
                "dist_code": loc.get("district_code", court.district_code),
                "court_complex_code": loc.get("complex_code", court.complex_code),
                "search_by": cs.get("search_flag", "CScaseType"),
            }))

    if not cases:
        return stats

    # Load enrichment checkpoint — tracks CNRs with confirmed "no orders" to skip viewHistory
    ckpt = EnrichCheckpoint(court.complex_code)
    ckpt.load()

    # Filter to un-processed cases
    pending = [(y, cnr, params) for y, cnr, params in cases if not ckpt.is_done(cnr)]
    if not pending:
        logger.debug(f"[{label}] all {len(cases)} cases already processed")
        return stats

    logger.info(f"[{label}] {len(pending)} cases to process ({len(cases)} total)")

    # Initialize Downloader session for viewHistory + display_pdf
    dummy_am = S3ArchiveManager(
        s3_bucket=S3_BUCKET, s3_prefix="", local_dir=PDF_DIR / "_tmp", local_only=True
    )
    task = _make_task(court)
    d = Downloader(task, dummy_am, compress_pdfs=False, fetch_case_details=False)
    if not _init_session(d):
        logger.error(f"[{label}] session init failed")
        return stats

    queries_since_init = 0
    REINIT_INTERVAL = 300

    for year, cnr, case_info in pending:
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
            continue

        pdf_params_list = extract_display_pdf_params(html)

        if not pdf_params_list:
            ckpt.record_no_orders(cnr)
            stats["no_orders"] += 1
            time.sleep(0.1)
            continue

        ckpt.record_has_orders(cnr, len(pdf_params_list))

        for params in pdf_params_list:
            if _shutdown.is_set():
                break

            pdf_filename = params["filename"]

            # Skip if already downloaded
            if pdf_am.file_exists(
                year=year,
                state_code=court.state_code,
                district_code=court.district_code,
                complex_code=court.complex_code,
                archive_type="data",
                filename=pdf_filename,
            ):
                stats["pdfs_skipped"] += 1
                continue

            order_data = {
                "onclick": (
                    f"displayPdf('{params['normal_v']}','{params['case_val']}',"
                    f"'{params['court_code']}','{pdf_filename}','{params['app_flag']}')"
                )
            }
            pdf_bytes = d.download_pdf(order_data)
            if pdf_bytes:
                pdf_am.add_to_archive(
                    year=year,
                    state_code=court.state_code,
                    district_code=court.district_code,
                    complex_code=court.complex_code,
                    archive_type="data",
                    filename=pdf_filename,
                    content=pdf_bytes,
                )
                stats["pdfs_downloaded"] += 1
                logger.debug(f"[{label}] {cnr} → {pdf_filename} ({len(pdf_bytes)} bytes)")
            else:
                stats["errors"] += 1

            time.sleep(0.1)

        time.sleep(0.2)

    # Flush PDF archives
    for year in {y for y, _, _ in pending}:
        pdf_am.flush_complex_year(
            year, court.state_code, court.district_code, court.complex_code
        )

    logger.info(
        f"[{label}] DONE — "
        f"{stats['view_history_calls']} viewHistory, "
        f"{stats['pdfs_downloaded']} PDFs downloaded, "
        f"{stats['pdfs_skipped']} skipped, "
        f"{stats['no_orders']} no-order cases, "
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
        f"Phase 2+3: {len(courts_with_data)} complexes with data, "
        f"{args.max_workers} workers (no CAPTCHA)"
    )

    PDF_DIR.mkdir(parents=True, exist_ok=True)

    with S3ArchiveManager(
        s3_bucket=S3_BUCKET,
        s3_prefix="",
        local_dir=PDF_DIR,
        local_only=args.local_only,
    ) as pdf_am:

        total = {"view_history_calls": 0, "pdfs_downloaded": 0,
                 "pdfs_skipped": 0, "no_orders": 0, "errors": 0}
        completed = 0

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(_enrich_complex, court, pdf_am, args): court
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
                    f"{total['pdfs_downloaded']} PDFs, {total['view_history_calls']} viewHistory"
                )

    logger.info(
        f"Done: {total['pdfs_downloaded']} PDFs, "
        f"{total['view_history_calls']} viewHistory calls, "
        f"{total['no_orders']} no-order cases, "
        f"{total['errors']} errors"
    )


if __name__ == "__main__":
    main()
