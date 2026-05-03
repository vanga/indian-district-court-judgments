"""
web/casetype_pipeline.py — unified case-type discovery + enrichment pipeline.

Runs the complete web scraper in one process:
  - Phase 1 producer workers call submit_case_type (CAPTCHA-gated) and save sparse
    CNR metadata as soon as cases are discovered.
  - Phase 2+3 consumer workers immediately call viewHistory (CAPTCHA-free), extract
    displayPdf() params, and download PDFs.

One Downloader session is kept per court complex and shared between discovery and
enrichment for that complex. Access to each Downloader is serialized because the
session carries a rotating app_token.

Usage (from repo root):
    uv run --python 3.13 python web/casetype_pipeline.py [options]

Run via tmux:
    tmux new-session -d -s web-casetype-pipeline 'web/run_casetype_pipeline.sh'
"""

import argparse
import json
import logging
import queue
import re
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import boto3
import colorlog

# Path setup — must happen before local imports.
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mobile"))

from archive_manager import S3ArchiveManager  # noqa: E402
from common import SearchCheckpoint  # noqa: E402
from download import Downloader, DistrictCourtTask  # noqa: E402
from scrape_by_case_type import get_case_types_full, submit_case_type  # noqa: E402
from src.utils.court_utils import CourtComplex, load_courts_csv  # noqa: E402

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
START_YEAR = 2020
STATUSES = ["Pending", "Disposed"]
IST = timezone(timedelta(hours=5, minutes=30))
DISCOVERY_REINIT_INTERVAL = 200
ENRICH_REINIT_INTERVAL = 300

logger = logging.getLogger(__name__)
_shutdown = threading.Event()
_SENTINEL = object()

_DISPLAY_PDF_RE = re.compile(
    r"displayPdf\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,"
    r"\s*'([^']+)'\s*,\s*'([^']*)'\s*\)"
)


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


def _on_signal(sig, frame) -> None:
    if _shutdown.is_set():
        logger.warning("Second shutdown signal received; forcing exit")
        sys.exit(130)
    logger.info("Shutdown requested — stopping discovery and draining enrichment queue")
    _shutdown.set()


signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


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


def _build_metadata(
    row: Dict,
    case_type_code: str,
    case_type_label: str,
    case_status: str,
    year: int,
    court: CourtComplex,
) -> Dict:
    cnr = row.get("cnr", "")
    return {
        "case_summary": {
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
        },
        "location": {
            "state_code": court.state_code,
            "state_name": court.state_name,
            "district_code": court.district_code,
            "district_name": court.district_name,
            "complex_code": court.complex_code,
            "complex_name": court.complex_name,
        },
        "orders": {
            "final_orders": [],
            "interim_orders": [],
            "has_pdf": False,
        },
        "history": None,
        "scraped_at": datetime.now(IST).isoformat(),
        "source": "web_casetype",
    }


def _case_info_from_metadata(record: Dict) -> Tuple[str, Dict]:
    cs = record.get("case_summary", {})
    loc = record.get("location", {})
    cnr = cs.get("cino") or cs.get("case_no") or ""
    return cnr, {
        "internal_case_no": cs.get("internal_case_no", ""),
        "cino": cnr,
        "court_code": cs.get("court_code", ""),
        "hideparty": cs.get("hideparty", ""),
        "search_flag": cs.get("search_flag", "CScaseType"),
        "state_code": loc.get("state_code", ""),
        "dist_code": loc.get("district_code", ""),
        "court_complex_code": loc.get("complex_code", ""),
        "search_by": cs.get("search_flag", "CScaseType"),
    }


def extract_display_pdf_params(html: str) -> List[Dict]:
    results = []
    seen = set()
    for match in _DISPLAY_PDF_RE.finditer(html or ""):
        filename = match.group(4)
        if filename in seen:
            continue
        seen.add(filename)
        results.append(
            {
                "normal_v": match.group(1),
                "case_val": match.group(2),
                "court_code": match.group(3),
                "filename": filename,
                "app_flag": match.group(5),
            }
        )
    return results


class EnrichCheckpoint:
    """Tracks CNRs confirmed to have no uploaded orders for one complex."""

    def __init__(self, complex_code: str):
        self._path = ENRICH_CHECKPOINT_DIR / f"{complex_code}.json"
        self._lock = threading.Lock()
        self._data: dict[str, str] = {}
        self._dirty = False

    def load(self) -> None:
        ENRICH_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            if isinstance(data, dict):
                self._data = data
        except Exception:
            self._data = {}

    def is_no_orders(self, cnr: str) -> bool:
        with self._lock:
            return self._data.get(cnr) == "no_orders"

    def record_no_orders(self, cnr: str) -> None:
        with self._lock:
            self._data[cnr] = "no_orders"
            self._dirty = True

    def flush(self) -> None:
        with self._lock:
            if not self._dirty:
                return
            snapshot = dict(self._data)
            self._dirty = False
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(snapshot, separators=(",", ":")))


@dataclass
class PipelineStats:
    cases_saved: int = 0
    searches_done: int = 0
    searches_skipped: int = 0
    view_history_calls: int = 0
    pdfs_downloaded: int = 0
    pdfs_skipped: int = 0
    no_orders: int = 0
    errors: int = 0

    def add(self, other: "PipelineStats") -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))


@dataclass
class ComplexContext:
    court: CourtComplex
    downloader: Downloader
    search_checkpoint: SearchCheckpoint
    enrich_checkpoint: EnrichCheckpoint
    label: str
    session_lock: threading.Lock = field(default_factory=threading.Lock)
    enqueue_lock: threading.Lock = field(default_factory=threading.Lock)
    queued_cnrs: set[str] = field(default_factory=set)
    queries_since_init: int = 0

    def mark_queued(self, cnr: str) -> bool:
        with self.enqueue_lock:
            if cnr in self.queued_cnrs:
                return False
            self.queued_cnrs.add(cnr)
            return True

    def flush_checkpoints(self) -> None:
        self.search_checkpoint.flush()
        self.enrich_checkpoint.flush()


@dataclass
class EnrichItem:
    ctx: ComplexContext
    year: int
    cnr: str
    case_info: Dict


def _maybe_reinit_locked(ctx: ComplexContext, interval: int) -> bool:
    if ctx.queries_since_init < interval:
        return True
    logger.debug(f"[{ctx.label}] reiniting session after {ctx.queries_since_init} requests")
    ok = _init_session(ctx.downloader)
    ctx.queries_since_init = 0
    return ok


def _make_context(
    court: CourtComplex,
    metadata_am: S3ArchiveManager,
    args: argparse.Namespace,
) -> ComplexContext:
    s3 = None if args.local_only else boto3.client("s3")
    checkpoint = SearchCheckpoint(
        s3_bucket=S3_BUCKET,
        local_dir=METADATA_DIR,
        state_code=court.state_code,
        district_code=court.district_code,
        complex_code=court.complex_code,
        s3_client=s3,
        local_only=args.local_only,
    )
    checkpoint.load()

    enrich_checkpoint = EnrichCheckpoint(court.complex_code)
    enrich_checkpoint.load()

    task = _make_task(court)
    downloader = Downloader(
        task, metadata_am, compress_pdfs=False, fetch_case_details=False
    )
    label = f"{court.state_code}/{court.district_code}/{court.complex_code} ({court.complex_name})"
    return ComplexContext(court, downloader, checkpoint, enrich_checkpoint, label)


def _save_and_queue_case(
    ctx: ComplexContext,
    metadata_am: S3ArchiveManager,
    enrich_queue: "queue.Queue[EnrichItem | object]",
    row: Dict,
    case_type_code: str,
    case_type_label: str,
    status: str,
    year: int,
) -> bool:
    cnr = row.get("cnr")
    if not cnr:
        return False

    metadata = _build_metadata(row, case_type_code, case_type_label, status, year, ctx.court)
    metadata_am.add_to_archive(
        year=year,
        state_code=ctx.court.state_code,
        district_code=ctx.court.district_code,
        complex_code=ctx.court.complex_code,
        archive_type="metadata",
        filename=f"{cnr}.json",
        content=json.dumps(metadata, ensure_ascii=False),
    )

    if ctx.enrich_checkpoint.is_no_orders(cnr) or not ctx.mark_queued(cnr):
        return True

    _, case_info = _case_info_from_metadata(metadata)
    enrich_queue.put(EnrichItem(ctx=ctx, year=year, cnr=cnr, case_info=case_info))
    return True


def _process_complex(
    court: CourtComplex,
    metadata_am: S3ArchiveManager,
    enrich_queue: "queue.Queue[EnrichItem | object]",
    args: argparse.Namespace,
) -> PipelineStats:
    stats = PipelineStats()
    ctx = _make_context(court, metadata_am, args)

    if _shutdown.is_set():
        return stats

    try:
        with ctx.session_lock:
            if not _init_session(ctx.downloader):
                logger.error(f"[{ctx.label}] session init failed, skipping")
                stats.errors += 1
                return stats

            try:
                case_types = get_case_types_full(ctx.downloader)
                ctx.queries_since_init += 1
            except Exception as e:
                logger.error(f"[{ctx.label}] fillCaseType failed: {e}")
                stats.errors += 1
                return stats

        if not case_types:
            logger.warning(f"[{ctx.label}] no case types returned")
            return stats

        current_year = datetime.now(IST).year
        total_searches = len(case_types) * len(STATUSES) * (
            current_year - args.start_year + 1
        )
        logger.info(
            f"[{ctx.label}] {len(case_types)} case types, "
            f"{current_year - args.start_year + 1} years, ~{total_searches} searches"
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

                    if ctx.search_checkpoint.is_completed(code, year, status):
                        stats.searches_skipped += 1
                        continue

                    with ctx.session_lock:
                        _maybe_reinit_locked(ctx, DISCOVERY_REINIT_INTERVAL)
                        payload, _ = submit_case_type(ctx.downloader, code, str(year), status)
                        ctx.queries_since_init += 1

                        if payload is None:
                            logger.warning(
                                f"[{ctx.label}] null payload {code}/{year}/{status}, reiniting"
                            )
                            if _init_session(ctx.downloader):
                                ctx.queries_since_init = 0
                                payload, _ = submit_case_type(
                                    ctx.downloader, code, str(year), status
                                )
                                ctx.queries_since_init += 1

                    if payload is None:
                        stats.errors += 1
                        logger.warning(f"[{ctx.label}] giving up {code}/{year}/{status}")
                        continue

                    saved = 0
                    for row in payload.get("rows", []):
                        if _save_and_queue_case(
                            ctx,
                            metadata_am,
                            enrich_queue,
                            row,
                            code,
                            label_ct,
                            status,
                            year,
                        ):
                            saved += 1

                    total_reported = payload.get("total") or len(payload.get("rows", []))
                    ctx.search_checkpoint.record(code, year, status, found=total_reported)
                    ctx.search_checkpoint.flush()

                    stats.cases_saved += saved
                    stats.searches_done += 1
                    year_cases += saved

                    if total_reported > 0:
                        logger.info(
                            f"[{ctx.label}] {status[:3]} {code} {year}: "
                            f"{total_reported} found, {saved} queued"
                        )

                    time.sleep(0.3)

            metadata_am.flush_complex_year(
                year,
                court.state_code,
                court.district_code,
                court.complex_code,
            )
            if year_cases > 0:
                logger.info(f"[{ctx.label}] year {year}: {year_cases} cases, metadata flushed")

    finally:
        ctx.flush_checkpoints()
        logger.info(
            f"[{ctx.label}] discovery done — {stats.cases_saved} saved, "
            f"{stats.searches_done} searches, {stats.searches_skipped} skipped, "
            f"{stats.errors} errors"
        )

    return stats


def _download_one_pdf(
    item: EnrichItem,
    params: Dict,
    pdf_am: S3ArchiveManager,
    stats: PipelineStats,
) -> None:
    ctx = item.ctx
    filename = params["filename"]

    if pdf_am.file_exists(
        year=item.year,
        state_code=ctx.court.state_code,
        district_code=ctx.court.district_code,
        complex_code=ctx.court.complex_code,
        archive_type="data",
        filename=filename,
    ):
        stats.pdfs_skipped += 1
        return

    order_data = {
        "onclick": (
            f"displayPdf('{params['normal_v']}','{params['case_val']}',"
            f"'{params['court_code']}','{filename}','{params['app_flag']}')"
        )
    }

    with ctx.session_lock:
        _maybe_reinit_locked(ctx, ENRICH_REINIT_INTERVAL)
        pdf_bytes = ctx.downloader.download_pdf(order_data)
        ctx.queries_since_init += 1

        if pdf_bytes is None:
            logger.debug(f"[{ctx.label}] display_pdf failed for {item.cnr}/{filename}; reiniting")
            if _init_session(ctx.downloader):
                ctx.queries_since_init = 0
                pdf_bytes = ctx.downloader.download_pdf(order_data)
                ctx.queries_since_init += 1

    if not pdf_bytes:
        stats.errors += 1
        return

    pdf_am.add_to_archive(
        year=item.year,
        state_code=ctx.court.state_code,
        district_code=ctx.court.district_code,
        complex_code=ctx.court.complex_code,
        archive_type="data",
        filename=filename,
        content=pdf_bytes,
    )
    stats.pdfs_downloaded += 1
    logger.debug(f"[{ctx.label}] {item.cnr} -> {filename} ({len(pdf_bytes)} bytes)")


def _enrich_worker(
    worker_id: int,
    enrich_queue: "queue.Queue[EnrichItem | object]",
    pdf_am: S3ArchiveManager,
    stats: PipelineStats,
    stats_lock: threading.Lock,
) -> None:
    local = PipelineStats()

    while True:
        try:
            item = enrich_queue.get(timeout=1)
        except queue.Empty:
            if _shutdown.is_set():
                continue
            continue

        try:
            if item is _SENTINEL:
                return

            assert isinstance(item, EnrichItem)
            ctx = item.ctx

            if ctx.enrich_checkpoint.is_no_orders(item.cnr):
                continue

            with ctx.session_lock:
                _maybe_reinit_locked(ctx, ENRICH_REINIT_INTERVAL)
                html = ctx.downloader.view_case_history(item.case_info)
                ctx.queries_since_init += 1

                if not html:
                    logger.debug(f"[{ctx.label}] viewHistory empty for {item.cnr}; reiniting")
                    if _init_session(ctx.downloader):
                        ctx.queries_since_init = 0
                        html = ctx.downloader.view_case_history(item.case_info)
                        ctx.queries_since_init += 1

            local.view_history_calls += 1
            if not html:
                local.errors += 1
                continue

            pdf_params_list = extract_display_pdf_params(html)
            if not pdf_params_list:
                ctx.enrich_checkpoint.record_no_orders(item.cnr)
                ctx.enrich_checkpoint.flush()
                local.no_orders += 1
                time.sleep(0.1)
                continue

            for params in pdf_params_list:
                _download_one_pdf(item, params, pdf_am, local)
                time.sleep(0.1)

            time.sleep(0.2)

        except Exception as e:
            local.errors += 1
            logger.error(f"Enrichment worker {worker_id} failed item: {e}", exc_info=True)
        finally:
            enrich_queue.task_done()
            with stats_lock:
                stats.add(local)
                local = PipelineStats()


def _filter_courts(courts: List[CourtComplex], args: argparse.Namespace) -> List[CourtComplex]:
    if args.complex_code:
        return [c for c in courts if c.complex_code == args.complex_code]
    if args.district_code and args.state_code:
        return [
            c
            for c in courts
            if c.state_code == args.state_code and c.district_code == args.district_code
        ]
    if args.state_code:
        return [c for c in courts if c.state_code == args.state_code]
    return courts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start_year", type=int, default=START_YEAR)
    parser.add_argument("--max_workers", type=int, default=2, help="CAPTCHA workers")
    parser.add_argument("--enrich_workers", type=int, default=8)
    parser.add_argument("--courts_csv", default="courts.csv")
    parser.add_argument("--state_code")
    parser.add_argument("--district_code")
    parser.add_argument("--complex_code")
    parser.add_argument("--local_only", action="store_true")
    parser.add_argument("--log_level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    courts = _filter_courts(load_courts_csv(Path(args.courts_csv)), args)
    if not courts:
        logger.error("No courts matched the given filters")
        sys.exit(1)

    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Starting case-type pipeline: {len(courts)} complexes, "
        f"{args.max_workers} CAPTCHA workers, {args.enrich_workers} enrichment workers, "
        f"years {args.start_year}-{datetime.now(IST).year}"
    )

    enrich_queue: queue.Queue[EnrichItem | object] = queue.Queue(maxsize=5000)
    total = PipelineStats()
    stats_lock = threading.Lock()

    with (
        S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix="",
            local_dir=METADATA_DIR,
            local_only=args.local_only,
        ) as metadata_am,
        S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix="",
            local_dir=PDF_DIR,
            local_only=args.local_only,
        ) as pdf_am,
    ):
        consumers = [
            threading.Thread(
                target=_enrich_worker,
                args=(i + 1, enrich_queue, pdf_am, total, stats_lock),
                name=f"enrich-{i + 1}",
                daemon=False,
            )
            for i in range(args.enrich_workers)
        ]
        for consumer in consumers:
            consumer.start()

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {
                executor.submit(_process_complex, court, metadata_am, enrich_queue, args): court
                for court in courts
            }
            completed = 0
            for future in as_completed(futures):
                court = futures[future]
                try:
                    stats = future.result()
                    with stats_lock:
                        total.add(stats)
                except Exception as e:
                    logger.error(
                        f"Complex {court.complex_code} raised unhandled exception: {e}",
                        exc_info=True,
                    )
                    with stats_lock:
                        total.errors += 1
                completed += 1
                with stats_lock:
                    logger.info(
                        f"Discovery progress: {completed}/{len(courts)} complexes — "
                        f"{total.cases_saved} cases, {total.pdfs_downloaded} PDFs, "
                        f"queue={enrich_queue.qsize()}"
                    )

        logger.info("Discovery complete; waiting for enrichment queue to drain")
        enrich_queue.join()

        for _ in consumers:
            enrich_queue.put(_SENTINEL)
        for consumer in consumers:
            consumer.join()

        # Flush any still-open per-year archives before context manager exit uploads the rest.
        for court in courts:
            metadata_am.flush_complex(court.state_code, court.district_code, court.complex_code)
            pdf_am.flush_complex(court.state_code, court.district_code, court.complex_code)

    logger.info(
        f"Finished: {total.cases_saved} cases saved, {total.searches_done} searches done, "
        f"{total.searches_skipped} skipped, {total.view_history_calls} viewHistory, "
        f"{total.pdfs_downloaded} PDFs downloaded, {total.pdfs_skipped} PDFs skipped, "
        f"{total.no_orders} no-order cases, {total.errors} errors"
    )


if __name__ == "__main__":
    main()
