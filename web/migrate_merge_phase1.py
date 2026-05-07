"""
web/migrate_merge_phase1.py — One-time migration: produce canonical combined metadata.

For each complex+year in local_casetype_data/:
  1. Read existing Phase 1 sparse records from metadata.tar
  2. Merge any already-enriched records from enriched.tar (reuse prior work)
  3. For CNRs still lacking Phase 2 data: call viewHistory (CAPTCHA-free) inline
  4. Write a fresh metadata.tar where every record is the combined form
  5. Upload to S3 metadata/tar/year=YYYY/state=XX/.../metadata.tar

Every output record has:
  - All Phase 1 fields (case_type, parties, reg_year, etc.)
  - Phase 2 fields when found (filing_date, hearing dates, judge, acts, case_history, etc.)
  - phase1_scraped_at: when Phase 1 was scraped
  - phase2_scraped_at: when Phase 2 was fetched (if done)
  - phase2_status: "enriched" | "no_data" | "failed"
    "enriched"  — viewHistory returned case details
    "no_data"   — viewHistory returned HTML but no history (expected for some cases)
    "failed"    — viewHistory call failed or returned nothing

Usage (from repo root):
    # Dry run first
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/migrate_merge_phase1.py --dry_run

    # Full run
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/migrate_merge_phase1.py

    # Restrict scope
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/migrate_merge_phase1.py --state_code 2
"""

import argparse
import io
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

import boto3
import colorlog

sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "mobile"))

from download import Downloader, DistrictCourtTask  # noqa: E402
from src.utils.court_utils import CourtComplex, load_courts_csv  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
S3_BUCKET = "indian-district-court-judgments-test"
METADATA_DIR = Path.home() / "judgments/indian-district-court-judgments/local_casetype_data"
ENRICHED_DIR = Path.home() / "judgments/indian-district-court-judgments/local_casetype_enriched"
OPERATOR_DIR = Path.home() / ".scraper-operator"
MIGRATION_STATE_FILE = OPERATOR_DIR / "migration_state.json"

_shutdown = threading.Event()
_state_lock = threading.Lock()


def _on_signal(sig, frame):
    logging.getLogger(__name__).info("Shutdown requested — finishing current tasks...")
    _shutdown.set()


signal.signal(signal.SIGINT, _on_signal)
signal.signal(signal.SIGTERM, _on_signal)


def setup_logging(level: str = "INFO") -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-8s%(reset)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


logger = logging.getLogger(__name__)


# ─── Migration state tracking ─────────────────────────────────────────────────

def _load_state() -> dict:
    try:
        return json.loads(MIGRATION_STATE_FILE.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    OPERATOR_DIR.mkdir(parents=True, exist_ok=True)
    MIGRATION_STATE_FILE.write_text(json.dumps(state, indent=2))


def _update_state(delta: dict) -> None:
    with _state_lock:
        state = _load_state()
        for k, v in delta.items():
            if isinstance(v, (int, float)) and k in state:
                state[k] += v
            else:
                state[k] = v
        state["last_updated"] = datetime.now(IST).isoformat()
        _save_state(state)


# ─── Tar I/O ──────────────────────────────────────────────────────────────────

def read_tar_records(tar_path: Path) -> Dict[str, dict]:
    """Return {cnr: record} from a tar of JSON files."""
    records = {}
    if not tar_path.exists():
        return records
    try:
        with tarfile.open(tar_path, "r") as tf:
            for member in tf.getmembers():
                if not member.name.endswith(".json"):
                    continue
                f = tf.extractfile(member)
                if f is None:
                    continue
                try:
                    rec = json.load(f)
                    cnr = (rec.get("case_summary", {}).get("cino")
                           or member.name.removesuffix(".json"))
                    records[cnr] = rec
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"Failed to read {tar_path}: {e}")
    return records


def write_tar(path: Path, records: Dict[str, dict]) -> int:
    """Write records dict to a fresh tar. Returns count written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".migrating")
    count = 0
    with tarfile.open(tmp, "w") as tf:
        for cnr, rec in records.items():
            data = json.dumps(rec, ensure_ascii=False).encode()
            info = tarfile.TarInfo(name=f"{cnr}.json")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
            count += 1
    tmp.rename(path)
    return count


# ─── Record building ──────────────────────────────────────────────────────────

def build_combined(phase1: dict, details: Optional[dict], phase2_status: str, phase2_at: Optional[str]) -> dict:
    """Merge Phase 1 record with viewHistory details into the canonical form."""
    rec = json.loads(json.dumps(phase1))  # deep copy

    # Normalise timestamps: rename scraped_at → phase1_scraped_at if needed
    if "scraped_at" in rec and "phase1_scraped_at" not in rec:
        rec["phase1_scraped_at"] = rec.pop("scraped_at")
    rec.pop("enriched_at", None)
    rec.pop("enrichment_source", None)

    rec["phase2_status"] = phase2_status
    if phase2_at:
        rec["phase2_scraped_at"] = phase2_at

    if details:
        cs = rec.setdefault("case_summary", {})
        for field in (
            "case_type_full", "filing_number", "filing_date",
            "registration_number", "registration_date",
            "first_hearing_date", "next_hearing_date",
            "case_stage", "court_number_and_judge",
            "petitioners_with_advocates", "respondents_with_advocates",
        ):
            if details.get(field):
                cs[field] = details[field]
        rec["acts"] = details.get("acts") or []
        rec["case_history"] = details.get("case_history") or []
    else:
        rec.setdefault("acts", [])
        rec.setdefault("case_history", [])

    return rec


def already_migrated(rec: dict) -> bool:
    """True if this record is already in the canonical combined form."""
    return "phase2_status" in rec


# ─── Session helpers ──────────────────────────────────────────────────────────

def _make_task(court: CourtComplex) -> DistrictCourtTask:
    return DistrictCourtTask(
        id=str(uuid.uuid4()),
        state_code=court.state_code, state_name=court.state_name,
        district_code=court.district_code, district_name=court.district_name,
        complex_code=court.complex_code, complex_name=court.complex_name,
        court_numbers=court.court_numbers,
        from_date="01-01-2024", to_date="31-12-2024", order_type="both",
    )


def _init_session(d: Downloader) -> bool:
    try:
        d.init_session()
        return d.set_court_data()
    except Exception as e:
        logger.warning(f"Session init error: {e}")
        return False


# ─── S3 upload ────────────────────────────────────────────────────────────────

def s3_key(tar_path: Path) -> str:
    parts = tar_path.relative_to(METADATA_DIR).parts
    year, state, district, complex_code, fname = parts
    return f"metadata/tar/year={year}/state={state}/district={district}/complex={complex_code}/{fname}"


def upload(tar_path: Path, s3_client) -> None:
    key = s3_key(tar_path)
    s3_client.upload_file(str(tar_path), S3_BUCKET, key)
    logger.info(f"  → s3://{S3_BUCKET}/{key}")


# ─── Per-complex-year migration ───────────────────────────────────────────────

def migrate_complex_year(
    meta_tar: Path,
    court: CourtComplex,
    downloader,
    dry_run: bool,
    local_only: bool,
    s3_client,
    skip_viewhistory: bool = False,
) -> dict:
    stats = {"total": 0, "already_done": 0, "reused": 0, "fetched": 0, "no_data": 0, "failed": 0, "skipped": 0}

    sparse = read_tar_records(meta_tar)
    parts = meta_tar.relative_to(METADATA_DIR).parts
    enrich_tar = ENRICHED_DIR / Path(*parts[:-1]) / "enriched.tar"
    enriched_existing = read_tar_records(enrich_tar)

    stats["total"] = len(sparse)
    if not sparse:
        return stats

    # Check if already fully migrated
    sample = next(iter(sparse.values()))
    if already_migrated(sample):
        stats["already_done"] = len(sparse)
        return stats

    if dry_run:
        reusable = sum(1 for cnr in sparse if cnr in enriched_existing)
        needs_fetch = len(sparse) - reusable
        logger.info(
            f"  DRY RUN {parts[-2]}: {len(sparse)} records — "
            f"{reusable} reuse enriched.tar, {needs_fetch} need viewHistory"
            + (" (will be skipped)" if skip_viewhistory else "")
        )
        return stats

    combined: Dict[str, dict] = {}
    now_str = datetime.now(IST).isoformat()

    for cnr, phase1_rec in sparse.items():
        if _shutdown.is_set():
            break

        # Already in canonical form (e.g. from prior partial run)
        if already_migrated(phase1_rec):
            combined[cnr] = phase1_rec
            stats["already_done"] += 1
            continue

        # Reuse from enriched.tar backfill
        if cnr in enriched_existing:
            enr = enriched_existing[cnr]
            combined[cnr] = build_combined(
                phase1_rec,
                details={
                    "acts": enr.get("acts"),
                    "case_history": enr.get("case_history"),
                    **{k: enr.get("case_summary", {}).get(k) for k in (
                        "case_type_full", "filing_number", "filing_date",
                        "registration_number", "registration_date",
                        "first_hearing_date", "next_hearing_date",
                        "case_stage", "court_number_and_judge",
                        "petitioners_with_advocates", "respondents_with_advocates",
                    )},
                },
                phase2_status="enriched",
                phase2_at=enr.get("enriched_at", now_str),
            )
            stats["reused"] += 1
            continue

        # Skip viewHistory — mark as skipped for later enrichment
        if skip_viewhistory:
            combined[cnr] = build_combined(phase1_rec, None, "skipped", None)
            stats["skipped"] += 1
            continue

        # Fetch via viewHistory
        cs = phase1_rec.get("case_summary", {})
        loc = phase1_rec.get("location", {})
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

        fetch_at = datetime.now(IST).isoformat()
        html = downloader.view_case_history(case_info)
        time.sleep(0.05)

        if not html:
            combined[cnr] = build_combined(phase1_rec, None, "failed", None)
            stats["failed"] += 1
            continue

        details = downloader.parse_case_details(html)
        if details.get("case_history") or details.get("acts"):
            combined[cnr] = build_combined(phase1_rec, details, "enriched", fetch_at)
            stats["fetched"] += 1
        else:
            combined[cnr] = build_combined(phase1_rec, details or None, "no_data", fetch_at)
            stats["no_data"] += 1

    if _shutdown.is_set():
        return stats

    written = write_tar(meta_tar, combined)
    logger.info(
        f"  Wrote {written} records to {meta_tar.name} "
        f"(reused={stats['reused']} fetched={stats['fetched']} "
        f"no_data={stats['no_data']} skipped={stats['skipped']} failed={stats['failed']})"
    )

    if not local_only and s3_client:
        upload(meta_tar, s3_client)

    return stats


# ─── Per-complex orchestration ────────────────────────────────────────────────

def migrate_complex(
    court: CourtComplex,
    dry_run: bool,
    local_only: bool,
    s3_client,
    skip_viewhistory: bool = False,
) -> dict:
    label = f"{court.state_code}/{court.district_code}/{court.complex_code}"
    totals = {"total": 0, "already_done": 0, "reused": 0, "fetched": 0, "no_data": 0, "failed": 0, "skipped": 0}

    meta_tars = sorted(METADATA_DIR.glob(
        f"*/{court.state_code}/{court.district_code}/{court.complex_code}/metadata.tar"
    ))
    if not meta_tars:
        return totals

    # No network calls needed when skipping viewHistory or doing a dry run
    d = None
    if not dry_run and not skip_viewhistory:
        from archive_manager import S3ArchiveManager  # noqa: E402
        dummy_am = S3ArchiveManager(
            s3_bucket=S3_BUCKET, s3_prefix="",
            local_dir=METADATA_DIR / "_tmp", local_only=True,
        )
        task = _make_task(court)
        d = Downloader(task, dummy_am, compress_pdfs=False, fetch_case_details=False)
        if not _init_session(d):
            logger.error(f"[{label}] session init failed — skipping")
            return totals

    logger.info(f"[{label}] {len(meta_tars)} year tars to migrate")
    for meta_tar in meta_tars:
        if _shutdown.is_set():
            break
        stats = migrate_complex_year(meta_tar, court, d, dry_run, local_only, s3_client, skip_viewhistory)
        for k in totals:
            totals[k] += stats.get(k, 0)

    logger.info(
        f"[{label}] DONE — {totals['total']} records: "
        f"reused={totals['reused']} fetched={totals['fetched']} "
        f"no_data={totals['no_data']} skipped={totals['skipped']} failed={totals['failed']} "
        f"already_done={totals['already_done']}"
    )
    if not dry_run:
        _update_state({
            "complexes_done": 1,
            "records_total": totals["total"],
            "records_enriched": totals["reused"] + totals["fetched"] + totals["already_done"],
            "records_no_data": totals["no_data"],
            "records_skipped": totals["skipped"],
            "records_failed": totals["failed"],
        })
    return totals


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--local_only", action="store_true")
    parser.add_argument("--skip_viewhistory", action="store_true",
                        help="Reuse enriched.tar backfill only; mark remaining as skipped. No network calls.")
    parser.add_argument("--state_code")
    parser.add_argument("--complex_code")
    parser.add_argument("--max_workers", type=int, default=8)
    parser.add_argument("--log_level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    courts: List[CourtComplex] = load_courts_csv(Path("courts.csv"))
    if args.complex_code:
        courts = [c for c in courts if c.complex_code == args.complex_code]
    elif args.state_code:
        courts = [c for c in courts if c.state_code == args.state_code]

    # Only complexes that have local Phase 1 data
    courts_with_data = [
        c for c in courts
        if list(METADATA_DIR.glob(
            f"*/{c.state_code}/{c.district_code}/{c.complex_code}/metadata.tar"
        ))
    ]

    if not courts_with_data:
        logger.info("No local Phase 1 data found")
        return

    mode = " (DRY RUN)" if args.dry_run else (" (skip_viewhistory — local merge only)" if args.skip_viewhistory else "")
    logger.info(f"Migrating {len(courts_with_data)} complexes, {args.max_workers} workers{mode}")

    s3_client = None if args.local_only else boto3.client("s3")

    if not args.dry_run:
        # Count total tars to give operator a denominator
        total_tars = sum(
            len(list(METADATA_DIR.glob(
                f"*/{c.state_code}/{c.district_code}/{c.complex_code}/metadata.tar"
            )))
            for c in courts_with_data
        )
        existing = _load_state()
        _save_state({
            **existing,
            "status": "running",
            "started_at": existing.get("started_at", datetime.now(IST).isoformat()),
            "complexes_total": len(courts_with_data),
            "complexes_done": existing.get("complexes_done", 0),
            "tars_total": total_tars,
            "records_total": existing.get("records_total", 0),
            "records_enriched": existing.get("records_enriched", 0),
            "records_no_data": existing.get("records_no_data", 0),
            "records_skipped": existing.get("records_skipped", 0),
            "records_failed": existing.get("records_failed", 0),
            "skip_viewhistory": args.skip_viewhistory,
            "last_updated": datetime.now(IST).isoformat(),
        })

    totals = {"total": 0, "already_done": 0, "reused": 0, "fetched": 0, "no_data": 0, "failed": 0, "skipped": 0}
    completed = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(migrate_complex, court, args.dry_run, args.local_only, s3_client, args.skip_viewhistory): court
            for court in courts_with_data
        }
        for future in as_completed(futures):
            if _shutdown.is_set():
                break
            court = futures[future]
            try:
                stats = future.result()
                for k in totals:
                    totals[k] += stats.get(k, 0)
            except Exception as e:
                logger.error(f"{court.complex_code}: {e}", exc_info=True)
            completed += 1
            if completed % 10 == 0:
                logger.info(f"Progress: {completed}/{len(courts_with_data)} complexes")

    if not args.dry_run:
        _update_state({"status": "complete" if not _shutdown.is_set() else "interrupted"})

    logger.info(
        f"\nMigration complete: {totals['total']:,} records across {completed} complexes\n"
        f"  reused from enriched.tar : {totals['reused']:,}\n"
        f"  fetched via viewHistory  : {totals['fetched']:,}\n"
        f"  no_data (expected)       : {totals['no_data']:,}\n"
        f"  skipped (no server)      : {totals['skipped']:,}\n"
        f"  failed                   : {totals['failed']:,}\n"
        f"  already in canonical form: {totals['already_done']:,}"
    )


if __name__ == "__main__":
    main()
