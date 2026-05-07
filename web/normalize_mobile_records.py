"""
web/normalize_mobile_records.py — One-time normalisation of existing mobile API records on S3.

Reads mobile metadata.tar files from S3, converts them to the canonical combined schema
(same as web casetype records), and re-uploads in place. Safe to re-run — records that
already have phase2_status are skipped.

Canonical schema additions:
  - phase1_scraped_at  (renamed from scraped_at)
  - phase2_scraped_at  (same as phase1_scraped_at — mobile fetches everything inline)
  - phase2_status      ("enriched" | "no_data")
  - acts               []  (raw HTML is in history['act'])
  - case_history       []  (raw HTML is in history['historyOfCaseHearing'])
  - case_summary fields mapped from history: filing_date, registration_date,
    first/next_hearing_date, court_number_and_judge, petitioners/respondents_with_advocates

Usage (from repo root):
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/normalize_mobile_records.py
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/normalize_mobile_records.py --state_code 18
    AWS_PROFILE=dattam-od uv run --python 3.13 python web/normalize_mobile_records.py --dry_run
"""

import argparse
import io
import json
import logging
import sys
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import colorlog

IST = timezone(timedelta(hours=5, minutes=30))
S3_BUCKET = "indian-district-court-judgments-test"
S3_PREFIX = "metadata/tar/"


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


# ─── Normalisation ────────────────────────────────────────────────────────────

def _hval(h: dict, key: str) -> Optional[str]:
    v = h.get(key)
    return v if v and v not in ("None", "0", "") else None


def normalize_record(rec: dict) -> Tuple[dict, bool]:
    """
    Convert a mobile API record to canonical combined schema.
    Returns (normalised_record, was_changed).
    """
    if "phase2_status" in rec:
        return rec, False  # already canonical

    h = rec.get("history") or {}

    # scraped_at → phase1_scraped_at
    scraped_at = rec.pop("scraped_at", None)
    phase1_at = scraped_at or datetime.now(IST).isoformat()
    rec["phase1_scraped_at"] = phase1_at
    rec["phase2_scraped_at"] = phase1_at

    # Map history fields into case_summary
    cs = rec.setdefault("case_summary", {})
    court_no = _hval(h, "court_no") or _hval(h, "courtno") or ""
    judge = _hval(h, "desgname") or ""
    court_and_judge = f"{court_no} {judge}".strip() or None

    mappings = {
        "filing_date": _hval(h, "date_of_filing"),
        "registration_date": _hval(h, "dt_regis"),
        "first_hearing_date": _hval(h, "date_first_list"),
        "next_hearing_date": _hval(h, "date_next_list"),
        "court_number_and_judge": court_and_judge,
        "petitioners_with_advocates": _hval(h, "petNameAdd"),
        "respondents_with_advocates": _hval(h, "resNameAdd"),
    }
    for field, val in mappings.items():
        if val and not cs.get(field):
            cs[field] = val

    has_data = bool(_hval(h, "date_of_filing") or _hval(h, "historyOfCaseHearing"))
    rec["phase2_status"] = "enriched" if has_data else "no_data"
    rec.setdefault("acts", [])
    rec.setdefault("case_history", [])

    return rec, True


# ─── Tar I/O ──────────────────────────────────────────────────────────────────

def read_tar_from_bytes(data: bytes) -> Dict[str, dict]:
    records = {}
    with tarfile.open(fileobj=io.BytesIO(data)) as tf:
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
    return records


def write_tar_to_bytes(records: Dict[str, dict]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for cnr, rec in records.items():
            data = json.dumps(rec, ensure_ascii=False).encode()
            info = tarfile.TarInfo(name=f"{cnr}.json")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def list_mobile_tars(s3, state_filter: Optional[str] = None) -> List[str]:
    """List all metadata.tar keys on S3, optionally filtered by state."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    prefix = S3_PREFIX
    if state_filter:
        prefix = f"{S3_PREFIX}year="  # will filter by state below

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/metadata.tar"):
                continue
            if state_filter and f"/state={state_filter}/" not in key:
                continue
            keys.append(key)
    return keys


def process_tar(s3, key: str, dry_run: bool) -> dict:
    stats = {"records": 0, "normalised": 0, "already_done": 0, "errors": 0}

    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        raw = resp["Body"].read()
    except Exception as e:
        logger.error(f"Download failed {key}: {e}")
        stats["errors"] += 1
        return stats

    try:
        records = read_tar_from_bytes(raw)
    except Exception as e:
        logger.error(f"Tar read failed {key}: {e}")
        stats["errors"] += 1
        return stats

    stats["records"] = len(records)

    # Check source — skip if not mobile_api
    sample = next(iter(records.values()), {})
    if sample.get("source") != "mobile_api":
        logger.debug(f"Skipping non-mobile tar: {key}")
        return stats

    changed = False
    normalised = {}
    for cnr, rec in records.items():
        norm, was_changed = normalize_record(rec)
        normalised[cnr] = norm
        if was_changed:
            stats["normalised"] += 1
            changed = True
        else:
            stats["already_done"] += 1

    if not changed:
        logger.debug(f"All already canonical: {key}")
        return stats

    if dry_run:
        logger.info(f"DRY RUN {key}: {stats['normalised']} to normalise, {stats['already_done']} done")
        return stats

    try:
        new_bytes = write_tar_to_bytes(normalised)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=new_bytes)
        logger.info(f"  ✓ {key.split('/')[-3]}/{key.split('/')[-2]}: "
                    f"{stats['normalised']} normalised, {stats['already_done']} already done")
    except Exception as e:
        logger.error(f"Upload failed {key}: {e}")
        stats["errors"] += 1

    return stats


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state_code", help="Restrict to one state code")
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--max_workers", type=int, default=8)
    parser.add_argument("--log_level", default="INFO")
    args = parser.parse_args()
    setup_logging(args.log_level)

    s3 = boto3.client("s3")

    logger.info("Listing mobile metadata tars on S3...")
    keys = list_mobile_tars(s3, args.state_code)
    logger.info(f"Found {len(keys)} metadata.tar files{' (DRY RUN)' if args.dry_run else ''}")

    totals = {"records": 0, "normalised": 0, "already_done": 0, "errors": 0}
    completed = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(process_tar, s3, key, args.dry_run): key for key in keys}
        for future in as_completed(futures):
            key = futures[future]
            try:
                stats = future.result()
                for k in totals:
                    totals[k] += stats.get(k, 0)
            except Exception as e:
                logger.error(f"{key}: {e}")
                totals["errors"] += 1
            completed += 1
            if completed % 50 == 0:
                logger.info(f"Progress: {completed}/{len(keys)} tars")

    logger.info(
        f"\nDone: {completed} tars processed\n"
        f"  records total   : {totals['records']:,}\n"
        f"  normalised      : {totals['normalised']:,}\n"
        f"  already canonical: {totals['already_done']:,}\n"
        f"  errors          : {totals['errors']}"
    )


if __name__ == "__main__":
    main()
