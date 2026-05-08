"""
Re-enrich phase2_status="skipped" records in casetype tars using the mobile API.

These records were migrated from web casetype phase1 while the web portal was down,
and have no phase2 history data. This script fetches phase2 for each via the mobile
API and rewrites the tar both locally and on S3.

Two modes:
  Local mode (default): reads tars from --data-dir on disk.
  S3 mode (--from-s3):  lists and streams tars directly from S3 — no local data needed.
                        Ideal for running on EC2 where local data isn't available.

Usage (run from the mobile/ directory so pycryptodome is available):
    cd mobile

    # Local mode (has local_casetype_data/)
    uv run python ../web/enrich_skipped.py
    uv run python ../web/enrich_skipped.py --dry-run

    # S3 mode (EC2 or any machine without local data)
    uv run python ../web/enrich_skipped.py --from-s3
    uv run python ../web/enrich_skipped.py --from-s3 --workers 16 --dry-run
"""

import argparse
import io
import json
import logging
import os
import pathlib
import sys
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Optional

os.environ.setdefault("AWS_PROFILE", "dattam-od")
import boto3

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "mobile"))
from api_client import MobileAPIClient

IST = timezone(timedelta(hours=5, minutes=30))
S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")
REPO_ROOT = pathlib.Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_thread_local = threading.local()
_shared_jwt_lock = threading.Lock()
_shared_jwt: str = ""
_shared_jwt_initialized = False


def _get_shared_jwt() -> str:
    global _shared_jwt, _shared_jwt_initialized
    with _shared_jwt_lock:
        if _shared_jwt_initialized:
            return _shared_jwt
        bootstrap = MobileAPIClient()
        for attempt in range(5):
            try:
                if bootstrap.initialize_session():
                    _shared_jwt = bootstrap.jwt_token
                    break
            except Exception as e:
                logger.warning(f"Session init attempt {attempt + 1}/5: {e}")
            if attempt < 4:
                time.sleep((2 ** attempt) + 1)
        else:
            logger.error("Session init failed — proceeding without JWT")
        _shared_jwt_initialized = True
        return _shared_jwt


def _get_client() -> MobileAPIClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        jwt = _get_shared_jwt()
        client = MobileAPIClient()
        if jwt:
            client.jwt_token = jwt
            client._initialized = True
        _thread_local.client = client
        logger.debug(f"Client ready on {threading.current_thread().name}")
    return client


def _hval(h: dict, key: str):
    v = h.get(key)
    return v if v and v not in ("None", "0", "") else None


def _enrich_record(record: dict, history: Optional[dict]) -> dict:
    """Merge mobile API history into a skipped web casetype record."""
    h = history or {}

    court_no = _hval(h, "court_no") or _hval(h, "courtno") or ""
    judge = _hval(h, "desgname") or ""
    court_and_judge = f"{court_no} {judge}".strip() or None

    cs = dict(record["case_summary"])
    for field, val in [
        ("filing_date", _hval(h, "date_of_filing")),
        ("registration_date", _hval(h, "dt_regis")),
        ("first_hearing_date", _hval(h, "date_first_list")),
        ("next_hearing_date", _hval(h, "date_next_list")),
        ("court_number_and_judge", court_and_judge),
        ("petitioners_with_advocates", _hval(h, "petNameAdd")),
        ("respondents_with_advocates", _hval(h, "resNameAdd")),
    ]:
        if val:
            cs[field] = val

    has_data = bool(_hval(h, "date_of_filing") or _hval(h, "historyOfCaseHearing"))

    updated = dict(record)
    updated["case_summary"] = cs
    updated["history"] = history
    updated["phase2_scraped_at"] = datetime.now(IST).isoformat()
    updated["phase2_status"] = "enriched" if has_data else "no_data"
    return updated


def _s3_key_to_coords(s3_key: str) -> tuple[int, str, str, str]:
    """Parse year/state/district/complex from S3 key like metadata/tar/year=Y/state=S/..."""
    parts = s3_key.rstrip("/metadata.tar").split("/")
    coords = {p.split("=")[0]: p.split("=")[1] for p in parts if "=" in p}
    return int(coords["year"]), coords["state"], coords["district"], coords["complex"]


def _read_tar_bytes(tar_bytes: bytes) -> tuple[list[str], dict[str, bytes]]:
    member_names: list[str] = []
    member_bytes: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tf:
        for m in tf.getmembers():
            if not m.name.endswith(".json"):
                continue
            f = tf.extractfile(m)
            if f:
                member_names.append(m.name)
                member_bytes[m.name] = f.read()
    return member_names, member_bytes


def process_tar(
    tar_path: pathlib.Path,
    dry_run: bool,
    s3,
    stats: dict,
    stats_lock: threading.Lock,
) -> bool:
    """
    Enrich all skipped records in one tar. Rewrites tar locally and uploads to S3.
    Returns True if the tar was successfully processed (even if no skipped records found).
    """
    # Parse path: local_casetype_data/{year}/{state}/{district}/{complex}/metadata.tar
    parts = tar_path.parts
    complex_code = parts[-2]
    district_code = parts[-3]
    state_code = parts[-4]
    year = int(parts[-5])

    s3_key = (
        f"metadata/tar/year={year}/state={state_code}/"
        f"district={district_code}/complex={complex_code}/metadata.tar"
    )

    # Read all members in order
    member_names: list[str] = []
    member_bytes: dict[str, bytes] = {}
    try:
        with tarfile.open(tar_path, "r") as tf:
            for m in tf.getmembers():
                if not m.name.endswith(".json"):
                    continue
                f = tf.extractfile(m)
                if f:
                    member_names.append(m.name)
                    member_bytes[m.name] = f.read()
    except Exception as e:
        logger.error(f"Read error {tar_path}: {e}")
        return False

    if not member_names:
        return True

    # Parse and find skipped records
    parsed: dict[str, dict] = {}
    skipped_names: list[str] = []
    for name in member_names:
        try:
            rec = json.loads(member_bytes[name])
            parsed[name] = rec
            if rec.get("phase2_status") == "skipped":
                skipped_names.append(name)
        except Exception:
            pass

    if not skipped_names:
        return True

    # Enrich each skipped record
    client = _get_client()
    n_enriched = 0
    n_no_data = 0
    n_failed = 0

    for name in skipped_names:
        rec = parsed[name]
        loc = rec.get("location", {})
        cs = rec.get("case_summary", {})
        cino = cs.get("cino") or cs.get("case_no", "")
        if not cino:
            n_failed += 1
            continue

        try:
            history = client.get_case_history(
                state_code=int(loc.get("state_code", 0)),
                dist_code=int(loc.get("district_code", 0)),
                court_code=cs.get("court_code", ""),
                case_no=cino,
            )
            updated = _enrich_record(rec, history)
            parsed[name] = updated
            member_bytes[name] = json.dumps(updated, ensure_ascii=False).encode()
            if updated["phase2_status"] == "enriched":
                n_enriched += 1
            else:
                n_no_data += 1
        except Exception as e:
            logger.warning(f"API error {cino} in {tar_path.name}: {e}")
            n_failed += 1
            # Leave as skipped so next run retries

    with stats_lock:
        stats["enriched"] += n_enriched
        stats["no_data"] += n_no_data
        stats["failed"] += n_failed
        stats["tars_modified"] += 1

    if n_failed == len(skipped_names):
        # Every record failed — don't write a broken tar
        logger.error(f"{tar_path}: all {len(skipped_names)} records failed, skipping write")
        return False

    if dry_run:
        logger.info(
            f"[DRY-RUN] {tar_path}: {n_enriched} enriched, {n_no_data} no_data, "
            f"{n_failed} failed / {len(skipped_names)} skipped"
        )
        return True

    # Rebuild tar in memory preserving member order
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name in member_names:
            data = member_bytes[name]
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    tar_bytes = buf.getvalue()

    # Write locally (replace original)
    tar_path.write_bytes(tar_bytes)

    # Upload to S3
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=tar_bytes)
        logger.info(
            f"{year}/{state_code}/{district_code}/{complex_code}: "
            f"{n_enriched} enriched, {n_no_data} no_data, {n_failed} failed"
        )
    except Exception as e:
        logger.error(f"S3 upload failed {s3_key}: {e}")
        return False

    return True


def process_s3_tar(
    s3_key: str,
    dry_run: bool,
    s3,
    stats: dict,
    stats_lock: threading.Lock,
) -> bool:
    """
    S3 mode: stream tar from S3, enrich skipped records, upload back.
    No local disk needed beyond a small in-memory buffer.
    """
    year, state_code, district_code, complex_code = _s3_key_to_coords(s3_key)
    label = f"{year}/{state_code}/{district_code}/{complex_code}"

    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        tar_bytes = resp["Body"].read()
    except Exception as e:
        logger.error(f"S3 get failed {s3_key}: {e}")
        return False

    try:
        member_names, member_bytes = _read_tar_bytes(tar_bytes)
    except Exception as e:
        logger.error(f"Tar read error {label}: {e}")
        return False

    if not member_names:
        return True

    parsed: dict[str, dict] = {}
    skipped_names: list[str] = []
    for name in member_names:
        try:
            rec = json.loads(member_bytes[name])
            parsed[name] = rec
            if rec.get("phase2_status") == "skipped":
                skipped_names.append(name)
        except Exception:
            pass

    if not skipped_names:
        return True

    client = _get_client()
    n_enriched = 0
    n_no_data = 0
    n_failed = 0

    for name in skipped_names:
        rec = parsed[name]
        loc = rec.get("location", {})
        cs = rec.get("case_summary", {})
        cino = cs.get("cino") or cs.get("case_no", "")
        if not cino:
            n_failed += 1
            continue
        try:
            history = client.get_case_history(
                state_code=int(loc.get("state_code", 0)),
                dist_code=int(loc.get("district_code", 0)),
                court_code=cs.get("court_code", ""),
                case_no=cino,
            )
            updated = _enrich_record(rec, history)
            parsed[name] = updated
            member_bytes[name] = json.dumps(updated, ensure_ascii=False).encode()
            if updated["phase2_status"] == "enriched":
                n_enriched += 1
            else:
                n_no_data += 1
        except Exception as e:
            logger.warning(f"API error {cino} ({label}): {e}")
            n_failed += 1

    with stats_lock:
        stats["enriched"] += n_enriched
        stats["no_data"] += n_no_data
        stats["failed"] += n_failed
        stats["tars_modified"] += 1

    if n_failed == len(skipped_names):
        logger.error(f"{label}: all {len(skipped_names)} records failed, skipping write")
        return False

    if dry_run:
        logger.info(
            f"[DRY-RUN] {label}: {n_enriched} enriched, {n_no_data} no_data, "
            f"{n_failed} failed / {len(skipped_names)} skipped"
        )
        return True

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name in member_names:
            data = member_bytes[name]
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.getvalue())
        logger.info(f"{label}: {n_enriched} enriched, {n_no_data} no_data, {n_failed} failed")
    except Exception as e:
        logger.error(f"S3 put failed {s3_key}: {e}")
        return False

    return True


def _list_s3_tars(s3) -> list[str]:
    """List all metadata.tar keys in S3."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="metadata/tar/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/metadata.tar"):
                keys.append(obj["Key"])
    return sorted(keys)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich skipped phase2 casetype records via mobile API"
    )
    parser.add_argument("--workers", type=int, default=8, help="Parallel API threads")
    parser.add_argument("--dry-run", action="store_true", help="No writes")
    parser.add_argument("--from-s3", action="store_true",
                        help="Stream tars from S3 instead of local disk (use on EC2)")
    parser.add_argument(
        "--data-dir",
        type=pathlib.Path,
        default=REPO_ROOT / "local_casetype_data",
        help="Local casetype data dir (local mode only)",
    )
    parser.add_argument(
        "--checkpoint",
        type=pathlib.Path,
        default=pathlib.Path.home() / ".scraper-operator/enrich_skipped_checkpoint.json",
        help="Checkpoint file for resume",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N tars (testing)")
    args = parser.parse_args()

    # Load checkpoint
    completed: set[str] = set()
    if args.checkpoint.exists():
        try:
            data = json.loads(args.checkpoint.read_text())
            completed = set(data.get("completed", []))
            logger.info(f"Checkpoint: {len(completed)} tars already done")
        except Exception:
            pass

    checkpoint_lock = threading.Lock()

    def save_checkpoint(key: str):
        with checkpoint_lock:
            completed.add(key)
            args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
            args.checkpoint.write_text(
                json.dumps({"completed": sorted(completed)}, indent=2)
            )

    s3 = boto3.client("s3")

    if args.from_s3:
        logger.info("S3 mode: listing tars from S3...")
        all_keys = _list_s3_tars(s3)
        pending = [k for k in all_keys if k not in completed]
        if args.limit:
            pending = pending[: args.limit]
        logger.info(f"Found {len(all_keys)} tars in S3, {len(pending)} to process")

        if not pending:
            logger.info("Nothing to do.")
            return

        logger.info("Initializing shared JWT...")
        jwt = _get_shared_jwt()
        logger.info("JWT ready" if jwt else "Proceeding without JWT")

        stats = {"enriched": 0, "no_data": 0, "failed": 0, "tars_modified": 0}
        stats_lock = threading.Lock()
        start = time.time()
        processed = 0

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(process_s3_tar, k, args.dry_run, s3, stats, stats_lock): k
                for k in pending
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    ok = future.result()
                    if ok and not args.dry_run:
                        save_checkpoint(key)
                except Exception as e:
                    logger.error(f"Unhandled error {key}: {e}")

                processed += 1
                if processed % 20 == 0 or processed == len(pending):
                    elapsed = time.time() - start
                    rate = processed / elapsed * 3600
                    eta_h = (len(pending) - processed) / max(rate, 1)
                    with stats_lock:
                        s_copy = dict(stats)
                    logger.info(
                        f"Progress: {processed}/{len(pending)} tars | "
                        f"{s_copy['enriched']} enriched {s_copy['no_data']} no_data "
                        f"{s_copy['failed']} failed | {rate:.0f} tars/hr | ETA {eta_h:.1f}h"
                    )

    else:
        # Local mode
        all_tars = sorted(args.data_dir.glob("**/metadata.tar"))
        pending = [t for t in all_tars if str(t) not in completed]
        if args.limit:
            pending = pending[: args.limit]
        logger.info(f"Found {len(all_tars)} tars total, {len(pending)} to process")

        if not pending:
            logger.info("Nothing to do.")
            return

        logger.info("Initializing shared JWT...")
        jwt = _get_shared_jwt()
        logger.info("JWT ready" if jwt else "Proceeding without JWT")

        if args.dry_run:
            s3 = None

        stats = {"enriched": 0, "no_data": 0, "failed": 0, "tars_modified": 0}
        stats_lock = threading.Lock()
        start = time.time()
        processed = 0

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(process_tar, t, args.dry_run, s3, stats, stats_lock): t
                for t in pending
            }
            for future in as_completed(futures):
                tar_path = futures[future]
                try:
                    ok = future.result()
                    if ok and not args.dry_run:
                        save_checkpoint(str(tar_path))
                except Exception as e:
                    logger.error(f"Unhandled error {tar_path}: {e}")

                processed += 1
                if processed % 20 == 0 or processed == len(pending):
                    elapsed = time.time() - start
                    rate = processed / elapsed * 3600
                    eta_h = (len(pending) - processed) / max(rate, 1)
                    with stats_lock:
                        s_copy = dict(stats)
                    logger.info(
                        f"Progress: {processed}/{len(pending)} tars | "
                        f"{s_copy['enriched']} enriched {s_copy['no_data']} no_data "
                        f"{s_copy['failed']} failed | {rate:.0f} tars/hr | ETA {eta_h:.1f}h"
                    )

    elapsed = time.time() - start
    with stats_lock:
        s_copy = dict(stats)
    logger.info(
        f"Done: {processed} tars in {elapsed / 3600:.1f}h | "
        f"{s_copy['enriched']} enriched, {s_copy['no_data']} no_data, {s_copy['failed']} failed"
    )


if __name__ == "__main__":
    main()
