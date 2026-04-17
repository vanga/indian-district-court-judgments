"""
Flush all pending local archive files to S3 with correct index files.

Handles two cases:
1. part-YYYYMMDDTHHMMSS.tar — partial parts from in-progress scrape, never uploaded
2. data.tar / metadata.tar — either new (never on S3) or larger than S3 version

Run after stopping the scraper:
  AWS_PROFILE=dattam-od uv run python flush_pending.py

Index files are created/updated so the scraper won't re-download on next run.
"""
import sys
import os
import json
import tarfile
import boto3
import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")
LOCAL_BASE = Path(__file__).parent / "local_mobile_data"


def ist_now():
    tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    return datetime.datetime.now(tz).isoformat()


def get_s3_object_size(s3, key: str) -> int:
    """Return size of S3 object in bytes, or 0 if not found."""
    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=key)
        return resp["ContentLength"]
    except Exception:
        return 0


def load_s3_index(s3, index_key: str, year: int, state: str, district: str, complex_code: str, archive_type: str) -> dict:
    """Load existing S3 index or return a fresh empty one."""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key=index_key)
        return json.loads(resp["Body"].read())
    except Exception:
        return {
            "year": year, "state_code": state, "district_code": district,
            "complex_code": complex_code, "archive_type": archive_type,
            "file_count": 0, "total_size": 0, "total_size_human": "0 B",
            "created_at": ist_now(), "updated_at": ist_now(), "parts": []
        }


def read_tar_files(tar_path: Path) -> list[str]:
    with tarfile.open(tar_path, "r") as tf:
        return [m.name for m in tf.getmembers() if m.isfile()]


def upload_and_index(s3, local_tar: Path, part_name: str, archive_type: str,
                     year: int, state: str, district: str, complex_code: str,
                     replace_existing: bool = False):
    """Upload tar to S3 and create/update its index entry."""
    s3_dir = f"{archive_type}/tar/year={year}/state={state}/district={district}/complex={complex_code}/"
    s3_tar_key = f"{s3_dir}{part_name}"
    s3_index_key = f"{s3_dir}{archive_type}.index.json"

    size_bytes = local_tar.stat().st_size

    # Check if part already on S3 at same or larger size
    existing_size = get_s3_object_size(s3, s3_tar_key)
    if existing_size >= size_bytes and not replace_existing:
        print(f"  {part_name}: already on S3 ({existing_size/1e6:.1f}MB >= local {size_bytes/1e6:.1f}MB), skipping")
        local_tar.unlink()
        return

    # Read file list
    print(f"  Reading {part_name} ({size_bytes/1e6:.1f}MB)...")
    files = read_tar_files(local_tar)
    print(f"  {len(files)} files")

    # Upload tar
    print(f"  Uploading to s3://{S3_BUCKET}/{s3_tar_key} ...")
    s3.upload_file(str(local_tar), S3_BUCKET, s3_tar_key)
    print(f"  ✓ Uploaded")

    # Load index, replace or add this part
    index = load_s3_index(s3, s3_index_key, year, state, district, complex_code, archive_type)

    # Remove old entry for this part name if exists
    index["parts"] = [p for p in index["parts"] if p["name"] != part_name]

    # Add new entry
    index["parts"].append({
        "name": part_name,
        "files": files,
        "file_count": len(files),
        "size_bytes": size_bytes,
        "size_human": f"{size_bytes/1e6:.2f} MB",
        "created_at": ist_now(),
    })

    # Sort parts by name for deterministic ordering
    index["parts"].sort(key=lambda p: p["name"])
    index["file_count"] = sum(p["file_count"] for p in index["parts"])
    index["total_size"] = sum(p.get("size_bytes", 0) for p in index["parts"])
    index["total_size_human"] = f"{index['total_size']/1e9:.3f} GB"
    index["updated_at"] = ist_now()

    # Upload index
    s3.put_object(Bucket=S3_BUCKET, Key=s3_index_key, Body=json.dumps(index, indent=2))
    print(f"  ✓ Index updated: {index['file_count']} total files across {len(index['parts'])} parts")

    # Clean up local file
    local_tar.unlink()
    print(f"  ✓ Local file removed")


def detect_archive_type(tar_path: Path) -> str:
    """Detect whether tar contains PDFs (data) or JSONs (metadata)."""
    with tarfile.open(tar_path, "r") as tf:
        first = next((m.name for m in tf.getmembers() if m.isfile()), "")
    return "data" if first.endswith(".pdf") else "metadata"


def find_all_local_archives() -> list[tuple]:
    """
    Find all local tars under local_mobile_data.
    Returns list of (tar_path, part_name, archive_type, year, state, district, complex_code).
    """
    results = []
    for tar_path in sorted(LOCAL_BASE.rglob("*.tar")):
        parts = tar_path.parts
        try:
            idx = parts.index("local_mobile_data")
            year = int(parts[idx + 1])
            state = parts[idx + 2]
            district = parts[idx + 3]
            complex_code = parts[idx + 4]
            part_name = tar_path.name
            archive_type = detect_archive_type(tar_path)
            results.append((tar_path, part_name, archive_type, year, state, district, complex_code))
        except (ValueError, IndexError):
            print(f"  Skipping unexpected path: {tar_path}")
    return results


def main():
    s3 = boto3.client("s3")
    print(f"Flushing all local archives to s3://{S3_BUCKET}/")
    print()

    archives = find_all_local_archives()
    if not archives:
        print("No local archives found.")
        return

    print(f"Found {len(archives)} local archives:")
    for tar_path, part_name, archive_type, year, state, district, complex_code in archives:
        print(f"  {archive_type} year={year} state={state} district={district} complex={complex_code} / {part_name} ({tar_path.stat().st_size/1e6:.1f}MB)")
    print()

    for tar_path, part_name, archive_type, year, state, district, complex_code in archives:
        print(f"[{archive_type} year={year} {complex_code} / {part_name}]")
        # For data.tar/metadata.tar (first parts), replace_existing=True to handle
        # cases where local is larger than S3 (scraper wrote more after upload)
        replace_existing = part_name in ("data.tar", "metadata.tar")
        upload_and_index(s3, tar_path, part_name, archive_type,
                         year, state, district, complex_code,
                         replace_existing=replace_existing)
        print()

    print("All done. All local archives flushed and indexes updated.")


if __name__ == "__main__":
    main()
