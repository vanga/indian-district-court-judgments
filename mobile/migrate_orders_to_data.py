"""
One-time migration script: rename orders.tar → data.tar and
orders.index.json → data.index.json in S3.

Part files (part-*.tar) don't need renaming — only the main archive
and index files use the archive_type as a filename prefix.

USAGE:
    # Dry run (list what would be renamed)
    uv run python migrate_orders_to_data.py --dry-run

    # Execute migration
    uv run python migrate_orders_to_data.py

    # Specific bucket
    uv run python migrate_orders_to_data.py --s3-bucket my-bucket
"""

import argparse
import json
import logging
import os

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")


def migrate(s3_bucket: str, dry_run: bool = True):
    s3 = boto3.client("s3")
    prefix = "data/tar/"

    paginator = s3.get_paginator("list_objects_v2")
    rename_pairs = []

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1]

            if filename == "orders.tar":
                new_key = key.rsplit("/", 1)[0] + "/data.tar"
                rename_pairs.append((key, new_key, "tar"))
            elif filename == "orders.index.json":
                new_key = key.rsplit("/", 1)[0] + "/data.index.json"
                rename_pairs.append((key, new_key, "index"))

    logger.info(f"Found {len(rename_pairs)} files to rename")

    if not rename_pairs:
        logger.info("Nothing to migrate")
        return

    renamed = 0
    errors = 0

    for old_key, new_key, file_type in rename_pairs:
        if dry_run:
            logger.info(f"  [DRY RUN] {old_key} → {new_key}")
            continue

        try:
            if file_type == "index":
                # Download, update archive_type and part names, re-upload
                response = s3.get_object(Bucket=s3_bucket, Key=old_key)
                data = json.loads(response["Body"].read().decode("utf-8"))

                # Update archive_type field
                if data.get("archive_type") == "orders":
                    data["archive_type"] = "data"

                # Update part names that reference orders.tar
                for part in data.get("parts", []):
                    if part.get("name") == "orders.tar":
                        part["name"] = "data.tar"

                s3.put_object(
                    Bucket=s3_bucket,
                    Key=new_key,
                    Body=json.dumps(data, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
            else:
                # Simple copy for tar files
                s3.copy_object(
                    Bucket=s3_bucket,
                    CopySource={"Bucket": s3_bucket, "Key": old_key},
                    Key=new_key,
                )

            # Delete original
            s3.delete_object(Bucket=s3_bucket, Key=old_key)
            renamed += 1
            logger.info(f"  Renamed: {old_key} → {new_key}")

        except Exception as e:
            errors += 1
            logger.error(f"  Failed: {old_key} → {e}")

    logger.info(f"\nMigration complete: {renamed} renamed, {errors} errors")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate orders.tar/orders.index.json to data.tar/data.index.json in S3"
    )
    parser.add_argument(
        "--s3-bucket", type=str, default=S3_BUCKET,
        help=f"S3 bucket name (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List files that would be renamed without actually renaming",
    )

    args = parser.parse_args()
    migrate(s3_bucket=args.s3_bucket, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
