"""
Generate Parquet tables from metadata tar archives in S3.

Produces two tables:
  - cases: One row per case (flattened summary + location + history fields)
  - orders: One row per order/judgment (linked to case via case_no + cino)

Both tables are partitioned by state_code and reg_year.

USAGE:
    pip install pyspark
    spark-submit generate_parquet.py
    # or
    python generate_parquet.py --local  # for testing with local files
"""

import argparse
import io
import json
import os
import sys
import tarfile
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
    ArrayType, TimestampType,
)

S3_BUCKET = os.environ.get("S3_BUCKET", "indian-district-court-judgments-test")
S3_OUTPUT_PREFIX = "parquet"

# History fields to extract into the cases table (skip HTML blobs)
HISTORY_SCALAR_FIELDS = [
    "regcase_type", "date_of_filing", "dt_regis", "fil_no", "fil_year",
    "reg_no", "reg_year", "date_first_list", "date_next_list", "archive",
    "date_of_decision", "disp_nature", "purpose_next", "court_no",
    "pet_name", "pet_adv", "res_name", "res_adv",
    "under_act1", "under_sec1", "under_act2", "under_sec2",
    "fir_no", "fir_year", "police_st_code",
    "lower_court", "lower_court_dec_dt",
    "date_last_list", "type_name", "ltype_name",
    "court_name", "est_code", "desgname", "desgcode",
    "state_code", "district_code", "complex_code", "court_code",
]

# HTML/large fields to skip
HISTORY_SKIP_FIELDS = {
    "historyOfCaseHearing", "interimOrder", "finalOrder",
    "petNameAdd", "resNameAdd", "str_error", "str_error1",
    "subordinateCourtInfoStr", "act", "fir_details",
    "writinfo", "processes", "iaFiling", "transfer",
    "sub_matter", "main_matter", "link_cases", "casetype_conversion",
    "last_order",
}


def list_metadata_tars(spark, s3_bucket, local_dir=None):
    """List all metadata tar files from S3 or local filesystem."""
    if local_dir:
        tars = []
        for p in Path(local_dir).rglob("metadata.tar"):
            # Extract year/state/district/complex from path
            parts = p.relative_to(local_dir).parts
            if len(parts) >= 5:
                year, state, district, complex_code = parts[0], parts[1], parts[2], parts[3]
                tars.append({"path": str(p), "year": year, "state": state,
                            "district": district, "complex": complex_code})
        return tars

    # List from S3
    import boto3
    s3 = boto3.client("s3")
    tars = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_bucket, Prefix="metadata/tar/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("metadata.tar"):
                # Parse: metadata/tar/year=YYYY/state=XX/district=YY/complex=ZZ/metadata.tar
                import re
                m = re.search(
                    r"year=(\d+)/state=(\w+)/district=(\w+)/complex=(\w+)/",
                    key,
                )
                if m:
                    tars.append({
                        "path": f"s3://{s3_bucket}/{key}",
                        "year": m.group(1), "state": m.group(2),
                        "district": m.group(3), "complex": m.group(4),
                        "s3_key": key,
                    })
    return tars


def extract_cases_and_orders_from_tar(tar_path, s3_bucket=None, s3_key=None):
    """Extract case and order rows from a metadata tar file."""
    cases = []
    orders = []

    # Download from S3 if needed
    if s3_key:
        import boto3
        s3 = boto3.client("s3")
        buf = io.BytesIO()
        s3.download_fileobj(s3_bucket, s3_key, buf)
        buf.seek(0)
        tf = tarfile.open(fileobj=buf, mode="r")
    else:
        tf = tarfile.open(tar_path, "r")

    try:
        for member in tf.getmembers():
            if not member.name.endswith(".json") or not member.isfile():
                continue

            try:
                content = tf.extractfile(member).read()
                data = json.loads(content)
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                continue

            cs = data.get("case_summary", {})
            loc = data.get("location", {})
            hist = data.get("history", {})
            ords = data.get("orders", {})

            # Build case row
            case_row = {
                # Case summary
                "case_no": str(cs.get("case_no", "")),
                "cino": str(cs.get("cino", "")),
                "case_type": str(cs.get("case_type", "")),
                "case_number": str(cs.get("case_number", "")),
                "petitioner": str(cs.get("petitioner", "")),
                "case_court_code": str(cs.get("court_code", "")),

                # Location
                "state_code": int(loc.get("state_code", 0)),
                "state_name": str(loc.get("state_name", "")),
                "district_code": int(loc.get("district_code", 0)),
                "district_name": str(loc.get("district_name", "")),
                "complex_code": str(loc.get("complex_code", "")),
                "complex_name": str(loc.get("complex_name", "")),

                # Order counts
                "final_order_count": len(ords.get("final_orders", [])),
                "interim_order_count": len(ords.get("interim_orders", [])),
                "has_pdf": bool(ords.get("has_pdf", False)),

                # Metadata
                "scraped_at": str(data.get("scraped_at", "")),
                "source": str(data.get("source", "")),
            }

            # Flatten history scalar fields
            if isinstance(hist, dict):
                for field in HISTORY_SCALAR_FIELDS:
                    val = hist.get(field)
                    if val is not None:
                        case_row[f"h_{field}"] = str(val)
                    else:
                        case_row[f"h_{field}"] = None

                # Extract reg_year for partitioning
                reg_year = hist.get("reg_year")
                if reg_year:
                    try:
                        case_row["reg_year"] = int(reg_year)
                    except (ValueError, TypeError):
                        case_row["reg_year"] = None
                else:
                    case_row["reg_year"] = None
            else:
                for field in HISTORY_SCALAR_FIELDS:
                    case_row[f"h_{field}"] = None
                case_row["reg_year"] = None

            # Fallback reg_year from case_summary
            if case_row["reg_year"] is None:
                try:
                    case_row["reg_year"] = int(cs.get("reg_year", 0)) or None
                except (ValueError, TypeError):
                    case_row["reg_year"] = None

            cases.append(case_row)

            # Build order rows
            case_key = {
                "case_no": case_row["case_no"],
                "cino": case_row["cino"],
                "state_code": case_row["state_code"],
                "district_code": case_row["district_code"],
                "complex_code": case_row["complex_code"],
                "reg_year": case_row["reg_year"],
            }

            for order_list, is_final in [
                (ords.get("final_orders", []), True),
                (ords.get("interim_orders", []), False),
            ]:
                for o in order_list:
                    order_row = {
                        **case_key,
                        "order_number": o.get("order_number"),
                        "order_date": str(o.get("order_date", "")),
                        "order_type": str(o.get("order_type", "")),
                        "is_final": is_final,
                        "pdf_filename": str(o.get("pdf_filename", "")),
                        "has_pdf_url": bool(o.get("pdf_url")),
                    }
                    orders.append(order_row)
    finally:
        tf.close()

    return cases, orders


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet from metadata archives")
    parser.add_argument("--local", action="store_true", help="Read from local files instead of S3")
    parser.add_argument("--local-dir", type=str, default="local_mobile_data",
                       help="Local directory with metadata tars")
    parser.add_argument("--output", type=str, default=None,
                       help="Output path (local dir or s3:// path)")
    parser.add_argument("--s3-bucket", type=str, default=S3_BUCKET)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("eCourts Metadata to Parquet")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    print("Listing metadata archives...")
    if args.local:
        tars = list_metadata_tars(spark, args.s3_bucket, local_dir=args.local_dir)
    else:
        tars = list_metadata_tars(spark, args.s3_bucket)

    print(f"Found {len(tars)} metadata tar archives")

    if not tars:
        print("No archives found. Exiting.")
        return

    # Process all tars and collect rows
    all_cases = []
    all_orders = []
    total_tars = len(tars)

    for i, tar_info in enumerate(tars):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"Processing tar {i+1}/{total_tars}: {tar_info.get('path', tar_info.get('s3_key', ''))[:80]}")

        try:
            if args.local:
                cases, orders = extract_cases_and_orders_from_tar(tar_info["path"])
            else:
                cases, orders = extract_cases_and_orders_from_tar(
                    None, s3_bucket=args.s3_bucket, s3_key=tar_info["s3_key"]
                )
            all_cases.extend(cases)
            all_orders.extend(orders)
        except Exception as e:
            print(f"  Error processing {tar_info.get('path', tar_info.get('s3_key', ''))}: {e}")
            continue

    print(f"\nTotal cases: {len(all_cases)}")
    print(f"Total orders: {len(all_orders)}")

    if not all_cases:
        print("No cases extracted. Exiting.")
        spark.stop()
        return

    # Determine output path
    if args.output:
        output_base = args.output
    elif args.local:
        output_base = "parquet_output"
    else:
        output_base = f"s3a://{args.s3_bucket}/{S3_OUTPUT_PREFIX}"

    # Create DataFrames and write Parquet
    print(f"\nWriting cases table to {output_base}/cases/ ...")
    cases_df = spark.createDataFrame([Row(**c) for c in all_cases])

    # Cast reg_year and state_code for partitioning
    cases_df = (
        cases_df
        .withColumn("reg_year", F.col("reg_year").cast(IntegerType()))
        .withColumn("state_code", F.col("state_code").cast(IntegerType()))
    )

    cases_df.write.partitionBy("state_code", "reg_year").mode("overwrite").parquet(
        f"{output_base}/cases/"
    )

    cases_count = cases_df.count()
    print(f"  Written {cases_count} cases")

    if all_orders:
        print(f"Writing orders table to {output_base}/orders/ ...")
        orders_df = spark.createDataFrame([Row(**o) for o in all_orders])

        orders_df = (
            orders_df
            .withColumn("reg_year", F.col("reg_year").cast(IntegerType()))
            .withColumn("state_code", F.col("state_code").cast(IntegerType()))
        )

        orders_df.write.partitionBy("state_code", "reg_year").mode("overwrite").parquet(
            f"{output_base}/orders/"
        )

        orders_count = orders_df.count()
        print(f"  Written {orders_count} orders")
    else:
        print("No orders to write.")

    # Print schema summary
    print("\n=== Cases Schema ===")
    cases_df.printSchema()
    print(f"Partitions: state_code, reg_year")
    print(f"Total rows: {cases_count}")

    if all_orders:
        print("\n=== Orders Schema ===")
        orders_df.printSchema()

    # Print sample
    print("\n=== Sample Cases ===")
    cases_df.select(
        "case_no", "cino", "case_type", "state_name", "district_name",
        "complex_name", "reg_year", "final_order_count", "interim_order_count",
        "h_date_of_filing", "h_date_of_decision", "h_pet_name", "h_res_name",
    ).show(5, truncate=40)

    spark.stop()
    print(f"\nDone! Output at: {output_base}/")


if __name__ == "__main__":
    main()
