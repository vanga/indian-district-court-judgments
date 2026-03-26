"""
Stage 2: PDF Download Stage.

Reads metadata archives (from Stage 1) and downloads PDFs independently.
Uses download_pdf_direct() with a fresh JWT session — fully independent of Stage 1.

USAGE:
    # Download PDFs for a specific district
    uv run python pdf_stage.py --state 29 --district 22 --start-year 2020 --end-year 2025

    # Dry run (no S3 upload)
    uv run python pdf_stage.py --state 29 --district 22 --local-only
"""

import argparse
import json
import logging
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from archive_manager import S3ArchiveManager
from api_client import MobileAPIClient
from common import (
    IST, S3_BUCKET, LOCAL_DIR, DEFAULT_DELAY,
    setup_logging, add_common_args, ScraperBase,
)
from gs import check_ghostscript_available, compress_pdf_bytes

setup_logging()
logger = logging.getLogger(__name__)

MAX_PDF_RETRIES = 3

COMPRESSION_AVAILABLE = check_ghostscript_available()
if COMPRESSION_AVAILABLE:
    logger.info("PDF compression enabled (Ghostscript found)")
else:
    logger.warning("PDF compression not available (Ghostscript not found)")


class PDFDownloadStage(ScraperBase):
    """
    Stage 2: Downloads PDFs by reading metadata archives as the work queue.

    Reads metadata.tar files from S3 (or local), extracts pdf_filename from
    each case JSON, and downloads PDFs using download_pdf_direct() with a
    fresh JWT session.
    """

    def __init__(
        self,
        s3_bucket: str = S3_BUCKET,
        local_dir: Path = LOCAL_DIR,
        delay: float = DEFAULT_DELAY,
        local_only: bool = False,
        immediate_upload: bool = True,
        compress_pdfs: bool = True,
    ):
        super().__init__(
            client=MobileAPIClient(),
            s3_bucket=s3_bucket,
            local_dir=local_dir,
            delay=delay,
        )
        self.local_only = local_only
        self.immediate_upload = immediate_upload
        self.compress_pdfs = compress_pdfs and COMPRESSION_AVAILABLE

        self.archive_manager: Optional[S3ArchiveManager] = None
        self.stats = {
            "locations_discovered": 0,
            "locations_processed": 0,
            "cases_read": 0,
            "pdfs_downloaded": 0,
            "pdfs_compressed": 0,
            "pdfs_skipped": 0,
            "pdfs_failed": 0,
            "pdfs_retried": 0,
            "pdfs_no_filename": 0,
            "bytes_saved": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None,
        }

    def _discover_locations(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        start_year: int = 2020,
        end_year: int = 2025,
    ) -> list[dict]:
        """Discover metadata archive locations from S3 or local filesystem."""
        if self.local_only:
            return self._discover_locations_local(
                state_codes, district_codes, start_year, end_year
            )

        # Single S3 listing, filter client-side
        all_locs = S3ArchiveManager.list_archive_locations(
            s3_bucket=self.s3_bucket,
            archive_type="metadata",
        )

        locations = []
        for loc in all_locs:
            year = int(loc["year"])
            if year < start_year or year > end_year:
                continue
            if state_codes and int(loc["state"]) not in state_codes:
                continue
            if district_codes and int(loc["district"]) not in district_codes:
                continue
            locations.append(loc)

        return locations

    def _discover_locations_local(
        self,
        state_codes: Optional[list[int]],
        district_codes: Optional[list[int]],
        start_year: int,
        end_year: int,
    ) -> list[dict]:
        """Discover metadata locations from local filesystem."""
        locations = []
        for index_file in sorted(self.local_dir.glob("*/*/*/*/metadata.index.json")):
            parts = index_file.relative_to(self.local_dir).parts
            year_s, state_s, district_s, complex_s = parts[0], parts[1], parts[2], parts[3]

            try:
                year = int(year_s)
            except ValueError:
                continue
            if year < start_year or year > end_year:
                continue

            try:
                if state_codes and int(state_s) not in state_codes:
                    continue
                if district_codes and int(district_s) not in district_codes:
                    continue
            except ValueError:
                continue

            locations.append(
                {"year": year_s, "state": state_s, "district": district_s, "complex": complex_s}
            )

        return locations

    def _download_pdf_with_retry(
        self,
        pdf_filename: str,
        case_no: str,
        court_code: str,
        year_str: str,
        state_code_str: str,
        district_code_str: str,
        complex_code_str: str,
    ) -> bool:
        """Download a single PDF with retry logic."""
        clean_filename = pdf_filename.lstrip("/").replace("/", "_")

        # Check if already downloaded
        if self.archive_manager.file_exists(
            year=int(year_str),
            state_code=state_code_str,
            district_code=district_code_str,
            complex_code=complex_code_str,
            archive_type="data",
            filename=clean_filename,
        ):
            self._update_stats(pdfs_skipped=1)
            return True

        temp_path = (
            self.local_dir
            / "temp"
            / f"{threading.current_thread().name}_{clean_filename}"
        )
        temp_path.parent.mkdir(parents=True, exist_ok=True)

        success = False
        for attempt in range(MAX_PDF_RETRIES):
            time.sleep(self.delay)

            try:
                download_result = self.client.download_pdf_direct(
                    state_code=int(state_code_str),
                    dist_code=int(district_code_str),
                    court_code=court_code,
                    filename=pdf_filename,
                    case_no=case_no,
                    output_path=str(temp_path),
                )

                if download_result and temp_path.exists():
                    with open(temp_path, "rb") as f:
                        header = f.read(4)

                    if header == b"%PDF":
                        success = True
                        if attempt > 0:
                            self._update_stats(pdfs_retried=1)
                        break
                    else:
                        logger.warning(
                            f"Invalid PDF content (attempt {attempt + 1}/{MAX_PDF_RETRIES}): {pdf_filename}"
                        )
                        if temp_path.exists():
                            temp_path.unlink()
                else:
                    logger.debug(
                        f"PDF download failed (attempt {attempt + 1}/{MAX_PDF_RETRIES}): {pdf_filename}"
                    )

            except Exception as e:
                logger.debug(
                    f"PDF download error (attempt {attempt + 1}/{MAX_PDF_RETRIES}): {pdf_filename} - {e}"
                )

            if attempt < MAX_PDF_RETRIES - 1:
                wait_time = (2**attempt) + (0.5 * attempt)
                time.sleep(wait_time)

        if success and temp_path.exists():
            with open(temp_path, "rb") as f:
                pdf_content = f.read()
            original_size = len(pdf_content)

            if self.compress_pdfs:
                try:
                    compressed_content = compress_pdf_bytes(
                        pdf_content, self.local_dir / "temp"
                    )
                    if len(compressed_content) < original_size:
                        saved = original_size - len(compressed_content)
                        self._update_stats(bytes_saved=saved, pdfs_compressed=1)
                        pdf_content = compressed_content
                except Exception as e:
                    logger.debug(f"PDF compression failed: {e}")

            self.archive_manager.add_to_archive(
                year=int(year_str),
                state_code=state_code_str,
                district_code=district_code_str,
                complex_code=complex_code_str,
                archive_type="data",
                filename=clean_filename,
                content=pdf_content,
            )
            self._update_stats(pdfs_downloaded=1)

            try:
                temp_path.unlink()
            except Exception:
                pass
            return True
        else:
            logger.warning(
                f"Failed to download PDF after {MAX_PDF_RETRIES} attempts: {pdf_filename}"
            )
            self._update_stats(pdfs_failed=1)
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass
            return False

    def _process_location(
        self,
        year_str: str,
        state_code_str: str,
        district_code_str: str,
        complex_code_str: str,
    ) -> int:
        """Process all cases in one (year, state, district, complex) location."""
        pdfs_processed = 0

        for filename, content in self.archive_manager.iter_archive_files(
            year=int(year_str),
            state_code=state_code_str,
            district_code=district_code_str,
            complex_code=complex_code_str,
            archive_type="metadata",
        ):
            if self._interrupted:
                break

            if not filename.endswith(".json"):
                continue

            try:
                case_data = json.loads(content)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.debug(f"Failed to parse {filename}: {e}")
                self._update_stats(errors=1)
                continue

            self._update_stats(cases_read=1)

            case_no = case_data.get("case_summary", {}).get("case_no", "")
            court_code = case_data.get("case_summary", {}).get("court_code", "")

            if not case_no or not court_code:
                continue

            orders = case_data.get("orders", {})
            all_orders = orders.get("final_orders", []) + orders.get(
                "interim_orders", []
            )

            for order in all_orders:
                if self._interrupted:
                    break

                pdf_filename = order.get("pdf_filename")
                if not pdf_filename:
                    self._update_stats(pdfs_no_filename=1)
                    continue

                if self._download_pdf_with_retry(
                    pdf_filename=pdf_filename,
                    case_no=case_no,
                    court_code=court_code,
                    year_str=year_str,
                    state_code_str=state_code_str,
                    district_code_str=district_code_str,
                    complex_code_str=complex_code_str,
                ):
                    pdfs_processed += 1

        # Flush archives for this location
        if self.archive_manager and pdfs_processed > 0:
            self.archive_manager.flush_complex_year(
                year=int(year_str),
                state_code=state_code_str,
                district_code=district_code_str,
                complex_code=complex_code_str,
            )

        return pdfs_processed

    def run(
        self,
        state_codes: Optional[list[int]] = None,
        district_codes: Optional[list[int]] = None,
        complex_codes: Optional[list[str]] = None,
        start_year: int = 2020,
        end_year: int = 2025,
    ) -> dict:
        """Main entry point: discover metadata locations and download PDFs."""
        self.stats["start_time"] = datetime.now(IST).isoformat()

        print("\n" + "=" * 60)
        print("PDF DOWNLOAD STAGE (Stage 2)")
        print("=" * 60)
        print(f"  Years: {start_year} to {end_year}")
        print(f"  S3 Bucket: {self.s3_bucket}")
        print(f"  Local Only: {self.local_only}")
        print(f"  Compression: {self.compress_pdfs}")
        print("=" * 60 + "\n")

        if not self._ensure_session():
            logger.error("Failed to initialize API session")
            return self.stats

        self.archive_manager = S3ArchiveManager(
            s3_bucket=self.s3_bucket,
            s3_prefix="",
            local_dir=self.local_dir,
            immediate_upload=self.immediate_upload,
            local_only=self.local_only,
        )

        with self.archive_manager:
            logger.info("Discovering metadata archive locations...")
            locations = self._discover_locations(
                state_codes=state_codes,
                district_codes=district_codes,
                start_year=start_year,
                end_year=end_year,
            )

            if complex_codes:
                locations = [
                    loc for loc in locations if loc["complex"] in complex_codes
                ]

            self.stats["locations_discovered"] = len(locations)
            logger.info(f"Found {len(locations)} metadata locations to process")

            if not locations:
                logger.warning("No metadata locations found - run Stage 1 first")
                return self.stats

            pbar = tqdm(locations, desc="Locations", unit="loc", ncols=100)

            for loc in pbar:
                if self._interrupted:
                    break

                pbar.set_description(
                    f"y={loc['year']} s={loc['state']} d={loc['district']} c={loc['complex'][:8]}"
                )

                try:
                    self._process_location(
                        year_str=loc["year"],
                        state_code_str=loc["state"],
                        district_code_str=loc["district"],
                        complex_code_str=loc["complex"],
                    )
                    self._update_stats(locations_processed=1)

                    pbar.set_postfix(
                        {
                            "dl": self.stats["pdfs_downloaded"],
                            "skip": self.stats["pdfs_skipped"],
                            "fail": self.stats["pdfs_failed"],
                        }
                    )
                except Exception as e:
                    logger.error(f"Error processing location {loc}: {e}")
                    self._update_stats(errors=1)

            pbar.close()

        self.stats["end_time"] = datetime.now(IST).isoformat()
        self._print_summary()
        return self.stats

    def _print_summary(self):
        print("\n" + "=" * 60)
        if self._interrupted:
            print("PDF DOWNLOAD INTERRUPTED - Archives saved properly")
        else:
            print("PDF DOWNLOAD COMPLETE")
        print("=" * 60)
        print(f"Locations discovered:  {self.stats['locations_discovered']}")
        print(f"Locations processed:   {self.stats['locations_processed']}")
        print(f"Cases read:            {self.stats['cases_read']}")
        print(f"PDFs downloaded:       {self.stats['pdfs_downloaded']}")
        print(f"PDFs skipped:          {self.stats['pdfs_skipped']}")
        if self.stats["pdfs_retried"] > 0:
            print(f"PDFs retried:          {self.stats['pdfs_retried']}")
        if self.compress_pdfs:
            print(f"PDFs compressed:       {self.stats['pdfs_compressed']}")
            saved_mb = self.stats["bytes_saved"] / (1024 * 1024)
            print(f"Space saved:           {saved_mb:.2f} MB")
        print(f"PDFs failed:           {self.stats['pdfs_failed']}")
        print(f"PDFs no filename:      {self.stats['pdfs_no_filename']}")
        print(f"Errors:                {self.stats['errors']}")

        if self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            print(f"Duration:              {end - start}")


def main():
    parser = argparse.ArgumentParser(
        description="PDF Download Stage (Stage 2) - Downloads PDFs from metadata archives"
    )
    add_common_args(parser)
    parser.add_argument(
        "--no-compress", action="store_true",
        help="Disable PDF compression (Ghostscript)",
    )

    args = parser.parse_args()

    stage = PDFDownloadStage(
        s3_bucket=args.s3_bucket,
        local_dir=Path(args.local_dir),
        delay=args.delay,
        local_only=args.local_only,
        compress_pdfs=not args.no_compress,
    )

    stats = stage.run(
        state_codes=args.states,
        district_codes=args.districts,
        complex_codes=args.complexes,
        start_year=args.start_year,
        end_year=args.end_year,
    )

    stats_file = Path(args.local_dir) / "pdf_stage_stats.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"\nStats saved to {stats_file}")


if __name__ == "__main__":
    main()
