"""
PDF Downloader using hybrid mobile + web API approach.

This module downloads PDFs by:
1. Reading case metadata from mobile API JSON files
2. Using the web API (services.ecourts.gov.in) to download PDFs

The mobile API provides the filename and case info, but cannot directly
download PDFs due to session token requirements. The web API requires
CAPTCHA solving but can download PDFs once a session is established.
"""

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

from crypto import decrypt_url_param


class PDFDownloader:
    """Download PDFs using web API with case info from mobile API."""

    WEB_BASE_URL = "https://services.ecourts.gov.in/ecourtindia_v6"

    def __init__(self, delay: float = 1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._app_token = None

    def _extract_app_token(self, html: str) -> Optional[str]:
        """Extract app_token from HTML response."""
        match = re.search(r'name="app_token"\s+value="([^"]+)"', html)
        if match:
            return match.group(1)
        match = re.search(r"app_token\s*=\s*['\"]([^'\"]+)['\"]", html)
        if match:
            return match.group(1)
        return None

    def init_session(self, state_code: str, district_code: str, complex_code: str) -> bool:
        """
        Initialize a web API session for a specific court.

        Args:
            state_code: State code
            district_code: District code
            complex_code: Court complex code

        Returns:
            True if session established successfully
        """
        try:
            # Step 1: Load main page to get initial cookies
            response = self.session.get(
                f"{self.WEB_BASE_URL}/?p=casestatus",
                timeout=30
            )
            if response.status_code != 200:
                return False

            self._app_token = self._extract_app_token(response.text)

            time.sleep(self.delay)

            # Step 2: Set state
            response = self.session.post(
                f"{self.WEB_BASE_URL}/?p=casestatus/fillDistrict",
                data={
                    "state_code": state_code,
                    "app_token": self._app_token or "",
                },
                timeout=30
            )
            self._app_token = self._extract_app_token(response.text) or self._app_token

            time.sleep(self.delay)

            # Step 3: Set district and complex
            response = self.session.post(
                f"{self.WEB_BASE_URL}/?p=casestatus/set_data",
                data={
                    "state_code": state_code,
                    "dist_code": district_code,
                    "court_complex_code": complex_code,
                    "app_token": self._app_token or "",
                },
                timeout=30
            )
            self._app_token = self._extract_app_token(response.text) or self._app_token

            return True

        except Exception as e:
            print(f"Session init error: {e}")
            return False

    def download_pdf_by_params(
        self,
        filename: str,
        case_val: str,
        court_code: str,
        output_path: str
    ) -> bool:
        """
        Download a PDF using web API's display_pdf endpoint.

        Args:
            filename: PDF filename (e.g., '/orders/2025/205400023292025_2.pdf')
            case_val: Case value/number
            court_code: Court code
            output_path: Local path to save PDF

        Returns:
            True if download successful
        """
        try:
            time.sleep(self.delay)

            # Call display_pdf endpoint
            response = self.session.post(
                f"{self.WEB_BASE_URL}/?p=home/display_pdf",
                data={
                    "normal_v": "normal_v",
                    "case_val": case_val,
                    "court_code": court_code,
                    "filename": filename,
                    "appFlag": "1",
                    "app_token": self._app_token or "",
                },
                timeout=60
            )

            if response.status_code != 200:
                return False

            # Try to parse JSON response with PDF path
            try:
                data = response.json()
                pdf_path = data.get("pdf_path") or data.get("path")
                if pdf_path:
                    # Download actual PDF
                    if not pdf_path.startswith("http"):
                        pdf_url = f"{self.WEB_BASE_URL}/{pdf_path.lstrip('/')}"
                    else:
                        pdf_url = pdf_path

                    time.sleep(self.delay)
                    pdf_response = self.session.get(pdf_url, timeout=120)

                    if pdf_response.status_code == 200 and pdf_response.content[:4] == b'%PDF':
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                        with open(output_path, 'wb') as f:
                            f.write(pdf_response.content)
                        return True

            except json.JSONDecodeError:
                pass

            return False

        except Exception as e:
            print(f"PDF download error: {e}")
            return False

    def process_mobile_json(self, json_path: str, output_dir: str) -> dict:
        """
        Process a mobile API JSON file and download its PDFs.

        Args:
            json_path: Path to mobile API case JSON file
            output_dir: Directory to save PDFs

        Returns:
            Stats dictionary with download results
        """
        stats = {"total": 0, "downloaded": 0, "failed": 0, "skipped": 0}

        with open(json_path, 'r') as f:
            data = json.load(f)

        location = data.get("location", {})
        state_code = str(location.get("state_code", ""))
        district_code = str(location.get("district_code", ""))
        complex_code = str(location.get("complex_code", ""))

        case_summary = data.get("case_summary", {})
        case_no = case_summary.get("case_no", "")
        court_code = case_summary.get("court_code", "")

        orders = data.get("orders", {})
        all_orders = orders.get("final_orders", []) + orders.get("interim_orders", [])

        if not all_orders:
            return stats

        # Initialize session for this court
        if not self.init_session(state_code, district_code, complex_code):
            print(f"Failed to initialize session for {json_path}")
            stats["failed"] = len(all_orders)
            return stats

        for order in all_orders:
            stats["total"] += 1

            pdf_url = order.get("pdf_url", "")
            if not pdf_url:
                stats["skipped"] += 1
                continue

            # Extract filename from PDF URL
            try:
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(pdf_url)
                query = parse_qs(parsed.query)
                params_enc = query.get("params", [""])[0]

                if params_enc:
                    params = decrypt_url_param(params_enc)
                    filename = params.get("filename", "")
                else:
                    stats["skipped"] += 1
                    continue

            except Exception:
                stats["skipped"] += 1
                continue

            # Generate output filename
            order_type = "final" if order.get("is_final") else "interim"
            order_num = order.get("order_number", 0)
            pdf_filename = f"{case_no}_{order_type}_{order_num}.pdf"
            output_path = os.path.join(output_dir, pdf_filename)

            if os.path.exists(output_path):
                stats["skipped"] += 1
                continue

            # Download PDF
            if self.download_pdf_by_params(filename, case_no, court_code, output_path):
                stats["downloaded"] += 1
                print(f"  Downloaded: {pdf_filename}")
            else:
                stats["failed"] += 1
                print(f"  Failed: {pdf_filename}")

        return stats


def main():
    """Process mobile API JSON files and download PDFs."""
    import argparse

    parser = argparse.ArgumentParser(description="Download PDFs from mobile API metadata")
    parser.add_argument("input", help="JSON file or directory containing JSON files")
    parser.add_argument("--output", default="./pdfs", help="Output directory for PDFs")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests")

    args = parser.parse_args()

    downloader = PDFDownloader(delay=args.delay)

    total_stats = {"total": 0, "downloaded": 0, "failed": 0, "skipped": 0}

    if os.path.isfile(args.input):
        files = [args.input]
    else:
        files = list(Path(args.input).rglob("*.json"))

    print(f"Processing {len(files)} files...")

    for json_file in files:
        print(f"\nProcessing: {json_file}")
        stats = downloader.process_mobile_json(str(json_file), args.output)

        for key in total_stats:
            total_stats[key] += stats.get(key, 0)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total orders: {total_stats['total']}")
    print(f"Downloaded: {total_stats['downloaded']}")
    print(f"Failed: {total_stats['failed']}")
    print(f"Skipped: {total_stats['skipped']}")


if __name__ == "__main__":
    main()
