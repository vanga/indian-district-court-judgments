"""
Indian District Court Judgments Downloader
Scrapes court orders from services.ecourts.gov.in

Usage:
    # Download locally for a date range
    python download.py --start_date 2025-01-01 --end_date 2025-01-03

    # Download for specific state/district/complex
    python download.py --state_code 24 --district_code 10 --complex_code 2400101 --start_date 2025-01-01 --end_date 2025-01-03

    # S3 sync mode (incremental)
    python download.py --sync-s3

    # S3 fill mode (historical backfill)
    python download.py --sync-s3-fill --timeout-hours 5.5
"""

import argparse
import concurrent.futures
import json
import logging
import random
import re
import sys
import threading
import time
import traceback
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional

import colorlog
import requests
import urllib3
from bs4 import BeautifulSoup
from PIL import Image
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from archive_manager import S3ArchiveManager
from src.captcha_solver.main import get_text
from src.utils.court_utils import CourtComplex, load_courts_csv
from src.gs import check_ghostscript_available, compress_pdf_if_enabled

# Configure logging
root_logger = logging.getLogger()
root_logger.setLevel("INFO")

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(
    colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", message=".*pin_memory.*not supported on MPS.*")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Check if Ghostscript is available for PDF compression
COMPRESSION_AVAILABLE = check_ghostscript_available()
if not COMPRESSION_AVAILABLE:
    logger.warning("PDF compression not available (Ghostscript not found)")

# Configuration
BASE_URL = "https://services.ecourts.gov.in/ecourtindia_v6/"
S3_BUCKET = "indian-district-court-judgments-test"
S3_PREFIX = ""
LOCAL_DIR = Path("./local_dc_judgments_data")
PACKAGES_DIR = Path("./packages")
IST = timezone(timedelta(hours=5, minutes=30))
START_DATE = "1950-01-01"
COMPLETED_TASKS_FILE = Path("./dc_completed_tasks.json")

# Directories for captcha handling
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)
captcha_failures_dir.mkdir(parents=True, exist_ok=True)

# Thread lock for completed tasks file
completed_tasks_lock = threading.Lock()

# Request headers
HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://services.ecourts.gov.in",
    "Referer": "https://services.ecourts.gov.in/ecourtindia_v6/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}


def get_task_key(task) -> str:
    """Generate a unique key for a task (court + date combination)"""
    return f"{task.state_code}_{task.district_code}_{task.complex_code}_{task.from_date}_{task.to_date}"


def load_completed_tasks() -> set:
    """Load completed tasks from file"""
    if not COMPLETED_TASKS_FILE.exists():
        return set()
    try:
        with open(COMPLETED_TASKS_FILE, "r") as f:
            data = json.load(f)
            return set(data.get("completed", []))
    except (json.JSONDecodeError, IOError):
        return set()


def save_completed_task(task_key: str):
    """Save a completed task to file (thread-safe)"""
    with completed_tasks_lock:
        completed = load_completed_tasks()
        completed.add(task_key)
        with open(COMPLETED_TASKS_FILE, "w") as f:
            json.dump({"completed": list(completed)}, f)


def is_task_completed(task) -> bool:
    """Check if a task has already been completed"""
    task_key = get_task_key(task)
    completed = load_completed_tasks()
    return task_key in completed


@dataclass
class DistrictCourtTask:
    """A task representing a date range to process for a specific court complex"""

    id: str
    state_code: str
    state_name: str
    district_code: str
    district_name: str
    complex_code: str
    complex_name: str
    court_numbers: str
    from_date: str  # DD-MM-YYYY format
    to_date: str  # DD-MM-YYYY format
    order_type: str = "both"  # "interim", "finalorder", or "both"

    def __str__(self):
        return f"Task({self.state_name}/{self.district_name}/{self.complex_name}, {self.from_date} to {self.to_date})"


def format_date_for_api(date_str: str) -> str:
    """Convert YYYY-MM-DD to DD-MM-YYYY format"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d-%m-%Y")


def parse_date_from_api(date_str: str) -> datetime:
    """Convert DD-MM-YYYY to datetime"""
    return datetime.strptime(date_str, "%d-%m-%Y")


def get_date_ranges(
    start_date: str, end_date: str, day_step: int = 1
) -> Generator[tuple[str, str], None, None]:
    """Generate date ranges in YYYY-MM-DD format"""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Cap at today
    today = datetime.now().date()
    if end_dt.date() > today:
        end_dt = datetime.combine(today, datetime.min.time())

    current = start_dt
    while current <= end_dt:
        range_end = min(current + timedelta(days=day_step - 1), end_dt)
        yield (current.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
        current = range_end + timedelta(days=1)


def generate_tasks(
    courts: List[CourtComplex],
    start_date: str,
    end_date: str,
    day_step: int = 1,
) -> Generator[DistrictCourtTask, None, None]:
    """Generate tasks for all courts and date ranges"""
    for from_date, to_date in get_date_ranges(start_date, end_date, day_step):
        for court in courts:
            yield DistrictCourtTask(
                id=str(uuid.uuid4()),
                state_code=court.state_code,
                state_name=court.state_name,
                district_code=court.district_code,
                district_name=court.district_name,
                complex_code=court.complex_code,
                complex_name=court.complex_name,
                court_numbers=court.court_numbers,
                from_date=format_date_for_api(from_date),
                to_date=format_date_for_api(to_date),
                order_type="both",  # "interim", "finalorder", or "both"
            )


class Downloader:
    """Downloads court orders from eCourts website"""

    def __init__(
        self,
        task: DistrictCourtTask,
        archive_manager: S3ArchiveManager,
        compress_pdfs: bool = True,
        fetch_case_details: bool = True,
    ):
        self.task = task
        self.archive_manager = archive_manager
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.app_token = None
        self.session_cookie = None
        # PDF compression (enabled by default if Ghostscript is available)
        self.compress_pdfs = compress_pdfs and COMPRESSION_AVAILABLE
        # Fetch detailed case information (CNR, filing date, hearing dates, etc.)
        self.fetch_case_details_enabled = fetch_case_details

    def _extract_app_token(self, html: str) -> Optional[str]:
        """Extract app_token from HTML content"""
        soup = BeautifulSoup(html, "lxml")
        token_input = soup.find("input", {"name": "app_token"})
        if token_input:
            return token_input.get("value", "")

        pattern = r"app_token['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]"
        match = re.search(pattern, html)
        if match:
            return match.group(1)

        pattern2 = r"app_token=([^&'\"]+)"
        match = re.search(pattern2, html)
        if match:
            return match.group(1)

        return None

    def _update_token(self, response_json: dict):
        """Update app_token from API response"""
        if "app_token" in response_json:
            self.app_token = response_json["app_token"]
            logger.debug(f"Updated app_token: {self.app_token[:20]}...")

    def init_session(self):
        """Initialize session and get app_token"""
        logger.debug(f"Initializing session for task: {self.task}")

        # Add small random delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.5))

        # Get the court orders page with retry
        url = f"{BASE_URL}?p=courtorder/index"
        response = self._fetch_with_retry("GET", url, timeout=30, verify=False)

        # Handle rate limiting (405 Security Page)
        if response.status_code == 405:
            logger.warning("Rate limited (405). Waiting 30s before retry...")
            time.sleep(30)
            response = self._fetch_with_retry("GET", url, timeout=30, verify=False)

        response.raise_for_status()

        # Extract app_token
        self.app_token = self._extract_app_token(response.text)
        if not self.app_token:
            token_match = re.search(r"app_token=([^&'\"]+)", response.url)
            if token_match:
                self.app_token = token_match.group(1)

        if not self.app_token:
            raise ValueError("Could not extract app_token from page")

        # Get session cookies
        self.session_cookie = response.cookies.get("SERVICES_SESSID")
        if not self.session_cookie:
            self.session_cookie = response.cookies.get("PHPSESSID")

        logger.debug(f"Got app_token: {self.app_token[:20]}...")

    def set_court_data(self):
        """Set the court complex in the session"""
        url = f"{BASE_URL}?p=casestatus/set_data"

        # Format: complex_code@court_numbers@flag
        complex_code_full = f"{self.task.complex_code}@{self.task.court_numbers}@N"

        data = {
            "complex_code": complex_code_full,
            "selected_state_code": self.task.state_code,
            "selected_dist_code": self.task.district_code,
            "selected_est_code": "null",
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        response = self._fetch_with_retry(
            "POST", url, data=data, timeout=30, verify=False
        )
        response.raise_for_status()

        result = response.json()
        self._update_token(result)

        if result.get("status") != 1:
            logger.warning(f"Failed to set court data: {result}")
            return False

        return True

    def solve_captcha(self, retries: int = 0) -> str:
        """Solve CAPTCHA using ONNX model"""
        if retries > 10:
            raise ValueError("Failed to solve CAPTCHA after 10 attempts")

        # Get captcha image with retry
        captcha_url = (
            f"{BASE_URL}vendor/securimage/securimage_show.php?{uuid.uuid4().hex}"
        )
        response = self._fetch_with_retry("GET", captcha_url, timeout=30, verify=False)

        # Save and process
        unique_id = uuid.uuid4().hex[:8]
        captcha_path = captcha_tmp_dir / f"captcha_dc_{unique_id}.png"
        with open(captcha_path, "wb") as f:
            f.write(response.content)

        try:
            img = Image.open(captcha_path)
            captcha_text = get_text(img).strip()

            # eCourts captcha is 6 characters
            if len(captcha_text) != 6:
                logger.debug(f"Invalid captcha length: {captcha_text}")
                captcha_path.unlink()
                return self.solve_captcha(retries + 1)

            captcha_path.unlink()
            return captcha_text

        except Exception as e:
            logger.error(f"Error solving captcha: {e}")
            # Move to failures dir for debugging (if file exists)
            if captcha_path.exists():
                new_path = (
                    captcha_failures_dir / f"{uuid.uuid4().hex[:8]}_{captcha_path.name}"
                )
                try:
                    captcha_path.rename(new_path)
                except Exception:
                    pass  # Ignore if file was already moved/deleted
            return self.solve_captcha(retries + 1)

    def search_orders(self) -> Optional[str]:
        """Search for orders by date range"""
        url = f"{BASE_URL}?p=courtorder/submitOrderDate"

        # Solve captcha
        captcha_code = self.solve_captcha()

        data = {
            "state_code": self.task.state_code,
            "dist_code": self.task.district_code,
            "court_complex": self.task.complex_code,
            "court_complex_arr": self.task.court_numbers,
            "est_code": "",
            "from_date": self.task.from_date,
            "to_date": self.task.to_date,
            "fradorderdt": self.task.order_type,  # "interim", "finalorder", or "both"
            "orderflagvaldate": self.task.order_type,  # "interim", "finalorder", or "both"
            "order_date_captcha_code": captcha_code,  # Correct field name for captcha
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry(
                "POST", url, data=data, timeout=60, verify=False
            )
            response.raise_for_status()

            # Check if response is JSON with token update
            try:
                result = response.json()
                self._update_token(result)

                # Check for captcha error
                if result.get("errormsg"):
                    logger.warning(f"Search error: {result.get('errormsg')}")
                    if "captcha" in result.get("errormsg", "").lower():
                        return self.search_orders()  # Retry with new captcha
                    return None

                # Check status
                if result.get("status") != 1:
                    logger.debug(f"Search returned non-success status: {result}")
                    return None

                # Check for HTML content in response (court_dt_data field)
                if "court_dt_data" in result:
                    return result["court_dt_data"]
                if "html" in result:
                    return result["html"]

            except json.JSONDecodeError:
                # Response is HTML
                return response.text

            return response.text

        except (
            ConnectionError,
            Timeout,
            ChunkedEncodingError,
            urllib3.exceptions.ProtocolError,
        ) as e:
            logger.error(f"Network error searching orders (after retries): {e}")
            return None
        except Exception as e:
            logger.error(f"Error searching orders: {e}")
            return None

    def parse_order_results(self, html: str) -> List[dict]:
        """Parse order search results from HTML"""
        soup = BeautifulSoup(html, "lxml")
        results = []

        # Look for the results table
        table = soup.find("table", {"id": "caseList"})
        if not table:
            tables = soup.find_all("table")
            for t in tables:
                rows = t.find_all("tr")
                if len(rows) > 1:  # Has data rows
                    table = t
                    break

        if not table:
            return results

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue

            order_data = {
                "raw_html": str(row),
            }

            # Column name mapping for the order results table
            # The table structure is: Serial | Case Number | Parties | Order Date | Order Link
            column_names = [
                "serial_number",
                "case_number",
                "parties",
                "order_date",
                "document_type",
            ]

            # Extract data from cells with proper field names
            for idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                if text:
                    # Use proper field name if available, otherwise fall back to indexed name
                    if idx < len(column_names):
                        order_data[column_names[idx]] = text
                    else:
                        order_data[f"column_{idx}"] = text

                # Look for links/buttons with PDF info
                link = cell.find("a")
                if link:
                    href = link.get("href", "")
                    onclick = link.get("onclick", "")
                    if href:
                        order_data["pdf_href"] = href
                    if onclick:
                        order_data["onclick"] = onclick

                button = cell.find("button")
                if button:
                    onclick = button.get("onclick", "")
                    if onclick:
                        order_data["onclick"] = onclick

            # Parse parties into petitioner and respondent if possible
            if order_data.get("parties"):
                parties = order_data["parties"]
                # Try different separator patterns: "Vs", "VS", "vs", "V/s", "v/s"
                for separator in ["Vs", "VS", "vs", "V/s", "v/s", " v ", " V "]:
                    if separator in parties:
                        parts = parties.split(separator, 1)
                        if len(parts) == 2:
                            order_data["petitioner"] = parts[0].strip()
                            order_data["respondent"] = parts[1].strip()
                        break

            # Try to extract CNR (16-char format)
            cnr_match = re.search(r"\b([A-Z]{4}\d{12})\b", str(row))
            if cnr_match:
                order_data["cnr"] = cnr_match.group(1)
            # If no CNR, try to use case number as unique identifier
            elif order_data.get("case_number"):
                # Sanitize case number for filename: MVOP/63/2021 -> MVOP_63_2021
                case_no = order_data["case_number"]
                case_id = re.sub(r"[^\w\d.-]", "_", case_no)
                order_data["cnr"] = case_id

            if order_data.get("onclick") or order_data.get("pdf_href"):
                results.append(order_data)

        return results

    def get_case_type_codes(self) -> Dict[str, str]:
        """
        Fetch case type code mapping from fillCaseType API.

        Returns:
            Dictionary mapping short code (e.g., "OS") to internal code (e.g., "17^43")
        """
        url = f"{BASE_URL}?p=casestatus/fillCaseType"

        data = {
            "state_code": self.task.state_code,
            "dist_code": self.task.district_code,
            "court_complex_code": self.task.complex_code,
            "est_code": "",
            "search_type": "c_no",
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry(
                "POST", url, data=data, timeout=30, verify=False
            )
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            case_type_mapping = {}
            casetype_html = result.get("casetype_list", "")

            if casetype_html:
                soup = BeautifulSoup(casetype_html, "lxml")
                for option in soup.find_all("option"):
                    value = option.get("value", "").strip()
                    text = option.get_text(strip=True)

                    if value and text:
                        # Extract short code from text like "OS - ORIGINAL SUIT"
                        short_code = (
                            text.split(" - ")[0].strip() if " - " in text else text
                        )
                        case_type_mapping[short_code] = value

            return case_type_mapping

        except Exception as e:
            logger.debug(f"Error fetching case type codes: {e}")
            return {}

    def search_case_status(
        self, case_type_code: str, case_number: str, year: str
    ) -> List[Dict]:
        """
        Search Case Status by case number to get list of matching cases.

        Args:
            case_type_code: Internal case type code (e.g., "17^43" for OS)
            case_number: Case number (e.g., "32")
            year: Case year (e.g., "2024")

        Returns:
            List of case dictionaries with viewHistory parameters
        """
        url = f"{BASE_URL}?p=casestatus/submitCaseNo"

        # Solve captcha for this request
        captcha_code = self.solve_captcha()

        # API parameters must match actual form submission
        data = {
            "state_code": self.task.state_code,
            "dist_code": self.task.district_code,
            "court_complex_code": self.task.complex_code,
            "est_code": "",
            "case_type": case_type_code,
            "search_case_no": case_number,
            "case_no": case_number,
            "rgyear": year,
            "case_captcha_code": captcha_code,
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry(
                "POST", url, data=data, timeout=60, verify=False
            )
            response.raise_for_status()

            try:
                result = response.json()
                self._update_token(result)

                # Check for captcha error
                if result.get("errormsg"):
                    error_msg = result.get("errormsg", "").lower()
                    if "captcha" in error_msg:
                        logger.debug("Captcha error in case status search, retrying...")
                        return self.search_case_status(
                            case_type_code, case_number, year
                        )
                    logger.debug(f"Case status search error: {result.get('errormsg')}")
                    return []

                # Parse case_data HTML to extract viewHistory parameters
                case_data_html = result.get("case_data", "")
                if not case_data_html:
                    return []

                return self._parse_case_list(case_data_html)

            except (json.JSONDecodeError, ValueError):
                return []

        except Exception as e:
            logger.debug(f"Error in case status search: {e}")
            return []

    def _parse_case_list(self, html: str) -> List[Dict]:
        """
        Parse case list HTML from submitCaseNo to extract viewHistory parameters and party names.

        Extracts from onclick like: viewHistory(201700000322025,'TSRA160001082025',24,'','CScaseNumber',29,9,1290105,'CScaseNumber')
        Also extracts party names from the table row for matching purposes.
        """
        cases = []
        soup = BeautifulSoup(html, "lxml")

        # Find all View links with viewHistory onclick
        for link in soup.find_all("a", onclick=True):
            onclick = link.get("onclick", "")
            if "viewHistory" not in onclick:
                continue

            # Parse viewHistory(case_no, cino, court_code, hideparty, search_flag, state_code, dist_code, complex_code, search_by)
            match = re.search(
                r"viewHistory\s*\(\s*(\d+)\s*,\s*'([^']+)'\s*,\s*(\d+)\s*,\s*'([^']*)'\s*,\s*'([^']+)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'([^']+)'\s*\)",
                onclick,
            )
            if match:
                case_info = {
                    "internal_case_no": match.group(1),
                    "cino": match.group(2),  # CNR number
                    "court_code": match.group(3),
                    "hideparty": match.group(4),
                    "search_flag": match.group(5),
                    "state_code": match.group(6),
                    "dist_code": match.group(7),
                    "court_complex_code": match.group(8),
                    "search_by": match.group(9),
                }

                # Extract party names from the table row
                # Table structure: <tr><td>Sr</td><td>Case</td><td>Parties</td><td><a>View</a></td></tr>
                row = link.find_parent("tr")
                if row:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        # Third cell contains "Petitioner<br>Vs</br>Respondent"
                        parties_cell = cells[2]
                        parties_text = parties_cell.get_text(separator=" ").strip()
                        # Parse "Petitioner Vs Respondent" format (Vs may or may not have surrounding spaces)
                        # Pattern handles: "Name Vs Name", "Name VsName", "NameVs Name", "NameVsName"
                        vs_match = re.search(
                            r"(.+?)\s*vs\s*(.+)", parties_text, flags=re.IGNORECASE
                        )
                        if vs_match:
                            case_info["petitioner"] = vs_match.group(1).strip()
                            case_info["respondent"] = vs_match.group(2).strip()
                        case_info["parties"] = parties_text

                cases.append(case_info)

        return cases

    def view_case_history(self, case_info: Dict) -> Optional[str]:
        """
        Get full case details by calling viewHistory API.

        Args:
            case_info: Dictionary with viewHistory parameters from _parse_case_list

        Returns:
            HTML content with full case details
        """
        url = f"{BASE_URL}?p=home/viewHistory"

        data = {
            "court_code": case_info.get("court_code"),
            "state_code": case_info.get("state_code", self.task.state_code),
            "dist_code": case_info.get("dist_code", self.task.district_code),
            "court_complex_code": case_info.get(
                "court_complex_code", self.task.complex_code
            ),
            "case_no": case_info.get("internal_case_no"),
            "cino": case_info.get("cino"),
            "hideparty": case_info.get("hideparty", ""),
            "search_flag": case_info.get("search_flag", "CScaseNumber"),
            "search_by": case_info.get("search_by", "CScaseNumber"),
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry(
                "POST", url, data=data, timeout=60, verify=False
            )
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            if result.get("errormsg"):
                logger.debug(f"viewHistory error: {result.get('errormsg')}")
                return None

            return result.get("data_list", "")

        except Exception as e:
            logger.debug(f"Error in viewHistory: {e}")
            return None

    def parse_case_details(self, html: str) -> dict:
        """
        Parse detailed case information from Case Status HTML response.

        Extracts:
        - CNR number
        - Case type (full name)
        - Filing number and date
        - Registration number and date
        - First hearing date
        - Next hearing date
        - Case status
        - Case stage
        - Court number and judge
        - Petitioners with advocates
        - Respondents with advocates
        - Acts and sections
        - Case history

        Args:
            html: HTML content from case status search

        Returns:
            Dictionary with all extracted case details
        """
        soup = BeautifulSoup(html, "lxml")
        details = {}

        # Helper to extract text from table cells
        def get_cell_value(label: str) -> Optional[str]:
            """Find a label and return the next cell's value"""
            for td in soup.find_all("td"):
                text = td.get_text(strip=True)
                if label.lower() in text.lower():
                    next_td = td.find_next_sibling("td")
                    if next_td:
                        return next_td.get_text(strip=True)
            return None

        # Extract CNR Number (16 character format: XXXX########YYYY)
        cnr_pattern = r"\b([A-Z]{4}\d{12})\b"
        cnr_match = re.search(cnr_pattern, html)
        if cnr_match:
            details["cnr"] = cnr_match.group(1)

        # Extract from Case Details section
        details["case_type_full"] = get_cell_value("Case Type")
        details["filing_number"] = get_cell_value("Filing Number")
        details["filing_date"] = get_cell_value("Filing Date")
        details["registration_number"] = get_cell_value("Registration Number")
        details["registration_date"] = get_cell_value("Registration Date")

        # Extract from Case Status section
        details["first_hearing_date"] = get_cell_value("First Hearing Date")
        details["next_hearing_date"] = get_cell_value("Next Hearing Date")
        details["case_status"] = get_cell_value("Case Status")
        details["case_stage"] = get_cell_value("Stage of Case")
        details["court_number_and_judge"] = get_cell_value("Court Number and Judge")

        # Alternative field names
        if not details.get("case_stage"):
            details["case_stage"] = get_cell_value("Stage")
        if not details.get("court_number_and_judge"):
            details["court_number_and_judge"] = get_cell_value("Court No")

        # Extract Petitioner and Advocate section
        petitioners = []
        pet_section = soup.find(string=re.compile(r"Petitioner.*Advocate", re.I))
        if pet_section:
            parent = pet_section.find_parent("div") or pet_section.find_parent("table")
            if parent:
                for item in parent.find_all(["li", "tr", "p"]):
                    text = item.get_text(strip=True)
                    if text and "petitioner" not in text.lower():
                        petitioners.append(text)
        if petitioners:
            details["petitioners_with_advocates"] = petitioners

        # Extract Respondent and Advocate section
        respondents = []
        resp_section = soup.find(string=re.compile(r"Respondent.*Advocate", re.I))
        if resp_section:
            parent = resp_section.find_parent("div") or resp_section.find_parent(
                "table"
            )
            if parent:
                for item in parent.find_all(["li", "tr", "p"]):
                    text = item.get_text(strip=True)
                    if text and "respondent" not in text.lower():
                        respondents.append(text)
        if respondents:
            details["respondents_with_advocates"] = respondents

        # Extract Acts section
        acts = []
        acts_section = soup.find(string=re.compile(r"Under Act", re.I))
        if acts_section:
            parent = acts_section.find_parent("table")
            if parent:
                rows = parent.find_all("tr")
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        act = cells[0].get_text(strip=True)
                        section = (
                            cells[1].get_text(strip=True) if len(cells) > 1 else ""
                        )
                        if act:
                            acts.append({"act": act, "section": section})
        if acts:
            details["acts"] = acts

        # Extract Case History
        history = []
        history_section = soup.find(string=re.compile(r"Case History", re.I))
        if history_section:
            parent = history_section.find_parent("table") or history_section.find_next(
                "table"
            )
            if parent:
                rows = parent.find_all("tr")
                headers = []
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if not headers:
                        headers = [c.get_text(strip=True) for c in cells]
                        continue
                    if len(cells) >= 2:
                        entry = {}
                        for i, cell in enumerate(cells):
                            if i < len(headers):
                                entry[headers[i]] = cell.get_text(strip=True)
                        if entry:
                            history.append(entry)
        if history:
            details["case_history"] = history

        # Clean up None values
        return {k: v for k, v in details.items() if v is not None}

    def fetch_case_details(self, order_data: dict) -> dict:
        """
        Fetch detailed case information for an order.

        Parses the case_number (e.g., "OS/32/2024") and searches Case Status API.

        Args:
            order_data: Order data dictionary with case_number field

        Returns:
            Dictionary with detailed case information
        """
        case_number_full = order_data.get("case_number", "")
        if not case_number_full:
            return {}

        # Parse case number: "OS/32/2024" -> case_type="OS", case_no="32", year="2024"
        parts = case_number_full.split("/")
        if len(parts) >= 3:
            case_type = parts[0]
            case_no = parts[1]
            year = parts[2]
        elif len(parts) == 2:
            # Could be "OS/32" with year from order_date
            case_type = parts[0]
            case_no = parts[1]
            order_date = order_data.get("order_date", "")
            if order_date:
                try:
                    year = parse_date_from_api(order_date).year
                except:
                    year = datetime.now().year
            else:
                year = datetime.now().year
            year = str(year)
        else:
            logger.debug(f"Could not parse case number: {case_number_full}")
            return {}

        # Get case type code mapping (cached after first call)
        if not hasattr(self, "_case_type_codes"):
            self._case_type_codes = self.get_case_type_codes()

        # Look up internal case type code (e.g., "OS" -> "17^43")
        case_type_code = self._case_type_codes.get(case_type)
        if not case_type_code:
            logger.debug(f"Unknown case type: {case_type}, cannot fetch details")
            return {}

        # Step 1: Search case status to get list of matching cases
        logger.debug(
            f"Fetching case details for {case_type}/{case_no}/{year} (code: {case_type_code})"
        )
        case_list = self.search_case_status(case_type_code, case_no, year)

        if not case_list:
            logger.debug(f"No cases found for {case_number_full}")
            return {}

        # Step 2: Find the matching case from this court complex
        # Filter by court_code that matches the task's court_numbers
        valid_court_codes = [c.strip() for c in self.task.court_numbers.split(",")]
        matching_cases = [
            c for c in case_list if c.get("court_code") in valid_court_codes
        ]

        # Use court-filtered cases if available, otherwise all cases
        candidates = matching_cases if matching_cases else case_list

        # If multiple candidates, narrow down by matching party names
        if len(candidates) > 1:
            order_petitioner = (order_data.get("petitioner") or "").lower().strip()
            order_respondent = (order_data.get("respondent") or "").lower().strip()
            order_parties = (order_data.get("parties") or "").lower().strip()

            def parties_match(case_info: dict) -> bool:
                """Check if case party names match order party names"""
                case_petitioner = (case_info.get("petitioner") or "").lower().strip()
                case_respondent = (case_info.get("respondent") or "").lower().strip()
                case_parties = (case_info.get("parties") or "").lower().strip()

                # Try exact match on petitioner/respondent
                if order_petitioner and case_petitioner:
                    if (
                        order_petitioner in case_petitioner
                        or case_petitioner in order_petitioner
                    ):
                        if order_respondent and case_respondent:
                            if (
                                order_respondent in case_respondent
                                or case_respondent in order_respondent
                            ):
                                return True
                        elif not order_respondent:
                            return True

                # Try matching on combined parties string
                if order_parties and case_parties:
                    # Normalize "vs" variations
                    order_norm = re.sub(
                        r"\s+",
                        " ",
                        order_parties.replace(" vs ", " ").replace(" v/s ", " "),
                    )
                    case_norm = re.sub(
                        r"\s+",
                        " ",
                        case_parties.replace(" vs ", " ").replace(" v/s ", " "),
                    )
                    if order_norm in case_norm or case_norm in order_norm:
                        return True

                return False

            party_matched = [c for c in candidates if parties_match(c)]
            if len(party_matched) == 1:
                candidates = party_matched
                logger.debug(
                    f"Matched case by party names: {candidates[0].get('cino')}"
                )
            elif len(party_matched) > 1:
                logger.debug(
                    f"Multiple cases ({len(party_matched)}) matched by party names, using first"
                )
                candidates = party_matched
            else:
                logger.debug(
                    f"No party name match found among {len(candidates)} candidates"
                )

        # Select the best candidate
        if not candidates:
            logger.debug(f"No matching cases found for {case_number_full}")
            return {}

        case_info = candidates[0]
        if len(candidates) > 1:
            logger.warning(
                f"Multiple cases ({len(candidates)}) for {case_number_full}, "
                f"using first: CNR={case_info.get('cino')}, court={case_info.get('court_code')}"
            )

        # Fetch case history for selected case
        html = self.view_case_history(case_info)
        if not html:
            logger.debug(
                f"No case history found for {case_number_full} (CNR: {case_info.get('cino')})"
            )
            return {}

        # Parse the HTML
        details = self.parse_case_details(html)

        # Add CNR from case_info if not parsed
        if not details.get("cnr") and case_info.get("cino"):
            details["cnr"] = case_info.get("cino")

        # Store raw HTML for reference
        details["case_details_html"] = html[:10000]  # Limit size

        return details

    def _fetch_with_retry(
        self, method: str, url: str, max_retries: int = 3, **kwargs
    ) -> requests.Response:
        """Fetch URL with retry logic for network errors"""
        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                if method == "GET":
                    return self.session.get(url, **kwargs)
                else:
                    return self.session.post(url, **kwargs)
            except (
                ConnectionError,
                Timeout,
                ChunkedEncodingError,
                urllib3.exceptions.ProtocolError,
                requests.exceptions.RequestException,
            ) as e:
                last_exception = e
                if attempt < max_retries:
                    delay = min(1.0 * (2**attempt) + random.uniform(0, 1), 30.0)
                    logger.warning(
                        f"Network error (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    raise
        raise last_exception

    def download_pdf(self, order_data: dict) -> Optional[bytes]:
        """Download PDF for an order using displayPdf parameters"""
        onclick = order_data.get("onclick", "")

        # Extract displayPdf parameters: displayPdf('normal_v','case_val','court_code','filename','appFlag')
        pattern = r"displayPdf\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*\)"
        match = re.search(pattern, onclick)

        if not match:
            logger.debug(
                f"Could not extract displayPdf parameters from: {onclick[:100]}"
            )
            return None

        normal_v, case_val, court_code, filename, app_flag = match.groups()

        # Call the display_pdf endpoint to get PDF URL
        url = f"{BASE_URL}?p=home/display_pdf"
        data = {
            "normal_v": normal_v,
            "case_val": case_val,
            "court_code": court_code,
            "filename": filename,
            "appFlag": app_flag,
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self._fetch_with_retry(
                "POST", url, data=data, timeout=60, verify=False
            )
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            # Get PDF URL from response (presence of 'order' indicates success)
            pdf_path = result.get("order", "")
            if not pdf_path:
                logger.debug(f"No PDF path in response: {result}")
                return None

            # Make URL absolute
            if not pdf_path.startswith("http"):
                pdf_url = f"{BASE_URL}{pdf_path.lstrip('/')}"
            else:
                pdf_url = pdf_path

            # Download the actual PDF with retry
            pdf_response = self._fetch_with_retry(
                "GET", pdf_url, timeout=120, verify=False
            )
            if pdf_response.status_code == 200 and len(pdf_response.content) > 100:
                # Verify it's a PDF
                if pdf_response.content[:4] == b"%PDF":
                    return pdf_response.content
                else:
                    logger.debug(f"Response is not a PDF: {pdf_response.content[:50]}")
                    return None

        except json.JSONDecodeError:
            logger.debug(f"Non-JSON response from display_pdf: {response.text[:100]}")
        except Exception as e:
            logger.error(f"Error downloading PDF: {e}")

        return None

    def _compress_pdf_bytes(self, pdf_content: bytes) -> bytes:
        """
        Compress PDF content (bytes) using Ghostscript.
        Returns compressed bytes if successful, original bytes otherwise.
        """
        import tempfile

        try:
            # Write to temp file
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_in:
                tmp_in.write(pdf_content)
                tmp_in_path = Path(tmp_in.name)

            original_size = len(pdf_content)

            # Compress the PDF
            compressed_path = compress_pdf_if_enabled(
                tmp_in_path, COMPRESSION_AVAILABLE
            )

            # Read the result
            with open(compressed_path, "rb") as f:
                result_content = f.read()

            compressed_size = len(result_content)

            # Clean up temp file
            if tmp_in_path.exists():
                tmp_in_path.unlink()

            # Log compression result
            if compressed_size < original_size:
                reduction = (1 - compressed_size / original_size) * 100
                logger.debug(
                    f"Compressed PDF: {original_size} -> {compressed_size} bytes ({reduction:.1f}% reduction)"
                )

            return result_content

        except Exception as e:
            logger.debug(f"PDF compression failed: {e}")
            return pdf_content

    def process_order(self, order_data: dict) -> bool:
        """Process a single order - download PDF and save metadata"""
        cnr = order_data.get("cnr", "")

        # Fetch detailed case information if enabled
        case_details = {}
        if self.fetch_case_details_enabled:
            try:
                case_details = self.fetch_case_details(order_data)
                # Update CNR if we got a real one from case details
                if case_details.get("cnr"):
                    cnr = case_details["cnr"]
                    logger.debug(f"Got CNR from case details: {cnr}")
            except Exception as e:
                logger.debug(f"Failed to fetch case details: {e}")

        if not cnr:
            # Generate a unique ID from the raw HTML
            cnr = f"UNKNOWN_{uuid.uuid4().hex[:12]}"

        # Extract year from order date or use task date
        try:
            # Try to parse order date from order_data
            order_date_str = order_data.get("order_date", "")
            if order_date_str:
                order_date = parse_date_from_api(order_date_str)
                year = order_date.year
            else:
                # Fall back to task from_date
                year = parse_date_from_api(self.task.from_date).year
        except (ValueError, KeyError, AttributeError):
            year = datetime.now().year

        # Check if metadata already exists
        metadata_filename = f"{cnr}.json"
        if not self.archive_manager.file_exists(
            year,
            self.task.state_code,
            self.task.district_code,
            self.task.complex_code,
            "metadata",
            metadata_filename,
        ):
            # Save metadata
            metadata = {
                "cnr": cnr,
                "state_code": self.task.state_code,
                "state_name": self.task.state_name,
                "district_code": self.task.district_code,
                "district_name": self.task.district_name,
                "complex_code": self.task.complex_code,
                "complex_name": self.task.complex_name,
                "raw_html": order_data.get("raw_html", ""),
                "scraped_at": datetime.now(IST).isoformat(),
            }

            # Add order data fields (excluding internal fields)
            internal_fields = {"raw_html", "onclick", "pdf_href", "cnr"}
            for key, value in order_data.items():
                if key not in internal_fields and key not in metadata:
                    metadata[key] = value

            # Add detailed case information if available
            if case_details:
                # Exclude the raw HTML from case details to avoid bloat
                internal_case_fields = {"case_details_html"}
                for key, value in case_details.items():
                    if key not in internal_case_fields and key not in metadata:
                        metadata[key] = value

            self.archive_manager.add_to_archive(
                year,
                self.task.state_code,
                self.task.district_code,
                self.task.complex_code,
                "metadata",
                metadata_filename,
                json.dumps(metadata, indent=2, ensure_ascii=False),
            )

        # Check if PDF already exists
        pdf_filename = f"{cnr}.pdf"
        if not self.archive_manager.file_exists(
            year,
            self.task.state_code,
            self.task.district_code,
            self.task.complex_code,
            "orders",
            pdf_filename,
        ):
            # Download PDF
            pdf_content = self.download_pdf(order_data)
            if pdf_content:
                # Compress PDF if enabled
                if self.compress_pdfs:
                    pdf_content = self._compress_pdf_bytes(pdf_content)

                self.archive_manager.add_to_archive(
                    year,
                    self.task.state_code,
                    self.task.district_code,
                    self.task.complex_code,
                    "orders",
                    pdf_filename,
                    pdf_content,
                )
                return True

        return False

    def download(self):
        """Process the task - search and download orders"""
        # Check if task already completed (BEFORE hitting the server)
        if is_task_completed(self.task):
            logger.debug(f"Skipping already completed task: {self.task}")
            return

        try:
            self.init_session()

            # Set court data
            if not self.set_court_data():
                logger.error(f"Failed to set court data for task: {self.task}")
                return

            # Search for orders
            html = self.search_orders()
            if not html:
                logger.debug(f"No results for task: {self.task}")
                # Mark as completed even if no results (so we don't retry)
                save_completed_task(get_task_key(self.task))
                return

            # Parse results
            orders = self.parse_order_results(html)
            if not orders:
                logger.debug(f"No orders found for task: {self.task}")
                # Mark as completed even if no orders
                save_completed_task(get_task_key(self.task))
                return

            logger.info(f"Found {len(orders)} orders for task: {self.task}")

            # Process each order with progress bar
            downloaded = 0
            pbar = tqdm(
                orders,
                desc=f"PDFs ({self.task.complex_name[:20]})",
                leave=False,
                unit="pdf",
            )
            for order in pbar:
                if self.process_order(order):
                    downloaded += 1
                    pbar.set_postfix({"new": downloaded})

            logger.info(
                f"Downloaded {downloaded} new PDFs out of {len(orders)} orders for task: {self.task}"
            )

            # Mark task as completed
            save_completed_task(get_task_key(self.task))

        except Exception as e:
            logger.error(f"Error processing task {self.task}: {e}")
            traceback.print_exc()


def process_task(
    task: DistrictCourtTask,
    archive_manager: S3ArchiveManager,
    compress_pdfs: bool = True,
):
    """Process a single task"""
    try:
        downloader = Downloader(task, archive_manager, compress_pdfs=compress_pdfs)
        downloader.download()
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        traceback.print_exc()


def run(
    courts: List[CourtComplex],
    start_date: str,
    end_date: str,
    day_step: int = 1,
    max_workers: int = 5,
    archive_manager: Optional[S3ArchiveManager] = None,
    compress_pdfs: bool = True,
):
    """Run the downloader for all courts and date ranges"""
    # Create archive manager if not provided
    if archive_manager is None:
        archive_manager = S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            local_only=True,
        )

    # Generate tasks
    tasks = list(generate_tasks(courts, start_date, end_date, day_step))
    logger.info(f"Generated {len(tasks)} tasks")

    # Process tasks
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_task, task, archive_manager, compress_pdfs)
            for task in tasks
        ]

        for i, future in enumerate(
            tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing tasks",
            )
        ):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Task failed: {e}")

    logger.info("All tasks completed")


def main():
    parser = argparse.ArgumentParser(
        description="Download Indian District Court Judgments"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step",
        type=int,
        default=2100,  # Large default to minimize chunks & districts has no pagination & ~5yrs data
        help="Number of days per chunk",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2 to avoid rate limiting)",
    )
    parser.add_argument(
        "--state_code",
        type=str,
        default=None,
        help="Filter by state code",
    )
    parser.add_argument(
        "--district_code",
        type=str,
        default=None,
        help="Filter by district code",
    )
    parser.add_argument(
        "--complex_code",
        type=str,
        default=None,
        help="Filter by complex code",
    )
    parser.add_argument(
        "--courts_csv",
        type=str,
        default="courts.csv",
        help="Path to courts.csv file",
    )
    parser.add_argument(
        "--sync-s3",
        action="store_true",
        default=False,
        help="Sync mode (incremental updates)",
    )
    parser.add_argument(
        "--sync-s3-fill",
        action="store_true",
        default=False,
        help="Gap-filling mode (historical backfill)",
    )
    parser.add_argument(
        "--timeout-hours",
        type=float,
        default=5.5,
        help="Maximum hours to run before graceful exit",
    )
    parser.add_argument(
        "--no-compress",
        action="store_true",
        default=False,
        help="Disable PDF compression (compression is enabled by default)",
    )
    parser.add_argument(
        "--upload-local",
        action="store_true",
        default=False,
        help="Upload existing local TAR files to S3 (no downloading)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would be uploaded without actually uploading (use with --upload-local)",
    )
    args = parser.parse_args()

    # Handle PDF compression settings
    compress_pdfs = not args.no_compress
    if compress_pdfs:
        if COMPRESSION_AVAILABLE:
            logger.info("PDF compression enabled (using Ghostscript)")
        else:
            logger.warning(
                "PDF compression requested but Ghostscript not available - compression disabled"
            )
            compress_pdfs = False
    else:
        logger.info("PDF compression disabled (--no-compress flag)")

    # Load courts
    courts_path = Path(args.courts_csv)
    if not courts_path.exists():
        logger.error(f"Courts file not found: {courts_path}")
        logger.info("Run 'python scrape_courts.py' first to generate courts.csv")
        sys.exit(1)

    courts = load_courts_csv(courts_path)
    logger.info(f"Loaded {len(courts)} court complexes")

    # Apply filters
    if args.state_code:
        courts = [c for c in courts if c.state_code == args.state_code]
    if args.district_code:
        courts = [c for c in courts if c.district_code == args.district_code]
    if args.complex_code:
        courts = [c for c in courts if c.complex_code == args.complex_code]

    if not courts:
        logger.error("No courts match the specified filters")
        sys.exit(1)

    logger.info(f"Processing {len(courts)} court complexes")

    if args.upload_local:
        from upload_local import run_upload_local

        run_upload_local(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            state_code=args.state_code,
            district_code=args.district_code,
            complex_code=args.complex_code,
            dry_run=args.dry_run,
        )
    elif args.sync_s3_fill:
        from sync_s3_fill import sync_s3_fill_gaps

        sync_s3_fill_gaps(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            courts=courts,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            timeout_hours=args.timeout_hours,
            compress_pdfs=compress_pdfs,
        )
    elif args.sync_s3:
        from sync_s3 import run_sync_s3

        run_sync_s3(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            courts=courts,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            compress_pdfs=compress_pdfs,
        )
    else:
        # Default: local download mode
        if not args.start_date:
            logger.error("--start_date is required for local download mode")
            sys.exit(1)

        end_date = args.end_date or datetime.now().strftime("%Y-%m-%d")

        with S3ArchiveManager(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            local_dir=LOCAL_DIR,
            local_only=True,
        ) as archive_manager:
            run(
                courts=courts,
                start_date=args.start_date,
                end_date=end_date,
                day_step=args.day_step,
                max_workers=args.max_workers,
                archive_manager=archive_manager,
                compress_pdfs=compress_pdfs,
            )


if __name__ == "__main__":
    main()
