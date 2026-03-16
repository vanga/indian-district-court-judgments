"""
Court Hierarchy Scraper for Indian District Courts
Scrapes the complete State → District → Court Complex hierarchy from services.ecourts.gov.in
and saves it to courts.csv

Usage:
    python scrape_courts.py
    python scrape_courts.py --state 24  # Scrape only Telangana (state code 24)
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path

import colorlog
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.utils.court_utils import CourtComplex, save_courts_csv

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

# Base URL
BASE_URL = "https://services.ecourts.gov.in/ecourtindia_v6/"

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


class CourtHierarchyScraper:
    """Scrapes the court hierarchy from eCourts website"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.app_token = None
        self.courts = []

    def init_session(self) -> str:
        """Initialize session and get initial app_token"""
        logger.info("Initializing session...")

        # Get the court orders page to extract states and initial token
        url = f"{BASE_URL}?p=courtorder/index"
        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        # Extract app_token from URL or page content
        self.app_token = self._extract_app_token(response.text)
        if not self.app_token:
            # Try to get token from URL
            token_match = re.search(r"app_token=([^&'\"]+)", response.url)
            if token_match:
                self.app_token = token_match.group(1)

        if not self.app_token:
            raise ValueError("Could not extract app_token from page")

        logger.info(f"Got initial app_token: {self.app_token[:20]}...")

        return response.text

    def _extract_app_token(self, html: str) -> str | None:
        """Extract app_token from HTML content"""
        # Look for app_token in hidden input
        soup = BeautifulSoup(html, "lxml")
        token_input = soup.find("input", {"name": "app_token"})
        if token_input:
            return token_input.get("value", "")

        # Look for app_token in JavaScript
        pattern = r"app_token['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]"
        match = re.search(pattern, html)
        if match:
            return match.group(1)

        # Look for app_token in URL
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

    def get_states(self, html: str) -> list[tuple[str, str]]:
        """Extract state codes from page HTML"""
        soup = BeautifulSoup(html, "lxml")

        # Look for state select element
        state_select = soup.find("select", {"id": "sess_state_code"})
        if not state_select:
            state_select = soup.find("select", {"name": "state_code"})

        if not state_select:
            logger.error("Could not find state select element")
            return []

        states = []
        for opt in state_select.find_all("option"):
            value = opt.get("value", "").strip()
            text = opt.get_text(strip=True)
            if value and value not in ("", "--Select--", "0"):
                states.append((value, text))

        logger.info(f"Found {len(states)} states")
        return states

    def get_districts(self, state_code: str) -> list[tuple[str, str]]:
        """Get districts for a state"""
        url = f"{BASE_URL}?p=casestatus/fillDistrict"

        data = {
            "state_code": state_code,
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self.session.post(url, data=data, timeout=30)
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            if result.get("status") != 1:
                logger.warning(f"Failed to get districts for state {state_code}")
                return []

            # Parse the HTML options
            dist_list_html = result.get("dist_list", "")
            soup = BeautifulSoup(dist_list_html, "lxml")

            districts = []
            for opt in soup.find_all("option"):
                value = opt.get("value", "").strip()
                text = opt.get_text(strip=True)
                if value and value not in ("", "--Select--", "0"):
                    districts.append((value, text))

            return districts

        except Exception as e:
            logger.error(f"Error getting districts for state {state_code}: {e}")
            return []

    def get_complexes(
        self, state_code: str, district_code: str
    ) -> list[tuple[str, str, str, str]]:
        """Get court complexes for a district"""
        url = f"{BASE_URL}?p=casestatus/fillcomplex"

        data = {
            "state_code": state_code,
            "dist_code": district_code,
            "ajax_req": "true",
            "app_token": self.app_token,
        }

        try:
            response = self.session.post(url, data=data, timeout=30)
            response.raise_for_status()

            result = response.json()
            self._update_token(result)

            if result.get("status") != 1:
                logger.warning(
                    f"Failed to get complexes for state {state_code}, district {district_code}"
                )
                return []

            # Parse the HTML options
            complex_list_html = result.get("complex_list", "")
            soup = BeautifulSoup(complex_list_html, "lxml")

            complexes = []
            for opt in soup.find_all("option"):
                value = opt.get("value", "").strip()
                text = opt.get_text(strip=True)

                if not value or value in ("", "--Select--", "0"):
                    continue

                # Parse the complex code format: {complex_id}@{court_numbers}@{flag}
                # Example: 1100120@10,11,12@N
                parts = value.split("@")
                if len(parts) >= 3:
                    complex_id = parts[0]
                    court_numbers = parts[1]
                    flag = parts[2]
                    complexes.append((complex_id, text, court_numbers, flag))
                elif len(parts) == 2:
                    complex_id = parts[0]
                    court_numbers = parts[1]
                    complexes.append((complex_id, text, court_numbers, "N"))
                else:
                    complexes.append((value, text, "", "N"))
                    logger.warning(f"Unexpected complex format: {value}")

            return complexes

        except Exception as e:
            logger.error(
                f"Error getting complexes for state {state_code}, district {district_code}: {e}"
            )
            return []

    def scrape_all(self, state_filter: str | None = None):
        """Scrape the complete court hierarchy"""
        # Initialize session
        page_html = self.init_session()

        # Get states
        states = self.get_states(page_html)
        if not states:
            logger.error("No states found!")
            return

        # Filter states if specified
        if state_filter:
            states = [(code, name) for code, name in states if code == state_filter]
            if not states:
                logger.error(f"State {state_filter} not found!")
                return
            logger.info(f"Filtering to state: {states[0][1]}")

        total_courts = 0

        # Iterate through states
        for state_code, state_name in tqdm(states, desc="States"):
            logger.info(f"Processing state: {state_name} ({state_code})")

            # Get districts
            districts = self.get_districts(state_code)
            logger.info(f"  Found {len(districts)} districts")

            # Small delay between API calls
            time.sleep(0.5)

            # Iterate through districts
            for district_code, district_name in tqdm(
                districts, desc=f"  {state_name} districts", leave=False
            ):
                # Get court complexes
                complexes = self.get_complexes(state_code, district_code)

                # Add to courts list
                for complex_id, complex_name, court_numbers, flag in complexes:
                    court = CourtComplex(
                        state_code=state_code,
                        state_name=state_name,
                        district_code=district_code,
                        district_name=district_name,
                        complex_code=complex_id,
                        complex_name=complex_name,
                        court_numbers=court_numbers,
                        flag=flag,
                    )
                    self.courts.append(court)
                    total_courts += 1

                # Small delay between API calls
                time.sleep(0.3)

            logger.info(
                f"  Added {len(self.courts) - total_courts + len([c for c in self.courts if c.state_code == state_code])} complexes"
            )

        logger.info(f"Total court complexes found: {total_courts}")

    def save(self, output_path: Path):
        """Save courts to CSV file"""
        if not self.courts:
            logger.warning("No courts to save!")
            return

        save_courts_csv(self.courts, output_path)
        logger.info(f"Saved {len(self.courts)} court complexes to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Indian District Court hierarchy"
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Only scrape a specific state code (e.g., 24 for Telangana)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="courts.csv",
        help="Output CSV file path (default: courts.csv)",
    )
    args = parser.parse_args()

    scraper = CourtHierarchyScraper()
    scraper.scrape_all(state_filter=args.state)
    scraper.save(Path(args.output))


if __name__ == "__main__":
    main()
