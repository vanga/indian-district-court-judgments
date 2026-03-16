"""
eCourts Mobile API Client.

Provides access to the mobile app API for fetching court data.
"""

import json
import logging
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

from crypto import encrypt_data_cbc, decrypt_response_cbc, decrypt_url_param, encrypt_server_format, RESPONSE_KEY_HEX
from urllib.parse import urlparse, parse_qs, unquote


# API Configuration
BASE_URL = "https://app.ecourts.gov.in/ecourt_mobile_DC"
PACKAGE_NAME = "in.gov.ecourts.eCourtsServices"  # Android package name
APP_VERSION = "3.0"  # Current app version

# Request headers (Android style - required for token generation)
DEFAULT_HEADERS = {
    "Accept-Charset": "UTF-8",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 14; SM-X205 Build/UP1A.231005.007)",
    "Accept-Encoding": "gzip",
}


@dataclass
class State:
    """State data."""
    code: int
    name: str
    bilingual: bool = False
    hindi_name: str = ""
    national_code: str = ""


@dataclass
class District:
    """District data."""
    code: int
    name: str
    state_code: int


@dataclass
class CourtComplex:
    """Court complex data."""
    code: str
    name: str
    njdg_est_code: str
    state_code: int
    district_code: int


@dataclass
class CaseType:
    """Case type data."""
    code: int
    name: str
    local_name: str = ""


@dataclass
class Case:
    """Case summary data."""
    case_no: str
    cino: str
    case_type: str
    case_number: str
    reg_year: str
    petitioner: str
    court_code: str


@dataclass
class Order:
    """Order/judgment data with PDF link."""
    order_number: int
    order_date: str
    order_type: str  # "Order", "Judgement", etc.
    pdf_url: Optional[str] = None
    is_final: bool = False  # True for final orders, False for interim


class MobileAPIClient:
    """Client for eCourts Mobile API."""

    def __init__(self, base_url: str = BASE_URL, auto_init: bool = False, verify_ssl: bool = False):
        self.base_url = base_url
        self.device_uuid = str(uuid.uuid4()).replace('-', '')[:16]
        self.jwt_token = ""
        self.jsession = f"JSESSION={random.randint(1000000, 99999999)}"
        self.session = requests.Session()
        self.verify_ssl = verify_ssl  # API server uses self-signed certificate
        self._initialized = False

        if auto_init:
            self.initialize_session()

    def _get_uid(self) -> str:
        """Get device UID."""
        return f"{self.device_uuid}:{PACKAGE_NAME}"

    def initialize_session(self) -> bool:
        """
        Initialize session by calling appReleaseWebService.

        This mimics the app's startup flow and provides a JWT token.

        Returns:
            True if initialization succeeded
        """
        if self._initialized:
            return True

        # Simple params format as used by the Android app
        result = self._make_request(
            "appReleaseWebService.php",
            {
                "version": APP_VERSION,
            },
            include_auth=False
        )

        if result:
            self._initialized = True
            # Token is automatically stored by _make_request if present
            return True

        return False

    def set_jwt_token(self, token: str) -> None:
        """
        Set the JWT token directly.

        Useful when the token is captured from traffic analysis.

        Args:
            token: The JWT token string
        """
        self.jwt_token = token
        self._initialized = True

    def get_jwt_token(self) -> str:
        """Get the current JWT token."""
        return self.jwt_token

    def build_pdf_url(
        self,
        filename: str,
        case_no: str,
        court_code: str,
        state_code: int,
        dist_code: int,
    ) -> str:
        """
        Build an authenticated PDF download URL.

        The PDF URL requires:
        1. params: PDF file info encrypted with server format (IV + base64)
        2. authtoken: "Bearer " + encrypted_jwt, encrypted with server format

        The encrypted_jwt is the JWT token encrypted with REQUEST_KEY using
        the standard encrypt_data_cbc format. This is the same value used in
        the Authorization header.

        Args:
            filename: PDF filename (e.g., '/orders/2025/xxx.pdf')
            case_no: Case number
            court_code: Court code
            state_code: State code
            dist_code: District code

        Returns:
            Complete URL with encrypted params and authtoken
        """
        import json

        params = {
            "filename": filename,
            "caseno": case_no,
            "cCode": court_code,
            "appFlag": "1",
            "state_cd": str(state_code),
            "dist_cd": str(dist_code),
            "court_code": court_code,
            "bilingual_flag": "0",
        }

        # Encrypt params using server format (IV + base64) with RESPONSE_KEY
        params_json = json.dumps(params)
        encrypted_params = encrypt_server_format(params_json, RESPONSE_KEY_HEX)

        # Encrypt JWT token with REQUEST_KEY (standard format for Authorization header)
        jwt_token = self.jwt_token if self.jwt_token else ""
        encrypted_jwt = encrypt_data_cbc(jwt_token)

        # Construct auth value: "Bearer " + encrypted_jwt
        auth_value = f"Bearer {encrypted_jwt}"

        # Encrypt the whole auth value using server format with RESPONSE_KEY
        encrypted_auth = encrypt_server_format(auth_value, RESPONSE_KEY_HEX)

        return f"{self.base_url}/display_pdf.php?params={encrypted_params}&authtoken={encrypted_auth}"

    def get_authorization_header(self) -> str:
        """
        Get the Authorization header value for API requests.

        Returns:
            Authorization header value (e.g., "Bearer <encrypted_jwt>")
        """
        jwt_token = self.jwt_token if self.jwt_token else ""
        encrypted_jwt = encrypt_data_cbc(jwt_token)
        return f"Bearer {encrypted_jwt}"

    def _make_request(
        self,
        endpoint: str,
        params: dict,
        include_auth: bool = True,
        retry_count: int = 3
    ) -> Optional[Any]:
        """
        Make an encrypted request to the API.

        Args:
            endpoint: API endpoint
            params: Request parameters
            include_auth: Whether to include Authorization header
            retry_count: Number of retries on failure

        Returns:
            Decrypted response data or None on error
        """
        url = f"{self.base_url}/{endpoint}"

        # Add UID to params
        params_with_uid = {**params, "uid": self._get_uid()}

        # Encrypt params
        encrypted_params = encrypt_data_cbc(params_with_uid)

        # Build headers
        headers = {
            **DEFAULT_HEADERS,
            "Cookie": self.jsession,
        }

        if include_auth:
            encrypted_token = encrypt_data_cbc(self.jwt_token if self.jwt_token else "")
            headers["Authorization"] = f"Bearer {encrypted_token}"

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    url,
                    params={"params": encrypted_params},
                    headers=headers,
                    timeout=60,
                    verify=self.verify_ssl
                )

                if response.status_code != 200:
                    continue

                text = response.text.strip()

                # Try to decrypt
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    try:
                        decrypted = decrypt_response_cbc(text)

                        # Store token if present
                        if isinstance(decrypted, dict) and decrypted.get("token"):
                            self.jwt_token = decrypted["token"]

                        # Check for error status
                        if isinstance(decrypted, dict) and decrypted.get("status") == "N":
                            msg = decrypted.get("Msg") or decrypted.get("msg")
                            if msg == "Not in session !":
                                # Session expired, retry without auth
                                self.jwt_token = ""
                                continue
                            return None

                        return decrypted
                    except Exception:
                        continue

                # Try JSON
                try:
                    return response.json()
                except json.JSONDecodeError:
                    continue

            except requests.RequestException as e:
                logger.warning(f"Request to {endpoint} failed (attempt {attempt + 1}/{retry_count}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        logger.debug(f"All {retry_count} attempts to {endpoint} failed")
        return None

    def get_states(self) -> list[State]:
        """Get list of all states."""
        result = self._make_request(
            "stateWebService.php",
            {"action_code": "getStates", "time": str(random.randint(1000000, 9999999))}
        )

        if not result or "states" not in result:
            return []

        return [
            State(
                code=s["state_code"],
                name=s["state_name"],
                bilingual=s.get("bilingual") == "Y",
                hindi_name=s.get("state_name_hindi", ""),
                national_code=s.get("nationalstate_code", ""),
            )
            for s in result["states"]
        ]

    def get_districts(self, state_code: int) -> list[District]:
        """Get districts for a state."""
        result = self._make_request(
            "districtWebService.php",
            {"state_code": str(state_code), "test_param": "1"}
        )

        if not result:
            return []

        # Key can be "district" or "districts"
        districts_data = result.get("districts") or result.get("district") or []

        return [
            District(
                code=d["dist_code"],
                name=d["dist_name"],
                state_code=state_code,
            )
            for d in districts_data
        ]

    def get_court_complexes(self, state_code: int, dist_code: int) -> list[CourtComplex]:
        """Get court complexes for a district."""
        result = self._make_request(
            "courtEstWebService.php",
            {
                "action_code": "fillCourtComplex",
                "state_code": str(state_code),
                "dist_code": str(dist_code),
            }
        )

        if not result or "courtComplex" not in result:
            return []

        complexes = []
        for c in result["courtComplex"]:
            # njdg_est_code is a comma-separated list of court codes in this complex
            # Keep the full string for use in search queries
            njdg_code = str(c["njdg_est_code"]).strip()
            complexes.append(CourtComplex(
                code=c["complex_code"],
                name=c["court_complex_name"],
                njdg_est_code=njdg_code,
                state_code=state_code,
                district_code=dist_code,
            ))
        return complexes

    def get_case_types(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        language: str = "english"
    ) -> list[CaseType]:
        """Get case types for a court.

        Args:
            court_code: Comma-separated court codes. Only the first one is used
                       for fetching case types (they are shared across the complex).
        """
        # Use only the first court code (case types are shared across the complex)
        first_court_code = court_code.split(",")[0].strip()
        result = self._make_request(
            "caseNumberWebService.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code": first_court_code,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result:
            return []

        # Key can be "caseType" or "case_types"
        case_types_data = result.get("caseType") or result.get("case_types") or []

        case_types = []
        for ct in case_types_data:
            # Handle nested structure: {"case_type": "code~name#code~name#..."}
            if "case_type" in ct and isinstance(ct["case_type"], str):
                # Format: "69~ARBEP - Description#51~A.R.B.O.P - Description#..."
                entries = ct["case_type"].split("#")
                for entry in entries:
                    if "~" in entry:
                        code_part, name_part = entry.split("~", 1)
                        try:
                            code = int(code_part.strip())
                        except ValueError:
                            code = 0
                        case_types.append(CaseType(code=code, name=name_part.strip(), local_name=""))
            else:
                # Handle different key names
                code = ct.get("type_code") or ct.get("case_type_code") or ct.get("code", 0)
                name = ct.get("type_name") or ct.get("case_type_name") or ct.get("name", "")
                local_name = ct.get("ltype_name") or ct.get("local_name", "")
                case_types.append(CaseType(code=code, name=name, local_name=local_name))

        return case_types

    def search_cases_by_type(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_type: int,
        year: int,
        pending_disposed: str = "Disposed",
        language: str = "english"
    ) -> list[Case]:
        """
        Search cases by type and year.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Comma-separated court codes (njdg_est_code from court complex)
            case_type: Case type code
            year: Registration year
            pending_disposed: "Pending" or "Disposed"
            language: Language flag

        Returns:
            List of matching cases
        """
        result = self._make_request(
            "searchByCaseType.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code_arr": court_code,  # Use court_code_arr like the app
                "case_type": str(case_type),
                "year": str(year),
                "pendingDisposed": pending_disposed,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result:
            return []

        # print(f"   DEBUG search result: {json.dumps(result, indent=2)[:500]}")

        # Response is a dict with numeric keys (0, 1, ...) -> court entries
        # Each entry has: court_code, establishment_name, caseNos[]
        cases = []
        if isinstance(result, dict):
            for court_key, court_data in result.items():
                if isinstance(court_data, dict) and "caseNos" in court_data:
                    # Get court_code from the court entry, not individual case
                    entry_court_code = str(court_data.get("court_code", ""))
                    for c in court_data["caseNos"]:
                        cases.append(Case(
                            case_no=c.get("case_no") or c.get("filing_no", ""),
                            cino=c.get("cino", ""),
                            case_type=c.get("type_name", ""),
                            case_number=c.get("case_no2", ""),
                            reg_year=c.get("reg_year", ""),
                            petitioner=c.get("petnameadArr", ""),
                            court_code=entry_court_code,
                        ))

        return cases

    def get_case_history(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_no: str,
        language: str = "english"
    ) -> Optional[dict]:
        """
        Get full case history and details.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Court code
            case_no: Case number
            language: Language flag

        Returns:
            Case history data or None
        """
        result = self._make_request(
            "caseHistoryWebService.php",
            {
                "state_code": str(state_code),
                "dist_code": str(dist_code),
                "court_code": court_code,
                "case_no": case_no,
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )

        if not result or "history" not in result:
            return None

        return result["history"]

    def get_labels(self, language: str = "english") -> Optional[dict]:
        """Get UI labels (useful for understanding data)."""
        result = self._make_request(
            "getAllLabelsWebService.php",
            {
                "language_flag": language,
                "bilingual_flag": "0",
            }
        )
        return result

    @staticmethod
    def extract_orders_from_html(html: str, is_final: bool = True) -> list[Order]:
        """
        Extract order information from HTML response.

        Args:
            html: HTML string from finalOrder or interimOrder field
            is_final: Whether this is from finalOrder (True) or interimOrder (False)

        Returns:
            List of Order objects with PDF URLs
        """
        orders = []
        if not html or "Order not uploaded" in html:
            return orders

        # Find all rows in the order table
        # Pattern: order number, date, and link
        row_pattern = r'<tr><td[^>]*>(?:&nbsp;)*(\d+)</td><td[^>]*>(?:&nbsp;)*([^<]+)</td><td[^>]*>.*?(?:<a[^>]*href\s*=\s*[\'"]([^\'"]+)[\'"][^>]*>.*?<font[^>]*>(?:&nbsp;)*([^<]+)</font>|<span[^>]*color:[^>]*green[^>]*>(?:&nbsp;)*([^<]+)</span>)'

        # Also try simpler pattern for links
        link_pattern = r"<a[^>]*href\s*=\s*['\"]([^'\"]+display_pdf[^'\"]+)['\"][^>]*>.*?<font[^>]*>\s*(?:&nbsp;)*\s*([^<]+?)\s*</font>"

        # Find all PDF links
        links = re.findall(link_pattern, html, re.IGNORECASE | re.DOTALL)

        # Try to parse table rows
        # Simple pattern: look for order number and date in table cells
        cell_pattern = r'<td[^>]*>(?:&nbsp;)*(\d+)</td>\s*<td[^>]*>(?:&nbsp;)*(\d{2}-\d{2}-\d{4})</td>'
        rows = re.findall(cell_pattern, html)

        if links:
            for i, match in enumerate(links):
                url = match[0]
                order_type = (match[1] if len(match) > 1 else "Order").strip()
                order_num = i + 1
                order_date = ""

                # Try to match with row data
                if i < len(rows):
                    order_num = int(rows[i][0])
                    order_date = rows[i][1]

                orders.append(Order(
                    order_number=order_num,
                    order_date=order_date,
                    order_type=order_type,
                    pdf_url=url,
                    is_final=is_final
                ))

        return orders

    def get_orders_from_history(self, history: dict) -> tuple[list[Order], list[Order]]:
        """
        Extract all orders from case history.

        Args:
            history: Case history dictionary

        Returns:
            Tuple of (final_orders, interim_orders)
        """
        final_orders = []
        interim_orders = []

        final_html = history.get("finalOrder", "")
        if final_html:
            final_orders = self.extract_orders_from_html(final_html, is_final=True)

        interim_html = history.get("interimOrder", "")
        if interim_html:
            interim_orders = self.extract_orders_from_html(interim_html, is_final=False)

        return final_orders, interim_orders

    def download_pdf(
        self,
        pdf_url: str,
        output_path: str,
        retry_count: int = 3
    ) -> bool:
        """
        Download a PDF from the given URL.

        The PDF URLs from case history contain encrypted params and authtoken that
        are session-bound and already valid. This method uses them directly without
        re-encrypting.

        Args:
            pdf_url: Full URL to the PDF (containing encrypted params/authtoken)
            output_path: Local path to save the PDF
            retry_count: Number of retries on failure

        Returns:
            True if download succeeded, False otherwise
        """
        # Parse the URL to extract params and authtoken
        parsed = urlparse(pdf_url)
        query_params = parse_qs(parsed.query)

        encrypted_params = query_params.get("params", [""])[0]
        encrypted_auth = query_params.get("authtoken", [""])[0]

        if not encrypted_params or not encrypted_auth:
            return False

        # Build the request URL
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        headers = {
            **DEFAULT_HEADERS,
        }

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    base_url,
                    params={
                        "params": encrypted_params,
                        "authtoken": encrypted_auth,
                    },
                    headers=headers,
                    timeout=120,
                    verify=self.verify_ssl
                )

                if response.status_code == 200:
                    content = response.content

                    # Server may return content with leading whitespace
                    # Check if PDF magic bytes exist in first 20 bytes
                    pdf_start = content.find(b'%PDF')
                    if pdf_start >= 0 and pdf_start < 20:
                        # Strip leading whitespace if present
                        content = content[pdf_start:]

                        # Ensure directory exists
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

                        with open(output_path, "wb") as f:
                            f.write(content)
                        return True
                    else:
                        # Check if it's an encrypted error response
                        try:
                            text = content.decode('utf-8', errors='ignore').strip()
                            if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                                decrypted = decrypt_response_cbc(text)
                                # Got an error response - retry won't help
                                return False
                        except Exception:
                            pass
                        return False

            except requests.RequestException:
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        return False

    def download_pdf_direct(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        filename: str,
        case_no: str,
        output_path: str,
        retry_count: int = 3
    ) -> bool:
        """
        Download a PDF directly using filename and case information.

        This is an alternative method when you have the filename from decrypted params.

        Args:
            state_code: State code
            dist_code: District code
            court_code: Court code
            filename: PDF filename (e.g., '/orders/2025/205400023292025_2.pdf')
            case_no: Case number
            output_path: Local path to save the PDF
            retry_count: Number of retries on failure

        Returns:
            True if download succeeded, False otherwise
        """
        import json as json_module

        # Build params
        params = {
            "filename": filename,
            "caseno": case_no,
            "cCode": court_code,
            "appFlag": "1",
            "state_cd": str(state_code),
            "dist_cd": str(dist_code),
            "court_code": court_code,
            "bilingual_flag": "0",
        }

        # Encrypt params using server format (IV + base64) with RESPONSE_KEY
        params_json = json_module.dumps(params)
        encrypted_params = encrypt_server_format(params_json, RESPONSE_KEY_HEX)

        # Encrypt JWT token with REQUEST_KEY (standard format)
        jwt_token = self.jwt_token if self.jwt_token else ""
        encrypted_jwt = encrypt_data_cbc(jwt_token)

        # Construct and encrypt auth value using server format with RESPONSE_KEY
        auth_value = f"Bearer {encrypted_jwt}"
        encrypted_auth = encrypt_server_format(auth_value, RESPONSE_KEY_HEX)

        url = f"{self.base_url}/display_pdf.php"

        # Include Authorization header with the same encrypted JWT
        headers = {
            **DEFAULT_HEADERS,
            "Cookie": self.jsession,
            "Authorization": f"Bearer {encrypted_jwt}",
        }

        for attempt in range(retry_count):
            try:
                response = self.session.get(
                    url,
                    params={
                        "params": encrypted_params,
                        "authtoken": encrypted_auth,
                    },
                    headers=headers,
                    timeout=120,
                    stream=True,
                    verify=self.verify_ssl
                )

                if response.status_code == 200:
                    first_chunk = next(response.iter_content(chunk_size=4), b'')

                    if first_chunk == b'%PDF':
                        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

                        with open(output_path, "wb") as f:
                            f.write(first_chunk)
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                        return True
                    else:
                        # Try to decrypt error response
                        content = first_chunk + response.content
                        if len(content) > 32:
                            try:
                                text = content.decode('utf-8', errors='ignore').strip()
                                if all(c in '0123456789abcdef' for c in text[:32]):
                                    decrypted = decrypt_response_cbc(text)
                                    # Log the error for debugging
                                    print(f"PDF download error: {decrypted}")
                            except Exception:
                                pass
                        return False

            except requests.RequestException:
                if attempt < retry_count - 1:
                    time.sleep(1)
                continue

        return False


def main():
    """Test the API client."""
    client = MobileAPIClient()

    print("Testing eCourts Mobile API Client")
    print("=" * 60)

    # Get states
    print("\n1. Getting states...")
    states = client.get_states()
    print(f"   Found {len(states)} states")
    for s in states[:5]:
        print(f"   - {s.code}: {s.name}")
    print("   ...")

    if not states:
        print("Failed to get states")
        return

    # Get districts for state 29 (Telangana based on results)
    print("\n2. Getting districts for state 29...")
    districts = client.get_districts(29)
    print(f"   Found {len(districts)} districts")
    for d in districts[:5]:
        print(f"   - {d.code}: {d.name}")
    print("   ...")

    if not districts:
        return

    # Get court complexes for Hyderabad (larger city, more cases)
    district = next((d for d in districts if "hyderabad" in d.name.lower()), districts[0])
    print(f"\n3. Getting court complexes for {district.name}...")
    complexes = client.get_court_complexes(29, district.code)
    print(f"   Found {len(complexes)} court complexes")
    for c in complexes[:3]:
        print(f"   - {c.code}: {c.name} (NJDG: {c.njdg_est_code})")
    print("   ...")

    if not complexes:
        return

    # Get case types for City Small Causes Court (more civil cases)
    complex_ = next((c for c in complexes if "small" in c.name.lower()), complexes[0])
    print(f"\n4. Getting case types for {complex_.name}...")
    case_types = client.get_case_types(29, district.code, complex_.njdg_est_code)
    print(f"   Found {len(case_types)} case types")
    for ct in case_types[:5]:
        print(f"   - {ct.code}: {ct.name}")
    print("   ...")

    if not case_types:
        return

    # Try multiple case types until we find one with data
    print(f"\n5. Searching cases (using complex_code={complex_.code})...")
    cases = []
    for case_type in case_types[:5]:  # Try first 5 case types
        for year in [2023, 2022]:
            # Try both complex_code and njdg_est_code
            for court_id in [complex_.code, complex_.njdg_est_code]:
                print(f"   Trying court={court_id}, type={case_type.code}, year={year}...")
                cases = client.search_cases_by_type(
                    state_code=29,
                    dist_code=district.code,
                    court_code=str(court_id),
                    case_type=case_type.code,
                    year=year,
                    pending_disposed="Disposed"
                )
                if cases:
                    break
            if cases:
                break
        if cases:
            break
    print(f"   Found {len(cases)} cases")
    for c in cases[:3]:
        print(f"   - {c.case_type}/{c.case_number}/{c.reg_year}: {c.petitioner[:50]}...")
    print("   ...")

    if cases:
        # Get case history
        case = cases[0]
        print(f"\n6. Getting case history for {case.case_no}...")
        history = client.get_case_history(
            state_code=29,
            dist_code=district.code,
            court_code=case.court_code,
            case_no=case.case_no
        )
        if history:
            print(f"   Got case history with {len(history)} sections")
            print(f"   Keys: {list(history.keys()) if isinstance(history, dict) else 'list'}")


if __name__ == "__main__":
    main()
