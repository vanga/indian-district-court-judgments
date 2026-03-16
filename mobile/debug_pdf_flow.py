"""
Debug script to test PDF download flow and token acquisition.

This script helps diagnose the JWT token issue with the mobile API.
It tests various API calls and inspects responses for token data.

Usage:
    python debug_pdf_flow.py [--capture-traffic]
"""

import argparse
import json
import re
import time
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs

import requests

from api_client import MobileAPIClient, BASE_URL, DEFAULT_HEADERS, PACKAGE_NAME
from crypto import encrypt_data_cbc, decrypt_response_cbc, decrypt_url_param


def debug_print(title: str, data: Any, max_len: int = 1000):
    """Print debug info."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)
    if isinstance(data, (dict, list)):
        text = json.dumps(data, indent=2, ensure_ascii=False)
    else:
        text = str(data)
    if len(text) > max_len:
        text = text[:max_len] + f"\n... (truncated, total {len(text)} chars)"
    print(text)


class TokenDebugger:
    """Debug JWT token acquisition for mobile API."""

    def __init__(self, base_url: str = BASE_URL, verbose: bool = True, verify_ssl: bool = False):
        self.base_url = base_url
        self.verbose = verbose
        self.verify_ssl = verify_ssl  # API server uses self-signed certificate
        self.session = requests.Session()
        self.jwt_token = ""
        self.responses = []  # Store all responses for analysis

    def _make_raw_request(
        self,
        endpoint: str,
        params: dict,
        include_auth: bool = True,
        method: str = "GET"
    ) -> tuple[Optional[str], Optional[dict], Optional[Any]]:
        """
        Make a request and return raw response, decrypted response, and any token found.

        Returns:
            (raw_text, decrypted_dict, token_if_found)
        """
        url = f"{self.base_url}/{endpoint}"

        # Add UID to params
        device_uuid = "debug1234567890ab"
        uid = f"{device_uuid}:{PACKAGE_NAME}"
        params_with_uid = {**params, "uid": uid}

        # Encrypt params
        encrypted_params = encrypt_data_cbc(params_with_uid)

        # Build headers
        headers = {
            **DEFAULT_HEADERS,
            "Cookie": "JSESSION=12345678",
        }

        if include_auth:
            encrypted_token = encrypt_data_cbc(self.jwt_token if self.jwt_token else "")
            headers["Authorization"] = f"Bearer {encrypted_token}"

        if self.verbose:
            print(f"\n[REQUEST] {method} {endpoint}")
            print(f"  Params (raw): {json.dumps(params_with_uid)[:200]}")
            print(f"  Auth header: {headers.get('Authorization', 'None')[:60]}...")

        try:
            if method == "GET":
                response = self.session.get(
                    url,
                    params={"params": encrypted_params},
                    headers=headers,
                    timeout=60
                )
            else:
                response = self.session.post(
                    url,
                    data={"params": encrypted_params},
                    headers=headers,
                    timeout=60
                )

            raw_text = response.text.strip()

            if self.verbose:
                print(f"  Status: {response.status_code}")
                print(f"  Response length: {len(raw_text)}")
                print(f"  First 100 chars: {raw_text[:100]}")

            # Try to decrypt
            decrypted = None
            token = None

            if len(raw_text) > 32 and all(c in '0123456789abcdef' for c in raw_text[:32]):
                try:
                    decrypted = decrypt_response_cbc(raw_text)

                    # Check for token
                    if isinstance(decrypted, dict):
                        token = decrypted.get("token")
                        if token:
                            print(f"  [TOKEN FOUND] {token[:50]}...")
                            self.jwt_token = token

                except Exception as e:
                    print(f"  Decrypt error: {e}")

            self.responses.append({
                "endpoint": endpoint,
                "params": params,
                "raw": raw_text,
                "decrypted": decrypted,
                "token": token,
            })

            return raw_text, decrypted, token

        except Exception as e:
            print(f"  Request error: {e}")
            return None, None, None

    def test_app_release(self) -> Optional[dict]:
        """Test appReleaseWebService.php for token."""
        print("\n" + "="*80)
        print(" TESTING: appReleaseWebService.php")
        print("="*80)

        params = {
            "version_code": "17",
            "versionCode": "17",
            "version_name": "2.0.1",
            "os_version": "26.2",
            "mobile_os": "iOS",
            "model_name": "iPhone13,4",
        }

        raw, decrypted, token = self._make_raw_request(
            "appReleaseWebService.php",
            params,
            include_auth=False
        )

        if decrypted:
            debug_print("appReleaseWebService Response", decrypted)

        return decrypted

    def test_states(self) -> Optional[dict]:
        """Test stateWebService.php."""
        print("\n" + "="*80)
        print(" TESTING: stateWebService.php (getStates)")
        print("="*80)

        params = {
            "action_code": "getStates",
            "time": "1234567",
        }

        raw, decrypted, token = self._make_raw_request("stateWebService.php", params)

        if decrypted and self.verbose:
            if "states" in decrypted:
                print(f"  Found {len(decrypted['states'])} states")
                debug_print("First 3 states", decrypted['states'][:3])

        return decrypted

    def test_districts(self, state_code: int = 29) -> Optional[dict]:
        """Test districtWebService.php."""
        print("\n" + "="*80)
        print(f" TESTING: districtWebService.php (state={state_code})")
        print("="*80)

        params = {
            "state_code": str(state_code),
            "test_param": "1",
        }

        raw, decrypted, token = self._make_raw_request("districtWebService.php", params)

        if decrypted and self.verbose:
            districts = decrypted.get("districts") or decrypted.get("district") or []
            print(f"  Found {len(districts)} districts")
            if districts:
                debug_print("First 3 districts", districts[:3])

        return decrypted

    def test_court_complexes(self, state_code: int = 29, dist_code: int = 1) -> Optional[dict]:
        """Test courtEstWebService.php."""
        print("\n" + "="*80)
        print(f" TESTING: courtEstWebService.php (state={state_code}, dist={dist_code})")
        print("="*80)

        params = {
            "action_code": "fillCourtComplex",
            "state_code": str(state_code),
            "dist_code": str(dist_code),
        }

        raw, decrypted, token = self._make_raw_request("courtEstWebService.php", params)

        if decrypted and self.verbose:
            complexes = decrypted.get("courtComplex", [])
            print(f"  Found {len(complexes)} court complexes")
            if complexes:
                debug_print("First 3 complexes", complexes[:3])

        return decrypted

    def test_case_search(
        self,
        state_code: int = 29,
        dist_code: int = 1,
        court_code: str = "1",
        case_type: int = 1,
        year: int = 2023
    ) -> Optional[dict]:
        """Test searchByCaseType.php."""
        print("\n" + "="*80)
        print(f" TESTING: searchByCaseType.php")
        print("="*80)

        params = {
            "state_code": str(state_code),
            "dist_code": str(dist_code),
            "court_code_arr": court_code,
            "case_type": str(case_type),
            "year": str(year),
            "pendingDisposed": "D",
            "language_flag": "english",
            "bilingual_flag": "0",
        }

        raw, decrypted, token = self._make_raw_request("searchByCaseType.php", params)

        if decrypted and self.verbose:
            debug_print("Case search response", decrypted, max_len=2000)

        return decrypted

    def test_case_history(
        self,
        state_code: int,
        dist_code: int,
        court_code: str,
        case_no: str
    ) -> Optional[dict]:
        """Test caseHistoryWebService.php."""
        print("\n" + "="*80)
        print(f" TESTING: caseHistoryWebService.php (case={case_no})")
        print("="*80)

        params = {
            "state_code": str(state_code),
            "dist_code": str(dist_code),
            "court_code": court_code,
            "case_no": case_no,
            "language_flag": "english",
            "bilingual_flag": "0",
        }

        raw, decrypted, token = self._make_raw_request("caseHistoryWebService.php", params)

        if decrypted and self.verbose:
            history = decrypted.get("history", {})

            # Check for orders
            final_order = history.get("finalOrder", "")
            interim_order = history.get("interimOrder", "")

            if final_order and "Order not uploaded" not in final_order:
                print(f"  Has final orders: Yes ({len(final_order)} chars)")

                # Extract PDF URLs
                pdf_urls = re.findall(r'href\s*=\s*[\'"]([^\'"]*display_pdf[^\'"]*)[\'"]', final_order)
                if pdf_urls:
                    print(f"  PDF URLs found: {len(pdf_urls)}")
                    debug_print("First PDF URL", pdf_urls[0])

                    # Decode the params
                    for url in pdf_urls[:1]:
                        try:
                            parsed = urlparse(url)
                            query = parse_qs(parsed.query)
                            params_enc = query.get("params", [""])[0]
                            if params_enc:
                                decoded_params = decrypt_url_param(params_enc)
                                debug_print("Decoded PDF params", decoded_params)
                        except Exception as e:
                            print(f"  Failed to decode params: {e}")

            if interim_order and "Order not uploaded" not in interim_order:
                print(f"  Has interim orders: Yes ({len(interim_order)} chars)")

            debug_print("Case history (keys)", list(history.keys()) if isinstance(history, dict) else history)

        return decrypted

    def test_pdf_download(
        self,
        pdf_url: str,
        output_path: str = "/tmp/test_pdf.pdf"
    ) -> bool:
        """Test PDF download with various auth approaches."""
        print("\n" + "="*80)
        print(" TESTING: PDF Download")
        print("="*80)

        print(f"  URL: {pdf_url[:100]}...")

        # Parse original URL
        parsed = urlparse(pdf_url)
        query = parse_qs(parsed.query)

        original_params = query.get("params", [""])[0]
        original_auth = query.get("authtoken", [""])[0]

        print(f"  Original params: {original_params[:60]}...")
        print(f"  Original authtoken: {original_auth[:60] if original_auth else 'None'}...")

        # Decode original params
        if original_params:
            try:
                decoded_params = decrypt_url_param(original_params)
                debug_print("Decoded params", decoded_params)
            except Exception as e:
                print(f"  Failed to decode params: {e}")
                decoded_params = {}
        else:
            decoded_params = {}

        # Try different approaches
        approaches = [
            ("Original URL as-is", pdf_url, {}),
        ]

        # Build fresh authtoken with our JWT
        if self.jwt_token and decoded_params:
            fresh_params = encrypt_data_cbc(decoded_params)
            fresh_auth = encrypt_data_cbc(f"Bearer {self.jwt_token}")
            fresh_url = f"{self.base_url}/display_pdf.php?params={fresh_params}&authtoken={fresh_auth}"
            approaches.append(("Fresh JWT authtoken", fresh_url, {}))

        # Try with Bearer header instead of authtoken param
        if self.jwt_token and decoded_params:
            encrypted_token = encrypt_data_cbc(self.jwt_token)
            headers = {"Authorization": f"Bearer {encrypted_token}"}
            url_no_auth = f"{self.base_url}/display_pdf.php?params={encrypt_data_cbc(decoded_params)}"
            approaches.append(("JWT in Authorization header", url_no_auth, headers))

        for name, url, extra_headers in approaches:
            print(f"\n  Approach: {name}")
            try:
                headers = {**DEFAULT_HEADERS, "Cookie": "JSESSION=12345678", **extra_headers}

                response = self.session.get(url, headers=headers, timeout=60, stream=True)
                print(f"    Status: {response.status_code}")
                print(f"    Content-Type: {response.headers.get('Content-Type', 'unknown')}")

                content = response.content[:1000]
                print(f"    First bytes: {content[:50]}")

                if content[:4] == b'%PDF':
                    print("    [SUCCESS] Received PDF!")
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    print(f"    Saved to: {output_path}")
                    return True
                elif len(content) > 32:
                    # Try to decrypt error
                    try:
                        text = content.decode('utf-8', errors='ignore').strip()
                        if all(c in '0123456789abcdef' for c in text[:32]):
                            decrypted = decrypt_response_cbc(text)
                            debug_print("Decrypted error response", decrypted)
                    except Exception:
                        pass

            except Exception as e:
                print(f"    Error: {e}")

        return False

    def summarize(self):
        """Print summary of token findings."""
        print("\n" + "="*80)
        print(" TOKEN SUMMARY")
        print("="*80)

        tokens_found = [r for r in self.responses if r.get("token")]
        print(f"Endpoints called: {len(self.responses)}")
        print(f"Tokens received: {len(tokens_found)}")

        if tokens_found:
            print("\nEndpoints that returned tokens:")
            for r in tokens_found:
                print(f"  - {r['endpoint']}: {r['token'][:50]}...")
        else:
            print("\nNo tokens received from any endpoint!")

        if self.jwt_token:
            print(f"\nCurrent JWT token: {self.jwt_token[:50]}...")
        else:
            print("\nNo JWT token acquired.")


def run_full_flow(debugger: TokenDebugger):
    """Run a complete flow to test token acquisition."""

    # Step 1: App release (might give initial token)
    debugger.test_app_release()
    time.sleep(0.5)

    # Step 2: Get states
    debugger.test_states()
    time.sleep(0.5)

    # Step 3: Get districts for state 29 (Telangana)
    districts = debugger.test_districts(state_code=29)
    time.sleep(0.5)

    if not districts:
        print("Failed to get districts")
        return

    # Pick first district
    dist_list = districts.get("districts") or districts.get("district") or []
    if not dist_list:
        print("No districts found")
        return

    dist_code = dist_list[0]["dist_code"]

    # Step 4: Get court complexes
    complexes = debugger.test_court_complexes(state_code=29, dist_code=dist_code)
    time.sleep(0.5)

    if not complexes or not complexes.get("courtComplex"):
        print("Failed to get court complexes")
        return

    # Pick first complex
    complex_data = complexes["courtComplex"][0]
    court_code = complex_data["njdg_est_code"].split(",")[0].strip()

    # Step 5: Search for disposed cases
    client = MobileAPIClient()
    case_types = client.get_case_types(29, dist_code, court_code)

    if case_types:
        for ct in case_types[:3]:
            cases = debugger.test_case_search(
                state_code=29,
                dist_code=dist_code,
                court_code=court_code,
                case_type=ct.code,
                year=2023
            )
            time.sleep(0.5)

            if cases:
                # Find a case with results
                for court_key, court_data in cases.items():
                    if isinstance(court_data, dict) and "caseNos" in court_data and court_data["caseNos"]:
                        case = court_data["caseNos"][0]
                        case_no = case.get("case_no") or case.get("filing_no", "")

                        if case_no:
                            # Step 6: Get case history
                            history = debugger.test_case_history(
                                state_code=29,
                                dist_code=dist_code,
                                court_code=court_code,
                                case_no=case_no
                            )
                            time.sleep(0.5)

                            if history and "history" in history:
                                h = history["history"]
                                final_order = h.get("finalOrder", "")

                                # Extract PDF URL and test download
                                pdf_urls = re.findall(r'href\s*=\s*[\'"]([^\'"]*display_pdf[^\'"]*)[\'"]', final_order)
                                if pdf_urls:
                                    debugger.test_pdf_download(pdf_urls[0])
                                    break
                        break
                break

    debugger.summarize()


def main():
    parser = argparse.ArgumentParser(description="Debug PDF download flow")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--state", type=int, default=29, help="State code (default: 29)")
    parser.add_argument("--test-url", help="Test a specific PDF URL")

    args = parser.parse_args()

    debugger = TokenDebugger(verbose=True)

    if args.test_url:
        debugger.test_pdf_download(args.test_url)
    else:
        run_full_flow(debugger)

    print("\n" + "="*80)
    print(" INSTRUCTIONS FOR TRAFFIC CAPTURE")
    print("="*80)
    print("""
To capture Android traffic with AnyProxy:

1. Install AnyProxy:
   npm install -g anyproxy

2. Generate and install CA certificate:
   anyproxy-ca
   # Install the certificate on your Android device

3. Start AnyProxy:
   anyproxy --rule /path/to/custom_rule.js

4. Configure Android device proxy to point to your computer

5. Open eCourts app and:
   a) Open a disposed case with judgments
   b) Click on the judgment/order PDF link
   c) Capture the full request to display_pdf.php

6. Look for:
   - API calls that return a 'token' field
   - The exact display_pdf.php request URL and headers
   - Any cookies or session IDs used

The JWT token should appear in API responses. If we can identify
which endpoint provides it, we can properly initialize sessions.
""")


if __name__ == "__main__":
    main()
