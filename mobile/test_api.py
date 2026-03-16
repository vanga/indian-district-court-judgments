"""
Test eCourts Mobile API endpoints.

The mobile app sends:
1. All request data encrypted via encryptData() as 'params' query parameter
2. Device UID in format deviceUUID:packageName
3. Authorization header with encrypted JWT token (after first request)
"""

import json
import random
import string
import requests
import uuid
from typing import Optional, Any

from crypto import encrypt_data_cbc, decrypt_response_cbc

# API Base URLs
BASE_URL_ENCRYPTED = "https://app.ecourts.gov.in/ecourt_mobile_encrypted_DC"
BASE_URL_PLAIN = "https://app.ecourts.gov.in/ecourt_mobile_DC"

# Package name from the app
PACKAGE_NAME = "gov.ecourts.eCourtsServices"

# Common headers from the captured traffic
BASE_HEADERS = {
    "Accept": "*/*",
    "User-Agent": "eCourtsServices/2.0.1 (iPhone; iOS 26.2; Scale/3.00)",
    "Accept-Language": "en-IN;q=1",
    "Accept-Encoding": "gzip, deflate, br",
}


class MobileAPISession:
    """Session handler for eCourts Mobile API."""

    def __init__(self):
        self.device_uuid = str(uuid.uuid4()).replace('-', '')[:16]
        self.jwt_token = ""
        self.jsession = f"JSESSION={random.randint(1000000, 99999999)}"

    def get_uid(self) -> str:
        """Get device UID in format deviceUUID:packageName."""
        return f"{self.device_uuid}:{PACKAGE_NAME}"

    def make_request(self, endpoint: str, params: dict, base_url: str = BASE_URL_PLAIN) -> Optional[Any]:
        """
        Make a request to the API with proper encryption.

        Args:
            endpoint: API endpoint (e.g., "stateWebService.php")
            params: Request parameters (will be encrypted)
            base_url: Base URL to use

        Returns:
            Decrypted response or None on error
        """
        url = f"{base_url}/{endpoint}"

        # Add UID to params
        params_with_uid = {**params, "uid": self.get_uid()}

        # Encrypt the entire params object
        encrypted_params = encrypt_data_cbc(params_with_uid)

        # Build headers
        headers = {
            **BASE_HEADERS,
            "Cookie": self.jsession,
        }

        # Add Authorization header - always send, even empty (like the app does)
        # When token is empty, encryptData("") still encrypts an empty string
        encrypted_token = encrypt_data_cbc(self.jwt_token if self.jwt_token else "")
        headers["Authorization"] = f"Bearer {encrypted_token}"

        print(f"\n{'='*60}")
        print(f"REQUEST: {endpoint}")
        print(f"URL: {url}")
        print(f"Params (plain): {params_with_uid}")
        print(f"Params (encrypted): {encrypted_params[:50]}...")

        try:
            response = requests.get(
                url,
                params={"params": encrypted_params},
                headers=headers,
                timeout=30
            )
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                text = response.text.strip()

                # Try to parse as JSON first (some endpoints return plain JSON)
                try:
                    data = response.json()
                    print(f"Response (JSON): {json.dumps(data, indent=2)[:300]}...")
                    return data
                except json.JSONDecodeError:
                    pass

                # Try to decrypt
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    try:
                        decrypted = decrypt_response_cbc(text)
                        print(f"Response (decrypted): {json.dumps(decrypted, indent=2)[:500]}...")

                        # Extract and store JWT token if present
                        if isinstance(decrypted, dict) and decrypted.get("token"):
                            self.jwt_token = decrypted["token"]
                            print(f"Got JWT token: {self.jwt_token[:30]}...")

                        return decrypted
                    except Exception as e:
                        print(f"Decryption failed: {e}")

                print(f"Response (raw): {text[:200]}...")
                return {"raw": text}
            else:
                print(f"Error: {response.text[:200]}")
                return None

        except Exception as e:
            print(f"Exception: {e}")
            return None


def test_unencrypted_params():
    """Test with unencrypted params (the way captured traffic showed)."""
    print("\n" + "="*60)
    print("TEST: Unencrypted Parameters (like captured traffic)")
    print("="*60)

    session = MobileAPISession()
    url = f"{BASE_URL_PLAIN}/stateWebService.php"

    # Send params unencrypted like in captured traffic
    params = {
        "action_code": "getStates",
        "time": str(random.randint(1000000, 9999999))
    }

    headers = {
        **BASE_HEADERS,
        "Cookie": session.jsession,
    }

    print(f"URL: {url}")
    print(f"Params: {params}")

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        text = response.text.strip()

        if len(text) > 32:
            try:
                decrypted = decrypt_response_cbc(text)
                print(f"Decrypted: {json.dumps(decrypted, indent=2)[:500]}")
                return decrypted
            except Exception as e:
                print(f"Decryption error: {e}")

        print(f"Raw: {text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

    return None


def test_encrypted_params():
    """Test with encrypted params (as the cordova plugin does)."""
    print("\n" + "="*60)
    print("TEST: Encrypted Parameters (cordova.plugin.http style)")
    print("="*60)

    session = MobileAPISession()

    # Test state API with encrypted params
    result = session.make_request(
        "stateWebService.php",
        {"action_code": "getStates", "time": str(random.randint(1000000, 9999999))}
    )

    return result


def test_app_release_plain():
    """Test app release API which might not need encryption."""
    print("\n" + "="*60)
    print("TEST: App Release (may not need encryption)")
    print("="*60)

    url = f"{BASE_URL_PLAIN}/appReleaseWebService.php"
    params = {"version": "2.0.1"}

    headers = {
        **BASE_HEADERS,
        "Cookie": f"JSESSION={random.randint(1000000, 99999999)}",
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        return response.json() if response.headers.get('Content-Type', '').find('json') >= 0 else response.text
    except Exception as e:
        print(f"Error: {e}")
        return None


def test_full_flow():
    """Test a full API flow with session handling."""
    print("\n" + "="*60)
    print("TEST: Full API Flow with Session")
    print("="*60)

    session = MobileAPISession()

    # Step 0: Try appRelease first (might establish session)
    print("\n--- Step 0: App Release (bootstrap) ---")
    # appReleaseWebService doesn't need Authorization header
    url = f"{BASE_URL_PLAIN}/appReleaseWebService.php"
    headers = {
        **BASE_HEADERS,
        "Cookie": session.jsession,
    }
    # Send uid as encrypted param
    params_data = {"version": "2.0.1", "uid": session.get_uid()}
    encrypted_params = encrypt_data_cbc(params_data)

    try:
        response = requests.get(url, params={"params": encrypted_params}, headers=headers, timeout=30)
        print(f"Status: {response.status_code}")
        text = response.text.strip()
        if len(text) > 32:
            try:
                decrypted = decrypt_response_cbc(text)
                print(f"Decrypted: {json.dumps(decrypted, indent=2)}")
                if isinstance(decrypted, dict) and decrypted.get("token"):
                    session.jwt_token = decrypted["token"]
                    print(f"Got token!")
            except Exception as e:
                print(f"Decrypt error: {e}")
        else:
            print(f"Response: {text}")
    except Exception as e:
        print(f"Error: {e}")

    # 1. Get states
    print("\n--- Step 1: Get States ---")
    states = session.make_request(
        "stateWebService.php",
        {"action_code": "getStates", "time": str(random.randint(1000000, 9999999))}
    )

    if not states or (isinstance(states, dict) and states.get("status") == "N"):
        print("Failed to get states or session issue")

        # Try getting labels first (might establish session)
        print("\n--- Trying Labels API first ---")
        labels = session.make_request(
            "getAllLabelsWebService.php",
            {"language_flag": "english", "bilingual_flag": "0"}
        )

        # Retry states
        print("\n--- Retry: Get States ---")
        states = session.make_request(
            "stateWebService.php",
            {"action_code": "getStates", "time": str(random.randint(1000000, 9999999))}
        )


def main():
    """Run API tests."""
    print("eCourts Mobile API Test Suite")
    print("="*60)

    # Test different approaches
    test_app_release_plain()
    test_unencrypted_params()
    test_encrypted_params()
    test_full_flow()


if __name__ == "__main__":
    main()
