"""
Test PDF download using tokens from captured traffic.
"""

import os
import json
import time
import urllib3
from urllib.parse import urlparse, parse_qs

import requests

from crypto import encrypt_data_cbc, encrypt_server_format, decrypt_response_cbc, decrypt_url_param, RESPONSE_KEY_HEX, REQUEST_KEY_HEX

# Disable SSL warnings and proxy
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'


def test_with_captured_token():
    """Test using the captured encrypted JWT from traffic."""

    # The encrypted JWT from the Authorization header (captured from caseHistoryWebService)
    # This is: encrypt_data_cbc(raw_jwt_token, REQUEST_KEY)
    encrypted_jwt = "ed2b4058d38286624Epm9tbhDFw/hY9U17d3JqAxkUnQR33/Ce+qjdSjRvVXGX38AfMwX1SsfHqVk6G0eZKvUdQaZrCYWkv/wHRoz2Xk9H9njUxiIrWC4tMkdF+6RtPmJBcitF0aKncM2nDyOjwzYn/CXa+EVzjIz1PRvS2rLXyQ/Asvbvi6clxmu1zLMnyjgnLj3y2ffYyyRe18MQL9e4GRR8H8YEm0irFW81nAWvG3yefQXk5DTOF6x3FYV4of4U8srIVRhFeJF8N9lyZjxqnj4nWLnB/T/SgxesYivKMfz9bAdhwwHdsOsJ5JfjqKi5fKgIOorHEMedgDnYRUk/7zyUXpiTrGlFqyJPwl7d9UoZSYqSwGsi4ZbOxpmxAEKdTj//JToyJsTNeadbVFkA9Bz1vI2LFXp17MQpFaoDtN0XkWwoHTYWtf+Rk9THuzixx97yja+pu1NPUeXCadUsvUt7PyjzDiuaF6I+x4La+/IUMRLbaRGlOPZizzrHT/mkw+bU02VwHHbsnmbEiiw5B/qdTH8dNMGKvi8zw=="

    base_url = "https://app.ecourts.gov.in/ecourt_mobile_DC"

    session = requests.Session()
    session.trust_env = False  # Disable proxy

    print("="*80)
    print("Test with Captured Token")
    print("="*80)

    # First, let's search for cases in Telangana > Rangareddy > Maheshwaram
    # State 29, need to find district and complex

    headers = {
        "Accept": "*/*",
        "User-Agent": "eCourtsServices/2.0.1 (iPhone; iOS 26.2; Scale/3.00)",
        "Accept-Language": "en-IN;q=1",
        "Cookie": "JSESSION=81500815",
        "Authorization": f"Bearer {encrypted_jwt}",
    }

    # Step 1: Get districts for Telangana (state 29)
    print("\n1. Getting districts for Telangana...")
    params = {"state_code": "29", "test_param": "1", "uid": "test:gov.ecourts.eCourtsServices"}
    encrypted_params = encrypt_data_cbc(params)

    try:
        resp = session.get(
            f"{base_url}/districtWebService.php",
            params={"params": encrypted_params},
            headers=headers,
            verify=False,
            timeout=30
        )
        print(f"   Status: {resp.status_code}")

        if resp.status_code == 200:
            decrypted = decrypt_response_cbc(resp.text.strip())
            districts = decrypted.get("districts") or decrypted.get("district") or []
            print(f"   Found {len(districts)} districts")

            # Find Rangareddy
            rangareddy = None
            for d in districts:
                if 'ranga' in d.get('dist_name', '').lower():
                    rangareddy = d
                    break

            if rangareddy:
                print(f"   Rangareddy: code={rangareddy['dist_code']}")
            else:
                print("   Rangareddy not found")
                for d in districts[:5]:
                    print(f"     - {d['dist_code']}: {d['dist_name']}")
                return
    except Exception as e:
        print(f"   Error: {e}")
        return

    dist_code = rangareddy['dist_code']
    time.sleep(0.5)

    # Step 2: Get court complexes for Rangareddy
    print("\n2. Getting court complexes...")
    params = {
        "action_code": "fillCourtComplex",
        "state_code": "29",
        "dist_code": str(dist_code),
        "uid": "test:gov.ecourts.eCourtsServices"
    }
    encrypted_params = encrypt_data_cbc(params)

    try:
        resp = session.get(
            f"{base_url}/courtEstWebService.php",
            params={"params": encrypted_params},
            headers=headers,
            verify=False,
            timeout=30
        )

        if resp.status_code == 200:
            decrypted = decrypt_response_cbc(resp.text.strip())
            complexes = decrypted.get("courtComplex", [])
            print(f"   Found {len(complexes)} complexes")

            # Find Maheshwaram
            maheshwaram = None
            for c in complexes:
                if 'mahesh' in c.get('court_complex_name', '').lower():
                    maheshwaram = c
                    break

            if maheshwaram:
                print(f"   Maheshwaram: code={maheshwaram['complex_code']}, njdg={maheshwaram['njdg_est_code']}")
            else:
                print("   Maheshwaram not found")
                for c in complexes[:10]:
                    print(f"     - {c['complex_code']}: {c['court_complex_name']}")
                return
    except Exception as e:
        print(f"   Error: {e}")
        return

    complex_code = maheshwaram['complex_code']
    # njdg_est_code might be comma-separated, try all values
    njdg_codes = [x.strip() for x in str(maheshwaram['njdg_est_code']).split(",")]
    print(f"   NJDG codes: {njdg_codes}")
    njdg_code = njdg_codes[-1] if len(njdg_codes) > 1 else njdg_codes[0]  # Try last one (24)
    print(f"   Using NJDG code: {njdg_code}")
    time.sleep(0.5)

    # Step 3: Get case types
    print("\n3. Getting case types...")
    params = {
        "state_code": "29",
        "dist_code": str(dist_code),
        "court_code": njdg_code,
        "language_flag": "english",
        "bilingual_flag": "0",
        "uid": "test:gov.ecourts.eCourtsServices"
    }
    encrypted_params = encrypt_data_cbc(params)

    try:
        resp = session.get(
            f"{base_url}/caseNumberWebService.php",
            params={"params": encrypted_params},
            headers=headers,
            verify=False,
            timeout=30
        )

        if resp.status_code == 200:
            decrypted = decrypt_response_cbc(resp.text.strip())
            case_types = decrypted.get("caseType") or decrypted.get("case_types") or []

            # Parse case types
            cc_code = None
            for ct in case_types:
                if "case_type" in ct and isinstance(ct["case_type"], str):
                    entries = ct["case_type"].split("#")
                    for entry in entries:
                        if "~" in entry:
                            code, name = entry.split("~", 1)
                            if name.strip().upper().startswith("CC"):
                                cc_code = int(code.strip())
                                print(f"   Found CC: code={cc_code}, name={name.strip()}")
                                break
                if cc_code:
                    break

            if not cc_code:
                print("   CC case type not found")
                return
    except Exception as e:
        print(f"   Error: {e}")
        return

    time.sleep(0.5)

    # Step 4: Search for disposed cases in 2024
    print("\n4. Searching for disposed CC cases in 2024...")
    params = {
        "state_code": "29",
        "dist_code": str(dist_code),
        "court_code_arr": njdg_code,
        "case_type": str(cc_code),
        "year": "2024",
        "pendingDisposed": "D",
        "language_flag": "english",
        "bilingual_flag": "0",
        "uid": "test:gov.ecourts.eCourtsServices"
    }
    encrypted_params = encrypt_data_cbc(params)

    try:
        resp = session.get(
            f"{base_url}/searchByCaseType.php",
            params={"params": encrypted_params},
            headers=headers,
            verify=False,
            timeout=60
        )

        print(f"   Status: {resp.status_code}")
        print(f"   Response length: {len(resp.text)}")
        print(f"   First 100 chars: {resp.text[:100]}")

        if resp.status_code == 200:
            text = resp.text.strip()
            # Check if it's encrypted
            if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                decrypted = decrypt_response_cbc(text)
            else:
                # Might be plain JSON
                decrypted = json.loads(text)

            # Find cases
            cases = []
            if isinstance(decrypted, dict):
                for court_key, court_data in decrypted.items():
                    if isinstance(court_data, dict) and "caseNos" in court_data:
                        for c in court_data["caseNos"]:
                            cases.append(c)

            print(f"   Found {len(cases)} cases")

            if not cases:
                print("   No cases found")
                return

            # Get first case
            case = cases[0]
            case_no = case.get("case_no") or case.get("filing_no", "")
            print(f"   First case: {case_no}")
            print(f"   Petitioner: {case.get('petnameadArr', '')[:50]}")
    except Exception as e:
        print(f"   Error: {e}")
        return

    court_code = str(case.get("court_code", njdg_code))
    time.sleep(0.5)

    # Step 5: Get case history
    print("\n5. Getting case history...")
    params = {
        "state_code": "29",
        "dist_code": str(dist_code),
        "court_code": court_code,
        "case_no": case_no,
        "language_flag": "english",
        "bilingual_flag": "0",
        "uid": "test:gov.ecourts.eCourtsServices"
    }
    encrypted_params = encrypt_data_cbc(params)

    try:
        resp = session.get(
            f"{base_url}/caseHistoryWebService.php",
            params={"params": encrypted_params},
            headers=headers,
            verify=False,
            timeout=30
        )

        if resp.status_code == 200:
            decrypted = decrypt_response_cbc(resp.text.strip())
            history = decrypted.get("history", {})
            print(f"   Got history with keys: {list(history.keys())[:10]}")

            # Check for token in response
            if decrypted.get("token"):
                print(f"   NEW TOKEN FOUND: {decrypted['token'][:50]}...")

            # Get final order HTML
            final_html = history.get("finalOrder", "")
            if final_html and "Order not uploaded" not in final_html:
                print(f"   Has final orders: Yes ({len(final_html)} chars)")

                # Extract PDF URL
                import re
                pdf_urls = re.findall(r'href\s*=\s*[\'"]([^\'"]*display_pdf[^\'"]*)[\'"]', final_html)
                if pdf_urls:
                    print(f"   Found {len(pdf_urls)} PDF URLs")
                    pdf_url = pdf_urls[0]
                else:
                    print("   No PDF URLs found")
                    print(f"   HTML sample: {final_html[:300]}")
                    return
            else:
                print("   No final orders with PDFs")
                return
    except Exception as e:
        print(f"   Error: {e}")
        return

    # Step 6: Extract PDF filename
    print("\n6. Extracting PDF filename...")
    try:
        parsed = urlparse(pdf_url)
        query = parse_qs(parsed.query)
        params_enc = query.get("params", [""])[0]

        if params_enc:
            pdf_params = decrypt_url_param(params_enc)
            print(f"   Params: {json.dumps(pdf_params, indent=2)}")
            filename = pdf_params.get("filename", "")
        else:
            print("   No params in URL")
            return
    except Exception as e:
        print(f"   Error: {e}")
        return

    # Step 7: Download PDF
    print("\n7. Downloading PDF...")

    # Build params for PDF request
    pdf_request_params = {
        "filename": filename,
        "caseno": case_no,
        "cCode": court_code,
        "appFlag": "1",
        "state_cd": "29",
        "dist_cd": str(dist_code),
        "court_code": court_code,
        "bilingual_flag": "0",
    }

    # Encrypt params using server format
    params_json = json.dumps(pdf_request_params)
    encrypted_pdf_params = encrypt_server_format(params_json, RESPONSE_KEY_HEX)

    # Construct and encrypt authtoken
    auth_value = f"Bearer {encrypted_jwt}"
    encrypted_auth = encrypt_server_format(auth_value, RESPONSE_KEY_HEX)

    print(f"   Encrypted params: {encrypted_pdf_params[:60]}...")
    print(f"   Encrypted auth: {encrypted_auth[:60]}...")

    try:
        resp = session.get(
            f"{base_url}/display_pdf.php",
            params={
                "params": encrypted_pdf_params,
                "authtoken": encrypted_auth,
            },
            headers=headers,
            verify=False,
            timeout=120,
            stream=True
        )

        print(f"   Status: {resp.status_code}")
        print(f"   Content-Type: {resp.headers.get('Content-Type', 'unknown')}")

        content = resp.content
        print(f"   Content length: {len(content)}")
        print(f"   First bytes: {content[:20]}")

        if content[:4] == b'%PDF':
            output_path = "/tmp/ecourts_test.pdf"
            with open(output_path, "wb") as f:
                f.write(content)
            print(f"\n   SUCCESS! PDF saved to {output_path}")
            print(f"   File size: {len(content)} bytes")
        else:
            # Try to decrypt error
            try:
                text = content.decode('utf-8', errors='ignore').strip()
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    decrypted = decrypt_response_cbc(text)
                    print(f"   Error response: {decrypted}")
            except Exception:
                print(f"   Response text: {content[:200]}")

    except Exception as e:
        print(f"   Error: {e}")


if __name__ == "__main__":
    test_with_captured_token()
