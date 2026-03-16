"""
Test PDF download for a specific case.

Telangana > Rangareddy > Maheshwaram > CC case type > Disposed > 2024
"""

import json
import re
import time
from urllib.parse import urlparse, parse_qs

from api_client import MobileAPIClient
from crypto import decrypt_url_param


def test_pdf_download():
    """Test downloading a PDF from a specific case."""

    # Disable SSL warnings
    import urllib3
    import os
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Disable proxy - Proxyman is running on 9090
    os.environ['NO_PROXY'] = '*'
    os.environ['no_proxy'] = '*'
    for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
        if key in os.environ:
            del os.environ[key]

    print("="*80)
    print("PDF Download Test")
    print("="*80)

    client = MobileAPIClient(verify_ssl=False)
    # Clear any proxy settings on the session
    client.session.trust_env = False

    # Step 1: Initialize session
    print("\n1. Initializing session...")
    init_result = client.initialize_session()
    print(f"   Init result: {init_result}")
    print(f"   JWT token: {client.jwt_token[:50] if client.jwt_token else 'None'}...")

    # Step 2: Get states and find Telangana (code 29)
    print("\n2. Getting states...")
    states = client.get_states()
    print(f"   Got {len(states)} states")

    if not states:
        print("   ERROR: No states returned. Checking raw request...")
        # Try a direct request
        import requests
        try:
            from crypto import encrypt_data_cbc
            params = {"action_code": "getStates", "time": "1234567", "uid": "test:gov.ecourts.eCourtsServices"}
            encrypted = encrypt_data_cbc(params)
            resp = requests.get(
                "https://app.ecourts.gov.in/ecourt_mobile_DC/stateWebService.php",
                params={"params": encrypted},
                headers={"User-Agent": "eCourtsServices/2.0.1"},
                verify=False,
                timeout=30
            )
            print(f"   Direct request status: {resp.status_code}")
            print(f"   Response length: {len(resp.text)}")
            print(f"   First 100 chars: {resp.text[:100]}")
        except Exception as e:
            print(f"   Direct request error: {e}")
        return

    telangana = next((s for s in states if s.code == 29), None)
    if not telangana:
        print("   ERROR: Telangana not found")
        print("   Available states:")
        for s in states[:5]:
            print(f"     - {s.code}: {s.name}")
        return
    print(f"   Found: {telangana.name} (code={telangana.code})")

    # Step 3: Get districts and find Rangareddy
    print("\n3. Getting districts for Telangana...")
    time.sleep(0.5)
    districts = client.get_districts(29)
    print(f"   Found {len(districts)} districts")

    # Find Rangareddy (try different spellings)
    rangareddy = None
    for d in districts:
        if 'ranga' in d.name.lower() or 'rangareddy' in d.name.lower():
            rangareddy = d
            break

    if not rangareddy:
        print("   Available districts:")
        for d in districts:
            print(f"     - {d.code}: {d.name}")
        return

    print(f"   Found: {rangareddy.name} (code={rangareddy.code})")

    # Step 4: Get court complexes and find Maheshwaram
    print("\n4. Getting court complexes for Rangareddy...")
    time.sleep(0.5)
    complexes = client.get_court_complexes(29, rangareddy.code)
    print(f"   Found {len(complexes)} complexes")

    # Find Maheshwaram
    maheshwaram = None
    for c in complexes:
        if 'mahesh' in c.name.lower():
            maheshwaram = c
            break

    if not maheshwaram:
        print("   Available complexes:")
        for c in complexes[:10]:
            print(f"     - {c.code}: {c.name}")
        print("   ...")
        return

    print(f"   Found: {maheshwaram.name} (code={maheshwaram.code}, njdg={maheshwaram.njdg_est_code})")

    # Step 5: Get case types and find CC
    print("\n5. Getting case types...")
    time.sleep(0.5)
    case_types = client.get_case_types(29, rangareddy.code, maheshwaram.njdg_est_code)
    print(f"   Found {len(case_types)} case types")

    # Find CC case type
    cc_type = None
    for ct in case_types:
        if ct.name.upper().startswith('CC') or 'CC -' in ct.name.upper():
            cc_type = ct
            break

    if not cc_type:
        print("   Available case types (first 10):")
        for ct in case_types[:10]:
            print(f"     - {ct.code}: {ct.name}")
        # Use first case type as fallback
        if case_types:
            cc_type = case_types[0]
            print(f"   Using first type: {cc_type.name}")
        else:
            return
    else:
        print(f"   Found: {cc_type.name} (code={cc_type.code})")

    # Step 6: Search for disposed cases in 2024
    print("\n6. Searching for disposed cases in 2024...")
    time.sleep(0.5)
    cases = client.search_cases_by_type(
        state_code=29,
        dist_code=rangareddy.code,
        court_code=maheshwaram.njdg_est_code,
        case_type=cc_type.code,
        year=2024,
        pending_disposed="D"  # Disposed
    )
    print(f"   Found {len(cases)} cases")

    if not cases:
        print("   No cases found, trying with complex_code...")
        cases = client.search_cases_by_type(
            state_code=29,
            dist_code=rangareddy.code,
            court_code=maheshwaram.code,
            case_type=cc_type.code,
            year=2024,
            pending_disposed="D"
        )
        print(f"   Found {len(cases)} cases with complex_code")

    if not cases:
        print("   ERROR: No disposed cases found")
        return

    # Show first few cases
    for c in cases[:3]:
        print(f"     - {c.case_type}/{c.case_number}/{c.reg_year}: {c.petitioner[:40]}...")

    # Step 7: Get case history for first case
    case = cases[0]
    print(f"\n7. Getting case history for {case.case_no}...")
    time.sleep(0.5)
    history = client.get_case_history(
        state_code=29,
        dist_code=rangareddy.code,
        court_code=case.court_code,
        case_no=case.case_no
    )

    if not history:
        print("   ERROR: Failed to get case history")
        return

    print(f"   Got history with keys: {list(history.keys())}")

    # Step 8: Extract PDF URL from finalOrder
    print("\n8. Extracting PDF URL from orders...")
    final_orders, interim_orders = client.get_orders_from_history(history)

    print(f"   Final orders: {len(final_orders)}")
    print(f"   Interim orders: {len(interim_orders)}")

    if not final_orders and not interim_orders:
        print("   No orders with PDFs found")
        # Check raw HTML
        final_html = history.get("finalOrder", "")
        if final_html:
            print(f"   Final order HTML length: {len(final_html)}")
            print(f"   First 200 chars: {final_html[:200]}")
        return

    # Get first order with PDF
    order = final_orders[0] if final_orders else interim_orders[0]
    print(f"   Order: {order.order_type} on {order.order_date}")
    print(f"   PDF URL: {order.pdf_url[:100] if order.pdf_url else 'None'}...")

    if not order.pdf_url:
        print("   ERROR: No PDF URL found")
        return

    # Step 9: Extract PDF filename from URL
    print("\n9. Extracting PDF filename...")
    try:
        parsed = urlparse(order.pdf_url)
        query = parse_qs(parsed.query)
        params_enc = query.get("params", [""])[0]

        if params_enc:
            params = decrypt_url_param(params_enc)
            print(f"   Decrypted params: {json.dumps(params, indent=2)}")
            filename = params.get("filename", "")
            print(f"   Filename: {filename}")
        else:
            print("   No params in URL")
            return
    except Exception as e:
        print(f"   ERROR: Failed to extract filename: {e}")
        return

    # Step 10: Download PDF
    print("\n10. Downloading PDF...")
    print(f"    Current JWT token: {client.jwt_token[:50] if client.jwt_token else 'None'}...")

    output_path = "/tmp/test_ecourts.pdf"

    success = client.download_pdf_direct(
        state_code=29,
        dist_code=rangareddy.code,
        court_code=case.court_code,
        filename=filename,
        case_no=case.case_no,
        output_path=output_path
    )

    if success:
        print(f"\n   SUCCESS! PDF saved to {output_path}")
        import os
        size = os.path.getsize(output_path)
        print(f"   File size: {size} bytes")
    else:
        print("\n   FAILED to download PDF")
        print("   This likely means the JWT token is not valid.")
        print("   Please provide the token from the API response.")

    # Also try the original URL method
    print("\n11. Trying original URL method...")
    success2 = client.download_pdf(order.pdf_url, "/tmp/test_ecourts2.pdf")
    if success2:
        print("   SUCCESS with original URL!")
    else:
        print("   Failed with original URL too")


if __name__ == "__main__":
    test_pdf_download()
