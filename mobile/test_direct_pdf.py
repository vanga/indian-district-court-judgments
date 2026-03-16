"""
Test downloading the exact PDF from captured traffic.

From the captured traffic:
- filename: /orders/2024/202100005392024_1.pdf
- caseno: CC/0000539/2024
- cCode: 24
- state_cd: 29
- dist_cd: 9
- court_code: 24
"""

import os
import json
import urllib3
import requests

from crypto import encrypt_data_cbc, encrypt_server_format, decrypt_response_cbc, RESPONSE_KEY_HEX

# Disable SSL warnings and proxy
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'


def test_direct_pdf():
    """Test downloading the exact PDF from captured traffic."""

    # The encrypted JWT from the captured Authorization header
    encrypted_jwt = "ed2b4058d38286624Epm9tbhDFw/hY9U17d3JqAxkUnQR33/Ce+qjdSjRvVXGX38AfMwX1SsfHqVk6G0eZKvUdQaZrCYWkv/wHRoz2Xk9H9njUxiIrWC4tMkdF+6RtPmJBcitF0aKncM2nDyOjwzYn/CXa+EVzjIz1PRvS2rLXyQ/Asvbvi6clxmu1zLMnyjgnLj3y2ffYyyRe18MQL9e4GRR8H8YEm0irFW81nAWvG3yefQXk5DTOF6x3FYV4of4U8srIVRhFeJF8N9lyZjxqnj4nWLnB/T/SgxesYivKMfz9bAdhwwHdsOsJ5JfjqKi5fKgIOorHEMedgDnYRUk/7zyUXpiTrGlFqyJPwl7d9UoZSYqSwGsi4ZbOxpmxAEKdTj//JToyJsTNeadbVFkA9Bz1vI2LFXp17MQpFaoDtN0XkWwoHTYWtf+Rk9THuzixx97yja+pu1NPUeXCadUsvUt7PyjzDiuaF6I+x4La+/IUMRLbaRGlOPZizzrHT/mkw+bU02VwHHbsnmbEiiw5B/qdTH8dNMGKvi8zw=="

    base_url = "https://app.ecourts.gov.in/ecourt_mobile_DC"

    session = requests.Session()
    session.trust_env = False

    headers = {
        "Accept": "*/*",
        "User-Agent": "eCourtsServices/2.0.1 (iPhone; iOS 26.2; Scale/3.00)",
        "Accept-Language": "en-IN;q=1",
        "Cookie": "JSESSION=81500815",
        "Authorization": f"Bearer {encrypted_jwt}",
    }

    print("="*80)
    print("Test Direct PDF Download (from captured traffic)")
    print("="*80)

    # The exact params from the captured successful PDF download
    pdf_params = {
        "filename": "/orders/2024/202100005392024_1.pdf",
        "caseno": "CC/0000539/2024",
        "cCode": "24",
        "appFlag": "1",
        "state_cd": "29",
        "dist_cd": "9",
        "court_code": "24",
        "bilingual_flag": "0",
    }

    print(f"\nPDF params: {json.dumps(pdf_params, indent=2)}")

    # Encrypt params using server format (IV + base64) with RESPONSE_KEY
    params_json = json.dumps(pdf_params)
    encrypted_params = encrypt_server_format(params_json, RESPONSE_KEY_HEX)

    # Construct and encrypt authtoken
    auth_value = f"Bearer {encrypted_jwt}"
    encrypted_auth = encrypt_server_format(auth_value, RESPONSE_KEY_HEX)

    print(f"\nEncrypted params: {encrypted_params[:60]}...")
    print(f"Encrypted authtoken: {encrypted_auth[:60]}...")

    print("\nAttempting PDF download...")

    try:
        resp = session.get(
            f"{base_url}/display_pdf.php",
            params={
                "params": encrypted_params,
                "authtoken": encrypted_auth,
            },
            headers=headers,
            verify=False,
            timeout=120,
        )

        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(resp.content)}")
        print(f"First 50 bytes: {resp.content[:50]}")

        if resp.content[:4] == b'%PDF':
            output_path = "/tmp/ecourts_direct.pdf"
            with open(output_path, "wb") as f:
                f.write(resp.content)
            print(f"\nSUCCESS! PDF saved to {output_path}")
            print(f"File size: {len(resp.content)} bytes")
            return True
        else:
            # Try to decrypt error response
            try:
                text = resp.text.strip()
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    decrypted = decrypt_response_cbc(text)
                    print(f"\nDecrypted error: {decrypted}")
                else:
                    print(f"\nResponse text: {resp.text[:500]}")
            except Exception as e:
                print(f"\nFailed to decrypt: {e}")
                print(f"Response text: {resp.text[:500]}")

    except Exception as e:
        print(f"Error: {e}")

    # Also try with the EXACT captured params and authtoken (replay attack)
    print("\n" + "="*80)
    print("Trying EXACT captured params/authtoken (replay)")
    print("="*80)

    captured_params = "e506cd26fa1d8322de04800e4ae2f652hAJ9Q5ijXuZ9pF2KfaBjPq7Izp7/rlu4kOjIneK+85n3g/Mwn5T3fpvwpHJVNabo874YDqn/8iElOX41loTVRCQRasbCs/OO9E0yackp3NaUAUeR+xxk4Ej8iR4sKHBpJ4rOlXplFukIZxUZYboLcaUV1Ub1Jfhj1bqQWJULiD7NPmuO0ax/1PPUctWbG0xfquFzpR+xssgBt1+HhnZXz0lCoSlYNKVNyqoOQlGX/NFRkgkzJgWq0vxqCHykBsAu"
    captured_authtoken = "a2522d9d8d4211babba89c91438439dedK0nTISTlUyW0ameA+xPQotVBWL67zBNDxcfLVp1XptPZ6rSPq6hD8pb2iKBE/7u4qfuOYlmbc3BW/HIZusn4lE9NgYdAtIRMUXcDEjOuSbGafSJNTPAR43KP6FaCw5JITSEqJxC8/Iwkrf/JlFiuzaH5NhyV9WLlSIY36NgjVsGJayypEfCOoNf1jWcjnbe8FEqXFfo8ashYFSn4OyOL1m9c001l3G7Ruiq5FcY7hiy3ywDYsj6mWcXlE5BiOtZrpd+3Yqvpa8yOM7KW8myCSNIoyP+l6jgdfDvfTc5M5i9Ajtp7tLBPCoM3+8W4zN9LcU9NPqNwcDJWE4wHxl8CrzAaNlY+QLsniVb7GE8KWov9N3OihKZn9AaaTf4cA76Iqe+HqXwldaWWzU4yeVzfgE3D2Vv9dtib8URndL5DXaR1GrvU9UjNGu6Ioqzd4GH4axhtn00EcOc/9vn67u9ehVd81rM87vmDaYkIkHu7N1ItM+cWhUiE4qrlPrGikvcp2wUhSLVpzsP+R6oC4fSBNX0yH1xSaQyGE9NKnSh2zW4fGHPOGibX66KcNBRtX1xENlfyWF7dg3DjkXJUCq0xr9o5MeAiLi++J3qf5DDvogu5zemrk+NCVO/rN9V9+XRoyYkrOEinxIjEArmsvdOlvhazFRYQMbIoXk+6fZPfT3yoK66GfTmgYvnYiSSqcWkdaqMmbL+9rRNxf7LyFJ2iSxxiCf4+xt5nQucF9dDGfS2NqC8uNujH8XHF6sxfSp3"

    try:
        resp = session.get(
            f"{base_url}/display_pdf.php",
            params={
                "params": captured_params,
                "authtoken": captured_authtoken,
            },
            headers=headers,
            verify=False,
            timeout=120,
        )

        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(resp.content)}")
        print(f"First 50 bytes: {resp.content[:50]}")

        if resp.content[:4] == b'%PDF':
            output_path = "/tmp/ecourts_replay.pdf"
            with open(output_path, "wb") as f:
                f.write(resp.content)
            print(f"\nSUCCESS! PDF saved to {output_path}")
            return True
        else:
            try:
                text = resp.text.strip()
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    decrypted = decrypt_response_cbc(text)
                    print(f"\nDecrypted error: {decrypted}")
                else:
                    print(f"\nResponse: {resp.text[:300]}")
            except Exception:
                print(f"\nResponse: {resp.text[:300]}")

    except Exception as e:
        print(f"Error: {e}")

    return False


if __name__ == "__main__":
    test_direct_pdf()
