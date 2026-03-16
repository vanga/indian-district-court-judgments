"""
Analyze captured traffic data from Proxyman.

This script decrypts the captured params and authtoken from the PDF request
to understand the structure needed for PDF downloads.
"""

import json
from urllib.parse import unquote, parse_qs, urlparse

from crypto import decrypt_response_cbc, decrypt_url_param, RESPONSE_KEY_HEX, REQUEST_KEY_HEX


def analyze_captured_pdf_url():
    """Analyze the captured display_pdf.php URL."""
    # The captured URL from the traffic (URL-encoded)
    captured_url = "https://app.ecourts.gov.in/ecourt_mobile_DC/display_pdf.php?params=e506cd26fa1d8322de04800e4ae2f652hAJ9Q5ijXuZ9pF2KfaBjPq7Izp7%2Frlu4kOjIneK%2B85n3g%2FMwn5T3fpvwpHJVNabo874YDqn%2F8iElOX41loTVRCQRasbCs%2FOO9E0yackp3NaUAUeR%2Bxxk4Ej8iR4sKHBpJ4rOlXplFukIZxUZYboLcaUV1Ub1Jfhj1bqQWJULiD7NPmuO0ax%2F1PPUctWbG0xfquFzpR%2BxssgBt1%2BHhnZXz0lCoSlYNKVNyqoOQlGX%2FNFRkgkzJgWq0vxqCHykBsAu&authtoken=a2522d9d8d4211babba89c91438439dedK0nTISTlUyW0ameA%2BxPQotVBWL67zBNDxcfLVp1XptPZ6rSPq6hD8pb2iKBE%2F7u4qfuOYlmbc3BW%2FHIZusn4lE9NgYdAtIRMUXcDEjOuSbGafSJNTPAR43KP6FaCw5JITSEqJxC8%2FIwkrf%2FJlFiuzaH5NhyV9WLlSIY36NgjVsGJayypEfCOoNf1jWcjnbe8FEqXFfo8ashYFSn4OyOL1m9c001l3G7Ruiq5FcY7hiy3ywDYsj6mWcXlE5BiOtZrpd%2B3Yqvpa8yOM7KW8myCSNIoyP%2Bl6jgdfDvfTc5M5i9Ajtp7tLBPCoM3%2B8W4zN9LcU9NPqNwcDJWE4wHxl8CrzAaNlY%2BQLsniVb7GE8KWov9N3OihKZn9AaaTf4cA76Iqe%2BHqXwldaWWzU4yeVzfgE3D2Vv9dtib8URndL5DXaR1GrvU9UjNGu6Ioqzd4GH4axhtn00EcOc%2F9vn67u9ehVd81rM87vmDaYkIkHu7N1ItM%2BcWhUiE4qrlPrGikvcp2wUhSLVpzsP%2BR6oC4fSBNX0yH1xSaQyGE9NKnSh2zW4fGHPOGibX66KcNBRtX1xENlfyWF7dg3DjkXJUCq0xr9o5MeAiLi%2B%2BJ3qf5DDvogu5zemrk%2BNCVO%2FrN9V9%2BXRoyYkrOEinxIjEArmsvdOlvhazFRYQMbIoXk%2B6fZPfT3yoK66GfTmgYvnYiSSqcWkdaqMmbL%2B9rRNxf7LyFJ2iSxxiCf4%2Bxt5nQucF9dDGfS2NqC8uNujH8XHF6sxfSp3"

    print("="*80)
    print("Analyzing captured PDF download URL")
    print("="*80)

    # Parse URL
    parsed = urlparse(captured_url)
    query = parse_qs(parsed.query)

    params_enc = query.get("params", [""])[0]
    authtoken_enc = query.get("authtoken", [""])[0]

    print(f"\nParams (encoded): {params_enc[:80]}...")
    print(f"Authtoken (encoded): {authtoken_enc[:80]}...")

    # URL decode
    params_decoded = unquote(params_enc)
    authtoken_decoded = unquote(authtoken_enc)

    print(f"\nParams (decoded): {params_decoded[:80]}...")
    print(f"Authtoken (decoded): {authtoken_decoded[:80]}...")

    # Try to decrypt params with response key (server-generated)
    print("\n" + "-"*60)
    print("Decrypting params:")
    print("-"*60)

    try:
        decrypted_params = decrypt_response_cbc(params_decoded)
        print(f"SUCCESS with RESPONSE_KEY:")
        print(json.dumps(decrypted_params, indent=2))
    except Exception as e:
        print(f"Failed with RESPONSE_KEY: {e}")

        # Try with request key
        try:
            decrypted_params = decrypt_response_cbc(params_decoded, REQUEST_KEY_HEX)
            print(f"SUCCESS with REQUEST_KEY:")
            print(json.dumps(decrypted_params, indent=2))
        except Exception as e2:
            print(f"Failed with REQUEST_KEY: {e2}")

    # Try to decrypt authtoken with response key
    print("\n" + "-"*60)
    print("Decrypting authtoken:")
    print("-"*60)

    decrypted_auth = None
    try:
        decrypted_auth = decrypt_response_cbc(authtoken_decoded)
        print(f"SUCCESS with RESPONSE_KEY:")
        if isinstance(decrypted_auth, str):
            print(f"  {decrypted_auth}")
        else:
            print(json.dumps(decrypted_auth, indent=2))
    except Exception as e:
        print(f"Failed with RESPONSE_KEY: {e}")

        # Try with request key (since app encrypts the authtoken)
        try:
            decrypted_auth = decrypt_response_cbc(authtoken_decoded, REQUEST_KEY_HEX)
            print(f"SUCCESS with REQUEST_KEY:")
            if isinstance(decrypted_auth, str):
                print(f"  {decrypted_auth}")
            else:
                print(json.dumps(decrypted_auth, indent=2))
        except Exception as e2:
            print(f"Failed with REQUEST_KEY: {e2}")

    # Now try to decrypt the inner JWT token
    if decrypted_auth and isinstance(decrypted_auth, str) and decrypted_auth.startswith("Bearer "):
        inner_token = decrypted_auth[7:]  # Remove "Bearer "
        print("\n" + "-"*60)
        print("Decrypting inner JWT token:")
        print("-"*60)
        print(f"Inner token: {inner_token[:80]}...")

        # The inner token should be encrypted with REQUEST_KEY (app encrypts it)
        try:
            inner_decrypted = decrypt_response_cbc(inner_token, REQUEST_KEY_HEX)
            print(f"SUCCESS with REQUEST_KEY (app encryption):")
            if isinstance(inner_decrypted, str):
                print(f"  JWT: {inner_decrypted}")
            else:
                print(json.dumps(inner_decrypted, indent=2))
        except Exception as e:
            print(f"Failed with REQUEST_KEY: {e}")

            # Try with response key
            try:
                inner_decrypted = decrypt_response_cbc(inner_token)
                print(f"SUCCESS with RESPONSE_KEY:")
                if isinstance(inner_decrypted, str):
                    print(f"  JWT: {inner_decrypted}")
                else:
                    print(json.dumps(inner_decrypted, indent=2))
            except Exception as e2:
                print(f"Failed with RESPONSE_KEY: {e2}")


def analyze_case_history_url():
    """Analyze the captured caseHistoryWebService URL."""
    # The captured URL
    captured_url = "https://app.ecourts.gov.in/ecourt_mobile_DC/caseHistoryWebService.php?params=c51284740281448b4B0ONals4Sxsorsvbpl7U6RTw8rs9Ce%2FHakrjL3AnApOdJ2utHdZTHPe649bMu0X8zz%2FO2ezKnadSE0vkhlPuGRtK%2ByhQ2X9jkcAHjl8byrV42r0muJiLrChi0tGu1r8Tcnrnk8qat6tFPUi6dU9EldvnxOKYhGiDZt4FUrnH0tSuQmg0dLG%2F3GwPFjfwuJfK"

    print("\n" + "="*80)
    print("Analyzing captured caseHistoryWebService URL")
    print("="*80)

    parsed = urlparse(captured_url)
    query = parse_qs(parsed.query)

    params_enc = query.get("params", [""])[0]
    params_decoded = unquote(params_enc)

    print(f"\nParams (decoded): {params_decoded[:80]}...")

    # Try to decrypt - the format is: 16-char random IV + 1-digit index + base64
    # But the captured params don't follow this exact format
    # Let me try different approaches

    # Try direct approach
    try:
        decrypted = decrypt_response_cbc(params_decoded, REQUEST_KEY_HEX)
        print(f"Decrypted params (REQUEST_KEY):")
        print(json.dumps(decrypted, indent=2))
    except Exception as e:
        print(f"Failed with REQUEST_KEY: {e}")

        # Try with response key
        try:
            decrypted = decrypt_response_cbc(params_decoded)
            print(f"Decrypted params (RESPONSE_KEY):")
            print(json.dumps(decrypted, indent=2))
        except Exception as e2:
            print(f"Failed with RESPONSE_KEY: {e2}")


def analyze_app_release_url():
    """Analyze the captured appReleaseWebService URL."""
    captured_url = "https://app.ecourts.gov.in/ecourt_mobile_DC/appReleaseWebService.php?params=a705f4f63b82c24747qPE%2FFpOwcPJjKVjd%2FuSgN3RxPO8Ykz8CU47k4f8z0qkFwkpn0Cp3f3%2F1drsdqVSC2ZuUTCxp8GMZTexZutqkNee9zgJ1QX%2BNQZaaTcJIkU%3D"

    print("\n" + "="*80)
    print("Analyzing captured appReleaseWebService URL")
    print("="*80)

    parsed = urlparse(captured_url)
    query = parse_qs(parsed.query)

    params_enc = query.get("params", [""])[0]
    params_decoded = unquote(params_enc)

    print(f"\nParams (decoded): {params_decoded[:80]}...")

    # Try to decrypt
    try:
        decrypted = decrypt_response_cbc(params_decoded, REQUEST_KEY_HEX)
        print(f"Decrypted params (REQUEST_KEY):")
        print(json.dumps(decrypted, indent=2))
    except Exception as e:
        print(f"Failed with REQUEST_KEY: {e}")


if __name__ == "__main__":
    analyze_captured_pdf_url()
    analyze_case_history_url()
    analyze_app_release_url()

    print("\n" + "="*80)
    print("KEY FINDINGS:")
    print("="*80)
    print("""
STRUCTURE DISCOVERED:
1. params = encrypted JSON containing: filename, caseno, cCode, appFlag, state_cd, dist_cd, court_code, bilingual_flag
2. authtoken = "Bearer " + encryptData(jwttoken) encrypted with RESPONSE_KEY

The authtoken structure suggests:
- The OUTER encryption uses RESPONSE_KEY (server provides to client)
- The INNER JWT token is encrypted with REQUEST_KEY (client encrypts before sending)

NEXT STEPS:
1. Need the response body from appReleaseWebService.php or other API to find the 'token' field
2. This token is what gets encrypted as the inner JWT
3. Once we have the token, we can construct valid authtoken values

Please provide the decrypted response bodies from Proxyman for:
- appReleaseWebService.php
- caseHistoryWebService.php
Look for any 'token' field in the responses.
""")
