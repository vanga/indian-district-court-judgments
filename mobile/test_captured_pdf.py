"""
Test PDF download using captured traffic data.

This script tests downloading PDFs using tokens captured from Proxyman.
"""

import requests
from urllib.parse import unquote

from crypto import encrypt_data_cbc, decrypt_response_cbc, REQUEST_KEY_HEX, RESPONSE_KEY_HEX


def test_captured_pdf_download():
    """Test downloading PDF using captured params and authtoken."""

    # Captured from traffic - the exact working request
    captured_params = "e506cd26fa1d8322de04800e4ae2f652hAJ9Q5ijXuZ9pF2KfaBjPq7Izp7/rlu4kOjIneK+85n3g/Mwn5T3fpvwpHJVNabo874YDqn/8iElOX41loTVRCQRasbCs/OO9E0yackp3NaUAUeR+xxk4Ej8iR4sKHBpJ4rOlXplFukIZxUZYboLcaUV1Ub1Jfhj1bqQWJULiD7NPmuO0ax/1PPUctWbG0xfquFzpR+xssgBt1+HhnZXz0lCoSlYNKVNyqoOQlGX/NFRkgkzJgWq0vxqCHykBsAu"

    captured_authtoken = "a2522d9d8d4211babba89c91438439dedK0nTISTlUyW0ameA+xPQotVBWL67zBNDxcfLVp1XptPZ6rSPq6hD8pb2iKBE/7u4qfuOYlmbc3BW/HIZusn4lE9NgYdAtIRMUXcDEjOuSbGafSJNTPAR43KP6FaCw5JITSEqJxC8/Iwkrf/JlFiuzaH5NhyV9WLlSIY36NgjVsGJayypEfCOoNf1jWcjnbe8FEqXFfo8ashYFSn4OyOL1m9c001l3G7Ruiq5FcY7hiy3ywDYsj6mWcXlE5BiOtZrpd+3Yqvpa8yOM7KW8myCSNIoyP+l6jgdfDvfTc5M5i9Ajtp7tLBPCoM3+8W4zN9LcU9NPqNwcDJWE4wHxl8CrzAaNlY+QLsniVb7GE8KWov9N3OihKZn9AaaTf4cA76Iqe+HqXwldaWWzU4yeVzfgE3D2Vv9dtib8URndL5DXaR1GrvU9UjNGu6Ioqzd4GH4axhtn00EcOc/9vn67u9ehVd81rM87vmDaYkIkHu7N1ItM+cWhUiE4qrlPrGikvcp2wUhSLVpzsP+R6oC4fSBNX0yH1xSaQyGE9NKnSh2zW4fGHPOGibX66KcNBRtX1xENlfyWF7dg3DjkXJUCq0xr9o5MeAiLi++J3qf5DDvogu5zemrk+NCVO/rN9V9+XRoyYkrOEinxIjEArmsvdOlvhazFRYQMbIoXk+6fZPfT3yoK66GfTmgYvnYiSSqcWkdaqMmbL+9rRNxf7LyFJ2iSxxiCf4+xt5nQucF9dDGfS2NqC8uNujH8XHF6sxfSp3"

    # Also captured - the Authorization header from caseHistoryWebService request
    # This is the same encrypted token that's inside the authtoken
    captured_auth_header = "Bearer ed2b4058d38286624Epm9tbhDFw/hY9U17d3JqAxkUnQR33/Ce+qjdSjRvVXGX38AfMwX1SsfHqVk6G0eZKvUdQaZrCYWkv/wHRoz2Xk9H9njUxiIrWC4tMkdF+6RtPmJBcitF0aKncM2nDyOjwzYn/CXa+EVzjIz1PRvS2rLXyQ/Asvbvi6clxmu1zLMnyjgnLj3y2ffYyyRe18MQL9e4GRR8H8YEm0irFW81nAWvG3yefQXk5DTOF6x3FYV4of4U8srIVRhFeJF8N9lyZjxqnj4nWLnB/T/SgxesYivKMfz9bAdhwwHdsOsJ5JfjqKi5fKgIOorHEMedgDnYRUk/7zyUXpiTrGlFqyJPwl7d9UoZSYqSwGsi4ZbOxpmxAEKdTj//JToyJsTNeadbVFkA9Bz1vI2LFXp17MQpFaoDtN0XkWwoHTYWtf+Rk9THuzixx97yja+pu1NPUeXCadUsvUt7PyjzDiuaF6I+x4La+/IUMRLbaRGlOPZizzrHT/mkw+bU02VwHHbsnmbEiiw5B/qdTH8dNMGKvi8zw=="

    url = "https://app.ecourts.gov.in/ecourt_mobile_DC/display_pdf.php"

    headers = {
        "Accept": "*/*",
        "User-Agent": "eCourtsServices/2.0.1 (iPhone; iOS 26.2; Scale/3.00)",
        "Accept-Language": "en-IN;q=1",
        "Cookie": "JSESSION=81500815",  # Same as captured
    }

    print("="*80)
    print("Test 1: Using captured params and authtoken directly")
    print("="*80)

    try:
        response = requests.get(
            url,
            params={
                "params": captured_params,
                "authtoken": captured_authtoken,
            },
            headers=headers,
            timeout=60,
            verify=False,  # API uses self-signed cert
        )
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(response.content)}")
        print(f"First bytes: {response.content[:20]}")

        if response.content[:4] == b'%PDF':
            print("SUCCESS! Got PDF response")
            with open("/tmp/test_captured.pdf", "wb") as f:
                f.write(response.content)
            print("Saved to /tmp/test_captured.pdf")
            return True
        else:
            # Try to decrypt error
            try:
                text = response.text.strip()
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    decrypted = decrypt_response_cbc(text)
                    print(f"Decrypted response: {decrypted}")
            except Exception as e:
                print(f"Response text: {response.text[:200]}")

    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "="*80)
    print("Test 2: Adding Authorization header")
    print("="*80)

    headers["Authorization"] = captured_auth_header

    try:
        response = requests.get(
            url,
            params={
                "params": captured_params,
                "authtoken": captured_authtoken,
            },
            headers=headers,
            timeout=60,
            verify=False,
        )
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(response.content)}")
        print(f"First bytes: {response.content[:20]}")

        if response.content[:4] == b'%PDF':
            print("SUCCESS! Got PDF response")
            with open("/tmp/test_captured2.pdf", "wb") as f:
                f.write(response.content)
            print("Saved to /tmp/test_captured2.pdf")
            return True
        else:
            try:
                text = response.text.strip()
                if len(text) > 32 and all(c in '0123456789abcdef' for c in text[:32]):
                    decrypted = decrypt_response_cbc(text)
                    print(f"Decrypted response: {decrypted}")
            except Exception:
                print(f"Response text: {response.text[:200]}")

    except Exception as e:
        print(f"Error: {e}")

    return False


def analyze_authtoken_construction():
    """Analyze how the authtoken is constructed."""
    print("\n" + "="*80)
    print("Analyzing authtoken construction")
    print("="*80)

    # The decrypted authtoken is: "Bearer ed2b4058d3828662..."
    # This means authtoken = encrypt(RESPONSE_KEY, "Bearer " + <encrypted_jwt>)

    # The encrypted JWT is: ed2b4058d38286624Epm9tbhDFw...
    # This is the same as what's in the Authorization header

    # So to construct authtoken, we need:
    # 1. The raw JWT token from API response
    # 2. Encrypt it: encrypted_jwt = encrypt(REQUEST_KEY, jwt_raw)
    # 3. Construct auth value: "Bearer " + encrypted_jwt
    # 4. Encrypt again: authtoken = encrypt(RESPONSE_KEY, auth_value)

    # But wait - the outer encryption uses RESPONSE_KEY for decryption
    # So the outer encryption must use REQUEST_KEY for encryption!

    # Let me try to understand by looking at the captured data
    captured_authtoken = "a2522d9d8d4211babba89c91438439dedK0nTISTlUyW0ameA+xPQotVBWL67zBNDxcfLVp1XptPZ6rSPq6hD8pb2iKBE/7u4qfuOYlmbc3BW/HIZusn4lE9NgYdAtIRMUXcDEjOuSbGafSJNTPAR43KP6FaCw5JITSEqJxC8/Iwkrf/JlFiuzaH5NhyV9WLlSIY36NgjVsGJayypEfCOoNf1jWcjnbe8FEqXFfo8ashYFSn4OyOL1m9c001l3G7Ruiq5FcY7hiy3ywDYsj6mWcXlE5BiOtZrpd+3Yqvpa8yOM7KW8myCSNIoyP+l6jgdfDvfTc5M5i9Ajtp7tLBPCoM3+8W4zN9LcU9NPqNwcDJWE4wHxl8CrzAaNlY+QLsniVb7GE8KWov9N3OihKZn9AaaTf4cA76Iqe+HqXwldaWWzU4yeVzfgE3D2Vv9dtib8URndL5DXaR1GrvU9UjNGu6Ioqzd4GH4axhtn00EcOc/9vn67u9ehVd81rM87vmDaYkIkHu7N1ItM+cWhUiE4qrlPrGikvcp2wUhSLVpzsP+R6oC4fSBNX0yH1xSaQyGE9NKnSh2zW4fGHPOGibX66KcNBRtX1xENlfyWF7dg3DjkXJUCq0xr9o5MeAiLi++J3qf5DDvogu5zemrk+NCVO/rN9V9+XRoyYkrOEinxIjEArmsvdOlvhazFRYQMbIoXk+6fZPfT3yoK66GfTmgYvnYiSSqcWkdaqMmbL+9rRNxf7LyFJ2iSxxiCf4+xt5nQucF9dDGfS2NqC8uNujH8XHF6sxfSp3"

    encrypted_jwt_from_auth = "ed2b4058d38286624Epm9tbhDFw/hY9U17d3JqAxkUnQR33/Ce+qjdSjRvVXGX38AfMwX1SsfHqVk6G0eZKvUdQaZrCYWkv/wHRoz2Xk9H9njUxiIrWC4tMkdF+6RtPmJBcitF0aKncM2nDyOjwzYn/CXa+EVzjIz1PRvS2rLXyQ/Asvbvi6clxmu1zLMnyjgnLj3y2ffYyyRe18MQL9e4GRR8H8YEm0irFW81nAWvG3yefQXk5DTOF6x3FYV4of4U8srIVRhFeJF8N9lyZjxqnj4nWLnB/T/SgxesYivKMfz9bAdhwwHdsOsJ5JfjqKi5fKgIOorHEMedgDnYRUk/7zyUXpiTrGlFqyJPwl7d9UoZSYqSwGsi4ZbOxpmxAEKdTj//JToyJsTNeadbVFkA9Bz1vI2LFXp17MQpFaoDtN0XkWwoHTYWtf+Rk9THuzixx97yja+pu1NPUeXCadUsvUt7PyjzDiuaF6I+x4La+/IUMRLbaRGlOPZizzrHT/mkw+bU02VwHHbsnmbEiiw5B/qdTH8dNMGKvi8zw=="

    # Verify: decrypt authtoken with RESPONSE_KEY should give "Bearer " + encrypted_jwt
    print("Step 1: Decrypt authtoken with RESPONSE_KEY")
    try:
        decrypted = decrypt_response_cbc(captured_authtoken)
        print(f"  Result: {decrypted[:80]}...")
        expected = "Bearer " + encrypted_jwt_from_auth
        if decrypted == expected:
            print("  MATCHES expected!")
        else:
            print(f"  Expected: Bearer {encrypted_jwt_from_auth[:60]}...")
    except Exception as e:
        print(f"  Error: {e}")

    # Now we know the construction:
    # 1. encrypted_jwt = encrypt(REQUEST_KEY, jwt_raw)
    # 2. authtoken = encrypt(RESPONSE_KEY, "Bearer " + encrypted_jwt)

    # But wait - the encrypt function is symmetric, so:
    # - To decrypt with RESPONSE_KEY, the encryption was done with RESPONSE_KEY
    # - The app encrypts the authtoken using RESPONSE_KEY (not REQUEST_KEY)

    # This is strange because typically request data uses REQUEST_KEY
    # But looking at the decryption - it works with RESPONSE_KEY

    # Let me try to recreate the authtoken
    print("\nStep 2: Try to recreate authtoken")
    bearer_with_jwt = "Bearer " + encrypted_jwt_from_auth

    # Try encrypting with RESPONSE_KEY (since that's what we used to decrypt)
    # But actually our encrypt function doesn't take a key parameter in the same way

    # Let me check the crypto.py - the encrypt_data_cbc takes REQUEST_KEY by default
    # and decrypt_response_cbc uses RESPONSE_KEY by default

    # This means:
    # - For requests (params), we encrypt with REQUEST_KEY, server decrypts with REQUEST_KEY
    # - For responses, server encrypts with RESPONSE_KEY, we decrypt with RESPONSE_KEY

    # But the authtoken is client-generated, so it should use REQUEST_KEY for encryption
    # However, our decryption with RESPONSE_KEY worked...

    # Actually looking at the format - the first 32 chars are the IV (16 hex bytes)
    # For authtoken: a2522d9d8d4211babba89c91438439de
    # For our encrypt: uses random IV + global index format

    # The authtoken format is just: IV (32 hex) + base64(ciphertext)
    # This is different from our encrypt format: random_iv (16 hex) + index (1 digit) + base64

    print("\nConclusion:")
    print("The authtoken uses a simpler encryption format: IV (32 hex) + base64(ciphertext)")
    print("Our encrypt_data_cbc uses: random_iv (16 hex) + index (1 digit) + base64")
    print("This explains why direct encryption doesn't work - format mismatch!")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    test_captured_pdf_download()
    analyze_authtoken_construction()
