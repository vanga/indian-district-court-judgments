"""
AES encryption/decryption for eCourts Mobile API.

Two encryption modes are supported:
1. Per-parameter AES-ECB: For individually encrypted URL parameters
2. Full-body AES-CBC: For encrypting entire request body

Keys extracted from: apk_extracted/assets/www/js/main.js
"""

import base64
import json
import os
import re
from typing import Any

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


# Encryption keys (from main.js)
REQUEST_KEY_HEX = "4D6251655468576D5A7134743677397A"  # MbQeThWmZq4t6w9z
RESPONSE_KEY_HEX = "3273357638782F413F4428472B4B6250"  # 2s5v8x/A?D(G+KbP

# Global IV options for CBC mode (from main.js generateGlobalIv function)
GLOBAL_IV_OPTIONS = [
    "556A586E32723575",  # UjXn2r5u
    "34743777217A2543",  # 4t7w!z%C
    "413F4428472B4B62",  # A?D(G+Kb
    "48404D635166546A",  # H@McQfTj
    "614E645267556B58",  # aNdRgUkX
    "655368566D597133",  # eShVmYq3
]

# Default global IV (from main.js line 28)
DEFAULT_GLOBAL_IV = "4B6250655368566D"  # KbPeShVm


def _hex_to_bytes(hex_str: str) -> bytes:
    """Convert hex string to bytes."""
    return bytes.fromhex(hex_str)


def _bytes_to_hex(b: bytes) -> str:
    """Convert bytes to hex string."""
    return b.hex()


# ============================================================================
# Per-Parameter Encryption (AES-ECB) - For encrypted URL parameters
# ============================================================================

def encrypt_param_ecb(plaintext: str, key_hex: str = REQUEST_KEY_HEX) -> str:
    """
    Encrypt a single parameter value using AES-ECB.

    This is used for individually encrypted URL parameters in the
    ecourt_mobile_encrypted_DC endpoint.

    Args:
        plaintext: The value to encrypt
        key_hex: AES key in hex format

    Returns:
        Base64-encoded ciphertext
    """
    key = _hex_to_bytes(key_hex)
    cipher = AES.new(key, AES.MODE_ECB)

    # Pad to 16-byte boundary
    padded = pad(plaintext.encode('utf-8'), AES.block_size)
    encrypted = cipher.encrypt(padded)

    return base64.b64encode(encrypted).decode('utf-8')


def decrypt_param_ecb(ciphertext_b64: str, key_hex: str = RESPONSE_KEY_HEX) -> str:
    """
    Decrypt a single parameter value using AES-ECB.

    Args:
        ciphertext_b64: Base64-encoded ciphertext
        key_hex: AES key in hex format

    Returns:
        Decrypted plaintext
    """
    key = _hex_to_bytes(key_hex)
    cipher = AES.new(key, AES.MODE_ECB)

    ciphertext = base64.b64decode(ciphertext_b64)
    decrypted = cipher.decrypt(ciphertext)

    # Unpad
    try:
        return unpad(decrypted, AES.block_size).decode('utf-8')
    except ValueError:
        # If unpadding fails, try to decode as-is and strip nulls
        return decrypted.rstrip(b'\x00').decode('utf-8', errors='ignore')


# ============================================================================
# Full-Body Encryption (AES-CBC) - For native app HTTP plugin
# ============================================================================

def encrypt_data_cbc(data: dict, key_hex: str = REQUEST_KEY_HEX) -> str:
    """
    Encrypt entire data object using AES-CBC.

    This matches the encryptData() function in main.js:
    1. JSON-stringify the data
    2. Generate random IV (globaliv + randomiv)
    3. Encrypt with AES-CBC
    4. Return: randomiv + globalIndex + base64(ciphertext)

    Args:
        data: Dictionary to encrypt
        key_hex: AES key in hex format

    Returns:
        Encrypted string in format: randomiv + globalIndex + base64(ciphertext)
    """
    import random

    key = _hex_to_bytes(key_hex)

    # Generate random parts of IV
    global_index = random.randint(0, len(GLOBAL_IV_OPTIONS) - 1)
    global_iv = GLOBAL_IV_OPTIONS[global_index]
    random_iv = ''.join(random.choice('0123456789abcdef') for _ in range(16))

    # Full IV = globaliv (8 bytes) + randomiv (8 bytes) = 16 bytes
    full_iv = _hex_to_bytes(global_iv + random_iv)

    # Encrypt
    cipher = AES.new(key, AES.MODE_CBC, iv=full_iv)
    plaintext = json.dumps(data)
    padded = pad(plaintext.encode('utf-8'), AES.block_size)
    ciphertext = cipher.encrypt(padded)

    # Format: randomiv (16 hex chars) + globalIndex (1 digit) + base64(ciphertext)
    encrypted_b64 = base64.b64encode(ciphertext).decode('utf-8')
    return f"{random_iv}{global_index}{encrypted_b64}"


def decrypt_response_cbc(encrypted_str: str, key_hex: str = RESPONSE_KEY_HEX) -> Any:
    """
    Decrypt response using AES-CBC.

    This matches the decodeResponse() function in main.js:
    1. First 32 chars = IV in hex
    2. Rest = base64(ciphertext)

    Args:
        encrypted_str: Encrypted string from server
        key_hex: AES key in hex format

    Returns:
        Decrypted and JSON-parsed data
    """
    key = _hex_to_bytes(key_hex)

    # Parse the encrypted string
    encrypted_str = encrypted_str.strip()
    iv_hex = encrypted_str[:32]
    ciphertext_b64 = encrypted_str[32:]

    iv = _hex_to_bytes(iv_hex)
    ciphertext = base64.b64decode(ciphertext_b64)

    # Decrypt
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    decrypted = cipher.decrypt(ciphertext)

    # Unpad and decode
    try:
        plaintext = unpad(decrypted, AES.block_size).decode('utf-8')
    except ValueError:
        plaintext = decrypted.rstrip(b'\x00').decode('utf-8', errors='ignore')

    # Clean up non-printable characters (from main.js)
    plaintext = re.sub(r'[\x00-\x19]+', '', plaintext)

    # Parse JSON
    try:
        return json.loads(plaintext)
    except json.JSONDecodeError:
        return plaintext


# ============================================================================
# Server-format encryption (for authtoken in PDF URLs)
# ============================================================================

def encrypt_server_format(data: str, key_hex: str = RESPONSE_KEY_HEX) -> str:
    """
    Encrypt data using server's format: IV (32 hex) + base64(ciphertext).

    This is used for the authtoken parameter in PDF download URLs.
    The server format uses a full 16-byte IV (32 hex chars) followed by base64-encoded
    ciphertext, without the global index that our standard encrypt_data_cbc uses.

    Args:
        data: String to encrypt (e.g., "Bearer <encrypted_jwt>")
        key_hex: AES key in hex format

    Returns:
        Encrypted string in format: IV (32 hex) + base64(ciphertext)
    """
    import random

    key = _hex_to_bytes(key_hex)

    # Generate random 16-byte IV
    iv_bytes = bytes([random.randint(0, 255) for _ in range(16)])
    iv_hex = _bytes_to_hex(iv_bytes)

    # Encrypt
    cipher = AES.new(key, AES.MODE_CBC, iv=iv_bytes)
    padded = pad(data.encode('utf-8'), AES.block_size)
    ciphertext = cipher.encrypt(padded)

    # Format: IV (32 hex) + base64(ciphertext)
    encrypted_b64 = base64.b64encode(ciphertext).decode('utf-8')
    return f"{iv_hex}{encrypted_b64}"


# ============================================================================
# URL Parameter Decryption (for PDF URLs)
# ============================================================================

def decrypt_url_param(encrypted_str: str, key_hex: str = RESPONSE_KEY_HEX) -> Any:
    """
    Decrypt URL parameter (params or authtoken from PDF URLs).

    The format is the same as CBC response: 32 hex chars (IV) + base64(ciphertext)

    Args:
        encrypted_str: URL-encoded encrypted parameter
        key_hex: AES key in hex format

    Returns:
        Decrypted data (JSON parsed if applicable)
    """
    from urllib.parse import unquote

    # URL decode first
    decoded = unquote(encrypted_str)

    # Use the same decryption as response
    return decrypt_response_cbc(decoded, key_hex)


# ============================================================================
# Utility Functions
# ============================================================================

def encrypt_params(params: dict) -> dict:
    """
    Encrypt all parameter values in a dictionary using AES-ECB.

    Args:
        params: Dictionary of parameter names and values

    Returns:
        Dictionary with encrypted values
    """
    return {key: encrypt_param_ecb(str(value)) for key, value in params.items()}


def try_decrypt_captured_params():
    """
    Try to decrypt the captured parameters from traffic analysis.
    This helps verify if our encryption keys are correct.
    """
    # Captured encrypted values from the mobile traffic
    captured = {
        "language_flag": "OYXI/PbEQ5UvCTNs18Z8ug==",
        "bilingual_flag": "+b82aw/FHEpfVc9p01n6zw==",
        "state_code": "YanVGicWreBFsmjSO6meCQ==",
        "dist_code": "kgI20ZnBO8qQnloxM9nZug==",
        "court_code_1": "YoiXCreKw/X0xiLh42VjTg==",
        "court_code_2": "kgI20ZnBO8qQnloxM9nZug==",
        "court_code_3": "zcxPbzl3NJD4rFVGssvT3A==",
        "case_type_1": "0iKR9RdsYAzRCXpsDvKOlQ==",
        "case_type_2": "kTGHfHPvNufi4i1jshZOOg==",
        "year": "5UpdYwEFmGu5Lez877lQVg==",
        "pendingDisposed_1": "gLDF1wsJRH4W108VeQEsEQ==",
        "pendingDisposed_2": "w1RPHYl2wFOlR/7fYKFhVQ==",
        "action_code_1": "POaJ42M9nP6pkEJim6CFmQ==",
        "case_no": "9205DYheAtPf7UzOSB1Hyg==",
    }

    print("Attempting to decrypt captured parameters...")
    print("=" * 60)

    # Try with request key
    print("\nUsing REQUEST_KEY (for encryption):")
    for name, encrypted in captured.items():
        try:
            decrypted = decrypt_param_ecb(encrypted, REQUEST_KEY_HEX)
            print(f"  {name}: {encrypted[:20]}... -> {decrypted}")
        except Exception as e:
            print(f"  {name}: Failed - {e}")

    # Try with response key
    print("\nUsing RESPONSE_KEY (for decryption):")
    for name, encrypted in captured.items():
        try:
            decrypted = decrypt_param_ecb(encrypted, RESPONSE_KEY_HEX)
            print(f"  {name}: {encrypted[:20]}... -> {decrypted}")
        except Exception as e:
            print(f"  {name}: Failed - {e}")


if __name__ == "__main__":
    # Test the encryption functions
    print("Testing AES Encryption/Decryption")
    print("=" * 60)

    # Test ECB encryption
    test_value = "english"
    encrypted = encrypt_param_ecb(test_value)
    print(f"\nECB Test:")
    print(f"  Original: {test_value}")
    print(f"  Encrypted: {encrypted}")

    # Test CBC encryption
    test_data = {"state_code": "1", "action_code": "getStates"}
    encrypted_cbc = encrypt_data_cbc(test_data)
    print(f"\nCBC Test:")
    print(f"  Original: {test_data}")
    print(f"  Encrypted: {encrypted_cbc[:50]}...")

    # Try to decrypt captured values
    print("\n")
    try_decrypt_captured_params()
