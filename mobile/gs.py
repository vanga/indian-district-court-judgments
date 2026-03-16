"""
PDF compression utility using Ghostscript.
"""

import os
import shutil
import subprocess
from pathlib import Path


def check_ghostscript_available() -> bool:
    """Check if Ghostscript is available on the system"""
    try:
        result = subprocess.run(
            ["gs", "--version"], capture_output=True, text=True, timeout=5
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def get_file_size_kb(file_path) -> float:
    """Return file size in KB."""
    return os.path.getsize(file_path) / 1024


def compress_pdf(
    input_path, output_path, compression_level: str = "screen"
) -> tuple[bool, str]:
    """
    Compress a PDF file using Ghostscript.

    Args:
        input_path: Path to the input PDF file
        output_path: Path where the compressed PDF will be saved
        compression_level: Compression level (screen, ebook, printer, prepress, or default)

    Returns:
        tuple: (success, message)
    """
    # Validate compression level
    valid_levels = ["screen", "ebook", "printer", "prepress", "default"]
    if compression_level not in valid_levels:
        return (
            False,
            f"Invalid compression level. Choose from: {', '.join(valid_levels)}",
        )

    try:
        # Find Ghostscript path
        gs_path = shutil.which("gs")
        if not gs_path:
            gs_path = "/usr/bin/gs"  # Fallback

        # Construct Ghostscript command
        gs_command = [
            gs_path,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS=/{compression_level}",
            "-dNOPAUSE",
            "-dBATCH",
            "-dQUIET",
            f"-sOutputFile={output_path}",
            str(input_path),
        ]

        # Execute command
        result = subprocess.run(
            gs_command, capture_output=True, text=True, timeout=120
        )

        if result.returncode != 0:
            return False, f"Ghostscript failed with exit code {result.returncode}"

        # Check file sizes after compression
        if os.path.exists(output_path):
            input_size = get_file_size_kb(input_path)
            output_size = get_file_size_kb(output_path)
            reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0

            if reduction <= 0:
                # No reduction achieved, keep original
                os.remove(output_path)
                shutil.copy(input_path, output_path)
                return True, "No reduction achieved, keeping original"

            return True, f"Compression successful ({reduction:.2f}% reduction)"
        else:
            return False, "Output file was not created"

    except subprocess.TimeoutExpired:
        if os.path.exists(output_path):
            os.remove(output_path)
        return False, "Compression timed out"
    except Exception as e:
        if os.path.exists(output_path):
            os.remove(output_path)
        return False, f"Error during compression: {str(e)}"


def compress_pdf_bytes(pdf_content: bytes, temp_dir: Path) -> bytes:
    """
    Compress PDF content (bytes) using Ghostscript.

    Args:
        pdf_content: Raw PDF bytes
        temp_dir: Directory for temporary files

    Returns:
        Compressed PDF bytes (or original if compression fails/doesn't help)
    """
    temp_dir.mkdir(parents=True, exist_ok=True)

    import uuid
    temp_id = uuid.uuid4().hex[:8]
    input_path = temp_dir / f"input_{temp_id}.pdf"
    output_path = temp_dir / f"output_{temp_id}.pdf"

    try:
        # Write input PDF
        with open(input_path, "wb") as f:
            f.write(pdf_content)

        # Compress
        success, message = compress_pdf(input_path, output_path)

        if success and output_path.exists():
            with open(output_path, "rb") as f:
                compressed_content = f.read()

            # Only use compressed if smaller
            if len(compressed_content) < len(pdf_content):
                return compressed_content

        return pdf_content

    finally:
        # Cleanup
        if input_path.exists():
            input_path.unlink()
        if output_path.exists():
            output_path.unlink()
