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


def compress_pdf_if_enabled(pdf_path: Path, compression_available: bool) -> Path:
    """
    Compress a PDF file if compression is enabled and available.
    Returns the path to the final PDF (original or compressed).
    """
    if not compression_available:
        return pdf_path

    try:
        # Create temporary compressed file
        compressed_path = pdf_path.with_suffix(".compressed.pdf")

        # Compress the PDF
        success, message = compress_pdf(pdf_path, compressed_path)

        if success and compressed_path.exists():
            original_size = pdf_path.stat().st_size
            compressed_size = compressed_path.stat().st_size

            # Only replace if compressed version is smaller
            if compressed_size < original_size:
                # Replace original with compressed version
                pdf_path.unlink()  # Remove original
                compressed_path.rename(pdf_path)  # Rename compressed to original name
                return pdf_path
            else:
                # Keep original, remove compressed version
                compressed_path.unlink()
                return pdf_path
        else:
            # Compression failed, keep original
            if compressed_path.exists():
                compressed_path.unlink()
            return pdf_path

    except Exception:
        return pdf_path
