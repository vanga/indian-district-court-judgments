"""
HTML Utility Functions for District Court Judgments
Handles parsing of eCourts HTML responses
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def parse_select_options(html: str) -> List[Tuple[str, str]]:
    """
    Parse HTML select options into list of (value, text) tuples

    Args:
        html: HTML string containing <option> elements

    Returns:
        List of (value, text) tuples
    """
    soup = BeautifulSoup(html, "lxml")
    options = soup.find_all("option")

    result = []
    for opt in options:
        value = opt.get("value", "").strip()
        text = opt.get_text(strip=True)
        # Skip empty values or placeholder options
        if value and value != "" and text:
            result.append((value, text))

    return result


def parse_state_codes(html: str) -> List[Tuple[str, str]]:
    """
    Parse state codes from main page HTML

    Args:
        html: Full HTML page content

    Returns:
        List of (state_code, state_name) tuples
    """
    soup = BeautifulSoup(html, "lxml")

    # Look for the state select element
    state_select = soup.find("select", {"id": "sess_state_code"})
    if not state_select:
        state_select = soup.find("select", {"name": "state_code"})

    if not state_select:
        logger.warning("Could not find state select element")
        return []

    options = state_select.find_all("option")
    result = []

    for opt in options:
        value = opt.get("value", "").strip()
        text = opt.get_text(strip=True)
        # Skip empty values or placeholder options
        if value and value not in ("", "--Select--"):
            result.append((value, text))

    return result


def parse_district_response(json_response: dict) -> List[Tuple[str, str]]:
    """
    Parse district list from fillDistrict API response

    Args:
        json_response: JSON response from fillDistrict API

    Returns:
        List of (district_code, district_name) tuples
    """
    dist_list_html = json_response.get("dist_list", "")
    return parse_select_options(dist_list_html)


def parse_complex_response(json_response: dict) -> List[Tuple[str, str, str, str]]:
    """
    Parse court complex list from fillcomplex API response

    Complex code format: {complex_id}@{court_numbers}@{flag}
    Example: 1100120@10,11,12@N

    Args:
        json_response: JSON response from fillcomplex API

    Returns:
        List of (complex_id, complex_name, court_numbers, flag) tuples
    """
    complex_list_html = json_response.get("complex_list", "")
    soup = BeautifulSoup(complex_list_html, "lxml")
    options = soup.find_all("option")

    result = []
    for opt in options:
        value = opt.get("value", "").strip()
        text = opt.get_text(strip=True)

        if not value or value == "":
            continue

        # Parse the complex code format: {complex_id}@{court_numbers}@{flag}
        parts = value.split("@")
        if len(parts) >= 3:
            complex_id = parts[0]
            court_numbers = parts[1]
            flag = parts[2]
            result.append((complex_id, text, court_numbers, flag))
        elif len(parts) == 2:
            # Some complexes might not have a flag
            complex_id = parts[0]
            court_numbers = parts[1]
            result.append((complex_id, text, court_numbers, "N"))
        else:
            # Single value, use as complex_id with empty court_numbers
            result.append((value, text, "", "N"))
            logger.warning(f"Unexpected complex format: {value}")

    return result


def parse_order_search_results(html: str) -> List[Dict]:
    """
    Parse order search results from submitOrderDate API response

    Args:
        html: HTML response containing order list table

    Returns:
        List of order dictionaries with extracted metadata
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Look for the results table
    table = soup.find("table", {"id": "caseList"})
    if not table:
        table = soup.find("table", class_="table")

    if not table:
        # Try to find any table with order data
        tables = soup.find_all("table")
        for t in tables:
            if t.find("tr"):
                table = t
                break

    if not table:
        return results

    # Parse table rows
    rows = table.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if not cells:
            continue

        order_data = {}

        # Column name mapping for the order results table
        # The table structure is: Serial | Case Number | Parties | Order Date | Order Link
        column_names = [
            "serial_number",
            "case_number",
            "parties",
            "order_date",
            "document_type",
        ]

        # Extract data from cells - structure varies by response
        for idx, cell in enumerate(cells):
            # Look for links to order PDFs
            link = cell.find("a")
            if link:
                href = link.get("href", "")
                onclick = link.get("onclick", "")
                if "viewOrderPdf" in onclick or ".pdf" in href.lower():
                    order_data["pdf_link"] = href
                    order_data["onclick"] = onclick

            # Look for buttons with onclick handlers
            button = cell.find("button")
            if button:
                onclick = button.get("onclick", "")
                if onclick:
                    order_data["onclick"] = onclick

            # Extract text content with proper field names
            text = cell.get_text(strip=True)
            if text:
                if idx < len(column_names):
                    order_data[column_names[idx]] = text
                else:
                    order_data[f"column_{idx}"] = text

        if order_data:
            order_data["raw_html"] = str(row)
            results.append(order_data)

    return results


def extract_pdf_info_from_onclick(onclick: str) -> Optional[Dict]:
    """
    Extract PDF download information from onclick attribute

    Example onclick patterns:
    - viewOrderPdf('CNR','CASE_NO','ORDER_NO','ORDER_DATE')
    - javascript:downloadPdf('path/to/file.pdf')

    Args:
        onclick: onclick attribute value

    Returns:
        Dictionary with extracted PDF info, or None if not parseable
    """
    if not onclick:
        return None

    # Pattern 1: viewOrderPdf with parameters
    pattern1 = r"viewOrderPdf\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)"
    match = re.search(pattern1, onclick)
    if match:
        return {
            "cnr": match.group(1),
            "case_no": match.group(2),
            "order_no": match.group(3),
            "order_date": match.group(4),
        }

    # Pattern 2: downloadPdf with path
    pattern2 = r"downloadPdf\s*\(\s*'([^']+)'\s*\)"
    match = re.search(pattern2, onclick)
    if match:
        return {"pdf_path": match.group(1)}

    # Pattern 3: window.open with URL
    pattern3 = r"window\.open\s*\(\s*'([^']+)'"
    match = re.search(pattern3, onclick)
    if match:
        return {"pdf_url": match.group(1)}

    # Pattern 4: Generic function call with quoted arguments
    pattern4 = r"(\w+)\s*\(\s*([^)]+)\s*\)"
    match = re.search(pattern4, onclick)
    if match:
        func_name = match.group(1)
        args_str = match.group(2)
        # Extract quoted arguments
        args = re.findall(r"'([^']*)'", args_str)
        return {"function": func_name, "args": args}

    return None


def extract_cnr_from_html(html: str) -> Optional[str]:
    """
    Extract CNR (Case Number Record) from HTML content

    Args:
        html: HTML string containing case details

    Returns:
        CNR string if found, None otherwise
    """
    # Pattern: CNR is typically a 16-character alphanumeric code
    # Format: XXYY123456789012 where XX=state, YY=district
    pattern = r"\b([A-Z]{4}\d{12})\b"
    match = re.search(pattern, html)
    if match:
        return match.group(1)
    return None


def parse_app_token(html: str) -> Optional[str]:
    """
    Extract app_token from HTML page

    Args:
        html: Full HTML page content

    Returns:
        app_token value if found
    """
    # Look for app_token in hidden input
    soup = BeautifulSoup(html, "lxml")
    token_input = soup.find("input", {"name": "app_token"})
    if token_input:
        return token_input.get("value", "")

    # Look for app_token in JavaScript
    pattern = r"app_token['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]"
    match = re.search(pattern, html)
    if match:
        return match.group(1)

    # Look for app_token in URL
    pattern2 = r"app_token=([^&'\"]+)"
    match = re.search(pattern2, html)
    if match:
        return match.group(1)

    return None
