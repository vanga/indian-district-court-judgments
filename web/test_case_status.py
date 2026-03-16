"""
Test script for the full scraping flow:
1. Scrape orders using submitOrderDate (normal date-based API)
2. Show what metadata we get from order results
3. Pick one order and fetch detailed case info
4. Show additional fields from case details
"""

import uuid
import sys
from dataclasses import dataclass

sys.path.insert(0, ".")
from download import Downloader, DistrictCourtTask


@dataclass
class TestConfig:
    """Test configuration"""
    state_code: str
    district_code: str
    complex_code: str
    complex_name: str
    court_numbers: str
    from_date: str  # DD-MM-YYYY
    to_date: str    # DD-MM-YYYY
    description: str


def run_test(config: TestConfig):
    """Run the full flow test"""
    print("=" * 80)
    print(f"TEST: {config.description}")
    print("=" * 80)
    print(f"Complex: {config.complex_name} (code: {config.complex_code})")
    print(f"Courts: {config.court_numbers}")
    print(f"Date range: {config.from_date} to {config.to_date}")
    print()

    # Create task
    task = DistrictCourtTask(
        id=str(uuid.uuid4()),
        state_code=config.state_code,
        state_name="Telangana",
        district_code=config.district_code,
        district_name="Rangareddy",
        complex_code=config.complex_code,
        complex_name=config.complex_name,
        court_numbers=config.court_numbers,
        from_date=config.from_date,
        to_date=config.to_date,
    )

    # Create downloader
    downloader = Downloader(task, archive_manager=None, fetch_case_details=True)

    # Initialize session
    print("-" * 40)
    print("STEP 1: Initialize session")
    print("-" * 40)
    downloader.init_session()
    print(f"Session initialized, app_token: {downloader.app_token[:20]}...")
    print()

    # Search orders by date (normal scraping API)
    print("-" * 40)
    print("STEP 2: Search orders by date (submitOrderDate API)")
    print("-" * 40)
    html = downloader.search_orders()
    if not html:
        print("ERROR: No orders found for this date range")
        return

    orders = downloader.parse_order_results(html)
    print(f"Orders found: {len(orders)}")
    print()

    if not orders:
        print("No orders to process")
        return

    # Show first few orders and their fields
    print("-" * 40)
    print("STEP 3: Order metadata from submitOrderDate")
    print("-" * 40)
    print("Fields available from order search:")

    for i, order in enumerate(orders[:3], 1):  # Show first 3
        print(f"\n  Order {i}:")
        print(f"    serial_number: {order.get('serial_number', 'N/A')}")
        print(f"    case_number: {order.get('case_number', 'N/A')}")
        print(f"    parties: {order.get('parties', 'N/A')}")
        print(f"    order_date: {order.get('order_date', 'N/A')}")
        print(f"    document_type: {order.get('document_type', 'N/A')}")
        print(f"    petitioner: {order.get('petitioner', 'N/A')}")
        print(f"    respondent: {order.get('respondent', 'N/A')}")
        print(f"    cnr: {order.get('cnr', 'N/A')}")
        print(f"    onclick: {order.get('onclick', 'N/A')}")
        print(f"    raw_html snippet: {order.get('raw_html', '')[:300]}...")

        # Count non-empty fields
        fields = [k for k, v in order.items() if v and k != 'raw_html']
        print(f"    Total fields: {len(fields)}")

    print()

    # Pick the first order for detailed case lookup
    test_order = orders[0]
    print("-" * 40)
    print("STEP 4: Search for case using submitCaseNo")
    print("-" * 40)
    print(f"Looking up: {test_order.get('case_number')}")

    # Parse case number
    parts = test_order.get('case_number', '').split('/')
    if len(parts) >= 3:
        case_type, case_no, year = parts[0], parts[1], parts[2]
    else:
        print("ERROR: Cannot parse case number")
        return

    # Get case type code
    case_type_codes = downloader.get_case_type_codes()
    case_type_code = case_type_codes.get(case_type)
    print(f"Case type '{case_type}' -> code '{case_type_code}'")

    # Search cases
    case_list = downloader.search_case_status(case_type_code, case_no, year)
    print(f"\nCases found from submitCaseNo: {len(case_list)}")
    for i, c in enumerate(case_list, 1):
        print(f"  {i}. CNR: {c.get('cino')}, court_code: {c.get('court_code')}")
        # Show party names extracted from submitCaseNo HTML (for matching)
        if c.get('petitioner') or c.get('respondent'):
            print(f"      Petitioner: {c.get('petitioner', 'N/A')}")
            print(f"      Respondent: {c.get('respondent', 'N/A')}")
        if c.get('parties'):
            print(f"      Parties: {c.get('parties', 'N/A')}")

    # Show matching logic verification
    if len(case_list) > 1:
        print(f"\n  ** MULTIPLE CASES FOUND - Party name matching will be used **")
        print(f"  Order petitioner: {test_order.get('petitioner', 'N/A')}")
        print(f"  Order respondent: {test_order.get('respondent', 'N/A')}")

    print()
    print("-" * 40)
    print("STEP 5: Get full details from viewHistory")
    print("-" * 40)

    # Get case details using fetch_case_details
    details = downloader.fetch_case_details(test_order)

    if not details:
        print("ERROR: Could not fetch case details")
        print()
    else:
        print("Additional fields from case details (viewHistory API):")
        print(f"    cnr: {details.get('cnr', 'N/A')}")
        print(f"    case_type_full: {details.get('case_type_full', 'N/A')}")
        print(f"    filing_number: {details.get('filing_number', 'N/A')}")
        print(f"    filing_date: {details.get('filing_date', 'N/A')}")
        print(f"    registration_number: {details.get('registration_number', 'N/A')}")
        print(f"    registration_date: {details.get('registration_date', 'N/A')}")
        print(f"    first_hearing_date: {details.get('first_hearing_date', 'N/A')}")
        print(f"    next_hearing_date: {details.get('next_hearing_date', 'N/A')}")
        print(f"    case_stage: {details.get('case_stage', 'N/A')}")
        print(f"    court_number_and_judge: {details.get('court_number_and_judge', 'N/A')}")

        # Count additional fields
        detail_fields = [k for k, v in details.items() if v and k != 'case_details_html']
        print(f"    Total detail fields: {len(detail_fields)}")
    print()

    # Summary comparison
    print("=" * 80)
    print("SUMMARY: Fields comparison")
    print("=" * 80)

    order_fields = set(k for k, v in test_order.items() if v and k != 'raw_html')
    detail_fields = set(k for k, v in details.items() if v and k != 'case_details_html') if details else set()

    print(f"\nFrom ORDER search ({len(order_fields)} fields):")
    print(f"  {sorted(order_fields)}")

    print(f"\nFrom CASE DETAILS ({len(detail_fields)} fields):")
    print(f"  {sorted(detail_fields)}")

    new_fields = detail_fields - order_fields
    print(f"\nNEW fields from case details ({len(new_fields)}):")
    print(f"  {sorted(new_fields)}")
    print()


if __name__ == "__main__":
    # Test: Maheshwaram court, recent date
    config = TestConfig(
        state_code="29",  # Telangana
        district_code="9",  # Rangareddy
        complex_code="1290105",  # Maheshwaram
        complex_name="Maheshwaram",
        court_numbers="9,24",
        from_date="01-01-2025",
        to_date="10-01-2025",
        description="Scrape orders from Maheshwaram Jan 1-10, 2025",
    )

    run_test(config)
