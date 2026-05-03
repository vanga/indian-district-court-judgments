"""
Web scraper: search by Case Type + Year + Status (mirror of mobile API's searchByCaseType).

Iterates every (case_type, status) combo for a given complex/year and saves the
parsed rows + the raw HTML to a single JSON output file.

Reuses Downloader from web/download.py for session init, app_token rotation, and
CAPTCHA solving. Implements its own POST to ?p=casestatus/submit_case_type and
parses the case_data HTML returned by the endpoint.

Usage:
    uv run --python 3.13 python web/scrape_by_case_type.py \
      --state_code 18 --district_code 19 --complex_code 1180084 \
      --court_numbers 3 --year 2024 \
      --output /tmp/gunderdehi_casetype_2024.json
"""

import argparse
import json
import logging
import re
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Local imports — make sure repo root + web/ are importable
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from bs4 import BeautifulSoup

from download import BASE_URL, Downloader, DistrictCourtTask  # noqa: E402
from archive_manager import S3ArchiveManager  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------- Helpers ----------------------------------------------------------


def make_dummy_task(
    state_code: str,
    district_code: str,
    complex_code: str,
    court_numbers: str,
) -> DistrictCourtTask:
    """Build a minimal task so we can reuse Downloader's session machinery."""
    return DistrictCourtTask(
        id=str(uuid.uuid4()),
        state_code=state_code,
        state_name="",
        district_code=district_code,
        district_name="",
        complex_code=complex_code,
        complex_name="",
        court_numbers=court_numbers,
        from_date="01-01-2024",
        to_date="31-12-2024",
        order_type="both",
    )


def get_case_types_full(d: Downloader) -> List[Tuple[str, str]]:
    """
    Fetch case-type list via fillCaseType. Returns list of (code, label) tuples.
    code is the option's `value`, e.g. "21^3" or "2^21".
    """
    url = f"{BASE_URL}?p=casestatus/fillCaseType"
    data = {
        "state_code": d.task.state_code,
        "dist_code": d.task.district_code,
        "court_complex_code": d.task.complex_code,
        "est_code": "",
        "search_type": "c_no",
        "ajax_req": "true",
        "app_token": d.app_token,
    }
    response = d._fetch_with_retry("POST", url, data=data, timeout=30, verify=False)
    response.raise_for_status()
    result = response.json()
    d._update_token(result)

    casetype_html = result.get("casetype_list", "")
    case_types: List[Tuple[str, str]] = []
    if casetype_html:
        soup = BeautifulSoup(casetype_html, "lxml")
        for option in soup.find_all("option"):
            value = option.get("value", "").strip()
            label = option.get_text(strip=True)
            if value and value != "0" and label and label.lower() != "select":
                case_types.append((value, label))
    return case_types


def parse_case_rows(case_data_html: str) -> Tuple[Optional[int], List[Dict]]:
    """
    Parse the case_data HTML returned by submit_case_type.

    Returns (total_cases_reported, list_of_rows). Each row dict carries:
      - sr_no, case_number, parties, petitioner, respondent
      - cnr, internal_case_no, court_code, est_id (parsed from viewHistory)
      - establishment_name, raw_row_html
    """
    if not case_data_html:
        return (None, [])

    soup = BeautifulSoup(case_data_html, "lxml")

    # Total reported in the <h3> header
    total = None
    h3 = soup.find("h3")
    if h3:
        m = re.search(r"Total number of cases\s*:\s*(\d+)", h3.get_text())
        if m:
            total = int(m.group(1))

    rows_out: List[Dict] = []

    # Walk the table; track current establishment label from <th colspan='3' id='td_court_name_*'>
    table = soup.find("table", id="dispTable") or soup.find("table")
    if not table:
        return (total, [])

    current_establishment = None
    current_est_id = None

    for tr in table.find_all("tr"):
        th = tr.find("th")
        if th and th.get("id", "").startswith("td_court_name_"):
            current_establishment = th.get_text(strip=True)
            m = re.match(r"td_court_name_(\d+)", th.get("id", ""))
            current_est_id = m.group(1) if m else None
            continue

        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        link = tr.find("a", onclick=True)
        onclick = link.get("onclick", "") if link else ""

        # viewHistory(case_id, CNR, court_no, hideparty, search_flag, state, dist, complex, search_by)
        cnr = ""
        internal_case_no = ""
        court_code = ""
        hideparty = ""
        search_flag = ""
        m = re.search(
            r"viewHistory\s*\(\s*(\d+)\s*,\s*'([^']+)'\s*,\s*(\d+)\s*,\s*'([^']*)'\s*,"
            r"\s*'([^']+)'\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*,\s*'([^']+)'\s*\)",
            onclick,
        )
        if m:
            internal_case_no = m.group(1)
            cnr = m.group(2)
            court_code = m.group(3)
            hideparty = m.group(4)
            search_flag = m.group(5)

        sr_no = cells[0].get_text(strip=True)
        case_number = cells[1].get_text(strip=True)
        parties_text = cells[2].get_text(separator=" ", strip=True)

        petitioner = ""
        respondent = ""
        vs_match = re.search(r"(.+?)\s*vs\s*(.+)", parties_text, flags=re.IGNORECASE)
        if vs_match:
            petitioner = vs_match.group(1).strip()
            respondent = vs_match.group(2).strip()

        rows_out.append(
            {
                "sr_no": sr_no,
                "case_number": case_number,
                "parties": parties_text,
                "petitioner": petitioner,
                "respondent": respondent,
                "cnr": cnr,
                "internal_case_no": internal_case_no,
                "court_code": court_code,
                "hideparty": hideparty,
                "search_flag": search_flag,
                "establishment_name": current_establishment,
                "establishment_id": current_est_id,
                "raw_row_html": str(tr),
            }
        )

    return (total, rows_out)


def submit_case_type(
    d: Downloader,
    case_type_code: str,
    search_year: str,
    case_status: str,
    max_captcha_retries: int = 5,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    POST ?p=casestatus/submit_case_type. Returns (parsed_payload, raw_case_data_html).

    parsed_payload has keys: status, total, returned, rows, errormsg.
    """
    url = f"{BASE_URL}?p=casestatus/submit_case_type"

    last_response_json = None
    for attempt in range(max_captcha_retries):
        captcha_code = d.solve_captcha()
        data = {
            "case_type_1": case_type_code,
            "search_year": str(search_year),
            "case_status": case_status,  # "Pending" / "Disposed" — full word!
            "ct_captcha_code": captcha_code,
            "state_code": d.task.state_code,
            "dist_code": d.task.district_code,
            "court_complex_code": d.task.complex_code,
            "est_code": "",
            "ajax_req": "true",
            "app_token": d.app_token,
        }

        try:
            response = d._fetch_with_retry(
                "POST", url, data=data, timeout=60, verify=False
            )
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"submit_case_type network error: {e}")
            return (None, None)

        try:
            result = response.json()
        except json.JSONDecodeError:
            logger.warning(
                "submit_case_type returned non-JSON (likely session reset); aborting this query"
            )
            return (None, None)

        d._update_token(result)
        last_response_json = result

        errormsg = (result.get("errormsg") or "").lower()
        if errormsg:
            if "captcha" in errormsg:
                logger.info(
                    f"[{case_type_code}/{search_year}/{case_status}] captcha failed (attempt {attempt + 1}); retrying"
                )
                time.sleep(0.3)
                continue
            logger.info(
                f"[{case_type_code}/{search_year}/{case_status}] errormsg={result.get('errormsg')}"
            )
            return (
                {
                    "status": result.get("status"),
                    "errormsg": result.get("errormsg"),
                    "total": 0,
                    "returned": 0,
                    "rows": [],
                },
                result.get("case_data", ""),
            )

        # status=0 sometimes = captcha fail with empty msg
        if result.get("status") == 0:
            logger.info(
                f"[{case_type_code}/{search_year}/{case_status}] status=0 (captcha?); retrying"
            )
            time.sleep(0.3)
            continue

        case_data_html = result.get("case_data", "") or ""
        # "Record not found" body — valid empty result
        if "nodata" in case_data_html.lower() or "record not found" in case_data_html.lower():
            return (
                {
                    "status": result.get("status"),
                    "errormsg": "",
                    "total": 0,
                    "returned": 0,
                    "rows": [],
                },
                case_data_html,
            )

        total, rows = parse_case_rows(case_data_html)
        return (
            {
                "status": result.get("status"),
                "errormsg": "",
                "total": total,
                "returned": len(rows),
                "rows": rows,
            },
            case_data_html,
        )

    logger.warning(
        f"[{case_type_code}/{search_year}/{case_status}] gave up after {max_captcha_retries} captcha attempts"
    )
    return (
        {
            "status": (last_response_json or {}).get("status"),
            "errormsg": (last_response_json or {}).get("errormsg") or "captcha_exhausted",
            "total": 0,
            "returned": 0,
            "rows": [],
        },
        None,
    )


# ---------- Main scrape loop -------------------------------------------------


def scrape(
    state_code: str,
    district_code: str,
    complex_code: str,
    court_numbers: str,
    year: int,
    output_path: Path,
):
    task = make_dummy_task(state_code, district_code, complex_code, court_numbers)

    # local-only archive_manager — we don't actually use it here, but Downloader
    # constructor takes it.
    archive_manager = S3ArchiveManager(
        s3_bucket="indian-district-court-judgments-test",
        s3_prefix="",
        local_dir=Path("./local_dc_judgments_data"),
        local_only=True,
    )

    d = Downloader(task, archive_manager, compress_pdfs=False, fetch_case_details=False)

    print(f"[init] starting session for complex={complex_code} year={year}", flush=True)
    d.init_session()
    if not d.set_court_data():
        print("[init] set_court_data failed, aborting", flush=True)
        sys.exit(2)

    print("[fill] fetching case_type list via fillCaseType", flush=True)
    case_types = get_case_types_full(d)
    print(f"[fill] got {len(case_types)} case types", flush=True)
    for code, label in case_types:
        print(f"   - {code}\t{label}", flush=True)

    statuses = ["Pending", "Disposed"]

    summary_per_query: List[Dict] = []
    all_rows: List[Dict] = []
    cnr_seen: Dict[str, Dict] = {}

    for status in statuses:
        for code, label in case_types:
            t0 = time.time()
            payload, raw_html = submit_case_type(
                d, code, str(year), status
            )
            elapsed = time.time() - t0

            total = payload["total"] if payload else None
            returned = payload["returned"] if payload else 0
            err = payload.get("errormsg") if payload else "no_payload"
            print(
                f"[query] {status:<8} {code:<6} ({label[:40]:<40}) "
                f"total={total} returned={returned} err={err!r} took={elapsed:.1f}s",
                flush=True,
            )

            entry = {
                "case_type_code": code,
                "case_type_label": label,
                "case_status": status,
                "search_year": year,
                "total_reported": total,
                "rows_returned": returned,
                "errormsg": err,
                "raw_case_data_html": raw_html,
                "rows": payload["rows"] if payload else [],
            }
            summary_per_query.append(entry)

            for row in (payload["rows"] if payload else []):
                row_with_meta = dict(row)
                row_with_meta["_case_type_code"] = code
                row_with_meta["_case_type_label"] = label
                row_with_meta["_case_status"] = status
                all_rows.append(row_with_meta)

                cnr = row.get("cnr") or f"_NO_CNR_{len(cnr_seen)}"
                if cnr not in cnr_seen:
                    cnr_seen[cnr] = row_with_meta

            # tiny delay between queries
            time.sleep(0.3)

    output = {
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "state_code": state_code,
        "district_code": district_code,
        "complex_code": complex_code,
        "court_numbers": court_numbers,
        "year": year,
        "case_types": [{"code": c, "label": l} for c, l in case_types],
        "queries": summary_per_query,
        "totals": {
            "total_rows": len(all_rows),
            "unique_cnrs": len(cnr_seen),
            "by_status": {
                s: sum(
                    q["rows_returned"]
                    for q in summary_per_query
                    if q["case_status"] == s
                )
                for s in statuses
            },
            "by_status_x_case_type": {
                f"{q['case_status']}|{q['case_type_code']}": q["rows_returned"]
                for q in summary_per_query
            },
        },
        "all_rows": all_rows,
    }

    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"[done] wrote {output_path}", flush=True)
    print(
        f"[done] total rows={len(all_rows)} unique CNRs={len(cnr_seen)} "
        f"Pending={output['totals']['by_status'].get('Pending', 0)} "
        f"Disposed={output['totals']['by_status'].get('Disposed', 0)}",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state_code", required=True)
    parser.add_argument("--district_code", required=True)
    parser.add_argument("--complex_code", required=True)
    parser.add_argument("--court_numbers", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        scrape(
            state_code=args.state_code,
            district_code=args.district_code,
            complex_code=args.complex_code,
            court_numbers=args.court_numbers,
            year=args.year,
            output_path=Path(args.output),
        )
    except KeyboardInterrupt:
        print("[abort] interrupted", flush=True)
        sys.exit(130)
    except Exception as e:
        print(f"[error] {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
