"""
Test whether case_type=0 acts as an "all types" query.

Ground truth from Gunderdehi checkpoint (state=18, district=19, complex=1180084):
  2024/Disposed: 2,047 cases across 16 types (type 21 dominates with 2,013)
  2024/Pending:    276 cases

If case_type=0 returns ~2,047 cases for 2024/Disposed with multiple distinct types,
it's an all-types query and the optimization is valid.
"""

import sys
import time

sys.path.insert(0, ".")

from api_client import MobileAPIClient

STATE = 18
DISTRICT = 19
COURT_CODE = "3"  # njdg_est_code for Gunderdehi
YEAR = 2024

KNOWN_SUMS = {
    "Disposed": 2047,
    "Pending":  276,
}

# Per-type breakdown from checkpoint (non-zero only)
KNOWN_PER_TYPE = {
    "Disposed": {13: 3, 21: 2013, 47: 1, 48: 3, 49: 8, 61: 16, 62: 3},
    "Pending":  {21: 232, 47: 1, 48: 3, 49: 15, 61: 21, 62: 4},
}


def test_case_type(client: MobileAPIClient, case_type: int, status: str) -> dict:
    t0 = time.time()
    cases = client.search_cases_by_type(
        state_code=STATE,
        dist_code=DISTRICT,
        court_code=COURT_CODE,
        case_type=case_type,
        year=YEAR,
        pending_disposed=status,
    )
    elapsed = time.time() - t0
    if cases is None:
        return {"count": None, "elapsed": elapsed, "types": [], "error": "API returned None"}
    type_counts: dict[str, int] = {}
    for c in cases:
        t = c.case_type or "?"
        type_counts[t] = type_counts.get(t, 0) + 1
    return {"count": len(cases), "elapsed": elapsed, "types": type_counts}


def main():
    client = MobileAPIClient()
    print("Initializing session...")
    client.initialize_session()
    print("Session ready.\n")

    for status in ("Disposed", "Pending"):
        known = KNOWN_SUMS[status]
        known_types = KNOWN_PER_TYPE[status]
        print(f"=== 2024/{status} (known sum from checkpoint: {known} cases) ===")

        result = test_case_type(client, case_type=0, status=status)
        count = result.get("count")
        elapsed = result["elapsed"]
        types = result.get("types", {})
        err = result.get("error")

        if err:
            print(f"  case_type=0: ERROR — {err}")
        else:
            match = "MATCH" if count == known else f"MISMATCH (expected {known})"
            type_diversity = len(types)
            print(f"  case_type=0: {count} cases in {elapsed:.1f}s — {type_diversity} distinct type(s) — {match}")
            if types:
                for t, n in sorted(types.items(), key=lambda x: -x[1])[:10]:
                    print(f"    type={t!r}: {n}")

        print(f"  Known per-type breakdown: {known_types}")
        print()


if __name__ == "__main__":
    main()
