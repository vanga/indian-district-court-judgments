# eCourts Web Scraper — Agent Reference

**Last updated:** 2026-05-02  
**Portal:** `https://services.ecourts.gov.in/ecourtindia_v6/`  
**Detailed investigation logs:** `WEB_SCRAPER_FINDINGS.md`, `WEB_PORTAL_SEARCH_OPTIONS.md`

---

## Overview

The web portal at `services.ecourts.gov.in` is a complete fallback for the mobile API at `app.ecourts.gov.in`. When the mobile API IP is blocked, the web portal can reach the **full case docket** — same coverage, same metadata fields — at the cost of CAPTCHA solving per query.

**Coverage equivalence confirmed:** Running `submit_case_type` against Civil Court Gunderdehi (state=18, district=19, complex=1180084) for year 2024 returned exactly 2,323 cases (2,050 Disposed + 273 Pending), matching the mobile API's 2,047+276=2,323 (3-case delta explained by cases that moved Pending→Disposed between the two runs).

---

## Session Architecture

Every search follows this protocol:

```
GET  ?p=courtorder/index          → SERVICES_SESSID cookie + initial app_token (often empty)
POST ?p=casestatus/set_data       → binds session to a specific complex (no CAPTCHA)
GET  vendor/securimage/...        → fetch CAPTCHA image (6-char alphanumeric)
POST ?p=<module>/<submitFn>       → search (CAPTCHA-gated)
POST ?p=home/viewHistory          → per-case enrichment (NO CAPTCHA)
```

**Key invariants:**
- `app_token` rotates with every JSON response — always pass back the latest value
- `case_status` must be full words: `"Pending"` / `"Disposed"` / `"Both"` — sending `P`/`D`/`B` silently returns "Record not found"
- `ajax_req=true` is mandatory on every POST; without it the server returns full HTML instead of JSON
- Session cookies expire; Phase 2 (viewHistory enrichment) must start a fresh `init_session()` — there is **no session stickiness between phases**

---

## Two-Phase Architecture

### Phase 1 — Case List (CAPTCHA-gated)

Run `submit_case_type` for each `(case_type, year, status)` combination at a complex. Outputs per-case:

| Field | Source |
|-------|--------|
| CNR | `viewHistory(case_id, **CNR**, ...)` onclick in row HTML |
| `internal_case_no` | First arg of viewHistory onclick |
| `court_code` | Third arg of viewHistory onclick |
| `case_number` | Table cell (e.g. `CRIMINAL CASE/918/2025`) |
| `parties` | Table cell |
| `petitioner`, `respondent` | Parsed from parties text |
| `establishment_name` | `<th id="td_court_name_X">` group header |

**Cost:** 1 CAPTCHA per `(case_type, year, status)` tuple. A complex with 16 case types costs 32 CAPTCHAs (~35s) to get the full case list.

### Phase 2 — Case Enrichment (NO CAPTCHA)

For each case from Phase 1, POST to `?p=home/viewHistory` using the stored `internal_case_no`, `cnr`, `court_code`. Returns full case HTML.

**Phase 2 is fully independent of Phase 1's session.** All identifiers needed (`internal_case_no`, `cnr`, `court_code`, `state_code`, `dist_code`, `court_complex_code`) are permanent database IDs saved in the Phase 1 JSON. Phase 2 only needs:
1. A fresh `init_session()` call to get new session cookies (2 lightweight HTTP calls, no CAPTCHA)
2. One `init_session()` per complex (not per case)

**viewHistory enrichment fields:**

| Field | Available |
|-------|-----------|
| CNR, case type (full name) | Yes |
| Filing date, registration date | Yes |
| Decision date | Yes |
| First / next hearing date | Yes |
| Case status, case stage | Yes |
| Court number + judge name | Yes |
| Petitioner + advocate name | Yes |
| Respondent + advocate name | Yes |
| Acts and sections | Yes |
| Full hearing history | Yes (HTML table, keys need careful parsing — see known bug below) |
| Hindi transliterations | **No** (mobile API only) |
| FIR number / police station | **No** (mobile API only) |

**Known bug in `case_history` parsing:** The hearing history HTML table columns are keyed by arbitrary text, resulting in garbled dict keys (e.g. `{"Civil Judge Sr Div Gunderdehi": "Civil Judge..."}` instead of named fields). Data is present but structure is noisy. Needs reprocessing.

---

## All Search Axes

### Currently implemented

| Script | Axis | CAPTCHA/query | Coverage |
|--------|------|---------------|----------|
| `download.py` | Order date (`submitOrderDate`) | 1 | Cases with orders in date range only |
| `scrape_by_case_type.py` | Case type + year + status (`submit_case_type`) | 1 | **Full docket** — equivalent to mobile API |

### Not yet implemented (valuable)

| Axis | Endpoint | CAPTCHA | Value |
|------|----------|---------|-------|
| **Case type + year** | `casestatus/submit_case_type` | 1 | **Priority 1** — closes coverage gap vs mobile API |
| **Advocate name** | `casestatus/submitAdvName` | 1 | Bulk coverage check; 1,785 cases/query measured |
| **Party name** | `casestatus/submitPartyName` | 1 | 879 cases/query measured; CNR in each row |
| **Act / Section** | `casestatus/submitAct` | 1 | Legal-research slicing (e.g. all IPC §302 cases) |
| **Cause list** | `cause_list/submitCauseList` | 1 | Forward-looking: cases scheduled for hearing; date range ±1 month only |
| **FIR number** | `casestatus/submitFirNo` | 1 | Criminal case provenance; single-case lookup |
| **Filing number** | `casestatus/submitFillingNo` | 1 | Single-case lookup |

### Does not exist
- **Search by judge** — no dedicated endpoint. Closest: `cause_list/fillCauseList` returns judge names per complex; `cause_list/submitCauseList` filters by judge+date (±1 month only).
- **Search by date of decision** — no endpoint for ordinary cases. `caveat_search/submitCaveat` mode 5 has `caveat_dateofdecision_date` but only for caveats.
- **Village search** — `villageSearch/index` returns HTTP 400; route is a disabled stub.

---

## CAPTCHA-Free Helper Endpoints

These populate dropdowns and require no CAPTCHA:

| Endpoint | Returns |
|----------|---------|
| `casestatus/fillCaseType` | Case type codes for a complex (e.g. `21^3` = "CRIMINAL CASE") |
| `casestatus/fillActType` | Act codes (e.g. `actcode=12` = "IPC") |
| `casestatus/fillPoliceStation` | Police station list for FIR search |
| `cause_list/fillCauseList` | Judge list per complex (name + designation + court_no) |
| `casestatus/fillDistrict` | District list for a state |
| `casestatus/fillcomplex` | Complex list for a district |

---

## Cost Model

| Operation | CAPTCHAs |
|-----------|----------|
| Full case list for 1 complex/year (16 case types) | ~32 |
| Enrich 1 case (viewHistory) | 0 |
| Full enrich for Gunderdehi 2024 (2,323 cases) | 32 + 0 = 32 |
| Full pass: 3,567 complexes × 6 years (case list only) | ~680,000 |
| Full pass: 3,567 complexes × 6 years + enrichment | 680,000 + 0 = 680,000 |

CAPTCHA solver (`web/src/captcha_solver/captcha.onnx`) takes ~1-2s per solve. At 2 workers: **full first pass (case list only) ≈ 2-4 weeks**. Mobile API comparison: no CAPTCHAs, 10-50x faster.

---

## Existing Code

| File | Purpose |
|------|---------|
| `web/download.py` | Order-date scraper. `Downloader` class handles all session/CAPTCHA/token logic — reusable |
| `web/scrape_by_case_type.py` | Phase 1 case-type scraper. Saves CNRs + identifiers to JSON. Uses `Downloader`. |
| `web/scrape_courts.py` | One-time court hierarchy builder → `courts.csv` |
| `web/src/captcha_solver/captcha.onnx` | ONNX CAPTCHA solver model |

**Runtime requirement:** `onnxruntime` only has wheels for Python ≤3.13. Always run with:
```bash
uv run --python 3.13 python web/<script>.py
```

**Phase 2 (viewHistory enrichment) script:** not yet written. Would read Phase 1 JSON output, call `init_session()` once per complex, then `viewHistory` per case. No CAPTCHAs needed.

---

## Coverage Comparison: Web vs Mobile API

| Dimension | Web (`submit_case_type` + `viewHistory`) | Mobile API |
|-----------|------------------------------------------|------------|
| Case discovery axis | Case type + year + status | Case type + year + status |
| Coverage | Full docket (identical to mobile API) | Full docket |
| Cases with no orders | Included | Included |
| Metadata richness | All fields except Hindi names + FIR | Full |
| CAPTCHA per complex/year | ~32 | 0 |
| Speed (relative) | ~10-50x slower | Baseline |
| IP requirement | `services.ecourts.gov.in` (separate IP path) | `app.ecourts.gov.in` |

`submitOrderDate` (the existing date-based scraper) is **not** a substitute for full coverage — it only finds cases that had an order uploaded in the searched date window. Use `submit_case_type` for bulk historical collection.
