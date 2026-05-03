# Web Scraper Findings Report
**Date:** 2026-04-27  
**Context:** Mobile API IP (14.140.185.73) blocked; evaluating web scraper as fallback  
**Source analysed:** `web/download.py`, `web/scrape_courts.py`, `web/src/captcha_solver/`, `web/src/utils/`

---

## 1. How the Web Scraper Works End-to-End

The web scraper targets `https://services.ecourts.gov.in/ecourtindia_v6/` and searches for court **orders** (not cases) by date range. The pipeline has two distinct phases.

### Phase 0 — Court Hierarchy (scrape_courts.py)

Run once to build `courts.csv`. Three sequential API calls per district:

```
GET  ?p=courtorder/index          → page HTML → extract app_token + state list
POST ?p=casestatus/fillDistrict   → district options for a state
POST ?p=casestatus/fillcomplex    → court complex options for a district
```

Complex codes come back in the format `{complex_id}@{court_numbers}@{flag}` (e.g., `1100120@10,11,12@N`). These are saved to `courts.csv` and reused by `download.py`.

### Phase 1 — Session Init (per task/worker)

Each `Downloader` instance handles one *task* (one court complex × one date range).

```
GET  ?p=courtorder/index                → extracts app_token + SERVICES_SESSID/PHPSESSID cookie
POST ?p=casestatus/set_data             → binds the session to a specific complex
```

The `app_token` is a rotating CSRF-style token. It is embedded in every subsequent POST and is refreshed from each JSON response (`response["app_token"]`). The scraper handles this token rotation automatically.

### Phase 2 — Order Search

```
GET  vendor/securimage/securimage_show.php?<uuid>   → CAPTCHA image (PNG)
POST ?p=courtorder/submitOrderDate                  → returns HTML table of matching orders
```

POST params include: `state_code`, `dist_code`, `court_complex`, `court_complex_arr` (court numbers), `from_date`/`to_date` (DD-MM-YYYY), `fradorderdt`/`orderflagvaldate` (`"both"` | `"interim"` | `"finalorder"`), plus the solved CAPTCHA and `app_token`.

The response JSON has a `court_dt_data` field containing an HTML table of orders. Each row has: serial number, case number (e.g. `OS/32/2024`), parties, order date, and a link/button with an `onclick="displayPdf(...)"` handler.

### Phase 3 — Case Detail Enrichment (optional, enabled by default)

For each order found, the scraper optionally fetches full case details — this costs **2 additional CAPTCHA solves** per order:

```
POST ?p=casestatus/fillCaseType     → maps short codes ("OS") to internal codes ("17^43")
GET  vendor/securimage/securimage_show.php?<uuid>   → 2nd CAPTCHA
POST ?p=casestatus/submitCaseNo     → finds viewHistory params (CNR, court_code, etc.)
POST ?p=home/viewHistory            → full case details HTML (no CAPTCHA needed)
```

`viewHistory` returns: CNR, case type (full name), filing number/date, registration number/date, first/next hearing date, case status, stage, court number + judge, petitioners with advocates, respondents with advocates, acts/sections, and full case history.

### Phase 4 — PDF Download

```
POST ?p=home/display_pdf    → returns JSON with {"order": "/path/to/file.pdf"}
GET  <pdf_url>              → binary PDF bytes
```

The `onclick` on each order row is `displayPdf('normal_v', 'case_val', 'court_code', 'filename', 'appFlag')`. These five params are posted to `display_pdf` to resolve the actual URL. PDFs are then optionally compressed with Ghostscript.

### Checkpointing

Completed tasks (complex × date range) are recorded to `dc_completed_tasks.json` (a flat JSON file on local disk, protected by a threading lock). On re-run, completed tasks are skipped before any network call.

### Concurrency

`concurrent.futures.ThreadPoolExecutor` with configurable workers (default 2 to stay under rate limits). Each worker manages its own `requests.Session` with its own cookies and token state.

---

## 2. Endpoints on services.ecourts.gov.in

All under `https://services.ecourts.gov.in/ecourtindia_v6/`:

| Method | Path | Purpose | CAPTCHA? |
|--------|------|---------|----------|
| GET | `?p=courtorder/index` | Get page HTML, extract app_token + state list | No |
| POST | `?p=casestatus/set_data` | Bind session to court complex | No |
| POST | `?p=casestatus/fillDistrict` | Get district list for a state | No |
| POST | `?p=casestatus/fillcomplex` | Get court complex list for a district | No |
| POST | `?p=casestatus/fillCaseType` | Get case type code map | No |
| GET | `vendor/securimage/securimage_show.php?<uuid>` | Fetch CAPTCHA image | — |
| POST | `?p=courtorder/submitOrderDate` | Search orders by date range | **Yes** |
| POST | `?p=casestatus/submitCaseNo` | Look up case by case number | **Yes** |
| POST | `?p=home/viewHistory` | Fetch full case details | No |
| POST | `?p=home/display_pdf` | Resolve PDF URL from displayPdf params | No |
| GET | `<pdf_url>` | Download actual PDF | No |

**Total CAPTCHAs per order (with enrichment enabled):** 2 (one for `submitOrderDate`, one for `submitCaseNo`). With enrichment disabled (`fetch_case_details=False`), it drops to 1 per search (which can return multiple orders).

---

## 3. CAPTCHAs — Requirement and Solver

### Requirement

CAPTCHAs are mandatory on two endpoints:
- `submitOrderDate` — every date-range search, regardless of court
- `submitCaseNo` — every case lookup during detail enrichment

The CAPTCHA is served by **Securimage** (PHP open-source CAPTCHA library). It always returns a **6-character** alphanumeric string. A fresh CAPTCHA must be fetched each time (the UUID query param prevents caching).

### Solver

**Model:** `web/src/captcha_solver/captcha.onnx` — a sequence recognition model based on a vision transformer (sourced from [huggingface.co/spaces/Acetde/captchabreaker](https://huggingface.co/spaces/Acetde/captchabreaker)).

**Pipeline:**
1. Fetch CAPTCHA image (PNG)
2. Open with PIL, convert to RGB
3. Resize to (32 × 128) with bicubic interpolation
4. Normalize to [-1, 1]
5. Run through ONNX Runtime inference session
6. Softmax logits → greedy decode via `Tokenizer` (EOS/BOS/PAD-aware seq decoder)
7. Return 6-character string

**Validation:** If the decoded string is not exactly 6 characters, the solver retries (up to 10 times, recursively). Failed images are saved to `./captcha-failures/` for debugging.

**Dependencies:** `onnx`, `onnxruntime`, `torch`, `torchvision`, `pillow` — all heavy ML packages, included in root `pyproject.toml`. The model and session are **initialized at import time** (module-level call at the bottom of `main.py`), so the first import of `download.py` loads the full ML model into memory.

**Accuracy:** Unknown without a live test. The Securimage default CAPTCHA format is relatively simple (distorted alphanumeric text on a noisy background), and the model was trained on similar CAPTCHAs. However, accuracy can degrade if eCourts has updated its Securimage configuration (font, noise level, distortion). The retry loop (up to 10 attempts) mitigates single-failure impact.

---

## 4. Current State — Does It Work As-Is?

### What Is Ready

- Session init, court data binding, order search, PDF download — all logic is complete and appears correct.
- CAPTCHA solver model file (`captcha.onnx`) is present and the inference pipeline is implemented.
- Archive management via `S3ArchiveManager` (shared with mobile scraper) is wired up.
- `courts.csv` at the repo root can be reused directly — the web scraper reads the same format.
- Basic rate-limit handling: 405 response triggers a 30-second wait and retry; network errors use exponential backoff (max 3 retries, up to 30s delay).

### Issues That Need Fixing Before Running

**1. Python path — `archive_manager` and `gs` imports will fail**

`download.py` line 48: `from archive_manager import S3ArchiveManager`  
`download.py` line 51: `from gs import check_ghostscript_available, compress_pdf_if_enabled`

The `sys.path.insert` on line 45 only adds `web/src` to the path. `archive_manager.py` and `gs.py` are in the repo root. This works only if the script is run from the repo root as `python web/download.py` (because Python adds the CWD to `sys.path`), but **not** from inside `web/` as `python download.py`.

**Fix:** Add repo root to sys.path at the top of `download.py`:
```python
sys.path.insert(0, str(Path(__file__).parent.parent))  # repo root
```

**2. `courts.csv` path mismatch**

`download.py` defaults `--courts_csv` to `"courts.csv"` (relative path). If run from the `web/` directory, it won't find the file at the repo root. Either pass `--courts_csv ../courts.csv` or change the default to the repo-root file.

**3. `compress_pdf_if_enabled` vs `compress_pdf_bytes`**

`download.py` imports `compress_pdf_if_enabled` from `gs.py`, but the mobile scraper uses `compress_pdf_bytes`. Need to verify `gs.py` exports `compress_pdf_if_enabled` — if not, this is an import error on startup.

**4. CAPTCHA accuracy unknown**

No empirical test has been run against the live site. If the model has low accuracy on current eCourts CAPTCHAs, the retry loop will burn time and may hit rate limits from repeated CAPTCHA fetches. The captcha-failures directory will indicate model quality after a first run.

**5. Detail enrichment is expensive and fragile**

`fetch_case_details=True` by default. For each order this fires 3+ additional API calls (fillCaseType + CAPTCHA + submitCaseNo + viewHistory). If the case type code mapping is absent for a case (unknown type), enrichment silently returns `{}`. For a metadata-only run to establish baseline coverage, disabling enrichment (`fetch_case_details=False`) is strongly advisable.

**6. `day_step` default is 2100 days (~5.75 years)**

The default `--day_step=2100` means one task covers the entire date range per court complex. This is intentional (the comment says "districts has no pagination"), but it means a task failure loses the entire complex's date range with no partial progress saved.

### Quick Fix Checklist to Get Running

```bash
# From repo root:
# 1. Fix sys.path in download.py (add parent dir to path)
# 2. Install dependencies
uv sync

# 3. Test with a single court, small date range, enrichment disabled
uv run python web/download.py \
    --state_code 29 --district_code 22 \
    --start_date 2025-01-01 --end_date 2025-01-07 \
    --max_workers 1 \
    --courts_csv courts.csv \
    --no-compress
```

---

## 5. Data Coverage vs Mobile API

### Search Approach

| Dimension | Web Scraper | Mobile API |
|-----------|-------------|------------|
| Search axis | **Order date** (when a document was uploaded) | **Case type + year + status** (Pending/Disposed) |
| Coverage | Cases with at least one order in the date range | All cases of each type/year, regardless of orders |
| Granularity | 1-day to 2100-day chunks per complex | All cases per type/year per complex |
| Completeness | Misses cases with no orders (filed but never heard) | Full coverage of all registered cases |

### Fields Captured

| Field | Web Scraper | Mobile API |
|-------|-------------|------------|
| CNR number | Yes (via enrichment or regex) | Yes |
| Case number | Yes (from order table) | Yes |
| Party names | Yes | Yes |
| Order date | Yes | Yes |
| PDF | Yes (downloaded) | Yes |
| Filing number/date | Yes (via enrichment) | Yes |
| Registration number/date | Yes (via enrichment) | Yes |
| First/next hearing date | Yes (via enrichment) | Yes |
| Case status / stage | Yes (via enrichment) | Yes |
| Court number + judge | Yes (via enrichment) | Yes |
| Petitioners + advocates | Yes (via enrichment) | Yes |
| Respondents + advocates | Yes (via enrichment) | Yes |
| Acts/sections | Yes (via enrichment) | Yes |
| Full case history | Yes (via enrichment) | Yes |
| Case type (full name) | Yes (via enrichment) | Yes |
| Case type code | Mapped via fillCaseType | Native |

### Key Differences

1. **Coverage gap:** The mobile API iterates over all case types and years and fetches every case. The web scraper only finds cases that had an order uploaded on the searched dates. A case filed in 2020 but with no orders after 2022 will be missed if you only search 2023+.

2. **No CAPTCHA vs CAPTCHA:** The mobile API never requires CAPTCHA solving. The web scraper needs a CAPTCHA solve per search and per case enrichment lookup. At ~6 chars each and 10-retry fallback, this is the primary throughput bottleneck.

3. **Date-based search is complementary:** The web portal's order-date search is useful for *incremental* runs (find what was filed/decided this week). The mobile API's case-type search is better for *bulk historical* collection.

4. **Parallelism ceiling:** The web scraper is limited by CAPTCHA rate (each CAPTCHA fetch is a tracked HTTP request) and session rate limiting (405). The mobile API can run more parallel workers because it has no CAPTCHA gate.

5. **IP blocking:** The mobile API's IP is currently blocked. The web portal at `services.ecourts.gov.in` is still reachable. Since both services are NIC infrastructure, rate limiting on the web portal may be more aggressive than the mobile API — however the web portal is the correct fallback until the mobile API IP is unblocked or rotated.

---

## Recommendation

**The web scraper can run as a fallback with two small code fixes** (sys.path and courts_csv path). It will be slower than the mobile API due to CAPTCHA overhead, but it covers the same courts and can retrieve comparable metadata with enrichment enabled.

**Suggested run configuration:**

```bash
# Metadata-only (no enrichment, no PDFs) for fast baseline
uv run python web/download.py \
    --state_code 29 --district_code 22 \
    --start_date 2020-01-01 --end_date 2026-04-27 \
    --day_step 30 \
    --max_workers 2 \
    --courts_csv courts.csv \
    --no-compress
```

- `--day_step 30`: finer checkpointing (monthly chunks)
- `--max_workers 2`: conservative to avoid 405 rate limits  
- Disable `fetch_case_details` in code for a first metadata pass to gauge CAPTCHA accuracy before enabling enrichment

**Coverage strategy while blocked:** Use the web scraper to collect recent orders (last 2 years by order date). When the mobile API IP is unblocked or rotated, use it for the full historical sweep by case type/year, which will fill in cases that had no orders.

---

## 6. Other Search Dimensions on services.ecourts.gov.in

The `?p=casestatus/index` page offers 7 distinct search tabs beyond court orders:

| Tab | Fields | Coverage axis |
|-----|--------|---------------|
| **By Petitioner/Respondent name** | `petres_name` + court selection | Any case mentioning a name |
| **By Case Number** | `search_case_no` + `search_year` + case type | Single case lookup |
| **By Filing Number** | `filing_no` + `filyear` + court | Single case lookup |
| **By Advocate** | `advocate_name` or `adv_bar_code/state/year` | All cases for an advocate |
| **By FIR Number** | `fir_no` + `firyear` + police station | Criminal cases only |
| **By Act/Section** | `search_act` + `under_sec` | All cases under a specific law |
| **By Case Type** | `case_type` + `rgyear` + court | All cases of a type in a year |

Other portal sections:
- `?p=cause_list/index` — daily cause list (cases scheduled for hearing on a date)
- `?p=caveat_search/index` — caveat filings
- `?p=villageSearch/index` — village-wise case search (rural courts)

### "Search by Case Type" tab

This is the web portal equivalent of what the mobile API does. It accepts:
- Court complex + case type + registration year
- Returns all registered cases of that type/year

**Implication**: The web portal's case-type search covers the same dimension as the mobile API's `searchByCaseType.php` — but **requires CAPTCHA per query** and paginates in a browser context. It would be slow but provides complete coverage identical to the mobile API if implemented.

### Most Promising Unexploited Dimension: Act/Section Search

`search_act` + `under_sec` is not used anywhere in our current scrapers. It lets you find **all cases filed under a specific act across all years** for a court. For targeted legal research (e.g. "all NDPS Act cases in Telangana"), this search axis would be highly efficient compared to iterating all case types.

### Metadata Fields from viewHistory (already captured)

What `?p=home/viewHistory` returns vs mobile API `caseHistoryWebService`:

| Field | Web viewHistory | Mobile API |
|-------|----------------|------------|
| CNR | Yes | Yes |
| Case type (name) | Yes | Yes |
| Filing date | Yes | Yes |
| Registration date | Yes | Yes |
| First/next hearing | Yes | Yes |
| Decision date | Yes | Yes |
| Case status | Yes (text) | Yes (code) |
| Petitioner + advocate | Yes (from HTML) | Yes (structured + Hindi) |
| Respondent + advocate | Yes (from HTML) | Yes (structured + Hindi) |
| Acts/sections | Yes (parsed table) | Yes (4 separate fields) |
| Case history (hearings) | Yes (HTML table, keys garbled) | Yes (structured array) |
| FIR number | No | Yes |
| Court number/judge | Yes (from HTML) | Yes |
| Hindi transliterations | No | Yes |

The web scraper's `case_history` dict parsing has a structural bug — HTML table column headers become dict keys, resulting in entries like `{"Civil Judge Sr Div Gunderdehi": "Civil Judge..."}` instead of named fields. The data is present but needs reprocessing.

### Current vs Possible Coverage

| Mode | Cases found at Gunderdehi | Notes |
|------|--------------------------|-------|
| Mobile API (2024) | 2,047 Disposed + 276 Pending | All registered cases |
| Web scraper (2020–2026, order date) | 448 orders | Only cases with recent orders |
| Web casestatus by case type (not implemented) | ~2,323 | Would match mobile API |

