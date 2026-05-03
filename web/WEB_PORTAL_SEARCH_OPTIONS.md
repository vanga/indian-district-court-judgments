# eCourts Web Portal — Search Options Inventory

**Date:** 2026-04-28
**Portal:** `https://services.ecourts.gov.in/ecourtindia_v6/`
**Test complex (small):** Civil Court Gunderdehi, Chhattisgarh — `state=18, district=19, complex=1180084, court_numbers=3`
**Test complex (large):** Mahabubnagar PDJ Court Complex, Telangana — `state=29, district=5, complex=1290056, court_numbers=1,3,4,5,15,21`
**Existing scraper coverage:** date-range search via `?p=courtorder/submitOrderDate` only
**Method:** form/JS source analysis + live POSTs with the project's CAPTCHA solver

---

## 0. Common protocol every search uses

All search forms in `?p=casestatus/index`, `?p=cause_list/index`, `?p=caveat_search/index`, and `?p=courtorder/index` go through the same `ajaxCall()` wrapper (defined in `js/components.js`). Every searchable endpoint follows this contract:

| Step | Request | Notes |
|------|---------|-------|
| 1 | `GET ?p=courtorder/index` (or any tab) | Receive `SERVICES_SESSID` cookie + initial empty `app_token` hidden input |
| 2 | `POST ?p=casestatus/set_data` | Bind session to `complex_code@court_numbers@flag`; sets `state_code`, `dist_code`, `est_code` server-side |
| 3 | `GET vendor/securimage/securimage_show.php?<random>` | 6-char alphanumeric CAPTCHA (only required for *search* endpoints, not for *fill* endpoints) |
| 4 | `POST ?p=<tab>/<submitFn>` | Form-encoded, must include `ajax_req=true`, `app_token=<rotating>`, `<form>_captcha_code=<solution>` |
| 5 | Parse JSON | Every response is JSON with `status` (1=hits, 0=captcha-fail), a fresh `app_token`, a `div_captcha` HTML blob with the next CAPTCHA, and one of `case_data` / `party_data` / `adv_data` / `act_data` / `filing_data` / `court_dt_data` containing the result table HTML |

`csrf-magic.js` is referenced but commented out — `app_token` is the actual CSRF token (rotates per response). Without `ajax_req=true`, every endpoint returns the full HTML shell instead of JSON. **`case_status` radio values must be the full word `Pending` / `Disposed` / `Both`** — sending `P`/`D`/`B` returns "Record not found" silently. (This is the single most common reason these endpoints are misreported as broken.)

`displayPdf()` onclick handlers contain four AES-CBC-encrypted base64 blobs (the same opaque tokens the existing scraper resolves through `?p=home/display_pdf`). The plaintext params (state/district/complex codes, CNR, case number, year) are sent in the clear; only the PDF-locator triple is encrypted.

---

## 1. `?p=casestatus/index` — seven search tabs

For each tab below: live-tested submit endpoint, full param set sent on the wire, JSON keys the server returns, whether CAPTCHA is required, and whether the search enumerates many cases (good for bulk) or returns one (lookup only).

### 1.1 Search by Petitioner / Respondent name — `submit_party_name()` → `casestatus/submitPartyName`

| | |
|--|--|
| Form id | `frmsearch_name` |
| POST URL | `?p=casestatus/submitPartyName` |
| CAPTCHA | **Yes** (`fcaptcha_code`) |
| Coverage | **Bulk** — returns every case in the complex matching the name substring (case-insensitive) |
| Required params | `petres_name`, `rgyearP` (registration year, 4 digits), `case_status` ∈ {Pending, Disposed, Both}, `fcaptcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `ajax_req=true`, `app_token` |
| Validation | `petres_name` must be alphabetic + spaces only; numbers/punctuation are stripped client-side |

**Response keys:** `party_data` (HTML table), `status`, `div_captcha`, `app_token`.

**Live test:** `petres_name=KUMAR`, `case_status=Both`, `rgyearP=2024` at Mahabubnagar PDJ → **879 cases**, 300 KB of `party_data` HTML.

Each row carries Sr No, Case Type/Number/Year, Petitioner-vs-Respondent text, and a `viewHistory(case_id, cnr, court_no, '', 'CScaseNumber', state, dist, complex, 'CSpartyName')` onclick — i.e. CNR is included in the row, no enrichment call required to get it.

### 1.2 Search by Case Number — `submitCaseNo()` → `casestatus/submitCaseNo`

| | |
|--|--|
| Form id | `search_case` |
| POST URL | `?p=casestatus/submitCaseNo` |
| CAPTCHA | **Yes** (`case_captcha_code`) |
| Coverage | **Single-case lookup** — exact `case_type + number + year` |
| Required params | `case_type` (e.g. `21^3`), `search_case_no`, `case_no` (same value, sent twice), `rgyear`, `case_captcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `ajax_req=true`, `app_token` |

**Response keys:** `case_data`, `status`, `div_captcha`. Already used by the existing scraper for enrichment. Returns 0 or 1 row.

### 1.3 Search by Filing Number — `submit_filing_no()` → `casestatus/submitFillingNo`

| | |
|--|--|
| Form id | `frm_file_search_name` |
| POST URL | `?p=casestatus/submitFillingNo` (note the double-L) |
| CAPTCHA | **Yes** (`file_captcha_code`) |
| Coverage | **Single-case lookup** — by filing number |
| Required params | `case_type_1` (mandatory only when `state_code==3`, otherwise empty), `filing_no`, `filyear`, `file_captcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `case_type` (echo of `case_type_1`), `ajax_req=true`, `app_token` |

**Response keys:** `filing_data`, `status`, `div_captcha`. Live test (Gunderdehi, filyear=2024) returned 2 cases — same row format as party-name search, with the `viewHistory(...)` onclick using `'CSfilingNumber'` as the trailing flag.

### 1.4 Search by Advocate — `submit_adv_name()` → `casestatus/submitAdvName`

| | |
|--|--|
| Form id | `frm_adv_search_name` |
| POST URL | `?p=casestatus/submitAdvName` |
| CAPTCHA | **Yes** (`adv_captcha_code`) |
| Coverage | **Bulk** — every case in the complex where the advocate appears |
| Required params | `radAdvt` ∈ {1=name, 2=bar code, 3=date case list}, mode-specific subset, `case_status`, `adv_captcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `case_type=''`, `ajax_req=true`, `app_token` |

**Three sub-modes:**

- **Mode 1 (`radAdvt=1`):** `advocate_name` (text, alphabetic + space + `.` + `_`). Returns all cases where any listed advocate name matches.
- **Mode 2 (`radAdvt=2`):** `adv_bar_state` (state abbreviation), `adv_bar_code` (numeric), `adv_bar_year` (numeric). Exact bar-registration lookup.
- **Mode 3 (`radAdvt=3`):** "Date Case List" — `advocate_name` + `caselist_date` (DD-MM-YYYY). Returns cases listed for that advocate on that specific date.

**Response keys:** `adv_data`, `status`, `div_captcha`.

**Live test:** Mode 1, `advocate_name=KUMAR`, `case_status=Pending` at Mahabubnagar PDJ → **1,785 cases**, 700 KB of HTML. This is the highest-volume search axis we measured. Each row in `adv_data` has the same `viewHistory(...)` onclick with CNR.

### 1.5 Search by FIR Number — `submitFirNumber()` → `casestatus/submitFirNo`

| | |
|--|--|
| Form id | `search_fir` |
| POST URL | `?p=casestatus/submitFirNo` |
| CAPTCHA | **Yes** (`fir_captcha_code`) |
| Coverage | **Single criminal case lookup** — one (police_station, FIR_no, year) tuple |
| Required params | `police_st_code` (parsed from a `police-uniform` tuple), `uniform_code`, `fir_no`, `firyear`, `case_status` ∈ {Pending, Disposed, Both}, `fir_captcha_code`, plus standard session fields, `ajax_req=true`, `app_token` |

The police-station list comes from a separate (unauthenticated, no-CAPTCHA) call:
```
POST ?p=casestatus/fillPoliceStation
data: state_code, dist_code, court_complex_code, est_code, ajax_req=true, app_token
→ {"police_station_list": "<option value='2-33325003'>Arjunda 2</option>...", "status": 1, "app_token": ...}
```
Each option value is `police_st_code-uniform_code` (split on `-`). At Gunderdehi this returned 7 stations.

**Response keys:** `case_data`, `status`, `div_captcha`. Mostly useful for criminal-case provenance, not bulk collection.

### 1.6 Search by Act / Section — `submitAct()` → `casestatus/submitAct`

| | |
|--|--|
| Form id | `frm_act` |
| POST URL | `?p=casestatus/submitAct` |
| CAPTCHA | **Yes** (`act_captcha_code`) |
| Coverage | **Bulk** — every case in the complex filed under an act (optionally a specific section) |
| Required params | `search_act` (free-text filter), `actcode` (numeric act ID from `fillActType`), `under_sec` (optional, e.g. `302`), `case_status` ∈ {Pending, Disposed}, `act_captcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `ajax_req=true`, `app_token` |
| **Note:** | The radio buttons only expose Pending and Disposed — there is no `Both` option for Act search. |

Two-step flow. First, populate the act dropdown (no CAPTCHA):
```
POST ?p=casestatus/fillActType
data: search_act=IPC, state_code, dist_code, court_complex_code, est_code, ajax_req=true, app_token
→ {"act_list": "<option value=12>IPC CRIMINAL \</option>", "status": 1, "app_token": ...}
```

Then the CAPTCHA-gated search.

**Response keys:** `act_data`, `status`, `div_captcha`.

**Live test:** Gunderdehi, `actcode=12 (IPC)`, `under_sec=302`, `case_status=Disposed` → **3 cases**, all `Misc.Criminal Cases`, with full CNRs in the `viewHistory(...)` onclick. Sample row payload — exact text from the live response:

```html
<td>1</td>
<td>Misc.Criminal Cases/22/2026</td>
<td>CG State<br>Vs</br>Unknown</td>
<td><a onClick="viewHistory(204900000222026,'CGBL030000492026',3,'','CScaseNumber',18,19,1180084,'CSact')">View</a></td>
```

This axis is uniquely useful for *legal-research* slicing (e.g. "all cases filed under §304-A IPC") that neither the mobile API nor the order-date scraper can express.

### 1.7 Search by Case Type — `submitCaseType()` → `casestatus/submit_case_type`

| | |
|--|--|
| Form id | `frm_casetype` |
| POST URL | `?p=casestatus/submit_case_type` (underscores, not camelCase) |
| CAPTCHA | **Yes** (`ct_captcha_code`) |
| Coverage | **Bulk** — every registered case of `(case_type, year, status)` in the complex |
| Required params | `case_type_1` (e.g. `2^21`), `search_year` (4-digit registration year), `case_status` ∈ {Pending, Disposed} (no `Both`), `ct_captcha_code`, `state_code`, `dist_code`, `court_complex_code`, `est_code`, `ajax_req=true`, `app_token` |

The case-type code list comes from `casestatus/fillCaseType` (no CAPTCHA), which the existing scraper already calls. Codes are formatted `<short_id>^<establishment_id>`, e.g. `21^3` for "CRIMINAL CASE" at Gunderdehi.

**Response keys:** `case_data`, `status`, `div_captcha`.

**This is the web-portal mirror of the mobile API's `searchByCaseType.php`.** Live test at Mahabubnagar PDJ Court Complex with `case_type_1=2^21` (APPEAL SUIT), `search_year=2024`, `case_status=Pending` → **50 cases** spread across 2 establishments inside the complex, with full CNR for each row. Sample:

```html
<tr><th colspan='3' id='td_court_name_1'>District Courts, Mahabubnagar</th></tr>
<tr><td>1</td><td>AS/1/2024</td>
    <td>Smt. S. Manjula<br>Vs</br>Sudheer Goud</td>
    <td><a onClick="viewHistory(200200000012024,'TSMA000000012024',1,'','CScaseNumber',29,5,1290056,'CScaseType')">View</a></td></tr>
```

**Important — this contradicts a claim in `WEB_SCRAPER_FINDINGS.md`:** earlier docs implied `case_status` accepted single-letter codes; in practice the endpoint silently returns `"Record not found"` for `P`/`D`/`B` and only honours the full words `Pending` / `Disposed`. (The same applies to all six other tabs that have a status radio.) Once that is fixed, the existing claim that case-type search would yield ~2,323 cases at Gunderdehi is reproducible.

---

## 2. `?p=cause_list/index` — daily hearing list

| | |
|--|--|
| Form id | `frm_causelist` |
| Court list endpoint | `POST ?p=cause_list/fillCauseList` (no CAPTCHA) |
| Submit endpoint | `POST ?p=cause_list/submitCauseList` |
| CAPTCHA | **Yes** (sent as `fcaptcha_code` per JS — *not* `cause_list_captcha_code` despite the field name on the page) |
| Coverage | **Bulk per (judge, date)** — list of every case scheduled for hearing on that date in front of that judge |
| Required params | `CL_court_no` (judge code, e.g. `3^1`), `court_name_txt` (display text), `causelist_date` (DD-MM-YYYY), `cicri` ∈ {`C`=Civil, `CR`=Criminal}, `selprevdays` (0 if today/future, 1 if past), `fcaptcha_code`, plus standard session fields, `ajax_req=true`, `app_token` |
| Date constraint | JS rejects dates more than 1 month in the past or future (`checkDateInpuWithNextMonth`) |

The court-list endpoint is the closest thing to "search by judge" the portal exposes (see §4):

```
POST ?p=cause_list/fillCauseList
→ {"cause_list": "<option value='3^1'>1-Prashant Kumar Dewangan-Civil Judge Sr Div Gunderdehi</option>", ...}
```

Each option value is `<court_no>^<establishment_id>`, with display text `<court_no>-<judge_name>-<designation>`.

**Response keys:** `case_data`, `status`, `div_captcha`. Live test at Gunderdehi for `causelist_date=28-04-2026`, `cicri=C` → "Record not found" (date is in the future and that judge has no civil hearings scheduled — expected). The endpoint shape mirrors the case-type response.

---

## 3. `?p=caveat_search/index` — caveat filings

| | |
|--|--|
| Form id | `frm_caveat_search` |
| Submit endpoint | `POST ?p=caveat_search/submitCaveat` |
| CAPTCHA | **Yes** (`caveat_captcha_code`) |
| Coverage | **Multiple sub-modes** controlled by `searchcaveat` (1–5) |

The caveat form bundles five distinct search modes into one POST:

| `searchcaveat` value | Mode | Fields |
|---|---|---|
| 1 | By party name | `caveatorcaveatee` ∈ {caveator, caveatee}, `caveatorname` or `caveateename`, `rcaveator` (radio "1") |
| 2 | By case number | `case_type_caveat`, `caveat_search_case_no`, `caveat_rgyear` |
| 3 | By filing number | `filing_type`, `caveat_filing_no`, `caveat_rgyear` |
| 4 | By caveat number | `caveat_no`, `caveat_year` |
| 5 | By date of decision | **`caveat_dateofdecision_date`** (DD-MM-YYYY) |

**Response keys:** `case_data`, `status`, `div_captcha` (same shape as the casestatus tabs). Live test at Mahabubnagar with `searchcaveat=1, caveatorname=KUMAR` → "Record not found". Caveats are a niche record type (party warning before being sued); volume is much smaller than the case docket. Mode 5 is the only "search by date of decision" surface anywhere in the portal — it applies *only to caveats*, not to ordinary judgments.

---

## 4. "Search by Judge" — does not exist as a dedicated endpoint

There is **no dedicated by-judge search** in the public portal. The closest thing is the cause-list flow described in §2:

1. `cause_list/fillCauseList` returns a per-complex list of `(court_no, judge_name, designation)` tuples (e.g. `3^1` = "1-Prashant Kumar Dewangan-Civil Judge Sr Div Gunderdehi").
2. `cause_list/submitCauseList` then scopes results to that judge for one specific date.

To enumerate "all cases ever heard by judge X" you would have to iterate `cause_list/submitCauseList` over every date in the judge's tenure — an expensive workaround.

The judge name **is** captured per case as part of the existing `?p=home/viewHistory` enrichment response (the case-history table shows the bench), but the portal exposes no inverse index.

---

## 5. "Search by Date of Decision" — does not exist for ordinary cases

Searched every form on every tab. The only field carrying that semantics is `caveat_dateofdecision_date` inside `frm_caveat_search` (mode 5), which only filters caveats. For regular case dispositions, the closest substitutes are:

- `?p=courtorder/submitOrderDate` (already in use) — finds cases that received any order in a date range; *order date* is a superset of *decision date* but not identical.
- The decision date is captured per case in `viewHistory` enrichment HTML (field "Decision Date") but is not an indexable search axis.

---

## 6. `?p=villageSearch/index` — exposed in nav, returns 400

The link `<a href="/ecourtindia_v6/?p=villageSearch/index">Village Search</a>` is present in the sidebar of every search page, but the route returns `400 Invalid Parameter` regardless of:

- session state (with/without `casestatus/set_data` binding)
- referer / user-agent / `ajax_req=true`
- alternate URL paths tried: `villageSearch/show`, `village_search/index`, `villageSearch&state_code=18`, `villageSearch/villageSearch`, etc.

No JS file references a `villageSearch/*` submit endpoint either. The route appears to be **registered but disabled** in the v6 build — likely a stub for a feature ported from the older portal that was never deployed. Treat it as non-functional.

---

## 7. `?p=processDetails/processData` — single-record lookup, not searchable

Form `frmprocess` accepts only `fprocess_id` (a 16-character case-process ID known a-priori) plus a captcha. It is a barcode-style lookup for court process documents — useless for enumeration. CAPTCHA-gated. Response shape unverified (no public process IDs to test with), but irrelevant for bulk metadata harvesting.

---

## 8. Hidden / non-search routes seen but not relevant

`home/index` listed these routes in the sidebar/footer that we did **not** test because they do not enumerate cases:

`about_us/index`, `accessibility_statement/index`, `contact_us/index`, `copy_right_policy/index`, `disclaimer/index`, `forms_for_advocates/index`, `help/index`, `help_videos/index`, `high_court/index`, `home/fillLangState&lang=...`, `hyper_linking_policy/index`, `location/index` (court-finder map), `news_letter/index`, `privacy_policy/index`, `screen_reader_access/index`, `site_map/index`, `terms_and_conditions/index`, `web_info_manager/index`.

`location/index` was previously thought to expose a per-court geocoding lookup. Quick spot-check: it serves the static "find a court near you" map UI; no JSON search endpoint behind it.

---

## 9. Priority ranking — unexploited search options for bulk collection

Ordered by **(distinct cases reachable) × (cases not already obtainable via order-date search) ÷ (CAPTCHAs per call)**:

| Rank | Search axis | Why valuable | CAPTCHAs / search | Status of the existing scraper |
|------|-------------|--------------|-------------------|--------------------------------|
| **1** | **Case Type + Year + Status** (`submit_case_type`) | The only axis that returns the **complete** case docket like the mobile API (registered cases with no orders are otherwise invisible). 50 hits in one query at Mahabubnagar AS-2024. | 1 | Not used — the existing scraper only finds cases that have an order in the date range |
| **2** | **Advocate name** (`submitAdvName` mode 1) | Highest measured single-query yield (1,785 cases at one complex for "KUMAR" Pending). Useful for fast complex-coverage spot-checks and for finding cases missed by any other axis. | 1 | Not used |
| **3** | **Party name** (`submitPartyName`) | 879 hits for one query at Mahabubnagar; full CNR per row. Cheaper than enrichment when you need to map a litigant across years/courts. | 1 | Not used |
| **4** | **Act / Section** (`submitAct`) | The only axis that lets you slice by *legal subject* (e.g. all NDPS Act cases). Lower volume per query but unique research utility. | 1 (+1 for `fillActType` which is free) | Not used |
| **5** | **FIR number** (`submitFirNo`) | Forensically valuable for criminal-case provenance; small volume. | 1 (+1 free `fillPoliceStation` per complex) | Not used |
| **6** | **Cause list** (`cause_list/submitCauseList`) | Per-judge, per-date hearing scheduling — useful for *forward-looking* monitoring of upcoming hearings, not historical archive. JS limits date range to ±1 month. | 1 | Not used |
| **7** | **Caveat search** (`caveat_search/submitCaveat` modes 1–5) | Niche record type, low volume. Mode 5 (date-of-decision) is the only "decision date" search anywhere on the portal. | 1 | Not used |

**Practical recommendation:** Adding `submit_case_type` to the existing scraper closes the only known coverage gap vs. the mobile API. It costs the same per-query CAPTCHA budget as `submitOrderDate` and uses the same `viewHistory(...)` enrichment. The mobile-API IP-block fallback path the project is currently planning should default to this axis, with `submitOrderDate` retained for incremental "what's new" sweeps.

---

## 10. Endpoint reference (consolidated)

All paths are appended to `https://services.ecourts.gov.in/ecourtindia_v6/`. CAPTCHA column is `Y` only when the response is gated by a `*_captcha_code` field; "fill" endpoints (lookup helpers) are CAPTCHA-free.

| Method | Path | Purpose | CAPTCHA | JSON response shape |
|--------|------|---------|---------|---------------------|
| GET | `?p=casestatus/index` | Fetch the seven-tab search page | — | HTML |
| GET | `?p=courtorder/index` | Same purpose, different sidebar default | — | HTML |
| GET | `?p=cause_list/index` | Cause-list form page | — | HTML |
| GET | `?p=caveat_search/index` | Caveat form page | — | HTML |
| POST | `?p=casestatus/set_data` | Bind session to a complex | N | `{status, complex_code, dist_code, est_code, differ_mast_est, app_token}` |
| POST | `?p=casestatus/fillCaseType` | Case-type dropdown options | N | `{casetype_list, subordinate_court_list, status, app_token}` |
| POST | `?p=casestatus/fillActType` | Act dropdown for §1.6 | N | `{act_list, status, app_token}` |
| POST | `?p=casestatus/fillPoliceStation` | Police-station dropdown for §1.5 | N | `{police_station_list, status, app_token}` |
| POST | `?p=cause_list/fillCauseList` | Judge/court dropdown for §2 | N | `{cause_list, status, app_token}` (judge names included) |
| POST | `?p=casestatus/submitPartyName` | §1.1 | Y | `{party_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submitCaseNo` | §1.2 (already in scraper) | Y | `{case_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submitFillingNo` | §1.3 | Y | `{filing_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submitAdvName` | §1.4 | Y | `{adv_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submitFirNo` | §1.5 | Y | `{case_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submitAct` | §1.6 | Y | `{act_data, status, div_captcha, app_token}` |
| POST | `?p=casestatus/submit_case_type` | §1.7 | Y | `{case_data, status, div_captcha, app_token}` |
| POST | `?p=cause_list/submitCauseList` | §2 | Y | `{case_data, status, div_captcha, app_token}` |
| POST | `?p=caveat_search/submitCaveat` | §3 | Y | `{case_data, status, div_captcha, app_token}` |
| POST | `?p=courtorder/submitOrderDate` | Existing scraper | Y | `{court_dt_data, status, div_captcha, app_token}` |
| POST | `?p=home/viewHistory` | Per-case detail enrichment | N | HTML snippet (used by existing scraper) |
| POST | `?p=home/display_pdf` | Resolve the AES-encoded PDF locator into a real URL | N | `{order: "/path/to.pdf"}` |
| GET | `?p=villageSearch/index` | **Returns 400 — non-functional** | — | — |
| GET | `?p=processDetails/processData` | Process-ID lookup (irrelevant) | Y | unverified |

`status=0` from any submit endpoint means CAPTCHA failure; `status=1` with a `<div id='nodata'>Record not found</div>` body is "valid query, zero hits"; `status=1` with `Total number of cases :  N` means N hits.

---

## 11. Raw response samples

### 11.1 `submit_case_type` — Mahabubnagar PDJ, AS/2024/Pending

POST `?p=casestatus/submit_case_type` with `case_type_1=2^21, search_year=2024, case_status=Pending`. Truncated `case_data`:

```html
<h3>Total Number of Establishments in Court Complex :  6 | Total number of cases : 50</h3>
<ul><li>District Courts, Mahabubnagar:&nbsp;18</li>
    <li>X court cum Spl Sessions Judge for attrocities against women, Mahabubnagar:&nbsp;32</li></ul>
<table id='dispTable'>
  <tr><th colspan='3' id='td_court_name_1'>District Courts, Mahabubnagar</th></tr>
  <tr><td>1</td>
      <td>AS/1/2024</td>
      <td>Smt. S. Manjula<br>Vs</br>Sudheer Goud</td>
      <td><a onClick="viewHistory(200200000012024,'TSMA000000012024',1,'','CScaseNumber',29,5,1290056,'CScaseType')">View</a></td></tr>
  <tr><td>2</td>
      <td>AS/6/2024</td>
      <td>Ganji Ramanjamma<br>Vs</br>Sulohana</td>
      <td><a onClick="viewHistory(200200000062024,'TSMA000001212024',1,'','CScaseNumber',29,5,1290056,'CScaseType')">View</a></td></tr>
  ... (48 more rows)
</table>
```

The 9 args of `viewHistory` are: `(case_id, CNR, court_no, '', source_flag='CScaseNumber', state_code, dist_code, complex_code, originating_tab)`. CNR is in the second position — usable directly without an extra enrichment hop.

### 11.2 `submitAct` — Gunderdehi, IPC §302, Disposed

POST `?p=casestatus/submitAct` with `actcode=12, under_sec=302, case_status=Disposed`. Full `act_data`:

```html
<h3>Total Number of Establishments in Court Complex :  1 | Total number of cases : 3</h3>
<ul><li>Civil Court Gunderdehi, Balod:&nbsp;3</li></ul>
<table id='dispTable'>
  <tr><th colspan='3' id='td_court_name_3'>Civil Court Gunderdehi, Balod</th></tr>
  <tr><td>1</td><td>Misc.Criminal Cases/22/2026</td>
      <td>CG State<br>Vs</br>Unknown</td>
      <td><a onClick="viewHistory(204900000222026,'CGBL030000492026',3,'','CScaseNumber',18,19,1180084,'CSact')">View</a></td></tr>
  <tr><td>2</td><td>Misc.Criminal Cases/9/2026</td>
      <td>CG State<br>Vs</br>unknown</td>
      <td><a onClick="viewHistory(204900000092026,'CGBL030000132026',3,'','CScaseNumber',18,19,1180084,'CSact')">View</a></td></tr>
  <tr><td>3</td><td>Misc.Criminal Cases/5/2026</td>
      <td>CG State<br>Vs</br>Unknown</td>
      <td><a onClick="viewHistory(204900000052026,'CGBL030000092026',3,'','CScaseNumber',18,19,1180084,'CSact')">View</a></td></tr>
</table>
```

Note the trailing `'CSact'` arg in `viewHistory` — that string changes per originating tab (`'CScaseNumber'`, `'CSpartyName'`, `'CSadvocate'`, `'CSact'`, `'CSfirNumber'`, `'CSfilingNumber'`, `'CScaseType'`, `'CScauseList'`, `'CScaveat'`). It is informational only; the enrichment endpoint accepts any of these.

### 11.3 `submitOrderDate` — Gunderdehi, 01-04-2026 to 15-04-2026 (existing scraper axis)

For comparison, the response from the search the existing scraper uses. Truncated `court_dt_data`:

```html
<h3>Total Number of Establishments in Court Complex :  1 | Total number of cases : 10</h3>
<table>
  <tr><td>1</td>
      <td>CRIMINAL CASE/918/2025</td>
      <td>CG State<br>Vs</br>CHATURBHUJ DHIMAR</td>
      <td>13-04-2026</td>
      <td><a onclick="displayPdf('cl+sGJoSJ58oyVespa8VHg==','U8BskvqePV/p3Zp4pt2ppIK1n6asdIt1dKi3auMLEoA=','5rHUl/fJ8k9+dheUWTi6OQ==','SgyEAcaqCziTeSAURi3TUxNZSIcRfGv2jA5C+gRrg3bDKQX2rXFGE/jix9CQ4KIx','')">Copy of Judgment</a></td></tr>
  ... (9 more)
</table>
```

Order date is in column 4. The four base64 args of `displayPdf` are AES-CBC-encrypted PDF locator params; resolving them requires `?p=home/display_pdf` (which the existing scraper does). The new search axes (§§1.1, 1.2, 1.3, 1.4, 1.6, 1.7) emit `viewHistory(...)` instead of `displayPdf(...)`, so they don't directly hand back a PDF — you go via `viewHistory` first, then the case-history HTML lists every order with its own `displayPdf` call.

---

## 12. Open questions / things still to verify

- **`submitAdvName` mode-3 ("Date Case List"):** server returned 500 on every parameter set we attempted (with and without `caselist_date`, with and without bar fields). The form submits via the same handler as modes 1/2 — likely needs an extra session-state field we haven't replicated. Out of scope for this report; tracked here for completeness.
- **Pagination:** none of the case-list responses showed any pagination markers in the HTML we retrieved. `submit_case_type` returned 50 rows in a single response at Mahabubnagar; `submitAdvName` returned 1,785 rows in one shot (700 KB). No `LIMIT` truncation was observed up to those volumes. If a complex has >10k matches in one query the backend behaviour is unverified.
- **Rate limits:** session 405 responses (per existing scraper notes) trigger 30s back-off. We did not stress-test these new endpoints; assume the same quota.
- **Multi-establishment complexes:** when a complex contains multiple establishments (Mahabubnagar has 6), every search axis returns rows from *all* establishments grouped by `td_court_name_<establishment_id>`. There's no per-establishment filter except `est_code`, which the existing scraper sends as empty.
