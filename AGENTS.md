# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## What This Project Does

Scrapes and archives Indian District Court judgments from eCourts (services.ecourts.gov.in). Two scrapers exist: a **mobile API scraper** (recommended, no CAPTCHA) and a **web scraper** (requires CAPTCHA solving via ML models).

## Development Commands

```bash
# Mobile scraper (main scraper) — run from repo root
cd mobile && uv sync
uv run python scraper.py --state 29 --district 22 --start-year 2020 --end-year 2025

# Dry run (no S3 upload)
uv run python scraper.py --state 29 --district 22 --local-only

# Web scraper — run from repo root
uv sync
uv run python web/download.py

# Lint
uv run ruff check .
```

There are no automated tests.

## Architecture

### Two Independent Scrapers

- **`mobile/`** — Uses the eCourts Android app API. All requests/responses are AES-CBC encrypted (`crypto.py`). The `api_client.py` handles encryption, auth (JWT), and all API calls. `scraper.py` orchestrates: iterates court hierarchy → searches cases by type/year → downloads PDFs → archives to S3.
- **`web/`** — Uses the eCourts website. Requires CAPTCHA solving via ONNX/PyTorch models in `web/src/`. `download.py` is the entry point.

### Shared Components

- **`archive_manager.py`** (repo root) — Both scrapers use this for TAR archive creation, S3 uploads, and index tracking. Archives are partitioned at 1GB. Imported by mobile scraper via `sys.path` manipulation.
- **`gs.py`** (repo root) — PDF compression via Ghostscript. Used by both scrapers.
- **`courts.csv`** — All 3,567 court complexes with state/district/complex codes.

### Reference Materials

- **`reference/traffic/`** — Captured HTTP traffic, Postman collection, and OpenAPI spec from reverse engineering the mobile API.

### Mobile API Encryption Flow

`crypto.py` has two hardcoded AES keys (from APK reverse engineering):
- `REQUEST_KEY` — encrypts client→server params
- `RESPONSE_KEY` — decrypts server→client responses

Request params format: `16 hex random_iv + 1 digit global_index + base64(ciphertext)`. PDF URL params use a different format: `32 hex IV + base64(ciphertext)`.

### S3 Data Layout

```
s3://bucket/data/tar/year=YYYY/state=XX/district=YY/complex=ZZ/orders.tar
s3://bucket/metadata/tar/.../metadata.tar
s3://bucket/metadata/parquet/year=YYYY/state=XX/metadata.parquet
```

Index files (`*.index.json`) track which files are in each archive for resume capability.

### Key State/District Codes

Telangana=29 (Mancherial=22, Rangareddy=6, Hyderabad=5). Full list in `courts.csv`.

## Important Conventions

- Python 3.13+ required. Uses `uv` for dependency management.
- Mobile scraper has its own `pyproject.toml` and venv; web scraper uses the root `pyproject.toml`.
- All timestamps use IST (UTC+5:30).
- SSL verification is disabled for eCourts API (`verify=False`).
- The mobile API has rate limiting — always include delays between requests (default 0.3s).
- Archives use uncompressed TAR format for speed.
