# Indian District Court Judgments Scraper

This project scrapes and archives Indian District Court judgments from eCourts.

## Project Structure

```
.
├── mobile/                  # MAIN SCRAPER - Mobile API (no CAPTCHA)
│   ├── scraper.py           # Main entry point for scraping
│   ├── api_client.py        # Mobile API client with encryption
│   └── crypto.py            # AES encryption/decryption
├── web/                     # Web scraper (requires CAPTCHA)
│   ├── download.py          # Web scraper entry point
│   ├── sync_s3.py           # S3 sync module
│   ├── sync_s3_fill.py      # Gap-filling for historical data
│   ├── process_metadata.py  # Parquet generation
│   ├── scrape_courts.py     # Court hierarchy scraper
│   └── src/                 # CAPTCHA solver and utilities
├── archive_manager.py       # S3 archive management (shared)
└── courts.csv               # All 3,567 court complexes
```

## Mobile Scraper (Recommended)

The mobile scraper uses the eCourts mobile app API which **requires NO CAPTCHA**.

### Features

- No CAPTCHA required (major advantage over web scraper)
- Downloads PDFs directly
- Syncs to S3 using archive_manager
- Robust retry logic with exponential backoff
- Resume capability via S3 index files

### Usage

```bash
cd mobile

# Install dependencies
uv sync

# Scrape specific district (Telangana -> Mancherial, 1950-2025)
uv run python scraper.py --state 29 --district 22 --start-year 1950 --end-year 2025

# Dry run (local only, no S3 upload)
uv run python scraper.py --state 29 --district 22 --start-year 2020 --end-year 2025 --local-only

# Scrape only disposed cases
uv run python scraper.py --state 29 --district 22 --filter Disposed

# Custom S3 bucket
uv run python scraper.py --state 29 --s3-bucket my-bucket
```

### Command Line Arguments

| Argument         | Description                          | Default                               |
| ---------------- | ------------------------------------ | ------------------------------------- |
| `--state`        | State code (can repeat)              | All states                            |
| `--district`     | District code (can repeat)           | All districts                         |
| `--complex`      | Complex code (can repeat)            | All complexes                         |
| `--start-year`   | Start year (inclusive)               | 2020                                  |
| `--end-year`     | End year (inclusive)                 | 2025                                  |
| `--filter`       | Pending/Disposed/Both                | Both                                  |
| `--delay`        | Delay between API calls (seconds)    | 0.3                                   |
| `--local-only`   | Don't upload to S3                   | False                                 |
| `--s3-bucket`    | S3 bucket name                       | indian-district-court-judgments-test  |

### State/District Codes

- **Telangana**: State code `29`
  - **Mancherial**: District code `22`
  - **Rangareddy**: District code `6`
  - **Hyderabad**: District code `5`

## S3 Structure

All archives use uncompressed TAR format (`.tar`).

```
indian-district-court-judgments-test/
├── data/
│   └── tar/
│       └── year=YYYY/state=XX/district=YY/complex=ZZ/
│           ├── orders.tar
│           └── orders.index.json
└── metadata/
    ├── tar/
    │   └── year=YYYY/state=XX/district=YY/complex=ZZ/
    │       ├── metadata.tar
    │       └── metadata.index.json
    └── parquet/
        └── year=YYYY/state=XX/
            └── metadata.parquet
```

## Mobile API Architecture

### Encryption

- Uses AES-CBC encryption for all request/response data
- Two encryption keys (from APK analysis):
  - `REQUEST_KEY`: `4D6251655468576D5A7134743677397A` (client -> server)
  - `RESPONSE_KEY`: `3273357638782F413F4428472B4B6250` (server -> client)

### Request Format

- Params: `16 hex random_iv + 1 digit global_index + base64(ciphertext)`
- Server format (PDF URLs): `32 hex IV + base64(ciphertext)`

### Authentication

- JWT token from `appReleaseWebService.php`
- Token encrypted and sent in `Authorization: Bearer <encrypted_token>` header
- PDF URLs contain session-bound `params` and `authtoken` that must be used directly

### API Endpoints

| Endpoint                    | Purpose                    |
| --------------------------- | -------------------------- |
| `appReleaseWebService.php`  | Initialize session, get JWT |
| `stateWebService.php`       | Get list of states         |
| `districtWebService.php`    | Get districts for a state  |
| `courtEstWebService.php`    | Get court complexes        |
| `caseNumberWebService.php`  | Get case types             |
| `searchByCaseType.php`      | Search cases by type/year  |
| `caseHistoryWebService.php` | Get case history with PDFs |
| `display_pdf.php`           | Download PDF               |

## Archive Manager

Shared component (`archive_manager.py`) for both scrapers:

- TAR archive creation with size-based partitioning (1GB max)
- Index file tracking (V2 format with parts array)
- Immediate upload mode for crash recovery
- File existence checking against S3
- 3-level hierarchy: state/district/complex

## Court Hierarchy

- **36 States/UTs**
- **~700+ Districts**
- **3,567 Court Complexes**

Data in `courts.csv` with columns:
- `state_code`, `state_name`
- `district_code`, `district_name`
- `complex_code`, `complex_name`
- `court_numbers`, `flag`

## Notes

- All archives use uncompressed TAR format for faster read/write
- IST timezone (UTC+5:30) used for timestamps
- Retry logic with exponential backoff (5 retries max)
- 1GB maximum archive size before splitting into parts
- Mobile API has rate limiting - use appropriate delay between requests
