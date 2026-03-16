# Indian District Court Judgments

A scraper for Indian District Court judgments from [services.ecourts.gov.in](https://services.ecourts.gov.in).

## Overview

This project scrapes court orders and judgments from all District Courts across India. The eCourts system covers:

- **36 States/UTs**
- **~700+ Districts**
- **3,567 Court Complexes**

Data is organized by state, district, and court complex, with support for historical records dating back to 1950.

## Data Structure

**Local storage** (when running locally):

```
local_dc_judgments_data/
└── {year}/{state_code}/{district_code}/{complex_code}/
    ├── orders.tar              # PDF judgments
    ├── orders.index.json       # Archive index
    ├── metadata.tar            # Case metadata (JSON)
    └── metadata.index.json     # Metadata index
```

**S3 storage** (when using --sync-s3):

```
s3://indian-district-court-judgments-test/
├── data/
│   └── tar/
│       └── year={YYYY}/state={code}/district={code}/complex={code}/
│           ├── orders.tar           # PDF judgments (uncompressed TAR)
│           └── orders.index.json    # Archive index (V2 format)
└── metadata/
    ├── tar/
    │   └── year={YYYY}/state={code}/district={code}/complex={code}/
    │       ├── metadata.tar         # Case metadata JSON files
    │       └── metadata.index.json  # Metadata archive index
    └── parquet/
        └── year={YYYY}/state={code}/
            └── metadata.parquet     # Aggregated metadata for analytics
```

**Index File Format (V2):**

Each archive has an accompanying `.index.json` file:

```json
{
  "year": 2025,
  "state_code": "29",
  "district_code": "22",
  "complex_code": "1290148",
  "archive_type": "orders",
  "file_count": 578,
  "total_size": 85000000,
  "total_size_human": "81.01 MB",
  "created_at": "2025-01-14T19:25:47+05:30",
  "updated_at": "2025-01-14T19:25:47+05:30",
  "parts": [
    {
      "name": "orders.tar",
      "files": ["MVOP_63_2021.pdf", "CRL_A_123_2022.pdf", ...],
      "file_count": 578,
      "size": 85000000,
      "size_human": "81.01 MB",
      "created_at": "2025-01-14T19:25:47+05:30"
    }
  ]
}
```

**Multi-part Archives:**

When archives exceed 1GB, they are automatically split into parts:

- First part: `orders.tar`
- Subsequent parts: `part-20250114T192547.tar`, etc.

The index file tracks all parts and their contents.

## Installation

Requires Python 3.13+ and [uv](https://github.com/astral-sh/uv) package manager.

```bash
# Clone the repository
git clone https://github.com/vanga/indian-district-court-judgments-test.git
cd indian-district-court-judgments-test

# Install dependencies
uv sync
```

## Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Download judgments for Delhi (state_code=26) for Jan 1-3, 2025
uv run python download.py --state_code 26 --start_date 2025-01-01 --end_date 2025-01-03

# Output will be in: local_dc_judgments_data/2025/26/{district}/{complex}/
```

## Usage

### Step 1: Scrape Court Hierarchy (Optional)

The repository includes a pre-generated `courts.csv` with all 3,567 court complexes. To regenerate:

```bash
# Scrape all states (~6 minutes)
uv run python scrape_courts.py

# Or scrape a specific state (e.g., Telangana = 29)
uv run python scrape_courts.py --state 29
```

This creates `courts.csv` with columns:

- `state_code`, `state_name`
- `district_code`, `district_name`
- `complex_code`, `complex_name`
- `court_numbers`, `flag`

### Step 2: Download Judgments

**Local download mode** (saves to `local_dc_judgments_data/`):

```bash
# Download for a date range (all 3,567 courts - use with caution!)
uv run python download.py --start_date 2025-01-01 --end_date 2025-01-03

# Download for a specific state (recommended)
uv run python download.py --state_code 26 --start_date 2025-01-01 --end_date 2025-01-03

# Download for a specific district
uv run python download.py --state_code 29 --district_code 22 --start_date 2025-01-01 --end_date 2025-01-03

# Download for a specific court complex
uv run python download.py --state_code 29 --district_code 22 --complex_code 1290148 --start_date 2025-01-01 --end_date 2025-01-03

# Increase parallelism for faster downloads
uv run python download.py --state_code 26 --start_date 2025-01-01 --end_date 2025-01-03 --max_workers 10
```

**View downloaded files:**

```bash
# List all archives
find local_dc_judgments_data -name "*.tar"

# Extract and view a PDF
tar -xf local_dc_judgments_data/2025/26/1/260001/orders.tar -C /tmp/
open /tmp/*.pdf  # macOS
```

**S3 sync mode (incremental updates):**

```bash
uv run python download.py --sync-s3
```

**S3 fill mode (historical backfill):**

```bash
# Processes data in 5-year chunks, automatically resuming
uv run python download.py --sync-s3-fill --timeout-hours 5.5
```

### Step 3: Generate Parquet Files (Optional)

After uploading metadata to S3, generate Parquet files for analytics:

```bash
# Process all metadata in S3 bucket
uv run python process_metadata.py

# Process specific year
uv run python process_metadata.py --year 2025

# Process specific state
uv run python process_metadata.py --state 29

# Process specific year and state
uv run python process_metadata.py --year 2025 --state 29
```

This reads metadata TAR files from S3 and generates Parquet files at:
`metadata/parquet/year={YYYY}/state={code}/metadata.parquet`

### Command Line Options

| Option            | Description                                           |
| ----------------- | ----------------------------------------------------- |
| `--start_date`    | Start date in YYYY-MM-DD format                       |
| `--end_date`      | End date in YYYY-MM-DD format                         |
| `--day_step`      | Days per chunk (default: 1)                           |
| `--max_workers`   | Parallel workers (default: 2, to avoid rate limiting) |
| `--state_code`    | Filter by state code                                  |
| `--district_code` | Filter by district code                               |
| `--complex_code`  | Filter by complex code                                |
| `--courts_csv`    | Path to courts.csv (default: courts.csv)              |
| `--sync-s3`       | Enable S3 sync mode                                   |
| `--sync-s3-fill`  | Enable historical backfill mode                       |
| `--timeout-hours` | Max runtime before graceful exit (default: 5.5)       |

## State Codes

All state/UT codes (from `courts.csv`):

| Code | State             | Code | State                          |
| ---- | ----------------- | ---- | ------------------------------ |
| 1    | Maharashtra       | 20   | Tripura                        |
| 2    | Andhra Pradesh    | 21   | Meghalaya                      |
| 3    | Karnataka         | 22   | Punjab                         |
| 4    | Kerala            | 23   | Madhya Pradesh                 |
| 5    | Himachal Pradesh  | 24   | Sikkim                         |
| 6    | Assam             | 25   | Manipur                        |
| 7    | Jharkhand         | 26   | Delhi                          |
| 8    | Bihar             | 27   | Chandigarh                     |
| 9    | Rajasthan         | 28   | Andaman and Nicobar            |
| 10   | Tamil Nadu        | 29   | Telangana                      |
| 11   | Odisha            | 30   | Goa                            |
| 12   | Jammu and Kashmir | 33   | Ladakh                         |
| 13   | Uttar Pradesh     | 34   | Nagaland                       |
| 14   | Haryana           | 35   | Puducherry                     |
| 15   | Uttarakhand       | 36   | Arunachal Pradesh              |
| 16   | West Bengal       | 37   | Lakshadweep                    |
| 17   | Gujarat           | 38   | Dadra Nagar Haveli & Daman Diu |
| 18   | Chhattisgarh      |      |                                |
| 19   | Mizoram           |      |                                |

## Architecture

```
indian-district-court-judgments-test/
├── pyproject.toml          # Dependencies
├── courts.csv              # Court hierarchy (generated)
├── scrape_courts.py        # Court hierarchy scraper
├── download.py             # Main judgment downloader
├── archive_manager.py      # TAR archive & S3 management
├── process_metadata.py     # Metadata to Parquet conversion
├── sync_s3.py              # Incremental S3 sync
├── sync_s3_fill.py         # Historical backfill
└── src/
    ├── captcha_solver/     # ONNX-based CAPTCHA solver
    │   ├── main.py
    │   ├── tokenizer_base.py
    │   └── captcha.onnx
    └── utils/
        ├── court_utils.py  # Court data structures
        └── html_utils.py   # HTML parsing utilities
```

## API Endpoints

The scraper interacts with the following eCourts API endpoints:

| Endpoint                        | Purpose                            |
| ------------------------------- | ---------------------------------- |
| `?p=casestatus/fillDistrict`    | Get districts for a state          |
| `?p=casestatus/fillcomplex`     | Get court complexes for a district |
| `?p=casestatus/set_data`        | Set session court context          |
| `?p=courtorder/submitOrderDate` | Search orders by date              |
| `?p=home/display_pdf`           | Get PDF download URL               |

## Metadata Fields

Each judgment record includes:

- **Case Information**: Case number, type, parties
- **Court Details**: State, district, complex, court number
- **Dates**: Order date, scraped timestamp
- **Document**: PDF judgment/order

## Dependencies

- `requests` - HTTP client
- `beautifulsoup4` + `lxml` - HTML parsing
- `boto3` - AWS S3 integration
- `pandas` + `pyarrow` - Data processing
- `onnx` + `onnxruntime` - CAPTCHA solving
- `torch` + `torchvision` + `pillow` - Image processing
- `tqdm` - Progress bars
- `colorlog` - Colored logging

## Related Projects

- [indian-supreme-court-judgments](https://github.com/vanga/indian-supreme-court-judgments) - Supreme Court of India
- [indian-high-court-judgments](https://github.com/vanga/indian-high-court-judgments) - High Courts of India

## License

MIT License

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
