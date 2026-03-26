# Pipeline Split: Two-Stage Metadata + PDF Download

**Date:** 2026-03-26
**Status:** Requirements (pre-planning)

## Problem

The current mobile scraper (`mobile/scraper.py`) runs search → history → PDF download as a single monolithic pipeline per case in `scrape_case()` (lines 497–566). PDF downloads — the slowest step, with retries, backoff, and compression — block the search/history stages from progressing through the remaining 3,567 court complexes.

## Decision: Two-Stage Pipeline

Split into two fully independent stages:

### Stage 1: Metadata

Search cases + fetch history → write `metadata.tar`. No PDF downloads.

This is the existing flow up to line 545 of `scraper.py` — call `search_cases_by_type()`, `get_case_history()`, `get_orders_from_history()`, build metadata JSON via `_build_case_metadata()`, and archive it. The PDF download loop (lines 548–551) is removed from this stage entirely.

The metadata JSON already contains everything needed for Stage 2:
- `case_summary.case_no`, `case_summary.cino`, `case_summary.court_code` — case identifiers
- `location.state_code`, `location.district_code`, `location.complex_code` — S3 path components
- `orders.final_orders[].pdf_filename`, `orders.interim_orders[].pdf_filename` — extracted via `_extract_pdf_filename()` → `decrypt_url_param()` at metadata build time

### Stage 2: PDF Download

Read `metadata.tar` from S3 → for each case JSON, extract `pdf_filename` from orders → call `download_pdf_direct()` with a fresh JWT session → compress → write `data.tar`.

Key technical details:
- `download_pdf_direct()` (`api_client.py:719`) re-encrypts params from scratch using the filename, case_no, court_code, state/district codes. It does **not** depend on the original session's encrypted URL.
- A fresh `MobileAPIClient().initialize_session()` provides a new JWT. The stages share no auth state.
- PDF filenames are plain strings (e.g., `/orders/2025/205400023292025_2.pdf`). No crypto dependency between stages.

## Decision: No Separate Search Results Store

The metadata JSON is the work queue. It already contains all search result fields plus history. There is no need for an intermediate search-results-only format.

## Decision: Rename Archive Type "orders" → "data"

For consistency with the HC and SC repos which use `data.tar`.

The `archive_manager.py` is generic — `archive_type` is just a string that becomes the tar filename and index filename. Changing from `"orders"` to `"data"` in `_download_pdf_with_retry()` (line 391, 467) is a one-line change per call site.

**Migration needed:** 124 existing tar files in S3 at paths like `.../orders.tar` and `.../orders.index.json` need renaming to `.../data.tar` and `.../data.index.json`. One-time S3 copy + delete operation.

## Decision: Existing Archive Design Supports This

`metadata.tar` and `orders.tar` (→ `data.tar`) are already independent archives with independent indexes in `archive_manager.py`. Each stage writes to its own `archive_type`, no conflicts. The `file_exists()` check uses `(year, state_code, district_code, complex_code, archive_type, filename)` as the key — completely independent per archive type.

## Primary Goal: Throughput

- Stage 1 can sweep all 3,567 court complexes without waiting for PDF downloads.
- Stage 2 runs independently — potentially on a different machine, with different concurrency settings, or with higher parallelism since PDF downloads are I/O-bound and don't share the search API's rate limits.
- The metadata stage is CPU/network-light (small JSON responses). The PDF stage is network-heavy (large binary downloads). Separating them allows tuning each independently.

## Deferred to Planning

These decisions are explicitly **not** made yet:

- **Queue/state tracking for Stage 2:** How does the PDF stage know which PDFs from metadata still need downloading? Options: S3 index diffing (metadata index vs data index), SQLite tracking DB, or local checkpoint files.
- **Tracking mechanism:** Whether to use SQLite, local files, or just S3 index diffing.
- **Concurrency model:** Worker count, delay settings, and thread pool sizing for each stage independently.
- **S3 key migration strategy:** Scripted S3 copy+delete vs. in-place rename for the 124 existing `orders.*` files.
- **Multi-machine operation:** Whether and how the stages can run on separate machines (shared S3 is the only coupling point).
