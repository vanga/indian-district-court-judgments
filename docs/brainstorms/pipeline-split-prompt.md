Read the following files to understand the current scraper architecture:
- mobile/scraper.py
- mobile/api_client.py
- archive_manager.py
- AGENTS.md

Then write a requirements document to docs/brainstorms/2026-03-26-pipeline-split-requirements.md capturing these decisions from a brainstorm session:

## Context

The current scraper runs search → history → PDF download as a single monolithic pipeline per case. This means PDF downloads (the slowest step) block the search/history stages from progressing. We want to split this into two independent stages for throughput.

## Key Decisions

1. **Two-stage pipeline, not three**:
   - Stage 1 (Metadata): Search cases + fetch history → write metadata.tar (no PDF downloads)
   - Stage 2 (PDF Download): Read metadata.tar from S3 → extract pdf_filename from each case JSON → use download_pdf_direct() to download with a fresh session → write data.tar

2. **No separate search results store needed**: The metadata JSON already contains all search result fields (case_no, cino, case_type, court_code, etc.) plus history. It serves as the work queue for the PDF stage.

3. **No auth/crypto constraint forcing current ordering**: Search results are plain identifiers. PDF filenames can be extracted from metadata via decrypt_url_param() and re-downloaded using build_pdf_url()/download_pdf_direct() with a fresh JWT session. The stages are fully independent.

4. **Rename archive type from "orders" to "data"**: For consistency with the HC and SC repos which use data.tar. The archive_manager is generic — archive_type is just a string that becomes the tar/index filename. Migration of existing S3 keys (124 tar files) needed as a one-time step.

5. **Existing archive design supports this split**: metadata.tar and orders.tar (→ data.tar) are already independent archives with independent indexes. Each stage writes to its own archive type, no conflicts.

6. **Primary goal is throughput**: The metadata stage can sweep through all 3,567 court complexes without waiting for PDF downloads. The PDF stage runs independently, potentially on a different machine or with different concurrency settings.

## Deferred to Planning
- Concrete queue/state tracking mechanism for PDF stage (how does it know which PDFs from metadata still need downloading?)
- Whether to use SQLite, local files, or just S3 index diffing for tracking
- Concurrency model for each stage (workers, delays)
- S3 key migration strategy for orders→data rename
- Whether stages can run on separate machines
