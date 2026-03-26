---
date: 2026-03-26
topic: scraper-operator-skill
---

# Autonomous Scraper Operator Skill

## What We're Building

A Claude Code skill that runs periodically via `/loop` on the same server as the scraper. It acts as a fully autonomous operator — monitoring health, debugging failures, ensuring data quality, and optimizing scraping speed. It does everything a human operator would do: check logs, investigate errors, fix bugs, tune parameters, and keep the scraper running reliably across all ~3,500 court complexes in India.

## Why This Approach

Manual monitoring doesn't scale for a scrape that will run for weeks/months across all of India. The operator skill runs as a loop, checking in every N minutes, and escalates only when it truly can't resolve something. It makes local code changes but does not push to remote — the user reviews and deploys.

## Key Decisions

### Process Management
- **Scraper runs via `run_scraper.sh`** in a named tmux session (`scraper`)
- **Logs to `~/scraper.log`** with the wrapper script handling rotation
- **PID tracked** via `~/.scraper.pid` for process health checks
- **AWS profile `dattam-od`** set in the wrapper for S3 access

### Skill Modes

The skill operates in three modes each cycle:

#### 1. Monitor (every cycle)
- Tail `~/scraper.log` (last 500 lines)
- Parse failure rates: connection errors, API failures, PDF failures
- Check process is alive (`tmux has-session -t scraper`)
- Query S3 for progress delta since last check (store last-known counts locally)
- Check disk space (tar files accumulate locally before flush)
- Report: progress rate (cases/hour), failure rate %, estimated completion

#### 2. Debug (triggered by anomalies)
Anomaly triggers:
- Failure rate > 10%
- Zero progress for > 30 minutes
- Scraper process died
- Repeated same error pattern (>50 occurrences of identical error)

Debug actions:
- Classify errors: connection resets, timeouts, auth failures, parse errors
- **Determine if errors are load-related or network-related:**
  - Test same endpoint with single request at different times
  - Compare error rates across different districts/states
  - Check if reducing workers helps (suggests load) or not (suggests network)
  - Use headless browser (Playwright) to test the eCourts website directly
- Trace root cause through code
- Fix bugs if identified, commit locally
- Restart scraper if needed

#### 3. Optimize (periodic deep analysis)
Run less frequently (every ~1 hour or when monitor shows opportunity).

**Error source investigation (first priority):**
- Correlate connection errors with: time of day, specific districts, worker count
- Run controlled experiments: same district with 1 worker vs 10 workers
- Test if different states have different server backends (DNS resolution, response headers)
- Build a profile of which districts/states are more reliable

**Speed optimizations (after error understanding):**
- Adaptive delay: reduce delay when errors are low, increase when high
- Adaptive workers: tune per-district based on observed error rates
- Cross-district parallelism: if states use different servers, parallelize across states
- Skip empty court complexes faster (some may have 0 cases for certain years)
- Batch S3 operations where possible
- Session reuse optimization in requests

**Data quality checks:**
- Sample PDFs from S3: verify they're valid PDFs (not error pages)
- Check compression ratios — flag if Ghostscript isn't reducing size
- Verify metadata JSON is well-formed
- Check for duplicates across partitions
- Verify file counts in index match actual archive contents

### Headless Browser Access
- Use Playwright in headless mode (`chromium --headless`) for:
  - Testing eCourts website directly when API is failing
  - Checking if specific district court pages are down
  - Understanding server behavior and response patterns
  - Verifying that the mobile API and web interface serve same data
- Installed as a dependency on the scraper server

### State Persistence
The skill needs to track state across loop cycles:
- `~/.scraper-operator/last_check.json` — timestamp, last known stats
- `~/.scraper-operator/error_log.json` — error pattern history
- `~/.scraper-operator/experiments.json` — results of optimization experiments
- `~/.scraper-operator/district_profiles.json` — per-district reliability scores

### Boundaries
- **Does:** Monitor, debug, fix code, optimize, restart process
- **Does not:** Push to remote, modify S3 data directly, change AWS credentials
- **Escalates to user when:** Fundamental architecture change needed, persistent failures it can't diagnose, or when an optimization experiment needs approval before applying

## Open Questions

1. **Loop interval**: 5 minutes for monitoring seems right, but optimization analysis should run less often. Should the skill self-regulate frequency based on scraper state (healthy = less frequent, troubled = more frequent)?
2. **Log rotation**: Should we use `logrotate` or a simple size-based truncation in the wrapper script?
3. **Playwright installation**: Need to ensure `npx playwright install chromium` is run on the server. Should the skill handle this itself on first run?

## Next Steps
→ Create the SKILL.md file with full operator instructions
→ Update `run_scraper.sh` with PID tracking and tmux integration
→ Create the state persistence directory structure
→ Implement and test with `/loop 5m /scraper-operator`
