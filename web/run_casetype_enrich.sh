#!/bin/bash
# Phase 2+3 enrichment wrapper — CAPTCHA-free viewHistory + PDF download.
# Start with: tmux new-session -d -s web-enrich 'web/run_casetype_enrich.sh'
#
# Runs at 8 workers (no CAPTCHA bottleneck). Safe to run alongside Phase 1.
#
# Usage:
#   web/run_casetype_enrich.sh                        # all courts with local data
#   web/run_casetype_enrich.sh --state_code 29        # Telangana only
#   web/run_casetype_enrich.sh --metadata_only        # enrich without downloading PDFs

LOG_FILE="${HOME}/web_enrich.log"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"  # repo root

export AWS_PROFILE="${AWS_PROFILE:-dattam-od}"

cd "$SCRIPT_DIR"

rotate_log() {
  local size
  size=$(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null || echo 0)
  if [ "$size" -gt 104857600 ]; then
    tail -n 50000 "$LOG_FILE" > "${LOG_FILE}.tmp"
    mv "${LOG_FILE}.tmp" "$LOG_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Log rotated (was ${size} bytes)" >> "$LOG_FILE"
  fi
}

cleanup() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Enrich wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

EXTRA_ARGS="$*"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting enrichment pass..." >> "$LOG_FILE"

  uv run --python 3.13 python web/casetype_enrich.py \
    --max_workers 8 \
    --courts_csv courts.csv \
    $EXTRA_ARGS \
    2>&1 | tee -a "$LOG_FILE"

  EXIT_CODE=${PIPESTATUS[0]}

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Enrichment pass done (code $EXIT_CODE), restarting in 60s..." >> "$LOG_FILE"
  # Longer sleep — if there's nothing to enrich yet, wait for Phase 1 to produce data
  sleep 60
done
