#!/bin/bash
# Wrapper script that auto-restarts the scraper and logs everything
LOG_FILE="${HOME}/scraper.log"
cd "$(dirname "$0")"

while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting scraper..." >> "$LOG_FILE"
  uv run python scraper.py 2>&1 | tee -a "$LOG_FILE"
  EXIT_CODE=$?
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
