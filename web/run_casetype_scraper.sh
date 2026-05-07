#!/bin/bash
# Case-type web scraper wrapper — auto-restarts on exit, logs everything.
# Start with: tmux new-session -d -s web-casetype 'web/run_casetype_scraper.sh'
#
# Usage:
#   web/run_casetype_scraper.sh                        # all courts, 2020-today
#   web/run_casetype_scraper.sh --state_code 29        # Telangana only
#   web/run_casetype_scraper.sh --state_code 29 --district_code 22  # Mancherial only

LOG_FILE="${HOME}/web_casetype.log"
OPERATOR_DIR="${HOME}/.scraper-operator"
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"  # repo root

mkdir -p "$OPERATOR_DIR"

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
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Case-type scraper wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

EXTRA_ARGS="$*"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting case-type scraper..." >> "$LOG_FILE"

  uv run --python 3.13 python web/casetype_scraper.py \
    --max_workers 4 \
    --courts_csv courts.csv \
    $EXTRA_ARGS \
    2>&1 | tee -a "$LOG_FILE"

  EXIT_CODE=${PIPESTATUS[0]}

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Case-type scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
