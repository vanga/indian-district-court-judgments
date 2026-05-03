#!/bin/bash
# Web scraper wrapper — auto-restarts on exit, logs everything.
# Start with: tmux new-session -d -s web-scraper 'web/run_web_scraper.sh'
#
# Usage:
#   web/run_web_scraper.sh                         # all courts, 2020-today
#   web/run_web_scraper.sh --state_code 29         # Telangana only
#   web/run_web_scraper.sh --state_code 29 --district_code 22  # Mancherial only

LOG_FILE="${HOME}/web_scraper.log"
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
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Web scraper wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

START_DATE="2020-01-01"
END_DATE="$(date '+%Y-%m-%d')"
EXTRA_ARGS="$*"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting web scraper..." >> "$LOG_FILE"

  uv run python web/download.py \
    --start_date "$START_DATE" \
    --end_date "$END_DATE" \
    --day_step 30 \
    --max_workers 2 \
    --courts_csv courts.csv \
    --no-compress \
    $EXTRA_ARGS \
    2>&1 | tee -a "$LOG_FILE" &

  SCRAPER_PID=$!
  wait $SCRAPER_PID
  EXIT_CODE=$?

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Web scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
