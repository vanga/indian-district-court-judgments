#!/bin/bash
# Case-type web scraper wrapper — auto-restarts on exit, logs everything.
# Start with: tmux new-session -d -s web-casetype 'web/run_casetype_scraper.sh'
#
# State allocation: web scraper owns these states (mobile owns the rest).
# States: 9(Bihar) 11(Goa) 13(HP) 19(Manipur) 24(Nagaland) 25(Odisha)
#         29(Punjab) 33(Sikkim) 34(TN) 35(Tripura) 38(A&N Islands)

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

# Web-allocated states (pass extra args to override, e.g. --state_code 29)
STATE_ARGS="${*:---state_code 9 --state_code 11 --state_code 13 --state_code 19 --state_code 24 --state_code 25 --state_code 29 --state_code 33 --state_code 34 --state_code 35 --state_code 38}"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting case-type scraper..." >> "$LOG_FILE"

  uv run --python 3.13 python web/casetype_scraper.py \
    --max_workers 2 \
    --courts_csv courts.csv \
    $STATE_ARGS \
    2>&1 | tee -a "$LOG_FILE"

  EXIT_CODE=${PIPESTATUS[0]}

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Case-type scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
