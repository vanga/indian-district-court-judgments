#!/bin/bash
# Wrapper script that auto-restarts the scraper and logs everything
# Start with: tmux new-session -d -s scraper './mobile/run_scraper.sh'

LOG_FILE="${HOME}/scraper.log"
PID_FILE="${HOME}/.scraper.pid"
OPERATOR_DIR="${HOME}/.scraper-operator"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Ensure operator state directory exists
mkdir -p "$OPERATOR_DIR"

# Use the project's AWS profile
export AWS_PROFILE="${AWS_PROFILE:-dattam-od}"

cd "$SCRIPT_DIR"

# Log rotation: truncate if over 100MB
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
  rm -f "$PID_FILE"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Scraper wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting scraper..." >> "$LOG_FILE"

  uv run python scraper.py 2>&1 | tee -a "$LOG_FILE" &
  SCRAPER_PID=$!
  echo "$SCRAPER_PID" > "$PID_FILE"

  wait $SCRAPER_PID
  EXIT_CODE=$?

  rm -f "$PID_FILE"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
