#!/bin/bash
# Unified case-type web pipeline wrapper — auto-restarts on exit, logs everything.
# Start with: tmux new-session -d -s web-casetype-pipeline 'web/run_casetype_pipeline.sh'
#
# Usage:
#   web/run_casetype_pipeline.sh
#   web/run_casetype_pipeline.sh --state_code 29
#   web/run_casetype_pipeline.sh --state_code 29 --district_code 22

LOG_FILE="${HOME}/web_casetype_pipeline.log"
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
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Case-type pipeline wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

EXTRA_ARGS="$*"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting case-type pipeline..." >> "$LOG_FILE"

  uv run --python 3.13 python web/casetype_pipeline.py \
    --max_workers 2 \
    --enrich_workers 8 \
    --courts_csv courts.csv \
    $EXTRA_ARGS \
    2>&1 | tee -a "$LOG_FILE"

  EXIT_CODE=${PIPESTATUS[0]}

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Case-type pipeline exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
