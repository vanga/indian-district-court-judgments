#!/bin/bash
# Mobile API scraper wrapper — auto-restarts on exit, logs everything.
# Start with: tmux new-session -d -s scraper 'mobile/run_scraper.sh'
#
# State allocation: mobile scraper owns these states (web owns the rest).
# States: 1(J&K) 2(AP) 3(AR) 4(Assam) 5(Bihar-part) 6(CG) 7(Delhi)
#         8(Gujarat) 10(Haryana) 12(Jharkhand) 14(Karnataka) 15(Kerala)
#         16(MP) 17(Maharashtra) 18(Manipur-part) 20(Meghalaya) 21(Mizoram)
#         22(Nagaland-part) 23(Odisha-part) 26(Rajasthan) 27(Uttarakhand)
#         28(HP-done) 30(UP) 36(Telangana) 37(WB)

LOG_FILE="${HOME}/scraper.log"
PID_FILE="${HOME}/.scraper.pid"
OPERATOR_DIR="${HOME}/.scraper-operator"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

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
  rm -f "$PID_FILE"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Scraper wrapper stopped" >> "$LOG_FILE"
  exit 0
}

trap cleanup SIGINT SIGTERM

# Mobile-allocated states (pass extra args to override, e.g. --state 1 --state 2)
STATE_ARGS="${*:---state 1 --state 2 --state 3 --state 4 --state 5 --state 6 --state 7 --state 8 --state 10 --state 12 --state 14 --state 15 --state 16 --state 17 --state 18 --state 20 --state 21 --state 22 --state 23 --state 26 --state 27 --state 28 --state 30 --state 36 --state 37}"

while true; do
  rotate_log
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting scraper..." >> "$LOG_FILE"

  uv run python scraper.py \
    --no-pdfs \
    --parallel-complexes 2 \
    $STATE_ARGS \
    2>&1 | tee -a "$LOG_FILE" &
  SCRAPER_PID=$!
  echo "$SCRAPER_PID" > "$PID_FILE"

  wait $SCRAPER_PID
  EXIT_CODE=$?

  rm -f "$PID_FILE"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Scraper exited with code $EXIT_CODE, restarting in 30s..." >> "$LOG_FILE"
  sleep 30
done
