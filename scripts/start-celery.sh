#!/usr/bin/env bash
# 启动 Celery Worker + Beat（同进程，适合单机）
# 用法: ./scripts/start-celery.sh

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_DIR="$REPO_DIR/worker"
PID_FILE="$REPO_DIR/celery.pid"
LOG_FILE="$REPO_DIR/celery.log"
START_TIMEOUT="${DOUYIN_CRAWLER_START_TIMEOUT:-60}"

cd "$WORKER_DIR"

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Celery already running (PID: $OLD_PID)"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

wait_until_ready() {
  local service_pid="$1"
  local elapsed=0
  local start_lines=0
  if [ -f "$LOG_FILE" ]; then
    start_lines=$(wc -l < "$LOG_FILE" | tr -d '[:space:]')
  fi
  while [ "$elapsed" -lt "$START_TIMEOUT" ]; do
    if ! kill -0 "$service_pid" 2>/dev/null; then
      return 1
    fi
    # Only match "ready." from this boot (avoids stale grep if log was rotated/replaced).
    if [ -f "$LOG_FILE" ] && tail -n +$((start_lines + 1)) "$LOG_FILE" 2>/dev/null | grep -q 'ready\.'; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

start_detached() {
  python3 - "$LOG_FILE" <<'PY'
import subprocess
import sys

log_file = sys.argv[1]
# Celery opens the logfile itself (--logfile); do not shell-redirect stdout here.
# Redirecting stdout to a path that is later replaced leaves processes writing a
# detached inode while "celery.log" on disk appears frozen.
open(log_file, "a").close()
proc = subprocess.Popen(
    [
        "uv",
        "run",
        "celery",
        "-A",
        "celery_app",
        "worker",
        "--beat",
        "--loglevel=info",
        "--concurrency=1",
        "--logfile",
        log_file,
    ],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
)
print(proc.pid)
PY
}

echo "Starting Celery (worker + beat, concurrency=1 for webgemini)..."
start_detached > "$PID_FILE"

if ! wait_until_ready "$(cat "$PID_FILE")"; then
  echo "Celery failed to become ready within ${START_TIMEOUT}s."
  PID="$(cat "$PID_FILE")"
  kill "$PID" 2>/dev/null || true
  sleep 1
  if kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "Recent logs:"
  tail -n 40 "$LOG_FILE" 2>/dev/null || true
  exit 1
fi

echo "✓ Celery started (PID: $(cat $PID_FILE))"
echo "  Log: tail -f $LOG_FILE"
echo "  Stop: ./scripts/stop-celery.sh"
