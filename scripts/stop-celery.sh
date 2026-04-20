#!/usr/bin/env bash
# 停止 Celery 进程
# 用法: ./scripts/stop-celery.sh

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$REPO_DIR/celery.pid"
CELERY_BIN="$REPO_DIR/worker/.venv/bin/celery"

stop_repo_celery_children() {
  # Killing only the uv parent leaves prefork workers (reparented to PID 1).
  if pgrep -f "$CELERY_BIN" >/dev/null 2>&1; then
    echo "Stopping orphan worker processes under $REPO_DIR..."
    pkill -f "$CELERY_BIN" 2>/dev/null || true
    sleep 2
    pkill -9 -f "$CELERY_BIN" 2>/dev/null || true
  fi
}

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping Celery (PID: $PID)..."
    kill "$PID" 2>/dev/null || true
    sleep 2
    if kill -0 "$PID" 2>/dev/null; then
      kill -9 "$PID" 2>/dev/null || true
    fi
    echo "✓ Celery stopped (supervisor)"
  else
    echo "Process $PID not found. Removing stale PID file."
  fi
  rm -f "$PID_FILE"
else
  echo "No PID file (supervisor may already be stopped)."
fi

stop_repo_celery_children
echo "✓ Celery workers cleared for this repo"
