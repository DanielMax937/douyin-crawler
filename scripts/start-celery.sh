#!/usr/bin/env bash
# 启动 Celery Worker + Beat（同进程，适合单机）
# 用法: ./scripts/start-celery.sh

set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_DIR="$REPO_DIR/worker"
PID_FILE="$REPO_DIR/celery.pid"
LOG_FILE="$REPO_DIR/celery.log"

cd "$WORKER_DIR"

if [ -f "$PID_FILE" ]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Celery already running (PID: $OLD_PID)"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

echo "Starting Celery (worker + beat, concurrency=1 for webgemini)..."
nohup uv run celery -A celery_app worker --beat --loglevel=info --concurrency=1 >> "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"
echo "✓ Celery started (PID: $(cat $PID_FILE))"
echo "  Log: tail -f $LOG_FILE"
echo "  Stop: ./scripts/stop-celery.sh"
