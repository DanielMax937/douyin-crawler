#!/bin/bash

set -euo pipefail

MODE="${1:-trigger}"
REPO_DIR="/Users/caoxiaopeng/Desktop/git/douyin-crawler"
WORKER_DIR="$REPO_DIR/worker"
WORKER_ENV="$WORKER_DIR/.env"
WORKER_LOG="$REPO_DIR/worker.log"
TRIGGER_LOG="$REPO_DIR/worker-trigger.log"
RESET_LOG="$REPO_DIR/worker-reset.log"
WORKER_PIDFILE="$WORKER_DIR/celery-worker.pid"

start_worker_if_needed() {
  if [ -f "$WORKER_PIDFILE" ] && kill -0 "$(cat "$WORKER_PIDFILE")" 2>/dev/null; then
    return
  fi

  rm -f "$WORKER_PIDFILE"

  cd "$WORKER_DIR"
  /opt/homebrew/bin/uv run celery -A celery_app worker \
    --loglevel=info \
    --detach \
    --pidfile="$WORKER_PIDFILE" \
    --logfile="$WORKER_LOG"

  sleep 5
}

cd "$REPO_DIR"

if [ ! -f "$WORKER_ENV" ]; then
  cp "$WORKER_DIR/.env.example" "$WORKER_ENV"
fi

case "$MODE" in
  trigger)
    start_worker_if_needed
    {
      echo "=================================================="
      echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] worker trigger start"
      cd "$WORKER_DIR"
      /opt/homebrew/bin/uv run python cli.py trigger 20
      echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] worker trigger end"
    } >> "$TRIGGER_LOG" 2>&1
    ;;
  reset)
    {
      echo "=================================================="
      echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] worker reset start"
      cd "$WORKER_DIR"
      /opt/homebrew/bin/uv run python cli.py reset
      echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] worker reset end"
    } >> "$RESET_LOG" 2>&1
    ;;
  *)
    echo "Usage: $0 [trigger|reset]" >&2
    exit 1
    ;;
esac
