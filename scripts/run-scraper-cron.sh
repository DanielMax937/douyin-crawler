#!/bin/bash

set -euo pipefail

REPO_DIR="/Users/caoxiaopeng/Desktop/git/douyin-crawler"
LOG_FILE="$REPO_DIR/scraper.log"
SCRAPE_COUNT="${1:-100}"

cd "$REPO_DIR"

{
  echo "=================================================="
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] scraper cron start"
  /opt/homebrew/bin/node douyin-scraper.js "$SCRAPE_COUNT"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] scraper cron end"
} >> "$LOG_FILE" 2>&1
