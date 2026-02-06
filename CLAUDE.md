# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Douyin Video Scanner - a two-component system for scraping Douyin (Chinese TikTok) videos and processing them through an async pipeline.

## Architecture

```
Node.js Scraper (douyin-scraper.js)
    │  - Launches system Chrome via patchright (no CDP; no pre-started browser)
    │  - Scrapes videos with >= 5000 comments
    │  - Uses patchright (Playwright fork) for browser automation
    │  - Human-like behavior simulation (random delays, smooth scrolling)
    ▼
PostgreSQL (douyin database)
    │  - douyin_videos: Video metadata
    │  - douyin_comments: Top comments per video
    │  - video_tasks: Pipeline status tracking
    │  - video_task_steps: Step history with JSON results
    ▼
Python Celery Worker (worker/)
    - Pipeline: download → submit → get_summary
    - Skips completed steps on retry
    - CLI for manual triggering and status checks
```

## Common Commands

### Database
```bash
node init-db.js              # Initialize tables (idempotent)
node init-db.js --drop       # Reset all data
```

### Scraper (Node.js)
```bash
npm install                  # Install dependencies

# Run scraper (launches system Chrome automatically via patchright)
node douyin-scraper.js 10    # Scrape 10 videos
SAVE_TO_FILE=true node douyin-scraper.js 5  # Also save JSON/MD files

# Optional: custom user data dir (default: OS temp dir)
BROWSER_USER_DATA_DIR=./browser-profile node douyin-scraper.js 5
```

### Worker (Python/Celery)
```bash
cd worker
uv sync                      # Install dependencies

# Start worker (requires Redis running)
uv run celery -A celery_app worker --loglevel=info

# CLI commands
uv run python cli.py status              # Check pending videos and task stats
uv run python cli.py trigger 20          # Process 20 oldest videos
uv run python cli.py process <video_id>  # Process single video
uv run python cli.py reset               # Reset stuck tasks (>24h)

# Monitoring
uv run --with flower celery -A celery_app flower  # Web UI at :5555
```

### Prerequisites
```bash
brew services start postgresql
brew services start redis
```

## Key Implementation Details

### Scraper (douyin-scraper.js)
- Launches system Chrome via patchright (`channel: "chrome"`, persistent context); no CDP or pre-started browser
- Optional `BROWSER_USER_DATA_DIR` for profile path (default: OS temp)
- Filters videos by `MIN_COMMENTS_THRESHOLD` (default: 5000)
- Resolves short URLs (v.douyin.com) to full video URLs
- Parses Chinese number notation (万 = 10,000, 亿 = 100,000,000)
- Uses `SELECTORS` object for all DOM queries (data-e2e attributes)

### Worker Pipeline (worker/tasks.py)
- Three steps: `download`, `submit`, `get_summary`
- Step implementations are placeholders (TODO) - actual AI integration needed
- Each step uses `start_step()` / `complete_step()` for state tracking
- Results stored as JSONB in `video_task_steps.result`

### Database Operations (worker/db.py)
- `get_videos_without_summary()` - finds unprocessed videos
- `get_task_status()` - returns completed steps and their results
- `complete_step()` - marks final step as 'completed' when `step_name == 'get_summary'`

## Environment Variables

| Variable | Default | Used By |
|----------|---------|---------|
| `PGHOST` | localhost | Both |
| `PGPORT` | 5432 | Both |
| `PGDATABASE` | douyin | Both |
| `PGUSER` | postgres | Both |
| `PGPASSWORD` | postgres | Both |
| `SAVE_TO_FILE` | false | Scraper |
| `REDIS_URL` | redis://localhost:6379/0 | Worker |
