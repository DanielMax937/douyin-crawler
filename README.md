# Douyin Video Scanner

A complete system for scraping Douyin (Chinese TikTok) videos and processing them through an async pipeline.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Node.js Scraper (douyin-scraper.js)            │
│  - Scrapes videos from Douyin via Chrome CDP                │
│  - Extracts: title, author, likes, comments, shares         │
│  - Saves to PostgreSQL                                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      PostgreSQL                             │
│  - douyin_videos      : Video metadata                      │
│  - douyin_comments    : Top comments per video              │
│  - video_tasks        : Pipeline status                     │
│  - video_task_steps   : Step history + JSON results         │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│           Python CLI + Celery Worker (worker/)              │
│  - Async pipeline: download → submit → get_summary          │
│  - Triggered via CLI (manual or crontab)                    │
│  - Skips completed steps on retry                           │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Node.js 18+
- Python 3.9+
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- PostgreSQL
- Redis
- Google Chrome

## Quick Start

### 1. Database Setup

```bash
# Start PostgreSQL (if not running)
brew services start postgresql

# Initialize database and tables
node init-db.js
```

Expected output:
```
🐘 Douyin Scraper Database Initialization
✅ Database "douyin" exists
📦 Ensuring tables exist...
✅ Tables ready
```

### 2. Scraper Setup (Node.js)

```bash
# Install dependencies
npm install

# Start Chrome with remote debugging (required)
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222 \
  --user-data-dir="./browser-profile" &

# Run scraper (scrape 5 videos with >= 5000 comments)
node douyin-scraper.js 5
```

Expected output:
```
🎬 Douyin Video Scraper
📊 Will scrape 5 video(s) with >= 5000 comments

📹 Scraping video #1...
  📝 Title: ...
  👤 Author: @username
  💬 Comments: 6.3万
  💾 Saved to PostgreSQL
  📊 Progress: 1/5 videos collected
```

#### Scraper Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SAVE_TO_FILE` | `false` | Set to `true` to save JSON/Markdown files |
| `PGHOST` | `localhost` | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | `douyin` | Database name |
| `PGUSER` | `postgres` | Database user |
| `PGPASSWORD` | `postgres` | Database password |

### 3. Worker Setup (Python/Celery)

```bash
# Start Redis (if not running)
brew services start redis

# Setup Python worker
cd worker
uv sync

# Copy environment config
cp .env.example .env
# Edit .env if needed

# Start Celery worker (keep running in terminal)
uv run celery -A celery_app worker --loglevel=info
```

Expected output:
```
[tasks]
  . tasks.process_pending_videos
  . tasks.process_video_pipeline
  . tasks.reset_stale_tasks
  . tasks.trigger_batch_now

celery@hostname ready.
```

### 4. Process Videos

```bash
cd worker

# Check status
uv run python cli.py status

# Process 20 oldest videos without summary
uv run python cli.py trigger 20

# Process a single video
uv run python cli.py process <video_id>

# Reset stuck tasks
uv run python cli.py reset
```

Expected output:
```
============================================================
DOUYIN VIDEO TASK STATUS
============================================================

📹 Videos without summary: 5

📊 Task Statistics:
------------------------------------------------------------
  completed: 10
  pending: 5

📋 Step Statistics:
------------------------------------------------------------
  download        | completed    | 10
  submit          | completed    | 10
  get_summary     | completed    | 10
============================================================
```

### 5. Automate with Crontab

```bash
# Edit crontab
crontab -e

# Add daily job at 8am
0 8 * * * cd /path/to/tiktok-scanner/worker && uv run python cli.py trigger 20 >> /var/log/douyin-worker.log 2>&1
```

## Project Structure

```
tiktok-scanner/
├── douyin-scraper.js     # Main scraper (Node.js)
├── init-db.js            # Database initialization
├── package.json          # Node.js dependencies
├── browser-profile/      # Chrome user data (cookies, session)
├── output/               # Scraped files (if SAVE_TO_FILE=true)
└── worker/               # Python Celery worker
    ├── pyproject.toml    # Python dependencies (uv)
    ├── uv.lock           # Lock file
    ├── .env.example      # Environment template
    ├── celery_app.py     # Celery configuration
    ├── config.py         # Settings from env vars
    ├── db.py             # Database operations
    ├── tasks.py          # Pipeline tasks (download, submit, get_summary)
    ├── cli.py            # CLI tool
    └── README.md         # Worker documentation
```

## Database Schema

### douyin_videos
| Column | Type | Description |
|--------|------|-------------|
| video_id | VARCHAR(64) | Unique video ID |
| title | TEXT | Video title |
| author | VARCHAR(255) | Author username |
| likes | INTEGER | Like count |
| comments_count | INTEGER | Comment count |
| favorites | INTEGER | Favorite count |
| shares | INTEGER | Share count |
| share_link | TEXT | Full video URL |

### video_tasks
| Column | Type | Description |
|--------|------|-------------|
| video_id | VARCHAR(64) | Reference to video |
| current_step | VARCHAR(32) | pending/download/submit/get_summary/completed |
| status | VARCHAR(32) | pending/processing/completed/failed |

### video_task_steps
| Column | Type | Description |
|--------|------|-------------|
| video_id | VARCHAR(64) | Reference to video |
| step_name | VARCHAR(32) | download/submit/get_summary |
| status | VARCHAR(32) | pending/processing/completed/failed |
| result | JSONB | Step output data |
| error_message | TEXT | Error details if failed |

## Pipeline Steps

The worker processes videos through 3 steps:

1. **download** - Download video file
2. **submit** - Submit to AI service for processing
3. **get_summary** - Retrieve generated summary

### Implementing Actual Logic

Edit `worker/tasks.py` to implement real logic:

```python
def _execute_download(video_id):
    # TODO: Download video from Douyin
    # - Fetch video URL from database
    # - Download video file
    # - Upload to cloud storage
    pass

def _execute_submit(video_id, download_result):
    # TODO: Submit to AI service
    # - Read video from download_result['file_path']
    # - Call AI API (OpenAI, Claude, etc.)
    # - Return submission ID
    pass

def _execute_get_summary(video_id, submit_result):
    # TODO: Get summary from AI service
    # - Poll using submit_result['submission_id']
    # - Store summary in database
    pass
```

## Common Commands

```bash
# === Database ===
node init-db.js              # Initialize tables
node init-db.js --drop       # Reset all data (WARNING!)

# === Scraper ===
node douyin-scraper.js 10    # Scrape 10 videos
SAVE_TO_FILE=true node douyin-scraper.js 5  # Save to files too

# === Worker ===
cd worker
uv run celery -A celery_app worker --loglevel=info  # Start worker
uv run celery -A celery_app beat --loglevel=info    # Start scheduler (optional)
uv run python cli.py status                          # Check status
uv run python cli.py trigger 20                      # Process 20 videos
uv run python cli.py process <video_id>              # Process one video
uv run python cli.py reset                           # Reset stuck tasks

# === Monitoring ===
uv run --with flower celery -A celery_app flower     # Web UI at :5555
```

## Troubleshooting

### Chrome connection refused
```
Error: connect ECONNREFUSED 127.0.0.1:9222
```
Start Chrome with remote debugging:
```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222 \
  --user-data-dir="./browser-profile"
```

### PostgreSQL connection failed
```
Error: ECONNREFUSED
```
Start PostgreSQL:
```bash
brew services start postgresql
```

### Redis connection failed
```
Error: Connection refused redis://localhost:6379
```
Start Redis:
```bash
brew services start redis
```

### Task stuck in processing
```bash
cd worker
uv run python cli.py reset
```

## License

MIT
