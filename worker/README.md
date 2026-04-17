# Douyin Video Worker

Celery-based async worker for processing Douyin videos through the pipeline:
**download → submit → get_summary**

## Quick Start

```bash
# 1. Start Redis
brew services start redis

# 2. Install dependencies
uv sync

# 3. Configure environment
cp .env.example .env

# 4. Start worker
uv run celery -A celery_app worker --loglevel=info --concurrency=1

# 5. Trigger processing (in another terminal)
uv run python cli.py trigger 20
```

## CLI Commands

```bash
# Show status of pending videos and tasks
uv run python cli.py status

# Process N oldest videos without summary
uv run python cli.py trigger 20

# Process a single video
uv run python cli.py process <video_id>

# Reset stale tasks (stuck > 24h)
uv run python cli.py reset
```

## Crontab Setup

```bash
# Edit crontab
crontab -e

# Add daily job at 4am
0 4 * * * cd /path/to/worker && uv run python cli.py trigger 20 >> /var/log/douyin.log 2>&1
```

## Configuration

Environment variables (`.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `PGHOST` | `localhost` | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | `douyin` | Database name |
| `PGUSER` | `postgres` | Database user |
| `PGPASSWORD` | `postgres` | Database password |
| `SCHEDULE_HOUR` | `4` | Hour for scheduled job (0-23) |
| `SCHEDULE_MINUTE` | `0` | Minute for scheduled job (0-59) |
| `BATCH_SIZE` | `20` | Videos per batch |
| `WEBGEMINI_POLL_INTERVAL` | `5` | Poll interval in seconds for WebGemini jobs |
| `WEBGEMINI_POLL_MAX_WAIT` | `1800` | Max wait in seconds before upload polling times out |
| `ENABLE_VIDEO_COMPRESSION` | `true` | Compress videos with `ffmpeg` before upload |
| `VIDEO_COMPRESSION_CRF` | `32` | `ffmpeg` CRF value; higher means smaller files |
| `VIDEO_COMPRESSION_PRESET` | `veryfast` | `ffmpeg` x264 preset for compression speed vs size |

## Pipeline Steps

| Step | Description | TODO |
|------|-------------|------|
| `download` | Download video file | Implement in `_execute_download()` |
| `submit` | Submit to AI service | Implement in `_execute_submit()` |
| `get_summary` | Retrieve summary | Implement in `_execute_get_summary()` |

### Step Behavior

- Steps run sequentially within each video
- Completed steps are **skipped** on retry
- Failed tasks can be retried - resume from failed step
- Each step result stored as JSON in `video_task_steps.result`

## Implementing Actual Logic

Edit `tasks.py`:

```python
def _execute_download(video_id):
    """Download video file."""
    start_step(video_id, 'download')
    try:
        # Your download logic here
        result = {'file_path': '/path/to/video.mp4'}
        complete_step(video_id, 'download', result)
        return result
    except Exception as e:
        complete_step(video_id, 'download', None, str(e))
        raise

def _execute_submit(video_id, download_result):
    """Submit to AI service."""
    start_step(video_id, 'submit')
    try:
        file_path = download_result['file_path']
        # Your submit logic here
        result = {'submission_id': 'abc123'}
        complete_step(video_id, 'submit', result)
        return result
    except Exception as e:
        complete_step(video_id, 'submit', None, str(e))
        raise

def _execute_get_summary(video_id, submit_result):
    """Get summary from AI service."""
    start_step(video_id, 'get_summary')
    try:
        submission_id = submit_result['submission_id']
        # Your retrieval logic here
        result = {'summary': 'Video summary text...'}
        complete_step(video_id, 'get_summary', result)
        return result
    except Exception as e:
        complete_step(video_id, 'get_summary', None, str(e))
        raise
```

## Monitoring

```bash
# Worker logs
uv run celery -A celery_app worker --loglevel=debug --concurrency=1

# Flower web UI (http://localhost:5555)
uv run --with flower celery -A celery_app flower

# Task status
uv run python cli.py status
```

## Database Tables

### video_tasks
Current pipeline status per video.

```sql
SELECT video_id, current_step, status, updated_at
FROM video_tasks;
```

### video_task_steps
History of all step executions.

```sql
SELECT video_id, step_name, status, result->>'status', completed_at
FROM video_task_steps
ORDER BY completed_at;
```

## Troubleshooting

### Redis connection failed
```bash
brew services start redis
redis-cli ping  # Should return PONG
```

### Task stuck in processing
```bash
uv run python cli.py reset
```

### Worker not picking up tasks
```bash
# Check if worker is running
pgrep -f "celery.*worker"

# Restart worker
pkill -f "celery.*celery_app"
uv run celery -A celery_app worker --loglevel=info --concurrency=1
```
