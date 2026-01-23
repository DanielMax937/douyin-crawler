from celery import Celery
from celery.schedules import crontab
from config import REDIS_URL, SCHEDULE_HOUR, SCHEDULE_MINUTE, BATCH_SIZE

# Create Celery app
app = Celery('douyin_worker', broker=REDIS_URL, backend=REDIS_URL)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max per task
    worker_prefetch_multiplier=1,  # Process one task at a time
)

# Scheduled tasks (Celery Beat)
app.conf.beat_schedule = {
    'process-videos-daily': {
        'task': 'tasks.process_pending_videos',
        'schedule': crontab(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        'args': (BATCH_SIZE,),
    },
    'reset-stale-tasks': {
        'task': 'tasks.reset_stale_tasks',
        'schedule': crontab(hour='*/6'),  # Every 6 hours
    },
}

# Import tasks to register them
import tasks
