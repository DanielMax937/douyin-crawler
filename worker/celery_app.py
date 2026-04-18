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
    task_time_limit=28800,  # 8 hours max per task
    worker_prefetch_multiplier=1,  # Process one task at a time
)

# Scheduled tasks (Celery Beat) — app timezone is Asia/Shanghai
app.conf.beat_schedule = {
    'process-videos-daily': {
        'task': 'tasks.process_pending_videos',
        'schedule': crontab(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE),
        'args': (BATCH_SIZE,),
    },
    # 中文语境「中午 12 点」与「晚上 6 点」各跑一批（与 daily 叠加）
    'process-videos-noon-cn': {
        'task': 'tasks.process_pending_videos',
        'schedule': crontab(hour=12, minute=0),
        'args': (BATCH_SIZE,),
    },
    'process-videos-evening-cn': {
        'task': 'tasks.process_pending_videos',
        'schedule': crontab(hour=18, minute=0),
        'args': (BATCH_SIZE,),
    },
    'reset-stale-tasks': {
        'task': 'tasks.reset_stale_tasks',
        'schedule': crontab(hour='*/6'),  # Every 6 hours
    },
    'cleanup-orphan-download-files': {
        'task': 'tasks.cleanup_orphan_download_files',
        'schedule': crontab(hour='*/6', minute=20),  # Every 6 hours, staggered from reset
    },
    'scrape-douyin-daily': {
        'task': 'tasks.scrape_douyin_daily',
        'schedule': crontab(hour=2, minute=0),  # 每天凌晨 2 点（原 crontab）
    },
}

# Import tasks to register them
import tasks
