import os
from dotenv import load_dotenv

load_dotenv()

# Redis configuration (for Celery broker)
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# PostgreSQL configuration
POSTGRES_CONFIG = {
    'host': os.getenv('PGHOST', 'localhost'),
    'port': int(os.getenv('PGPORT', '5432')),
    'database': os.getenv('PGDATABASE', 'douyin'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', 'postgres'),
}

# Schedule configuration
SCHEDULE_HOUR = int(os.getenv('SCHEDULE_HOUR', '4'))  # 4am by default
SCHEDULE_MINUTE = int(os.getenv('SCHEDULE_MINUTE', '0'))

# Batch size for processing
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
STALE_PROCESSING_HOURS = int(os.getenv('STALE_PROCESSING_HOURS', '24'))
ORPHAN_FILE_GRACE_HOURS = int(os.getenv('ORPHAN_FILE_GRACE_HOURS', '24'))

# Download API (Douyin_TikTok_Download_API)
DOWNLOAD_API_BASE_URL = os.getenv('DOWNLOAD_API_BASE_URL', 'http://127.0.0.1:8000')
DOWNLOAD_SAVE_DIR = os.getenv('DOWNLOAD_SAVE_DIR', './downloads')
# Skip oversized videos before writing to disk (default: 2 GiB)
MAX_DOWNLOAD_SIZE_BYTES = int(os.getenv('MAX_DOWNLOAD_SIZE_BYTES', str(2 * 1024 * 1024 * 1024)))

# WebGemini API (video analysis via chat with attachment)
WEBGEMINI_API_URL = os.getenv('WEBGEMINI_API_URL', 'http://127.0.0.1:8200')
WEBGEMINI_POLL_INTERVAL = int(os.getenv('WEBGEMINI_POLL_INTERVAL', '5'))
WEBGEMINI_POLL_MAX_WAIT = int(os.getenv('WEBGEMINI_POLL_MAX_WAIT', '1800'))

# Optional ffmpeg compression before upload
ENABLE_VIDEO_COMPRESSION = os.getenv('ENABLE_VIDEO_COMPRESSION', 'true').lower() in ('1', 'true', 'yes', 'on')
VIDEO_COMPRESSION_CRF = int(os.getenv('VIDEO_COMPRESSION_CRF', '32'))
VIDEO_COMPRESSION_PRESET = os.getenv('VIDEO_COMPRESSION_PRESET', 'veryfast')

# Telegram: set in this repo's .env (same variable names as blog2media)
TELEGRAM_BOT_TOKEN = (os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('CTI_TG_BOT_TOKEN') or '').strip()
TELEGRAM_ALLOWED_CHAT_ID = (
    os.getenv('TELEGRAM_ALLOWED_CHAT_ID') or os.getenv('CTI_TG_CHAT_ID') or ''
).strip()

# BitStripe: upload batch report .md before Telegram (same script as bitstripe-uploader skill)
_default_bitstripe_script = os.path.join(
    os.path.expanduser('~'), '.cursor', 'skills', 'bitstripe-uploader', 'scripts', 'upload.sh',
)
BITSTRIPE_UPLOAD_SCRIPT = os.path.expanduser(
    os.getenv('BITSTRIPE_UPLOAD_SCRIPT', _default_bitstripe_script),
)
BITSTRIPE_URL_PREFIX = os.getenv('BITSTRIPE_URL_PREFIX', 'https://www.bitstripe.cn/files/')

# External douyin-download project cache dir to purge after Telegram notification
_default_douyin_download_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    '..',
    'Douyin_TikTok_Download_API',
    'download',
)
DOUYIN_DOWNLOAD_DIR = os.path.expanduser(
    os.getenv('DOUYIN_DOWNLOAD_DIR', _default_douyin_download_dir),
)
