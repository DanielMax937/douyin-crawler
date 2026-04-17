from celery_app import app
from db import (
    get_videos_pending_summary,
    get_video_by_id_for_processing,
    get_comments_for_video,
    reset_stale_tasks as db_reset_stale_tasks,
    update_video_local_path,
    create_or_update_video_summary,
    update_video_summary_result,
    clear_video_local_path,
)
from datetime import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request

try:
    from config import (
        DOWNLOAD_API_BASE_URL,
        DOWNLOAD_SAVE_DIR,
        ENABLE_VIDEO_COMPRESSION,
        VIDEO_COMPRESSION_CRF,
        VIDEO_COMPRESSION_PRESET,
        WEBGEMINI_API_URL,
        WEBGEMINI_POLL_INTERVAL,
        WEBGEMINI_POLL_MAX_WAIT,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_ALLOWED_CHAT_ID,
    )
except ImportError:
    DOWNLOAD_API_BASE_URL = 'http://127.0.0.1:8000'
    DOWNLOAD_SAVE_DIR = './downloads'
    WEBGEMINI_API_URL = 'http://127.0.0.1:8200'
    WEBGEMINI_POLL_INTERVAL = 5
    WEBGEMINI_POLL_MAX_WAIT = 1800
    ENABLE_VIDEO_COMPRESSION = True
    VIDEO_COMPRESSION_CRF = 32
    VIDEO_COMPRESSION_PRESET = 'veryfast'
    TELEGRAM_BOT_TOKEN = ''
    TELEGRAM_ALLOWED_CHAT_ID = ''

# Repo root (parent of worker/)
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPER_SCRIPT = os.path.join(REPO_DIR, 'douyin-scraper.js')

logger = logging.getLogger(__name__)


def parse_video_id_from_url(url):
    """
    Parse Douyin video link to extract video_id.
    Supports: https://www.douyin.com/video/7611533789604433190,
              https://v.douyin.com/xxx (requires video_id from DB),
              share text containing links.
    """
    if not url or not url.strip():
        return None
    url = url.strip()
    m = re.search(r'douyin\.com/video/(\d+)', url)
    if m:
        return m.group(1)
    m = re.search(r'/(\d{15,})/', url)
    if m:
        return m.group(1)
    return None


def _build_webgemini_summary_prompt(video, comments):
    """
    Build prompt: video understanding + engagement stats + comments with per-comment likes,
    and instructions for narrative + why video got likes + why top comments are liked.
    video: dict from DB; comments: list of row dicts (username, content, time, location, likes).
    """
    def _s(val, default=''):
        if val is None:
            return default
        t = str(val).strip()
        return t if t else default

    vid = _s(video.get('video_id'), '—')
    title = _s(video.get('title'), '（无标题）')
    author = _s(video.get('author'), '（未知作者）')
    likes = video.get('likes')
    likes_disp = _s(video.get('likes_display'), str(likes) if likes is not None else '—')
    cc = video.get('comments_count')
    cc_disp = _s(video.get('comments_display'), str(cc) if cc is not None else '—')
    shares = video.get('shares')
    shares_disp = _s(video.get('shares_display'), str(shares) if shares is not None else '—')

    lines = [
        '【抖音视频互动数据（来自页面抓取，请结合视频画面一起分析）】',
        f'- 视频ID：{vid}',
        f'- 标题：{title}',
        f'- 作者：{author}',
        f'- 点赞数：{likes_disp}（数值：{likes if likes is not None else "—"}）',
        f'- 评论数：{cc_disp}（数值：{cc if cc is not None else "—"}）',
        f'- 转发/分享数：{shares_disp}（数值：{shares if shares is not None else "—"}）',
        '',
        '【评论列表】每条含：用户名、正文、时间、地点、该条评论获得的点赞数。',
    ]
    if not comments:
        lines.append('（当前没有可用的评论文本记录；请主要依据视频内容分析，并说明评论数据缺失。）')
    else:
        for i, c in enumerate(comments, 1):
            uname = _s(c.get('username'), '—')
            content = _s(c.get('content'), '—')
            ctime = _s(c.get('time'), '—')
            loc = _s(c.get('location'), '—')
            clikes = c.get('likes')
            cl = clikes if clikes is not None else 0
            lines.append(f'{i}. @{uname} | 该评论点赞数：{cl}')
            lines.append(f'   正文：{content}')
            lines.append(f'   时间：{ctime} | 地点：{loc}')
            lines.append('')

    lines.extend([
        '---',
        '请观看附件视频，用中文有条理地回答：',
        '1）视频内容：画面里发生了什么、主题是什么。',
        '2）结合评论（若有）：评论者在讨论什么，与画面如何呼应；这条视频整体「记录了什么事情」。',
        '3）为什么可能有这么高点赞：结合画面情绪、话题点与上方互动数据，分析视频为何容易引发点赞（说明你的推理依据）。',
        '4）高赞评论：指出点赞数突出的一条或几条，概括其内容，并分析观众为什么特别「喜欢」这条评论（笑点、梗、共鸣、争议等）。若无评论文本，说明无法逐条分析，仅可结合视频推测可能的讨论方向。',
    ])
    return '\n'.join(lines)


def _get_download_dir():
    """Resolve download dir to an absolute path under repo root when configured relatively."""
    if os.path.isabs(DOWNLOAD_SAVE_DIR):
        return DOWNLOAD_SAVE_DIR
    return os.path.normpath(os.path.join(REPO_DIR, DOWNLOAD_SAVE_DIR))


def _resolve_video_path(local_path):
    """Resolve relative path to absolute. Tries REPO_DIR and REPO_DIR/worker (celery cwd)."""
    if not local_path or not local_path.strip():
        return None
    path = local_path.strip()
    if os.path.isabs(path):
        return path
    p1 = os.path.normpath(os.path.join(REPO_DIR, path))
    if os.path.isfile(p1):
        return p1
    p2 = os.path.normpath(os.path.join(REPO_DIR, 'worker', path))
    if os.path.isfile(p2):
        return p2
    return p1


def _get_douyin_url(video):
    """Get douyin URL from video record."""
    share_link = (video.get('share_link') or '').strip()
    short_link = (video.get('short_link') or '').strip()
    video_id = video['video_id']
    if share_link and ('douyin.com' in share_link or 'iesdouyin.com' in share_link):
        return share_link
    if short_link:
        return short_link
    return f"https://www.douyin.com/video/{video_id}"


def _build_download_url(video):
    """Resolve share/short link for Douyin download API."""
    share_link = (video.get('share_link') or '').strip()
    short_link = (video.get('short_link') or '').strip()
    video_id = video['video_id']
    if share_link and ('douyin.com' in share_link or 'iesdouyin.com' in share_link):
        return share_link
    if short_link:
        return short_link
    return f"https://www.douyin.com/video/{video_id}"


def _download_one_video(video):
    """
    Download one video via Douyin API; write file and set local_file_path.
    Returns (ok, absolute_path_or_none, error_message_or_none).
    """
    video_id = video['video_id']
    url = _build_download_url(video)
    download_dir = _get_download_dir()
    os.makedirs(download_dir, exist_ok=True)
    file_path = os.path.normpath(os.path.join(download_dir, f"douyin_{video_id}.mp4"))
    download_url = f"{DOWNLOAD_API_BASE_URL.rstrip('/')}/api/download"
    params = f"url={urllib.parse.quote(url)}"
    try:
        with urllib.request.urlopen(f"{download_url}?{params}", timeout=300) as resp:
            if resp.status != 200:
                return False, None, f"HTTP {resp.status}"
            with open(file_path, 'wb') as f:
                f.write(resp.read())
        update_video_local_path(video_id, file_path)
        return True, file_path, None
    except Exception as e:
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
        logger.error("Download failed for %s: %s", video_id, e)
        return False, None, str(e)


def _remove_local_file_and_clear_db(video_id, local_path):
    """Delete file if present; always clear local_file_path in DB."""
    if local_path and os.path.isfile(local_path):
        try:
            os.remove(local_path)
            logger.info("Removed local video file after processing: %s", local_path)
        except OSError as e:
            logger.warning("Could not remove %s: %s", local_path, e)
    clear_video_local_path(video_id)


def _compress_video_for_upload(local_path):
    """
    Compress video with ffmpeg before upload.
    Returns the path to upload, which may be the original file if compression is disabled.
    """
    if not ENABLE_VIDEO_COMPRESSION:
        return local_path

    ffmpeg_bin = shutil.which('ffmpeg')
    if not ffmpeg_bin:
        raise RuntimeError("ffmpeg not found in PATH")

    base, ext = os.path.splitext(local_path)
    compressed_path = f"{base}_compressed{ext or '.mp4'}"
    cmd = [
        ffmpeg_bin,
        '-y',
        '-i', local_path,
        '-c:v', 'libx264',
        '-preset', VIDEO_COMPRESSION_PRESET,
        '-crf', str(VIDEO_COMPRESSION_CRF),
        '-c:a', 'aac',
        '-b:a', '128k',
        '-movflags', '+faststart',
        compressed_path,
    ]

    logger.info(
        "Compressing video before webgemini upload: src=%s dest=%s crf=%s preset=%s",
        local_path, compressed_path, VIDEO_COMPRESSION_CRF, VIDEO_COMPRESSION_PRESET,
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if result.returncode != 0:
        err = (result.stderr or result.stdout or '').strip()
        raise RuntimeError(f"ffmpeg compression failed: {err}")
    if not os.path.isfile(compressed_path):
        raise RuntimeError(f"Compressed file not found: {compressed_path}")
    return compressed_path


def _submit_webgemini_chat(prompt, attachments):
    """POST to webgemini /chat, return job_id."""
    url = f"{WEBGEMINI_API_URL.rstrip('/')}/chat"
    logger.info(
        "[webgemini] POST /chat before: url=%s prompt=%r attachments_count=%d attachments=%s",
        url, prompt[:80] + "..." if len(prompt) > 80 else prompt, len(attachments or []),
        [os.path.basename(p) for p in (attachments or [])],
    )
    data = json.dumps({'prompt': prompt, 'attachments': attachments}).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status}")
        body = json.loads(resp.read().decode())
        job_id = body.get('job_id')
        logger.info("[webgemini] POST /chat after: job_id=%s status=%s", job_id, body.get('status', 'N/A'))
        return job_id


def _poll_webgemini_chat(job_id, poll_interval=WEBGEMINI_POLL_INTERVAL, max_wait=WEBGEMINI_POLL_MAX_WAIT):
    """Poll GET /chat/{job_id} until completed or failed. Return (status, text, error)."""
    url = f"{WEBGEMINI_API_URL.rstrip('/')}/chat/{job_id}"
    logger.info("[webgemini] GET /chat/{job_id} before: job_id=%s url=%s", job_id, url)
    start = time.time()
    poll_count = 0
    while time.time() - start < max_wait:
        poll_count += 1
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
        status = body.get('status', '')
        if status == 'completed':
            text = body.get('text') or ''
            logger.info(
                "[webgemini] GET /chat after: job_id=%s status=completed poll_count=%d text_len=%d text_preview=%s",
                job_id, poll_count, len(text), (text[:100] + "...") if len(text) > 100 else text,
            )
            return ('completed', text, None)
        if status == 'failed':
            err = body.get('error') or 'Unknown error'
            logger.info(
                "[webgemini] GET /chat after: job_id=%s status=failed poll_count=%d error=%s",
                job_id, poll_count, err,
            )
            return ('failed', None, err)
        if poll_count <= 3 or poll_count % 10 == 0:
            logger.info("[webgemini] GET /chat polling: job_id=%s poll_count=%d status=%s", job_id, poll_count, status)
        time.sleep(poll_interval)
    logger.info("[webgemini] GET /chat after: job_id=%s status=timeout poll_count=%d", job_id, poll_count)
    return ('failed', None, 'Poll timeout')


TELEGRAM_MAX_MESSAGE_LEN = 4096


def _split_telegram_text(text):
    if not text:
        return ['']
    return [
        text[i : i + TELEGRAM_MAX_MESSAGE_LEN]
        for i in range(0, len(text), TELEGRAM_MAX_MESSAGE_LEN)
    ]


def _send_telegram_plain(text):
    """
    Send plain text via Telegram Bot API.
    Uses TELEGRAM_BOT_TOKEN + TELEGRAM_ALLOWED_CHAT_ID, or CTI_TG_* (set in douyin-crawler .env).
    """
    token = (TELEGRAM_BOT_TOKEN or '').strip()
    chat_id = (TELEGRAM_ALLOWED_CHAT_ID or '').strip()
    if not token or not chat_id:
        logger.warning(
            "Telegram skipped: set TELEGRAM_BOT_TOKEN and TELEGRAM_ALLOWED_CHAT_ID "
            "(or CTI_TG_BOT_TOKEN / CTI_TG_CHAT_ID) in douyin-crawler .env",
        )
        return False
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chunk in _split_telegram_text(text):
        payload = json.dumps({
            'chat_id': chat_id,
            'text': chunk,
            'disable_web_page_preview': False,
        }).encode('utf-8')
        req = urllib.request.Request(api_url, data=payload, method='POST')
        req.add_header('Content-Type', 'application/json')
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    logger.error("Telegram sendMessage HTTP %s", resp.status)
                    return False
        except Exception as e:
            logger.exception("Telegram sendMessage failed: %s", e)
            return False
    return True


def _notify_telegram_douyin_success_batch(items):
    """
    After process_pending_videos, if at least one video succeeded, send each link + AI reply.
    items: list of dicts with keys video_id, douyin_url, ai_reply.
    """
    if not items:
        return False
    n = len(items)
    lines = [
        f'douyin-crawler 视频解析完成（本批成功 {n} 条）',
        '',
    ]
    for it in items:
        lines.append(f"video_id: {it.get('video_id', '—')}")
        lines.append(f"链接: {it.get('douyin_url', '—')}")
        lines.append('')
        lines.append('AI 回复:')
        lines.append(it.get('ai_reply') or '（空）')
        lines.append('')
    body = '\n'.join(lines).rstrip()
    ok = _send_telegram_plain(body)
    if ok:
        logger.info("Telegram notification sent for douyin summary batch (%s success)", n)
    return ok


def _process_one_video_download_summary_delete(video):
    """
    Download if needed → WebGemini submit + poll → delete file + clear DB path.
    """
    video_id = video['video_id']
    douyin_url = _get_douyin_url(video)
    local_path = None
    upload_path = None

    try:
        local_path = _resolve_video_path(video.get('local_file_path') or '')
        if not local_path or not os.path.isfile(local_path):
            ok, path, err = _download_one_video(video)
            if not ok:
                create_or_update_video_summary(
                    video_id, douyin_url, status='failed',
                )
                return {'status': 'failed', 'video_id': video_id, 'error': err or 'download failed'}

            local_path = path

        try:
            local_path = os.path.abspath(local_path)
            upload_path = _compress_video_for_upload(local_path)
            upload_path = os.path.abspath(upload_path)
            comments = get_comments_for_video(video_id)
            summary_prompt = _build_webgemini_summary_prompt(video, comments)
            job_id = _submit_webgemini_chat(summary_prompt, [upload_path])
            create_or_update_video_summary(
                video_id, douyin_url, webgemini_job_id=job_id, status='processing',
            )
            logger.info("Submitted to webgemini, job_id=%s", job_id)

            status, text, error = _poll_webgemini_chat(job_id)
            if status == 'completed':
                update_video_summary_result(video_id, text, status='completed')
                logger.info("Webgemini summary completed for %s", video_id)
                return {
                    'status': 'completed',
                    'video_id': video_id,
                    'douyin_url': douyin_url,
                    'ai_reply': text or '',
                }
            update_video_summary_result(video_id, error or 'Unknown', status='failed')
            logger.error("Webgemini failed for %s: %s", video_id, error)
            return {'status': 'failed', 'video_id': video_id, 'error': error}
        except Exception as e:
            logger.exception("Webgemini summary failed for %s", video_id)
            create_or_update_video_summary(video_id, douyin_url, status='failed')
            return {'status': 'failed', 'video_id': video_id, 'error': str(e)}
    finally:
        if upload_path and upload_path != local_path and os.path.isfile(upload_path):
            try:
                os.remove(upload_path)
                logger.info("Removed compressed upload file after processing: %s", upload_path)
            except OSError as e:
                logger.warning("Could not remove compressed file %s: %s", upload_path, e)
        _remove_local_file_and_clear_db(video_id, local_path)


@app.task(bind=True, name='tasks.process_pending_videos')
def process_pending_videos(self, batch_size=20):
    """
    Scheduled / batch: for each pending video — download (if needed) → WebGemini → delete file.
    Sequential: one video at a time.
    """
    logger.info(
        "Starting process_pending_videos: up to %s videos (download → webgemini → delete)",
        batch_size,
    )

    videos = get_videos_pending_summary(limit=batch_size)
    logger.info("Found %s videos pending summary", len(videos))

    completed = 0
    failed = 0
    results = []
    success_rows = []

    for video in videos:
        video_id = video['video_id']
        logger.info("Processing video %s", video_id)
        try:
            outcome = _process_one_video_download_summary_delete(video)
            if outcome.get('status') == 'completed':
                completed += 1
                results.append({'video_id': video_id, 'status': 'completed'})
                success_rows.append({
                    'video_id': video_id,
                    'douyin_url': outcome.get('douyin_url') or '',
                    'ai_reply': outcome.get('ai_reply') or '',
                })
            else:
                failed += 1
                results.append({
                    'video_id': video_id,
                    'status': 'failed',
                    'error': outcome.get('error', ''),
                })
        except Exception as e:
            failed += 1
            logger.error("Pipeline failed for %s: %s", video_id, e)
            results.append({'video_id': video_id, 'status': 'failed', 'error': str(e)})

    telegram_sent = None
    if success_rows:
        telegram_sent = _notify_telegram_douyin_success_batch(success_rows)

    out = {
        'completed': completed,
        'failed': failed,
        'total': len(videos),
        'results': results,
        'timestamp': datetime.now().isoformat(),
    }
    if telegram_sent is not None:
        out['telegram_sent'] = telegram_sent
    return out


@app.task(bind=True, name='tasks.process_one_video_summary')
def process_one_video_summary(self, video_id):
    """Process a single video: download → WebGemini → delete file."""
    video = get_video_by_id_for_processing(video_id)
    if not video:
        return {'status': 'failed', 'error': 'video not found', 'video_id': video_id}
    return _process_one_video_download_summary_delete(video)


@app.task(name='tasks.reset_stale_tasks')
def reset_stale_tasks():
    """Reset tasks that have been stuck in processing state."""
    logger.info("Resetting stale tasks")
    db_reset_stale_tasks(hours=24)
    return {'status': 'completed', 'timestamp': datetime.now().isoformat()}


@app.task(name='tasks.scrape_douyin_daily')
def scrape_douyin_daily(count=100):
    """
    每天凌晨 2 点执行（原 crontab 定时任务）。
    直接调用 node douyin-scraper.js 抓取新视频，不再依赖 server.js。
    """
    count = min(500, max(1, int(count)))
    logger.info("Starting scrape_douyin_daily: running node douyin-scraper.js %s", count)
    if not os.path.isfile(SCRAPER_SCRIPT):
        raise FileNotFoundError(f"Scraper script not found: {SCRAPER_SCRIPT}")
    try:
        env = os.environ.copy()
        env.setdefault('PGUSER', 'caoxiaopeng')
        result = subprocess.run(
            ['node', SCRAPER_SCRIPT, str(count)],
            cwd=REPO_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=None,
        )
        if result.returncode != 0:
            logger.error("Scraper failed: %s", result.stderr or result.stdout)
            return {
                'status': 'failed',
                'returncode': result.returncode,
                'stderr': result.stderr,
                'stdout': result.stdout,
                'timestamp': datetime.now().isoformat(),
            }
        logger.info("Scraper completed successfully")
        return {
            'status': 'completed',
            'count': count,
            'timestamp': datetime.now().isoformat(),
        }
    except subprocess.TimeoutExpired:
        logger.error("Scraper timed out (should not happen with timeout=None)")
        raise
