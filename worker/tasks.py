from celery_app import app
from db import (
    get_videos_pending_summary,
    get_video_by_id_for_processing,
    get_comments_for_video,
    reset_stale_tasks as db_reset_stale_tasks,
    get_stale_processing_summaries,
    get_nonempty_video_local_paths,
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
import urllib.error
import urllib.parse
import urllib.request

try:
    from config import (
        DOWNLOAD_API_BASE_URL,
        DOWNLOAD_SAVE_DIR,
        MAX_DOWNLOAD_SIZE_BYTES,
        ENABLE_VIDEO_COMPRESSION,
        VIDEO_COMPRESSION_CRF,
        VIDEO_COMPRESSION_PRESET,
        WEBGEMINI_API_URL,
        WEBGEMINI_POLL_INTERVAL,
        WEBGEMINI_POLL_MAX_WAIT,
        STALE_PROCESSING_HOURS,
        ORPHAN_FILE_GRACE_HOURS,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_ALLOWED_CHAT_ID,
        BITSTRIPE_UPLOAD_SCRIPT,
        BITSTRIPE_URL_PREFIX,
        DOUYIN_DOWNLOAD_DIR,
    )
except ImportError:
    DOWNLOAD_API_BASE_URL = 'http://127.0.0.1:8000'
    DOWNLOAD_SAVE_DIR = './downloads'
    MAX_DOWNLOAD_SIZE_BYTES = 2 * 1024 * 1024 * 1024
    WEBGEMINI_API_URL = 'http://127.0.0.1:8200'
    WEBGEMINI_POLL_INTERVAL = 5
    WEBGEMINI_POLL_MAX_WAIT = 1800
    STALE_PROCESSING_HOURS = 24
    ORPHAN_FILE_GRACE_HOURS = 24
    ENABLE_VIDEO_COMPRESSION = True
    VIDEO_COMPRESSION_CRF = 32
    VIDEO_COMPRESSION_PRESET = 'veryfast'
    TELEGRAM_BOT_TOKEN = ''
    TELEGRAM_ALLOWED_CHAT_ID = ''
    BITSTRIPE_UPLOAD_SCRIPT = os.path.join(
        os.path.expanduser('~'), '.cursor', 'skills', 'bitstripe-uploader', 'scripts', 'upload.sh',
    )
    BITSTRIPE_URL_PREFIX = 'https://www.bitstripe.cn/files/'
    DOUYIN_DOWNLOAD_DIR = os.path.normpath(
        os.path.join(REPO_DIR, '..', 'Douyin_TikTok_Download_API', 'download')
    )

# Repo root (parent of worker/)
REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER_DIR = os.path.dirname(os.path.abspath(__file__))
TELEGRAM_EXPORTS_DIR = os.path.join(WORKER_DIR, 'telegram_exports')
SCRAPER_SCRIPT = os.path.join(REPO_DIR, 'douyin-scraper.js')

logger = logging.getLogger(__name__)

# Prefixed download error when Content-Length exceeds MAX_DOWNLOAD_SIZE_BYTES (for batch stats).
SKIP_DOWNLOAD_TOO_LARGE_PREFIX = 'SKIP_DOWNLOAD_TOO_LARGE:'


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
    Build prompt: video understanding + shooting/editing analysis + engagement stats + comments
    with per-comment likes, and instructions for narrative + why video got likes + why top
    comments are liked.
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
        '2）拍摄与剪辑技法（请用影视专业术语作答）：按时间线指出在哪些节点（可用大致时间点或情节段落）运用了何种拍摄手法与剪辑/后期手法，并写出对应的专业称谓（例如希区柯克变焦、推轨、手持跟拍、浅景深、跳切、匹配剪辑、交叉剪辑、蒙太奇、升格/降格等）；说明各手法在叙事或情绪上的作用。若画面或剪辑特征不明显，请如实说明。',
        '3）结合评论（若有）：评论者在讨论什么，与画面如何呼应；这条视频整体「记录了什么事情」。',
        '4）为什么可能有这么高点赞：结合画面情绪、话题点与上方互动数据，分析视频为何容易引发点赞（说明你的推理依据）。',
        '5）高赞评论：指出点赞数突出的一条或几条，概括其内容，并分析观众为什么特别「喜欢」这条评论（笑点、梗、共鸣、争议等）。若无评论文本，说明无法逐条分析，仅可结合视频推测可能的讨论方向。',
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


def _parse_content_length_header(value) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


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
    full_url = f"{download_url}?{params}"
    declared_size: int | None = None
    chunk_size = 1024 * 1024

    def _remove_partial_file() -> None:
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass

    try:
        with urllib.request.urlopen(full_url, timeout=300) as resp:
            if resp.status != 200:
                declared_size = _parse_content_length_header(resp.headers.get('Content-Length'))
                logger.error(
                    "Download failed for %s: HTTP %s (declared_content_length=%s)",
                    video_id,
                    resp.status,
                    declared_size,
                )
                return False, None, f"HTTP {resp.status}"

            declared_size = _parse_content_length_header(resp.headers.get('Content-Length'))
            if declared_size is not None and declared_size > MAX_DOWNLOAD_SIZE_BYTES:
                logger.warning(
                    "Skip download for %s: content-length %s exceeds limit %s",
                    video_id,
                    declared_size,
                    MAX_DOWNLOAD_SIZE_BYTES,
                )
                return False, None, (
                    f"{SKIP_DOWNLOAD_TOO_LARGE_PREFIX} content-length {declared_size} bytes "
                    f"exceeds limit {MAX_DOWNLOAD_SIZE_BYTES} bytes"
                )

            bytes_written = 0
            try:
                with open(file_path, 'wb') as out:
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            break
                        out.write(chunk)
                        bytes_written += len(chunk)
            except Exception as stream_err:
                logger.error(
                    "Download failed for %s (declared_content_length=%s, bytes_written=%s): %s",
                    video_id,
                    declared_size,
                    bytes_written,
                    stream_err,
                )
                _remove_partial_file()
                return False, None, str(stream_err)

        update_video_local_path(video_id, file_path)
        return True, file_path, None
    except urllib.error.HTTPError as e:
        declared = _parse_content_length_header(
            e.headers.get('Content-Length') if e.headers else None
        )
        logger.error(
            "Download failed for %s: HTTP %s (declared_content_length=%s)",
            video_id,
            e.code,
            declared,
        )
        _remove_partial_file()
        return False, None, f"HTTP {e.code}"
    except Exception as e:
        _remove_partial_file()
        logger.error(
            "Download failed for %s (declared_content_length=%s): %s",
            video_id,
            declared_size,
            e,
        )
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


def _build_douyin_batch_telegram_body(items):
    """
    Build the full plaintext/markdown body for a successful batch (same as legacy Telegram content).
    items: list of dicts with keys video_id, douyin_url, ai_reply.
    """
    if not items:
        return ''
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
    return '\n'.join(lines).rstrip()


def _write_telegram_batch_report_md(body):
    """
    Persist batch report to a new .md under worker/telegram_exports/.
    Returns absolute path to the file.
    """
    os.makedirs(TELEGRAM_EXPORTS_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    name = f'douyin-crawler-batch-{ts}.md'
    path = os.path.join(TELEGRAM_EXPORTS_DIR, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(body)
        if not body.endswith('\n'):
            f.write('\n')
    logger.info("Saved Telegram batch report to %s", path)
    return path


def _upload_file_via_bitstripe(local_path):
    """
    Upload a local file using the bitstripe-uploader skill script (scp + public URL on stdout).
    Returns HTTPS URL or None on failure.
    """
    script = (BITSTRIPE_UPLOAD_SCRIPT or '').strip()
    if not script or not os.path.isfile(script):
        logger.warning(
            "BitStripe upload skipped: script missing or not a file (%s). "
            "Install ~/.cursor/skills/bitstripe-uploader/scripts/upload.sh or set BITSTRIPE_UPLOAD_SCRIPT.",
            script or '(empty)',
        )
        return None
    try:
        completed = subprocess.run(
            ['bash', script, local_path],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.error("BitStripe upload timed out for %s", local_path)
        return None
    except Exception as e:
        logger.exception("BitStripe upload failed: %s", e)
        return None
    if completed.returncode != 0:
        err = (completed.stderr or completed.stdout or '').strip()
        logger.error(
            "BitStripe upload.sh exit %s for %s: %s",
            completed.returncode,
            local_path,
            err[:500],
        )
        return None
    url = (completed.stdout or '').strip().splitlines()
    url = url[-1].strip() if url else ''
    prefix = (BITSTRIPE_URL_PREFIX or 'https://www.bitstripe.cn/files/').strip()
    if not url.startswith(prefix):
        logger.error("BitStripe upload unexpected stdout (expected URL with prefix %s): %s", prefix, url[:200])
        return None
    logger.info("BitStripe upload ok: %s", url)
    return url


def _split_telegram_text(text):
    if not text:
        return ['']
    return [
        text[i : i + TELEGRAM_MAX_MESSAGE_LEN]
        for i in range(0, len(text), TELEGRAM_MAX_MESSAGE_LEN)
    ]


def _purge_douyin_download_dir():
    """
    Remove files/subdirs under douyin-download's download dir after Telegram notification.
    Keeps the root directory itself.
    """
    target = os.path.abspath(os.path.expanduser((DOUYIN_DOWNLOAD_DIR or '').strip()))
    if not target:
        return {'ok': False, 'reason': 'empty DOUYIN_DOWNLOAD_DIR'}
    if not os.path.exists(target):
        logger.info("Skip purge: DOUYIN_DOWNLOAD_DIR does not exist: %s", target)
        return {'ok': True, 'removed_files': 0, 'removed_dirs': 0, 'target': target, 'reason': 'missing'}
    if not os.path.isdir(target):
        logger.warning("Skip purge: DOUYIN_DOWNLOAD_DIR is not a directory: %s", target)
        return {'ok': False, 'reason': 'not_directory', 'target': target}

    removed_files = 0
    removed_dirs = 0
    for name in os.listdir(target):
        child = os.path.join(target, name)
        try:
            if os.path.isdir(child):
                shutil.rmtree(child)
                removed_dirs += 1
            else:
                os.remove(child)
                removed_files += 1
        except OSError as e:
            logger.warning("Failed to remove %s during purge: %s", child, e)
    logger.info(
        "Purged douyin-download cache dir after Telegram notify: target=%s removed_files=%s removed_dirs=%s",
        target, removed_files, removed_dirs,
    )
    return {
        'ok': True,
        'target': target,
        'removed_files': removed_files,
        'removed_dirs': removed_dirs,
    }


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
    After process_pending_videos, if at least one video succeeded:
    save full report to a new .md, upload via bitstripe upload.sh, send the public URL to Telegram.
    If upload fails, fall back to sending the full report as plain text (legacy behavior).
    items: list of dicts with keys video_id, douyin_url, ai_reply.
    """
    if not items:
        return False
    n = len(items)
    body = _build_douyin_batch_telegram_body(items)
    md_path = _write_telegram_batch_report_md(body)
    public_url = _upload_file_via_bitstripe(md_path)
    if public_url:
        telegram_text = (
            f'douyin-crawler 视频解析完成（本批成功 {n} 条）\n'
            f'完整报告: {public_url}'
        )
    else:
        logger.warning(
            "Sending full batch text on Telegram (BitStripe upload unavailable); see local file %s",
            md_path,
        )
        telegram_text = body
    ok = _send_telegram_plain(telegram_text)
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
                err_msg = err or 'download failed'
                out = {'status': 'failed', 'video_id': video_id, 'error': err_msg}
                if err_msg.startswith(SKIP_DOWNLOAD_TOO_LARGE_PREFIX):
                    out['skip_reason'] = 'too_large'
                return out

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
    skipped_too_large = 0
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
            elif outcome.get('skip_reason') == 'too_large':
                skipped_too_large += 1
                results.append({
                    'video_id': video_id,
                    'status': 'skipped_too_large',
                    'error': outcome.get('error', ''),
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
    download_dir_purge = _purge_douyin_download_dir()

    out = {
        'completed': completed,
        'failed': failed,
        'skipped_too_large': skipped_too_large,
        'total': len(videos),
        'results': results,
        'timestamp': datetime.now().isoformat(),
    }
    if telegram_sent is not None:
        out['telegram_sent'] = telegram_sent
    if download_dir_purge is not None:
        out['download_dir_purge'] = download_dir_purge
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
    logger.info("Resetting stale tasks and stale processing summaries")
    db_reset_stale_tasks(hours=STALE_PROCESSING_HOURS)
    stale_rows = get_stale_processing_summaries(hours=STALE_PROCESSING_HOURS, limit=500)
    stale_summary_reset = 0
    stale_files_removed = 0
    for row in stale_rows:
        video_id = row['video_id']
        local_path = _resolve_video_path(row.get('local_file_path') or '')
        had_file = bool(local_path and os.path.isfile(local_path))
        _remove_local_file_and_clear_db(video_id, local_path)
        if had_file:
            stale_files_removed += 1
        update_video_summary_result(
            video_id,
            'Timeout - reset by scheduler',
            status='failed',
        )
        stale_summary_reset += 1
    return {
        'status': 'completed',
        'timestamp': datetime.now().isoformat(),
        'stale_summary_reset': stale_summary_reset,
        'stale_files_removed': stale_files_removed,
    }


@app.task(name='tasks.cleanup_orphan_download_files')
def cleanup_orphan_download_files():
    """
    Remove old files under DOWNLOAD_SAVE_DIR that are no longer referenced by DB local_file_path.
    """
    download_dir = _get_download_dir()
    if not os.path.isdir(download_dir):
        return {
            'status': 'completed',
            'timestamp': datetime.now().isoformat(),
            'removed': 0,
            'scanned': 0,
            'reason': f'download dir missing: {download_dir}',
        }
    referenced = set()
    for path in get_nonempty_video_local_paths():
        resolved = _resolve_video_path(path)
        if resolved:
            referenced.add(os.path.abspath(resolved))
    now = time.time()
    grace_seconds = max(1, ORPHAN_FILE_GRACE_HOURS) * 3600
    removed = 0
    scanned = 0
    for name in os.listdir(download_dir):
        if not name.startswith('douyin_') or not name.endswith('.mp4'):
            continue
        full_path = os.path.abspath(os.path.join(download_dir, name))
        if not os.path.isfile(full_path):
            continue
        scanned += 1
        if full_path in referenced:
            continue
        age_seconds = now - os.path.getmtime(full_path)
        if age_seconds < grace_seconds:
            continue
        try:
            os.remove(full_path)
            removed += 1
        except OSError as e:
            logger.warning("Could not remove orphan file %s: %s", full_path, e)
    logger.info(
        "Orphan download cleanup completed: scanned=%s removed=%s dir=%s",
        scanned, removed, download_dir,
    )
    return {
        'status': 'completed',
        'timestamp': datetime.now().isoformat(),
        'scanned': scanned,
        'removed': removed,
        'download_dir': download_dir,
    }


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
