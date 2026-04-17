import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
from config import POSTGRES_CONFIG


def get_connection():
    """Get a PostgreSQL connection."""
    return psycopg2.connect(**POSTGRES_CONFIG)


def get_videos_without_summary(limit=20):
    """
    Fetch oldest videos that don't have a completed summary.
    Returns videos where:
    - No task exists, OR
    - Task exists but not completed
    Uses FOR UPDATE SKIP LOCKED to prevent race conditions.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT v.video_id, v.title, v.author, v.share_link,
                       t.current_step, t.status as task_status
                FROM douyin_videos v
                LEFT JOIN video_tasks t ON v.video_id = t.video_id
                WHERE t.id IS NULL
                   OR (t.status != 'completed' AND t.status != 'processing')
                ORDER BY v.created_at ASC
                LIMIT %s
                FOR UPDATE OF v SKIP LOCKED
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def get_task_status(video_id):
    """Get current task status and completed steps for a video."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get task
            cur.execute("""
                SELECT * FROM video_tasks WHERE video_id = %s
            """, (video_id,))
            task = cur.fetchone()

            # Get completed steps
            cur.execute("""
                SELECT step_name, status, result
                FROM video_task_steps
                WHERE video_id = %s AND status = 'completed'
                ORDER BY completed_at
            """, (video_id,))
            completed_steps = cur.fetchall()

            return {
                'task': task,
                'completed_steps': [s['step_name'] for s in completed_steps],
                'step_results': {s['step_name']: s['result'] for s in completed_steps}
            }
    finally:
        conn.close()


def create_or_get_task(video_id):
    """Create a new task or get existing one."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO video_tasks (video_id, current_step, status)
                VALUES (%s, 'pending', 'pending')
                ON CONFLICT (video_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
                RETURNING *
            """, (video_id,))
            conn.commit()
            return cur.fetchone()
    finally:
        conn.close()


def start_step(video_id, step_name):
    """Mark a step as started."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Update task current step
            cur.execute("""
                UPDATE video_tasks
                SET current_step = %s, status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE video_id = %s
            """, (step_name, video_id))

            # Insert or update step record
            cur.execute("""
                INSERT INTO video_task_steps (video_id, step_name, status, started_at)
                VALUES (%s, %s, 'processing', CURRENT_TIMESTAMP)
                ON CONFLICT (video_id, step_name) DO UPDATE SET
                    status = 'processing',
                    started_at = CURRENT_TIMESTAMP,
                    error_message = NULL
            """, (video_id, step_name))

            conn.commit()
    finally:
        conn.close()


def complete_step(video_id, step_name, result, error=None):
    """Mark a step as completed or failed."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            status = 'failed' if error else 'completed'

            # Update step record
            cur.execute("""
                UPDATE video_task_steps
                SET status = %s, result = %s, error_message = %s, completed_at = CURRENT_TIMESTAMP
                WHERE video_id = %s AND step_name = %s AND status = 'processing'
            """, (status, json.dumps(result) if result else None, error, video_id, step_name))

            # Update task status
            if error:
                cur.execute("""
                    UPDATE video_tasks
                    SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = %s
                """, (video_id,))
            elif step_name == 'get_summary':
                # Final step completed
                cur.execute("""
                    UPDATE video_tasks
                    SET current_step = 'completed', status = 'completed', updated_at = CURRENT_TIMESTAMP
                    WHERE video_id = %s
                """, (video_id,))

            conn.commit()
    finally:
        conn.close()


def get_videos_without_local_file(limit=500, include_today=True):
    """
    Fetch videos that don't have local_file_path set.
    If include_today: created_at >= yesterday (today + yesterday)
    Else: created_at::date = yesterday only.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if include_today:
                cur.execute("""
                    SELECT video_id, share_link, short_link
                    FROM douyin_videos
                    WHERE created_at::date >= CURRENT_DATE - INTERVAL '1 day'
                      AND (local_file_path IS NULL OR local_file_path = '')
                      AND (share_link IS NOT NULL AND share_link != ''
                           OR short_link IS NOT NULL AND short_link != '')
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT video_id, share_link, short_link
                    FROM douyin_videos
                    WHERE created_at::date = CURRENT_DATE - INTERVAL '1 day'
                      AND (local_file_path IS NULL OR local_file_path = '')
                      AND (share_link IS NOT NULL AND share_link != ''
                           OR short_link IS NOT NULL AND short_link != '')
                    ORDER BY created_at ASC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def get_videos_created_yesterday_without_local_file(limit=500):
    """Fetch videos created yesterday that don't have local_file_path set."""
    return get_videos_without_local_file(limit=limit, include_today=False)


def update_video_local_path(video_id, local_path):
    """Update douyin_videos with the local file path after download."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE douyin_videos
                SET local_file_path = %s, updated_at = CURRENT_TIMESTAMP
                WHERE video_id = %s
            """, (local_path, video_id))
            conn.commit()
    finally:
        conn.close()


def get_video_by_id_for_processing(video_id):
    """Fetch a single video row (for download + summary pipeline)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT video_id, share_link, short_link, local_file_path,
                       title, author,
                       COALESCE(likes, 0) AS likes,
                       likes_display,
                       COALESCE(comments_count, 0) AS comments_count,
                       comments_display,
                       COALESCE(shares, 0) AS shares,
                       shares_display
                FROM douyin_videos
                WHERE video_id = %s
            """, (video_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_comments_for_video(video_id):
    """
    Comments scraped for this video, ordered by comment likes (desc) then id.
    Returns list of dicts: username, content, time, location, likes.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT username, content, time, location, COALESCE(likes, 0) AS likes
                FROM douyin_comments
                WHERE video_id = %s
                ORDER BY likes DESC NULLS LAST, id ASC
            """, (video_id,))
            return cur.fetchall()
    finally:
        conn.close()


def clear_video_local_path(video_id):
    """Clear local_file_path after the file is deleted."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE douyin_videos
                SET local_file_path = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE video_id = %s
            """, (video_id,))
            conn.commit()
    finally:
        conn.close()


def get_videos_pending_summary(limit=20):
    """
    Videos needing summary: no summary row or failed summary.
    Must have share_link, short_link, and/or existing local_file_path (legacy).
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT v.video_id, v.share_link, v.short_link, v.local_file_path,
                       v.title, v.author,
                       COALESCE(v.likes, 0) AS likes,
                       v.likes_display,
                       COALESCE(v.comments_count, 0) AS comments_count,
                       v.comments_display,
                       COALESCE(v.shares, 0) AS shares,
                       v.shares_display
                FROM douyin_videos v
                LEFT JOIN douyin_video_summaries s ON v.video_id = s.video_id
                WHERE (s.id IS NULL OR s.status = 'failed')
                  AND (
                    (v.local_file_path IS NOT NULL AND v.local_file_path != '')
                    OR (v.share_link IS NOT NULL AND v.share_link != '')
                    OR (v.short_link IS NOT NULL AND v.short_link != '')
                  )
                ORDER BY v.created_at ASC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def create_or_update_video_summary(video_id, douyin_url, webgemini_job_id=None, webgemini_result=None, status='pending'):
    """Create or update douyin_video_summaries record."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO douyin_video_summaries (video_id, douyin_url, webgemini_job_id, webgemini_result, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE SET
                    douyin_url = EXCLUDED.douyin_url,
                    webgemini_job_id = COALESCE(EXCLUDED.webgemini_job_id, douyin_video_summaries.webgemini_job_id),
                    webgemini_result = COALESCE(EXCLUDED.webgemini_result, douyin_video_summaries.webgemini_result),
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
            """, (video_id, douyin_url, webgemini_job_id, webgemini_result, status))
            conn.commit()
    finally:
        conn.close()


def update_video_summary_result(video_id, webgemini_result, status='completed'):
    """Update webgemini_result and status for a video summary."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE douyin_video_summaries
                SET webgemini_result = %s, status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE video_id = %s
            """, (webgemini_result, status, video_id))
            conn.commit()
    finally:
        conn.close()


def reset_stale_tasks(hours=24):
    """Reset tasks that have been processing for too long."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE video_tasks
                SET status = 'pending', updated_at = CURRENT_TIMESTAMP
                WHERE status = 'processing'
                AND updated_at < NOW() - INTERVAL '1 hour' * %s
            """, (hours,))

            cur.execute("""
                UPDATE video_task_steps
                SET status = 'failed', error_message = 'Timeout - reset by scheduler'
                WHERE status = 'processing'
                AND started_at < NOW() - INTERVAL '1 hour' * %s
            """, (hours,))

            conn.commit()
    finally:
        conn.close()


def get_stale_processing_summaries(hours=24, limit=500):
    """
    Fetch stale summary rows stuck in processing for too long.
    Returns video_id + current local_file_path for cleanup.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT s.video_id, v.local_file_path
                FROM douyin_video_summaries s
                JOIN douyin_videos v ON v.video_id = s.video_id
                WHERE s.status = 'processing'
                  AND s.updated_at < NOW() - INTERVAL '1 hour' * %s
                ORDER BY s.updated_at ASC
                LIMIT %s
            """, (hours, limit))
            return cur.fetchall()
    finally:
        conn.close()


def get_nonempty_video_local_paths():
    """Return all non-empty local_file_path values currently referenced in DB."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT local_file_path
                FROM douyin_videos
                WHERE local_file_path IS NOT NULL
                  AND local_file_path != ''
            """)
            rows = cur.fetchall()
            return [row[0] for row in rows]
    finally:
        conn.close()
