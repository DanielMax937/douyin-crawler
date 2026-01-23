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

            # Insert step record
            cur.execute("""
                INSERT INTO video_task_steps (video_id, step_name, status, started_at)
                VALUES (%s, %s, 'processing', CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING
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


def reset_stale_tasks(hours=24):
    """Reset tasks that have been processing for too long."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE video_tasks
                SET status = 'pending', updated_at = CURRENT_TIMESTAMP
                WHERE status = 'processing'
                AND updated_at < NOW() - INTERVAL '%s hours'
            """, (hours,))

            cur.execute("""
                UPDATE video_task_steps
                SET status = 'failed', error_message = 'Timeout - reset by scheduler'
                WHERE status = 'processing'
                AND started_at < NOW() - INTERVAL '%s hours'
            """, (hours,))

            conn.commit()
    finally:
        conn.close()
