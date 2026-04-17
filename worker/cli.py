#!/usr/bin/env python3
"""
CLI tool to manage video processing tasks.

Usage:
    python cli.py status              # Show pending videos and task status
    python cli.py trigger [count]     # Create tasks for videos without summary and process them
    python cli.py process <video_id>  # Process a single video
    python cli.py reset               # Reset stale tasks

Crontab example (run daily at 8am):
    0 8 * * * cd /path/to/worker && uv run python cli.py trigger 20
"""

import sys
import argparse
from datetime import datetime
from db import (
    get_videos_without_summary,
    get_task_status,
    reset_stale_tasks,
    create_or_get_task,
    get_connection,
)
from tasks import process_pending_videos, process_one_video_summary


def show_status():
    """Show current status of videos and tasks."""
    print("=" * 60)
    print("DOUYIN VIDEO TASK STATUS")
    print("=" * 60)

    # Get pending videos
    videos = get_videos_without_summary(limit=50)
    print(f"\n📹 Videos without summary: {len(videos)}")

    if videos:
        print("\nOldest 10 pending videos:")
        print("-" * 60)
        for v in videos[:10]:
            status = v.get('task_status') or 'no task'
            step = v.get('current_step') or '-'
            print(f"  {v['video_id'][:20]}... | {status:12} | step: {step}")

    # Get task statistics
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*) as count
                FROM video_tasks
                GROUP BY status
            """)
            stats = cur.fetchall()

            print("\n📊 Task Statistics:")
            print("-" * 60)
            for status, count in stats:
                print(f"  {status}: {count}")

            cur.execute("""
                SELECT step_name, status, COUNT(*) as count
                FROM video_task_steps
                GROUP BY step_name, status
                ORDER BY step_name, status
            """)
            step_stats = cur.fetchall()

            print("\n📋 Step Statistics:")
            print("-" * 60)
            for step, status, count in step_stats:
                print(f"  {step:15} | {status:12} | {count}")
    except Exception as e:
        print(f"\n⚠️  Error fetching statistics: {e}")
    finally:
        conn.close()

    print("\n" + "=" * 60)


def trigger_batch(count=20):
    """Create tasks for videos without summary and trigger processing."""
    print(f"🚀 Finding up to {count} videos without summary...")

    videos = get_videos_without_summary(limit=count)
    print(f"   Found {len(videos)} videos to process")

    if not videos:
        print("   No videos need processing")
        return

    # Create tasks for each video
    created = 0
    for video in videos:
        video_id = video['video_id']
        task = create_or_get_task(video_id)
        if task:
            created += 1
            print(f"   📋 Created/updated task for {video_id}")

    print(f"\n📤 Queuing batch Celery task (download → WebGemini → delete)...")

    result = process_pending_videos.delay(count)
    print(f"   Task ID: {result.id}")
    print("   Processing started in background")
    print("\n   Use 'python cli.py status' to check progress")


def process_single(video_id):
    """Process a single video."""
    print(f"🎬 Processing video: {video_id}")

    # Create task if not exists
    task = create_or_get_task(video_id)
    if not task:
        print(f"   ⚠️  Failed to create task")
        return

    # Show current status
    status = get_task_status(video_id)
    if status['task']:
        print(f"   Current status: {status['task']['status']}")
        print(f"   Current step: {status['task']['current_step']}")
        print(f"   Completed steps: {status['completed_steps']}")

    result = process_one_video_summary.delay(video_id)
    print(f"   Task ID: {result.id}")
    print("   Processing... (check worker logs for progress)")


def reset_tasks():
    """Reset stale tasks."""
    print("🔄 Resetting stale tasks...")
    reset_stale_tasks(hours=24)
    print("   ✅ Done")


def main():
    parser = argparse.ArgumentParser(description='Douyin Video Task Manager')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # status command
    subparsers.add_parser('status', help='Show pending videos and task status')

    # trigger command
    trigger_parser = subparsers.add_parser('trigger', help='Create tasks and trigger batch processing')
    trigger_parser.add_argument('count', type=int, nargs='?', default=20, help='Number of videos to process')

    # process command
    process_parser = subparsers.add_parser('process', help='Process a single video')
    process_parser.add_argument('video_id', help='Video ID to process')

    # reset command
    subparsers.add_parser('reset', help='Reset stale tasks')

    args = parser.parse_args()

    if args.command == 'status':
        show_status()
    elif args.command == 'trigger':
        trigger_batch(args.count)
    elif args.command == 'process':
        process_single(args.video_id)
    elif args.command == 'reset':
        reset_tasks()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
