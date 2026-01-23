from celery_app import app
from db import (
    get_videos_without_summary,
    get_task_status,
    create_or_get_task,
    start_step,
    complete_step,
    reset_stale_tasks as db_reset_stale_tasks,
)
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

STEPS = ['download', 'submit', 'get_summary']


@app.task(bind=True, name='tasks.process_pending_videos')
def process_pending_videos(self, batch_size=20):
    """
    Scheduled task: Fetch oldest videos without summary and process them.
    """
    logger.info(f"Starting scheduled job: processing up to {batch_size} videos")

    videos = get_videos_without_summary(limit=batch_size)
    logger.info(f"Found {len(videos)} videos to process")

    for video in videos:
        video_id = video['video_id']
        logger.info(f"Queuing video {video_id} for processing")
        # Queue each video as a separate task
        process_video_pipeline.delay(video_id)

    return {
        'queued': len(videos),
        'video_ids': [v['video_id'] for v in videos],
        'timestamp': datetime.now().isoformat()
    }


@app.task(bind=True, name='tasks.process_video_pipeline')
def process_video_pipeline(self, video_id):
    """
    Process a single video through the pipeline.
    Skips already completed steps. Runs all steps in sequence.
    """
    logger.info(f"Processing video {video_id}")

    # Get or create task
    create_or_get_task(video_id)

    # Get current status
    status = get_task_status(video_id)
    completed_steps = status['completed_steps']
    step_results = status['step_results']

    logger.info(f"Video {video_id} - completed steps: {completed_steps}")

    # Process each step in order, skipping completed ones
    for step in STEPS:
        if step in completed_steps:
            logger.info(f"Skipping {step} - already completed")
            continue

        logger.info(f"Executing step: {step}")

        try:
            if step == 'download':
                result = _execute_download(video_id)
            elif step == 'submit':
                download_result = step_results.get('download', {})
                result = _execute_submit(video_id, download_result)
            elif step == 'get_summary':
                submit_result = step_results.get('submit', {})
                result = _execute_get_summary(video_id, submit_result)

            # Update step_results for next iteration
            step_results[step] = result

        except Exception as e:
            logger.error(f"Step {step} failed for video {video_id}: {e}")
            return {'status': 'failed', 'step': step, 'error': str(e)}

    logger.info(f"Video {video_id} pipeline completed")
    return {'status': 'completed', 'video_id': video_id}


def _execute_download(video_id):
    """
    Step 1: Download video.
    TODO: Implement actual download logic.
    """
    logger.info(f"Downloading video {video_id}")
    start_step(video_id, 'download')

    try:
        # TODO: Implement actual video download logic
        # Example:
        # - Fetch video URL from douyin_videos table
        # - Download video file
        # - Store locally or upload to cloud storage

        result = {
            'video_id': video_id,
            'downloaded_at': datetime.now().isoformat(),
            'file_path': f'/tmp/videos/{video_id}.mp4',  # Placeholder
            'status': 'success'
        }

        complete_step(video_id, 'download', result)
        return result

    except Exception as e:
        logger.error(f"Download failed for {video_id}: {e}")
        complete_step(video_id, 'download', None, str(e))
        raise


def _execute_submit(video_id, download_result):
    """
    Step 2: Submit video for processing (e.g., to AI service).
    TODO: Implement actual submit logic.
    """
    logger.info(f"Submitting video {video_id}")
    start_step(video_id, 'submit')

    try:
        # TODO: Implement actual submit logic
        # Example:
        # - Read video file from download_result['file_path']
        # - Upload to AI service (e.g., OpenAI, Claude, etc.)
        # - Get submission ID for tracking

        result = {
            'video_id': video_id,
            'submitted_at': datetime.now().isoformat(),
            'submission_id': f'sub_{video_id}_{int(datetime.now().timestamp())}',  # Placeholder
            'status': 'success'
        }

        complete_step(video_id, 'submit', result)
        return result

    except Exception as e:
        logger.error(f"Submit failed for {video_id}: {e}")
        complete_step(video_id, 'submit', None, str(e))
        raise


def _execute_get_summary(video_id, submit_result):
    """
    Step 3: Get summary from AI service.
    TODO: Implement actual summary retrieval logic.
    """
    logger.info(f"Getting summary for video {video_id}")
    start_step(video_id, 'get_summary')

    try:
        # TODO: Implement actual summary retrieval logic
        # Example:
        # - Poll AI service using submit_result['submission_id']
        # - Wait for processing to complete
        # - Retrieve and store summary

        result = {
            'video_id': video_id,
            'retrieved_at': datetime.now().isoformat(),
            'summary': 'Video summary placeholder - implement actual AI integration',  # Placeholder
            'status': 'success'
        }

        complete_step(video_id, 'get_summary', result)
        return result

    except Exception as e:
        logger.error(f"Get summary failed for {video_id}: {e}")
        complete_step(video_id, 'get_summary', None, str(e))
        raise


@app.task(name='tasks.reset_stale_tasks')
def reset_stale_tasks():
    """Reset tasks that have been stuck in processing state."""
    logger.info("Resetting stale tasks")
    db_reset_stale_tasks(hours=24)
    return {'status': 'completed', 'timestamp': datetime.now().isoformat()}


# Manual trigger task (for testing)
@app.task(bind=True, name='tasks.trigger_batch_now')
def trigger_batch_now(self, batch_size=20):
    """Manually trigger batch processing (for testing)."""
    return process_pending_videos(batch_size)
