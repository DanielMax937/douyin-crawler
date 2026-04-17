#!/usr/bin/env python3
"""
Unit tests for tasks.py

Run with: uv run python -m pytest test_tasks.py -v
"""

import pytest
import os
import shutil
import tempfile
from unittest.mock import patch


class TestProcessPendingVideos:
    """Test batch processing (download → webgemini → delete)."""

    @patch('tasks._notify_telegram_douyin_success_batch')
    @patch('tasks._process_one_video_download_summary_delete')
    @patch('tasks.get_videos_pending_summary')
    def test_processes_all_found_videos(self, mock_get_videos, mock_one, mock_notify):
        """Should process all videos returned by get_videos_pending_summary."""
        from tasks import process_pending_videos

        mock_notify.return_value = True
        mock_get_videos.return_value = [
            {'video_id': 'vid1', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
            {'video_id': 'vid2', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
        ]
        mock_one.return_value = {
            'status': 'completed',
            'douyin_url': 'https://example.com/v',
            'ai_reply': 'ok',
        }

        result = process_pending_videos(batch_size=10)

        assert result['total'] == 2
        assert result['completed'] == 2
        assert mock_one.call_count == 2
        mock_notify.assert_called_once_with([
            {'video_id': 'vid1', 'douyin_url': 'https://example.com/v', 'ai_reply': 'ok'},
            {'video_id': 'vid2', 'douyin_url': 'https://example.com/v', 'ai_reply': 'ok'},
        ])

    @patch('tasks._process_one_video_download_summary_delete')
    @patch('tasks.get_videos_pending_summary')
    def test_respects_batch_size(self, mock_get_videos, mock_one):
        """Should pass batch_size as limit to get_videos_pending_summary."""
        from tasks import process_pending_videos

        mock_get_videos.return_value = []
        mock_one.return_value = {'status': 'completed'}

        process_pending_videos(batch_size=5)

        mock_get_videos.assert_called_once_with(limit=5)

    @patch('tasks._notify_telegram_douyin_success_batch')
    @patch('tasks._process_one_video_download_summary_delete')
    @patch('tasks.get_videos_pending_summary')
    def test_telegram_when_exactly_one_success(self, mock_get, mock_one, mock_notify):
        """When batch has one successful video, notify Telegram with link + AI reply."""
        from tasks import process_pending_videos

        mock_notify.return_value = True
        mock_get.return_value = [
            {'video_id': 'v1', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
        ]
        mock_one.return_value = {
            'status': 'completed',
            'douyin_url': 'https://www.douyin.com/video/v1',
            'ai_reply': 'analysis text',
        }
        result = process_pending_videos(batch_size=10)

        assert result['completed'] == 1
        mock_notify.assert_called_once_with([
            {
                'video_id': 'v1',
                'douyin_url': 'https://www.douyin.com/video/v1',
                'ai_reply': 'analysis text',
            },
        ])
        assert result.get('telegram_sent') is True

    @patch('tasks._notify_telegram_douyin_success_batch')
    @patch('tasks._process_one_video_download_summary_delete')
    @patch('tasks.get_videos_pending_summary')
    def test_telegram_when_two_successes(self, mock_get, mock_one, mock_notify):
        """When batch has multiple successes, notify once with all rows."""
        from tasks import process_pending_videos

        mock_notify.return_value = True
        mock_get.return_value = [
            {'video_id': 'a', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
            {'video_id': 'b', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
        ]
        mock_one.return_value = {'status': 'completed', 'douyin_url': 'u', 'ai_reply': 't'}
        result = process_pending_videos(batch_size=10)

        assert result['completed'] == 2
        mock_notify.assert_called_once_with([
            {'video_id': 'a', 'douyin_url': 'u', 'ai_reply': 't'},
            {'video_id': 'b', 'douyin_url': 'u', 'ai_reply': 't'},
        ])
        assert result.get('telegram_sent') is True

    @patch('tasks._notify_telegram_douyin_success_batch')
    @patch('tasks._process_one_video_download_summary_delete')
    @patch('tasks.get_videos_pending_summary')
    def test_no_telegram_when_zero_successes(self, mock_get, mock_one, mock_notify):
        from tasks import process_pending_videos

        mock_get.return_value = [
            {'video_id': 'a', 'share_link': 'x', 'short_link': '', 'local_file_path': None},
        ]
        mock_one.return_value = {'status': 'failed', 'error': 'x'}
        result = process_pending_videos(batch_size=10)

        assert result['completed'] == 0
        mock_notify.assert_not_called()
        assert 'telegram_sent' not in result


class TestProcessOneVideoPipeline:
    """Merged pipeline: download → webgemini → delete."""

    @patch('tasks.get_comments_for_video')
    @patch('tasks._remove_local_file_and_clear_db')
    @patch('tasks.update_video_summary_result')
    @patch('tasks.create_or_update_video_summary')
    @patch('tasks._poll_webgemini_chat')
    @patch('tasks._submit_webgemini_chat')
    @patch('tasks._compress_video_for_upload')
    @patch('tasks._download_one_video')
    @patch('tasks._resolve_video_path')
    def test_downloads_when_no_local_file(
        self, mock_resolve, mock_dl, mock_compress, mock_submit, mock_poll,
        mock_create, mock_update, mock_remove, mock_comments,
    ):
        from tasks import _process_one_video_download_summary_delete

        mock_resolve.return_value = None
        mock_dl.return_value = (True, '/tmp/douyin_vid.mp4', None)
        mock_compress.return_value = '/tmp/douyin_vid_compressed.mp4'
        mock_submit.return_value = 'job1'
        mock_poll.return_value = ('completed', 'summary text', None)
        mock_comments.return_value = []

        video = {
            'video_id': 'vid1',
            'share_link': 'https://www.douyin.com/video/vid1',
            'short_link': '',
            'local_file_path': None,
        }
        with patch('os.path.isfile', return_value=True):
            out = _process_one_video_download_summary_delete(video)

        assert out['status'] == 'completed'
        mock_dl.assert_called_once()
        mock_compress.assert_called_once_with('/tmp/douyin_vid.mp4')
        mock_submit.assert_called_once()
        call_prompt, call_paths = mock_submit.call_args[0]
        assert '高赞评论' in call_prompt
        assert call_paths == ['/tmp/douyin_vid_compressed.mp4']
        mock_remove.assert_called_once()

    @patch('tasks.get_comments_for_video')
    @patch('tasks._remove_local_file_and_clear_db')
    @patch('tasks._compress_video_for_upload')
    @patch('tasks._download_one_video')
    @patch('tasks._resolve_video_path')
    def test_skips_download_when_file_exists(
        self, mock_resolve, mock_dl, mock_compress, mock_remove, mock_comments,
    ):
        from tasks import _process_one_video_download_summary_delete

        mock_resolve.return_value = '/existing/vid.mp4'
        mock_compress.return_value = '/existing/vid_compressed.mp4'
        mock_comments.return_value = []
        with patch('tasks._submit_webgemini_chat', return_value='j') as mock_submit, \
                patch('tasks.create_or_update_video_summary'), \
                patch('tasks._poll_webgemini_chat', return_value=('completed', 't', None)), \
                patch('tasks.update_video_summary_result'), \
                patch('os.path.isfile', return_value=True):
            out = _process_one_video_download_summary_delete({
                'video_id': 'v',
                'share_link': 'https://www.douyin.com/video/v',
                'short_link': '',
                'local_file_path': '/existing/vid.mp4',
            })

        assert out['status'] == 'completed'
        mock_dl.assert_not_called()
        mock_compress.assert_called_once_with('/existing/vid.mp4')
        mock_submit.assert_called_once()
        assert '转发/分享数' in mock_submit.call_args[0][0]
        assert mock_submit.call_args[0][1] == ['/existing/vid_compressed.mp4']
        mock_remove.assert_called_once()


class TestCompressionAndUploadPath:
    """Integration-style test for compress -> upload -> poll path."""

    def test_compresses_and_uploads_sample_video(self):
        from tasks import (
            _build_webgemini_summary_prompt,
            _compress_video_for_upload,
            _poll_webgemini_chat,
            _submit_webgemini_chat,
        )

        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sample_video = os.path.join(
            repo_root, 'worker', 'downloads', 'douyin_7607831118478603402.mp4',
        )
        assert os.path.isfile(sample_video), f"Sample video missing: {sample_video}"
        assert os.path.getsize(sample_video) >= 40 * 1024 * 1024, "Sample video should be a larger MP4"

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_video = os.path.join(tmpdir, 'sample.mp4')
            shutil.copyfile(sample_video, temp_video)
            compressed_path = _compress_video_for_upload(temp_video)
            assert compressed_path.endswith('_compressed.mp4')
            assert os.path.isfile(compressed_path)

            try:
                prompt = _build_webgemini_summary_prompt(
                    {'video_id': 'integration-test'}, [],
                )
                job_id = _submit_webgemini_chat(prompt, [compressed_path])
                assert job_id

                # Real integration polling against WebGemini until terminal status.
                status, text, error = _poll_webgemini_chat(
                    job_id,
                    poll_interval=int(os.getenv('TEST_WEBGEMINI_POLL_INTERVAL', '5')),
                    max_wait=int(os.getenv('TEST_WEBGEMINI_MAX_WAIT', '2400')),
                )

                assert status in ('completed', 'failed')
                if status == 'completed':
                    assert text
                    assert not error
                else:
                    assert error
            finally:
                if os.path.exists(compressed_path):
                    os.remove(compressed_path)

            assert os.path.exists(temp_video)
            assert not os.path.exists(compressed_path)


class TestResetStaleTasks:
    """Test the reset stale tasks Celery task."""

    @patch('tasks.db_reset_stale_tasks')
    def test_calls_db_reset(self, mock_db_reset):
        """Should call db reset function with 24 hours."""
        from tasks import reset_stale_tasks

        result = reset_stale_tasks()

        mock_db_reset.assert_called_once_with(hours=24)
        assert result['status'] == 'completed'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
