#!/usr/bin/env python3
"""
临时脚本：触发 Celery 的 process_pending_videos（下载 → WebGemini → 删除本地文件）。

用法：
  cd /Users/caoxiaopeng/Desktop/git/douyin-crawler/worker
  python ../scripts/trigger_download_yesterday.py

依赖：
  - Redis 运行中
  - Douyin_TikTok_Download_API 服务运行在 127.0.0.1:8000
  - Webgemini 运行在 127.0.0.1:8200
  - PostgreSQL 可连接
"""

import os
import sys

WORKER_DIR = os.path.join(os.path.dirname(__file__), '..', 'worker')
os.chdir(WORKER_DIR)
sys.path.insert(0, WORKER_DIR)


def main():
    print("=" * 60)
    print("1. 触发 process_pending_videos 任务")
    print("=" * 60)

    from tasks import process_pending_videos

    result = process_pending_videos.apply(kwargs={'batch_size': 20})
    print(f"\n任务结果: {result.result}")
    print()

    print("=" * 60)
    print("2. 说明：成功后 local_file_path 会清空，本地 mp4 已删除")
    print("=" * 60)


if __name__ == '__main__':
    main()
