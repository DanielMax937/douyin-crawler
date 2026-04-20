#!/usr/bin/env python3
"""
临时：按与 worker/tasks.py 相同的方式拼下载 URL，向本机 Douyin 下载 API 探测体积。

用法（在仓库根目录）:
  cd worker && uv run python ../scripts/check_download_video_sizes.py 7608635061281246500 7596656092772338998

若数据库里有 share_link / short_link，会优先使用；否则用 https://www.douyin.com/video/{id}。
"""
from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def _setup_worker_path() -> Path:
    repo = Path(__file__).resolve().parent.parent
    worker = repo / "worker"
    if not worker.is_dir():
        print("error: expected worker/ next to scripts/", file=sys.stderr)
        sys.exit(1)
    sys.path.insert(0, str(worker))
    os.chdir(worker)
    return worker


def _load_env(worker: Path) -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(worker / ".env")


def _build_page_url(video_id: str, row: dict | None) -> str:
    share = (row or {}).get("share_link") or ""
    short = (row or {}).get("short_link") or ""
    share = str(share).strip()
    short = str(short).strip()
    if share and ("douyin.com" in share or "iesdouyin.com" in share):
        return share
    if short:
        return short
    return f"https://www.douyin.com/video/{video_id}"


def _probe_download_api(download_base: str, page_url: str) -> tuple[int | None, str | None, str | None]:
    """
    Returns (http_status, content_length_str_or_none, note).

    Douyin_TikTok_Download_API 常见行为：对 /api/download 发 HEAD 会 404；发 GET + Range: bytes=0-0
    仍返回 200，并在 Content-Length 里给出整文件大小（便于探测而不拉全量）。
    """
    base = download_base.rstrip("/")
    full = f"{base}/api/download?{urllib.parse.urlencode({'url': page_url})}"

    def _range_get() -> tuple[int, dict]:
        req = urllib.request.Request(full, method="GET", headers={"Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.headers.items()}
            resp.read(64 * 1024)
        return status, headers

    def _head() -> tuple[int, dict]:
        req = urllib.request.Request(full, method="HEAD")
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.headers.items()}
            resp.read()
        return status, headers

    try:
        status, headers = _range_get()
        cl = headers.get("content-length")
        cr = headers.get("content-range")
        if cr and "/" in cr:
            return status, cr.split("/")[-1].strip(), "GET Range Content-Range"
        if cl:
            return status, cl, "GET Range Content-Length"
        return status, None, "GET Range (no size header)"
    except urllib.error.HTTPError:
        try:
            status, headers = _head()
            cl = headers.get("content-length")
            if cl and status == 200:
                return status, cl, "HEAD"
            return status, cl, "HEAD (unexpected status or missing length)"
        except urllib.error.HTTPError as e2:
            return e2.code, None, str(e2)
    except Exception as exc:
        return None, None, str(exc)


def _human(n: int) -> str:
    for unit, label in ((1 << 40, "TiB"), (1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")):
        if n >= unit:
            return f"{n / unit:.3f} {label}"
    return f"{n} B"


def main() -> None:
    worker = _setup_worker_path()
    _load_env(worker)

    from config import DOWNLOAD_API_BASE_URL, MAX_DOWNLOAD_SIZE_BYTES
    from db import get_video_by_id_for_processing

    p = argparse.ArgumentParser(description="Probe download API Content-Length for Douyin video IDs.")
    p.add_argument("video_ids", nargs="+", help="One or more video_id strings")
    args = p.parse_args()

    print(f"DOWNLOAD_API_BASE_URL={DOWNLOAD_API_BASE_URL}")
    print(f"MAX_DOWNLOAD_SIZE_BYTES={MAX_DOWNLOAD_SIZE_BYTES} (~{_human(MAX_DOWNLOAD_SIZE_BYTES)})")
    print()

    for vid in args.video_ids:
        row = get_video_by_id_for_processing(vid)
        if row is not None:
            row = dict(row)
        page = _build_page_url(vid, row)
        title = (row or {}).get("title") if row else None
        print(f"video_id={vid}")
        if title:
            print(f"  title: {title[:120]}{'…' if title and len(title) > 120 else ''}")
        print(f"  page_url: {page}")

        status, cl, note = _probe_download_api(DOWNLOAD_API_BASE_URL, page)
        print(f"  probe: status={status} via={note}")
        if cl is None:
            print("  content_length: (unknown — API 未返回长度，或 HEAD 不支持且 Range 无 Content-Range)")
        else:
            try:
                n = int(cl)
                over = n > MAX_DOWNLOAD_SIZE_BYTES
                print(f"  content_length: {n} bytes (~{_human(n)})")
                print(f"  exceeds MAX_DOWNLOAD_SIZE_BYTES: {over}")
            except ValueError:
                print(f"  content_length: {cl!r} (unparseable)")
        print()


if __name__ == "__main__":
    main()
