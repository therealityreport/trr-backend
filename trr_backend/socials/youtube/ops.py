"""Operator helpers for YouTube social scripts."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from trr_backend.socials.youtube.scraper import YouTubeVideo

logger = logging.getLogger(__name__)


def default_download_root() -> Path:
    """Return a cache-like root outside the repo for ad hoc media downloads."""
    override = str(os.getenv("TRR_WORKSPACE_CACHE_ROOT") or "").strip()
    if override:
        return Path(override).expanduser() / "youtube-downloads"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "TRR" / "youtube-downloads"
    return Path.home() / ".cache" / "trr" / "youtube-downloads"


def resolve_download_dir(download_dir: str | None, channel_handle: str) -> Path:
    """Resolve the target download directory for yt-dlp media exports."""
    if download_dir:
        return Path(download_dir).expanduser().resolve()
    return default_download_root() / channel_handle


def download_videos(videos: list[YouTubeVideo], output_dir: Path) -> None:
    """Download videos/shorts at best available quality using yt-dlp."""
    if not shutil.which("yt-dlp"):
        logger.error("yt-dlp is not installed. Install with: pip install yt-dlp")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    succeeded = 0
    failed = 0

    for i, video in enumerate(videos, 1):
        label = "Short" if video.is_short else "Video"
        title_preview = video.title[:50] + "..." if len(video.title) > 50 else video.title
        print(f"\n[{i}/{len(videos)}] Downloading {label}: {title_preview}")

        output_template = str(output_dir / "%(id)s.%(ext)s")
        cmd = [
            "yt-dlp",
            "--format",
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best",
            "--merge-output-format",
            "mp4",
            "--output",
            output_template,
            "--no-playlist",
            "--write-thumbnail",
            "--convert-thumbnails",
            "jpg",
            "--no-overwrites",
            video.url,
        ]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
            if proc.returncode == 0:
                succeeded += 1
                logger.info("  Downloaded: %s", video.video_id)
            else:
                failed += 1
                error_msg = (proc.stderr or proc.stdout or "").strip()[:200]
                logger.error("  Failed to download %s: %s", video.video_id, error_msg)
        except subprocess.TimeoutExpired:
            failed += 1
            logger.error("  Download timed out for %s", video.video_id)

    print(f"\nDownload complete: {succeeded} succeeded, {failed} failed")
    print(f"Files saved to: {output_dir}")
