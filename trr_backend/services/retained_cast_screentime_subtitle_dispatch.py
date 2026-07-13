"""Retained dispatch boundary for embedded cast-screentime subtitles."""

from __future__ import annotations

from typing import Any

from trr_backend import modal_dispatch


def extract_video_asset_subtitles(video_asset_id: str, force: bool = False) -> dict[str, Any]:
    """Queue extraction and return the standard Modal dispatch payload."""

    return modal_dispatch.dispatch_cast_screentime_subtitle_extraction(
        video_asset_id=str(video_asset_id or "").strip(),
        force=bool(force),
    )
