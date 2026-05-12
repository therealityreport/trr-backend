"""Small TikTok route-facing parser helpers.

These helpers parse direct profile preview payloads only. They are not part of
the queued TikTok posts Scraping lane and do not define a comments ingestion
contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def extract_profile_preview_sections(data: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Return the user and stats sections from TikTok's profile detail payload."""

    user_info = data.get("userInfo", {})
    if not isinstance(user_info, Mapping):
        return {}, {}

    user_data = user_info.get("user", {})
    stats = user_info.get("stats", {})

    return (
        user_data if isinstance(user_data, Mapping) else {},
        stats if isinstance(stats, Mapping) else {},
    )
