"""Route-independent Twitter/X search query helpers."""

from __future__ import annotations

import re
from datetime import datetime, timedelta

ADVANCED_QUERY_HINT_RE = re.compile(
    r'(^|\s)(from:|to:|since:|until:|filter:|-filter:)|\bOR\b|\bAND\b|[()"]',
    re.IGNORECASE,
)
WHOLE_DAY_WINDOW_CONTRACT = "whole_day"


def normalize_twitter_search_window(date_start: datetime, date_end: datetime) -> tuple[datetime, datetime]:
    """Normalize the public Twitter search contract to whole-day bounds."""
    start_day = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day_exclusive = date_end.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return start_day, end_day_exclusive


def build_twitter_search_query(query: str, date_start: datetime, date_end: datetime) -> str:
    """Build a Twitter advanced search query string for the normalized window."""
    parts: list[str] = []
    normalized_query = query.strip()

    if ADVANCED_QUERY_HINT_RE.search(normalized_query):
        parts.append(normalized_query)
    elif normalized_query.startswith("#") or normalized_query.startswith("@"):
        parts.append(normalized_query)
    else:
        parts.append(f'"{normalized_query}" OR #{normalized_query}')

    parts.append(f"since:{date_start.strftime('%Y-%m-%d')}")
    parts.append(f"until:{date_end.strftime('%Y-%m-%d')}")
    return " ".join(parts)
