"""Auto-categorize Reddit community flairs as 'cast' or 'season'."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from trr_backend.db import pg

logger = logging.getLogger(__name__)

# Season patterns: S1, S2, ..., Season 1, Season 2, etc.
_SEASON_PATTERN = re.compile(r"^(?:s|season\s*)(\d{1,2})$", re.IGNORECASE)


def _normalize_for_matching(text: str) -> str:
    """Lowercase, strip emoji shortcodes like :name:, and collapse whitespace."""
    cleaned = re.sub(r":[^:\s]+:", "", text)  # strip Reddit emoji shortcodes
    cleaned = re.sub(r"[^\w\s]", "", cleaned)  # strip punctuation
    return " ".join(cleaned.lower().split())


def _get_cast_names(show_id: str) -> list[dict[str, str]]:
    """Fetch cast member names for a show from core.show_cast + core.people."""
    sql = """
        SELECT DISTINCT
            COALESCE(po.full_name_override, p.full_name) AS display_name
        FROM core.show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.people_overrides po ON po.person_id = p.id
        WHERE sc.show_id = %s
        ORDER BY display_name
    """
    with pg.db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (show_id,))
            rows = cur.fetchall()

    names = []
    for row in rows:
        full_name = (row[0] or "").strip()
        if not full_name:
            continue
        parts = full_name.split()
        entry: dict[str, str] = {"full": full_name.lower()}
        if len(parts) >= 2:
            entry["first"] = parts[0].lower()
            entry["last"] = parts[-1].lower()
        elif len(parts) == 1:
            entry["first"] = parts[0].lower()
        names.append(entry)
    return names


def _get_season_numbers(show_id: str) -> set[int]:
    """Fetch season numbers for a show."""
    sql = "SELECT season_number FROM core.seasons WHERE show_id = %s AND season_number IS NOT NULL"
    with pg.db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (show_id,))
            return {row[0] for row in cur.fetchall()}


def _get_community_flairs(community_id: str) -> list[str]:
    """Fetch post_flairs from admin.reddit_communities."""
    sql = "SELECT post_flairs FROM admin.reddit_communities WHERE id = %s"
    with pg.db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (community_id,))
            row = cur.fetchone()
    if not row or not row[0]:
        return []
    flairs = row[0]
    if isinstance(flairs, str):
        flairs = json.loads(flairs)
    return [str(f) for f in flairs if isinstance(f, str)]


def _categorize_flair(
    flair_text: str,
    cast_names: list[dict[str, str]],
    season_numbers: set[int],
) -> str | None:
    """Return 'cast', 'season', or None for a single flair."""
    normalized = _normalize_for_matching(flair_text)
    if not normalized:
        return None

    # Check season patterns
    match = _SEASON_PATTERN.match(normalized)
    if match:
        num = int(match.group(1))
        if num in season_numbers or num <= 30:  # generous range for seasons
            return "season"

    # Check cast names
    for name_entry in cast_names:
        full = name_entry.get("full", "")
        first = name_entry.get("first", "")
        last = name_entry.get("last", "")

        # Exact full name match
        if full and normalized == full:
            return "cast"
        # Full name contained in flair
        if full and full in normalized:
            return "cast"
        # First + last name both present
        if first and last and first in normalized and last in normalized:
            return "cast"
        # Last name match (only if 4+ chars to avoid false positives)
        if last and len(last) >= 4 and normalized == last:
            return "cast"

    return None


def auto_categorize_flairs(*, community_id: str, show_id: str) -> dict[str, Any]:
    """Auto-categorize flairs for a single community."""
    cast_names = _get_cast_names(show_id)
    season_numbers = _get_season_numbers(show_id)
    flairs = _get_community_flairs(community_id)

    categories: dict[str, str] = {}
    for flair in flairs:
        canonical = _normalize_for_matching(flair)
        if not canonical:
            continue
        cat = _categorize_flair(flair, cast_names, season_numbers)
        if cat:
            categories[canonical] = cat

    return {
        "categories": categories,
        "matched": len(categories),
        "total": len(flairs),
    }


def _get_communities_for_show(show_id: str) -> list[dict[str, Any]]:
    """Get all reddit communities linked to a show."""
    sql = """
        SELECT id, subreddit, post_flairs
        FROM admin.reddit_communities
        WHERE trr_show_id = %s
    """
    with pg.db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (show_id,))
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def auto_categorize_flairs_batch(*, show_id: str) -> dict[str, Any]:
    """Auto-categorize flairs for ALL communities linked to a show."""
    cast_names = _get_cast_names(show_id)
    season_numbers = _get_season_numbers(show_id)
    communities = _get_communities_for_show(show_id)

    results = []
    total_matched = 0
    total_flairs = 0

    for community in communities:
        community_id = str(community["id"])
        flairs_raw = community.get("post_flairs") or []
        if isinstance(flairs_raw, str):
            flairs_raw = json.loads(flairs_raw)
        flairs = [str(f) for f in flairs_raw if isinstance(f, str)]

        categories: dict[str, str] = {}
        for flair in flairs:
            canonical = _normalize_for_matching(flair)
            if not canonical:
                continue
            cat = _categorize_flair(flair, cast_names, season_numbers)
            if cat:
                categories[canonical] = cat

        results.append(
            {
                "community_id": community_id,
                "subreddit": community.get("subreddit"),
                "categories": categories,
                "matched": len(categories),
                "total": len(flairs),
            }
        )
        total_matched += len(categories)
        total_flairs += len(flairs)

    return {
        "communities": results,
        "total_communities": len(communities),
        "total_matched": total_matched,
        "total_flairs": total_flairs,
    }
