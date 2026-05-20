"""Tests for season analytics read helper behavior."""

from __future__ import annotations

import pytest

from api.routers.socials.analytics_read import (
    analytics_read_path_extra,
    page_week_detail_payload,
    parse_analytics_include,
    week_detail_cached_post_counts,
)


@pytest.mark.parametrize(
    ("include", "expected"),
    [
        (
            None,
            {
                "include_rows": False,
                "include_flags": True,
                "include_schedule": True,
                "include_benchmark": True,
            },
        ),
        (
            "rows,benchmark",
            {
                "include_rows": True,
                "include_flags": False,
                "include_schedule": False,
                "include_benchmark": True,
            },
        ),
        (
            " FLAGS , schedule ",
            {
                "include_rows": False,
                "include_flags": True,
                "include_schedule": True,
                "include_benchmark": False,
            },
        ),
    ],
)
def test_parse_analytics_include(include: str | None, expected: dict[str, bool]) -> None:
    assert parse_analytics_include(include).__dict__ == expected


def test_analytics_read_path_extra_formats_platforms() -> None:
    assert analytics_read_path_extra(
        cache="hit",
        source_scope="network",
        week=3,
        platforms=["instagram", "tiktok"],
    ) == {
        "cache": "hit",
        "source_scope": "network",
        "week": 3,
        "platforms": "instagram,tiktok",
    }


def test_analytics_read_path_extra_defaults_platforms_to_all() -> None:
    assert analytics_read_path_extra(
        cache="miss",
        source_scope="creator",
        week=None,
        platforms=None,
    ) == {
        "cache": "miss",
        "source_scope": "creator",
        "week": None,
        "platforms": "all",
    }


def test_week_detail_cached_post_counts_uses_total_fallbacks() -> None:
    assert week_detail_cached_post_counts(
        {
            "platforms": {
                "instagram": {"posts": [{"source_id": "a"}], "total_posts": 3},
                "tiktok": {"posts": [{"source_id": "b"}, {"source_id": "c"}]},
            }
        }
    ) == (3, 5)


def test_page_week_detail_payload_deduplicates_sorts_and_paginates_without_mutating_source() -> None:
    source = {
        "totals": {},
        "platforms": {
            "instagram": {
                "total_posts": 3,
                "posts": [
                    {"source_id": "old", "posted_at": "2026-01-01T00:00:00Z", "engagement": 1},
                    {"source_id": "new", "posted_at": "2026-01-03T00:00:00Z", "engagement": 5},
                    {"source_id": "new", "posted_at": "2026-01-03T00:00:00Z", "engagement": 5},
                ],
            },
            "youtube": {
                "posts": [
                    {"source_id": "yt", "posted_at": "2026-01-02T00:00:00Z", "engagement": 3},
                ],
            },
        },
    }

    paged = page_week_detail_payload(
        source,
        post_limit=2,
        post_offset=0,
        sort_field="posted_at",
        sort_dir="desc",
    )

    assert paged["pagination"] == {"limit": 2, "offset": 0, "returned": 2, "total": 4, "has_more": True}
    assert [post["source_id"] for post in paged["platforms"]["instagram"]["posts"]] == ["new"]
    assert [post["source_id"] for post in paged["platforms"]["youtube"]["posts"]] == ["yt"]
    assert "sort_rank" not in source["platforms"]["instagram"]["posts"][1]
