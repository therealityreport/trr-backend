from __future__ import annotations

import pytest

from trr_backend.socials.socialblade.scraper import (
    _extract_profile_stats_from_body_text,
    _followers_chart_from_table,
    _normalize_table_data,
    _page_access_denied,
)

BODY_TEXT = """
Lisa Barlow
@lisabarlow14
View on Instagram

Followers

475,444

Following

7,090

Media Count

1,703

Engagement Rate

3.02%

Average Likes

13,894.63

Average Comments

456.75

Login to Favorite
Page Summary
Future Projections
Live Follower Count
B+
Grade
38,982nd

SB Rank

139,823rd

Followers Rank

45,085th

Engagement Rate Rank

LAST 14 DAYS
1.6K
Followers for the last 14 days
5
Media Count for the last 14 days
Daily Channel Metrics
Last 14 Days
Date    Followers   Following   Media Count
Thu2026-03-05   40  473,873 -2  7,072   1   1,699
Fri2026-03-06   145 474,018 -1  7,071   1   1,700
"""


def test_extract_profile_stats_from_body_text_prefers_primary_values() -> None:
    stats, rankings = _extract_profile_stats_from_body_text(BODY_TEXT)

    assert stats == {
        "followers": 475444,
        "following": 7090,
        "media_count": 1703,
        "engagement_rate": "3.02%",
        "average_likes": pytest.approx(13894.63),
        "average_comments": pytest.approx(456.75),
    }
    assert rankings == {
        "grade": "B+",
        "sb_rank": "38,982nd",
        "followers_rank": "139,823rd",
        "engagement_rate_rank": "45,085th",
    }


def test_normalize_table_data_and_build_chart_from_followers_totals() -> None:
    metrics = _normalize_table_data(
        {
            "headers": [],
            "data": [
                {
                    "Date": "Thu2026-03-05",
                    "Followers Delta": "40",
                    "Followers Total": "473,873",
                    "Following Delta": "-2",
                    "Following Total": "7,072",
                    "Media Count Delta": "1",
                    "Media Count Total": "1,699",
                },
                {
                    "Date": "Fri2026-03-06",
                    "Followers Delta": "145",
                    "Followers Total": "474,018",
                    "Following Delta": "-1",
                    "Following Total": "7,071",
                    "Media Count Delta": "1",
                    "Media Count Total": "1,700",
                },
            ],
        },
        BODY_TEXT,
    )

    assert metrics["period"] == "Last 14 Days"
    assert metrics["row_count"] == 2
    assert metrics["headers"] == [
        "Date",
        "Followers Delta",
        "Followers Total",
        "Following Delta",
        "Following Total",
        "Media Count Delta",
        "Media Count Total",
    ]

    chart = _followers_chart_from_table(metrics)
    assert chart == {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": 2,
        "date_range": {"from": "2026-03-05", "to": "2026-03-06"},
        "data": [
            {"date": "2026-03-05", "followers": 473873},
            {"date": "2026-03-06", "followers": 474018},
        ],
    }


def test_page_access_denied_detects_cloudflare_block() -> None:
    assert _page_access_denied("Access denied. Error reference number: 1020")
