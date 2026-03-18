from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trr_backend.repositories.socialblade_growth import merge_chart_data


def test_merge_chart_data_preserves_existing_stats_when_partial_refresh_returns_zeroes() -> None:
    now = datetime.now(tz=UTC)
    previous_day = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    current_day = now.strftime("%Y-%m-%d")
    existing = {
        "scraped_at": "2026-03-16T07:29:43Z",
        "stats_refreshed": True,
        "profile_stats": {
            "followers": 475378,
            "following": 7088,
            "media_count": 1703,
            "engagement_rate": "3.01%",
            "average_likes": 13841.44,
            "average_comments": 455.13,
        },
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {
            "data": [
                {"date": previous_day, "followers": 475283},
                {"date": current_day, "followers": 475378},
            ]
        },
    }
    partial_refresh = {
        "scraped_at": "2026-03-16T17:22:19.398530+00:00",
        "stats_refreshed": False,
        "profile_stats": {
            "followers": 0,
            "following": 0,
            "media_count": 0,
            "engagement_rate": "0%",
            "average_likes": 0,
            "average_comments": 0,
        },
        "rankings": {"grade": ""},
        "daily_channel_metrics_60day": {"data": [], "row_count": 0},
        "daily_total_followers_chart": {
            "data": [
                {"date": previous_day, "followers": 475283},
                {"date": current_day, "followers": 475372},
            ]
        },
    }

    merged = merge_chart_data(existing, partial_refresh)

    assert merged["scraped_at"] == "2026-03-16T17:22:19.398530+00:00"
    assert merged["stats_refreshed"] is False
    assert merged["profile_stats"] == existing["profile_stats"]
    assert merged["rankings"] == existing["rankings"]
    assert merged["daily_channel_metrics_60day"] == existing["daily_channel_metrics_60day"]
    assert merged["daily_total_followers_chart"]["data"][-1] == {
        "date": current_day,
        "followers": 475372,
    }
