from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trr_backend.repositories.socialblade_growth import _row_to_response, merge_chart_data


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


def test_merge_chart_data_keeps_older_history_when_fresh_window_starts_later() -> None:
    existing = {
        "scraped_at": "2026-03-18T05:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 685081},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {
            "data": [
                {"date": "2023-03-18", "followers": 480000},
                {"date": "2023-03-19", "followers": 480120},
                {"date": "2026-03-17", "followers": 685063},
                {"date": "2026-03-18", "followers": 685081},
            ]
        },
    }
    fresh = {
        "scraped_at": "2026-04-07T08:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 687613},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {
            "data": [
                {"date": "2023-04-01", "followers": 482000},
                {"date": "2026-03-18", "followers": 685081},
                {"date": "2026-04-07", "followers": 687613},
            ]
        },
    }

    merged = merge_chart_data(existing, fresh)
    merged_points = merged["daily_total_followers_chart"]["data"]
    merged_dates = {point["date"] for point in merged_points}

    assert "2023-03-18" in merged_dates
    assert "2023-03-19" in merged_dates
    assert "2023-04-01" in merged_dates
    assert merged_points[-1] == {"date": "2026-04-07", "followers": 687613}


def test_merge_chart_data_stores_previous_run_snapshot_on_full_refresh() -> None:
    existing = {
        "scraped_at": "2026-03-18T05:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {
            "followers": 685081,
            "following": 900,
            "media_count": 250,
            "engagement_rate": "3.11%",
            "average_likes": 1200,
            "average_comments": 45,
        },
        "profile_stats_labels": {"followers": "Followers"},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-03-18", "followers": 685081}]},
    }
    fresh = {
        "scraped_at": "2026-04-07T08:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {
            "followers": 687613,
            "following": 930,
            "media_count": 252,
            "engagement_rate": "3.25%",
            "average_likes": 1325,
            "average_comments": 49,
        },
        "rankings": {"grade": "A-"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-04-07", "followers": 687613}]},
    }

    merged = merge_chart_data(existing, fresh)

    assert merged["previous_run"] == {
        "scraped_at": "2026-03-18T05:30:00Z",
        "profile_stats": existing["profile_stats"],
        "profile_stats_labels": {"followers": "Followers"},
        "rankings": {"grade": "B+"},
    }


def test_row_to_response_exposes_previous_run_snapshot() -> None:
    row = {
        "platform": "instagram",
        "account_handle": "thetraitors.us",
        "scraped_at": datetime.now(tz=UTC),
        "stats_refreshed": True,
        "profile_stats": {"followers": 1000},
        "rankings": {"grade": "B"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": None,
        "raw_response": {
            "profile_stats_labels": {"followers": "Followers"},
            "chart_metric_label": "Followers",
            "socialblade_url": "https://socialblade.com/instagram/user/thetraitors.us",
            "previous_run": {
                "scraped_at": "2026-04-19T12:00:00Z",
                "profile_stats": {"followers": 950},
                "rankings": {"grade": "B-"},
            },
        },
    }

    response = _row_to_response(row)

    assert response["previous_run"] == {
        "scraped_at": "2026-04-19T12:00:00Z",
        "profile_stats": {"followers": 950},
        "rankings": {"grade": "B-"},
    }
