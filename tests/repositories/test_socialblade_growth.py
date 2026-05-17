from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trr_backend.repositories import socialblade_growth as growth_repo
from trr_backend.repositories.socialblade_growth import (
    _row_to_response,
    insert_growth_snapshot,
    merge_chart_data,
    normalize_socialblade_account_handle,
)


def test_merge_chart_data_records_failed_attempt_without_refreshing_reusable_timestamp() -> None:
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

    assert merged["scraped_at"] == "2026-03-16T07:29:43Z"
    assert merged["last_attempt_at"] == "2026-03-16T17:22:19.398530+00:00"
    assert merged["last_attempt_stats_refreshed"] is False
    assert merged["stats_refreshed"] is True
    assert merged["profile_stats"] == existing["profile_stats"]
    assert merged["rankings"] == existing["rankings"]
    assert merged["daily_channel_metrics_60day"] == existing["daily_channel_metrics_60day"]
    assert merged["daily_total_followers_chart"] == existing["daily_total_followers_chart"]


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


def test_merge_chart_data_preserves_fresh_instagram_following_sidecar() -> None:
    existing = {
        "scraped_at": "2026-03-18T05:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 685081},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-03-18", "followers": 685081}]},
        "instagram_following_scrape": {"status": "failed"},
    }
    fresh = {
        "scraped_at": "2026-04-07T08:30:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 687613},
        "rankings": {"grade": "A-"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-04-07", "followers": 687613}]},
        "instagram_following_scrape": {"status": "completed", "relationships_upserted": 50},
    }

    merged = merge_chart_data(existing, fresh)

    assert merged["instagram_following_scrape"] == {"status": "completed", "relationships_upserted": 50}


def test_merge_chart_data_updates_scraper_metadata_on_full_refresh() -> None:
    existing = {
        "scraped_at": "2026-05-12T08:00:00Z",
        "stats_refreshed": True,
        "history_source": "unavailable",
        "profile_stats": {"followers": 1500000, "media_count": 100000000},
        "profile_stats_labels": {"media_count": "Videos"},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 14},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-12", "followers": 1500000}]},
        "socialblade_url": "https://socialblade.com/tiktok/user/bravotv",
    }
    fresh = {
        "scraped_at": "2026-05-13T08:00:00Z",
        "stats_refreshed": True,
        "history_source": "page_trpc_capture",
        "profile_stats": {"followers": 1600000, "media_count": 111400000},
        "profile_stats_labels": {"media_count": "Likes"},
        "rankings": {"grade": "A-"},
        "daily_channel_metrics_60day": {"row_count": 31},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-13", "followers": 1600000}]},
        "chart_metric_label": "Followers",
        "socialblade_url": "https://socialblade.com/tiktok/user/bravotv",
        "runtime_metadata": {"fallback_chain": ["scrapling_warmup", "tiktok_page_trpc_capture"]},
    }

    merged = merge_chart_data(existing, fresh)

    assert merged["history_source"] == "page_trpc_capture"
    assert merged["profile_stats_labels"]["media_count"] == "Likes"
    assert merged["chart_metric_label"] == "Followers"
    assert merged["runtime_metadata"] == {"fallback_chain": ["scrapling_warmup", "tiktok_page_trpc_capture"]}
    assert merged["daily_channel_metrics_60day"] == {"row_count": 31}


def test_merge_chart_data_clears_failed_attempt_metadata_on_full_refresh() -> None:
    existing = {
        "scraped_at": "2026-05-12T08:00:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 1500000},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-12", "followers": 1500000}]},
        "last_attempt_at": "2026-05-13T08:00:00Z",
        "last_attempt_stats_refreshed": False,
        "last_attempt_history_source": "table_fallback",
        "last_attempt_error": "Headless SocialBlade login was challenged in Modal",
        "last_attempt_runtime_metadata": {"selected_proxy_fingerprint": "none"},
    }
    fresh = {
        "scraped_at": "2026-05-14T08:00:00Z",
        "stats_refreshed": True,
        "history_source": "page_trpc_capture",
        "profile_stats": {"followers": 1600000},
        "rankings": {"grade": "A-"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-14", "followers": 1600000}]},
    }

    merged = merge_chart_data(existing, fresh)

    assert merged["stats_refreshed"] is True
    assert merged["history_source"] == "page_trpc_capture"
    assert "last_attempt_at" not in merged
    assert "last_attempt_stats_refreshed" not in merged
    assert "last_attempt_history_source" not in merged
    assert "last_attempt_error" not in merged
    assert "last_attempt_runtime_metadata" not in merged


def test_merge_chart_data_preserves_wider_metrics_when_short_table_fallback_refreshes() -> None:
    existing = {
        "scraped_at": "2026-05-13T08:00:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 1500000},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {
            "period": "Last 3 Days",
            "row_count": 3,
            "headers": ["Date", "Followers Total"],
            "data": [
                {"Date": "2026-05-11", "Followers Total": "1,498,000"},
                {"Date": "2026-05-12", "Followers Total": "1,499,000"},
                {"Date": "2026-05-13", "Followers Total": "1,500,000"},
            ],
        },
        "daily_total_followers_chart": {"data": [{"date": "2026-05-13", "followers": 1500000}]},
    }
    fresh = {
        "scraped_at": "2026-05-14T08:00:00Z",
        "stats_refreshed": True,
        "history_source": "table_fallback",
        "profile_stats": {"followers": 1501000},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {
            "period": "Last 2 Days",
            "row_count": 2,
            "headers": ["Date", "Followers Total"],
            "data": [
                {"Date": "2026-05-13", "Followers Total": "1,500,500"},
                {"Date": "2026-05-14", "Followers Total": "1,501,000"},
            ],
        },
        "daily_total_followers_chart": {"data": [{"date": "2026-05-14", "followers": 1501000}]},
    }

    merged = merge_chart_data(existing, fresh)

    assert merged["daily_channel_metrics_60day"] == {
        "period": "Last 4 Days",
        "row_count": 4,
        "headers": ["Date", "Followers Total"],
        "data": [
            {"Date": "2026-05-11", "Followers Total": "1,498,000"},
            {"Date": "2026-05-12", "Followers Total": "1,499,000"},
            {"Date": "2026-05-13", "Followers Total": "1,500,500"},
            {"Date": "2026-05-14", "Followers Total": "1,501,000"},
        ],
    }


def test_normalize_socialblade_account_handle_extracts_full_urls() -> None:
    assert normalize_socialblade_account_handle("https://socialblade.com/instagram/user/TheTraitors.US") == (
        "thetraitors.us"
    )
    assert normalize_socialblade_account_handle("https://www.instagram.com/BravoTV/?igsh=abc") == "bravotv"
    assert normalize_socialblade_account_handle("https://www.tiktok.com/@BravoTV?lang=en") == "bravotv"


def test_row_to_response_exposes_previous_run_snapshot() -> None:
    row = {
        "id": "growth-row-1",
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
            "instagram_following_scrape": {"status": "completed", "relationships_upserted": 50},
            "last_attempt_at": "2026-04-20T12:00:00Z",
            "last_attempt_stats_refreshed": False,
            "last_attempt_history_source": "page_trpc_capture_short",
            "previous_run": {
                "scraped_at": "2026-04-19T12:00:00Z",
                "profile_stats": {"followers": 950},
                "rankings": {"grade": "B-"},
            },
        },
    }

    response = _row_to_response(row)

    assert response["row_id"] == "growth-row-1"
    assert response["previous_run"] == {
        "scraped_at": "2026-04-19T12:00:00Z",
        "profile_stats": {"followers": 950},
        "rankings": {"grade": "B-"},
    }
    assert response["instagram_following_scrape"] == {"status": "completed", "relationships_upserted": 50}
    assert response["last_attempt_at"] == "2026-04-20T12:00:00Z"
    assert response["last_attempt_stats_refreshed"] is False
    assert response["last_attempt_history_source"] == "page_trpc_capture_short"


def test_insert_growth_snapshot_writes_immutable_row(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute_returning(sql: str, params: list[object]):
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": "snapshot-1",
                "growth_data_id": "growth-row-1",
                "platform": "instagram",
                "account_handle": "bravotv",
                "scraped_at": datetime(2026, 5, 13, tzinfo=UTC),
            }
        ]

    monkeypatch.setattr(growth_repo.pg, "execute_returning", fake_execute_returning)

    row = insert_growth_snapshot(
        "person-1",
        "@BravoTV",
        {
            "scraped_at": "2026-05-13T12:00:00Z",
            "stats_refreshed": True,
            "profile_stats": {"followers": 1000},
            "rankings": {"grade": "B+"},
            "daily_channel_metrics_60day": {"row_count": 60},
            "daily_total_followers_chart": {"data": []},
        },
        growth_data_id="growth-row-1",
        source="all_saved_instagram_backfill",
        force=True,
    )

    assert "pipeline.socialblade_growth_snapshots" in str(captured["sql"])
    assert captured["params"][0:6] == [
        "growth-row-1",
        "person-1",
        "instagram",
        "bravotv",
        "bravotv",
        "2026-05-13T12:00:00Z",
    ]
    assert captured["params"][-3:] == [
        "all_saved_instagram_backfill",
        "all_saved_instagram_backfill",
        True,
    ]
    assert row == {
        "id": "snapshot-1",
        "growth_data_id": "growth-row-1",
        "platform": "instagram",
        "account_handle": "bravotv",
        "scraped_at": "2026-05-13T00:00:00+00:00",
    }
