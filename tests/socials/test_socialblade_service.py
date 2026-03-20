from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.socials.socialblade.service import is_growth_data_fresh


def _recent_scrape_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat()


def test_is_growth_data_fresh_rejects_short_chart_without_history_source() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "daily_total_followers_chart": {
                    "total_data_points": 14,
                    "date_range": {"from": "2026-03-05", "to": "2026-03-18"},
                },
            }
        )
        is False
    )


def test_is_growth_data_fresh_accepts_authenticated_api_history() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "history_source": "authenticated_api",
                "daily_total_followers_chart": {
                    "total_data_points": 14,
                    "date_range": {"from": "2026-03-05", "to": "2026-03-18"},
                },
            }
        )
        is True
    )


def test_is_growth_data_fresh_rejects_table_fallback_history() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "history_source": "table_fallback",
                "daily_total_followers_chart": {
                    "total_data_points": 365,
                    "date_range": {"from": "2025-03-19", "to": "2026-03-18"},
                },
            }
        )
        is False
    )
