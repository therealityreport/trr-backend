from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.repositories.socialblade_growth import normalize_socialblade_account_handle
from trr_backend.socials.socialblade import service as service_module
from trr_backend.socials.socialblade.service import (
    attach_instagram_following_scrape,
    is_growth_data_fresh,
    persist_scraped_payload,
    sanitize_socialblade_handle,
    sanitize_socialblade_platform,
    socialblade_instagram_following_config,
)


def _recent_scrape_timestamp() -> str:
    return datetime.now(tz=UTC).isoformat()


def test_is_growth_data_fresh_rejects_short_chart_without_history_source() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "stats_refreshed": True,
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
                "stats_refreshed": True,
                "history_source": "authenticated_api",
                "daily_total_followers_chart": {
                    "total_data_points": 14,
                    "date_range": {"from": "2026-03-05", "to": "2026-03-18"},
                },
            }
        )
        is True
    )


def test_is_growth_data_fresh_rejects_short_page_trpc_capture_history() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "stats_refreshed": True,
                "history_source": "page_trpc_capture",
                "daily_total_followers_chart": {
                    "total_data_points": 31,
                    "date_range": {"from": "2026-04-13", "to": "2026-05-13"},
                },
            }
        )
        is False
    )


def test_is_growth_data_fresh_accepts_complete_page_trpc_capture_history() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "stats_refreshed": True,
                "history_source": "page_trpc_capture",
                "daily_channel_metrics_60day": {
                    "row_count": 60,
                    "data": [{"Date": "2026-05-13", "Followers Total": "172,666"}],
                },
                "daily_total_followers_chart": {
                    "total_data_points": 60,
                    "date_range": {"from": "2026-03-15", "to": "2026-05-13"},
                },
            }
        )
        is True
    )


def test_is_growth_data_fresh_rejects_failed_refresh_attempt() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "last_attempt_at": _recent_scrape_timestamp(),
                "stats_refreshed": False,
                "history_source": "authenticated_api",
                "daily_total_followers_chart": {
                    "total_data_points": 60,
                    "date_range": {"from": "2026-03-15", "to": "2026-05-13"},
                },
            }
        )
        is False
    )


def test_is_growth_data_fresh_rejects_chart_that_lags_metrics_table() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "stats_refreshed": True,
                "history_source": "page_trpc_capture",
                "daily_total_followers_chart": {
                    "total_data_points": 104,
                    "date_range": {"from": "2026-01-11", "to": "2026-04-24"},
                    "data": [
                        {"date": "2026-01-11", "followers": 40237},
                        {"date": "2026-04-24", "followers": 172238},
                    ],
                },
                "daily_channel_metrics_60day": {
                    "row_count": 31,
                    "data": [
                        {"Date": "2026-04-13", "Followers Total": "172,029"},
                        {"Date": "2026-05-13", "Followers Total": "172,666"},
                    ],
                },
            }
        )
        is False
    )


def test_is_growth_data_fresh_rejects_table_fallback_history() -> None:
    assert (
        is_growth_data_fresh(
            {
                "scraped_at": _recent_scrape_timestamp(),
                "stats_refreshed": True,
                "history_source": "table_fallback",
                "daily_total_followers_chart": {
                    "total_data_points": 365,
                    "date_range": {"from": "2025-03-19", "to": "2026-03-18"},
                },
            }
        )
        is False
    )


def test_socialblade_instagram_following_config_clamps_limits(monkeypatch) -> None:
    monkeypatch.setenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_PAGE_SIZE", "999")
    monkeypatch.setenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_PAGES", "999")
    monkeypatch.setenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_RELATIONSHIPS", "999999")

    assert socialblade_instagram_following_config() == {
        "page_size": 200,
        "max_pages": 25,
        "max_relationships": 5000,
    }


def test_socialblade_instagram_following_config_defaults_to_complete_snapshot_limits(monkeypatch) -> None:
    monkeypatch.delenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_PAGE_SIZE", raising=False)
    monkeypatch.delenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_PAGES", raising=False)
    monkeypatch.delenv("SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_RELATIONSHIPS", raising=False)

    assert socialblade_instagram_following_config() == {
        "page_size": 200,
        "max_pages": 25,
        "max_relationships": 5000,
    }


def test_sanitize_socialblade_platform_accepts_tiktok() -> None:
    assert sanitize_socialblade_platform("TikTok") == "tiktok"


def test_sanitize_socialblade_handle_extracts_full_profile_urls() -> None:
    assert sanitize_socialblade_handle("https://socialblade.com/instagram/user/TheTraitors.US") == "thetraitors.us"
    assert sanitize_socialblade_handle("https://www.tiktok.com/@BravoTV?lang=en") == "bravotv"


def test_socialblade_handle_normalization_preserves_youtube_channel_ids_and_facebook_profile_ids() -> None:
    assert sanitize_socialblade_handle("UCabcXYZ123", platform="youtube") == "UCabcXYZ123"
    assert (
        normalize_socialblade_account_handle(
            "https://www.youtube.com/channel/UCabcXYZ123",
            platform="youtube",
        )
        == "UCabcXYZ123"
    )
    assert sanitize_socialblade_handle("https://www.facebook.com/profile.php?id=123456789", platform="facebook") == (
        "123456789"
    )


def test_attach_instagram_following_scrape_completes(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_stage(*, handle: str, source_scope: str, config: dict[str, object]):
        captured["handle"] = handle
        captured["source_scope"] = source_scope
        captured["config"] = config
        return (
            0,
            0,
            {
                "relationships_fetched": 12,
                "relationships_upserted": 11,
                "relationships_missing": 2,
                "snapshot_id": "snapshot-1",
                "source_is_complete": True,
                "relationship_mismatches": [{"code": "ignored"}],
                "retrieval_meta": {
                    "profile_id": "2554414",
                    "pages_fetched": 1,
                    "next_cursor": "next",
                    "has_more": True,
                    "max_pages": 1,
                    "max_relationships": 50,
                    "profile_payload": {"raw": "not exposed"},
                },
            },
        )

    monkeypatch.setattr(service_module, "_run_instagram_following_sidecar_stage", fake_run_stage)

    payload = attach_instagram_following_scrape(
        {"username": "networkofficial", "stats_refreshed": True},
        handle="@NetworkOfficial",
        source="season_run",
        source_scope="creator",
        platform="instagram",
    )

    annotation = payload["instagram_following_scrape"]
    assert captured["handle"] == "networkofficial"
    assert captured["source_scope"] == "creator"
    assert captured["config"]["source_scope"] == "creator"
    assert annotation == {
        "enabled": True,
        "stage": "instagram_profile_following",
        "platform": "instagram",
        "handle": "networkofficial",
        "source": "season_run",
        "source_scope": "creator",
        "relationship_type": "following",
        "status": "completed",
        "relationships_fetched": 12,
        "relationships_upserted": 11,
        "relationships_missing": 2,
        "snapshot_id": "snapshot-1",
        "source_is_complete": True,
        "relationship_mismatches": [{"code": "ignored"}],
        "retrieval_meta": {
            "profile_id": "2554414",
            "pages_fetched": 1,
            "next_cursor": "next",
            "has_more": True,
            "max_pages": 1,
            "max_relationships": 50,
        },
    }


def test_attach_instagram_following_scrape_failure_is_nonfatal(monkeypatch) -> None:
    def fake_run_stage(*, handle: str, source_scope: str, config: dict[str, object]):
        raise RuntimeError("instagram blocked")

    monkeypatch.setattr(service_module, "_run_instagram_following_sidecar_stage", fake_run_stage)

    payload = attach_instagram_following_scrape(
        {"username": "networkofficial"},
        handle="networkofficial",
        source="season_run",
        platform="instagram",
    )

    assert payload["username"] == "networkofficial"
    assert payload["instagram_following_scrape"]["status"] == "failed"
    assert payload["instagram_following_scrape"]["reason"] == "instagram_following_scrape_failed"
    assert payload["instagram_following_scrape"]["error"] == "instagram blocked"


def test_attach_instagram_following_scrape_skips_non_instagram_platform() -> None:
    payload = attach_instagram_following_scrape(
        {"username": "networkofficial"},
        handle="networkofficial",
        source="account_page",
        platform="tiktok",
    )

    assert payload["instagram_following_scrape"]["status"] == "skipped"
    assert payload["instagram_following_scrape"]["reason"] == "platform_not_instagram"
    assert payload["instagram_following_scrape"]["platform"] == "tiktok"


def test_persist_scraped_payload_inserts_fresh_snapshot_and_upserts_merged_current_row(monkeypatch) -> None:
    captured: dict[str, object] = {}
    existing = {
        "row_id": "growth-row-1",
        "scraped_at": "2026-05-12T08:00:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 1500},
        "rankings": {"grade": "B"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-12", "followers": 1500}]},
        "instagram_following_scrape": {"status": "failed"},
    }
    fresh = {
        "scraped_at": "2026-05-13T08:00:00Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 1550},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 31},
        "daily_total_followers_chart": {"data": [{"date": "2026-05-13", "followers": 1550}]},
        "instagram_following_scrape": {"status": "completed", "source_scope": "creator"},
    }

    def fake_upsert(person_id: str | None, handle: str, data: dict[str, object], *, platform: str):
        captured["upsert"] = {
            "person_id": person_id,
            "handle": handle,
            "platform": platform,
            "data": data,
        }
        return {**data, "row_id": "growth-row-1"}

    def fake_snapshot(
        person_id: str | None,
        handle: str,
        data: dict[str, object],
        *,
        platform: str,
        growth_data_id: str | None,
        source: str | None,
        force: bool,
    ):
        captured["snapshot"] = {
            "person_id": person_id,
            "handle": handle,
            "platform": platform,
            "growth_data_id": growth_data_id,
            "source": source,
            "force": force,
            "data": data,
        }
        return {"id": "snapshot-1"}

    monkeypatch.setattr(service_module, "get_growth_data", lambda *args, **kwargs: existing)
    monkeypatch.setattr(service_module, "upsert_growth_data", fake_upsert)
    monkeypatch.setattr(service_module, "insert_growth_snapshot", fake_snapshot)

    result = persist_scraped_payload(
        person_id="person-1",
        handle="NetworkOfficial",
        payload=fresh,
        source="season_run",
        force=True,
        platform="instagram",
    )

    upsert_data = captured["upsert"]["data"]
    snapshot_data = captured["snapshot"]["data"]
    assert upsert_data["daily_total_followers_chart"]["data"] == [
        {"date": "2026-05-12", "followers": 1500},
        {"date": "2026-05-13", "followers": 1550},
    ]
    assert upsert_data["instagram_following_scrape"] == {"status": "completed", "source_scope": "creator"}
    assert snapshot_data == fresh
    assert snapshot_data["daily_total_followers_chart"]["data"] == [{"date": "2026-05-13", "followers": 1550}]
    assert captured["snapshot"]["growth_data_id"] == "growth-row-1"
    assert result["snapshot_id"] == "snapshot-1"
