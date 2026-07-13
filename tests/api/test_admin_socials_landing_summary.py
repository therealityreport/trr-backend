from __future__ import annotations

from psycopg2.pool import PoolError

from api.routers import socials
from trr_backend.socials.api.handlers import live_status


def test_social_landing_summary_returns_covered_shows_and_reddit_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.repositories.covered_shows.list_covered_shows",
        lambda: ([{"trr_show_id": "show-1", "show_name": "Show 1"}], 1),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.admin_reddit_reads.list_reddit_communities",
        lambda **_kwargs: (
            {
                "communities": [
                    {"trr_show_id": "show-1", "is_active": True},
                    {"trr_show_id": "show-2", "is_active": False},
                ]
            },
            1,
        ),
    )

    payload = socials.get_social_landing_summary()

    assert payload["covered_shows"] == [{"trr_show_id": "show-1", "show_name": "Show 1"}]
    assert payload["reddit_dashboard"] == {
        "active_community_count": 1,
        "archived_community_count": 1,
        "show_count": 2,
    }
    assert "omitted_sections" not in payload


def test_social_landing_summary_marks_reddit_omitted_when_reddit_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.repositories.covered_shows.list_covered_shows",
        lambda: ([{"trr_show_id": "show-1", "show_name": "Show 1"}], 1),
    )

    def fail_reddit(**_kwargs):
        raise RuntimeError("reddit unavailable")

    monkeypatch.setattr("trr_backend.repositories.admin_reddit_reads.list_reddit_communities", fail_reddit)

    payload = socials.get_social_landing_summary()

    assert payload["reddit_dashboard"] == {
        "active_community_count": 0,
        "archived_community_count": 0,
        "show_count": 0,
    }
    assert payload["omitted_sections"] == [{"section": "reddit_dashboard", "reason": "RuntimeError", "retryable": True}]


def test_social_landing_socialblade_rows_uses_social_profile_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query, params, *, pool_name):
        captured["query"] = query
        captured["params"] = params
        captured["pool_name"] = pool_name
        return [
            {
                "id": "row-1",
                "person_id": "11111111-1111-4111-8111-111111111111",
                "platform": "instagram",
                "account_handle": "heathergay",
                "scraped_at": "2026-04-01T00:00:00Z",
                "updated_at": "2026-04-02T00:00:00Z",
                "created_at": "2026-04-01T00:00:00Z",
                "stats_refreshed": True,
                "socialblade_url": "https://socialblade.com/instagram/user/heathergay",
            }
        ]

    monkeypatch.setattr("trr_backend.db.pg.fetch_all", fake_fetch_all)

    payload = socials.post_social_landing_socialblade_rows(
        socials.SocialLandingSocialBladeRowsRequest(
            platforms=["instagram"],
            person_ids=["11111111-1111-4111-8111-111111111111"],
            account_handles=["heathergay"],
        )
    )

    assert captured["pool_name"] == "social_profile"
    assert captured["params"] == [
        ["instagram"],
        ["11111111-1111-4111-8111-111111111111"],
        ["heathergay"],
    ]
    assert payload["rows"][0]["account_handle"] == "heathergay"


def test_social_landing_socialblade_progress_counts_uses_social_profile_pool(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query, params, *, pool_name):
        captured["query"] = query
        captured["params"] = params
        captured["pool_name"] = pool_name
        return [
            {
                "platform": "instagram",
                "account_handle": "heathergay",
                "socialblade_supported": True,
                "socialblade_scraped_count": 1,
                "socialblade_saved_count": 1,
            }
        ]

    monkeypatch.setattr("trr_backend.db.pg.fetch_all", fake_fetch_all)

    payload = socials.post_social_landing_socialblade_progress_counts(
        socials.SocialLandingSocialBladeProgressCountsRequest(
            platforms=["instagram", "twitter", "instagram"],
            account_handles=["@heathergay", "heathergay", "@heathergay"],
        )
    )

    assert captured["pool_name"] == "social_profile"
    assert "FROM targets" in str(captured["query"])
    assert "pipeline.socialblade_growth_data" in str(captured["query"])
    assert captured["params"] == [["instagram", "twitter"], ["heathergay", "heathergay"]]
    assert payload["rows"][0] == {
        "platform": "instagram",
        "account_handle": "heathergay",
        "socialblade_supported": True,
        "socialblade_scraped_count": 1,
        "socialblade_saved_count": 1,
    }


def test_social_landing_socialblade_progress_counts_rejects_mismatched_targets() -> None:
    try:
        socials.post_social_landing_socialblade_progress_counts(
            socials.SocialLandingSocialBladeProgressCountsRequest(
                platforms=["instagram"],
                account_handles=[],
            )
        )
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "status_code", None) == 400
        assert getattr(exc, "detail", None) == "platforms and account_handles must have matching lengths"
    else:  # pragma: no cover - assertion guard
        raise AssertionError("Expected mismatched SocialBlade progress targets to fail")


def test_social_landing_progress_rollup_returns_empty_rows_without_db(monkeypatch) -> None:
    def fail_fetch_all(*_args, **_kwargs):
        raise AssertionError("progress rollup should skip DB when no valid targets are provided")

    monkeypatch.setattr("trr_backend.db.pg.fetch_all", fail_fetch_all)

    payload = socials.post_social_landing_progress_rollup(
        socials.SocialLandingProgressRollupRequest(platforms=[], account_handles=[])
    )

    assert payload["rows"] == []
    assert payload["cache_status"] == "bypass"
    assert "generated_at" in payload
    assert payload["timing"]["database_ms"] == 0
    assert payload["timing"]["backend_ms"] >= 0
    assert payload["timing"]["total_ms"] >= 0


def test_social_landing_progress_rollup_rejects_mismatched_targets() -> None:
    try:
        socials.post_social_landing_progress_rollup(
            socials.SocialLandingProgressRollupRequest(
                platforms=["instagram"],
                account_handles=[],
            )
        )
    except Exception as exc:  # noqa: BLE001
        assert getattr(exc, "status_code", None) == 400
        assert getattr(exc, "detail", None) == "platforms and account_handles must have matching lengths"
    else:  # pragma: no cover - assertion guard
        raise AssertionError("Expected mismatched social progress targets to fail")


def test_social_landing_progress_rollup_uses_social_profile_pool_and_caches(monkeypatch) -> None:
    captured: dict[str, object] = {}
    calls = {"fetch_all": 0}
    socials._SOCIAL_LANDING_PROGRESS_ROLLUP_CACHE.clear()

    def fake_fetch_all(query, params, *, pool_name):
        calls["fetch_all"] += 1
        captured["query"] = query
        captured["params"] = params
        captured["pool_name"] = pool_name
        return [
            {
                "platform": "instagram",
                "account_handle": "bravotv",
                "saved_count": 2,
                "scraped_count": 3,
                "socialblade_supported": True,
                "socialblade_scraped_count": 1,
                "socialblade_saved_count": 1,
                "following_saved_count": 10,
                "following_total_count": 20,
                "comments_saved_count": 0,
                "comments_total_count": 120,
                "media_saved_count": 4,
                "media_total_count": 6,
            }
        ]

    monkeypatch.setattr("trr_backend.db.pg.fetch_all", fake_fetch_all)

    request = socials.SocialLandingProgressRollupRequest(
        platforms=["instagram", "instagram"],
        account_handles=["@BravoTV", "bravotv"],
    )
    first = socials.post_social_landing_progress_rollup(request)
    second = socials.post_social_landing_progress_rollup(request)

    assert calls["fetch_all"] == 1
    assert captured["pool_name"] == "social_profile"
    assert captured["params"] == [["instagram"], ["bravotv"]]
    assert "social.instagram_comments" not in str(captured["query"])
    assert "pipeline.socialblade_growth_data" in str(captured["query"])
    assert "comment_counts AS" not in str(captured["query"])
    assert first["cache_status"] == "miss"
    assert second["cache_status"] == "hit"
    assert first["timing"]["database_ms"] >= 0
    assert first["timing"]["backend_ms"] >= first["timing"]["database_ms"]
    assert first["timing"]["total_ms"] == first["timing"]["backend_ms"]
    assert second["timing"]["database_ms"] == 0
    assert second["timing"]["backend_ms"] >= 0
    assert second["timing"]["total_ms"] == second["timing"]["backend_ms"]
    assert first["rows"][0] == {
        "platform": "instagram",
        "account_handle": "bravotv",
        "saved_count": 2,
        "scraped_count": 3,
        "socialblade_supported": True,
        "socialblade_scraped_count": 1,
        "socialblade_saved_count": 1,
        "following_saved_count": 10,
        "following_total_count": 20,
        "comments_saved_count": 0,
        "comments_total_count": 120,
        "media_saved_count": 4,
        "media_total_count": 6,
    }


def test_social_live_status_reuses_cached_snapshot(monkeypatch) -> None:
    calls = {"queue": 0, "operations": 0}
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)

    def fake_get_queue_status(**_kwargs):
        calls["queue"] += 1
        return {
            "queue_enabled": True,
            "workers": {"healthy": True, "healthy_workers": 1},
            "queue": {"by_status": {"running": 1}},
        }

    def fake_get_admin_operations_health():
        calls["operations"] += 1
        return {"summary": {"active_total": 0}}

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.get_queue_status", fake_get_queue_status)
    monkeypatch.setattr(
        "trr_backend.repositories.admin_operations.get_admin_operations_health",
        fake_get_admin_operations_health,
    )

    first = live_status.build_live_status_payload()
    second = live_status.build_live_status_payload()

    assert calls == {"queue": 1, "operations": 1}
    assert first["snapshot"]["cache_status"] == "miss"
    assert second["snapshot"]["cache_status"] == "hit"
    assert second["snapshot"]["stale"] is False
    assert first["sequence"] == second["sequence"]


def test_social_live_status_serves_stale_snapshot_when_refresh_fails(monkeypatch) -> None:
    now = {"value": 100.0}
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)
    monkeypatch.setattr(live_status, "monotonic", lambda: now["value"])

    def fake_get_queue_status(**_kwargs):
        return {
            "queue_enabled": True,
            "workers": {"healthy": True, "healthy_workers": 1},
            "queue": {"by_status": {"running": 1}},
        }

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.get_queue_status", fake_get_queue_status)
    monkeypatch.setattr(
        "trr_backend.repositories.admin_operations.get_admin_operations_health",
        lambda: {"summary": {"active_total": 0}},
    )
    fresh = live_status.build_live_status_payload()

    def fail_live_status_refresh():
        raise RuntimeError("queue unavailable")

    now["value"] = 106.0
    monkeypatch.setattr(live_status, "_build_live_status_payload_uncached", fail_live_status_refresh)
    stale = live_status.build_live_status_payload()

    assert stale["sequence"] == fresh["sequence"]
    assert stale["snapshot"]["cache_status"] == "stale"
    assert stale["snapshot"]["stale"] is True
    assert stale["snapshot"]["refresh_error"] == "RuntimeError"
    assert stale["snapshot"]["cache_age_ms"] == 6000


def test_social_live_status_preserves_last_good_snapshot_on_pool_exhaustion(monkeypatch) -> None:
    now = {"value": 100.0}
    queue_reads = {"count": 0}
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)
    monkeypatch.setattr(live_status, "monotonic", lambda: now["value"])

    def fake_get_queue_status(**_kwargs):
        queue_reads["count"] += 1
        if queue_reads["count"] > 1:
            raise PoolError("connection pool exhausted")
        return {
            "queue_enabled": True,
            "workers": {"healthy": True, "healthy_workers": 2},
            "queue": {"by_status": {"running": 4}},
        }

    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.get_queue_status", fake_get_queue_status)
    monkeypatch.setattr(
        "trr_backend.repositories.admin_operations.get_admin_operations_health",
        lambda: {"summary": {"active_total": 4}},
    )

    fresh = live_status.build_live_status_payload()
    now["value"] = 106.0
    under_pressure = live_status.build_live_status_payload()

    assert under_pressure["sequence"] == fresh["sequence"]
    assert under_pressure["queue_status"]["queue_enabled"] is True
    assert under_pressure["queue_status"]["queue"]["by_status"]["running"] == 4
    assert under_pressure["snapshot"]["cache_status"] == "stale"
    assert under_pressure["snapshot"]["stale"] is True
    assert under_pressure["snapshot"]["refresh_error"] == "PoolError"


def test_social_live_status_serves_stale_snapshot_when_refresh_in_progress(monkeypatch) -> None:
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(live_status, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)
    monkeypatch.setattr(live_status, "monotonic", lambda: 106.0)
    monkeypatch.setattr(
        live_status,
        "_LIVE_STATUS_SNAPSHOT_CACHE",
        {
            "payload": {"sequence": 123, "generated_at": "2026-05-03T10:00:00+00:00"},
            "fetched_at": 100.0,
        },
    )

    live_status._LIVE_STATUS_SNAPSHOT_LOCK.acquire()
    try:
        payload = live_status.build_live_status_payload()
    finally:
        live_status._LIVE_STATUS_SNAPSHOT_LOCK.release()

    assert payload["sequence"] == 123
    assert payload["snapshot"]["cache_status"] == "stale-refreshing"
    assert payload["snapshot"]["stale"] is True
    assert payload["snapshot"]["cache_age_ms"] == 6000
