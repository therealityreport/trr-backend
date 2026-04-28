from __future__ import annotations

from api.routers import socials


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


def test_social_live_status_reuses_cached_snapshot(monkeypatch) -> None:
    calls = {"queue": 0, "operations": 0}
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)

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

    first = socials._build_live_status_payload()
    second = socials._build_live_status_payload()

    assert calls == {"queue": 1, "operations": 1}
    assert first["snapshot"]["cache_status"] == "miss"
    assert second["snapshot"]["cache_status"] == "hit"
    assert second["snapshot"]["stale"] is False
    assert first["sequence"] == second["sequence"]


def test_social_live_status_serves_stale_snapshot_when_refresh_fails(monkeypatch) -> None:
    now = {"value": 100.0}
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_CACHE", None)
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_TTL_SECONDS", 5.0)
    monkeypatch.setattr(socials, "_LIVE_STATUS_SNAPSHOT_STALE_SECONDS", 30.0)
    monkeypatch.setattr(socials, "monotonic", lambda: now["value"])

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
    fresh = socials._build_live_status_payload()

    def fail_get_queue_status(**_kwargs):
        raise RuntimeError("queue unavailable")

    now["value"] = 106.0
    monkeypatch.setattr("trr_backend.repositories.social_season_analytics.get_queue_status", fail_get_queue_status)
    stale = socials._build_live_status_payload()

    assert stale["sequence"] == fresh["sequence"]
    assert stale["snapshot"]["cache_status"] == "stale"
    assert stale["snapshot"]["stale"] is True
    assert stale["snapshot"]["refresh_error"] == "RuntimeError"
    assert stale["snapshot"]["cache_age_ms"] == 6000
