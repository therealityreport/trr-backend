from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_reddit_reads as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


@pytest.fixture(autouse=True)
def clear_cache():
    with router_module._CACHE_LOCK:  # type: ignore[attr-defined]
        router_module._CACHE.clear()  # type: ignore[attr-defined]
    with router_module._INFLIGHT_LOCK:  # type: ignore[attr-defined]
        router_module._INFLIGHT.clear()  # type: ignore[attr-defined]
    yield
    with router_module._CACHE_LOCK:  # type: ignore[attr-defined]
        router_module._CACHE.clear()  # type: ignore[attr-defined]
    with router_module._INFLIGHT_LOCK:  # type: ignore[attr-defined]
        router_module._INFLIGHT.clear()  # type: ignore[attr-defined]


def test_list_communities_returns_contract_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_list_communities(**kwargs):
        calls["count"] += 1
        return (
            {
                "communities": [
                    {
                        "id": "community-1",
                        "trr_show_id": "show-1",
                        "trr_show_name": "The Real Housewives of Salt Lake City",
                        "subreddit": "BravoRealHousewives",
                        "assigned_thread_count": 0,
                        "assigned_threads": [],
                    }
                ]
            },
            1,
        )

    monkeypatch.setattr(router_module.reddit_reads_repo, "list_reddit_communities", fake_list_communities)

    client = TestClient(app)
    first = client.get("/api/v1/admin/reddit/communities")
    second = client.get("/api/v1/admin/reddit/communities")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["communities"][0]["subreddit"] == "BravoRealHousewives"
    assert calls["count"] == 1


def test_community_create_update_post_flairs_and_delete_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    community_id = "11111111-1111-1111-1111-111111111111"
    show_id = "22222222-2222-2222-2222-222222222222"
    calls: dict[str, object] = {}

    def fake_create(**kwargs):
        calls["create"] = kwargs
        return (
            {
                "id": community_id,
                "trr_show_id": kwargs["payload"]["trr_show_id"],
                "trr_show_name": kwargs["payload"]["trr_show_name"],
                "subreddit": "BravoRealHousewives",
                "display_name": "Bravo",
                "notes": None,
                "post_flairs": [],
                "analysis_flairs": [],
                "analysis_all_flairs": [],
                "is_show_focused": True,
                "network_focus_targets": [],
                "franchise_focus_targets": [],
                "episode_title_patterns": [],
                "post_flair_categories": {},
                "post_flair_assignments": {},
                "post_flairs_updated_at": None,
                "is_active": True,
                "created_by_firebase_uid": kwargs["actor_uid"],
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T00:00:00Z",
            },
            1,
        )

    def fake_update(**kwargs):
        calls["update"] = kwargs
        return (
            {
                "id": kwargs["community_id"],
                "trr_show_id": show_id,
                "trr_show_name": "The Traitors",
                "subreddit": "TheTraitors",
                "display_name": "Traitors",
                "notes": kwargs["payload"].get("notes"),
                "post_flairs": [],
                "analysis_flairs": kwargs["payload"].get("analysis_flairs", []),
                "analysis_all_flairs": [],
                "is_show_focused": True,
                "network_focus_targets": [],
                "franchise_focus_targets": [],
                "episode_title_patterns": [],
                "post_flair_categories": {},
                "post_flair_assignments": {},
                "post_flairs_updated_at": None,
                "is_active": True,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T01:00:00Z",
            },
            1,
        )

    def fake_update_flairs(**kwargs):
        calls["flairs"] = kwargs
        return (
            {
                "id": kwargs["community_id"],
                "trr_show_id": show_id,
                "trr_show_name": "The Traitors",
                "subreddit": "TheTraitors",
                "display_name": "Traitors",
                "notes": None,
                "post_flairs": kwargs["post_flairs"],
                "analysis_flairs": [],
                "analysis_all_flairs": [],
                "is_show_focused": True,
                "network_focus_targets": [],
                "franchise_focus_targets": [],
                "episode_title_patterns": [],
                "post_flair_categories": {},
                "post_flair_assignments": {},
                "post_flairs_updated_at": kwargs["post_flairs_updated_at"],
                "is_active": True,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T01:00:00Z",
            },
            2,
        )

    monkeypatch.setattr(router_module.reddit_sources_repo, "create_reddit_community", fake_create)
    monkeypatch.setattr(router_module.reddit_sources_repo, "update_reddit_community", fake_update)
    monkeypatch.setattr(router_module.reddit_sources_repo, "update_reddit_community_post_flairs", fake_update_flairs)
    monkeypatch.setattr(router_module.reddit_sources_repo, "delete_reddit_community", lambda community_id: (True, 1))

    client = TestClient(app)
    created = client.post(
        "/api/v1/admin/reddit/communities",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={
            "trr_show_id": show_id,
            "trr_show_name": "The Traitors",
            "subreddit": "r/TheTraitors",
            "display_name": "Traitors",
            "is_show_focused": True,
        },
    )
    updated = client.patch(
        f"/api/v1/admin/reddit/communities/{community_id}",
        json={"notes": "Watch this community", "analysis_flairs": ["Episode Discussion"]},
    )
    flairs = client.patch(
        f"/api/v1/admin/reddit/communities/{community_id}/post-flairs",
        json={"post_flairs": ["Live Episode Discussion"], "post_flairs_updated_at": "2026-03-26T01:00:00Z"},
    )
    deleted = client.delete(f"/api/v1/admin/reddit/communities/{community_id}")

    assert created.status_code == 201
    assert created.json()["community"]["created_by_firebase_uid"] == "firebase:admin-1"
    assert updated.status_code == 200
    assert updated.json()["community"]["notes"] == "Watch this community"
    assert flairs.status_code == 200
    assert flairs.json()["flairs"] == ["Live Episode Discussion"]
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
    assert calls["create"]["actor_uid"] == "firebase:admin-1"  # type: ignore[index]


def test_threads_summary_collapses_cold_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_list_threads(**kwargs):
        calls["count"] += 1
        return (
            {
                "threads": [
                    {
                        "id": "thread-1",
                        "community_id": "community-1",
                        "trr_show_id": "show-1",
                        "reddit_post_id": "post-1",
                        "title": "Episode Thread",
                        "url": "https://reddit.com/r/show/comments/abc123",
                    }
                ]
            },
            1,
        )

    monkeypatch.setattr(router_module.reddit_reads_repo, "list_reddit_threads", fake_list_threads)

    client = TestClient(app)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(client.get, "/api/v1/admin/reddit/threads")
        second_future = executor.submit(client.get, "/api/v1/admin/reddit/threads")
        first = first_future.result()
        second = second_future.result()

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1


def test_thread_create_update_and_delete_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    community_id = "11111111-1111-1111-1111-111111111111"
    show_id = "22222222-2222-2222-2222-222222222222"
    thread_id = "33333333-3333-3333-3333-333333333333"
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        router_module.reddit_reads_repo,
        "get_reddit_community_by_id",
        lambda community_id: (
            {
                "id": community_id,
                "trr_show_id": show_id,
                "trr_show_name": "The Traitors",
                "subreddit": "TheTraitors",
            },
            1,
        ),
    )
    monkeypatch.setattr(
        router_module.reddit_reads_repo,
        "get_reddit_thread_by_id",
        lambda thread_id: (
            {
                "id": thread_id,
                "community_id": community_id,
                "trr_show_id": show_id,
                "trr_show_name": "The Traitors",
                "trr_season_id": None,
                "source_kind": "manual",
                "reddit_post_id": "abc123",
                "title": "Episode Thread",
                "url": "https://www.reddit.com/r/TheTraitors/comments/abc123",
                "created_by_firebase_uid": "firebase:admin-1",
            },
            1,
        ),
    )

    def fake_create(**kwargs):
        calls["create_thread"] = kwargs
        return (
            {
                "id": thread_id,
                "community_id": kwargs["payload"]["community_id"],
                "trr_show_id": kwargs["payload"]["trr_show_id"],
                "trr_show_name": kwargs["payload"]["trr_show_name"],
                "trr_season_id": kwargs["payload"]["trr_season_id"],
                "source_kind": kwargs["payload"].get("source_kind", "manual"),
                "reddit_post_id": kwargs["payload"]["reddit_post_id"],
                "title": kwargs["payload"]["title"],
                "url": kwargs["payload"]["url"],
                "permalink": kwargs["payload"]["permalink"],
                "author": None,
                "score": 0,
                "num_comments": 0,
                "posted_at": None,
                "notes": None,
                "created_by_firebase_uid": kwargs["actor_uid"],
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T00:00:00Z",
            },
            1,
        )

    def fake_update(**kwargs):
        calls["update_thread"] = kwargs
        return (
            {
                "id": kwargs["thread_id"],
                "community_id": community_id,
                "trr_show_id": show_id,
                "trr_show_name": "The Traitors",
                "trr_season_id": None,
                "source_kind": "manual",
                "reddit_post_id": "abc123",
                "title": kwargs["payload"]["title"],
                "url": "https://www.reddit.com/r/TheTraitors/comments/abc123",
                "permalink": None,
                "author": None,
                "score": 0,
                "num_comments": 0,
                "posted_at": None,
                "notes": None,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T01:00:00Z",
            },
            1,
        )

    monkeypatch.setattr(router_module.reddit_sources_repo, "create_reddit_thread", fake_create)
    monkeypatch.setattr(router_module.reddit_sources_repo, "update_reddit_thread", fake_update)
    monkeypatch.setattr(router_module.reddit_sources_repo, "delete_reddit_thread", lambda thread_id: (True, 1))

    client = TestClient(app)
    created = client.post(
        "/api/v1/admin/reddit/threads",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={
            "community_id": community_id,
            "trr_show_id": show_id,
            "trr_show_name": "The Traitors",
            "reddit_post_id": "abc123",
            "title": "Episode Thread",
            "url": "https://www.reddit.com/r/TheTraitors/comments/abc123",
            "permalink": "/r/TheTraitors/comments/abc123",
        },
    )
    updated = client.patch(
        f"/api/v1/admin/reddit/threads/{thread_id}",
        json={"title": "Updated Episode Thread"},
    )
    deleted = client.delete(f"/api/v1/admin/reddit/threads/{thread_id}")

    assert created.status_code == 201
    assert created.json()["thread"]["created_by_firebase_uid"] == "firebase:admin-1"
    assert updated.status_code == 200
    assert updated.json()["thread"]["title"] == "Updated Episode Thread"
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
    assert calls["create_thread"]["actor_uid"] == "firebase:admin-1"  # type: ignore[index]


def test_write_routes_return_consistent_uuid_validation_error() -> None:
    client = TestClient(app)
    response = client.delete("/api/v1/admin/reddit/threads/not-a-uuid")

    assert response.status_code == 400
    assert response.json() == {"detail": "thread_id must be a valid UUID"}


def test_summary_and_resolve_routes_preserve_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    community_id = "11111111-1111-1111-1111-111111111111"
    season_id = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setattr(
        router_module.reddit_reads_repo,
        "get_reddit_community_analytics_summary",
        lambda **kwargs: (
            {
                "scope": "season",
                "season_id": "season-1",
                "totals": {
                    "post_count": 12,
                    "tracked_flair_post_count": 8,
                    "show_match_post_count": 7,
                    "comment_count": 101,
                    "score_sum": 203,
                    "season_count": 1,
                },
                "diagnostics": {
                    "updated_at": "2026-03-26T00:00:00Z",
                    "source_table": "social.reddit_period_post_matches",
                    "row_count": 12,
                },
                "freshness": {"latest_data_timestamp": "2026-03-26T00:00:00Z"},
                "coverage": {"post_count": 12},
                "container_statuses": [],
            },
            2,
        ),
    )
    monkeypatch.setattr(
        router_module.reddit_reads_repo,
        "resolve_reddit_post_detail_by_slug",
        lambda **kwargs: (
            {
                "reddit_post_id": "post-1",
                "detail_slug": "episode-thread--u-bravofan",
                "collision": False,
                "post": {
                    "title": "Episode Thread",
                    "author": "BravoFan",
                    "posted_at": "2026-03-26T00:00:00Z",
                    "url": "https://reddit.com/r/show/comments/abc123",
                    "permalink": "https://reddit.com/r/show/comments/abc123",
                },
            },
            1,
        ),
    )

    client = TestClient(app)
    summary = client.get(
        f"/api/v1/admin/reddit/analytics/community/{community_id}/summary?scope=season&season_id={season_id}",
    )
    resolved = client.get(
        f"/api/v1/admin/reddit/communities/{community_id}/posts/resolve",
        params={"season_id": season_id, "window_key": "e1", "post_id": "post-1"},
    )

    assert summary.status_code == 200
    assert summary.json()["totals"]["score_sum"] == 203
    assert resolved.status_code == 200
    assert resolved.json()["detail_slug"] == "episode-thread--u-bravofan"


def test_post_detail_route_preserves_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    community_id = "11111111-1111-1111-1111-111111111111"
    season_id = "22222222-2222-2222-2222-222222222222"
    monkeypatch.setattr(
        router_module.reddit_reads_repo,
        "get_reddit_post_details_by_community_and_season",
        lambda **kwargs: (
            {
                "reddit_post_id": "post-1",
                "title": "Episode Thread",
                "comments": [{"reddit_comment_id": "c1", "score": 5}],
                "comment_summary": {
                    "total_comments": 1,
                    "top_level_comments": 1,
                    "earliest_comment_at": None,
                    "latest_comment_at": None,
                },
                "media": [],
                "media_summary": {
                    "total_media": 0,
                    "mirrored_media": 0,
                    "pending_media": 0,
                    "failed_media": 0,
                },
                "assigned_threads": [],
                "matches": [],
                "source_sorts": [],
                "media_metadata": {},
                "poll_data": {},
            },
            6,
        ),
    )

    client = TestClient(app)
    response = client.get(
        f"/api/v1/admin/reddit/communities/{community_id}/posts/post-1/details",
        params={"season_id": season_id, "comments_limit": 100},
    )

    assert response.status_code == 200
    assert response.json()["post"]["reddit_post_id"] == "post-1"
    assert response.json()["post"]["comment_summary"]["total_comments"] == 1
