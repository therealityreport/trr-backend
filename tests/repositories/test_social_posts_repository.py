from __future__ import annotations

from typing import Any

from trr_backend.repositories import social_posts as repo


def test_list_posts_for_show_uses_show_and_optional_season_filters(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params=None):
        captured["query"] = query
        captured["params"] = list(params or [])
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.list_posts_for_show("show-1", trr_season_id="season-1")

    assert payload == []
    assert query_count == 1
    assert captured["params"] == ["show-1", "season-1"]


def test_create_post_shapes_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "execute_returning",
        lambda query, params=None: [
            {
                "id": "post-1",
                "trr_show_id": "show-1",
                "trr_season_id": "season-1",
                "platform": "instagram",
                "url": "https://instagram.com/p/abc",
                "title": "Title",
                "notes": None,
                "created_by_firebase_uid": "firebase:admin-1",
                "created_at": "2026-03-26T00:00:00Z",
                "updated_at": "2026-03-26T00:00:00Z",
            }
        ],
    )

    payload, query_count = repo.create_post(
        trr_show_id="show-1",
        trr_season_id="season-1",
        platform="instagram",
        url="https://instagram.com/p/abc",
        title="Title",
        notes=None,
        actor_uid="firebase:admin-1",
    )

    assert query_count == 1
    assert payload["platform"] == "instagram"
    assert payload["created_by_firebase_uid"] == "firebase:admin-1"
