from __future__ import annotations

from trr_backend.repositories import recent_people as repo


def test_list_recent_people_returns_empty_when_relation_missing(monkeypatch) -> None:
    def fake_fetch_all(query: str, params=None):
        raise RuntimeError('relation "admin.recent_people_views" does not exist')

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.list_recent_people("firebase:admin-1", limit=20)

    assert payload == []
    assert query_count == 0
