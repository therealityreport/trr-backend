from __future__ import annotations

from trr_backend.repositories import admin_reddit_sources


def test_create_reddit_community_persists_analysis_flair_modes(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute_returning(query: str, params: list[object]) -> list[dict[str, object]]:
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "id": "community-1",
                "subreddit": "BravoRealHousewives",
                "analysis_flairs": ["Salt Lake City"],
                "analysis_all_flairs": ["Episode Discussion"],
            }
        ]

    monkeypatch.setattr(admin_reddit_sources.pg, "execute_returning", fake_execute_returning)

    row, query_count = admin_reddit_sources.create_reddit_community(
        payload={
            "trr_show_id": "22222222-2222-2222-2222-222222222222",
            "trr_show_name": "The Real Housewives of Salt Lake City",
            "subreddit": "r/BravoRealHousewives",
            "display_name": "Bravo Real Housewives",
            "analysis_flairs": ["Salt Lake City", "Episode Discussion"],
            "analysis_all_flairs": ["Episode Discussion"],
        },
        actor_uid="firebase:admin-1",
    )

    assert query_count == 1
    assert row["id"] == "community-1"
    query = str(captured["query"])
    params = list(captured["params"])  # type: ignore[arg-type]
    assert "analysis_flairs" in query
    assert "analysis_all_flairs" in query
    assert params[9] == '["Salt Lake City"]'
    assert params[10] == '["Episode Discussion"]'
