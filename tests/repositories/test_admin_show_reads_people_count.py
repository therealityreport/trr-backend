from __future__ import annotations

import pytest

from trr_backend.repositories import admin_show_reads as repo


def test_season_assets_preserve_a_valid_zero_tag_people_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch_one(query: str, params=None, cur=None):
        if "from core.seasons" in str(query):
            return {
                "id": "11111111-1111-1111-1111-111111111111",
                "premiere_date": "2024-01-01",
                "air_date": None,
                "episode_start_date": "2024-01-01",
                "episode_end_date": "2024-03-01",
                "name": "Bravo Show",
                "external_ids": {},
            }
        raise AssertionError(f"unexpected query: {query}")

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql or "from core.season_images" in sql:
            return []
        if "from core.episode_images" in sql:
            return []
        if "from core.credits as c" in sql:
            return [{"person_id": "22222222-2222-2222-2222-222222222222", "person_name": "Cast Member"}]
        if "from core.cast_photos as cp" in sql:
            return [
                {
                    "id": "33333333-3333-3333-3333-333333333333",
                    "person_id": "22222222-2222-2222-2222-222222222222",
                    "source": "getty",
                    "url": "https://source.example.com/cast.jpg",
                    "hosted_url": "https://cdn.example.com/cast.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": 1000,
                    "height": 1500,
                    "caption": None,
                    "context_section": None,
                    "context_type": None,
                    "season": 6,
                    "fetched_at": None,
                    "hosted_at": None,
                    "updated_at": None,
                    "title_imdb_ids": [],
                    "title_names": [],
                    "metadata": {"people_count": 7, "people_count_source": "auto"},
                    "people_count": 0,
                    "people_count_source": "manual",
                }
            ]
        raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, _query_count = repo._get_show_season_assets_impl(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        6,
        limit=20,
        offset=0,
    )

    assert assets[0]["people_count"] == 0
    assert assets[0]["people_count_source"] == "manual"
