from __future__ import annotations

from typing import Any

import pytest

from trr_backend.repositories import admin_show_person_writes as repository

SHOW_ID = "11111111-1111-1111-1111-111111111111"
PERSON_ID = "22222222-2222-2222-2222-222222222222"


def test_update_show_uses_one_post_write_cte_with_slug_and_featured_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def execute_returning(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": SHOW_ID,
                "name": "Updated Show",
                "canonical_slug": "updated-show--11111111",
                "poster_url": "https://cdn.example.test/poster.jpg",
                "backdrop_url": "https://cdn.example.test/backdrop.jpg",
                "logo_url": "https://cdn.example.test/logo.jpg",
            }
        ]

    monkeypatch.setattr(repository.pg, "execute_returning", execute_returning)

    show, query_count = repository.update_show(
        SHOW_ID,
        {"name": "Updated Show", "external_ids": {"instagram": "updated"}},
    )

    assert query_count == 1
    assert show is not None
    assert show["canonical_slug"] == "updated-show--11111111"
    assert "UPDATE core.shows AS updated" in captured["sql"]
    assert "COUNT(*) OVER" in captured["sql"]
    assert "LEFT JOIN core.show_images AS poster" in captured["sql"]
    assert captured["params"][-1] == SHOW_ID
    assert captured["params"][1].adapted == {"instagram": "updated"}


def test_empty_show_patch_reads_the_same_full_shape_without_mutating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def execute_returning(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(repository.pg, "execute_returning", execute_returning)

    show, query_count = repository.update_show(SHOW_ID, {})

    assert show is None
    assert query_count == 1
    assert "SELECT id\n          FROM core.shows" in captured["sql"]
    assert "UPDATE core.shows AS updated" not in captured["sql"]
    assert captured["params"] == [SHOW_ID]


@pytest.mark.parametrize(
    ("source_order", "message"),
    [
        (["imdb"], "source_order_must_include_all_sources"),
        (["imdb", "tmdb", "fandom", "fandom"], "source_order_contains_duplicates"),
        (["imdb", "tmdb", "fandom", "unknown"], "source_order_contains_invalid_source"),
    ],
)
def test_canonical_profile_source_order_rejects_incomplete_duplicate_or_unknown_sources(
    source_order: list[str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        repository.normalize_canonical_profile_source_order(source_order)


def test_canonical_profile_source_order_updates_jsonb_with_the_allowed_complete_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def execute_returning(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": PERSON_ID, "external_ids": {"canonical_profile_source_order": params[0]}}]

    monkeypatch.setattr(repository.pg, "execute_returning", execute_returning)

    person, query_count = repository.update_person_canonical_profile_source_order(
        PERSON_ID,
        ["TMDB", " imdb ", "manual", "fandom"],
    )

    assert query_count == 1
    assert person is not None
    assert captured["params"] == [["tmdb", "imdb", "manual", "fandom"], PERSON_ID]
    assert "jsonb_set" in captured["sql"]
    assert "updated_at = now()" in captured["sql"]


def test_effective_person_social_handles_preserve_override_precedence_and_missing_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_person_id = "33333333-3333-3333-3333-333333333333"
    captured: dict[str, Any] = {}

    def fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "person_id": PERSON_ID,
                "external_ids": {
                    "facebook_id": "https://www.facebook.com/people/A-Name/1000",
                    "instagram": "https://www.instagram.com/from-external/",
                    "tiktok": "@from-external",
                    "x_handle": "https://x.com/from-external",
                    "youtube": "https://www.youtube.com/channel/UC123",
                },
                "instagram_override": " https://instagram.com/override/ ",
                "tiktok_override": None,
                "twitter_override": "@override",
                "youtube_override": "https://www.youtube.com/@override",
            }
        ]

    monkeypatch.setattr(repository.pg, "fetch_all", fetch_all)

    handles, query_count = repository.list_effective_person_social_handles([PERSON_ID, missing_person_id, PERSON_ID])

    assert query_count == 1
    assert captured["params"] == [[PERSON_ID, missing_person_id]]
    assert "LEFT JOIN core.people_overrides" in captured["sql"]
    assert handles == [
        {
            "person_id": PERSON_ID,
            "facebook_handle": "1000",
            "instagram_handle": "override",
            "tiktok_handle": "from-external",
            "twitter_handle": "override",
            "youtube_handle": "@override",
        },
        {
            "person_id": missing_person_id,
            "facebook_handle": None,
            "instagram_handle": None,
            "tiktok_handle": None,
            "twitter_handle": None,
            "youtube_handle": None,
        },
    ]
