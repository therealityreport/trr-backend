from __future__ import annotations

from typing import Any

import pytest

from trr_backend.services import core_cast_credit_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "11111111-1111-1111-1111-111111111112"
PERSON_ID = "22222222-2222-2222-2222-222222222222"
PERSON_2_ID = "22222222-2222-2222-2222-222222222223"


def test_show_episode_evidence_cast_preserves_name_stats_and_photo_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def list_show_cast(*args: Any, **kwargs: Any):
        calls.append(("list_show_cast", args, kwargs))
        return (
            [
                {
                    "id": "33333333-3333-3333-3333-333333333333",
                    "show_id": SHOW_ID,
                    "person_id": PERSON_ID,
                    "show_name": "Test Show",
                    "cast_member_name": "Membership Name",
                    "role": "Self",
                    "billing_order": 2,
                    "credit_category": "Self",
                    "source_type": "imdb",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                    "eligible_total_episodes": 3,
                }
            ],
            1,
        )

    def get_people_by_ids(person_ids: list[str]):
        calls.append(("get_people_by_ids", (person_ids,), {}))
        return ({PERSON_ID: {"full_name": "Canonical Name", "known_for": "Reality TV"}}, 1)

    def get_show_cast_episode_totals(show_id: str, person_ids: list[str]):
        calls.append(("get_show_cast_episode_totals", (show_id, person_ids), {}))
        return (
            {
                PERSON_ID: {
                    "total_episodes": 4,
                    "archive_episodes": 1,
                    "person_name": "Evidence Name",
                }
            },
            1,
        )

    def get_preferred_cast_photos(person_ids: list[str], *, season_number: int | None = None):
        calls.append(("get_preferred_cast_photos", (person_ids,), {"season_number": season_number}))
        return (
            {
                PERSON_ID: {
                    "url": "https://cdn.example/person.jpg",
                    "thumbnail_focus_x": 45.0,
                    "thumbnail_focus_y": 30.0,
                    "thumbnail_zoom": 1.2,
                    "thumbnail_crop_mode": "manual",
                }
            },
            1,
        )

    monkeypatch.setattr(core_cast_credit_reads.repository, "list_show_cast", list_show_cast)
    monkeypatch.setattr(core_cast_credit_reads.repository, "get_people_by_ids", get_people_by_ids)
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_show_cast_episode_totals",
        get_show_cast_episode_totals,
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_preferred_cast_photos",
        get_preferred_cast_photos,
    )

    rows, query_count = core_cast_credit_reads.get_show_cast(
        SHOW_ID,
        view="episode_evidence",
        limit=20,
        offset=0,
        include_photos=True,
        photo_fallback="none",
    )

    assert query_count == 4
    assert rows == [
        {
            "id": "33333333-3333-3333-3333-333333333333",
            "show_id": SHOW_ID,
            "person_id": PERSON_ID,
            "show_name": "Test Show",
            "cast_member_name": "Membership Name",
            "role": "Self",
            "billing_order": 2,
            "credit_category": "Self",
            "source_type": "imdb",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "full_name": "Canonical Name",
            "known_for": "Reality TV",
            "photo_url": "https://cdn.example/person.jpg",
            "thumbnail_focus_x": 45.0,
            "thumbnail_focus_y": 30.0,
            "thumbnail_zoom": 1.2,
            "thumbnail_crop_mode": "manual",
            "total_episodes": 4,
            "archive_episode_count": 1,
        }
    ]
    assert calls[0] == (
        "list_show_cast",
        (SHOW_ID,),
        {"view": "episode_evidence", "limit": 20, "offset": 0},
    )


def test_show_membership_bravo_fallback_is_opt_in_bounded_and_soft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_row = {
        "id": "33333333-3333-3333-3333-333333333333",
        "show_id": SHOW_ID,
        "person_id": PERSON_ID,
        "show_name": "Test Show",
        "cast_member_name": "Canonical Name",
        "role": "Self",
        "billing_order": 1,
        "credit_category": "Self",
        "source_type": "imdb",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
    }
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_show_cast",
        lambda *args, **kwargs: ([base_row], 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_people_by_ids",
        lambda person_ids: ({PERSON_ID: {"full_name": "Canonical Name", "known_for": None}}, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_preferred_cast_photos",
        lambda person_ids, season_number=None: ({}, 3),
    )
    candidate_calls: list[list[str]] = []

    def get_bravo_photo_candidates(person_ids: list[str]):
        candidate_calls.append(person_ids)
        return (
            {
                PERSON_ID: {
                    "image_url": None,
                    "profile_url": "https://www.bravotv.com/people/canonical-name",
                }
            },
            2,
        )

    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_bravo_photo_candidates",
        get_bravo_photo_candidates,
    )
    request_calls: list[tuple[str, dict[str, Any]]] = []

    class FakeResponse:
        status_code = 200

        def iter_content(self, chunk_size: int):
            del chunk_size
            yield (
                b'<html><head><meta property="og:image" content="https://cdn.example/bravo-person.jpg"></head></html>'
            )

        def close(self) -> None:
            return None

    def fake_get(url: str, **kwargs: Any) -> FakeResponse:
        request_calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(core_cast_credit_reads.requests, "get", fake_get)

    rows, query_count = core_cast_credit_reads.get_show_cast(
        SHOW_ID,
        view="membership",
        include_photos=True,
        photo_fallback="bravo",
    )

    assert query_count == 7
    assert rows[0]["photo_url"] == "https://cdn.example/bravo-person.jpg"
    assert candidate_calls == [[PERSON_ID]]
    assert request_calls[0][0] == "https://www.bravotv.com/people/canonical-name"
    assert request_calls[0][1]["timeout"] == (1.0, 2.0)
    assert request_calls[0][1]["stream"] is True


def test_season_episode_counts_append_archive_only_and_preserve_current_total_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_season_context",
        lambda season_id: (
            {"id": season_id, "show_id": SHOW_ID, "season_number": 5},
            1,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_season_episode_counts",
        lambda *args, **kwargs: (
            [{"person_id": PERSON_ID, "episodes_in_season": 9}],
            {
                PERSON_ID: {"regular_episodes_in_season": 2, "archive_episodes_in_season": 1},
                PERSON_2_ID: {"regular_episodes_in_season": 0, "archive_episodes_in_season": 3},
            },
            2,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_season_membership_totals",
        lambda show_id, person_ids: (
            {
                PERSON_ID: {"person_name": "Evidence One", "total_episodes": 100},
                PERSON_2_ID: {"person_name": "Evidence Two", "total_episodes": 50},
            },
            1,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_people_by_ids",
        lambda person_ids: (
            {
                PERSON_ID: {"full_name": "Person One", "known_for": None},
                PERSON_2_ID: {"full_name": "Person Two", "known_for": None},
            },
            1,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_preferred_cast_photos",
        lambda person_ids, season_number=None: (
            {
                person_id: {
                    "url": f"https://cdn.example/{person_id}.jpg",
                    "thumbnail_focus_x": None,
                    "thumbnail_focus_y": None,
                    "thumbnail_zoom": None,
                    "thumbnail_crop_mode": None,
                }
                for person_id in person_ids
            },
            1,
        ),
    )

    rows, query_count = core_cast_credit_reads.get_season_cast(
        SEASON_ID,
        view="episode_counts",
        limit=1,
        offset=0,
        include_archive_only=True,
    )

    assert query_count == 6
    assert [row["person_id"] for row in rows] == [PERSON_ID, PERSON_2_ID]
    assert rows[0]["episodes_in_season"] == 2
    assert rows[0]["total_episodes"] == 2
    assert rows[0]["archive_episodes_in_season"] == 1
    assert rows[1]["episodes_in_season"] == 0
    assert rows[1]["total_episodes"] == 0
    assert rows[1]["archive_episodes_in_season"] == 3


def test_season_membership_preserves_person_show_seasons_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_season_context",
        lambda season_id: ({"id": season_id, "show_id": SHOW_ID, "season_number": 5}, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_season_membership",
        lambda *args, **kwargs: (
            [
                {
                    "person_id": PERSON_ID,
                    "person_name": "Person One",
                    "seasons_appeared": [4, 5],
                    "total_episodes": 12,
                }
            ],
            1,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_preferred_cast_photos",
        lambda person_ids, season_number=None: (
            {
                PERSON_ID: {
                    "url": "https://cdn.example/member.jpg",
                    "thumbnail_focus_x": None,
                    "thumbnail_focus_y": None,
                    "thumbnail_zoom": None,
                    "thumbnail_crop_mode": None,
                }
            },
            1,
        ),
    )

    rows, query_count = core_cast_credit_reads.get_season_cast(
        SEASON_ID,
        view="membership",
        limit=10,
        offset=0,
    )

    assert query_count == 3
    assert rows == [
        {
            "person_id": PERSON_ID,
            "person_name": "Person One",
            "seasons_appeared": [4, 5],
            "total_episodes": 12,
            "photo_url": "https://cdn.example/member.jpg",
            "thumbnail_focus_x": None,
            "thumbnail_focus_y": None,
            "thumbnail_zoom": None,
            "thumbnail_crop_mode": None,
        }
    ]


@pytest.mark.parametrize("view", ["membership", "episode_counts"])
def test_season_views_apply_opt_in_bravo_after_local_photo_sources(
    monkeypatch: pytest.MonkeyPatch,
    view: str,
) -> None:
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_season_context",
        lambda season_id: ({"id": season_id, "show_id": SHOW_ID, "season_number": 5}, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_season_membership",
        lambda *args, **kwargs: (
            [
                {
                    "person_id": PERSON_ID,
                    "person_name": "Person One",
                    "seasons_appeared": [5],
                    "total_episodes": 2,
                }
            ],
            1,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_season_episode_counts",
        lambda *args, **kwargs: (
            [{"person_id": PERSON_ID, "episodes_in_season": 2}],
            {PERSON_ID: {"regular_episodes_in_season": 2, "archive_episodes_in_season": 0}},
            2,
        ),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_season_membership_totals",
        lambda show_id, person_ids: ({PERSON_ID: {"person_name": "Person One", "total_episodes": 2}}, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_people_by_ids",
        lambda person_ids: ({PERSON_ID: {"full_name": "Person One", "known_for": None}}, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_preferred_cast_photos",
        lambda person_ids, season_number=None: ({}, 3),
    )
    bravo_calls: list[list[str]] = []

    def get_bravo_photo_candidates(person_ids: list[str]):
        bravo_calls.append(person_ids)
        return (
            {PERSON_ID: {"image_url": "https://cdn.example/bravo-season.jpg", "profile_url": None}},
            2,
        )

    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_bravo_photo_candidates",
        get_bravo_photo_candidates,
    )

    rows, _query_count = core_cast_credit_reads.get_season_cast(
        SEASON_ID,
        view=view,
        photo_fallback="bravo",
    )

    assert rows[0]["photo_url"] == "https://cdn.example/bravo-season.jpg"
    assert bravo_calls == [[PERSON_ID]]


def test_person_credits_keep_local_first_enrich_softly_then_paginate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local_credits = [
        {
            "id": "33333333-3333-3333-3333-333333333331",
            "show_id": SHOW_ID,
            "person_id": PERSON_ID,
            "show_name": "Local One",
            "role": "Host",
            "billing_order": 1,
            "credit_category": "Self",
            "source_type": "imdb",
            "external_imdb_id": "tt1000001",
            "external_url": "https://www.imdb.com/title/tt1000001/",
            "metadata": None,
        },
        {
            "id": "33333333-3333-3333-3333-333333333332",
            "show_id": None,
            "person_id": PERSON_ID,
            "show_name": "Local Two",
            "role": None,
            "billing_order": 2,
            "credit_category": "Self",
            "source_type": "manual",
            "external_imdb_id": None,
            "external_url": None,
            "metadata": None,
        },
    ]
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_local_person_credits",
        lambda person_id: (local_credits, 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "get_person_imdb_id",
        lambda person_id: ("nm1000001", 1),
    )
    mapped_show_id = "11111111-1111-1111-1111-111111111119"
    mapping_calls: list[list[str]] = []

    def map_imdb_titles(imdb_title_ids: list[str]):
        mapping_calls.append(imdb_title_ids)
        return ({"tt1000002": {"show_id": mapped_show_id, "show_name": "Mapped Show"}}, 1)

    monkeypatch.setattr(core_cast_credit_reads.repository, "map_imdb_titles", map_imdb_titles)
    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_curated_cast_show_ids",
        lambda person_id: ([mapped_show_id], 1),
    )
    monkeypatch.setattr(
        core_cast_credit_reads.name_filmography,
        "fetch_name_filmography",
        lambda imdb_person_id: [
            {
                "imdb_title_id": "tt1000001",
                "show_name": "Duplicate Local",
                "external_url": "https://www.imdb.com/title/tt1000001/",
            },
            {
                "imdb_title_id": "tt1000002",
                "show_name": "IMDb Mapped Name",
                "external_url": "https://www.imdb.com/title/tt1000002/",
            },
            {
                "imdb_title_id": "tt1000003",
                "show_name": "Zulu Show",
                "external_url": "https://www.imdb.com/title/tt1000003/",
            },
        ],
    )

    payload, query_count = core_cast_credit_reads.get_person_credits(
        PERSON_ID,
        limit=2,
        offset=1,
    )

    assert query_count == 4
    assert payload["total_count"] == 4
    assert payload["curated_cast_show_ids"] == [mapped_show_id]
    assert [credit["id"] for credit in payload["credits"]] == [
        "33333333-3333-3333-3333-333333333332",
        f"imdb-{PERSON_ID}-tt1000002",
    ]
    assert payload["credits"][1]["show_id"] == mapped_show_id
    assert mapping_calls == [["tt1000001", "tt1000002", "tt1000003"]]


def test_person_episode_credits_preserve_show_id_and_bound_response_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "show_id": SHOW_ID,
            "credit_id": f"33333333-3333-3333-3333-33333333333{index}",
            "credit_category": "Self",
            "role": "Host",
            "billing_order": index,
            "source_type": "imdb",
            "episode_id": f"44444444-4444-4444-4444-44444444444{index}",
            "season_number": 5,
            "episode_number": index,
            "episode_name": f"Episode {index}",
            "appearance_type": "appears",
        }
        for index in (1, 2, 3)
    ]
    calls: list[dict[str, Any]] = []

    def list_person_episode_credits(person_id: str, **kwargs: Any):
        calls.append({"person_id": person_id, **kwargs})
        return rows, 1

    monkeypatch.setattr(
        core_cast_credit_reads.repository,
        "list_person_episode_credits",
        list_person_episode_credits,
    )

    payload, query_count = core_cast_credit_reads.get_person_episode_credits(
        PERSON_ID,
        show_id=SHOW_ID,
        include_archive_footage=False,
        limit=2,
        offset=1,
    )

    assert query_count == 1
    assert payload["total_count"] == 3
    assert [row["episode_number"] for row in payload["episode_credits"]] == [2, 3]
    assert all(row["show_id"] == SHOW_ID for row in payload["episode_credits"])
    assert calls == [
        {
            "person_id": PERSON_ID,
            "show_id": SHOW_ID,
            "include_archive_footage": False,
        }
    ]
