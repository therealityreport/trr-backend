from __future__ import annotations

from typing import Any

import pytest

from trr_backend.repositories import core_cast_credit_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"
SEASON_ID = "11111111-1111-1111-1111-111111111112"
PERSON_ID = "22222222-2222-2222-2222-222222222222"
PERSON_2_ID = "22222222-2222-2222-2222-222222222223"
PERSON_3_ID = "22222222-2222-2222-2222-222222222224"


def _compact(sql: str) -> str:
    return " ".join(sql.lower().split())


@pytest.mark.parametrize(
    ("view", "expected_sql", "expected_params"),
    [
        ("membership", "from core.v_show_cast as vsc", [SHOW_ID, 500, 0]),
        (
            "episode_evidence",
            "having count(distinct episode_id) > 0",
            [SHOW_ID, SHOW_ID, 500, 0],
        ),
        (
            "archive_only",
            "coalesce(episode_counts.regular_episodes, 0) = 0",
            [SHOW_ID, SHOW_ID, 500, 0],
        ),
    ],
)
def test_list_show_cast_preserves_app_view_filters_order_and_limit(
    monkeypatch: pytest.MonkeyPatch,
    view: core_cast_credit_reads.ShowCastView,
    expected_sql: str,
    expected_params: list[Any],
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return [{"id": "33333333-3333-3333-3333-333333333333", "person_id": PERSON_ID}]

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_cast_credit_reads.list_show_cast(
        SHOW_ID,
        view=view,
        limit=1000,
        offset=-5,
    )

    assert rows == [{"id": "33333333-3333-3333-3333-333333333333", "person_id": PERSON_ID}]
    assert query_count == 1
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert expected_sql in normalized_sql
    assert "order by billing_order asc nulls last" in normalized_sql
    assert params == expected_params


def test_preferred_cast_photos_use_gallery_crop_then_cast_photo_then_entity_featured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        normalized_sql = _compact(sql)
        if "from core.media_links" in normalized_sql:
            return [
                {
                    "person_id": PERSON_ID,
                    "hosted_url": "https://cdn.example/gallery-original.jpg",
                    "hosted_content_type": "image/jpeg",
                    "metadata": {
                        "variants": {"base": {"thumb": {"webp": {"url": "https://cdn.example/thumb.webp"}}}},
                        "thumbnail_crop": {"x": 125, "y": -4, "zoom": 9, "mode": "manual"},
                    },
                    "context": None,
                }
            ]
        if "from core.v_cast_photos" in normalized_sql:
            return [
                {
                    "person_id": PERSON_2_ID,
                    "thumb_url": None,
                    "display_url": "https://cdn.example/cast-photo.jpg",
                    "hosted_url": None,
                    "url": None,
                }
            ]
        if "metadata->>'featured_image_url'" in normalized_sql:
            return [
                {
                    "person_id": PERSON_3_ID,
                    "featured_image_url": "https://cdn.example/featured.jpg",
                }
            ]
        raise AssertionError(normalized_sql)

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    photos, query_count = core_cast_credit_reads.get_preferred_cast_photos(
        [PERSON_ID, PERSON_2_ID, PERSON_3_ID],
        season_number=5,
    )

    assert query_count == 3
    assert photos == {
        PERSON_ID: {
            "url": "https://cdn.example/thumb.webp",
            "thumbnail_focus_x": 100.0,
            "thumbnail_focus_y": 0.0,
            "thumbnail_zoom": 4.0,
            "thumbnail_crop_mode": "manual",
        },
        PERSON_2_ID: {
            "url": "https://cdn.example/cast-photo.jpg",
            "thumbnail_focus_x": None,
            "thumbnail_focus_y": None,
            "thumbnail_zoom": None,
            "thumbnail_crop_mode": None,
        },
        PERSON_3_ID: {
            "url": "https://cdn.example/featured.jpg",
            "thumbnail_focus_x": None,
            "thumbnail_focus_y": None,
            "thumbnail_zoom": None,
            "thumbnail_crop_mode": None,
        },
    }
    assert calls[0][1] == [[PERSON_ID, PERSON_2_ID, PERSON_3_ID], 5, 5, 5, 5]
    assert calls[1][1] == [[PERSON_2_ID, PERSON_3_ID], 5, 5, 5, 5]
    assert calls[2][1][0] == [PERSON_3_ID]


def test_season_episode_counts_fall_back_through_current_relations_and_keep_archive_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    class MissingRelationError(RuntimeError):
        code = "42P01"

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        normalized_sql = _compact(sql)
        if "from core.v_season_cast" in normalized_sql:
            raise MissingRelationError("v_season_cast missing")
        if "from core.v_episode_cast" in normalized_sql:
            raise MissingRelationError("v_episode_cast missing")
        if "as episodes_in_season" in normalized_sql and "order by episodes_in_season" in normalized_sql:
            return [{"person_id": PERSON_ID, "episodes_in_season": 2}]
        if "as regular_episodes_in_season" in normalized_sql:
            return [
                {
                    "person_id": PERSON_ID,
                    "regular_episodes_in_season": 2,
                    "archive_episodes_in_season": 1,
                },
                {
                    "person_id": PERSON_2_ID,
                    "regular_episodes_in_season": 0,
                    "archive_episodes_in_season": 3,
                },
            ]
        raise AssertionError(normalized_sql)

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    counts, evidence, query_count = core_cast_credit_reads.list_season_episode_counts(
        SHOW_ID,
        SEASON_ID,
        5,
        limit=10,
        offset=4,
    )

    assert counts == [{"person_id": PERSON_ID, "episodes_in_season": 2}]
    assert evidence == {
        PERSON_ID: {"regular_episodes_in_season": 2, "archive_episodes_in_season": 1},
        PERSON_2_ID: {"regular_episodes_in_season": 0, "archive_episodes_in_season": 3},
    }
    assert query_count == 4
    assert calls[0][1] == [SHOW_ID, SEASON_ID, 10, 4]
    assert calls[1][1] == [SHOW_ID, SEASON_ID, 10, 4]
    assert calls[2][1] == [SHOW_ID, 5, 10, 4]
    assert calls[3][1] == [SHOW_ID, 5]


def test_season_membership_uses_person_show_seasons_order_and_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return [
            {
                "person_id": PERSON_ID,
                "person_name": "Person One",
                "seasons_appeared": [4, 5],
                "total_episodes": 12,
            }
        ]

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_cast_credit_reads.list_season_membership(
        SHOW_ID,
        5,
        limit=900,
        offset=-1,
    )

    assert query_count == 1
    assert rows[0]["seasons_appeared"] == [4, 5]
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "from core.v_person_show_seasons" in normalized_sql
    assert "seasons_appeared @> array[%s]::int[]" in normalized_sql
    assert "order by total_episodes desc" in normalized_sql
    assert params == [SHOW_ID, 5, 500, 0]


def test_local_person_credits_preserve_app_order_and_external_imdb_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return [
            {
                "id": "33333333-3333-3333-3333-333333333333",
                "show_id": SHOW_ID,
                "person_id": PERSON_ID,
                "show_name": "Show One",
                "role": "Host",
                "billing_order": 1,
                "credit_category": "Self",
                "source_type": "imdb",
                "show_imdb_id": "tt1000001",
                "metadata": {"episode_count": 3},
            }
        ]

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_cast_credit_reads.list_local_person_credits(PERSON_ID)

    assert query_count == 1
    assert rows == [
        {
            "id": "33333333-3333-3333-3333-333333333333",
            "show_id": SHOW_ID,
            "person_id": PERSON_ID,
            "show_name": "Show One",
            "role": "Host",
            "billing_order": 1,
            "credit_category": "Self",
            "source_type": "imdb",
            "external_imdb_id": "tt1000001",
            "external_url": "https://www.imdb.com/title/tt1000001/",
            "metadata": {"episode_count": 3},
        }
    ]
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "from core.credits as c" in normalized_sql
    assert "left join lateral" in normalized_sql
    assert "order by c.billing_order asc nulls last, s.name asc nulls last, c.id asc" in normalized_sql
    assert params == [PERSON_ID]


@pytest.mark.parametrize(
    ("show_id", "include_archive", "expected_params", "has_archive_filter"),
    [
        (None, False, [PERSON_ID], True),
        (SHOW_ID, True, [PERSON_ID, SHOW_ID], False),
    ],
)
def test_person_episode_credits_preserve_scope_archive_default_and_order(
    monkeypatch: pytest.MonkeyPatch,
    show_id: str | None,
    include_archive: bool,
    expected_params: list[Any],
    has_archive_filter: bool,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return [
            {
                "show_id": SHOW_ID,
                "credit_id": "33333333-3333-3333-3333-333333333333",
                "credit_category": "Self",
                "role": "Host",
                "billing_order": 1,
                "source_type": "imdb",
                "episode_id": "44444444-4444-4444-4444-444444444444",
                "season_number": 5,
                "episode_number": 2,
                "episode_name": "Dinner",
                "appearance_type": "appears",
            }
        ]

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    rows, query_count = core_cast_credit_reads.list_person_episode_credits(
        PERSON_ID,
        show_id=show_id,
        include_archive_footage=include_archive,
    )

    assert query_count == 1
    assert rows[0]["show_id"] == SHOW_ID
    sql, params = calls[0]
    normalized_sql = _compact(sql)
    assert "from core.v_episode_credits as vec" in normalized_sql
    assert ("coalesce(vec.appearance_type, 'appears') <> 'archive_footage'" in normalized_sql) is has_archive_filter
    assert "vec.billing_order asc nulls last" in normalized_sql
    assert "vec.season_number desc nulls last" in normalized_sql
    assert params == expected_params


def test_imdb_title_mapping_and_curated_cast_roles_use_current_active_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[Any]]] = []

    def fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        normalized_sql = _compact(sql)
        if "select distinct on (imdb_title_id)" in normalized_sql:
            return [{"show_id": SHOW_ID, "show_name": "Mapped Show", "imdb_title_id": "tt1000001"}]
        if "from core.show_cast_role_assignments" in normalized_sql:
            return [{"show_id": SHOW_ID}]
        raise AssertionError(normalized_sql)

    monkeypatch.setattr(core_cast_credit_reads.pg, "fetch_all", fake_fetch_all)

    mapping, mapping_queries = core_cast_credit_reads.map_imdb_titles(["TT1000001", "tt1000001"])
    curated, curated_queries = core_cast_credit_reads.list_curated_cast_show_ids(PERSON_ID)

    assert mapping == {"tt1000001": {"show_id": SHOW_ID, "show_name": "Mapped Show"}}
    assert curated == [SHOW_ID]
    assert mapping_queries == curated_queries == 1
    assert calls[0][1] == [["tt1000001"], ["tt1000001"]]
    curated_sql = _compact(calls[1][0])
    assert "join core.show_role_catalog as src" in curated_sql
    assert "src.is_active = true" in curated_sql
    assert calls[1][1] == [PERSON_ID]
