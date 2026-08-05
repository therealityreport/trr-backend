from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from trr_backend.repositories import external_id_reads

PERSON_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PERSON_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
SHOW_A = "11111111-1111-1111-1111-111111111111"
SHOW_B = "22222222-2222-2222-2222-222222222222"


def _person_row(person_id: str, source_id: str, row_id: int) -> dict[str, object]:
    return {
        "person_id": person_id,
        "ordinality": 1,
        "person_exists": True,
        "id": row_id,
        "source_id": source_id,
        "external_id": f"{source_id}-value",
        "is_primary": True,
        "valid_from": date(2026, 1, 1),
        "valid_to": None,
        "observed_at": datetime(2026, 1, 2, tzinfo=UTC),
        "created_at": datetime(2026, 1, 3, tzinfo=UTC),
        "updated_at": datetime(2026, 1, 4, tzinfo=UTC),
    }


def test_get_person_external_ids_is_one_ordered_query_and_reuses_record_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            _person_row(PERSON_A, "imdb", 1),
            _person_row(PERSON_A, "instagram", 2),
        ]

    monkeypatch.setattr(external_id_reads.pg, "fetch_all", fake_fetch_all)

    records, query_count = external_id_reads.get_person_external_ids(
        PERSON_A,
        include_inactive=True,
    )

    assert query_count == 1
    assert len(calls) == 1
    assert "ORDER BY pei.source_id ASC NULLS LAST" in calls[0][0]
    assert calls[0][1] == [True, PERSON_A]
    assert records is not None
    assert [record["source_id"] for record in records] == ["imdb", "instagram"]
    assert records[0]["valid_from"] == "2026-01-01"
    assert records[0]["observed_at"] == "2026-01-02T00:00:00+00:00"


def test_get_person_external_ids_distinguishes_missing_person_from_no_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(external_id_reads.pg, "fetch_all", lambda *_args, **_kwargs: [])
    missing, query_count = external_id_reads.get_person_external_ids(PERSON_A)
    assert missing is None
    assert query_count == 1

    monkeypatch.setattr(
        external_id_reads.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "person_id": PERSON_A,
                "id": None,
                "source_id": None,
                "external_id": None,
                "is_primary": None,
                "valid_from": None,
                "valid_to": None,
                "observed_at": None,
                "created_at": None,
                "updated_at": None,
            }
        ],
    )
    empty, query_count = external_id_reads.get_person_external_ids(PERSON_A)
    assert empty == []
    assert query_count == 1


def test_person_batch_is_one_query_and_preserves_first_seen_person_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {**_person_row(PERSON_B, "imdb", 3), "ordinality": 1},
            {**_person_row(PERSON_A, "imdb", 4), "ordinality": 2},
            {**_person_row(PERSON_A, "instagram", 5), "ordinality": 2},
        ]

    monkeypatch.setattr(external_id_reads.pg, "fetch_all", fake_fetch_all)

    people, query_count = external_id_reads.list_person_external_ids_by_person_ids(
        [PERSON_B, PERSON_A, PERSON_B],
        include_inactive=False,
    )

    assert query_count == 1
    assert len(calls) == 1
    assert "WITH ORDINALITY" in calls[0][0]
    assert "ORDER BY requested.ordinality ASC" in calls[0][0]
    assert calls[0][1] == [[PERSON_B, PERSON_A], False]
    assert [person["person_id"] for person in people] == [PERSON_B, PERSON_A]
    assert [record["source_id"] for record in people[1]["external_ids"]] == ["imdb", "instagram"]


def test_show_batch_is_one_query_and_omits_missing_rows_without_reordering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        calls.append((sql, params))
        return [
            {
                "show_id": SHOW_B,
                "ordinality": 1,
                "show_exists": True,
                "external_ids": {"imdb": "tt222"},
            },
            {
                "show_id": SHOW_A,
                "ordinality": 2,
                "show_exists": True,
                "external_ids": None,
            },
        ]

    monkeypatch.setattr(external_id_reads.pg, "fetch_all", fake_fetch_all)

    shows, query_count = external_id_reads.list_show_external_ids_by_show_ids([SHOW_B, SHOW_A, SHOW_B])

    assert query_count == 1
    assert len(calls) == 1
    assert "WITH ORDINALITY" in calls[0][0]
    assert calls[0][1] == [[SHOW_B, SHOW_A]]
    assert shows == [
        {"show_id": SHOW_B, "external_ids": {"imdb": "tt222"}},
        {"show_id": SHOW_A, "external_ids": None},
    ]
