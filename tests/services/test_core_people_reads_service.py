from __future__ import annotations

from typing import Any

import pytest

from trr_backend.services import core_people_reads

PERSON_ID = "11111111-1111-1111-1111-111111111111"
SHOW_ID = "22222222-2222-2222-2222-222222222222"


def test_service_delegates_people_and_relationship_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def record(name: str, result: object):
        def inner(*args: Any, **kwargs: Any) -> object:
            calls.append((name, args, kwargs))
            return result

        return inner

    monkeypatch.setattr(
        core_people_reads.repository,
        "search_people",
        record("search_people", ([{"id": PERSON_ID}], 1)),
    )
    monkeypatch.setattr(
        core_people_reads.repository,
        "get_person_by_id",
        record("get_person_by_id", ({"id": PERSON_ID}, 1)),
    )
    monkeypatch.setattr(
        core_people_reads.repository,
        "get_deduced_family_relationships_by_person_id",
        record("get_deduced_family_relationships_by_person_id", ({"Parent Person": "Mom"}, 4)),
    )

    assert core_people_reads.search_people("Lisa", limit=5, offset=2) == ([{"id": PERSON_ID}], 1)
    assert core_people_reads.get_person_by_id(PERSON_ID) == ({"id": PERSON_ID}, 1)
    assert core_people_reads.get_deduced_family_relationships_by_person_id(
        PERSON_ID,
        show_id=SHOW_ID,
    ) == ({"Parent Person": "Mom"}, 4)
    assert calls == [
        ("search_people", ("Lisa",), {"limit": 5, "offset": 2}),
        ("get_person_by_id", (PERSON_ID,), {}),
        (
            "get_deduced_family_relationships_by_person_id",
            (PERSON_ID,),
            {"show_id": SHOW_ID},
        ),
    ]
