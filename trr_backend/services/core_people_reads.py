"""Version-neutral service boundary for core people reads."""

from __future__ import annotations

from typing import Any

from trr_backend.repositories import core_people_reads as repository


def search_people(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    return repository.search_people(query, limit=limit, offset=offset)


def get_person_by_id(person_id: str) -> tuple[dict[str, Any] | None, int]:
    return repository.get_person_by_id(person_id)


def get_deduced_family_relationships_by_person_id(
    person_id: str,
    *,
    show_id: str | None = None,
) -> tuple[dict[str, str], int]:
    return repository.get_deduced_family_relationships_by_person_id(person_id, show_id=show_id)
