"""Bounded admin reads for canonical show and person external IDs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import person_external_ids


def _unique_ids(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _map_person_records(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return person_external_ids.map_primary_person_external_id_rows(rows)


def get_person_external_ids(
    person_id: str,
    *,
    include_inactive: bool = False,
) -> tuple[list[dict[str, Any]] | None, int]:
    rows = pg.fetch_all(
        """
        SELECT
          p.id::text AS person_id,
          pei.id,
          pei.source_id,
          pei.external_id,
          pei.is_primary,
          pei.valid_from,
          pei.valid_to,
          pei.observed_at,
          pei.created_at,
          pei.updated_at
        FROM core.people AS p
        LEFT JOIN core.person_external_ids AS pei
          ON pei.person_id = p.id
         AND pei.is_primary = true
         AND (%s::boolean OR pei.valid_to IS NULL)
        WHERE p.id = %s::uuid
        ORDER BY pei.source_id ASC NULLS LAST
        """,
        [include_inactive, person_id],
    )
    if not rows:
        return None, 1
    return _map_person_records(rows), 1


def list_person_external_ids_by_person_ids(
    person_ids: Sequence[str],
    *,
    include_inactive: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    unique_person_ids = _unique_ids(person_ids)
    if not unique_person_ids:
        return [], 0

    rows = pg.fetch_all(
        """
        WITH requested AS (
          SELECT requested_id, ordinality
          FROM unnest(%s::uuid[]) WITH ORDINALITY AS input(requested_id, ordinality)
        )
        SELECT
          requested.requested_id::text AS person_id,
          requested.ordinality,
          (p.id IS NOT NULL) AS person_exists,
          pei.id,
          pei.source_id,
          pei.external_id,
          pei.is_primary,
          pei.valid_from,
          pei.valid_to,
          pei.observed_at,
          pei.created_at,
          pei.updated_at
        FROM requested
        LEFT JOIN core.people AS p
          ON p.id = requested.requested_id
        LEFT JOIN core.person_external_ids AS pei
          ON pei.person_id = p.id
         AND pei.is_primary = true
         AND (%s::boolean OR pei.valid_to IS NULL)
        ORDER BY requested.ordinality ASC, pei.source_id ASC NULLS LAST
        """,
        [unique_person_ids, include_inactive],
    )

    records_by_person_id: dict[str, list[Mapping[str, Any]]] = {}
    existing_person_ids: set[str] = set()
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id or not bool(row.get("person_exists")):
            continue
        existing_person_ids.add(person_id)
        records_by_person_id.setdefault(person_id, []).append(row)

    people: list[dict[str, Any]] = []
    for person_id in unique_person_ids:
        if person_id not in existing_person_ids:
            continue
        people.append(
            {
                "person_id": person_id,
                "external_ids": _map_person_records(records_by_person_id.get(person_id, [])),
            }
        )
    return people, 1


def list_show_external_ids_by_show_ids(
    show_ids: Sequence[str],
) -> tuple[list[dict[str, Any]], int]:
    unique_show_ids = _unique_ids(show_ids)
    if not unique_show_ids:
        return [], 0

    rows = pg.fetch_all(
        """
        WITH requested AS (
          SELECT requested_id, ordinality
          FROM unnest(%s::uuid[]) WITH ORDINALITY AS input(requested_id, ordinality)
        )
        SELECT
          requested.requested_id::text AS show_id,
          requested.ordinality,
          (s.id IS NOT NULL) AS show_exists,
          s.external_ids
        FROM requested
        LEFT JOIN core.shows AS s
          ON s.id = requested.requested_id
        ORDER BY requested.ordinality ASC
        """,
        [unique_show_ids],
    )

    shows: list[dict[str, Any]] = []
    for row in rows:
        show_id = str(row.get("show_id") or "").strip()
        if not show_id or not bool(row.get("show_exists")):
            continue
        external_ids = row.get("external_ids")
        shows.append(
            {
                "show_id": show_id,
                "external_ids": dict(external_ids) if isinstance(external_ids, Mapping) else None,
            }
        )
    return shows, 1
