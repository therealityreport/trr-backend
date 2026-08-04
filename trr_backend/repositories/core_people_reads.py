"""Core people read queries for the strict admin API v2 adapter."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from trr_backend.db import pg

DEFAULT_LIMIT = 20
MAX_LIMIT = 500
_SPOUSE_LIKE_ROLES = frozenset(
    {
        "husband",
        "ex-husband",
        "boyfriend",
        "ex-boyfriend",
        "fiance",
        "ex-fiance",
    }
)


def normalize_pagination(limit: int | None = None, offset: int | None = None) -> tuple[int, int]:
    normalized_limit = min(max(limit if limit is not None else DEFAULT_LIMIT, 1), MAX_LIMIT)
    normalized_offset = max(offset if offset is not None else 0, 0)
    return normalized_limit, normalized_offset


def search_people(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        """
        SELECT id, full_name, known_for, external_ids, created_at, updated_at
          FROM core.people
         WHERE full_name ILIKE %s
         ORDER BY full_name ASC
         LIMIT %s OFFSET %s
        """,
        [f"{query}%", normalized_limit, normalized_offset],
    )
    return rows, 1


def get_person_by_id(person_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT *
          FROM core.people
         WHERE id = %s::uuid
         LIMIT 1
        """,
        [person_id],
    )
    return row, 1


def _normalize_gender_token(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = re.sub(r"\(.*?\)", " ", value)
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(character for character in normalized if not unicodedata.combining(character))
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-zA-Z0-9]+", " ", normalized).strip().lower()
    if not normalized:
        return None
    if "female" in normalized or normalized == "f":
        return "female"
    if "male" in normalized or normalized == "m":
        return "male"
    return None


def _parent_relation_label(*, gender: object, has_spouse_like_role: bool, parent_count: int) -> str:
    normalized_gender = _normalize_gender_token(gender)
    if normalized_gender == "female":
        return "Mom"
    if normalized_gender == "male":
        return "Dad"
    if has_spouse_like_role:
        return "Dad"
    if parent_count > 1:
        return "Mom"
    return "Parent"


def _sibling_relation_label(gender: object) -> str:
    normalized_gender = _normalize_gender_token(gender)
    if normalized_gender == "female":
        return "Sister"
    if normalized_gender == "male":
        return "Brother"
    return "Sibling"


def get_deduced_family_relationships_by_person_id(
    person_id: str,
    show_id: str | None = None,
) -> tuple[dict[str, str], int]:
    show_scope_clause = "AND sra.show_id = %s::uuid" if show_id else ""
    parent_name_params: list[object] = [person_id, show_id] if show_id else [person_id]
    parent_name_rows = pg.fetch_all(
        f"""
        SELECT DISTINCT
               NULLIF(BTRIM(COALESCE(sra.metadata->>'relationship_from', '')), '') AS relationship_from
          FROM core.show_cast_role_assignments AS sra
          JOIN core.show_role_catalog AS rc ON rc.id = sra.role_id
         WHERE sra.person_id = %s::uuid
           {show_scope_clause}
           AND LOWER(rc.name) = 'kid'
        """,
        parent_name_params,
    )
    query_count = 1
    parent_names = [name for row in parent_name_rows if (name := str(row.get("relationship_from") or "").strip())]
    if not parent_names:
        return {}, query_count

    deduped_parent_names = list(dict.fromkeys(parent_names))
    if show_id:
        parent_people_rows = pg.fetch_all(
            """
            SELECT DISTINCT
                   p.id::text AS person_id,
                   p.full_name,
                   cf.gender AS fandom_gender
              FROM UNNEST(%s::text[]) AS rel(name)
              JOIN core.v_show_cast AS sc
                ON sc.show_id = %s::uuid
              JOIN core.people AS p
                ON p.id = sc.person_id
               AND LOWER(p.full_name) = LOWER(rel.name)
              LEFT JOIN core.cast_fandom AS cf
                ON cf.person_id = p.id
               AND cf.source = 'fandom'
            """,
            [deduped_parent_names, show_id],
        )
    else:
        parent_people_rows = pg.fetch_all(
            """
            SELECT DISTINCT
                   p.id::text AS person_id,
                   p.full_name,
                   cf.gender AS fandom_gender
              FROM UNNEST(%s::text[]) AS rel(name)
              JOIN core.people AS p
                ON LOWER(p.full_name) = LOWER(rel.name)
              LEFT JOIN core.cast_fandom AS cf
                ON cf.person_id = p.id
               AND cf.source = 'fandom'
            """,
            [deduped_parent_names],
        )
    query_count += 1
    parents = [row for row in parent_people_rows if row.get("full_name")]
    if not parents:
        return {}, query_count

    parent_ids = [str(row.get("person_id") or "") for row in parents]
    parent_role_scope = "AND sra.show_id = %s::uuid" if show_id else ""
    parent_role_params: list[object] = [parent_ids, show_id] if show_id else [parent_ids]
    parent_role_rows = pg.fetch_all(
        f"""
        SELECT sra.person_id::text AS person_id,
               LOWER(rc.name) AS role_name
          FROM core.show_cast_role_assignments AS sra
          JOIN core.show_role_catalog AS rc ON rc.id = sra.role_id
         WHERE sra.person_id = ANY(%s::uuid[])
           {parent_role_scope}
        """,
        parent_role_params,
    )
    query_count += 1
    roles_by_parent: dict[str, set[str]] = {}
    for row in parent_role_rows:
        row_person_id = str(row.get("person_id") or "")
        role_name = str(row.get("role_name") or "")
        if not row_person_id or not role_name:
            continue
        roles_by_parent.setdefault(row_person_id, set()).add(role_name)

    family: dict[str, str] = {}
    for parent in parents:
        parent_name = str(parent.get("full_name") or "").strip()
        if not parent_name or parent_name in family:
            continue
        parent_roles = roles_by_parent.get(str(parent.get("person_id") or ""), set())
        family[parent_name] = _parent_relation_label(
            gender=parent.get("fandom_gender"),
            has_spouse_like_role=bool(parent_roles.intersection(_SPOUSE_LIKE_ROLES)),
            parent_count=len(parents),
        )

    sibling_scope = "AND sra.show_id = %s::uuid" if show_id else ""
    sibling_params: list[object] = (
        [person_id, deduped_parent_names, show_id] if show_id else [person_id, deduped_parent_names]
    )
    sibling_rows = pg.fetch_all(
        f"""
        SELECT DISTINCT
               p.id::text AS person_id,
               p.full_name,
               cf.gender AS fandom_gender
          FROM core.show_cast_role_assignments AS sra
          JOIN core.show_role_catalog AS rc ON rc.id = sra.role_id
          JOIN core.people AS p ON p.id = sra.person_id
          LEFT JOIN core.cast_fandom AS cf
            ON cf.person_id = p.id
           AND cf.source = 'fandom'
         WHERE sra.person_id <> %s::uuid
           AND LOWER(rc.name) = 'kid'
           AND NULLIF(BTRIM(COALESCE(sra.metadata->>'relationship_from', '')), '') = ANY(%s::text[])
           {sibling_scope}
        """,
        sibling_params,
    )
    query_count += 1
    for sibling in sibling_rows:
        sibling_name = str(sibling.get("full_name") or "").strip()
        if not sibling_name or sibling_name in family:
            continue
        family[sibling_name] = _sibling_relation_label(sibling.get("fandom_gender"))

    return family, query_count
