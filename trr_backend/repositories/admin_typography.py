"""Backend-owned persistence for the authenticated typography administration API."""

from __future__ import annotations

import base64
import gzip
import json
from typing import Any, Literal

from trr_backend.db import pg

TypographyArea = Literal["user-frontend", "surveys", "admin"]
TypographyState = dict[str, list[dict[str, Any]]]

_SET_FIELDS = "id::text AS id, slug, name, area, seed_source, roles, created_at, updated_at"
_ASSIGNMENT_FIELDS = (
    "id::text AS id, area, page_key, instance_key, set_id::text AS set_id, source_path, notes, created_at, updated_at"
)

# This is the canonical seeded state from TRR-APP's typography-seed module at
# this cutover. It is compressed only to keep the backend repository readable;
# `seed_typography_if_missing` materializes the same rows on the first write.
_SEEDED_TYPOGRAPHY_DATA_GZIP_B64 = (
    "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"
)


def _seed_data() -> dict[str, list[dict[str, Any]]]:
    return json.loads(gzip.decompress(base64.b64decode(_SEEDED_TYPOGRAPHY_DATA_GZIP_B64)))


def _roles(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return decoded
    return {}


def _map_set(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "slug": str(row["slug"]),
        "name": str(row["name"]),
        "area": str(row["area"]),
        "seed_source": str(row["seed_source"]),
        "roles": _roles(row.get("roles")),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _map_assignment(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "area": str(row["area"]),
        "page_key": row.get("page_key"),
        "instance_key": row.get("instance_key"),
        "set_id": str(row["set_id"]),
        "source_path": str(row["source_path"]),
        "notes": row.get("notes"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def build_seeded_typography_state() -> TypographyState:
    """Return the non-persisted state used before the migration is available."""
    seed = _seed_data()
    sets: list[dict[str, Any]] = []
    set_ids_by_slug: dict[str, str] = {}
    for index, item in enumerate(seed["sets"], start=1):
        set_id = f"seed-set-{index}"
        set_ids_by_slug[str(item["slug"])] = set_id
        sets.append(
            {
                "id": set_id,
                "slug": item["slug"],
                "name": item["name"],
                "area": item["area"],
                "seed_source": item["seedSource"],
                "roles": item["roles"],
                "created_at": "",
                "updated_at": "",
            }
        )
    assignments = [
        {
            "id": f"seed-assignment-{index}",
            "area": item["area"],
            "page_key": item["pageKey"],
            "instance_key": item["instanceKey"],
            "set_id": set_ids_by_slug.get(str(item["setSlug"]), ""),
            "source_path": item["sourcePath"],
            "notes": item["notes"],
            "created_at": "",
            "updated_at": "",
        }
        for index, item in enumerate(seed["assignments"], start=1)
    ]
    return {"sets": sets, "assignments": assignments}


def _is_missing_typography_table_error(error: Exception) -> bool:
    error_code = str(getattr(error, "pgcode", None) or getattr(error, "code", None) or "")
    return error_code == "42P01" and "site_typography_" in str(error).lower()


def read_typography_state() -> tuple[TypographyState, int]:
    """Read persisted state without running schema or seed writes on GET."""
    try:
        sets = pg.fetch_all(
            f"""
            SELECT {_SET_FIELDS}
            FROM public.site_typography_sets
            ORDER BY area ASC, name ASC
            """
        )
        assignments = pg.fetch_all(
            f"""
            SELECT {_ASSIGNMENT_FIELDS}
            FROM public.site_typography_assignments
            ORDER BY area ASC, page_key ASC NULLS FIRST, instance_key ASC NULLS FIRST, source_path ASC
            """
        )
    except Exception as error:
        if _is_missing_typography_table_error(error):
            return build_seeded_typography_state(), 2
        raise

    if not sets and not assignments:
        return build_seeded_typography_state(), 2
    return {
        "sets": [_map_set(row) for row in sets],
        "assignments": [_map_assignment(row) for row in assignments],
    }, 2


def seed_typography_if_missing() -> int:
    """Insert missing baseline rows for an authenticated write, never for GET."""
    seed = _seed_data()
    query_count = 0
    for item in seed["sets"]:
        pg.execute(
            """
            INSERT INTO public.site_typography_sets (slug, name, area, seed_source, roles)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (slug) DO NOTHING
            """,
            [item["slug"], item["name"], item["area"], item["seedSource"], json.dumps(item["roles"])],
        )
        query_count += 1

    set_rows = pg.fetch_all("SELECT id::text AS id, slug FROM public.site_typography_sets")
    query_count += 1
    set_ids_by_slug = {str(row["slug"]): str(row["id"]) for row in set_rows}
    for item in seed["assignments"]:
        set_id = set_ids_by_slug.get(str(item["setSlug"]))
        if not set_id:
            continue
        pg.execute(
            """
            INSERT INTO public.site_typography_assignments
                (area, page_key, instance_key, set_id, source_path, notes)
            VALUES (%s, %s, %s, %s::uuid, %s, %s)
            ON CONFLICT (area, COALESCE(page_key, ''), COALESCE(instance_key, '')) DO NOTHING
            """,
            [item["area"], item["pageKey"], item["instanceKey"], set_id, item["sourcePath"], item["notes"]],
        )
        query_count += 1
    return query_count


def _seeded_set_id_to_slug() -> dict[str, str]:
    return {f"seed-set-{index}": str(item["slug"]) for index, item in enumerate(_seed_data()["sets"], start=1)}


def resolve_typography_set_id(set_id: str) -> tuple[str, int]:
    query_count = seed_typography_if_missing()
    seeded_slug = _seeded_set_id_to_slug().get(set_id)
    if not seeded_slug:
        return set_id, query_count
    row = pg.fetch_one(
        "SELECT id::text AS id FROM public.site_typography_sets WHERE slug = %s LIMIT 1",
        [seeded_slug],
    )
    return str(row["id"]) if row else set_id, query_count + 1


def _slugify(value: str) -> str:
    import re

    return re.sub(r"(^-|-$)", "", re.sub(r"[^a-z0-9]+", "-", value.strip().lower()))


def create_typography_set(
    *,
    name: str,
    area: TypographyArea,
    seed_source: str,
    roles: dict[str, Any],
    slug: str | None = None,
) -> tuple[dict[str, Any], int]:
    query_count = seed_typography_if_missing()
    resolved_slug = _slugify(slug or name)
    rows = pg.execute_returning(
        f"""
        INSERT INTO public.site_typography_sets (slug, name, area, seed_source, roles)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        RETURNING {_SET_FIELDS}
        """,
        [resolved_slug, name.strip(), area, seed_source.strip(), json.dumps(roles)],
    )
    return _map_set(rows[0]), query_count + 1


def update_typography_set(
    set_id: str,
    *,
    name: str | None = None,
    area: TypographyArea | None = None,
    seed_source: str | None = None,
    roles: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, int]:
    resolved_set_id, query_count = resolve_typography_set_id(set_id)
    updates: list[str] = []
    values: list[Any] = []
    if name is not None:
        updates.append("name = %s")
        values.append(name.strip())
    if area is not None:
        updates.append("area = %s")
        values.append(area)
    if seed_source is not None:
        updates.append("seed_source = %s")
        values.append(seed_source.strip())
    if roles is not None:
        updates.append("roles = %s::jsonb")
        values.append(json.dumps(roles))

    if not updates:
        state, state_queries = read_typography_state()
        typography_set = next(
            (item for item in state["sets"] if item["id"] in {set_id, resolved_set_id}),
            None,
        )
        return typography_set, query_count + state_queries

    values.append(resolved_set_id)
    rows = pg.execute_returning(
        f"""
        UPDATE public.site_typography_sets
        SET {", ".join(updates)}
        WHERE id = %s::uuid
        RETURNING {_SET_FIELDS}
        """,
        values,
    )
    return (_map_set(rows[0]) if rows else None), query_count + 1


def delete_typography_set(set_id: str) -> tuple[Literal["deleted", "in-use", "missing"], int]:
    resolved_set_id, query_count = resolve_typography_set_id(set_id)
    count_row = pg.fetch_one(
        "SELECT count(*)::text AS count FROM public.site_typography_assignments WHERE set_id = %s::uuid",
        [resolved_set_id],
    )
    query_count += 1
    if int(str((count_row or {}).get("count", "0"))) > 0:
        return "in-use", query_count
    deleted = pg.execute_returning(
        "DELETE FROM public.site_typography_sets WHERE id = %s::uuid RETURNING id::text AS id",
        [resolved_set_id],
    )
    return ("deleted" if deleted else "missing"), query_count + 1


def upsert_typography_assignment(
    *,
    area: TypographyArea,
    page_key: str | None,
    instance_key: str | None,
    set_id: str,
    source_path: str,
    notes: str | None,
) -> tuple[dict[str, Any], int]:
    resolved_set_id, query_count = resolve_typography_set_id(set_id)
    existing = pg.fetch_one(
        f"""
        SELECT {_ASSIGNMENT_FIELDS}
        FROM public.site_typography_assignments
        WHERE area = %s
          AND COALESCE(page_key, '') = COALESCE(%s, '')
          AND COALESCE(instance_key, '') = COALESCE(%s, '')
        """,
        [area, page_key, instance_key],
    )
    query_count += 1
    if existing:
        rows = pg.execute_returning(
            f"""
            UPDATE public.site_typography_assignments
            SET set_id = %s::uuid, source_path = %s, notes = %s
            WHERE id = %s::uuid
            RETURNING {_ASSIGNMENT_FIELDS}
            """,
            [resolved_set_id, source_path.strip(), notes, existing["id"]],
        )
        return _map_assignment(rows[0]), query_count + 1
    rows = pg.execute_returning(
        f"""
        INSERT INTO public.site_typography_assignments
            (area, page_key, instance_key, set_id, source_path, notes)
        VALUES (%s, %s, %s, %s::uuid, %s, %s)
        RETURNING {_ASSIGNMENT_FIELDS}
        """,
        [area, page_key, instance_key, resolved_set_id, source_path.strip(), notes],
    )
    return _map_assignment(rows[0]), query_count + 1
