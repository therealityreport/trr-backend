"""Repository functions for media-link people tag writes."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg


class MediaLinkTagsNotFoundError(RuntimeError):
    """Raised when the requested media link row does not exist."""


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _clamp01(value: float) -> float:
    return min(1, max(0, value))


def _normalize_people(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    by_key: dict[str, dict[str, str]] = {}
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        person_id = str(entry.get("id") or "").strip() if isinstance(entry.get("id"), str) else ""
        name = str(entry.get("name") or "").strip() if isinstance(entry.get("name"), str) else ""
        if not name:
            continue
        key = f"id:{person_id}" if person_id else f"name:{name.lower()}"
        if key not in by_key:
            row = {"name": name}
            if person_id:
                row["id"] = person_id
            by_key[key] = row
    return list(by_key.values())


def _unique_names(people: list[dict[str, str]]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for person in people:
        name = person.get("name", "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def _parse_count(value: Any) -> int | None:
    if not _is_number(value):
        return None
    return max(0, math.floor(value))


def _parse_count_source(value: Any) -> str | None:
    return value if value in {"auto", "manual"} else None


def _normalize_face_boxes(raw: Any) -> list[dict[str, Any]] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        return []

    out: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        x = _clamp01(float(entry["x"])) if _is_number(entry.get("x")) else None
        y = _clamp01(float(entry["y"])) if _is_number(entry.get("y")) else None
        width = _clamp01(float(entry["width"])) if _is_number(entry.get("width")) else None
        height = _clamp01(float(entry["height"])) if _is_number(entry.get("height")) else None
        if x is None or y is None or width is None or height is None or width <= 0 or height <= 0:
            continue

        index = max(1, math.floor(float(entry["index"]))) if _is_number(entry.get("index")) else len(out) + 1
        confidence = _clamp01(float(entry["confidence"])) if _is_number(entry.get("confidence")) else None
        box: dict[str, Any] = {
            "index": index,
            "kind": "face",
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "confidence": confidence,
        }
        for source_key in ("person_id", "person_name", "label"):
            value = entry.get(source_key)
            if isinstance(value, str) and value.strip():
                box[source_key] = value.strip()
        out.append(box)
    return out


def _context_base(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _tag_context(
    base_context: Mapping[str, Any],
    *,
    people_ids: list[str],
    people_names: list[str],
    people_count: int | None,
    people_count_source: str | None,
    has_face_boxes: bool,
    face_boxes: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    next_context = {
        **dict(base_context),
        "people_ids": people_ids,
        "people_names": people_names,
        "people_count": people_count,
        "people_count_source": people_count_source,
    }
    if has_face_boxes:
        next_context["face_boxes"] = face_boxes
    return next_context


def _fetch_media_link(cur: Any, link_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT
          id::text,
          entity_type,
          entity_id::text,
          media_asset_id::text,
          kind,
          position,
          context,
          created_at
        FROM core.media_links
        WHERE id = %s::uuid
        LIMIT 1
        """,
        [link_id],
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _list_person_gallery_links(cur: Any, media_asset_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          id::text,
          entity_type,
          entity_id::text,
          media_asset_id::text,
          kind,
          position,
          context,
          created_at
        FROM core.media_links
        WHERE media_asset_id = %s::uuid
          AND entity_type = 'person'
          AND kind = 'gallery'
        """,
        [media_asset_id],
    )
    return [dict(row) for row in cur.fetchall()]


def _ensure_media_links_for_people(
    cur: Any,
    *,
    media_asset_id: str,
    people: list[dict[str, str]],
    base_context: Mapping[str, Any],
) -> None:
    people_ids = [person["id"] for person in people if person.get("id")]
    if not people_ids:
        return

    existing = _list_person_gallery_links(cur, media_asset_id)
    existing_ids = {str(link.get("entity_id") or "") for link in existing if link.get("entity_id")}
    missing_ids = [person_id for person_id in people_ids if person_id not in existing_ids]
    for person_id in missing_ids:
        cur.execute(
            """
            INSERT INTO core.media_links (
              entity_type,
              entity_id,
              media_asset_id,
              kind,
              position,
              context
            )
            VALUES ('person', %s::uuid, %s::uuid, 'gallery', NULL, %s::jsonb)
            ON CONFLICT (entity_type, entity_id, kind, media_asset_id) DO NOTHING
            """,
            [person_id, media_asset_id, Json(dict(base_context))],
        )


def sync_media_link_tags(link_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    people = _normalize_people(payload.get("people"))
    has_face_boxes = "face_boxes" in payload
    face_boxes = _normalize_face_boxes(payload.get("face_boxes")) if has_face_boxes else None

    with pg.db_connection(label="sync-media-link-tags") as conn:
        with pg.db_cursor(conn=conn, label="sync-media-link-tags") as cur:
            link = _fetch_media_link(cur, link_id)
            if not link:
                raise MediaLinkTagsNotFoundError("Media link not found")

            people_ids = [person["id"] for person in people if person.get("id")]
            people_names = _unique_names(people)
            base_context = _context_base(link.get("context"))
            people_count = _parse_count(base_context.get("people_count"))
            people_count_source = _parse_count_source(base_context.get("people_count_source"))

            if "people_count" in payload:
                people_count = _parse_count(payload.get("people_count"))
                people_count_source = "manual" if people_count is not None else None
            elif people_names or people_ids:
                people_count_source = "manual"

            merged_context = _tag_context(
                base_context,
                people_ids=people_ids,
                people_names=people_names,
                people_count=people_count,
                people_count_source=people_count_source,
                has_face_boxes=has_face_boxes,
                face_boxes=face_boxes,
            )

            media_asset_id = str(link["media_asset_id"])
            _ensure_media_links_for_people(
                cur,
                media_asset_id=media_asset_id,
                people=people,
                base_context=merged_context,
            )
            links_for_asset = _list_person_gallery_links(cur, media_asset_id)
            target_entity_ids = set(people_ids)
            original_entity_id = str(link.get("entity_id") or "").strip()
            if original_entity_id:
                target_entity_ids.add(original_entity_id)

            candidates_by_id: dict[str, dict[str, Any]] = {}
            for candidate in links_for_asset:
                candidate_id = str(candidate.get("id") or "")
                candidate_entity_id = str(candidate.get("entity_id") or "")
                if candidate_id == link_id or candidate_entity_id in target_entity_ids:
                    candidates_by_id[candidate_id] = candidate
            if link_id not in candidates_by_id:
                candidates_by_id[link_id] = link

            for candidate_id, candidate in candidates_by_id.items():
                next_context = _tag_context(
                    _context_base(candidate.get("context")),
                    people_ids=people_ids,
                    people_names=people_names,
                    people_count=people_count,
                    people_count_source=people_count_source,
                    has_face_boxes=has_face_boxes,
                    face_boxes=face_boxes,
                )
                cur.execute(
                    """
                    UPDATE core.media_links
                    SET context = %s::jsonb,
                        updated_at = NOW()
                    WHERE id = %s::uuid
                    """,
                    [Json(next_context), candidate_id],
                )

    return {
        "people_names": people_names,
        "people_ids": people_ids,
        "people_count": people_count,
        "people_count_source": people_count_source,
        "face_boxes": face_boxes if has_face_boxes else None,
    }
