from __future__ import annotations

from typing import Any, Literal

from trr_backend.db import pg

SocialPlatform = Literal["reddit", "twitter", "instagram", "tiktok", "youtube", "other"]

_TABLE = "admin.show_social_posts"
_VALID_PLATFORMS = {"reddit", "twitter", "instagram", "tiktok", "youtube", "other"}


def _to_string(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _normalize_platform(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in _VALID_PLATFORMS:
        raise ValueError("platform must be one of: reddit, twitter, instagram, tiktok, youtube, other")
    return normalized


def _map_post(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _to_string(row.get("id")),
        "trr_show_id": _to_string(row.get("trr_show_id")),
        "trr_season_id": _to_string(row.get("trr_season_id")),
        "platform": _normalize_platform(row.get("platform")),
        "url": _to_string(row.get("url")) or "",
        "title": _to_string(row.get("title")),
        "notes": _to_string(row.get("notes")),
        "created_by_firebase_uid": _to_string(row.get("created_by_firebase_uid")) or "",
        "created_at": _to_string(row.get("created_at")) or "",
        "updated_at": _to_string(row.get("updated_at")) or "",
    }


def get_season_show_id(trr_season_id: str) -> tuple[str | None, int]:
    row = pg.fetch_one("select show_id::text as show_id from core.seasons where id = %s::uuid limit 1", [trr_season_id])
    return _to_string((row or {}).get("show_id")), 1


def list_posts_for_show(show_id: str, *, trr_season_id: str | None = None) -> tuple[list[dict[str, Any]], int]:
    if trr_season_id:
        rows = pg.fetch_all(
            f"""
            select *
            from {_TABLE}
            where trr_show_id = %s::uuid
              and trr_season_id = %s::uuid
            order by created_at desc
            """,
            [show_id, trr_season_id],
        )
    else:
        rows = pg.fetch_all(
            f"""
            select *
            from {_TABLE}
            where trr_show_id = %s::uuid
            order by created_at desc
            """,
            [show_id],
        )
    return [_map_post(row) for row in rows], 1


def get_post(post_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(f"select * from {_TABLE} where id = %s::uuid limit 1", [post_id])
    if row is None:
        return None, 1
    return _map_post(row), 1


def create_post(
    *,
    trr_show_id: str,
    trr_season_id: str | None,
    platform: str,
    url: str,
    title: str | None,
    notes: str | None,
    actor_uid: str,
) -> tuple[dict[str, Any], int]:
    rows = pg.execute_returning(
        f"""
        insert into {_TABLE} (
          trr_show_id,
          trr_season_id,
          platform,
          url,
          title,
          notes,
          created_by_firebase_uid
        ) values (%s::uuid, %s::uuid, %s, %s, %s, %s, %s)
        returning *
        """,
        [
            trr_show_id,
            trr_season_id,
            _normalize_platform(platform),
            url,
            title,
            notes,
            actor_uid,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to create social post")
    return _map_post(rows[0]), 1


def update_post(
    *,
    post_id: str,
    trr_season_id: Any = None,
    platform: Any = None,
    url: Any = None,
    title: Any = None,
    notes: Any = None,
) -> tuple[dict[str, Any] | None, int]:
    updates: list[str] = []
    params: list[Any] = []

    def _push(value: Any) -> str:
        params.append(value)
        return "%s"

    if trr_season_id is not None:
        updates.append(f"trr_season_id = {_push(trr_season_id)}::uuid")
    if platform is not None:
        updates.append(f"platform = {_push(_normalize_platform(platform))}")
    if url is not None:
        updates.append(f"url = {_push(url)}")
    if title is not None:
        updates.append(f"title = {_push(title)}")
    if notes is not None:
        updates.append(f"notes = {_push(notes)}")

    if not updates:
        return get_post(post_id)

    params.append(post_id)
    rows = pg.execute_returning(
        f"""
        update {_TABLE}
        set {", ".join(updates)},
            updated_at = now()
        where id = %s::uuid
        returning *
        """,
        params,
    )
    if not rows:
        return None, 1
    return _map_post(rows[0]), 1


def delete_post(post_id: str) -> tuple[bool, int]:
    rows = pg.execute_returning(f"delete from {_TABLE} where id = %s::uuid returning id", [post_id])
    return bool(rows), 1
