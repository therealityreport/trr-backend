from __future__ import annotations

from typing import Any

from trr_backend.db import pg

_TABLE = "admin.recent_people_views"
_DEFAULT_RECENT_LIMIT = 20
_MAX_RECENT_LIMIT = 50


def _to_string(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _normalize_limit(value: int | None) -> int:
    parsed = value if isinstance(value, int) else _DEFAULT_RECENT_LIMIT
    return min(max(int(parsed), 1), _MAX_RECENT_LIMIT)


def _normalize_show_context(value: str | None) -> str | None:
    normalized = _to_string(value)
    return normalized if normalized else None


def _is_missing_recent_people_views_table_error(error: Exception) -> bool:
    message = str(error).lower()
    return "admin.recent_people_views" in message and "does not exist" in message


def list_recent_people(firebase_uid: str, *, limit: int | None = None) -> tuple[list[dict[str, Any]], int]:
    normalized_limit = _normalize_limit(limit)
    try:
        rows = pg.fetch_all(
            f"""
            select
              rv.person_id::text as person_id,
              p.full_name,
              p.known_for,
              coalesce(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) as photo_url,
              rv.show_context,
              rv.view_count,
              rv.first_viewed_at,
              rv.last_viewed_at
            from {_TABLE} as rv
            join core.people as p
              on p.id = rv.person_id
            left join lateral (
              select
                cp.thumb_url,
                cp.display_url,
                cp.hosted_url,
                cp.url
              from core.v_cast_photos as cp
              where cp.person_id = rv.person_id
              order by
                case
                  when lower(coalesce(cp.context_section, '')) = 'bravo_profile' then 0
                  when lower(coalesce(cp.context_section, '')) in (
                    'official season announcement',
                    'official_season_announcement'
                  ) then 1
                  else 2
                end,
                cp.gallery_index asc nulls last
              limit 1
            ) as photo on true
            where rv.firebase_uid = %s
            order by rv.last_viewed_at desc, rv.person_id asc
            limit %s
            """,
            [firebase_uid, normalized_limit],
        )
    except Exception as error:
        if _is_missing_recent_people_views_table_error(error):
            return [], 0
        raise
    return rows, 1


def record_recent_person_view(
    *,
    firebase_uid: str,
    person_id: str,
    show_context: str | None,
    cap: int | None = None,
) -> tuple[dict[str, bool], int]:
    normalized_cap = _normalize_limit(cap)
    normalized_show_context = _normalize_show_context(show_context)
    try:
        with pg.db_cursor(label="recent-people-write") as cur:
            cur.execute(
                f"""
                insert into {_TABLE} (
                  firebase_uid,
                  person_id,
                  show_context,
                  view_count,
                  first_viewed_at,
                  last_viewed_at
                )
                values (%s, %s::uuid, %s, 1, now(), now())
                on conflict (firebase_uid, person_id)
                do update set
                  show_context = coalesce(excluded.show_context, {_TABLE}.show_context),
                  view_count = {_TABLE}.view_count + 1,
                  last_viewed_at = now(),
                  updated_at = now()
                """,
                [firebase_uid, person_id, normalized_show_context],
            )
            cur.execute(
                f"""
                delete from {_TABLE} as stale
                using (
                  select person_id
                  from (
                    select
                      person_id,
                      row_number() over (
                        partition by firebase_uid
                        order by last_viewed_at desc, updated_at desc, person_id asc
                      ) as row_num
                    from {_TABLE}
                    where firebase_uid = %s
                  ) as ranked
                  where ranked.row_num > %s
                ) as overflow
                where stale.firebase_uid = %s
                  and stale.person_id = overflow.person_id
                """,
                [firebase_uid, normalized_cap, firebase_uid],
            )
    except Exception as error:
        if _is_missing_recent_people_views_table_error(error):
            return {"ok": True}, 0
        raise
    return {"ok": True}, 2
