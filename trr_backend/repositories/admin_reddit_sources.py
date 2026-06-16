from __future__ import annotations

import json
import re
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import reddit_refresh

COMMUNITIES_TABLE = "admin.reddit_communities"
THREADS_TABLE = "admin.reddit_threads"

VALID_THREAD_SOURCE_KINDS = {"manual", "episode_discussion"}
VALID_FLAIR_CATEGORIES = {"cast", "season"}
SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _to_string(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _to_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = re.sub(r"\s+", " ", item).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(cleaned)
    return sorted(output, key=str.lower)


def _to_nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float) and value.is_integer():
        return max(0, int(value))
    return 0


def _normalize_subreddit(value: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"^https?://(?:www\.)?reddit\.com/r/", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^r/", "", cleaned, flags=re.I)
    cleaned = cleaned.strip("/")
    cleaned = re.split(r"[/?#]", cleaned, maxsplit=1)[0] or cleaned
    return cleaned


def _validate_subreddit(value: str) -> str:
    normalized = _normalize_subreddit(value)
    if not SUBREDDIT_RE.fullmatch(normalized):
        raise ValueError("subreddit must be a valid subreddit name (2-21 letters, numbers, underscore)")
    return normalized


def _normalize_subreddit_key(value: str) -> str:
    return _normalize_subreddit(value).lower()


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _strip_edge_decorators(value: str) -> str:
    next_value = value
    previous = ""
    while next_value != previous:
        previous = next_value
        next_value = re.sub(r"^[^\w]+", "", next_value)
        next_value = re.sub(r"[^\w]+$", "", next_value)
    return next_value


def _headline_case_if_all_caps(value: str) -> str:
    if not re.search(r"[A-Za-z]", value) or value != value.upper():
        return value

    def _word(match: re.Match[str]) -> str:
        word = match.group(0)
        upper = word.upper()
        if upper in {"RHOSLC", "HW"} or re.fullmatch(r"S\d+", word, re.I):
            return upper
        if len(word) <= 1:
            return upper
        return word[0].upper() + word[1:].lower()

    return re.sub(r"[A-Za-z0-9]+", _word, value)


def _normalize_reddit_flair_label(subreddit: str, raw_flair: str) -> str | None:
    if _normalize_subreddit_key(subreddit) != "realhousewivesofslc":
        normalized = _collapse_whitespace(raw_flair)
        return normalized or None
    if ":whitney:" in raw_flair.lower():
        return None
    normalized = _collapse_whitespace(raw_flair)
    normalized = re.sub(r":[^:\s]+:", " ", normalized)
    normalized = _collapse_whitespace(normalized)
    normalized = _strip_edge_decorators(normalized)
    normalized = _collapse_whitespace(normalized)
    normalized = _headline_case_if_all_caps(normalized)
    return normalized or None


def _sanitize_reddit_flair_list(subreddit: str, raw_flairs: Any) -> list[str]:
    if not isinstance(raw_flairs, list):
        return []
    seen: set[str] = set()
    output: list[str] = []
    for raw in raw_flairs:
        if not isinstance(raw, str):
            continue
        normalized = _normalize_reddit_flair_label(subreddit, raw)
        if not normalized:
            continue
        key = reddit_refresh.to_canonical_flair_key(normalized)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return sorted(output, key=str.lower)


def _sanitize_flair_categories(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    output: dict[str, str] = {}
    for key, category in value.items():
        if not isinstance(key, str) or not isinstance(category, str):
            continue
        if category in VALID_FLAIR_CATEGORIES:
            output[key] = category
    return output


def _unique_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def _sanitize_flair_assignments(value: Any) -> dict[str, dict[str, list[str]]]:
    if not isinstance(value, dict):
        return {}
    output: dict[str, dict[str, list[str]]] = {}
    for key, assignment in value.items():
        if not isinstance(key, str) or not isinstance(assignment, dict):
            continue
        flair_key = reddit_refresh.to_canonical_flair_key(key)
        if not flair_key:
            continue
        output[flair_key] = {
            "show_ids": _unique_string_list(assignment.get("show_ids")),
            "season_ids": _unique_string_list(assignment.get("season_ids")),
            "episode_ids": _unique_string_list(assignment.get("episode_ids")),
            "person_ids": _unique_string_list(assignment.get("person_ids")),
        }
    return output


def _resolve_focus_state(
    *,
    is_show_focused: bool | None,
    network_focus_targets: Any,
    franchise_focus_targets: Any,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_is_show_focused = (
        is_show_focused if isinstance(is_show_focused, bool) else bool((fallback or {}).get("is_show_focused", False))
    )
    if resolved_is_show_focused:
        return {
            "is_show_focused": True,
            "network_focus_targets": [],
            "franchise_focus_targets": [],
        }
    return {
        "is_show_focused": False,
        "network_focus_targets": _to_string_list(
            network_focus_targets
            if network_focus_targets is not None
            else (fallback or {}).get("network_focus_targets")
        ),
        "franchise_focus_targets": _to_string_list(
            franchise_focus_targets
            if franchise_focus_targets is not None
            else (fallback or {}).get("franchise_focus_targets")
        ),
    }


def _sanitize_analysis_flair_modes(
    *,
    subreddit: str,
    existing_analysis_flairs: Any,
    existing_analysis_all_flairs: Any,
    analysis_flairs: Any,
    analysis_all_flairs: Any,
) -> tuple[list[str], list[str]]:
    next_all = (
        _sanitize_reddit_flair_list(subreddit, analysis_all_flairs)
        if analysis_all_flairs is not None
        else _sanitize_reddit_flair_list(subreddit, existing_analysis_all_flairs)
    )
    next_scan_before_overlap = (
        _sanitize_reddit_flair_list(subreddit, analysis_flairs)
        if analysis_flairs is not None
        else _sanitize_reddit_flair_list(subreddit, existing_analysis_flairs)
    )
    all_keys = {reddit_refresh.to_canonical_flair_key(flair) for flair in next_all}
    next_scan = [
        flair for flair in next_scan_before_overlap if reddit_refresh.to_canonical_flair_key(flair) not in all_keys
    ]
    return next_scan, next_all


def _thread_source_kind(value: Any, *, default: str = "manual") -> str:
    normalized = str(value or default).strip().lower()
    if normalized not in VALID_THREAD_SOURCE_KINDS:
        raise ValueError("source_kind must be one of: manual, episode_discussion")
    return normalized


def get_season_show_id(trr_season_id: str) -> tuple[str | None, int]:
    row = pg.fetch_one("select show_id::text as show_id from core.seasons where id = %s::uuid limit 1", [trr_season_id])
    return _to_string((row or {}).get("show_id")), 1


def create_reddit_community(*, payload: dict[str, Any], actor_uid: str) -> tuple[dict[str, Any], int]:
    focus_state = _resolve_focus_state(
        is_show_focused=payload.get("is_show_focused"),
        network_focus_targets=payload.get("network_focus_targets"),
        franchise_focus_targets=payload.get("franchise_focus_targets"),
    )
    subreddit = _validate_subreddit(str(payload.get("subreddit") or ""))
    analysis_flairs, analysis_all_flairs = _sanitize_analysis_flair_modes(
        subreddit=subreddit,
        existing_analysis_flairs=[],
        existing_analysis_all_flairs=[],
        analysis_flairs=payload.get("analysis_flairs"),
        analysis_all_flairs=payload.get("analysis_all_flairs"),
    )
    rows = pg.execute_returning(
        f"""
        insert into {COMMUNITIES_TABLE} (
          trr_show_id,
          trr_show_name,
          subreddit,
          display_name,
          notes,
          is_active,
          is_show_focused,
          network_focus_targets,
          franchise_focus_targets,
          analysis_flairs,
          analysis_all_flairs,
          episode_title_patterns,
          created_by_firebase_uid
        ) values (
          %s::uuid, %s, %s, %s, %s, %s,
          %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s
        )
        returning *
        """,
        [
            payload["trr_show_id"],
            _to_string(payload.get("trr_show_name")) or "",
            subreddit,
            _to_string(payload.get("display_name")) or subreddit,
            _to_string(payload.get("notes")),
            payload.get("is_active") if isinstance(payload.get("is_active"), bool) else True,
            focus_state["is_show_focused"],
            _json_dumps(focus_state["network_focus_targets"]),
            _json_dumps(focus_state["franchise_focus_targets"]),
            _json_dumps(analysis_flairs),
            _json_dumps(analysis_all_flairs),
            _json_dumps(_to_string_list(payload.get("episode_title_patterns"))),
            actor_uid,
        ],
    )
    if not rows:
        raise RuntimeError("Failed to create reddit community")
    return rows[0], 1


def update_reddit_community(*, community_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, int]:
    updates: list[str] = []
    params: list[Any] = []
    query_count = 0

    def _push(value: Any) -> str:
        params.append(value)
        return "%s"

    needs_lookup = any(
        key in payload
        for key in (
            "is_show_focused",
            "network_focus_targets",
            "franchise_focus_targets",
            "analysis_flairs",
            "analysis_all_flairs",
            "episode_title_patterns",
        )
    )

    with pg.db_connection(label="admin_reddit_sources.update_community") as conn:
        lookup_row: dict[str, Any] | None = None
        if needs_lookup:
            lookup_row = pg.fetch_one(
                f"""
                select
                  subreddit,
                  is_show_focused,
                  network_focus_targets,
                  franchise_focus_targets,
                  analysis_flairs,
                  analysis_all_flairs,
                  episode_title_patterns
                from {COMMUNITIES_TABLE}
                where id = %s::uuid
                limit 1
                """,
                [community_id],
                conn=conn,
            )
            query_count += 1
            if lookup_row is None:
                return None, query_count

        if "subreddit" in payload:
            updates.append(f"subreddit = {_push(_validate_subreddit(str(payload.get('subreddit') or '')))}")
        if "display_name" in payload:
            updates.append(f"display_name = {_push(_to_string(payload.get('display_name')))}")
        if "notes" in payload:
            updates.append(f"notes = {_push(_to_string(payload.get('notes')))}")
        if "is_active" in payload and isinstance(payload.get("is_active"), bool):
            updates.append(f"is_active = {_push(payload.get('is_active'))}")

        if any(key in payload for key in ("is_show_focused", "network_focus_targets", "franchise_focus_targets")):
            focus_state = _resolve_focus_state(
                is_show_focused=payload.get("is_show_focused") if "is_show_focused" in payload else None,
                network_focus_targets=payload.get("network_focus_targets")
                if "network_focus_targets" in payload
                else None,
                franchise_focus_targets=payload.get("franchise_focus_targets")
                if "franchise_focus_targets" in payload
                else None,
                fallback=lookup_row,
            )
            updates.append(f"is_show_focused = {_push(focus_state['is_show_focused'])}")
            updates.append(f"network_focus_targets = {_push(_json_dumps(focus_state['network_focus_targets']))}::jsonb")
            updates.append(
                f"franchise_focus_targets = {_push(_json_dumps(focus_state['franchise_focus_targets']))}::jsonb"
            )

        if "analysis_flairs" in payload or "analysis_all_flairs" in payload:
            subreddit = _to_string(payload.get("subreddit")) or _to_string((lookup_row or {}).get("subreddit")) or ""
            analysis_flairs, analysis_all_flairs = _sanitize_analysis_flair_modes(
                subreddit=subreddit,
                existing_analysis_flairs=(lookup_row or {}).get("analysis_flairs"),
                existing_analysis_all_flairs=(lookup_row or {}).get("analysis_all_flairs"),
                analysis_flairs=payload.get("analysis_flairs") if "analysis_flairs" in payload else None,
                analysis_all_flairs=payload.get("analysis_all_flairs") if "analysis_all_flairs" in payload else None,
            )
            updates.append(f"analysis_flairs = {_push(_json_dumps(analysis_flairs))}::jsonb")
            updates.append(f"analysis_all_flairs = {_push(_json_dumps(analysis_all_flairs))}::jsonb")

        if "episode_title_patterns" in payload:
            episode_patterns = _json_dumps(_to_string_list(payload.get("episode_title_patterns")))
            updates.append(f"episode_title_patterns = {_push(episode_patterns)}::jsonb")
        if "post_flair_categories" in payload:
            flair_categories = _json_dumps(_sanitize_flair_categories(payload.get("post_flair_categories")))
            updates.append(f"post_flair_categories = {_push(flair_categories)}::jsonb")
        if "post_flair_assignments" in payload:
            flair_assignments = _json_dumps(_sanitize_flair_assignments(payload.get("post_flair_assignments")))
            updates.append(f"post_flair_assignments = {_push(flair_assignments)}::jsonb")

        if not updates:
            row = pg.fetch_one(
                f"select * from {COMMUNITIES_TABLE} where id = %s::uuid limit 1",
                [community_id],
                conn=conn,
            )
            return (dict(row) if row else None), query_count + 1

        params.append(community_id)
        rows = pg.execute_returning(
            f"""
            update {COMMUNITIES_TABLE}
               set {", ".join(updates)},
                   updated_at = now()
             where id = %s::uuid
             returning *
            """,
            params,
            conn=conn,
        )
        query_count += 1
    return (rows[0] if rows else None), query_count


def update_reddit_community_post_flairs(
    *,
    community_id: str,
    post_flairs: list[str],
    post_flairs_updated_at: str | None = None,
) -> tuple[dict[str, Any] | None, int]:
    with pg.db_connection(label="admin_reddit_sources.update_post_flairs") as conn:
        lookup = pg.fetch_one(
            f"select subreddit from {COMMUNITIES_TABLE} where id = %s::uuid limit 1",
            [community_id],
            conn=conn,
        )
        if lookup is None:
            return None, 1
        sanitized = _sanitize_reddit_flair_list(str(lookup.get("subreddit") or ""), post_flairs)
        timestamp_sql = "%s::timestamptz" if post_flairs_updated_at else "now()"
        params: list[Any] = [_json_dumps(sanitized)]
        if post_flairs_updated_at:
            params.append(post_flairs_updated_at)
        params.append(community_id)
        rows = pg.execute_returning(
            f"""
            update {COMMUNITIES_TABLE}
               set post_flairs = %s::jsonb,
                   post_flairs_updated_at = {timestamp_sql},
                   updated_at = now()
             where id = %s::uuid
             returning *
            """,
            params,
            conn=conn,
        )
    return (rows[0] if rows else None), 2


def delete_reddit_community(community_id: str) -> tuple[bool, int]:
    rows = pg.execute_returning(f"delete from {COMMUNITIES_TABLE} where id = %s::uuid returning id", [community_id])
    return bool(rows), 1


def create_reddit_thread(*, payload: dict[str, Any], actor_uid: str) -> tuple[dict[str, Any], int]:
    rows = pg.execute_returning(
        f"""
        insert into {THREADS_TABLE} (
          community_id,
          trr_show_id,
          trr_show_name,
          trr_season_id,
          reddit_post_id,
          title,
          url,
          permalink,
          author,
          score,
          num_comments,
          posted_at,
          notes,
          source_kind,
          created_by_firebase_uid
        ) values (
          %s::uuid, %s::uuid, %s, %s::uuid, %s, %s, %s, %s, %s,
          %s, %s, %s::timestamptz, %s, %s, %s
        )
        on conflict (trr_show_id, reddit_post_id)
        do update set
          community_id = excluded.community_id,
          trr_show_name = excluded.trr_show_name,
          trr_season_id = excluded.trr_season_id,
          title = excluded.title,
          url = excluded.url,
          permalink = excluded.permalink,
          author = excluded.author,
          score = excluded.score,
          num_comments = excluded.num_comments,
          posted_at = excluded.posted_at,
          notes = excluded.notes,
          source_kind = case
            when %s::boolean then excluded.source_kind
            else {THREADS_TABLE}.source_kind
          end,
          updated_at = now()
        where {THREADS_TABLE}.community_id = excluded.community_id
        returning *
        """,
        [
            payload["community_id"],
            payload["trr_show_id"],
            _to_string(payload.get("trr_show_name")) or "",
            payload.get("trr_season_id"),
            _to_string(payload.get("reddit_post_id")) or "",
            _to_string(payload.get("title")) or "",
            _to_string(payload.get("url")) or "",
            _to_string(payload.get("permalink")),
            _to_string(payload.get("author")),
            _to_nonnegative_int(payload.get("score")),
            _to_nonnegative_int(payload.get("num_comments")),
            _to_string(payload.get("posted_at")),
            _to_string(payload.get("notes")),
            _thread_source_kind(payload.get("source_kind"), default="manual"),
            actor_uid,
            "source_kind" in payload,
        ],
    )
    if rows:
        return rows[0], 1

    conflict = pg.fetch_one(
        f"""
        select id
          from {THREADS_TABLE}
         where trr_show_id = %s::uuid
           and reddit_post_id = %s
         limit 1
        """,
        [payload["trr_show_id"], _to_string(payload.get("reddit_post_id")) or ""],
    )
    if conflict:
        raise ValueError("Thread already exists in another community for this show")
    raise RuntimeError("Failed to create reddit thread")


def update_reddit_thread(*, thread_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, int]:
    updates: list[str] = []
    params: list[Any] = []

    def _push(value: Any) -> str:
        params.append(value)
        return "%s"

    if "community_id" in payload:
        updates.append(f"community_id = {_push(payload.get('community_id'))}::uuid")
    if "trr_show_id" in payload:
        updates.append(f"trr_show_id = {_push(payload.get('trr_show_id'))}::uuid")
    if "trr_show_name" in payload:
        updates.append(f"trr_show_name = {_push(_to_string(payload.get('trr_show_name')) or '')}")
    if "trr_season_id" in payload:
        updates.append(f"trr_season_id = {_push(payload.get('trr_season_id'))}::uuid")
    if "source_kind" in payload:
        updates.append(f"source_kind = {_push(_thread_source_kind(payload.get('source_kind')))}")
    if "title" in payload:
        updates.append(f"title = {_push(_to_string(payload.get('title')) or '')}")
    if "url" in payload:
        updates.append(f"url = {_push(_to_string(payload.get('url')) or '')}")
    if "permalink" in payload:
        updates.append(f"permalink = {_push(_to_string(payload.get('permalink')))}")
    if "author" in payload:
        updates.append(f"author = {_push(_to_string(payload.get('author')))}")
    if "score" in payload:
        updates.append(f"score = {_push(_to_nonnegative_int(payload.get('score')))}")
    if "num_comments" in payload:
        updates.append(f"num_comments = {_push(_to_nonnegative_int(payload.get('num_comments')))}")
    if "posted_at" in payload:
        updates.append(f"posted_at = {_push(_to_string(payload.get('posted_at')))}::timestamptz")
    if "notes" in payload:
        updates.append(f"notes = {_push(_to_string(payload.get('notes')))}")

    if not updates:
        row = pg.fetch_one(f"select * from {THREADS_TABLE} where id = %s::uuid limit 1", [thread_id])
        return (dict(row) if row else None), 1

    params.append(thread_id)
    rows = pg.execute_returning(
        f"""
        update {THREADS_TABLE}
           set {", ".join(updates)},
               updated_at = now()
         where id = %s::uuid
         returning *
        """,
        params,
    )
    return (rows[0] if rows else None), 1


def delete_reddit_thread(thread_id: str) -> tuple[bool, int]:
    rows = pg.execute_returning(f"delete from {THREADS_TABLE} where id = %s::uuid returning id", [thread_id])
    return bool(rows), 1
