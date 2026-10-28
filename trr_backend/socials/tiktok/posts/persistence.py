# ruff: noqa: F821, UP037
"""TikTok post persistence: canonical ``social.tiktok_posts`` upsert, enrichment tables
(tokens, cast members, quality, velocity), anomaly events, and sound snapshots.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    from trr_backend.db import pg
    from trr_backend.socials.model_types import SeasonContext

    # Repository names resolved through the published provider namespace.
    def _apply_assignment_payload(*args: Any, **kwargs: Any) -> Any: ...
    def _as_text_list(*args: Any, **kwargs: Any) -> Any: ...
    def _coerce_dt(*args: Any, **kwargs: Any) -> Any: ...
    def _extract_keyword_tokens_from_text(*args: Any, **kwargs: Any) -> Any: ...
    def _extract_tiktok_sound_fields(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...
    def _now_utc(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_hashtags(*args: Any, **kwargs: Any) -> Any: ...
    def _parse_mentions(*args: Any, **kwargs: Any) -> Any: ...

    _parse_tiktok_time: Any

    def _pg_upsert(*args: Any, **kwargs: Any) -> Any: ...
    def _platform_posts_has_column(*args: Any, **kwargs: Any) -> Any: ...
    def _relation_exists(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_compute_tiktok_post_quality",
    "_compute_tiktok_velocity_metrics",
    "_persist_tiktok_post_enrichment",
    "_record_tiktok_anomaly_event",
    "_upsert_tiktok_post",
    "_persist_tiktok_sound_snapshot",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_POSTS_PERSISTENCE_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


def _compute_tiktok_post_quality(
    *,
    description: str | None,
    thumbnail_url: str | None,
    hashtags: list[str],
    mentions: list[str],
    media_urls: list[str],
    duration_seconds: int | None,
) -> tuple[float, list[str], list[str]]:
    score = 100.0
    missing_fields: list[str] = []
    flags: list[str] = []
    if not str(description or "").strip():
        score -= 25.0
        missing_fields.append("description")
    if not str(thumbnail_url or "").strip():
        score -= 20.0
        missing_fields.append("thumbnail_url")
    if not media_urls:
        score -= 20.0
        missing_fields.append("media_urls")
    if not hashtags:
        score -= 10.0
        flags.append("no_hashtags")
    if not mentions:
        flags.append("no_mentions")
    if not duration_seconds or int(duration_seconds) <= 0:
        score -= 5.0
        flags.append("duration_missing")
    return max(0.0, round(score, 2)), missing_fields, flags


def _compute_tiktok_velocity_metrics(*, views: int, posted_at: datetime | None) -> tuple[float, float, float]:
    _sync_core_overrides()
    if not posted_at:
        base = float(max(0, int(views or 0)))
        return base, base, base
    elapsed_hours = max(1.0, (_now_utc() - posted_at).total_seconds() / 3600.0)
    view_count = float(max(0, int(views or 0)))
    velocity_1h = round(view_count / min(elapsed_hours, 1.0), 3)
    velocity_24h = round(view_count / min(elapsed_hours, 24.0), 3)
    velocity_7d = round(view_count / min(elapsed_hours, 24.0 * 7.0), 3)
    return velocity_1h, velocity_24h, velocity_7d


def _persist_tiktok_post_enrichment(
    *,
    context: SeasonContext,
    run_id: str | None,
    post_row: dict[str, Any],
    description: str | None,
    hashtags: list[str],
    mentions: list[str],
    keywords: list[str],
    cast_mentions: list[dict[str, Any]],
    quality_score: float,
    quality_missing_fields: list[str],
    quality_flags: list[str],
    velocity_1h: float,
    velocity_24h: float,
    velocity_7d: float,
    conn: Any | None = None,
) -> None:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_tokens"):
        return
    post_id = str(post_row.get("id") or "").strip()
    if not post_id:
        return
    keyword_tokens = _extract_keyword_tokens_from_text(description, keywords)
    token_rows: list[tuple[str, str, str]] = []
    for token in hashtags:
        normalized = str(token or "").strip().lstrip("#").lower()
        if normalized:
            token_rows.append((f"#{normalized}", "hashtag", normalized))
    for token in mentions:
        normalized = str(token or "").strip().lstrip("@").lower()
        if normalized:
            token_rows.append((f"@{normalized}", "mention", normalized))
    for token in keyword_tokens:
        normalized = str(token or "").strip().lower()
        if normalized:
            token_rows.append((normalized, "keyword", normalized))
    deduped_token_rows: list[tuple[str, str, str]] = []
    seen_tokens: set[tuple[str, str]] = set()
    for token, token_type, normalized in token_rows:
        key = (token_type, normalized)
        if key in seen_tokens:
            continue
        seen_tokens.add(key)
        deduped_token_rows.append((token, token_type, normalized))

    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(
            cur,
            """
            delete from social.tiktok_post_tokens
            where post_id = %s::uuid
            """,
            [post_id],
        )
        for token, token_type, normalized in deduped_token_rows:
            pg.fetch_one_with_cursor(
                cur,
                """
                insert into social.tiktok_post_tokens (post_id, token, token_type, normalized_token)
                values (%s::uuid, %s, %s, %s)
                on conflict (post_id, token_type, normalized_token) do update
                set token = excluded.token
                returning id::text
                """,
                [post_id, token, token_type, normalized],
            )

        pg.fetch_one_with_cursor(
            cur,
            """
            delete from social.tiktok_post_cast_members
            where post_id = %s::uuid and source = 'auto'
            """,
            [post_id],
        )
        for mention in cast_mentions:
            person_id = str(mention.get("person_id") or "").strip() or None
            pg.fetch_one_with_cursor(
                cur,
                """
                insert into social.tiktok_post_cast_members (
                  post_id,
                  cast_member_id,
                  cast_member_name,
                  confidence,
                  source,
                  run_id
                )
                values (
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  'auto',
                  %s::uuid
                )
                on conflict (post_id, cast_member_id, source) do update
                set
                  cast_member_name = excluded.cast_member_name,
                  confidence = excluded.confidence,
                  run_id = excluded.run_id,
                  updated_at = now()
                returning id::text
                """,
                [post_id, person_id, mention.get("name"), float(mention.get("confidence") or 0.85), run_id],
            )

        pg.fetch_one_with_cursor(
            cur,
            """
            insert into social.tiktok_post_quality (
              post_id,
              quality_score,
              missing_fields,
              confidence_flags,
              updated_at
            )
            values (%s::uuid, %s, %s::jsonb, %s::jsonb, now())
            on conflict (post_id) do update
            set
              quality_score = excluded.quality_score,
              missing_fields = excluded.missing_fields,
              confidence_flags = excluded.confidence_flags,
              updated_at = now()
            returning post_id::text
            """,
            [post_id, quality_score, json.dumps(quality_missing_fields), json.dumps(quality_flags)],
        )
        pg.fetch_one_with_cursor(
            cur,
            """
            insert into social.tiktok_post_velocity (
              post_id,
              velocity_1h,
              velocity_24h,
              velocity_7d,
              snapshot_at,
              updated_at
            )
            values (%s::uuid, %s, %s, %s, now(), now())
            on conflict (post_id) do update
            set
              velocity_1h = excluded.velocity_1h,
              velocity_24h = excluded.velocity_24h,
              velocity_7d = excluded.velocity_7d,
              snapshot_at = now(),
              updated_at = now()
            returning post_id::text
            """,
            [post_id, velocity_1h, velocity_24h, velocity_7d],
        )
        post_updates: dict[str, Any] = {}
        if _platform_posts_has_column("tiktok", "quality_score"):
            post_updates["quality_score"] = quality_score
        if _platform_posts_has_column("tiktok", "quality_flags"):
            post_updates["quality_flags"] = quality_flags
        if _platform_posts_has_column("tiktok", "velocity_1h"):
            post_updates["velocity_1h"] = velocity_1h
        if _platform_posts_has_column("tiktok", "velocity_24h"):
            post_updates["velocity_24h"] = velocity_24h
        if _platform_posts_has_column("tiktok", "velocity_7d"):
            post_updates["velocity_7d"] = velocity_7d
        if _platform_posts_has_column("tiktok", "cast_member_mentions"):
            post_updates["cast_member_mentions"] = [
                str(item.get("name") or "") for item in cast_mentions if str(item.get("name") or "")
            ]
        if post_updates:
            assignments: list[str] = []
            params: list[Any] = []
            for key, value in post_updates.items():
                if key in {"quality_flags", "cast_member_mentions"}:
                    assignments.append(f"{key} = %s::jsonb")
                    params.append(json.dumps(value))
                else:
                    assignments.append(f"{key} = %s")
                    params.append(value)
            with pg.db_cursor(conn=conn) as update_cur:
                pg.fetch_one_with_cursor(
                    update_cur,
                    f"""
                    update social.tiktok_posts
                    set {", ".join(assignments)}
                    where id = %s::uuid
                    returning id::text
                    """,
                    [*params, post_id],
                )


def _record_tiktok_anomaly_event(
    *,
    season_id: str,
    post_id: str,
    account: str,
    event_type: str,
    event_value: float,
    baseline_value: float,
    metadata: dict[str, Any] | None = None,
) -> None:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_anomaly_events"):
        return
    severity = "warn"
    if baseline_value > 0 and event_value >= (3.0 * baseline_value):
        severity = "critical"
    with pg.db_cursor() as cur:
        pg.fetch_one_with_cursor(
            cur,
            """
            insert into social.tiktok_anomaly_events (
              season_id,
              post_id,
              account,
              event_type,
              severity,
              event_value,
              baseline_value,
              metadata
            )
            values (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s::jsonb)
            returning id::text
            """,
            [
                season_id,
                post_id,
                account,
                event_type,
                severity,
                event_value,
                baseline_value,
                json.dumps(dict(metadata or {})),
            ],
        )


def _upsert_tiktok_post(
    context: SeasonContext | None, *, job_id: str | None, account: str, post: Any, conn: Any | None = None
) -> dict[str, Any] | None:
    _sync_core_overrides()
    posted_at = _parse_tiktok_time(getattr(post, "create_time", None))
    raw_data = post.to_dict() if hasattr(post, "to_dict") else {}
    if not isinstance(raw_data, dict):
        raw_data = {}
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    description = str(getattr(post, "description", "") or "")
    hashtags = _as_text_list(
        [
            *_as_text_list(getattr(post, "hashtags", []), strip_prefix="#"),
            *_parse_hashtags(description),
        ],
        strip_prefix="#",
    )
    mentions = _as_text_list(
        [
            *_as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@"),
            *_parse_mentions(description),
        ],
        prefix="@",
        strip_prefix="@",
    )
    sound_fields = _extract_tiktok_sound_fields(raw_data, post)
    payload = {
        "video_id": getattr(post, "video_id", ""),
        "aweme_id": getattr(post, "video_id", ""),
        "username": getattr(post, "username", account),
        "user_id": None,
        "nickname": getattr(post, "author_nickname", None),
        "description": getattr(post, "description", None),
        "hashtags": hashtags,
        "music_info": {
            "title": getattr(post, "music_title", None),
            "author": getattr(post, "music_author", None),
        },
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "shares": int(getattr(post, "shares", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "duration_seconds": int(getattr(post, "duration", 0) or 0),
        "thumbnail_url": thumbnail_url,
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": raw_data,
        "source_account": account,
    }
    _apply_assignment_payload(payload, context)
    if job_id:
        payload["job_id"] = job_id
    if _platform_posts_has_column("tiktok", "saves", conn=conn):
        payload["saves"] = int(getattr(post, "saves", 0) or 0)
    if _platform_posts_has_column("tiktok", "mentions", conn=conn):
        payload["mentions"] = mentions
    if _platform_posts_has_column("tiktok", "media_urls", conn=conn):
        payload["media_urls"] = media_urls
    if _platform_posts_has_column("tiktok", "sound_id", conn=conn):
        payload["sound_id"] = sound_fields.get("sound_id")
    if _platform_posts_has_column("tiktok", "sound_title", conn=conn):
        payload["sound_title"] = sound_fields.get("sound_title")
    if _platform_posts_has_column("tiktok", "sound_author", conn=conn):
        payload["sound_author"] = sound_fields.get("sound_author")
    if _platform_posts_has_column("tiktok", "sound_usage_count", conn=conn):
        payload["sound_usage_count"] = _normalize_non_negative_int(sound_fields.get("sound_usage_count"))
    if _platform_posts_has_column("tiktok", "user_avatar_url", conn=conn):
        payload["user_avatar_url"] = str(getattr(post, "user_avatar_url", "") or "").strip() or None
    return _pg_upsert("tiktok_posts", payload, conflict_col="video_id", conn=conn)


def _persist_tiktok_sound_snapshot(*, sound_id: str, snapshot: dict[str, Any], conn: Any | None = None) -> None:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_sounds") or not _relation_exists("social.tiktok_sound_posts"):
        return
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(
            cur,
            """
            insert into social.tiktok_sounds (
              sound_id,
              title,
              artist_name,
              usage_count,
              source_url,
              last_seen_at,
              updated_at
            )
            values (%s, %s, %s, %s, %s, now(), now())
            on conflict (sound_id) do update
            set
              title = coalesce(excluded.title, social.tiktok_sounds.title),
              artist_name = coalesce(excluded.artist_name, social.tiktok_sounds.artist_name),
              usage_count = greatest(coalesce(excluded.usage_count, 0), coalesce(social.tiktok_sounds.usage_count, 0)),
              source_url = coalesce(excluded.source_url, social.tiktok_sounds.source_url),
              last_seen_at = now(),
              updated_at = now()
            returning sound_id
            """,
            [
                sound_id,
                snapshot.get("title"),
                snapshot.get("artist_name"),
                max(0, int(snapshot.get("usage_count") or 0)),
                snapshot.get("source_url"),
            ],
        )
        for post in snapshot.get("posts") or []:
            platform_post_id = str((post or {}).get("platform_post_id") or "").strip()
            if not platform_post_id:
                continue
            pg.fetch_one_with_cursor(
                cur,
                """
                insert into social.tiktok_sound_posts (
                  sound_id,
                  platform_post_id,
                  creator_handle,
                  posted_at,
                  views,
                  likes,
                  comments,
                  shares,
                  thumbnail_url,
                  caption,
                  raw_data,
                  updated_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                on conflict (sound_id, platform_post_id) do update
                set
                  creator_handle = coalesce(excluded.creator_handle, social.tiktok_sound_posts.creator_handle),
                  posted_at = coalesce(excluded.posted_at, social.tiktok_sound_posts.posted_at),
                  views = greatest(coalesce(excluded.views, 0), coalesce(social.tiktok_sound_posts.views, 0)),
                  likes = greatest(coalesce(excluded.likes, 0), coalesce(social.tiktok_sound_posts.likes, 0)),
                  comments = greatest(coalesce(excluded.comments, 0), coalesce(social.tiktok_sound_posts.comments, 0)),
                  shares = greatest(coalesce(excluded.shares, 0), coalesce(social.tiktok_sound_posts.shares, 0)),
                  thumbnail_url = coalesce(excluded.thumbnail_url, social.tiktok_sound_posts.thumbnail_url),
                  caption = coalesce(excluded.caption, social.tiktok_sound_posts.caption),
                  raw_data = case
                    when social.tiktok_sound_posts.raw_data is null or social.tiktok_sound_posts.raw_data = '{}'::jsonb
                      then excluded.raw_data
                    else social.tiktok_sound_posts.raw_data
                  end,
                  updated_at = now()
                returning id::text
                """,
                [
                    sound_id,
                    platform_post_id,
                    (post or {}).get("creator_handle"),
                    _coerce_dt((post or {}).get("posted_at")),
                    max(0, int((post or {}).get("views") or 0)),
                    max(0, int((post or {}).get("likes") or 0)),
                    max(0, int((post or {}).get("comments") or 0)),
                    max(0, int((post or {}).get("shares") or 0)),
                    (post or {}).get("thumbnail_url"),
                    (post or {}).get("caption"),
                    json.dumps(dict(post or {})),
                ],
            )


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_compute_tiktok_post_quality",
    "_compute_tiktok_velocity_metrics",
    "_persist_tiktok_post_enrichment",
    "_persist_tiktok_sound_snapshot",
    "_record_tiktok_anomaly_event",
    "_upsert_tiktok_post",
]
