"""Button-driven social sync-session orchestration."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from threading import Thread
from typing import Any, Literal
from uuid import uuid4

from trr_backend.db import pg
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

logger = logging.getLogger(__name__)

SyncPassKind = Literal["posts_and_comments", "comments_only", "details_refresh"]
SyncSessionStatus = Literal[
    "initializing",
    "pass_running",
    "pass_evaluating",
    "completing",
    "completed",
    "failed",
    "cancelling",
    "cancelled",
]

SYNC_PASS_SEQUENCE: tuple[SyncPassKind, ...] = ("posts_and_comments", "comments_only", "details_refresh")
SYNC_ACTIVE_STATUSES: tuple[SyncSessionStatus, ...] = (
    "initializing",
    "pass_running",
    "pass_evaluating",
    "completing",
    "cancelling",
)
SYNC_TERMINAL_STATUSES: tuple[SyncSessionStatus, ...] = ("completed", "failed", "cancelled")
SYNC_RETRYABLE_ERROR_CODES = {
    "AUTH_PRECHECK_FAILED",
    "AUTH_REQUIRED",
    "AUTH_SESSION_FAILURE",
    "BLOCKED",
    "COMMENT_MEDIA_FAILURE",
    "DETAIL_RESOLUTION_FAILURE",
    "ASSET_RESOLUTION_FAILURE",
    "AVATAR_RETRIEVAL_FAILURE",
    "MIRROR_DOWNLOAD_FAILURE",
    "NETWORK",
    "PARTIAL_MEDIA_SET_FAILURE",
    "RATE_LIMITED",
}
SYNC_RETRY_KIND_TO_PASS_KIND: dict[str, SyncPassKind] = {
    "retry_missing_comments": "comments_only",
    "retry_failed_media": "details_refresh",
    "retry_missing_avatars": "details_refresh",
    "retry_missing_comment_media": "details_refresh",
}
BOUNDED_DEPTH_PLATFORMS = {"tiktok", "facebook", "threads", "twitter"}
DEFAULT_SYNC_MAX_COMMENTS_PER_POST = 5_000
DEFAULT_SYNC_MAX_REPLIES_PER_POST = 1_000


def _social_repo() -> Any:
    from trr_backend.repositories import social_season_analytics as social_repo

    return social_repo


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _sync_sessions_ready() -> bool:
    social_repo = _social_repo()
    return bool(social_repo._relation_exists("social.sync_sessions"))  # noqa: SLF001


def _coerce_dt(value: datetime | str | None) -> datetime | None:
    social_repo = _social_repo()
    return social_repo._coerce_dt(value)  # noqa: SLF001


def _iso(value: datetime | None) -> str | None:
    social_repo = _social_repo()
    return social_repo._iso(value)  # noqa: SLF001


def _normalize_platforms(platforms: list[str] | None) -> list[str]:
    requested = platforms or list(SOCIAL_SUPPORTED_PLATFORMS)
    normalized: list[str] = []
    for platform in requested:
        token = str(platform or "").strip().lower()
        if token and token in SOCIAL_SUPPORTED_PLATFORMS and token not in normalized:
            normalized.append(token)
    return normalized or list(SOCIAL_SUPPORTED_PLATFORMS)


def _window_span_days(*, date_start: datetime | None, date_end: datetime | None) -> float:
    if date_start is None or date_end is None:
        return 0.0
    return max(0.0, (date_end - date_start).total_seconds() / 86400.0)


def _is_sync_profile_payload(config: dict[str, Any]) -> bool:
    return bool(config.get("sync_session_profile") == "button_sync_v1")


def _build_dedup_key(
    *,
    season_id: str,
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
    config: dict[str, Any],
) -> str:
    payload = {
        "season_id": season_id,
        "source_scope": source_scope,
        "platforms": _normalize_platforms(config.get("platforms")),
        "date_start": _iso(date_start),
        "date_end": _iso(date_end),
        "sync_strategy": config.get("sync_strategy"),
        "accounts_override": list(config.get("accounts_override") or []),
        "hashtags_override": list(config.get("hashtags_override") or []),
        "keywords_override": list(config.get("keywords_override") or []),
        "sound_ids": list(config.get("sound_ids") or []),
        "youtube_source_mode": config.get("youtube_source_mode"),
        "youtube_force_reindex": bool(config.get("youtube_force_reindex")),
        "youtube_force_media_refresh": bool(config.get("youtube_force_media_refresh")),
        "youtube_force_comment_refresh": bool(config.get("youtube_force_comment_refresh")),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def build_sync_profile(
    *,
    season_id: str,
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
    base_config: dict[str, Any],
) -> dict[str, Any]:
    normalized_platforms = _normalize_platforms(base_config.get("platforms"))
    span_days = _window_span_days(date_start=date_start, date_end=date_end)
    profile = dict(base_config)
    profile["platforms"] = normalized_platforms
    profile["source_scope"] = source_scope
    profile["date_start"] = _iso(date_start)
    profile["date_end"] = _iso(date_end)
    profile["max_posts_per_target"] = int(profile.get("max_posts_per_target") or 0)
    profile["max_comments_per_post"] = int(profile.get("max_comments_per_post") or 0)
    profile["max_replies_per_post"] = int(profile.get("max_replies_per_post") or 0)
    profile["allow_inline_dev_fallback"] = bool(profile.get("allow_inline_dev_fallback") or False)
    profile["sync_strategy"] = str(profile.get("sync_strategy") or "incremental").strip().lower() or "incremental"
    profile["comment_refresh_policy"] = str(profile.get("comment_refresh_policy") or "balanced").strip().lower()
    profile["fetch_replies"] = bool(profile.get("fetch_replies", True))
    profile["media_required"] = True
    profile["avatar_required"] = True
    profile["sync_session_profile"] = "button_sync_v1"
    profile["season_id"] = season_id
    single_platform = normalized_platforms[0] if len(normalized_platforms) == 1 else None
    if single_platform in BOUNDED_DEPTH_PLATFORMS:
        if int(profile.get("max_comments_per_post") or 0) <= 0:
            profile["max_comments_per_post"] = DEFAULT_SYNC_MAX_COMMENTS_PER_POST
        if int(profile.get("max_replies_per_post") or 0) <= 0:
            profile["max_replies_per_post"] = DEFAULT_SYNC_MAX_REPLIES_PER_POST
    if single_platform == "tiktok":
        profile["runner_strategy"] = "single_runner"
        profile["runner_count"] = 1
        profile["window_shard_hours"] = 24
    elif single_platform in {"facebook", "threads"}:
        profile["runner_strategy"] = "single_runner"
        profile["runner_count"] = 1
        profile["window_shard_hours"] = 12
    elif single_platform == "twitter" and span_days <= 14:
        profile["runner_strategy"] = "adaptive_dual_runner"
        profile["runner_count"] = 2
        profile["window_shard_hours"] = 8
    elif span_days <= 3:
        profile["runner_strategy"] = "single_runner"
        profile["runner_count"] = 1
        profile["window_shard_hours"] = 12
    elif span_days <= 14:
        profile["runner_strategy"] = "adaptive_dual_runner"
        profile["runner_count"] = 2
        profile["window_shard_hours"] = 6
    else:
        profile["runner_strategy"] = "adaptive_dual_runner"
        profile["runner_count"] = 2
        profile["window_shard_hours"] = 4
    return profile


def _base_config_for_pass(
    *,
    sync_config: dict[str, Any],
    pass_kind: SyncPassKind,
    source_ids_by_platform: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    config = dict(sync_config)
    config["ingest_mode"] = pass_kind
    if pass_kind == "posts_and_comments":
        config["comment_refresh_policy"] = "balanced"
    elif pass_kind == "comments_only":
        config["comment_refresh_policy"] = "missing_only"
        config["runner_strategy"] = "single_runner"
        config["runner_count"] = 1
        config["window_shard_hours"] = max(4, min(12, int(config.get("window_shard_hours") or 12)))
    elif pass_kind == "details_refresh":
        config["comment_refresh_policy"] = "missing_only"
    if source_ids_by_platform:
        config["comment_anchor_source_ids"] = source_ids_by_platform
    else:
        config.pop("comment_anchor_source_ids", None)
    return config


def _retry_backoff_seconds(attempt: int, *, base_delay_seconds: int) -> int:
    normalized_attempt = max(1, int(attempt))
    return max(1, int(base_delay_seconds)) * (2 ** max(0, normalized_attempt - 1))


def _fetch_sync_session_row(sync_session_id: str) -> dict[str, Any] | None:
    if not _sync_sessions_ready():
        return None
    return pg.fetch_one(
        """
        select
          s.id::text as id,
          s.season_id::text as season_id,
          s.show_id::text as show_id,
          s.source_scope,
          s.platforms,
          s.date_start,
          s.date_end,
          s.dedup_key,
          s.status,
          s.current_pass_kind,
          s.current_pass_attempt,
          s.current_run_id::text as current_run_id,
          s.pass_sequence,
          s.sync_config,
          s.pass_history,
          s.completeness_snapshot,
          s.follow_up_reason,
          s.max_attempts_per_kind,
          s.retry_base_delay_seconds,
          s.next_pass_available_at,
          s.initiated_by,
          s.client_session_id,
          s.client_workflow_id,
          s.created_at,
          s.started_at,
          s.completed_at,
          s.cancelled_at,
          s.updated_at
        from social.sync_sessions s
        where s.id = %s::uuid
        """,
        [sync_session_id],
    )


def _fetch_sync_session_lock(sync_session_id: str) -> tuple[int, bool]:
    lock_key = int(hashlib.md5(f"sync-session:{sync_session_id}".encode()).hexdigest()[:15], 16) % (2**31)
    locked_row = pg.fetch_one("select pg_try_advisory_lock(%s) as locked", [lock_key]) or {}
    return lock_key, bool(locked_row.get("locked"))


def _release_sync_session_lock(lock_key: int) -> None:
    try:
        pg.fetch_one("select pg_advisory_unlock(%s)", [lock_key])
    except Exception:  # noqa: BLE001
        logger.debug("Failed to release sync-session lock=%s", lock_key, exc_info=True)


def _update_sync_session(sync_session_id: str, **fields: Any) -> dict[str, Any]:
    if not fields:
        row = _fetch_sync_session_row(sync_session_id)
        if row is None:
            raise ValueError("sync_session_not_found")
        return row
    assignments: list[str] = []
    params: list[Any] = []
    for key, value in fields.items():
        assignments.append(f"{key} = %s")
        if key in {"sync_config", "pass_history", "completeness_snapshot"}:
            params.append(json.dumps(value))
            assignments[-1] = f"{key} = %s::jsonb"
        else:
            params.append(value)
    params.extend([_now_utc(), sync_session_id])
    row = pg.fetch_one(
        f"""
        update social.sync_sessions
        set
          {", ".join(assignments)},
          updated_at = %s
        where id = %s::uuid
        returning id::text
        """,
        params,
    )
    if not row:
        raise ValueError("sync_session_not_found")
    updated = _fetch_sync_session_row(sync_session_id)
    if updated is None:
        raise ValueError("sync_session_not_found")
    return updated


def _append_pass_history(
    pass_history: list[dict[str, Any]] | None,
    *,
    pass_kind: SyncPassKind,
    pass_attempt: int,
    pass_sequence: int,
    run_id: str,
    follow_up_reason: str,
) -> list[dict[str, Any]]:
    history = list(pass_history or [])
    history.append(
        {
            "pass_kind": pass_kind,
            "pass_attempt": int(pass_attempt),
            "pass_sequence": int(pass_sequence),
            "run_id": run_id,
            "follow_up_reason": follow_up_reason,
            "started_at": _iso(_now_utc()),
        }
    )
    return history


def _current_run_payload(current_run_id: str | None) -> dict[str, Any] | None:
    normalized = str(current_run_id or "").strip()
    if not normalized:
        return None
    row = pg.fetch_one(
        """
        select
          id::text as id,
          status,
          source_scope,
          config,
          summary,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where id = %s::uuid
        """,
        [normalized],
    )
    if not row:
        return None
    config = row.get("config") if isinstance(row.get("config"), dict) else {}
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
    return {
        "id": str(row.get("id") or ""),
        "status": str(row.get("status") or ""),
        "source_scope": str(row.get("source_scope") or ""),
        "config": config,
        "summary": summary,
        "created_at": _iso(_coerce_dt(row.get("created_at"))),
        "started_at": _iso(_coerce_dt(row.get("started_at"))),
        "completed_at": _iso(_coerce_dt(row.get("completed_at"))),
        "cancelled_at": _iso(_coerce_dt(row.get("cancelled_at"))),
    }


def _run_error_codes(run_id: str) -> set[str]:
    rows = pg.fetch_all(
        """
        select
          upper(
            coalesce(
              nullif(metadata->>'job_error_code', ''),
              nullif(last_error_code, ''),
              ''
            )
          ) as error_code
        from social.scrape_jobs
        where run_id = %s::uuid
          and status in ('failed', 'retrying')
        """,
        [run_id],
    )
    return {
        str(row.get("error_code") or "").strip().upper() for row in rows if str(row.get("error_code") or "").strip()
    }


def _has_retryable_errors(run_id: str) -> bool:
    return bool(_run_error_codes(run_id) & SYNC_RETRYABLE_ERROR_CODES)


def _build_avatar_coverage_snapshot(
    *,
    season_id: str,
    platforms: list[str],
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
) -> dict[str, Any]:
    social_repo = _social_repo()
    context = social_repo.get_season_context(season_id)
    target_accounts_by_platform = social_repo._target_accounts_by_platform(  # noqa: SLF001
        season_id,
        source_scope=source_scope,
        context=context,
    )
    completed_avatar_rows = (
        pg.fetch_all(
            """
            select platform, account_handle, source_url, status
            from social.avatar_registry
            where status in ('mirrored', 'unsupported')
            """
        )
        if social_repo._relation_exists("social.avatar_registry")  # noqa: SLF001
        else []
    )
    completed_avatar_keys = {
        (
            str(row.get("platform") or "").strip().lower(),
            str(row.get("account_handle") or "").strip().lstrip("@").lower(),
            str(row.get("source_url") or "").strip(),
        )
        for row in completed_avatar_rows
    }
    completed_avatar_handles = {
        (
            str(row.get("platform") or "").strip().lower(),
            str(row.get("account_handle") or "").strip().lstrip("@").lower(),
        )
        for row in completed_avatar_rows
    }
    by_platform: dict[str, dict[str, Any]] = {}
    total_posts = 0
    total_missing = 0
    for platform in platforms:
        post_table = social_repo.PLATFORM_POST_TABLES.get(platform)
        posted_at_column = social_repo.PLATFORM_POSTED_AT_COLUMN.get(platform)
        if not post_table or not posted_at_column:
            by_platform[platform] = {"posts_scanned": 0, "missing_avatar_count": 0, "up_to_date": True}
            continue
        account_handles = sorted(set((target_accounts_by_platform or {}).get(platform, set())))
        params: list[Any] = [season_id, date_start, date_end]
        account_filter = ""
        if account_handles:
            if platform == "instagram":
                account_filter = (
                    "and ltrim(lower(coalesce(nullif(p.source_account, ''), "
                    "nullif(p.username, ''), '')), '@') = any(%s)"
                )
            elif platform == "youtube":
                account_filter = (
                    "and ltrim(lower(coalesce(nullif(p.source_account, ''), "
                    "nullif(p.channel_title, ''), '')), '@') = any(%s)"
                )
            else:
                account_filter = (
                    "and ltrim(lower(coalesce(nullif(p.source_account, ''), "
                    "nullif(p.username, ''), '')), '@') = any(%s)"
                )
            params.append(account_handles)
        row = pg.fetch_all(
            f"""
            select to_jsonb(p) as post_json
            from social.{post_table} p
            where p.season_id = %s::uuid
              and p.{posted_at_column} >= %s
              and p.{posted_at_column} <= %s
              {account_filter}
            """,
            params,
        )
        posts_scanned = len(row)
        missing_avatar_count = 0
        for payload in row:
            post_json = payload.get("post_json") if isinstance(payload.get("post_json"), dict) else {}
            if platform == "instagram":
                owner_source = str(post_json.get("owner_profile_pic_url") or "").strip()
                owner_hosted = str(post_json.get("hosted_owner_profile_pic_url") or "").strip()
                owner_handle = str(post_json.get("username") or post_json.get("source_account") or "").strip()
                if owner_source and not owner_hosted:
                    if (platform, owner_handle.lstrip("@").lower(), owner_source) not in completed_avatar_keys:
                        missing_avatar_count += 1
                hosted_tagged = (
                    post_json.get("hosted_tagged_profile_pics")
                    if isinstance(post_json.get("hosted_tagged_profile_pics"), dict)
                    else {}
                )
                for detail_key in ("tagged_users_detail", "collaborators_detail"):
                    for detail in post_json.get(detail_key) or []:
                        if not isinstance(detail, dict):
                            continue
                        detail_handle = str(detail.get("username") or "").strip().lstrip("@").lower()
                        detail_source = str(detail.get("profile_pic_url") or "").strip()
                        detail_hosted = str((hosted_tagged or {}).get(detail_handle) or "").strip()
                        if detail_source and not detail_hosted:
                            if (platform, detail_handle, detail_source) not in completed_avatar_keys:
                                missing_avatar_count += 1
                for mention in social_repo._as_text_list(  # noqa: SLF001
                    post_json.get("mentions"),
                    prefix="@",
                    strip_prefix="@",
                ):
                    mention_handle = str(mention or "").strip().lstrip("@").lower()
                    if not mention_handle:
                        continue
                    mention_hosted = str((hosted_tagged or {}).get(mention_handle) or "").strip()
                    if mention_hosted:
                        continue
                    if (platform, mention_handle) not in completed_avatar_handles:
                        missing_avatar_count += 1
            else:
                source_avatar = str(post_json.get("user_avatar_url") or "").strip()
                hosted_avatar = str(post_json.get("hosted_user_avatar_url") or "").strip()
                account_handle = str(
                    post_json.get("username") or post_json.get("channel_title") or post_json.get("source_account") or ""
                ).strip()
                if source_avatar and not hosted_avatar:
                    if (platform, account_handle.lstrip("@").lower(), source_avatar) not in completed_avatar_keys:
                        missing_avatar_count += 1
        if platform == "instagram" and social_repo._column_exists(  # noqa: SLF001
            "social",
            "instagram_comments",
            "author_profile_pic_url",
        ):
            verified_comment_params: list[Any] = [season_id, date_start, date_end]
            verified_comment_account_filter = ""
            if account_handles:
                verified_comment_account_filter = (
                    "and ltrim(lower(coalesce(nullif(p.source_account, ''), "
                    "nullif(p.username, ''), '')), '@') = any(%s)"
                )
                verified_comment_params.append(account_handles)
            verified_comment_rows = pg.fetch_all(
                f"""
                select
                  c.username,
                  c.author_profile_pic_url
                from social.instagram_comments c
                join social.instagram_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  and coalesce(c.author_is_verified, false) = true
                  and coalesce(nullif(c.author_profile_pic_url, ''), '') <> ''
                  {verified_comment_account_filter}
                """,
                verified_comment_params,
            )
            seen_verified_commenters: set[tuple[str, str]] = set()
            for comment_row in verified_comment_rows:
                comment_handle = str(comment_row.get("username") or "").strip().lstrip("@").lower()
                comment_source = str(comment_row.get("author_profile_pic_url") or "").strip()
                if (
                    comment_handle
                    and comment_source
                    and (comment_handle, comment_source) not in seen_verified_commenters
                ):
                    seen_verified_commenters.add((comment_handle, comment_source))
                    if (platform, comment_handle, comment_source) not in completed_avatar_keys and (
                        platform,
                        comment_handle,
                    ) not in completed_avatar_handles:
                        missing_avatar_count += 1
        total_posts += posts_scanned
        total_missing += missing_avatar_count
        by_platform[platform] = {
            "posts_scanned": posts_scanned,
            "missing_avatar_count": missing_avatar_count,
            "up_to_date": missing_avatar_count <= 0,
        }
    return {
        "posts_scanned": total_posts,
        "missing_avatar_count": total_missing,
        "up_to_date": total_missing <= 0,
        "by_platform": by_platform,
    }


def _sync_snapshot_up_to_date(snapshot: dict[str, Any]) -> bool:
    comments = snapshot.get("comments_coverage") if isinstance(snapshot.get("comments_coverage"), dict) else {}
    assets = snapshot.get("asset_coverage") if isinstance(snapshot.get("asset_coverage"), dict) else {}
    comment_media = (
        snapshot.get("comment_media_coverage") if isinstance(snapshot.get("comment_media_coverage"), dict) else {}
    )
    avatars = snapshot.get("avatar_coverage") if isinstance(snapshot.get("avatar_coverage"), dict) else {}
    return (
        bool(comments.get("up_to_date"))
        and bool(assets.get("up_to_date"))
        and bool(comment_media.get("up_to_date"))
        and bool(avatars.get("up_to_date"))
    )


def _build_completeness_snapshot(
    *,
    season_id: str,
    source_scope: str,
    platforms: list[str],
    date_start: datetime,
    date_end: datetime,
) -> dict[str, Any]:
    social_repo = _social_repo()
    comments_coverage = social_repo.get_comments_coverage(
        season_id,
        platforms=platforms,
        timezone="America/New_York",
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )
    mirror_coverage = social_repo.get_mirror_coverage(
        season_id,
        platforms=platforms,
        timezone="America/New_York",
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )
    avatar_coverage = _build_avatar_coverage_snapshot(
        season_id=season_id,
        platforms=platforms,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )
    comment_media_coverage = {
        "items_scanned": int(mirror_coverage.get("comment_media_items_scanned") or 0),
        "needs_mirror_count": int(mirror_coverage.get("comment_media_needs_mirror_count") or 0),
        "mirrored_count": int(mirror_coverage.get("comment_media_mirrored_count") or 0),
        "failed_count": int(mirror_coverage.get("comment_media_failed_count") or 0),
        "pending_count": int(mirror_coverage.get("comment_media_pending_count") or 0),
        "up_to_date": int(mirror_coverage.get("comment_media_needs_mirror_count") or 0) <= 0,
    }
    comment_targets = _build_missing_comment_targets(
        season_id=season_id,
        platforms=platforms,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )
    detail_target_groups = _build_missing_detail_target_groups(
        season_id=season_id,
        platforms=platforms,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )
    comment_target_count = sum(len(source_ids) for source_ids in comment_targets.values())
    detail_target_count = sum(len(source_ids) for source_ids in detail_target_groups["details"].values())
    avatar_target_count = sum(len(source_ids) for source_ids in detail_target_groups["avatars"].values())
    comment_media_target_count = sum(len(source_ids) for source_ids in detail_target_groups["comment_media"].values())
    combined_incomplete_ids: set[tuple[str, str]] = set()
    for platform, source_ids in comment_targets.items():
        combined_incomplete_ids.update((platform, source_id) for source_id in source_ids)
    for platform_targets in detail_target_groups.values():
        for platform, source_ids in platform_targets.items():
            combined_incomplete_ids.update((platform, source_id) for source_id in source_ids)
    follow_up_dimensions = _follow_up_dimensions_from_snapshot(
        comments_coverage=comments_coverage,
        asset_coverage=mirror_coverage,
        comment_media_coverage=comment_media_coverage,
        avatar_coverage=avatar_coverage,
    )
    snapshot = {
        "window": {
            "start": _iso(date_start),
            "end": _iso(date_end),
        },
        "comments_coverage": comments_coverage,
        "asset_coverage": mirror_coverage,
        "comment_media_coverage": comment_media_coverage,
        "avatar_coverage": avatar_coverage,
        "missing_asset_count": int(mirror_coverage.get("needs_mirror_count") or 0),
        "missing_comment_media_count": int(comment_media_coverage.get("needs_mirror_count") or 0),
        "missing_avatar_count": int(avatar_coverage.get("missing_avatar_count") or 0),
        "incomplete_post_count": int(len(combined_incomplete_ids)),
        "targeted_anchor_count": int(comment_target_count),
        "comment_target_count": int(comment_target_count),
        "detail_target_count": int(detail_target_count),
        "avatar_target_count": int(avatar_target_count),
        "comment_media_target_count": int(comment_media_target_count),
        "follow_up_dimensions": follow_up_dimensions,
    }
    snapshot["up_to_date"] = _sync_snapshot_up_to_date(snapshot)
    return snapshot


def _env_present_any(*names: str) -> bool:
    return any(str(os.getenv(name) or "").strip() for name in names)


def _infer_auth_mode(platform: str) -> tuple[str | None, str | None]:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform == "tiktok":
        if _env_present_any(
            "SOCIAL_TIKTOK_COOKIES_JSON",
            "SOCIAL_TIKTOK_COOKIES_FILE",
            "TIKTOK_COOKIES_SESSIONID",
            "TIKTOK_COOKIES_SID_TT",
        ):
            return "cookies", None
        return "cookies", "tiktok_cookies_missing"
    if normalized_platform == "facebook":
        if _env_present_any(
            "SOCIAL_FACEBOOK_COOKIES_JSON",
            "SOCIAL_FACEBOOK_COOKIES_FILE",
            "FACEBOOK_COOKIES_C_USER",
            "FACEBOOK_COOKIES_XS",
        ):
            return "cookies", None
        return "public", "facebook_public_fallback"
    if normalized_platform == "threads":
        if _env_present_any(
            "SOCIAL_THREADS_COOKIES_JSON",
            "SOCIAL_THREADS_COOKIES_FILE",
            "THREADS_COOKIES_SESSIONID",
            "THREADS_COOKIES_CSRFTOKEN",
        ):
            return "cookies", None
        return "public", "threads_public_fallback"
    if normalized_platform == "twitter":
        if _env_present_any(
            "SOCIAL_TWITTER_COOKIES_JSON",
            "SOCIAL_TWITTER_COOKIES_FILE",
            "TWITTER_COOKIES_AUTH_TOKEN",
            "TWITTER_COOKIES_CT0",
        ):
            return "cookies", None
        if _env_present_any("SOCIAL_TWITTER_BEARER_TOKEN", "TWITTER_BEARER_TOKEN"):
            return "bearer", None
        if _env_present_any("TWIKIT_USERNAME", "TWIKIT_EMAIL") and _env_present_any("TWIKIT_PASSWORD"):
            return "twikit", None
        return "cookies_or_bearer_or_twikit", "twitter_auth_missing"
    return None, None


def _derive_execution_path(*, platform: str, auth_mode: str | None) -> str:
    from trr_backend.socials.crawlee_runtime.config import should_use_crawlee

    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform == "twitter" and auth_mode == "twikit":
        return "twikit-assisted"
    return "crawlee" if should_use_crawlee(normalized_platform) else "legacy"


def _derive_source_mode(*, platform: str, sync_config: dict[str, Any]) -> str | None:
    normalized_platform = str(platform or "").strip().lower()
    accounts_override = list(sync_config.get("accounts_override") or [])
    hashtags_override = list(sync_config.get("hashtags_override") or [])
    keywords_override = list(sync_config.get("keywords_override") or [])
    sound_ids = list(sync_config.get("sound_ids") or [])
    if normalized_platform == "tiktok":
        if sound_ids:
            return "sound"
        if hashtags_override:
            return "hashtag"
        if keywords_override:
            return "keyword"
        if accounts_override:
            return "account"
        return "week_targets"
    if normalized_platform == "facebook":
        if keywords_override:
            return "mixed"
        return "feed" if accounts_override else "mixed"
    if normalized_platform == "threads":
        if keywords_override:
            return "post"
        return "profile" if accounts_override else "mixed"
    if normalized_platform == "twitter":
        if keywords_override:
            return "search"
        return "profile" if accounts_override else "mixed"
    return None


def _coverage_dimension_payload(
    dimension: str,
    coverage: dict[str, Any] | None,
    *,
    platform: str,
) -> dict[str, Any] | None:
    if not isinstance(coverage, dict):
        return None
    by_platform = coverage.get("by_platform") if isinstance(coverage.get("by_platform"), dict) else {}
    platform_slice = by_platform.get(platform) if isinstance(by_platform.get(platform), dict) else None
    if platform_slice is not None:
        payload = dict(platform_slice)
    else:
        payload = {}
    if dimension == "comment_media" and not payload:
        payload = {
            "items_scanned": int(coverage.get("items_scanned") or 0),
            "needs_mirror_count": int(coverage.get("needs_mirror_count") or 0),
            "mirrored_count": int(coverage.get("mirrored_count") or 0),
            "failed_count": int(coverage.get("failed_count") or 0),
            "pending_count": int(coverage.get("pending_count") or 0),
        }
    if dimension == "avatars" and not payload:
        payload = dict(by_platform.get(platform)) if isinstance(by_platform.get(platform), dict) else {}
    if dimension == "comments":
        saved_comments = int(payload.get("saved_comments") or 0)
        reported_comments = int(payload.get("reported_comments") or 0)
        effective_reported_comments = max(saved_comments, reported_comments)
        if effective_reported_comments > reported_comments:
            payload["reported_comments_raw"] = reported_comments
            payload["reported_comments"] = effective_reported_comments
        comment_sync_status = (
            dict(payload.get("comment_sync_status")) if isinstance(payload.get("comment_sync_status"), dict) else {}
        )
        if comment_sync_status:
            expected_count = int(comment_sync_status.get("expected_count") or 0)
            fetched_count = int(comment_sync_status.get("fetched_count") or 0)
            upserted_count = int(comment_sync_status.get("upserted_count") or 0)
            effective_expected_count = max(expected_count, fetched_count, upserted_count)
            if effective_expected_count > expected_count:
                comment_sync_status["expected_count_raw"] = expected_count
                comment_sync_status["expected_count"] = effective_expected_count
                payload["comment_sync_status"] = comment_sync_status
    payload["up_to_date"] = bool(payload.get("up_to_date", coverage.get("up_to_date")))
    return payload


def _build_coverage_by_dimension(snapshot: dict[str, Any]) -> dict[str, Any]:
    comments_coverage = dict(snapshot.get("comments_coverage") or {})
    total_saved_comments = int(comments_coverage.get("total_saved_comments") or 0)
    total_reported_comments = int(comments_coverage.get("total_reported_comments") or 0)
    effective_total_reported_comments = max(total_saved_comments, total_reported_comments)
    if effective_total_reported_comments > total_reported_comments:
        comments_coverage["total_reported_comments_raw"] = total_reported_comments
        comments_coverage["total_reported_comments"] = effective_total_reported_comments
    total_comment_sync_status = (
        dict(comments_coverage.get("comment_sync_status"))
        if isinstance(comments_coverage.get("comment_sync_status"), dict)
        else {}
    )
    if total_comment_sync_status:
        expected_count = int(total_comment_sync_status.get("expected_count") or 0)
        fetched_count = int(total_comment_sync_status.get("fetched_count") or 0)
        upserted_count = int(total_comment_sync_status.get("upserted_count") or 0)
        effective_expected_count = max(expected_count, fetched_count, upserted_count)
        if effective_expected_count > expected_count:
            total_comment_sync_status["expected_count_raw"] = expected_count
            total_comment_sync_status["expected_count"] = effective_expected_count
            comments_coverage["comment_sync_status"] = total_comment_sync_status
    return {
        "comments": comments_coverage,
        "media": dict(snapshot.get("asset_coverage") or {}),
        "comment_media": dict(snapshot.get("comment_media_coverage") or {}),
        "avatars": dict(snapshot.get("avatar_coverage") or {}),
    }


def _build_follow_up_breakdown(snapshot: dict[str, Any]) -> dict[str, int]:
    return {
        "comments": int(snapshot.get("comment_target_count") or snapshot.get("targeted_anchor_count") or 0),
        "media": int(snapshot.get("detail_target_count") or snapshot.get("missing_asset_count") or 0),
        "avatars": int(snapshot.get("avatar_target_count") or snapshot.get("missing_avatar_count") or 0),
        "comment_media": int(
            snapshot.get("comment_media_target_count") or snapshot.get("missing_comment_media_count") or 0
        ),
    }


def _queue_wait_state(worker_health: dict[str, Any] | None) -> str:
    if not isinstance(worker_health, dict):
        return "unknown"
    if not bool(worker_health.get("queue_enabled")):
        return "queue_disabled"
    if not bool(worker_health.get("healthy")) and int(worker_health.get("healthy_workers") or 0) <= 0:
        return "waiting_for_workers"
    oldest_queued_age_seconds = int(worker_health.get("oldest_queued_age_seconds") or 0)
    if oldest_queued_age_seconds >= 300:
        return "backlogged"
    return "ready"


def _infer_worker_version(worker_health: dict[str, Any] | None, *, platform: str) -> str | None:
    if not isinstance(worker_health, dict):
        return None
    for worker in worker_health.get("workers") or []:
        if not isinstance(worker, dict):
            continue
        supported_platforms = [str(value or "").strip().lower() for value in worker.get("supported_platforms") or []]
        if supported_platforms and platform not in supported_platforms:
            continue
        metadata = worker.get("metadata") if isinstance(worker.get("metadata"), dict) else {}
        for key in ("worker_version", "image_version", "release_version", "git_sha"):
            value = str(metadata.get(key) or "").strip()
            if value:
                return value
    return None


def _build_platform_diagnostics(
    *,
    platforms: list[str],
    sync_config: dict[str, Any],
    snapshot: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    social_repo = _social_repo()
    worker_health = social_repo.get_worker_health() if hasattr(social_repo, "get_worker_health") else None
    queue_enabled = bool(social_repo.is_queue_enabled()) if hasattr(social_repo, "is_queue_enabled") else False
    queue_state = _queue_wait_state(worker_health)
    coverage_by_dimension = _build_coverage_by_dimension(snapshot)
    follow_up_breakdown = _build_follow_up_breakdown(snapshot)
    diagnostics: dict[str, Any] = {}
    for platform in platforms:
        auth_mode, auth_status_reason = _infer_auth_mode(platform)
        execution_path = _derive_execution_path(platform=platform, auth_mode=auth_mode)
        warnings: list[str] = []
        if platform in {"tiktok", "instagram"}:
            warnings.append("remote_only")
        if platform == "tiktok" and not bool(sync_config.get("fetch_replies", True)):
            warnings.append("replies_skipped_by_profile")
        if platform in {"facebook", "threads"} and auth_status_reason:
            warnings.append("degraded_public_mode")
        queue_cap = (
            social_repo._modal_dispatch_platform_cap("posts", platform)  # noqa: SLF001
            if hasattr(social_repo, "_modal_dispatch_platform_cap")
            else None
        )
        diagnostics[platform] = {
            "platform": platform,
            "auth_mode": auth_mode,
            "auth_status_reason": auth_status_reason,
            "execution_path": execution_path,
            "queue_cap": queue_cap,
            "queue_wait_state": queue_state,
            "queue_age_seconds": (
                int(worker_health.get("oldest_queued_age_seconds") or 0) if isinstance(worker_health, dict) else None
            ),
            "queue_enabled": queue_enabled,
            "worker_required": platform in {"instagram", "tiktok"},
            "remote_only": platform in {"instagram", "tiktok"},
            "source_mode": _derive_source_mode(platform=platform, sync_config=sync_config),
            "coverage_by_dimension": {
                dimension: _coverage_dimension_payload(dimension, coverage, platform=platform)
                for dimension, coverage in coverage_by_dimension.items()
            },
            "follow_up_breakdown": dict(follow_up_breakdown),
            "worker_version": _infer_worker_version(worker_health, platform=platform),
            "warnings": warnings,
        }
    return diagnostics, worker_health


def _aggregate_platform_field(platform_diagnostics: dict[str, Any], field: str) -> Any:
    values = []
    for payload in platform_diagnostics.values():
        if not isinstance(payload, dict):
            continue
        value = payload.get(field)
        if value in (None, "", []):
            continue
        values.append(value)
    if not values:
        return None
    first = values[0]
    return first if all(value == first for value in values) else "mixed"


def _build_missing_detail_target_groups(
    *,
    season_id: str,
    platforms: list[str],
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
) -> dict[str, dict[str, list[str]]]:
    social_repo = _social_repo()
    target_accounts_by_platform = social_repo._target_accounts_by_platform(  # noqa: SLF001
        season_id,
        source_scope=source_scope,
        context=social_repo.get_season_context(season_id),
    )
    completed_avatar_rows = (
        pg.fetch_all(
            """
            select platform, account_handle, source_url, status
            from social.avatar_registry
            where status in ('mirrored', 'unsupported')
            """
        )
        if social_repo._relation_exists("social.avatar_registry")  # noqa: SLF001
        else []
    )
    completed_avatar_keys = {
        (
            str(row.get("platform") or "").strip().lower(),
            str(row.get("account_handle") or "").strip().lstrip("@").lower(),
            str(row.get("source_url") or "").strip(),
        )
        for row in completed_avatar_rows
    }
    completed_avatar_handles = {
        (
            str(row.get("platform") or "").strip().lower(),
            str(row.get("account_handle") or "").strip().lstrip("@").lower(),
        )
        for row in completed_avatar_rows
    }
    grouped_targets: dict[str, dict[str, set[str]]] = {
        "details": {},
        "assets": {},
        "avatars": {},
        "comment_media": {},
    }

    def _add_group_target(group: str, platform_name: str, source_id: str) -> None:
        normalized_source_id = str(source_id or "").strip()
        if not normalized_source_id:
            return
        platform_targets = grouped_targets.setdefault(group, {})
        platform_targets.setdefault(platform_name, set()).add(normalized_source_id)
        grouped_targets.setdefault("details", {}).setdefault(platform_name, set()).add(normalized_source_id)

    for platform in platforms:
        post_table = social_repo.PLATFORM_POST_TABLES.get(platform)
        source_id_column = social_repo.PLATFORM_SOURCE_ID_COLUMN.get(platform)
        posted_at_column = social_repo.PLATFORM_POSTED_AT_COLUMN.get(platform)
        if not post_table or not source_id_column or not posted_at_column:
            continue
        account_handles = sorted(set((target_accounts_by_platform or {}).get(platform, set())))
        account_expr = (
            "ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.channel_title, ''), '')), '@')"
            if platform == "youtube"
            else "ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@')"
        )
        account_filter = f"and {account_expr} = any(%s)" if account_handles else ""
        params: list[Any] = [season_id, date_start, date_end]
        if account_handles:
            params.append(account_handles)
        rows = pg.fetch_all(
            f"""
            select
              p.{source_id_column}::text as source_id,
              to_jsonb(p) as post_json
            from social.{post_table} p
            where p.season_id = %s::uuid
              and p.{posted_at_column} >= %s
              and p.{posted_at_column} <= %s
              {account_filter}
            order by p.{posted_at_column} desc
            limit 5000
            """,
            params,
        )
        for row in rows:
            source_id = str(row.get("source_id") or "").strip()
            post_json = row.get("post_json") if isinstance(row.get("post_json"), dict) else {}
            if not source_id:
                continue
            post_json["_platform"] = platform
            if social_repo._platform_post_needs_media_mirror(platform, post_json):  # noqa: SLF001
                _add_group_target("assets", platform, source_id)
            avatar_state = social_repo._platform_post_avatar_repair_state(platform, post_json)  # noqa: SLF001
            if bool(avatar_state.get("needs_repair")):
                _add_group_target("avatars", platform, source_id)
            if platform == "instagram":
                hosted_tagged = (
                    post_json.get("hosted_tagged_profile_pics")
                    if isinstance(post_json.get("hosted_tagged_profile_pics"), dict)
                    else {}
                )
                mention_gap = False
                for mention in social_repo._as_text_list(  # noqa: SLF001
                    post_json.get("mentions"),
                    prefix="@",
                    strip_prefix="@",
                ):
                    mention_handle = str(mention or "").strip().lstrip("@").lower()
                    if not mention_handle:
                        continue
                    mention_hosted = str((hosted_tagged or {}).get(mention_handle) or "").strip()
                    if mention_hosted:
                        continue
                    if (platform, mention_handle) not in completed_avatar_handles:
                        mention_gap = True
                        break
                if mention_gap:
                    _add_group_target("avatars", platform, source_id)
        comment_media_rows: list[dict[str, Any]] = []
        if platform == "instagram" and social_repo._column_exists("social", "instagram_comments", "media_urls"):  # noqa: SLF001
            comment_media_rows = pg.fetch_all(
                f"""
                select distinct p.shortcode::text as source_id
                from social.instagram_comments c
                join social.instagram_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  {account_filter}
                  and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
                  and (
                    coalesce(c.media_mirror_status, '') in ('pending', 'partial', 'failed')
                    or jsonb_array_length(coalesce(c.hosted_media_urls, '[]'::jsonb))
                       < jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb))
                  )
                limit 5000
                """,
                params,
            )
        elif platform == "tiktok":
            comment_media_rows = pg.fetch_all(
                f"""
                select distinct p.video_id::text as source_id
                from social.tiktok_comments c
                join social.tiktok_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  {account_filter}
                  and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
                  and (
                    coalesce(c.media_mirror_status, '') in ('pending', 'partial', 'failed')
                    or jsonb_array_length(coalesce(c.hosted_media_urls, '[]'::jsonb))
                       < jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb))
                  )
                limit 5000
                """,
                params,
            )
        elif platform == "twitter":
            comment_media_rows = pg.fetch_all(
                f"""
                select distinct root.tweet_id::text as source_id
                from social.twitter_tweets c
                join social.twitter_tweets root
                  on root.tweet_id = coalesce(c.reply_to_tweet_id, c.quoted_tweet_id)
                 and root.is_reply = false
                where root.season_id = %s::uuid
                  and root.{posted_at_column} >= %s
                  and root.{posted_at_column} <= %s
                  {account_filter.replace("p.", "root.")}
                  and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
                  and (
                    coalesce(c.media_mirror_status, '') in ('pending', 'partial', 'failed')
                    or jsonb_array_length(coalesce(c.hosted_media_urls, '[]'::jsonb))
                       < jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb))
                  )
                limit 5000
                """,
                params,
            )
        elif platform == "facebook":
            comment_media_rows = pg.fetch_all(
                f"""
                select distinct p.post_id::text as source_id
                from social.facebook_comments c
                join social.facebook_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  {account_filter}
                  and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
                  and (
                    coalesce(c.media_mirror_status, '') in ('pending', 'partial', 'failed')
                    or jsonb_array_length(coalesce(c.hosted_media_urls, '[]'::jsonb))
                       < jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb))
                  )
                limit 5000
                """,
                params,
            )
        elif platform == "threads":
            comment_media_rows = pg.fetch_all(
                f"""
                select distinct p.post_id::text as source_id
                from social.meta_threads_comments c
                join social.meta_threads_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  {account_filter}
                  and jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb)) > 0
                  and (
                    coalesce(c.media_mirror_status, '') in ('pending', 'partial', 'failed')
                    or jsonb_array_length(coalesce(c.hosted_media_urls, '[]'::jsonb))
                       < jsonb_array_length(coalesce(c.media_urls, '[]'::jsonb))
                  )
                limit 5000
                """,
                params,
            )
        if platform == "instagram" and social_repo._column_exists(  # noqa: SLF001
            "social",
            "instagram_comments",
            "author_profile_pic_url",
        ):
            verified_avatar_rows = pg.fetch_all(
                f"""
                select distinct
                  p.shortcode::text as source_id,
                  c.username,
                  c.author_profile_pic_url
                from social.instagram_comments c
                join social.instagram_posts p on p.id = c.post_id
                where p.season_id = %s::uuid
                  and p.{posted_at_column} >= %s
                  and p.{posted_at_column} <= %s
                  {account_filter}
                  and coalesce(c.author_is_verified, false) = true
                  and coalesce(nullif(c.username, ''), '') <> ''
                limit 5000
                """,
                params,
            )
            for avatar_row in verified_avatar_rows:
                root_source_id = str(avatar_row.get("source_id") or "").strip()
                avatar_handle = str(avatar_row.get("username") or "").strip().lstrip("@").lower()
                avatar_source = str(avatar_row.get("author_profile_pic_url") or "").strip()
                if not root_source_id or not avatar_handle:
                    continue
                if avatar_source:
                    if (platform, avatar_handle, avatar_source) not in completed_avatar_keys and (
                        platform,
                        avatar_handle,
                    ) not in completed_avatar_handles:
                        _add_group_target("avatars", platform, root_source_id)
                elif (platform, avatar_handle) not in completed_avatar_handles:
                    _add_group_target("avatars", platform, root_source_id)
        for comment_row in comment_media_rows:
            _add_group_target("comment_media", platform, str(comment_row.get("source_id") or "").strip())
    return {
        group: {
            platform_name: sorted(source_ids) for platform_name, source_ids in platform_targets.items() if source_ids
        }
        for group, platform_targets in grouped_targets.items()
    }


def _build_missing_detail_targets(
    *,
    season_id: str,
    platforms: list[str],
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
) -> dict[str, list[str]]:
    return _build_missing_detail_target_groups(
        season_id=season_id,
        platforms=platforms,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )["details"]


def _build_missing_comment_targets(
    *,
    season_id: str,
    platforms: list[str],
    source_scope: str,
    date_start: datetime,
    date_end: datetime,
) -> dict[str, list[str]]:
    social_repo = _social_repo()
    target_accounts_by_platform = social_repo._target_accounts_by_platform(  # noqa: SLF001
        season_id,
        source_scope=source_scope,
        context=social_repo.get_season_context(season_id),
    )
    source_id_column_by_platform = social_repo.PLATFORM_SOURCE_ID_COLUMN
    posted_at_column_by_platform = social_repo.PLATFORM_POSTED_AT_COLUMN
    post_table_by_platform = social_repo.PLATFORM_POST_TABLES
    comment_table_by_platform = social_repo.PLATFORM_COMMENT_TABLES
    targets: dict[str, list[str]] = {}
    for platform in platforms:
        post_table = post_table_by_platform.get(platform)
        source_id_column = source_id_column_by_platform.get(platform)
        posted_at_column = posted_at_column_by_platform.get(platform)
        comment_table = comment_table_by_platform.get(platform)
        if not post_table or not source_id_column or not posted_at_column or not comment_table:
            continue
        account_handles = sorted(set((target_accounts_by_platform or {}).get(platform, set())))
        account_expr = (
            "ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.channel_title, ''), '')), '@')"
            if platform == "youtube"
            else "ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@')"
        )
        account_filter = f"and {account_expr} = any(%s)" if account_handles else ""
        params: list[Any] = [season_id, date_start, date_end]
        if account_handles:
            params.append(account_handles)
        if platform == "twitter":
            expected_expr = "coalesce(p.replies_count, 0) + coalesce(p.quotes, 0)"
            comment_count_sql = """
              left join (
                select coalesce(reply_to_tweet_id, quoted_tweet_id) as root_source_id, count(*)::int as saved_comments
                from social.twitter_tweets
                where season_id = %s::uuid
                  and (is_reply = true or is_quote = true)
                group by 1
              ) cc on cc.root_source_id = p.tweet_id
            """
            params = [season_id, season_id, date_start, date_end] + ([account_handles] if account_handles else [])
        elif platform == "threads":
            expected_expr = "coalesce(p.replies_count, 0) + coalesce(p.quotes, 0)"
            comment_count_sql = f"""
              left join (
                select post_id as join_post_id, count(*)::int as saved_comments
                from social.{comment_table}
                where season_id = %s::uuid
                group by 1
              ) cc on cc.join_post_id = p.id
            """
            params = [season_id, season_id, date_start, date_end] + ([account_handles] if account_handles else [])
        else:
            expected_expr = "coalesce(p.comments_count, 0)"
            join_key = "video_id" if platform == "youtube" else "post_id"
            comment_count_sql = f"""
              left join (
                select {join_key} as join_post_id, count(*)::int as saved_comments
                from social.{comment_table}
                where season_id = %s::uuid
                group by 1
              ) cc on cc.join_post_id = p.id
            """
            params = [season_id, season_id, date_start, date_end] + ([account_handles] if account_handles else [])
        rows = pg.fetch_all(
            f"""
            select
              p.{source_id_column}::text as source_id
            from social.{post_table} p
            {comment_count_sql}
            where p.season_id = %s::uuid
              and p.{posted_at_column} >= %s
              and p.{posted_at_column} <= %s
              {account_filter}
              and coalesce(cc.saved_comments, 0) < {expected_expr}
            order by {expected_expr} desc, p.{source_id_column} asc
            limit 5000
            """,
            params,
        )
        source_ids = [
            str(row.get("source_id") or "").strip() for row in rows if str(row.get("source_id") or "").strip()
        ]
        if source_ids:
            targets[platform] = source_ids
    return targets


def _next_pass_kind_from_snapshot(snapshot: dict[str, Any]) -> SyncPassKind | None:
    comments = snapshot.get("comments_coverage") if isinstance(snapshot.get("comments_coverage"), dict) else {}
    assets = snapshot.get("asset_coverage") if isinstance(snapshot.get("asset_coverage"), dict) else {}
    comment_media = (
        snapshot.get("comment_media_coverage") if isinstance(snapshot.get("comment_media_coverage"), dict) else {}
    )
    avatars = snapshot.get("avatar_coverage") if isinstance(snapshot.get("avatar_coverage"), dict) else {}
    if not bool(comments.get("up_to_date")):
        return "comments_only"
    if (
        not bool(assets.get("up_to_date"))
        or not bool(comment_media.get("up_to_date"))
        or not bool(avatars.get("up_to_date"))
    ):
        return "details_refresh"
    return None


def _follow_up_dimensions_from_snapshot(
    *,
    comments_coverage: dict[str, Any],
    asset_coverage: dict[str, Any],
    comment_media_coverage: dict[str, Any],
    avatar_coverage: dict[str, Any],
) -> list[str]:
    follow_up_dimensions: list[str] = []
    if not bool(comments_coverage.get("up_to_date")):
        follow_up_dimensions.append("comments")
    if not bool(asset_coverage.get("up_to_date")):
        follow_up_dimensions.append("media")
    if not bool(comment_media_coverage.get("up_to_date")):
        follow_up_dimensions.append("comment_media")
    if not bool(avatar_coverage.get("up_to_date")):
        follow_up_dimensions.append("avatars")
    return follow_up_dimensions


def _expected_after_current_pass(pass_kind: str | None) -> str | None:
    normalized_pass_kind = str(pass_kind or "").strip().lower()
    if normalized_pass_kind == "posts_and_comments":
        return (
            "Posts and comments are acquired here; media, comment media, avatars, and detail repairs can still remain."
        )
    if normalized_pass_kind == "comments_only":
        return "Only comment and reply gaps should close in this pass."
    if normalized_pass_kind == "details_refresh":
        return "Media, comment media, avatar, and detail gaps should close in this pass."
    return None


def _display_status_for_sync_session(
    *,
    status: str,
    snapshot: dict[str, Any],
    next_pass_kind: SyncPassKind | None,
) -> tuple[str, str | None]:
    normalized_status = str(status or "").strip().lower()
    follow_up_dimensions = list(snapshot.get("follow_up_dimensions") or [])
    if bool(snapshot.get("up_to_date")):
        return "Complete", "All required coverage dimensions are complete."
    if normalized_status in {"failed"}:
        if follow_up_dimensions:
            return "Blocked", f"Follow-up work is still missing for {', '.join(follow_up_dimensions)}."
        return "Blocked", "The sync session stopped before all required coverage dimensions were complete."
    if normalized_status in {"cancelled"}:
        return "Cancelled", "The sync session was cancelled before all follow-up work finished."
    if normalized_status in {"cancelling"}:
        return "Cancelling", "Cancellation is in progress."
    if follow_up_dimensions and next_pass_kind is not None:
        return "Follow-up needed", f"Further {', '.join(follow_up_dimensions)} work remains after this pass."
    if normalized_status in {"initializing", "pass_running", "pass_evaluating", "completing"}:
        return "Running", None
    return str(status or "").strip().replace("_", " ").title() or "Running", None


def _start_sync_pass(
    sync_session_id: str,
    *,
    pass_kind: SyncPassKind,
    pass_attempt: int,
    pass_sequence: int,
    follow_up_reason: str,
) -> dict[str, Any]:
    social_repo = _social_repo()
    session = _fetch_sync_session_row(sync_session_id)
    if session is None:
        raise ValueError("sync_session_not_found")
    sync_config = dict(session.get("sync_config") or {})
    season_id = str(session.get("season_id") or "")
    source_scope = str(session.get("source_scope") or "bravo")
    date_start = _coerce_dt(session.get("date_start"))
    date_end = _coerce_dt(session.get("date_end"))
    if not season_id or date_start is None or date_end is None:
        raise ValueError("invalid_sync_session")
    platforms = _normalize_platforms(session.get("platforms"))
    source_ids_by_platform: dict[str, list[str]] | None = None
    if pass_kind == "comments_only":
        source_ids_by_platform = _build_missing_comment_targets(
            season_id=season_id,
            platforms=platforms,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
    elif pass_kind == "details_refresh":
        source_ids_by_platform = _build_missing_detail_targets(
            season_id=season_id,
            platforms=platforms,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
        )
    pass_config = _base_config_for_pass(
        sync_config=sync_config,
        pass_kind=pass_kind,
        source_ids_by_platform=source_ids_by_platform,
    )
    orchestration_metadata = {
        "sync_session_id": sync_session_id,
        "pass_kind": pass_kind,
        "pass_attempt": int(pass_attempt),
        "pass_sequence": int(pass_sequence),
        "follow_up_reason": follow_up_reason,
    }
    run_payload = social_repo.ingest_season(
        season_id,
        platforms=platforms,
        accounts_override=list(pass_config.get("accounts_override") or []),
        hashtags_override=list(pass_config.get("hashtags_override") or []),
        keywords_override=list(pass_config.get("keywords_override") or []),
        source_scope=source_scope,
        sync_strategy=str(pass_config.get("sync_strategy") or "incremental"),
        max_posts_per_target=int(pass_config.get("max_posts_per_target") or 0),
        max_comments_per_post=int(pass_config.get("max_comments_per_post") or 0),
        max_replies_per_post=int(pass_config.get("max_replies_per_post") or 0),
        fetch_replies=bool(pass_config.get("fetch_replies", True)),
        ingest_mode=pass_kind,
        date_start=date_start,
        date_end=date_end,
        comment_refresh_policy=str(pass_config.get("comment_refresh_policy") or "balanced"),
        comment_anchor_source_ids=source_ids_by_platform,
        sound_ids=list(pass_config.get("sound_ids") or []),
        runner_strategy=str(pass_config.get("runner_strategy") or "single_runner"),
        runner_count=int(pass_config.get("runner_count") or 1),
        window_shard_hours=int(pass_config.get("window_shard_hours") or 12),
        runner_b_start_offset_hours=pass_config.get("runner_b_start_offset_hours"),
        day_weight_profile=pass_config.get("day_weight_profile"),
        priority_mode=pass_config.get("priority_mode"),
        youtube_source_mode=pass_config.get("youtube_source_mode"),
        youtube_force_reindex=bool(pass_config.get("youtube_force_reindex")),
        youtube_force_media_refresh=bool(pass_config.get("youtube_force_media_refresh")),
        youtube_force_comment_refresh=bool(pass_config.get("youtube_force_comment_refresh")),
        client_session_id=str(session.get("client_session_id") or "").strip() or None,
        client_workflow_id=str(session.get("client_workflow_id") or "").strip() or None,
        orchestration_metadata=orchestration_metadata,
        initiated_by=str(session.get("initiated_by") or "").strip() or None,
        inline_worker_id=None,
        week_index=None,
        window_timezone="America/New_York",
        run_scope_label="Sync Session",
    )
    run_id = str(run_payload.get("run_id") or "").strip()
    history = _append_pass_history(
        session.get("pass_history") if isinstance(session.get("pass_history"), list) else [],
        pass_kind=pass_kind,
        pass_attempt=pass_attempt,
        pass_sequence=pass_sequence,
        run_id=run_id,
        follow_up_reason=follow_up_reason,
    )
    updated = _update_sync_session(
        sync_session_id,
        status="pass_running",
        current_pass_kind=pass_kind,
        current_pass_attempt=int(pass_attempt),
        current_run_id=run_id,
        pass_sequence=int(pass_sequence),
        pass_history=history,
        follow_up_reason=follow_up_reason,
        next_pass_available_at=None,
        started_at=session.get("started_at") or _now_utc(),
    )
    if not social_repo.is_queue_enabled():
        Thread(
            target=social_repo.execute_run,
            args=(run_id,),
            kwargs={"worker_id": f"sync-session:inline:{pass_kind}:{pass_sequence}"},
            daemon=True,
        ).start()
    return {
        "sync_session_id": sync_session_id,
        "run_id": run_id,
        "pass_kind": pass_kind,
        "pass_attempt": int(pass_attempt),
        "pass_sequence": int(pass_sequence),
        "session": updated,
    }


def _serialize_sync_session(row: dict[str, Any]) -> dict[str, Any]:
    current_run_id = str(row.get("current_run_id") or "").strip() or None
    completeness_snapshot = dict(row.get("completeness_snapshot") or {})
    sync_config = dict(row.get("sync_config") or {})
    platforms = _normalize_platforms(row.get("platforms"))
    next_pass_kind = _next_pass_kind_from_snapshot(completeness_snapshot)
    display_status, status_reason = _display_status_for_sync_session(
        status=str(row.get("status") or ""),
        snapshot=completeness_snapshot,
        next_pass_kind=next_pass_kind,
    )
    platform_diagnostics, worker_health = _build_platform_diagnostics(
        platforms=platforms,
        sync_config=sync_config,
        snapshot=completeness_snapshot,
    )
    coverage_by_dimension = _build_coverage_by_dimension(completeness_snapshot)
    follow_up_breakdown = _build_follow_up_breakdown(completeness_snapshot)
    payload = {
        "sync_session_id": str(row.get("id") or ""),
        "season_id": str(row.get("season_id") or ""),
        "show_id": str(row.get("show_id") or ""),
        "source_scope": str(row.get("source_scope") or ""),
        "platforms": platforms,
        "date_start": _iso(_coerce_dt(row.get("date_start"))),
        "date_end": _iso(_coerce_dt(row.get("date_end"))),
        "status": str(row.get("status") or ""),
        "current_pass_kind": str(row.get("current_pass_kind") or "") or None,
        "current_pass_attempt": int(row.get("current_pass_attempt") or 0),
        "current_run_id": current_run_id,
        "pass_sequence": int(row.get("pass_sequence") or 0),
        "pass_history": list(row.get("pass_history") or []),
        "follow_up_reason": str(row.get("follow_up_reason") or "") or None,
        "display_status": display_status,
        "status_reason": status_reason,
        "follow_up_dimensions": list(completeness_snapshot.get("follow_up_dimensions") or []),
        "next_pass_kind": next_pass_kind,
        "expected_after_current_pass": _expected_after_current_pass(str(row.get("current_pass_kind") or "")),
        "completeness_snapshot": completeness_snapshot,
        "sync_config": sync_config,
        "platform_diagnostics": platform_diagnostics,
        "coverage_by_dimension": coverage_by_dimension,
        "follow_up_breakdown": follow_up_breakdown,
        "auth_mode": _aggregate_platform_field(platform_diagnostics, "auth_mode"),
        "execution_path": _aggregate_platform_field(platform_diagnostics, "execution_path"),
        "queue_cap": _aggregate_platform_field(platform_diagnostics, "queue_cap"),
        "queue_wait_state": _queue_wait_state(worker_health),
        "source_mode": _aggregate_platform_field(platform_diagnostics, "source_mode"),
        "worker_version": _aggregate_platform_field(platform_diagnostics, "worker_version"),
        "worker_health": worker_health,
        "created_at": _iso(_coerce_dt(row.get("created_at"))),
        "started_at": _iso(_coerce_dt(row.get("started_at"))),
        "completed_at": _iso(_coerce_dt(row.get("completed_at"))),
        "cancelled_at": _iso(_coerce_dt(row.get("cancelled_at"))),
        "updated_at": _iso(_coerce_dt(row.get("updated_at"))),
    }
    payload["current_run"] = _current_run_payload(current_run_id)
    return payload


def create_sync_session(
    season_id: str,
    *,
    source_scope: str,
    platforms: list[str] | None,
    date_start: datetime,
    date_end: datetime,
    config: dict[str, Any],
    initiated_by: str | None = None,
) -> dict[str, Any]:
    if not _sync_sessions_ready():
        raise ValueError("sync_sessions_schema_missing")
    social_repo = _social_repo()
    context = social_repo.get_season_context(season_id)
    normalized_start = _coerce_dt(date_start)
    normalized_end = _coerce_dt(date_end)
    if normalized_start is None or normalized_end is None:
        raise ValueError("date_window_required")
    if normalized_end < normalized_start:
        normalized_end = normalized_start
    sync_config = build_sync_profile(
        season_id=season_id,
        source_scope=source_scope,
        date_start=normalized_start,
        date_end=normalized_end,
        base_config={**config, "platforms": _normalize_platforms(platforms)},
    )
    dedup_key = _build_dedup_key(
        season_id=season_id,
        source_scope=source_scope,
        date_start=normalized_start,
        date_end=normalized_end,
        config=sync_config,
    )
    attached = pg.fetch_one(
        """
        select id::text as id
        from social.sync_sessions
        where season_id = %s::uuid
          and dedup_key = %s
          and status = any(%s)
        order by created_at desc
        limit 1
        """,
        [season_id, dedup_key, list(SYNC_ACTIVE_STATUSES)],
    )
    if attached and attached.get("id"):
        session = get_sync_session(str(attached["id"]))
        payload = dict(session)
        payload["status"] = "attached"
        return payload
    completeness_snapshot = _build_completeness_snapshot(
        season_id=season_id,
        source_scope=source_scope,
        platforms=_normalize_platforms(sync_config.get("platforms")),
        date_start=normalized_start,
        date_end=normalized_end,
    )
    if bool(completeness_snapshot.get("up_to_date")):
        return {
            "status": "already_up_to_date",
            "sync_session_id": None,
            "season_id": season_id,
            "show_id": context.show_id,
            "source_scope": source_scope,
            "platforms": _normalize_platforms(sync_config.get("platforms")),
            "date_start": _iso(normalized_start),
            "date_end": _iso(normalized_end),
            "completeness_snapshot": completeness_snapshot,
            "current_run_id": None,
        }
    sync_session_id = str(uuid4())
    row = pg.fetch_one(
        """
        insert into social.sync_sessions (
          id,
          season_id,
          show_id,
          source_scope,
          platforms,
          date_start,
          date_end,
          dedup_key,
          status,
          sync_config,
          completeness_snapshot,
          initiated_by,
          client_session_id,
          client_workflow_id
        )
        values (
          %s::uuid,
          %s::uuid,
          %s::uuid,
          %s,
          %s,
          %s,
          %s,
          %s,
          'initializing',
          %s::jsonb,
          %s::jsonb,
          %s,
          %s,
          %s
        )
        returning id::text
        """,
        [
            sync_session_id,
            season_id,
            context.show_id,
            source_scope,
            _normalize_platforms(sync_config.get("platforms")),
            normalized_start,
            normalized_end,
            dedup_key,
            json.dumps(sync_config),
            json.dumps(completeness_snapshot),
            initiated_by,
            str(sync_config.get("client_session_id") or "").strip() or None,
            str(sync_config.get("client_workflow_id") or "").strip() or None,
        ],
    )
    if not row:
        raise RuntimeError("Failed to create sync session")
    kickoff = _start_sync_pass(
        sync_session_id,
        pass_kind="posts_and_comments",
        pass_attempt=1,
        pass_sequence=1,
        follow_up_reason="initial_start",
    )
    session = get_sync_session(sync_session_id)
    payload = dict(session)
    payload["status"] = "created"
    payload["current_run_id"] = kickoff.get("run_id")
    return payload


def get_sync_session(sync_session_id: str) -> dict[str, Any]:
    row = _fetch_sync_session_row(sync_session_id)
    if row is None:
        raise ValueError("sync_session_not_found")
    status = str(row.get("status") or "").strip().lower()
    if status not in SYNC_TERMINAL_STATUSES:
        date_start = _coerce_dt(row.get("date_start"))
        date_end = _coerce_dt(row.get("date_end"))
        if date_start is not None and date_end is not None:
            row = dict(row)
            row["completeness_snapshot"] = _build_completeness_snapshot(
                season_id=str(row.get("season_id") or ""),
                source_scope=str(row.get("source_scope") or "bravo"),
                platforms=_normalize_platforms(row.get("platforms")),
                date_start=date_start,
                date_end=date_end,
            )
    return _serialize_sync_session(row)


def evaluate_sync_session(sync_session_id: str) -> dict[str, Any]:
    row = _fetch_sync_session_row(sync_session_id)
    if row is None:
        raise ValueError("sync_session_not_found")
    if str(row.get("status") or "") in SYNC_TERMINAL_STATUSES:
        return _serialize_sync_session(row)
    lock_key, locked = _fetch_sync_session_lock(sync_session_id)
    if not locked:
        return _serialize_sync_session(row)
    try:
        row = _fetch_sync_session_row(sync_session_id)
        if row is None:
            raise ValueError("sync_session_not_found")
        status = str(row.get("status") or "")
        if status in SYNC_TERMINAL_STATUSES:
            return _serialize_sync_session(row)
        current_run = _current_run_payload(str(row.get("current_run_id") or ""))
        if current_run is None:
            if status == "cancelling":
                row = _update_sync_session(
                    sync_session_id,
                    status="cancelled",
                    cancelled_at=_now_utc(),
                    follow_up_reason="cancelled",
                )
            return _serialize_sync_session(row)
        run_status = str(current_run.get("status") or "").strip().lower()
        active_run_statuses = {"queued", "pending", "retrying", "running"}
        now_utc = _now_utc()
        next_pass_available_at = _coerce_dt(row.get("next_pass_available_at"))
        if run_status in active_run_statuses:
            return _serialize_sync_session(row)
        if next_pass_available_at is not None and next_pass_available_at > now_utc:
            return _serialize_sync_session(row)

        session_platforms = _normalize_platforms(row.get("platforms"))
        date_start = _coerce_dt(row.get("date_start"))
        date_end = _coerce_dt(row.get("date_end"))
        if date_start is None or date_end is None:
            raise ValueError("invalid_sync_session")
        completeness_snapshot = _build_completeness_snapshot(
            season_id=str(row.get("season_id") or ""),
            source_scope=str(row.get("source_scope") or "bravo"),
            platforms=session_platforms,
            date_start=date_start,
            date_end=date_end,
        )
        if completeness_snapshot.get("up_to_date"):
            row = _update_sync_session(
                sync_session_id,
                status="completed",
                completeness_snapshot=completeness_snapshot,
                completed_at=now_utc,
                follow_up_reason="coverage_complete",
            )
            return _serialize_sync_session(row)

        current_pass_kind = str(row.get("current_pass_kind") or "").strip().lower() or "posts_and_comments"
        current_pass_attempt = int(row.get("current_pass_attempt") or 1)
        next_pass_kind = _next_pass_kind_from_snapshot(completeness_snapshot)
        if run_status == "cancelled" or status == "cancelling":
            row = _update_sync_session(
                sync_session_id,
                status="cancelled",
                completeness_snapshot=completeness_snapshot,
                cancelled_at=now_utc,
                follow_up_reason="cancelled",
            )
            return _serialize_sync_session(row)

        if run_status == "failed" and _has_retryable_errors(str(current_run.get("id") or "")):
            max_attempts = max(1, int(row.get("max_attempts_per_kind") or 3))
            if current_pass_attempt < max_attempts:
                due_at = now_utc + timedelta(
                    seconds=_retry_backoff_seconds(
                        current_pass_attempt,
                        base_delay_seconds=int(row.get("retry_base_delay_seconds") or 30),
                    )
                )
                row = _update_sync_session(
                    sync_session_id,
                    status="pass_evaluating",
                    completeness_snapshot=completeness_snapshot,
                    next_pass_available_at=due_at,
                    follow_up_reason="retry_scheduled",
                )
                return _serialize_sync_session(row)

        if status == "pass_evaluating" and next_pass_available_at is not None and next_pass_available_at <= now_utc:
            kickoff = _start_sync_pass(
                sync_session_id,
                pass_kind=current_pass_kind,  # retry same pass
                pass_attempt=current_pass_attempt + 1,
                pass_sequence=int(row.get("pass_sequence") or 0) + 1,
                follow_up_reason="retry_after_backoff",
            )
            session = get_sync_session(sync_session_id)
            session["current_run_id"] = kickoff.get("run_id")
            return session

        if next_pass_kind and next_pass_kind != current_pass_kind:
            kickoff = _start_sync_pass(
                sync_session_id,
                pass_kind=next_pass_kind,
                pass_attempt=1,
                pass_sequence=int(row.get("pass_sequence") or 0) + 1,
                follow_up_reason="coverage_gap",
            )
            session = get_sync_session(sync_session_id)
            session["current_run_id"] = kickoff.get("run_id")
            return session

        final_status = "failed" if run_status == "failed" else "completed"
        row = _update_sync_session(
            sync_session_id,
            status=final_status,
            completeness_snapshot=completeness_snapshot,
            completed_at=now_utc if final_status == "completed" else None,
            follow_up_reason="incomplete_after_passes" if final_status == "failed" else "completed",
        )
        return _serialize_sync_session(row)
    finally:
        _release_sync_session_lock(lock_key)


def tick_sync_orchestrator(*, limit: int = 20) -> dict[str, Any]:
    if not _sync_sessions_ready():
        return {"evaluated_sessions": 0, "sync_session_ids": []}
    safe_limit = max(1, min(int(limit), 100))
    rows = pg.fetch_all(
        """
        select id::text as id
        from social.sync_sessions
        where status = any(%s)
          and (
            next_pass_available_at is null
            or next_pass_available_at <= now()
          )
        order by created_at asc
        limit %s
        """,
        [list(SYNC_ACTIVE_STATUSES), safe_limit],
    )
    evaluated: list[str] = []
    for row in rows:
        sync_session_id = str(row.get("id") or "").strip()
        if not sync_session_id:
            continue
        try:
            evaluate_sync_session(sync_session_id)
            evaluated.append(sync_session_id)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to evaluate sync session %s", sync_session_id)
    return {"evaluated_sessions": len(evaluated), "sync_session_ids": evaluated}


def cancel_sync_session(
    season_id: str,
    sync_session_id: str,
    *,
    cancelled_by: str | None = None,
) -> dict[str, Any]:
    row = _fetch_sync_session_row(sync_session_id)
    if row is None or str(row.get("season_id") or "") != season_id:
        raise ValueError("sync_session_not_found")
    current_run_id = str(row.get("current_run_id") or "").strip()
    _update_sync_session(
        sync_session_id,
        status="cancelling",
        follow_up_reason="cancel_requested",
        cancelled_at=_now_utc(),
    )
    if current_run_id:
        social_repo = _social_repo()
        social_repo.cancel_run(season_id, current_run_id, cancelled_by=cancelled_by)
    return get_sync_session(sync_session_id)


def retry_sync_session(
    season_id: str,
    sync_session_id: str,
    *,
    retry_kind: str,
    initiated_by: str | None = None,
) -> dict[str, Any]:
    row = _fetch_sync_session_row(sync_session_id)
    if row is None or str(row.get("season_id") or "") != season_id:
        raise ValueError("sync_session_not_found")
    if str(row.get("status") or "") in SYNC_ACTIVE_STATUSES:
        raise ValueError("sync_session_busy")
    pass_kind = SYNC_RETRY_KIND_TO_PASS_KIND.get(str(retry_kind or "").strip().lower())
    if pass_kind is None:
        raise ValueError("unsupported_retry_kind")
    if initiated_by and not row.get("initiated_by"):
        row = _update_sync_session(sync_session_id, initiated_by=initiated_by)
    kickoff = _start_sync_pass(
        sync_session_id,
        pass_kind=pass_kind,
        pass_attempt=1,
        pass_sequence=int(row.get("pass_sequence") or 0) + 1,
        follow_up_reason=str(retry_kind or "").strip().lower() or "manual_retry",
    )
    session = get_sync_session(sync_session_id)
    session["current_run_id"] = kickoff.get("run_id")
    return session
