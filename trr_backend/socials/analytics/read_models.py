# ruff: noqa: F821
"""Analytics read models, exports, and TikTok reporting surfaces."""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def get_week_live_health_snapshot(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
) -> dict[str, Any]:
    _sync_core_overrides()
    context = get_season_context(season_id)
    selected_platforms = _resolve_requested_platforms(platforms)
    now_utc = _now_utc()
    week_windows, _ = _resolve_week_windows(context, timezone=timezone, source_scope=source_scope, now_utc=now_utc)
    window = next((candidate for candidate in week_windows if candidate.week_index == int(week_index)), None)
    if window is None:
        raise ValueError(f"Week {week_index} is not available for this season")

    start_utc = window.start_local.astimezone(UTC)
    end_utc = window.end_local.astimezone(UTC)
    platform_specs = _week_live_health_platform_specs()
    account_map = _target_accounts_by_platform(season_id, source_scope=source_scope, context=context)
    day_account_buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    asset_health: dict[str, dict[str, int]] = {
        "images": {"scraped": 0, "saved": 0},
        "videos": {"scraped": 0, "saved": 0},
        "captions": {"scraped": 0, "saved": 0},
        "profile_pictures": {"scraped": 0, "saved": 0},
    }
    requires_target_accounts = source_scope in {"network", "creator", "news"}

    for platform in selected_platforms:
        spec = platform_specs.get(platform)
        if spec is None:
            continue
        table_name = spec["table"]
        ts_col = spec["ts_col"]
        account_expr = spec["account_expr"]
        comments_expr = spec["comments_expr"]
        platform_accounts = sorted(set(account_map.get(platform, set())))
        if requires_target_accounts and not platform_accounts:
            continue
        account_filter_sql = ""
        params: list[Any] = [timezone, season_id, start_utc, end_utc]
        if source_scope != "community" and platform_accounts:
            account_filter_sql = f" and ltrim(lower({account_expr}), '@') = any(%s)"
            params.append(platform_accounts)
        rows = pg.fetch_all(
            f"""
            select
              timezone(%s, p.{ts_col})::date as day_local,
              ltrim(lower({account_expr}), '@') as account_handle,
              {comments_expr} as comments_count,
              coalesce(nullif(to_jsonb(p)->>'likes', '')::bigint, 0) as likes_count,
              coalesce(to_jsonb(p)->'media_urls', '[]'::jsonb) as media_urls,
              nullif(to_jsonb(p)->>'thumbnail_url', '') as thumbnail_url,
              coalesce(to_jsonb(p)->'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              nullif(to_jsonb(p)->>'hosted_thumbnail_url', '') as hosted_thumbnail_url,
              nullif(to_jsonb(p)->>'caption', '') as caption,
              nullif(to_jsonb(p)->>'description', '') as description,
              nullif(to_jsonb(p)->>'text', '') as text,
              nullif(to_jsonb(p)->>'title', '') as title,
              nullif(to_jsonb(p)->>'owner_profile_pic_url', '') as owner_profile_pic_url,
              nullif(to_jsonb(p)->>'user_avatar_url', '') as user_avatar_url,
              nullif(to_jsonb(p)->>'avatar_url', '') as avatar_url,
              nullif(to_jsonb(p)->>'profile_pic_url', '') as profile_pic_url,
              nullif(to_jsonb(p)->>'hosted_owner_profile_pic_url', '') as hosted_owner_profile_pic_url,
              coalesce(to_jsonb(p)->'hosted_tagged_profile_pics', '{{}}'::jsonb) as hosted_tagged_profile_pics
            from social.{table_name} p
            where p.season_id = %s::uuid
              and p.{ts_col} >= %s
              and p.{ts_col} < %s
              {account_filter_sql}
            """,
            params,
        )

        for row in rows:
            day_local = str(row.get("day_local") or "")
            if not day_local:
                continue
            account_handle = _normalize_account_handle(row.get("account_handle")) or "unknown"
            bucket_key = (day_local, platform, account_handle)
            bucket = day_account_buckets.get(bucket_key)
            if bucket is None:
                bucket = {
                    "day": day_local,
                    "platform": platform,
                    "account": account_handle,
                    "posts": 0,
                    "comments": 0,
                    "likes": 0,
                }
                day_account_buckets[bucket_key] = bucket
            bucket["posts"] += 1
            bucket["comments"] += _normalize_non_negative_int(row.get("comments_count"))
            bucket["likes"] += _normalize_non_negative_int(row.get("likes_count"))

            source_urls = [
                *_week_live_health_to_url_list(row.get("media_urls")),
                str(row.get("thumbnail_url") or "").strip(),
            ]
            hosted_urls = [
                *_week_live_health_to_url_list(row.get("hosted_media_urls")),
                str(row.get("hosted_thumbnail_url") or "").strip(),
            ]
            for source_url in source_urls:
                asset_type = _week_live_health_asset_type(source_url)
                if asset_type:
                    asset_health[asset_type]["scraped"] += 1
            for hosted_url in hosted_urls:
                asset_type = _week_live_health_asset_type(hosted_url)
                if asset_type:
                    asset_health[asset_type]["saved"] += 1

            caption_present = any(
                str(row.get(field) or "").strip() for field in ("caption", "description", "text", "title")
            )
            if caption_present:
                asset_health["captions"]["scraped"] += 1
                asset_health["captions"]["saved"] += 1

            profile_scraped = any(
                str(row.get(field) or "").strip()
                for field in ("owner_profile_pic_url", "user_avatar_url", "avatar_url", "profile_pic_url")
            )
            if profile_scraped:
                asset_health["profile_pictures"]["scraped"] += 1
            hosted_profile_map = _as_json_object(row.get("hosted_tagged_profile_pics"))
            profile_saved = bool(str(row.get("hosted_owner_profile_pic_url") or "").strip()) or bool(hosted_profile_map)
            if profile_saved:
                asset_health["profile_pictures"]["saved"] += 1

    day_account_rows = sorted(
        day_account_buckets.values(),
        key=lambda item: (
            str(item.get("day") or ""),
            str(item.get("platform") or ""),
            str(item.get("account") or ""),
        ),
    )
    asset_health_rows = [
        {"asset": "images", **asset_health["images"]},
        {"asset": "videos", **asset_health["videos"]},
        {"asset": "captions", **asset_health["captions"]},
        {"asset": "profile_pictures", **asset_health["profile_pictures"]},
    ]
    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "source_scope": source_scope,
        "week": {
            "week_index": int(window.week_index),
            "label": _week_window_label(window, timezone=timezone),
            "start": _iso(start_utc),
            "end": _iso(end_utc),
        },
        "timezone": timezone,
        "day_account_rows": day_account_rows,
        "asset_health": asset_health_rows,
        "updated_at": _iso(now_utc),
    }


def get_analytics(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    week: int | None,
    source_scope: str,
    include_rows: bool = False,
    include_jobs: bool = False,
    include_flags: bool = True,
    include_schedule: bool = True,
    include_benchmark: bool = True,
) -> dict[str, Any]:
    _sync_core_overrides()
    cache_key = _analytics_cache_key(
        season_id=season_id,
        platforms=platforms,
        timezone=timezone,
        week=week,
        source_scope=source_scope,
        include_rows=include_rows,
        include_jobs=include_jobs,
        include_flags=include_flags,
        include_schedule=include_schedule,
        include_benchmark=include_benchmark,
    )
    cached_response = _get_cached_analytics(cache_key)
    if cached_response is not None:
        return cached_response

    context = get_season_context(season_id)
    available_platforms = _resolve_requested_platforms(platforms)

    now = _now_utc()
    week_windows, week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {item.week_index: item for item in week_windows}

    selected_window = windows_by_index.get(week) if week is not None else None
    if week is not None and selected_window is None:
        raise ValueError(f"Week {week} is not available for this season")

    latest_window_end_local = max(
        (item.end_local for item in week_windows),
        default=week_zero_start_local,
    )
    latest_window_end_utc = latest_window_end_local.astimezone(UTC)

    if selected_window:
        start_dt = selected_window.start_local.astimezone(UTC)
        end_dt = (selected_window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    else:
        start_dt = week_zero_start_local.astimezone(UTC)
        end_dt = min(now, latest_window_end_utc)
    if end_dt < start_dt:
        end_dt = start_dt

    sentiment_context = _build_sentiment_context(context)
    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    rows = _build_rows(
        season_id,
        platforms=available_platforms,
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope=source_scope,
        season_context=context,
        analyzer_context=sentiment_context,
        target_accounts_by_platform=target_accounts_by_platform,
        include_post_text=include_rows,
    )

    try:
        post_metadata = _compute_post_metadata(
            season_id,
            platforms=available_platforms,
            start_dt=start_dt,
            end_dt=end_dt,
            target_accounts_by_platform=target_accounts_by_platform,
        )
    except Exception as exc:
        exc_text = str(exc).lower()
        if (
            exc_text.startswith("database pool initialization failed")
            or "invalid input syntax for type uuid" in exc_text
        ):
            logger.debug("Skipping data quality post metadata due unavailable database URL")
            post_metadata = None
        else:
            raise

    posts = [row for row in rows if row["kind"] == "post"]
    comments = [row for row in rows if row["kind"] == "comment"]

    sentiment_counts = {
        "positive": sum(1 for row in comments if row["sentiment"] == "positive"),
        "neutral": sum(1 for row in comments if row["sentiment"] == "neutral"),
        "negative": sum(1 for row in comments if row["sentiment"] == "negative"),
    }

    total_comments = max(1, len(comments))
    sentiment_mix = {
        "positive": round(sentiment_counts["positive"] / total_comments, 4),
        "neutral": round(sentiment_counts["neutral"] / total_comments, 4),
        "negative": round(sentiment_counts["negative"] / total_comments, 4),
        "counts": sentiment_counts,
    }

    visible_windows = [selected_window] if selected_window else week_windows
    weekly_map: dict[int, dict[str, Any]] = {
        item.week_index: {
            "post_volume": 0,
            "comment_volume": 0,
            "engagement": 0,
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "week_start": item.start_local,
            "week_end": item.end_local,
            "week_type": item.week_type,
            "episode_number": item.episode_number,
            "label": _week_window_label(item, timezone=timezone),
        }
        for item in visible_windows
    }

    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        entry = weekly_map[week_window.week_index]
        entry["engagement"] += int(row["engagement"])
        if row["kind"] == "post":
            entry["post_volume"] += 1
        else:
            entry["comment_volume"] += 1
            entry["sentiment"][row["sentiment"]] += 1

    weekly = []
    for week_index in sorted(weekly_map):
        entry = weekly_map[week_index]
        week_end_inclusive = entry["week_end"] - timedelta(microseconds=1)
        weekly.append(
            {
                "week_index": week_index,
                "label": entry["label"],
                "start": _iso(entry["week_start"].astimezone(UTC)),
                "end": _iso(week_end_inclusive.astimezone(UTC)),
                "week_type": entry["week_type"],
                "episode_number": entry["episode_number"],
                "post_volume": entry["post_volume"],
                "comment_volume": entry["comment_volume"],
                "engagement": entry["engagement"],
                "sentiment": entry["sentiment"],
            }
        )

    weekly_platform_posts_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_comments_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_reported_comments_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_engagement_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    local_zone = ZoneInfo(timezone)
    weekly_daily_activity_map: dict[int, dict[str, Any]] = {}
    day_seconds = 24 * 60 * 60
    for window in visible_windows:
        week_start_local = window.start_local.astimezone(local_zone)
        week_end_local = window.end_local.astimezone(local_zone)
        duration_seconds = max(0.0, (week_end_local - week_start_local).total_seconds())
        day_count = max(1, int((duration_seconds + day_seconds - 1) // day_seconds))
        days: list[dict[str, Any]] = []
        for day_index in range(day_count):
            day_start = week_start_local + timedelta(days=day_index)
            days.append(
                {
                    "day_index": day_index,
                    "date_local": day_start.date().isoformat(),
                    "posts": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "comments": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "reported_comments": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "total_posts": 0,
                    "total_comments": 0,
                    "total_reported_comments": 0,
                }
            )
        weekly_daily_activity_map[window.week_index] = {
            "week_index": window.week_index,
            "days": days,
            "week_start": week_start_local,
            "week_start_date": week_start_local.date(),
        }
    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        platform = row["platform"]
        if platform in weekly_platform_engagement_map[week_window.week_index]:
            weekly_platform_engagement_map[week_window.week_index][platform] += int(row["engagement"] or 0)
        if row["kind"] == "post" and platform in weekly_platform_posts_map[week_window.week_index]:
            weekly_platform_posts_map[week_window.week_index][platform] += 1
            weekly_platform_reported_comments_map[week_window.week_index][platform] += int(
                row.get("reported_comments") or 0
            )
        elif row["kind"] == "comment" and platform in weekly_platform_comments_map[week_window.week_index]:
            weekly_platform_comments_map[week_window.week_index][platform] += 1

        day_bucket = weekly_daily_activity_map.get(week_window.week_index)
        if not day_bucket:
            continue
        day_entries = day_bucket.get("days") or []
        if not day_entries:
            continue
        local_ts = row["ts"].astimezone(local_zone)
        day_index = (local_ts.date() - day_bucket["week_start_date"]).days
        if day_index < 0:
            continue
        if day_index >= len(day_entries):
            day_index = len(day_entries) - 1
        day_entry = day_entries[day_index]
        if platform not in day_entry["posts"]:
            continue
        if row["kind"] == "post":
            day_entry["posts"][platform] += 1
            day_entry["total_posts"] += 1
            reported_comments = int(row.get("reported_comments") or 0)
            day_entry["reported_comments"][platform] += reported_comments
            day_entry["total_reported_comments"] += reported_comments
        elif row["kind"] == "comment":
            day_entry["comments"][platform] += 1
            day_entry["total_comments"] += 1

    weekly_platform_posts: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_posts = weekly_platform_posts_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        platform_comments = weekly_platform_comments_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        platform_reported_comments = weekly_platform_reported_comments_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        saved_total_comments = int(sum(platform_comments.values()))
        total_reported_comments = int(sum(platform_reported_comments.values()))
        comments_saved_pct: float | None = None
        if total_reported_comments > 0:
            comments_saved_pct = round(min(100.0, (saved_total_comments * 100.0) / total_reported_comments), 1)
        weekly_platform_posts.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "posts": _platform_int_payload(platform_posts),
                "comments": _platform_int_payload(platform_comments),
                "reported_comments": _platform_int_payload(platform_reported_comments),
                "total_posts": int(sum(platform_posts.values())),
                "total_comments": saved_total_comments,
                "total_reported_comments": total_reported_comments,
                "comments_saved_pct": comments_saved_pct,
            }
        )

    weekly_platform_engagement: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_engagement = weekly_platform_engagement_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        engagement_payload = _platform_int_payload(platform_engagement)
        total_engagement = int(sum(engagement_payload.values()))
        weekly_platform_engagement.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "engagement": engagement_payload,
                "total_engagement": total_engagement,
                "has_data": total_engagement > 0,
            }
        )

    weekly_daily_activity: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        day_entries = (weekly_daily_activity_map.get(week_index) or {}).get("days") or []
        days_payload: list[dict[str, Any]] = []
        for day_entry in day_entries:
            posts_payload = day_entry.get("posts") or {}
            comments_payload = day_entry.get("comments") or {}
            reported_comments_payload = day_entry.get("reported_comments") or {}
            days_payload.append(
                {
                    "day_index": int(day_entry.get("day_index", 0)),
                    "date_local": str(day_entry.get("date_local") or ""),
                    "posts": _platform_int_payload(posts_payload),
                    "comments": _platform_int_payload(comments_payload),
                    "reported_comments": _platform_int_payload(reported_comments_payload),
                    "total_posts": int(day_entry.get("total_posts", 0)),
                    "total_comments": int(day_entry.get("total_comments", 0)),
                    "total_reported_comments": int(day_entry.get("total_reported_comments", 0)),
                }
            )

        weekly_daily_activity.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "days": days_payload,
            }
        )

    comments_saved_by_platform = dict.fromkeys(SUPPORTED_PLATFORMS, 0)
    reported_comments_by_platform = dict.fromkeys(SUPPORTED_PLATFORMS, 0)
    for week_row in weekly_platform_posts:
        comments_payload = week_row.get("comments") if isinstance(week_row.get("comments"), dict) else {}
        reported_payload = (
            week_row.get("reported_comments") if isinstance(week_row.get("reported_comments"), dict) else {}
        )
        for platform in SUPPORTED_PLATFORMS:
            comments_saved_by_platform[platform] += int(comments_payload.get(platform, 0))
            reported_comments_by_platform[platform] += int(reported_payload.get(platform, 0))

    platform_comments_saved_pct = {
        platform: (
            _safe_percent(
                comments_saved_by_platform.get(platform, 0),
                reported_comments_by_platform.get(platform, 0),
            )
            if platform in available_platforms
            else None
        )
        for platform in SUPPORTED_PLATFORMS
    }
    total_saved_comments = sum(comments_saved_by_platform.values())
    total_reported_comments = sum(reported_comments_by_platform.values())
    comments_saved_pct_overall = _safe_percent(total_saved_comments, total_reported_comments)
    youtube_posts = [row for row in posts if str(row.get("platform") or "") == "youtube"]
    youtube_reels_count = sum(
        1 for row in youtube_posts if bool(row.get("is_short")) or "/shorts/" in str(row.get("url") or "")
    )
    youtube_videos_count = max(0, len(youtube_posts) - youtube_reels_count)
    youtube_content_breakdown = {
        "videos_count": youtube_videos_count,
        "reels_count": youtube_reels_count,
        "total_count": len(youtube_posts),
    }

    last_post_dt = max((row["ts"] for row in posts), default=None)
    last_comment_dt = max((row["ts"] for row in comments), default=None)
    freshness_anchor = max(
        [value for value in [last_post_dt, last_comment_dt] if isinstance(value, datetime)],
        default=None,
    )
    data_freshness_minutes: int | None = None
    if isinstance(freshness_anchor, datetime):
        data_freshness_minutes = max(0, int((now - freshness_anchor).total_seconds() // 60))

    weekly_flags: list[dict[str, Any]] = []
    weekly_by_index = sorted(weekly, key=lambda item: int(item.get("week_index", 0)))
    trend_weeks = [item for item in weekly_by_index if str(item.get("week_type") or "") != "bye"]
    weekly_posts_by_index = {
        int(item.get("week_index", 0)): item
        for item in weekly_platform_posts
        if isinstance(item.get("week_index"), int)
    }
    for idx, week_entry in enumerate(trend_weeks):
        week_index = int(week_entry.get("week_index", 0))
        post_volume = int(week_entry.get("post_volume") or 0)
        if post_volume == 0:
            weekly_flags.append(
                {
                    "week_index": week_index,
                    "code": "zero_activity",
                    "severity": "info",
                    "message": "No posts captured in this week window.",
                }
            )

        trailing_values = [int(item.get("post_volume") or 0) for item in trend_weeks[max(0, idx - 2) : idx]]
        trailing_avg = (sum(trailing_values) / len(trailing_values)) if trailing_values else 0.0
        if trailing_avg > 0:
            if post_volume >= 6 and post_volume >= (2.0 * trailing_avg):
                weekly_flags.append(
                    {
                        "week_index": week_index,
                        "code": "spike",
                        "severity": "warn",
                        "message": f"Post volume spike vs trailing 2-week average ({trailing_avg:.1f}).",
                    }
                )
            if trailing_avg >= 4 and post_volume <= (0.4 * trailing_avg):
                weekly_flags.append(
                    {
                        "week_index": week_index,
                        "code": "drop",
                        "severity": "warn",
                        "message": f"Post volume drop vs trailing 2-week average ({trailing_avg:.1f}).",
                    }
                )

        weekly_post_row = weekly_posts_by_index.get(week_index) or {}
        week_reported_comments = int(weekly_post_row.get("total_reported_comments") or 0)
        week_saved_pct = weekly_post_row.get("comments_saved_pct")
        if week_reported_comments > 0 and isinstance(week_saved_pct, (int, float)) and float(week_saved_pct) < 60.0:
            weekly_flags.append(
                {
                    "week_index": week_index,
                    "code": "comment_gap",
                    "severity": "warn",
                    "message": f"Only {float(week_saved_pct):.1f}% of expected comments are saved.",
                }
            )

    schedule_profile: dict[str, Any] | None = None
    if include_schedule:
        schedule_platforms: list[dict[str, Any]] = []
        for platform in [item for item in SUPPORTED_PLATFORMS if item in available_platforms]:
            day_values: list[int] = []
            for week_row in weekly_daily_activity:
                for day_row in week_row.get("days") or []:
                    posts_payload = day_row.get("posts") or {}
                    day_values.append(int(posts_payload.get(platform, 0)))
            zero_days = sum(1 for value in day_values if value <= 0)
            active_days = sum(1 for value in day_values if value > 0)
            schedule_platforms.append(
                {
                    "platform": platform,
                    "zero_days": zero_days,
                    "peak_day_posts": max(day_values) if day_values else 0,
                    "median_day_posts": _median_int(day_values),
                    "active_days": active_days,
                }
            )
        schedule_profile = {
            "timezone": timezone,
            "platforms": schedule_platforms,
        }

    benchmark: dict[str, Any] | None = None
    if include_benchmark:
        benchmark_weeks = sorted(weekly, key=lambda item: int(item.get("week_index", 0)))
        current_week_entry: dict[str, Any] | None = None
        if week is not None:
            current_week_entry = next(
                (item for item in benchmark_weeks if int(item.get("week_index", -1)) == week),
                None,
            )
        if current_week_entry is None and benchmark_weeks:
            current_week_entry = benchmark_weeks[-1]

        def _metrics_payload(item: dict[str, Any] | None) -> dict[str, int]:
            return {
                "posts": int((item or {}).get("post_volume") or 0),
                "comments": int((item or {}).get("comment_volume") or 0),
                "engagement": int((item or {}).get("engagement") or 0),
            }

        def _delta_pct(current_value: int, baseline_value: int) -> float | None:
            if baseline_value <= 0:
                return None
            return round(((current_value - baseline_value) * 100.0) / float(baseline_value), 1)

        current_index = int((current_week_entry or {}).get("week_index", -1))
        previous_week_entry = next(
            (item for item in benchmark_weeks if int(item.get("week_index", -1)) == current_index - 1),
            None,
        )
        trailing_prev_weeks = [item for item in benchmark_weeks if int(item.get("week_index", -1)) < current_index][-3:]
        trailing_avg_metrics = {
            "posts": 0.0,
            "comments": 0.0,
            "engagement": 0.0,
        }
        if trailing_prev_weeks:
            trailing_avg_metrics = {
                "posts": (
                    sum(int(item.get("post_volume") or 0) for item in trailing_prev_weeks) / len(trailing_prev_weeks)
                ),
                "comments": sum(int(item.get("comment_volume") or 0) for item in trailing_prev_weeks)
                / len(trailing_prev_weeks),
                "engagement": sum(int(item.get("engagement") or 0) for item in trailing_prev_weeks)
                / len(trailing_prev_weeks),
            }

        consistency_score: dict[str, float | None] = {}
        for platform in [item for item in SUPPORTED_PLATFORMS if item in available_platforms]:
            total_days = 0
            active_days = 0
            for week_row in weekly_daily_activity:
                for day_row in week_row.get("days") or []:
                    total_days += 1
                    posts_payload = day_row.get("posts") or {}
                    if int(posts_payload.get(platform, 0)) > 0:
                        active_days += 1
            consistency_score[platform] = _safe_percent(active_days, total_days)

        current_metrics = _metrics_payload(current_week_entry)
        previous_metrics = _metrics_payload(previous_week_entry)
        benchmark = {
            "week_index": int((current_week_entry or {}).get("week_index", -1)),
            "current": current_metrics,
            "previous_week": {
                "week_index": int((previous_week_entry or {}).get("week_index", -1)) if previous_week_entry else None,
                "metrics": previous_metrics,
                "delta_pct": {
                    "posts": _delta_pct(current_metrics["posts"], previous_metrics["posts"]),
                    "comments": _delta_pct(current_metrics["comments"], previous_metrics["comments"]),
                    "engagement": _delta_pct(current_metrics["engagement"], previous_metrics["engagement"]),
                },
            },
            "trailing_3_week_avg": {
                "window_size": len(trailing_prev_weeks),
                "metrics": {
                    "posts": round(trailing_avg_metrics["posts"], 1),
                    "comments": round(trailing_avg_metrics["comments"], 1),
                    "engagement": round(trailing_avg_metrics["engagement"], 1),
                },
                "delta_pct": {
                    "posts": _delta_pct(current_metrics["posts"], int(round(trailing_avg_metrics["posts"]))),
                    "comments": _delta_pct(current_metrics["comments"], int(round(trailing_avg_metrics["comments"]))),
                    "engagement": _delta_pct(
                        current_metrics["engagement"],
                        int(round(trailing_avg_metrics["engagement"])),
                    ),
                },
            },
            "consistency_score_pct": consistency_score,
        }

    platform_breakdown = []
    for platform in available_platforms:
        platform_rows = [row for row in rows if row["platform"] == platform]
        platform_comments = [row for row in platform_rows if row["kind"] == "comment"]
        platform_breakdown.append(
            {
                "platform": platform,
                "posts": sum(1 for row in platform_rows if row["kind"] == "post"),
                "comments": len(platform_comments),
                "engagement": sum(int(row["engagement"]) for row in platform_rows),
                "sentiment": {
                    "positive": sum(1 for row in platform_comments if row["sentiment"] == "positive"),
                    "neutral": sum(1 for row in platform_comments if row["sentiment"] == "neutral"),
                    "negative": sum(1 for row in platform_comments if row["sentiment"] == "negative"),
                },
            }
        )

    leaderboards = {
        "bravo_content": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
                "thumbnail_url": row.get("thumbnail_url"),
            }
            for row in sorted(posts, key=lambda item: item["engagement"], reverse=True)[:15]
        ],
        "viewer_discussion": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
                "sentiment": row["sentiment"],
                "thumbnail_url": row.get("thumbnail_url"),
            }
            for row in sorted(comments, key=lambda item: item["engagement"], reverse=True)[:20]
        ],
    }

    jobs = list_jobs(season_id, limit=25) if include_jobs else []
    reddit_summary: dict[str, Any] | None = None
    try:
        community_row = pg.fetch_one(
            """
            select id::text as community_id,
                   subreddit
            from admin.reddit_communities
            where trr_show_id = %s::uuid
              and is_active = true
            order by updated_at desc nulls last, created_at desc
            limit 1
            """,
            [context.show_id],
        )
        community_id = str((community_row or {}).get("community_id") or "").strip()
        subreddit = str((community_row or {}).get("subreddit") or "").strip()
        if community_id and subreddit:
            from trr_backend.repositories.reddit_refresh import (
                get_reddit_community_analytics_summary,
                get_reddit_community_flair_breakdown,
            )

            reddit_summary_payload = get_reddit_community_analytics_summary(
                community_id=community_id,
                scope="season",
                season_id=season_id,
            )
            reddit_flair_payload = get_reddit_community_flair_breakdown(
                community_id=community_id,
                scope="season",
                season_id=season_id,
            )
            show_slug = str(context.show_slug or "").strip()
            reddit_summary = {
                "community_id": community_id,
                "subreddit": subreddit,
                "tracked_post_count": int(
                    reddit_summary_payload.get("totals", {}).get("tracked_flair_post_count") or 0
                ),
                "show_match_post_count": int(
                    reddit_summary_payload.get("totals", {}).get("show_match_post_count") or 0
                ),
                "comment_count": int(reddit_summary_payload.get("totals", {}).get("comment_count") or 0),
                "flair_mix": reddit_flair_payload.get("flairs") or [],
                "freshness": reddit_summary_payload.get("freshness") or {},
                "coverage": reddit_summary_payload.get("coverage") or {},
                "container_statuses": reddit_summary_payload.get("container_statuses") or [],
                "deep_link": {
                    "label": f"r/{subreddit}",
                    "path": (
                        f"/admin/social/reddit/{subreddit}/{show_slug}/s{context.season_number}"
                        if show_slug and context.season_number
                        else None
                    ),
                    "show_slug": show_slug or None,
                    "season_number": context.season_number,
                },
            }
    except Exception:  # noqa: BLE001
        logger.exception("Failed to assemble season Reddit analytics block: season_id=%s", season_id)

    response: dict[str, Any] = {
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
            "week_anchor": str(context.anchor_date),
            "week_zero_start": _iso(week_zero_start_local.astimezone(UTC)),
            "week": week,
            "source_scope": source_scope,
        },
        "summary": {
            "show_id": context.show_id,
            "season_id": context.season_id,
            "season_number": context.season_number,
            "show_name": context.show_name,
            "total_posts": len(posts),
            "total_comments": len(comments),
            "total_engagement": sum(int(row["engagement"]) for row in rows),
            "sentiment_mix": sentiment_mix,
            "deltas": {
                "posts": None,
                "comments": None,
                "engagement": None,
            },
            "data_quality": {
                "comments_saved_pct_overall": comments_saved_pct_overall,
                "platform_comments_saved_pct": platform_comments_saved_pct,
                "last_post_at": _iso(last_post_dt),
                "last_comment_at": _iso(last_comment_dt),
                "data_freshness_minutes": data_freshness_minutes,
                "youtube_content_breakdown": youtube_content_breakdown,
                "post_metadata": post_metadata,
            },
        },
        "weekly": weekly,
        "weekly_platform_posts": weekly_platform_posts,
        "weekly_platform_engagement": weekly_platform_engagement,
        "weekly_daily_activity": weekly_daily_activity,
        "platform_breakdown": platform_breakdown,
        "themes": _build_drivers(comments, analyzer_context=sentiment_context),
        "leaderboards": leaderboards,
        "jobs": jobs,
        "reddit": reddit_summary,
    }
    if include_flags:
        response["weekly_flags"] = weekly_flags
    if include_schedule and schedule_profile is not None:
        response["schedule_profile"] = schedule_profile
    if include_benchmark and benchmark is not None:
        response["benchmark"] = benchmark

    if include_rows:
        lookup_windows = visible_windows
        rows_payload: list[dict[str, Any]] = []
        for row in rows:
            bucket = _week_for_timestamp(row["ts"], windows=lookup_windows, timezone=timezone)
            rows_payload.append(
                {
                    "week_index": bucket.week_index if bucket else None,
                    "platform": row["platform"],
                    "kind": row["kind"],
                    "source_id": row["source_id"],
                    "timestamp": _iso(row["ts"]),
                    "author": row["author"],
                    "url": row["url"],
                    "engagement": row["engagement"],
                    "sentiment": row["sentiment"],
                    "text": row["text"],
                    "thumbnail_url": row.get("thumbnail_url"),
                }
            )
        response["rows"] = rows_payload

    _set_cached_analytics(cache_key, response)
    return response


def get_comments_coverage(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    context = get_season_context(season_id)
    available_platforms = _resolve_requested_platforms(platforms)

    start_dt, end_dt, now_utc = _resolve_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    active_job_status_by_platform = _active_job_status_by_platform_for_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        start_dt=start_dt,
        end_dt=end_dt,
        now_utc=now_utc,
        platforms=available_platforms,
    )

    by_platform: dict[str, dict[str, Any]] = {}
    total_saved = 0
    total_reported = 0
    total_stale_posts = 0
    total_posts = 0
    for platform in available_platforms:
        stats = _comments_coverage_for_platform(
            season_id,
            platform=platform,
            start_dt=start_dt,
            end_dt=end_dt,
            source_scope=source_scope,
            target_accounts_by_platform=target_accounts_by_platform,
            season_context=context,
        )
        saved = int(stats.get("saved_comments") or 0)
        reported = int(stats.get("reported_comments") or 0)
        stale_posts = int(stats.get("stale_posts_count") or 0)
        posts_scanned = int(stats.get("posts_scanned") or 0)
        total_saved += saved
        total_reported += reported
        total_stale_posts += stale_posts
        total_posts += posts_scanned
        comment_sync_status = _build_comment_sync_status(
            expected_count=reported,
            fetched_count=saved,
            upserted_count=saved,
            stale_posts_count=stale_posts,
            failure_reason=_platform_failure_reason(platform, "comment_gap")
            if stale_posts > 0 or saved < reported
            else None,
            attempted=posts_scanned > 0,
        )
        media_mirror_status = _build_media_mirror_status(
            source_count=0,
            mirrored_count=0,
            not_needed=True,
        )
        by_platform[platform] = {
            "saved_comments": saved,
            "reported_comments": reported,
            "coverage_pct": _safe_percent(saved, reported),
            "up_to_date": saved >= reported,
            "stale_posts_count": stale_posts,
            "posts_scanned": posts_scanned,
            "saved_replies": int(stats.get("saved_replies") or 0),
            "reported_replies": int(stats.get("reported_replies") or 0),
            "reply_coverage_pct": _safe_percent(
                int(stats.get("saved_replies") or 0),
                int(stats.get("reported_replies") or 0),
            ),
            "saved_quotes": int(stats.get("saved_quotes") or 0),
            "reported_quotes": int(stats.get("reported_quotes") or 0),
            "quote_coverage_pct": _safe_percent(
                int(stats.get("saved_quotes") or 0),
                int(stats.get("reported_quotes") or 0),
            ),
            **_overlay_platform_status_with_active_jobs(
                _build_platform_status_payload(
                    posts_scanned=posts_scanned,
                    comment_sync_status=comment_sync_status,
                    media_mirror_status=media_mirror_status,
                    last_refresh_at=None,
                    last_refresh_reason=None,
                    active_job_summary=active_job_status_by_platform.get(platform),
                ),
                active_job_status=active_job_status_by_platform.get(platform),
            ),
        }

    total_comment_sync_status = _build_comment_sync_status(
        expected_count=total_reported,
        fetched_count=total_saved,
        upserted_count=total_saved,
        stale_posts_count=total_stale_posts,
        failure_reason="comment_gap" if total_stale_posts > 0 or total_saved < total_reported else None,
        attempted=total_posts > 0,
    )
    total_media_status = _build_media_mirror_status(
        source_count=0,
        mirrored_count=0,
        not_needed=True,
    )
    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "platforms": available_platforms,
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
        },
        "total_saved_comments": total_saved,
        "total_reported_comments": total_reported,
        "coverage_pct": _safe_percent(total_saved, total_reported),
        "up_to_date": total_saved >= total_reported,
        "stale_posts_count": total_stale_posts,
        "posts_scanned": total_posts,
        "by_platform": by_platform,
        "evaluated_at": _iso(now_utc),
        **_build_platform_status_payload(
            posts_scanned=total_posts,
            comment_sync_status=total_comment_sync_status,
            media_mirror_status=total_media_status,
            last_refresh_at=None,
            last_refresh_reason=None,
        ),
    }


def get_mirror_coverage(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    context = get_season_context(season_id)
    available_platforms = _resolve_requested_platforms(platforms)

    start_dt, end_dt, now_utc = _resolve_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    active_job_status_by_platform = _active_job_status_by_platform_for_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        start_dt=start_dt,
        end_dt=end_dt,
        now_utc=now_utc,
        platforms=available_platforms,
    )

    by_platform: dict[str, dict[str, Any]] = {}
    total_posts = 0
    total_needs_mirror = 0
    total_mirrored = 0
    total_failed = 0
    total_partial = 0
    total_pending = 0
    total_comment_media_items_scanned = 0
    total_comment_media_needs_mirror = 0
    total_comment_media_mirrored = 0
    total_comment_media_failed = 0
    total_comment_media_pending = 0
    last_job_ids: list[str] = []
    for platform in available_platforms:
        stats = _mirror_coverage_for_platform(
            season_id,
            platform=platform,
            start_dt=start_dt,
            end_dt=end_dt,
            source_scope=source_scope,
            target_accounts_by_platform=target_accounts_by_platform,
            season_context=context,
        )
        posts_scanned = int(stats.get("posts_scanned") or 0)
        needs_mirror_count = int(stats.get("needs_mirror_count") or 0)
        mirrored_count = int(stats.get("mirrored_count") or 0)
        failed_count = int(stats.get("failed_count") or 0)
        partial_count = int(stats.get("partial_count") or 0)
        pending_count = int(stats.get("pending_count") or 0)
        total_posts += posts_scanned
        total_needs_mirror += needs_mirror_count
        total_mirrored += mirrored_count
        total_failed += failed_count
        total_partial += partial_count
        total_pending += pending_count
        try:
            comment_media_stats = _comment_media_coverage_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
            )
        except Exception:
            logger.debug(
                "Unable to resolve comment-media coverage for season=%s platform=%s source_scope=%s",
                season_id,
                platform,
                source_scope,
                exc_info=True,
            )
            comment_media_stats = {
                "items_scanned": 0,
                "needs_mirror_count": 0,
                "mirrored_count": 0,
                "failed_count": 0,
                "pending_count": 0,
            }
        comment_media_items_scanned = int(comment_media_stats.get("items_scanned") or 0)
        comment_media_needs_mirror_count = int(comment_media_stats.get("needs_mirror_count") or 0)
        comment_media_mirrored_count = int(comment_media_stats.get("mirrored_count") or 0)
        comment_media_failed_count = int(comment_media_stats.get("failed_count") or 0)
        comment_media_pending_count = int(comment_media_stats.get("pending_count") or 0)
        total_comment_media_items_scanned += comment_media_items_scanned
        total_comment_media_needs_mirror += comment_media_needs_mirror_count
        total_comment_media_mirrored += comment_media_mirrored_count
        total_comment_media_failed += comment_media_failed_count
        total_comment_media_pending += comment_media_pending_count
        media_mirror_status = _build_media_mirror_status(
            source_count=posts_scanned + comment_media_items_scanned,
            mirrored_count=mirrored_count + comment_media_mirrored_count,
            failed_count=failed_count + comment_media_failed_count,
            pending_count=pending_count + comment_media_pending_count,
            partial_count=partial_count,
            attempted=(posts_scanned + comment_media_items_scanned) > 0,
            failure_reason=_derive_media_failure_reason(
                platform,
                post_failed_count=failed_count,
                post_partial_count=partial_count,
                post_pending_count=pending_count,
                comment_failed_count=comment_media_failed_count,
                comment_pending_count=comment_media_pending_count,
            ),
        )
        comment_sync_status = _build_comment_sync_status(
            expected_count=0,
            fetched_count=0,
            upserted_count=0,
            attempted=False,
        )
        if media_mirror_status.get("last_job_id"):
            last_job_ids.append(str(media_mirror_status["last_job_id"]))
        by_platform[platform] = {
            "posts_scanned": posts_scanned,
            "needs_mirror_count": needs_mirror_count,
            "mirrored_count": mirrored_count,
            "failed_count": failed_count,
            "partial_count": partial_count,
            "pending_count": pending_count,
            "comment_media_items_scanned": comment_media_items_scanned,
            "comment_media_needs_mirror_count": comment_media_needs_mirror_count,
            "comment_media_mirrored_count": comment_media_mirrored_count,
            "comment_media_failed_count": comment_media_failed_count,
            "comment_media_pending_count": comment_media_pending_count,
            "up_to_date": needs_mirror_count == 0 and comment_media_needs_mirror_count == 0,
            **_overlay_platform_status_with_active_jobs(
                _build_platform_status_payload(
                    posts_scanned=posts_scanned,
                    comment_sync_status=comment_sync_status,
                    media_mirror_status=media_mirror_status,
                    last_refresh_at=None,
                    last_refresh_reason=None,
                    worker_run_id=media_mirror_status.get("last_job_id"),
                    active_job_summary=active_job_status_by_platform.get(platform),
                ),
                active_job_status=active_job_status_by_platform.get(platform),
            ),
        }

    total_comment_sync_status = _build_comment_sync_status(
        expected_count=0,
        fetched_count=0,
        upserted_count=0,
        attempted=False,
    )
    total_media_status = _build_media_mirror_status(
        source_count=total_posts + total_comment_media_items_scanned,
        mirrored_count=total_mirrored + total_comment_media_mirrored,
        failed_count=total_failed + total_comment_media_failed,
        pending_count=total_pending + total_comment_media_pending,
        partial_count=total_partial,
        attempted=(total_posts + total_comment_media_items_scanned) > 0,
        failure_reason=(
            "coverage_post_and_comment_media_mirror_failed"
            if (total_failed + total_comment_media_failed) > 0
            else "coverage_post_and_comment_media_mirror_gap"
            if (total_needs_mirror + total_comment_media_needs_mirror) > 0
            else None
        ),
    )
    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "platforms": available_platforms,
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
        },
        "up_to_date": total_needs_mirror == 0 and total_comment_media_needs_mirror == 0,
        "needs_mirror_count": total_needs_mirror,
        "mirrored_count": total_mirrored,
        "failed_count": total_failed,
        "partial_count": total_partial,
        "pending_count": total_pending,
        "posts_scanned": total_posts,
        "comment_media_items_scanned": total_comment_media_items_scanned,
        "comment_media_needs_mirror_count": total_comment_media_needs_mirror,
        "comment_media_mirrored_count": total_comment_media_mirrored,
        "comment_media_failed_count": total_comment_media_failed,
        "comment_media_pending_count": total_comment_media_pending,
        "by_platform": by_platform,
        "evaluated_at": _iso(now_utc),
        **_build_platform_status_payload(
            posts_scanned=total_posts,
            comment_sync_status=total_comment_sync_status,
            media_mirror_status=total_media_status,
            last_refresh_at=None,
            last_refresh_reason=None,
            worker_run_id=last_job_ids[0] if last_job_ids else None,
        ),
    }


def get_week_detail(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    max_comments_per_post: int = 0,
    post_limit: int = 20,
    post_offset: int = 0,
    sort_field: str = "posted_at",
    sort_dir: str = "desc",
    include_status: bool = True,
) -> dict[str, Any]:
    """Return detailed post-level data for a single week of a season."""
    _sync_core_overrides()
    started_at = time_module.perf_counter()
    post_limit = max(0, int(post_limit))
    if post_offset < 0:
        post_offset = 0
    normalized_sort_field, normalized_sort_dir = _normalize_week_detail_sort(sort_field, sort_dir)

    context = get_season_context(season_id)
    available_platforms = _resolve_requested_platforms(platforms)

    now = _now_utc()
    week_windows, _week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {w.week_index: w for w in week_windows}
    window = windows_by_index.get(week_index)
    if window is None:
        raise ValueError(f"Week {week_index} is not available for this season")

    start_dt = window.start_local.astimezone(UTC)
    end_dt = (window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    if end_dt < start_dt:
        end_dt = start_dt

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    requires_target_accounts = source_scope in {"network", "creator", "news"}

    platform_results: dict[str, Any] = {}
    status_posts_by_platform: dict[str, list[dict[str, Any]]] = {}
    merged_posts: list[tuple[str, dict[str, Any]]] = []
    grand_posts = 0
    grand_comments = 0
    grand_engagement = 0
    grand_expected_comments = 0
    grand_saved_comments = 0
    by_platform_duration_ms: dict[str, int] = {}

    def _load_platform_result(platform: str) -> tuple[str, dict[str, Any] | None, int]:
        handler = _WEEK_DETAIL_HANDLERS.get(platform)
        if not handler:
            return platform, None, 0
        account_handles = set(target_accounts_by_platform.get(platform, set()))
        if requires_target_accounts and not account_handles:
            result = {
                "posts": [],
                "total_posts": 0,
                "totals": {
                    "posts": 0,
                    "total_comments": 0,
                    "total_engagement": 0,
                },
            }
            return platform, result, 0
        handler_kwargs: dict[str, Any] = {
            "start_dt": start_dt,
            "end_dt": end_dt,
            "account_handles": account_handles,
            "max_comments": max_comments_per_post,
            "post_limit": post_limit,
            "post_offset": post_offset,
            "sort_field": normalized_sort_field,
            "sort_dir": normalized_sort_dir,
        }
        if platform == "threads":
            handler_kwargs["source_scope"] = source_scope
            handler_kwargs["season_context"] = context
        platform_started_at = time_module.perf_counter()
        result = handler(season_id, **handler_kwargs)
        duration_ms = int((time_module.perf_counter() - platform_started_at) * 1000)
        return platform, result, duration_ms

    loaded_results: dict[str, tuple[dict[str, Any], int]] = {}
    if len(available_platforms) <= 1:
        for platform in available_platforms:
            loaded_platform, result, duration_ms = _load_platform_result(platform)
            if result is not None:
                loaded_results[loaded_platform] = (result, duration_ms)
    else:
        max_workers = max(1, min(len(available_platforms), 6))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_platform = {
                executor.submit(_load_platform_result, platform): platform for platform in available_platforms
            }
            for future in as_completed(future_to_platform):
                loaded_platform, result, duration_ms = future.result()
                if result is not None:
                    loaded_results[loaded_platform] = (result, duration_ms)

    for platform in available_platforms:
        loaded = loaded_results.get(platform)
        if loaded is None:
            continue
        result, duration_ms = loaded
        result_posts = list(result.get("posts") or [])
        by_platform_duration_ms[platform] = duration_ms
        result_total_posts = int(result.get("total_posts", len(result.get("posts") or [])) or 0)
        result_totals = _normalize_week_totals_payload(result.get("totals"), total_posts=result_total_posts)
        platform_results[platform] = {
            "posts": [],
            "total_posts": result_total_posts,
            "totals": result_totals,
        }
        status_posts_by_platform[platform] = result_posts
        for post in result_posts:
            merged_posts.append((platform, post))
        totals = result_totals
        grand_posts += int(totals.get("posts", 0))
        grand_comments += totals.get("total_comments", 0)
        grand_engagement += totals.get("total_engagement", 0)
        grand_expected_comments += int(totals.get("expected_comments_total") or 0)
        grand_saved_comments += int(totals.get("saved_comments_total") or 0)

    status_by_platform: dict[str, dict[str, Any]] = {}
    active_week_runs: dict[str, dict[str, Any]] = {}
    primary_week_run_id: str | None = None
    if include_status:
        active_week_runs = _resolve_week_run_rows_by_platform(
            context.season_id,
            source_scope=source_scope,
            week_index=week_index,
            platforms=available_platforms,
        )
        active_job_status_by_platform = _active_week_job_status_by_platform(
            None,
            source_scope=source_scope,
            week_index=week_index,
            platforms=available_platforms,
            run_rows_by_platform=active_week_runs,
        )
        primary_week_run_id = _primary_week_run_id(active_week_runs)

        for platform in available_platforms:
            platform_payload = platform_results.get(platform, {})
            platform_posts = status_posts_by_platform.get(platform, [])
            post_failure_reasons = _extract_platform_post_failure_reasons(
                platform,
                platform_posts if isinstance(platform_posts, list) else [],
            )
            comments_stats = _comments_coverage_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
                season_context=context,
            )
            mirror_stats = _mirror_coverage_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
                season_context=context,
            )
            comment_media_stats = _comment_media_coverage_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
            )
            comment_status = _build_comment_sync_status(
                expected_count=int(comments_stats.get("reported_comments") or 0),
                fetched_count=int(comments_stats.get("saved_comments") or 0),
                upserted_count=int(comments_stats.get("saved_comments") or 0),
                stale_posts_count=int(comments_stats.get("stale_posts_count") or 0),
                failure_reason=(
                    post_failure_reasons.get("comment_failure_reason")
                    or _platform_failure_reason(platform, "comment_gap")
                    if int(comments_stats.get("stale_posts_count") or 0) > 0
                    or int(comments_stats.get("saved_comments") or 0)
                    < int(comments_stats.get("reported_comments") or 0)
                    else None
                ),
                attempted=int(comments_stats.get("posts_scanned") or 0) > 0,
            )
            media_status = _build_media_mirror_status(
                source_count=int(mirror_stats.get("posts_scanned") or 0)
                + int(comment_media_stats.get("items_scanned") or 0),
                mirrored_count=int(mirror_stats.get("mirrored_count") or 0)
                + int(comment_media_stats.get("mirrored_count") or 0),
                failed_count=int(mirror_stats.get("failed_count") or 0)
                + int(comment_media_stats.get("failed_count") or 0),
                pending_count=int(mirror_stats.get("pending_count") or 0)
                + int(comment_media_stats.get("pending_count") or 0),
                partial_count=int(mirror_stats.get("partial_count") or 0),
                last_job_id=_extract_last_platform_job_id(platform_posts if isinstance(platform_posts, list) else []),
                attempted=(
                    int(mirror_stats.get("posts_scanned") or 0) + int(comment_media_stats.get("items_scanned") or 0)
                )
                > 0,
                failure_reason=post_failure_reasons.get("media_failure_reason")
                or _derive_media_failure_reason(
                    platform,
                    post_failed_count=int(mirror_stats.get("failed_count") or 0),
                    post_partial_count=int(mirror_stats.get("partial_count") or 0),
                    post_pending_count=int(mirror_stats.get("pending_count") or 0),
                    comment_failed_count=int(comment_media_stats.get("failed_count") or 0),
                    comment_pending_count=int(comment_media_stats.get("pending_count") or 0),
                ),
            )
            status_payload = _build_platform_status_payload(
                posts_scanned=int(comments_stats.get("posts_scanned") or 0),
                comment_sync_status=comment_status,
                media_mirror_status=media_status,
                last_refresh_at=_extract_last_platform_refresh_at(platform_posts),
                last_refresh_reason=post_failure_reasons.get("last_refresh_reason")
                or comment_status.get("failure_reason")
                or media_status.get("failure_reason"),
                worker_run_id=str((active_week_runs.get(platform) or {}).get("run_id") or "").strip()
                or media_status.get("last_job_id")
                or None,
                active_job_summary=active_job_status_by_platform.get(platform),
            )
            status_payload = _overlay_platform_status_with_active_jobs(
                status_payload,
                active_job_status=active_job_status_by_platform.get(platform),
            )
            status_by_platform[platform] = status_payload
            if isinstance(platform_payload, dict):
                platform_payload["status"] = status_payload

    reverse = normalized_sort_dir == "desc"
    if normalized_sort_field == "posted_at":
        merged_posts.sort(
            key=lambda item: str((item[1] if isinstance(item[1], dict) else {}).get("posted_at") or ""),
            reverse=reverse,
        )
    else:
        merged_posts.sort(
            key=lambda item: (
                _coerce_week_detail_sort_metric(
                    (item[1] if isinstance(item[1], dict) else {}).get(normalized_sort_field)
                ),
                str((item[1] if isinstance(item[1], dict) else {}).get("posted_at") or ""),
            ),
            reverse=reverse,
        )
    if post_limit <= 0:
        page_posts = []
        page_end = post_offset
    else:
        page_end = post_offset + post_limit
        page_posts = merged_posts[post_offset:page_end]
    paged_total_posts = len(merged_posts)

    posts_by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for platform, post in page_posts:
        posts_by_platform[platform].append(post)
    for platform in available_platforms:
        if platform in platform_results:
            platform_results[platform]["posts"] = posts_by_platform.get(platform, [])
            if not posts_by_platform.get(platform):
                platform_results[platform]["posts"] = []
    grand_posts = 0
    for platform in available_platforms:
        grand_posts += int(platform_results.get(platform, {}).get("total_posts", 0) or 0)

    week_end_inclusive = window.end_local - timedelta(microseconds=1)
    return {
        "week": {
            "week_index": week_index,
            "label": _week_window_label(window, timezone=timezone),
            "start": _iso(window.start_local.astimezone(UTC)),
            "end": _iso(week_end_inclusive.astimezone(UTC)),
            "week_type": window.week_type,
            "episode_number": window.episode_number,
        },
        "season": {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "show_name": context.show_name,
            "show_slug": context.show_slug,
            "season_number": context.season_number,
        },
        "source_scope": source_scope,
        "platforms": platform_results,
        "status_by_platform": status_by_platform,
        "pagination": {
            "limit": post_limit,
            "offset": post_offset,
            "returned": len(page_posts),
            "total": paged_total_posts,
            "has_more": page_end < paged_total_posts,
        },
        "totals": {
            "posts": grand_posts,
            "total_comments": grand_comments,
            "total_engagement": grand_engagement,
            "expected_comments_total": grand_expected_comments,
            "saved_comments_total": grand_saved_comments,
            "comments_saved_pct": float(_safe_percent(grand_saved_comments, grand_expected_comments) or 0.0),
        },
        "diagnostics": {
            "run_id": primary_week_run_id,
            "run_ids_by_platform": {
                platform: str((active_week_runs.get(platform) or {}).get("run_id") or "").strip() or None
                for platform in available_platforms
            },
            "generated_at": _iso(_now_utc()),
            "source_scope": source_scope,
        },
        "meta": {
            "performance": {
                "total_duration_ms": int((time_module.perf_counter() - started_at) * 1000),
                "by_platform": by_platform_duration_ms,
                "max_comments_per_post": max(0, int(max_comments_per_post)),
            },
            "status_deferred": not include_status,
        },
    }


def get_week_detail_summary_fast(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
) -> dict[str, Any]:
    _sync_core_overrides()
    started_at = time_module.perf_counter()
    context = get_season_context(season_id)
    available_platforms = _resolve_requested_platforms(platforms)
    now = _now_utc()
    week_windows, _week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {w.week_index: w for w in week_windows}
    window = windows_by_index.get(week_index)
    if window is None:
        raise ValueError(f"Week {week_index} is not available for this season")
    start_dt = window.start_local.astimezone(UTC)
    end_dt = (window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    if end_dt < start_dt:
        end_dt = start_dt

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    requires_target_accounts = source_scope in {"network", "creator", "news"}

    platform_results: dict[str, Any] = {}
    by_platform_duration_ms: dict[str, int] = {}
    grand_posts = 0
    grand_comments = 0
    grand_engagement = 0
    grand_expected_comments = 0
    grand_saved_comments = 0
    for platform in available_platforms:
        handler = _WEEK_DETAIL_SUMMARY_FAST_HANDLERS.get(platform)
        if not handler:
            continue
        account_handles = set(target_accounts_by_platform.get(platform, set()))
        if requires_target_accounts and not account_handles:
            empty_payload = {
                "total_posts": 0,
                "totals": {
                    "posts": 0,
                    "total_comments": 0,
                    "total_engagement": 0,
                    "expected_comments_total": 0,
                    "saved_comments_total": 0,
                    "comments_saved_pct": 0.0,
                },
            }
            platform_results[platform] = empty_payload
            by_platform_duration_ms[platform] = 0
            continue
        platform_started_at = time_module.perf_counter()
        result = handler(
            season_id=season_id,
            start_dt=start_dt,
            end_dt=end_dt,
            account_handles=account_handles,
            **({"source_scope": source_scope} if platform == "threads" else {}),
        )
        by_platform_duration_ms[platform] = int((time_module.perf_counter() - platform_started_at) * 1000)
        total_posts = int(result.get("total_posts") or 0)
        totals = result.get("totals") if isinstance(result.get("totals"), dict) else {}
        merged_totals = {
            "posts": total_posts,
            "total_comments": int(totals.get("total_comments") or 0),
            "total_engagement": int(totals.get("total_engagement") or 0),
            "expected_comments_total": int(totals.get("expected_comments_total") or 0),
            "saved_comments_total": int(totals.get("saved_comments_total") or 0),
            "comments_saved_pct": float(totals.get("comments_saved_pct") or 0.0),
        }
        platform_results[platform] = {
            "total_posts": total_posts,
            "totals": merged_totals,
        }
        grand_posts += total_posts
        grand_comments += int(merged_totals.get("total_comments") or 0)
        grand_engagement += int(merged_totals.get("total_engagement") or 0)
        grand_expected_comments += int(merged_totals.get("expected_comments_total") or 0)
        grand_saved_comments += int(merged_totals.get("saved_comments_total") or 0)

    week_end_inclusive = window.end_local - timedelta(microseconds=1)
    return {
        "week": {
            "week_index": week_index,
            "label": _week_window_label(window, timezone=timezone),
            "start": _iso(window.start_local.astimezone(UTC)),
            "end": _iso(week_end_inclusive.astimezone(UTC)),
            "week_type": window.week_type,
            "episode_number": window.episode_number,
        },
        "season": {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "show_name": context.show_name,
            "show_slug": context.show_slug,
            "season_number": context.season_number,
        },
        "source_scope": source_scope,
        "platforms": platform_results,
        "totals": {
            "posts": grand_posts,
            "total_comments": grand_comments,
            "total_engagement": grand_engagement,
            "expected_comments_total": grand_expected_comments,
            "saved_comments_total": grand_saved_comments,
            "comments_saved_pct": _safe_percent(grand_saved_comments, grand_expected_comments),
        },
        "meta": {
            "performance": {
                "total_duration_ms": int((time_module.perf_counter() - started_at) * 1000),
                "by_platform": by_platform_duration_ms,
            }
        },
    }


def get_week_detail_summary(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    max_comments_per_post: int = 0,
    sort_field: str = "posted_at",
    sort_dir: str = "desc",
) -> dict[str, Any]:
    _sync_core_overrides()
    payload = _room_callable("get_week_detail", get_week_detail)(
        season_id,
        week_index=week_index,
        platforms=platforms,
        timezone=timezone,
        source_scope=source_scope,
        max_comments_per_post=max_comments_per_post,
        post_limit=0,
        post_offset=0,
        sort_field=sort_field,
        sort_dir=sort_dir,
    )
    platforms_payload = payload.get("platforms") if isinstance(payload.get("platforms"), dict) else {}
    summarized_platforms: dict[str, Any] = {}
    for platform_name, platform_payload in platforms_payload.items():
        if not isinstance(platform_payload, dict):
            continue
        summarized_platforms[platform_name] = {
            "total_posts": int(platform_payload.get("total_posts") or 0),
            "totals": _normalize_week_totals_payload(
                platform_payload.get("totals"),
                total_posts=int(platform_payload.get("total_posts") or 0),
            ),
        }
    return {
        "week": payload.get("week"),
        "season": payload.get("season"),
        "source_scope": payload.get("source_scope"),
        "platforms": summarized_platforms,
        "totals": _normalize_week_totals_payload(payload.get("totals")),
        "meta": payload.get("meta") or {},
    }


def get_tiktok_overview(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    cast_member_id: str | None = None,
    hashtag: str | None = None,
    keyword: str | None = None,
    sound_id: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    where_sql, where_params = _tiktok_filter_sql(
        season_id=season_id,
        date_start=date_start,
        date_end=date_end,
        cast_member_id=cast_member_id,
        hashtag=hashtag,
        keyword=keyword,
        sound_id=sound_id,
    )
    saves_expr = _tiktok_saves_expr("p")
    row = (
        pg.fetch_one(
            f"""
        select
          count(*)::int as post_count,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as total_views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as total_likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as total_comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as total_shares,
          coalesce(sum({saves_expr}), 0)::bigint as total_saves,
          coalesce(
            avg(
              case
                when coalesce(p.views, 0) > 0
                  then (
                    coalesce(p.likes, 0)
                    + coalesce(p.comments_count, 0)
                    + coalesce(p.shares, 0)
                    + {saves_expr}
                  )::numeric / p.views
                else null
              end
            ),
            0
          )::numeric as avg_engagement_rate
        from social.tiktok_posts p
        where {where_sql}
        """,
            where_params,
        )
        or {}
    )

    daily_rows = pg.fetch_all(
        f"""
        select
          date_trunc('day', p.posted_at)::date as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )
    hourly_rows = pg.fetch_all(
        f"""
        select
          date_trunc('hour', p.posted_at) as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )
    weekly_rows = pg.fetch_all(
        f"""
        select
          date_trunc('week', p.posted_at)::date as period_start,
          count(*)::int as posts,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
          coalesce(sum({saves_expr}), 0)::bigint as saves
        from social.tiktok_posts p
        where {where_sql}
          and p.posted_at is not null
        group by period_start
        order by period_start asc
        """,
        where_params,
    )

    def _series_row_payload(series_row: Mapping[str, Any]) -> dict[str, Any]:
        views = int(series_row.get("views") or 0)
        likes = int(series_row.get("likes") or 0)
        comments = int(series_row.get("comments") or 0)
        shares = int(series_row.get("shares") or 0)
        saves = int(series_row.get("saves") or 0)
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        return {
            "period_start": str(series_row.get("period_start") or ""),
            "posts": int(series_row.get("posts") or 0),
            "views": views,
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "saves": saves,
            "engagement_rate": engagement_rate,
        }

    hourly_series = [_series_row_payload(series_row) for series_row in hourly_rows]
    daily_series = [_series_row_payload(series_row) for series_row in daily_rows]
    weekly_series = [_series_row_payload(series_row) for series_row in weekly_rows]

    wow_delta: dict[str, float | None] = {
        "views": None,
        "likes": None,
        "comments": None,
        "shares": None,
        "saves": None,
        "engagement_rate": None,
    }
    if len(weekly_series) >= 2:
        current = weekly_series[-1]
        previous = weekly_series[-2]

        def _pct_delta(current_value: Any, previous_value: Any) -> float | None:
            current_number = float(current_value or 0)
            previous_number = float(previous_value or 0)
            if previous_number <= 0:
                return None
            return round(((current_number - previous_number) / previous_number) * 100.0, 3)

        wow_delta = {
            "views": _pct_delta(current.get("views"), previous.get("views")),
            "likes": _pct_delta(current.get("likes"), previous.get("likes")),
            "comments": _pct_delta(current.get("comments"), previous.get("comments")),
            "shares": _pct_delta(current.get("shares"), previous.get("shares")),
            "saves": _pct_delta(current.get("saves"), previous.get("saves")),
            "engagement_rate": _pct_delta(current.get("engagement_rate"), previous.get("engagement_rate")),
        }

    return {
        "season_id": season_id,
        "kpis": {
            "post_count": int(row.get("post_count") or 0),
            "views": int(row.get("total_views") or 0),
            "likes": int(row.get("total_likes") or 0),
            "comments": int(row.get("total_comments") or 0),
            "shares": int(row.get("total_shares") or 0),
            "saves": int(row.get("total_saves") or 0),
            "engagement_rate": float(row.get("avg_engagement_rate") or 0.0),
            "follower_delta": None,
        },
        "wow_delta_pct": wow_delta,
        "time_series": {
            "hourly": hourly_series,
            "daily": daily_series,
            "weekly": weekly_series,
        },
    }


def get_tiktok_cast_members(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_cast_members"):
        return {"season_id": season_id, "cast_members": []}
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          coalesce(cm.cast_member_id::text, '') as cast_member_id,
          coalesce(nullif(cm.cast_member_name, ''), 'Unknown') as cast_member_name,
          count(distinct cm.post_id)::int as post_count,
          coalesce(
            avg(coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.shares, 0)),
            0
          )::numeric as avg_engagement,
          max(coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.shares, 0))::bigint as max_engagement
        from social.tiktok_post_cast_members cm
        join social.tiktok_posts p on p.id = cm.post_id
        where p.season_id = %s::uuid
          {date_sql}
        group by cast_member_id, cast_member_name
        order by avg_engagement desc, post_count desc, cast_member_name asc
        """,
        params,
    )
    return {
        "season_id": season_id,
        "cast_members": [
            {
                "cast_member_id": str(row.get("cast_member_id") or "") or None,
                "cast_member_name": str(row.get("cast_member_name") or ""),
                "post_count": int(row.get("post_count") or 0),
                "avg_engagement": float(row.get("avg_engagement") or 0.0),
                "top_post_engagement": int(row.get("max_engagement") or 0),
            }
            for row in rows
        ],
    }


def get_tiktok_hashtags(
    season_id: str,
    *,
    token_type: str = "hashtag",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_tokens"):
        return {"season_id": season_id, "token_type": token_type, "tokens": []}
    normalized_type = str(token_type or "hashtag").strip().lower()
    if normalized_type not in {"hashtag", "keyword", "mention"}:
        normalized_type = "hashtag"
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id, normalized_type]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          tk.token,
          tk.normalized_token,
          count(*)::int as use_count,
          count(distinct tk.post_id)::int as post_count
        from social.tiktok_post_tokens tk
        join social.tiktok_posts p on p.id = tk.post_id
        where p.season_id = %s::uuid
          and tk.token_type = %s
          {date_sql}
        group by tk.token, tk.normalized_token
        order by use_count desc, post_count desc, tk.normalized_token asc
        limit 250
        """,
        params,
    )
    return {
        "season_id": season_id,
        "token_type": normalized_type,
        "tokens": [
            {
                "token": str(row.get("token") or ""),
                "normalized_token": str(row.get("normalized_token") or ""),
                "use_count": int(row.get("use_count") or 0),
                "post_count": int(row.get("post_count") or 0),
            }
            for row in rows
        ],
    }


def get_tiktok_sounds(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    search: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    safe_limit = max(1, min(int(limit), 250))
    if not _platform_posts_has_column("tiktok", "sound_id"):
        return {"season_id": season_id, "sounds": []}

    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    where_parts = ["p.season_id = %s::uuid", "coalesce(p.sound_id, '') <> ''"]
    if start_dt:
        where_parts.append("p.posted_at >= %s")
        params.append(start_dt)
    if end_dt:
        where_parts.append("p.posted_at <= %s")
        params.append(end_dt)

    search_value = str(search or "").strip().lower()
    if search_value:
        where_parts.append(
            """
            (
              lower(coalesce(p.sound_id, '')) like %s
              or lower(coalesce(p.sound_title, '')) like %s
              or lower(coalesce(p.sound_author, '')) like %s
            )
            """
        )
        wildcard = f"%{search_value}%"
        params.extend([wildcard, wildcard, wildcard])

    saves_expr = _tiktok_saves_expr("p")
    rows = pg.fetch_all(
        f"""
        select
          p.sound_id,
          coalesce(nullif(max(p.sound_title), ''), nullif(max(s.title), ''), nullif(max(p.sound_id), '')) as title,
          coalesce(nullif(max(p.sound_author), ''), nullif(max(s.artist_name), '')) as artist_name,
          max(coalesce(p.sound_usage_count, 0))::bigint as usage_count,
          count(*)::int as creator_post_count,
          coalesce(sum(coalesce(p.views, 0)), 0)::bigint as creator_views,
          coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as creator_likes,
          coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as creator_comments,
          coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as creator_shares,
          coalesce(sum({saves_expr}), 0)::bigint as creator_saves,
          max(p.posted_at) as last_creator_post_at,
          max(s.last_seen_at) as last_seen_at
        from social.tiktok_posts p
        left join social.tiktok_sounds s on s.sound_id = p.sound_id
        where {" and ".join(where_parts)}
        group by p.sound_id
        order by usage_count desc, creator_post_count desc, p.sound_id asc
        limit %s
        """,
        [*params, safe_limit],
    )

    related_counts_by_sound: dict[str, int] = {}
    if _relation_exists("social.tiktok_sound_posts"):
        related_rows = pg.fetch_all(
            """
            select sound_id, count(*)::int as related_post_count
            from social.tiktok_sound_posts
            where sound_id = any(%s::text[])
            group by sound_id
            """,
            [[str(row.get("sound_id") or "") for row in rows if str(row.get("sound_id") or "")]],
        )
        related_counts_by_sound = {
            str(item.get("sound_id") or ""): int(item.get("related_post_count") or 0) for item in related_rows
        }

    sounds = []
    for row in rows:
        sound_id = str(row.get("sound_id") or "")
        if not sound_id:
            continue
        views = int(row.get("creator_views") or 0)
        likes = int(row.get("creator_likes") or 0)
        comments = int(row.get("creator_comments") or 0)
        shares = int(row.get("creator_shares") or 0)
        saves = int(row.get("creator_saves") or 0)
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        sounds.append(
            {
                "sound_id": sound_id,
                "title": str(row.get("title") or "") or None,
                "artist_name": str(row.get("artist_name") or "") or None,
                "usage_count": int(row.get("usage_count") or 0),
                "creator_post_count": int(row.get("creator_post_count") or 0),
                "creator_views": views,
                "creator_likes": likes,
                "creator_comments": comments,
                "creator_shares": shares,
                "creator_saves": saves,
                "creator_engagement_rate": engagement_rate,
                "related_post_count": int(related_counts_by_sound.get(sound_id, 0)),
                "last_creator_post_at": _iso(_coerce_dt(row.get("last_creator_post_at"))),
                "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
            }
        )
    return {"season_id": season_id, "sounds": sounds}


def get_tiktok_content_health(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    cast_member_id: str | None = None,
    hashtag: str | None = None,
    keyword: str | None = None,
    sound_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    where_sql, where_params = _tiktok_filter_sql(
        season_id=season_id,
        date_start=date_start,
        date_end=date_end,
        cast_member_id=cast_member_id,
        hashtag=hashtag,
        keyword=keyword,
        sound_id=sound_id,
    )
    safe_limit = max(1, min(int(limit), 250))
    saves_expr = _tiktok_saves_expr("p")
    quality_expr = "coalesce(p.quality_score, 0)" if _platform_posts_has_column("tiktok", "quality_score") else "0"
    velocity_expr = "coalesce(p.velocity_24h, 0)" if _platform_posts_has_column("tiktok", "velocity_24h") else "0"
    sound_id_expr = "coalesce(p.sound_id, '')" if _platform_posts_has_column("tiktok", "sound_id") else "''"
    sound_title_expr = "coalesce(p.sound_title, '')" if _platform_posts_has_column("tiktok", "sound_title") else "''"
    sound_author_expr = "coalesce(p.sound_author, '')" if _platform_posts_has_column("tiktok", "sound_author") else "''"
    canonical_url_expr = _tiktok_canonical_url_expr("p")
    rows = pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.video_id,
          p.source_account,
          p.posted_at,
          coalesce(p.description, '') as caption,
          coalesce(p.thumbnail_url, '') as thumbnail_url,
          {canonical_url_expr} as url,
          coalesce(p.views, 0)::bigint as views,
          coalesce(p.likes, 0)::bigint as likes,
          coalesce(p.comments_count, 0)::bigint as comments,
          coalesce(p.shares, 0)::bigint as shares,
          {saves_expr}::bigint as saves,
          {quality_expr}::numeric as quality_score,
          {velocity_expr}::numeric as velocity_24h,
          {sound_id_expr} as sound_id,
          {sound_title_expr} as sound_title,
          {sound_author_expr} as sound_author
        from social.tiktok_posts p
        where {where_sql}
        order by p.posted_at desc nulls last
        limit %s
        """,
        [*where_params, max(400, safe_limit)],
    )
    if not rows:
        return {"season_id": season_id, "thresholds": {}, "posts": []}

    saves_values = [int(row.get("saves") or 0) for row in rows]
    comments_values = [int(row.get("comments") or 0) for row in rows]
    velocity_values = [int(float(row.get("velocity_24h") or 0)) for row in rows]
    quality_values = [float(row.get("quality_score") or 0) for row in rows]

    median_saves = _median_int(saves_values)
    median_comments = _median_int(comments_values)
    median_velocity = _median_int(velocity_values)
    non_zero_quality = sorted(value for value in quality_values if value > 0)
    median_quality = non_zero_quality[len(non_zero_quality) // 2] if non_zero_quality else 0.0

    posts: list[dict[str, Any]] = []
    for row in rows:
        saves = int(row.get("saves") or 0)
        comments = int(row.get("comments") or 0)
        views = int(row.get("views") or 0)
        likes = int(row.get("likes") or 0)
        shares = int(row.get("shares") or 0)
        quality_score = float(row.get("quality_score") or 0)
        velocity_24h = float(row.get("velocity_24h") or 0)

        flags: list[str] = []
        if median_saves > 0 and saves < (median_saves * 0.5):
            flags.append("low_saves")
        if median_comments > 0 and comments < (median_comments * 0.5):
            flags.append("low_comments")
        if median_velocity > 0 and velocity_24h < (median_velocity * 0.5):
            flags.append("low_velocity_24h")
        if median_quality > 0 and quality_score > 0 and quality_score < (median_quality * 0.6):
            flags.append("low_quality_score")
        if not str(row.get("thumbnail_url") or "").strip():
            flags.append("missing_thumbnail")
        if not str(row.get("caption") or "").strip():
            flags.append("missing_caption")

        if not flags:
            continue

        health_score = max(0.0, round(100.0 - (len(flags) * 12.5), 2))
        engagement_rate = round(((likes + comments + shares + saves) / views), 6) if views > 0 else 0.0
        posts.append(
            {
                "post_id": str(row.get("video_id") or ""),
                "source_account": str(row.get("source_account") or "") or None,
                "posted_at": _iso(_coerce_dt(row.get("posted_at"))),
                "caption": str(row.get("caption") or "") or None,
                "thumbnail_url": str(row.get("thumbnail_url") or "") or None,
                "url": str(row.get("url") or "") or None,
                "sound_id": str(row.get("sound_id") or "") or None,
                "sound_title": str(row.get("sound_title") or "") or None,
                "sound_author": str(row.get("sound_author") or "") or None,
                "metrics": {
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "saves": saves,
                    "velocity_24h": velocity_24h,
                    "quality_score": quality_score,
                    "engagement_rate": engagement_rate,
                },
                "reason_flags": flags,
                "health_score": health_score,
            }
        )

    posts.sort(
        key=lambda item: (
            len(item.get("reason_flags") or []),
            -(float((item.get("metrics") or {}).get("engagement_rate") or 0)),
            str(item.get("posted_at") or ""),
        ),
        reverse=True,
    )

    return {
        "season_id": season_id,
        "thresholds": {
            "median_saves": median_saves,
            "median_comments": median_comments,
            "median_velocity_24h": median_velocity,
            "median_quality_score": round(median_quality, 6),
        },
        "posts": posts[:safe_limit],
    }


def get_tiktok_sound_detail(season_id: str, *, sound_id: str) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_sound_id = _normalize_tiktok_sound_id(sound_id)
    if not normalized_sound_id:
        raise ValueError("Invalid sound_id")
    if not _relation_exists("social.tiktok_sounds"):
        return {"season_id": season_id, "sound": None}
    row = pg.fetch_one(
        """
        select
          sound_id,
          title,
          artist_name,
          usage_count,
          source_url,
          last_seen_at,
          updated_at
        from social.tiktok_sounds
        where sound_id = %s
        """,
        [normalized_sound_id],
    )
    creator_post_count = 0
    if _platform_posts_has_column("tiktok", "sound_id"):
        creator_row = (
            pg.fetch_one(
                """
            select count(*)::int as count
            from social.tiktok_posts p
            where p.season_id = %s::uuid
              and coalesce(p.sound_id, '') = %s
            """,
                [season_id, normalized_sound_id],
            )
            or {}
        )
        creator_post_count = int(creator_row.get("count") or 0)
    return {
        "season_id": season_id,
        "sound": (
            {
                "sound_id": str(row.get("sound_id") or ""),
                "title": str(row.get("title") or "") or None,
                "artist_name": str(row.get("artist_name") or "") or None,
                "usage_count": int(row.get("usage_count") or 0),
                "source_url": str(row.get("source_url") or "") or None,
                "last_seen_at": _iso(_coerce_dt(row.get("last_seen_at"))),
                "updated_at": _iso(_coerce_dt(row.get("updated_at"))),
                "creator_post_count": creator_post_count,
            }
            if row
            else None
        ),
    }


def get_tiktok_sound_posts(
    season_id: str,
    *,
    sound_id: str,
    limit: int = 100,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_sound_id = _normalize_tiktok_sound_id(sound_id)
    if not normalized_sound_id:
        raise ValueError("Invalid sound_id")
    safe_limit = max(1, min(int(limit), 500))
    if not _relation_exists("social.tiktok_sound_posts"):
        return {"season_id": season_id, "sound_id": normalized_sound_id, "posts": []}
    rows = pg.fetch_all(
        """
        select
          sound_id,
          platform_post_id,
          creator_handle,
          posted_at,
          views,
          likes,
          comments,
          shares,
          thumbnail_url,
          caption
        from social.tiktok_sound_posts
        where sound_id = %s
        order by coalesce(views, 0) desc, coalesce(likes, 0) desc, posted_at desc nulls last
        limit %s
        """,
        [normalized_sound_id, safe_limit],
    )
    return {
        "season_id": season_id,
        "sound_id": normalized_sound_id,
        "posts": [
            {
                "platform_post_id": str(row.get("platform_post_id") or ""),
                "creator_handle": str(row.get("creator_handle") or "") or None,
                "posted_at": _iso(_coerce_dt(row.get("posted_at"))),
                "views": int(row.get("views") or 0),
                "likes": int(row.get("likes") or 0),
                "comments": int(row.get("comments") or 0),
                "shares": int(row.get("shares") or 0),
                "thumbnail_url": str(row.get("thumbnail_url") or "") or None,
                "caption": str(row.get("caption") or "") or None,
            }
            for row in rows
        ],
    }


def get_tiktok_post_detail(season_id: str, *, post_id: str) -> dict[str, Any]:
    _sync_core_overrides()
    payload = _room_callable("get_post_comments", get_post_comments)(season_id, platform="tiktok", source_id=post_id)
    cast_rows: list[dict[str, Any]] = []
    if _relation_exists("social.tiktok_post_cast_members"):
        cast_rows = pg.fetch_all(
            """
            select
              cm.cast_member_id::text as cast_member_id,
              cm.cast_member_name,
              cm.confidence,
              cm.source
            from social.tiktok_post_cast_members cm
            join social.tiktok_posts p on p.id = cm.post_id
            where p.season_id = %s::uuid
              and p.video_id = %s
            order by cm.confidence desc nulls last, cm.cast_member_name asc
            """,
            [season_id, post_id],
        )
    payload["cast_members"] = [
        {
            "cast_member_id": str(row.get("cast_member_id") or "") or None,
            "cast_member_name": str(row.get("cast_member_name") or "") or None,
            "confidence": float(row.get("confidence") or 0.0),
            "source": str(row.get("source") or "") or None,
        }
        for row in cast_rows
    ]
    return payload


def get_tiktok_sentiment_trends(
    season_id: str,
    *,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    if not _relation_exists("social.tiktok_post_comment_enrichment"):
        return {"season_id": season_id, "timeline": []}
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    params: list[Any] = [season_id]
    date_sql = ""
    if start_dt:
        date_sql += " and p.posted_at >= %s"
        params.append(start_dt)
    if end_dt:
        date_sql += " and p.posted_at <= %s"
        params.append(end_dt)
    rows = pg.fetch_all(
        f"""
        select
          date_trunc('day', coalesce(p.posted_at, now()))::date as day,
          sum(coalesce(e.positive_count, 0))::bigint as positive_count,
          sum(coalesce(e.neutral_count, 0))::bigint as neutral_count,
          sum(coalesce(e.negative_count, 0))::bigint as negative_count,
          sum(coalesce(e.toxicity_count, 0))::bigint as toxicity_count,
          sum(coalesce(e.cast_mentions_count, 0))::bigint as cast_mentions_count
        from social.tiktok_post_comment_enrichment e
        join social.tiktok_posts p on p.id = e.post_id
        where p.season_id = %s::uuid
          {date_sql}
        group by day
        order by day asc
        """,
        params,
    )
    timeline = []
    for row in rows:
        positive_count = int(row.get("positive_count") or 0)
        neutral_count = int(row.get("neutral_count") or 0)
        negative_count = int(row.get("negative_count") or 0)
        toxicity_count = int(row.get("toxicity_count") or 0)
        total = positive_count + neutral_count + negative_count
        controversy_score = round(((negative_count + toxicity_count) / max(total, 1)) * 100.0, 3) if total > 0 else 0.0
        timeline.append(
            {
                "day": str(row.get("day") or ""),
                "positive_count": positive_count,
                "neutral_count": neutral_count,
                "negative_count": negative_count,
                "toxicity_count": toxicity_count,
                "cast_mentions_count": int(row.get("cast_mentions_count") or 0),
                "controversy_score": controversy_score,
            }
        )
    return {"season_id": season_id, "timeline": timeline}


def get_post_comments(
    season_id: str,
    *,
    platform: str,
    source_id: str,
) -> dict[str, Any]:
    """Return ALL comments for a single post, threaded by parent_comment_id."""
    _sync_core_overrides()
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform: {platform}")

    if platform == "instagram":
        thumbnail_expr = _instagram_posts_thumbnail_expr("p")
        hosted_media_urls_expr = _instagram_posts_json_array_expr("p", "hosted_media_urls")
        post_format_expr = _instagram_posts_json_text_expr("p", "post_format")
        profile_tags_expr = _instagram_posts_json_array_expr("p", "profile_tags")
        collaborators_expr = _instagram_posts_json_array_expr("p", "collaborators")
        hashtags_expr = _instagram_posts_json_array_expr("p", "hashtags")
        mentions_expr = _instagram_posts_json_array_expr("p", "mentions")
        duration_expr = _instagram_posts_duration_expr("p")
        mirror_attempt_count_expr = _instagram_posts_json_int_expr("p", "media_mirror_attempt_count")
        mirror_last_attempt_at_expr = _instagram_posts_json_timestamptz_expr("p", "media_mirror_last_attempt_at")
        mirror_last_job_id_expr = _instagram_posts_json_text_expr("p", "media_mirror_last_job_id")
        repost_count_expr = _instagram_posts_json_int_expr("p", "media_repost_count")

        post = pg.fetch_one(
            f"""
            select p.id, p.shortcode as source_id, p.username as author, p.caption as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.views, 0) as views,
                   coalesce({repost_count_expr}, 0) as shares,
                   p.media_type,
                   coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
                   {hosted_media_urls_expr} as hosted_media_urls,
                   coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   {thumbnail_expr} as thumbnail_url,
                   {post_format_expr} as post_format,
                   {profile_tags_expr} as profile_tags,
                   {collaborators_expr} as collaborators,
                   coalesce(to_jsonb(p) -> 'tagged_users_detail', '[]'::jsonb) as tagged_users_detail,
                   coalesce(to_jsonb(p) -> 'collaborators_detail', '[]'::jsonb) as collaborators_detail,
                   coalesce(to_jsonb(p) -> 'child_posts_data', '[]'::jsonb) as child_posts_data,
                   {hashtags_expr} as hashtags,
                   {mentions_expr} as mentions,
                   {duration_expr} as duration_seconds,
                   coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
                   {mirror_attempt_count_expr} as media_mirror_attempt_count,
                   {mirror_last_attempt_at_expr} as media_mirror_last_attempt_at,
                   {mirror_last_job_id_expr} as media_mirror_last_job_id,
                   p.posted_at as ts
            from social.instagram_posts p
            where p.season_id = %s and p.shortcode = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.username as author,
                   c.user_id,
                   nullif(coalesce(to_jsonb(c) ->> 'author_full_name', ''), '') as author_full_name,
                   nullif(coalesce(to_jsonb(c) ->> 'author_profile_pic_url', ''), '') as author_profile_pic_url,
                   nullif(
                     coalesce(to_jsonb(c) ->> 'hosted_author_profile_pic_url', ''),
                     ''
                   ) as hosted_author_profile_pic_url,
                   nullif(coalesce(to_jsonb(c) ->> 'author_profile_pic_url_hd', ''), '') as author_profile_pic_url_hd,
                   case
                     when lower(coalesce(to_jsonb(c) ->> 'author_is_verified', '')) in ('true', 'false')
                     then (to_jsonb(c) ->> 'author_is_verified')::boolean
                     else null
                   end as author_is_verified,
                   nullif(
                     coalesce(to_jsonb(c) ->> 'parent_comment_external_id', ''),
                     ''
                   ) as parent_comment_external_id,
                   nullif(coalesce(to_jsonb(c) ->> 'source_snapshot_type', ''), '') as source_snapshot_type,
                   case
                     when coalesce(to_jsonb(c) ->> 'reply_depth', '') ~ '^[0-9]+$'
                     then (to_jsonb(c) ->> 'reply_depth')::int
                     else null
                   end as reply_depth,
                   c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                   coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status,
                   c.created_at
            from social.instagram_comments c
            where c.post_id = %s
              and coalesce(c.is_missing, false) = false
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        shares = _normalize_non_negative_int(post.get("shares"))
        engagement = post["likes"] + post["comments_count"] + post["views"] + shares
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        profile_tags = _json_text_list(post.get("profile_tags"), prefix="@", strip_prefix="@")
        collaborators = _json_text_list(post.get("collaborators"), prefix="@", strip_prefix="@")
        tagged_users_detail = _as_json_object_list(post.get("tagged_users_detail"))
        collaborators_detail = _as_json_object_list(post.get("collaborators_detail"))
        child_posts_data = _as_json_object_list(post.get("child_posts_data"))
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        thumbnail_url = str(post.get("thumbnail_url") or "").strip() or source_thumbnail_url
        cover_source, cover_source_confidence = _instagram_cover_source_from_post_row(
            {
                "media_type": post.get("media_type"),
                "post_format": post.get("post_format"),
                "raw_data": post.get("raw_data"),
                "thumbnail_url": thumbnail_url,
                "source_media_urls": source_media_urls,
                "media_urls": source_media_urls,
            }
        )
        return {
            "platform": "instagram",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "url": f"https://www.instagram.com/p/{source_id}/",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": thumbnail_url,
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "cover_source": cover_source,
            "cover_source_confidence": cover_source_confidence,
            "post_format": post.get("post_format"),
            "profile_tags": profile_tags,
            "collaborators": collaborators,
            "tagged_users_detail": tagged_users_detail,
            "profile_tags_detail": tagged_users_detail,
            "mentions_detail": tagged_users_detail,
            "collaborators_detail": collaborators_detail,
            "child_posts_data": child_posts_data,
            "hashtags": hashtags,
            "mentions": mentions,
            "duration_seconds": post.get("duration_seconds"),
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(post.get("raw_data")),
            "media_mirror_attempt_count": post.get("media_mirror_attempt_count"),
            "media_mirror_last_attempt_at": _iso(_coerce_dt(post.get("media_mirror_last_attempt_at"))),
            "media_mirror_last_job_id": str(post.get("media_mirror_last_job_id") or "") or None,
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": shares,
                "reposts": shares,
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "tiktok":
        thumbnail_expr = _platform_thumbnail_expr("p", "tiktok")
        tiktok_saves_expr = _tiktok_saves_expr("p")
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)" if _platform_posts_has_column("tiktok", "mentions") else "'[]'::jsonb"
        )
        active_comment_filter = (
            " and coalesce(c.is_missing, false) = false" if _comment_lifecycle_supported("tiktok_comments") else ""
        )
        post = pg.fetch_one(
            f"""
            select p.id, p.video_id as source_id, p.username as author, p.description as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.shares, 0) as shares, {tiktok_saves_expr} as saves, coalesce(p.views, 0) as views,
                   coalesce(p.hashtags, '[]'::jsonb) as hashtags,
                   {mentions_expr} as mentions,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
                   coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
                   p.posted_at as ts
            from social.tiktok_posts p
            where p.season_id = %s and p.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            f"""
            select c.id, c.comment_id, c.parent_comment_id,
                   coalesce(nullif(c.username, ''), nullif(c.source_account, ''), '') as author,
                   c.user_id,
                   c.nickname,
                   c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at,
                   nullif(coalesce(to_jsonb(c) ->> 'comment_language', ''), '') as comment_language,
                   case
                     when lower(coalesce(to_jsonb(c) ->> 'is_author_liked', '')) in ('true', 'false')
                     then (to_jsonb(c) ->> 'is_author_liked')::boolean
                     else null
                   end as is_author_liked,
                   nullif(coalesce(to_jsonb(c) ->> 'aweme_id', ''), '') as aweme_id,
                   nullif(coalesce(to_jsonb(c) ->> 'parent_source_comment_id', ''), '') as parent_source_comment_id,
                   nullif(coalesce(to_jsonb(c) ->> 'user_url', ''), '') as user_url,
                   nullif(coalesce(to_jsonb(c) ->> 'user_bio', ''), '') as user_bio,
                   nullif(coalesce(to_jsonb(c) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                   nullif(coalesce(to_jsonb(c) ->> 'user_region', ''), '') as user_region,
                   nullif(coalesce(to_jsonb(c) ->> 'user_language', ''), '') as user_language,
                   coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                   coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status
            from social.tiktok_comments c
            where c.post_id = %s
              {active_comment_filter}
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["likes"] + post["comments_count"] + post["shares"] + post["views"]
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        return {
            "platform": "tiktok",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": f"https://www.tiktok.com/@{post['author'] or ''}/video/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(post.get("raw_data")),
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": post["shares"],
                "saves": int(post.get("saves") or 0),
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "youtube":
        thumbnail_expr = _platform_thumbnail_expr("v", "youtube")
        youtube_is_short_expr = _youtube_is_short_expr("v")
        youtube_duration_expr = (
            "coalesce(v.duration_seconds, 0)" if _platform_posts_has_column("youtube", "duration_seconds") else "0"
        )
        hashtags_expr = (
            "coalesce(v.hashtags, '[]'::jsonb)" if _platform_posts_has_column("youtube", "hashtags") else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(v.mentions, '[]'::jsonb)" if _platform_posts_has_column("youtube", "mentions") else "'[]'::jsonb"
        )
        transcript_text_expr = (
            "v.transcript_text" if _platform_posts_has_column("youtube", "transcript_text") else "null::text"
        )
        transcript_segments_expr = (
            "coalesce(v.transcript_segments, '[]'::jsonb)"
            if _platform_posts_has_column("youtube", "transcript_segments")
            else "'[]'::jsonb"
        )
        transcript_language_expr = (
            "v.transcript_language" if _platform_posts_has_column("youtube", "transcript_language") else "null::text"
        )
        transcript_source_expr = (
            "v.transcript_source" if _platform_posts_has_column("youtube", "transcript_source") else "null::text"
        )
        transcript_synced_at_expr = (
            "v.transcript_synced_at"
            if _platform_posts_has_column("youtube", "transcript_synced_at")
            else "null::timestamptz"
        )
        transcript_error_expr = (
            "v.transcript_error" if _platform_posts_has_column("youtube", "transcript_error") else "null::text"
        )
        post = pg.fetch_one(
            f"""
            select v.id, v.video_id as source_id, v.channel_title as author,
                   v.title, v.description as text,
                   coalesce(v.views, 0) as views, coalesce(v.likes, 0) as likes,
                   coalesce(v.comments_count, 0) as comments_count,
                   {youtube_duration_expr} as duration_seconds,
                   {hashtags_expr} as hashtags,
                   {mentions_expr} as mentions,
                   {youtube_is_short_expr} as is_short,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(to_jsonb(v) -> 'media_urls', '[]'::jsonb) as source_media_urls,
                   coalesce(to_jsonb(v) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   coalesce(nullif(v.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(v) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   coalesce(to_jsonb(v) -> 'raw_data', '{{}}'::jsonb) as raw_data,
                   {transcript_text_expr} as transcript_text,
                   {transcript_segments_expr} as transcript_segments,
                   {transcript_language_expr} as transcript_language,
                   {transcript_source_expr} as transcript_source,
                   {transcript_synced_at_expr} as transcript_synced_at,
                   {transcript_error_expr} as transcript_error,
                   v.published_at as ts
            from social.youtube_videos v
            where v.season_id = %s and v.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        youtube_comment_active_filter = (
            " and coalesce(c.is_missing, false) = false" if _comment_lifecycle_supported("youtube_comments") else ""
        )

        comments = pg.fetch_all(
            f"""
            select c.id, c.comment_id, c.parent_comment_id,
                   c.author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.youtube_comments c
            where c.video_id = %s
              {youtube_comment_active_filter}
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        effective_comments_count = _youtube_effective_comment_count(
            reported_count=post["comments_count"],
            saved_count=len(comments),
        )
        engagement = post["views"] + post["likes"] + effective_comments_count
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        normalized_title, normalized_text = _normalize_youtube_title_description(
            post.get("title"),
            post.get("text"),
            is_short=bool(post.get("is_short")),
        )
        token_text = f"{normalized_title}\n{normalized_text}"
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(token_text)
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(token_text)
        return {
            "platform": "youtube",
            "source_id": source_id,
            "author": post["author"] or "",
            "title": normalized_title,
            "text": normalized_text,
            "hashtags": hashtags,
            "mentions": mentions,
            "url": (
                f"https://www.youtube.com/shorts/{source_id}"
                if bool(post.get("is_short"))
                else f"https://www.youtube.com/watch?v={source_id}"
            ),
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(post.get("raw_data")),
            "duration_seconds": _normalize_non_negative_int(post.get("duration_seconds")),
            "transcript_text": str(post.get("transcript_text") or "") or None,
            "transcript_segments": (
                list(post.get("transcript_segments") or []) if isinstance(post.get("transcript_segments"), list) else []
            ),
            "transcript_language": str(post.get("transcript_language") or "") or None,
            "transcript_source": str(post.get("transcript_source") or "") or None,
            "transcript_synced_at": _iso(_coerce_dt(post.get("transcript_synced_at"))),
            "transcript_error": str(post.get("transcript_error") or "") or None,
            "stats": {
                "views": post["views"],
                "likes": post["likes"],
                "comments_count": effective_comments_count,
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "twitter":
        thumbnail_expr = _platform_thumbnail_expr("t", "twitter")
        hosted_media_expr = _platform_hosted_media_expr("t")
        post = pg.fetch_one(
            f"""
            select t.tweet_id as source_id,
                   coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                   nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
                   coalesce(
                     nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                     case
                       when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                       else 'https://x.com/' || ltrim(
                         coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                         '@'
                       )
                     end
                   ) as user_profile_url,
                   nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                   t.display_name, t.text,
                   coalesce(t.likes, 0) as likes,
                   coalesce(t.retweets, 0) as retweets,
                   coalesce(t.replies_count, 0) as replies_count,
                   coalesce(t.quotes, 0) as quotes,
                   coalesce(t.views, 0) as views,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(t.media_urls, '[]'::jsonb) as source_media_urls,
                   {hosted_media_expr} as hosted_media_urls,
                   coalesce(nullif(to_jsonb(t) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   coalesce(to_jsonb(t) -> 'raw_data', '{{}}'::jsonb) as raw_data,
                   t.created_at as ts
            from social.twitter_tweets t
            where t.season_id = %s and t.tweet_id = %s and t.is_reply = false
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        twitter_comment_active_filter = (
            " and coalesce(is_missing, false) = false" if _comment_lifecycle_supported("twitter_tweets") else ""
        )

        # Full recursive reply chain
        reply_rows = pg.fetch_all(
            f"""
            with recursive thread_replies as (
              select r.tweet_id, r.reply_to_tweet_id,
                     r.reply_to_tweet_id as parent_id
              from social.twitter_tweets r
              where r.season_id = %s
                and r.is_reply = true
                and r.reply_to_tweet_id = %s
                {twitter_comment_active_filter}
              union
              select child.tweet_id, child.reply_to_tweet_id,
                     child.reply_to_tweet_id as parent_id
              from social.twitter_tweets child
              join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
              where child.season_id = %s
                and child.is_reply = true
                {twitter_comment_active_filter.replace("is_missing", "child.is_missing")}
            )
            select
              t.tweet_id as id,
              t.tweet_id as comment_id,
              t.reply_to_tweet_id as parent_comment_id,
              coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
              coalesce(t.display_name, '') as display_name,
              nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
              coalesce(
                nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                case
                  when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                  else 'https://x.com/' || ltrim(
                    coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                    '@'
                  )
                end
              ) as user_url,
              nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
              t.text,
              coalesce(t.likes, 0) as likes,
              true as is_reply,
              coalesce(t.replies_count, 0) as reply_count,
              t.media_urls,
              {hosted_media_expr} as hosted_media_urls,
              t.created_at
            from thread_replies tr
            join social.twitter_tweets t on t.tweet_id = tr.tweet_id
            where t.season_id = %s
              {twitter_comment_active_filter.replace("is_missing", "t.is_missing")}
            order by t.likes desc nulls last, t.created_at asc
            """,
            [season_id, source_id, season_id, season_id],
        )

        quote_rows = pg.fetch_all(
            f"""
            select
              t.tweet_id as comment_id,
              coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
              nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
              coalesce(
                nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                case
                  when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                  else 'https://x.com/' || ltrim(
                    coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                    '@'
                  )
                end
              ) as user_url,
              nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
              coalesce(t.display_name, '') as display_name,
              t.text,
              coalesce(t.likes, 0) as likes,
              coalesce(t.retweets, 0) as retweets,
              coalesce(t.replies_count, 0) as reply_count,
              coalesce(t.quotes, 0) as quotes,
              coalesce(t.views, 0) as views,
              t.media_urls,
              {hosted_media_expr} as hosted_media_urls,
              {thumbnail_expr} as thumbnail_url,
              t.created_at
            from social.twitter_tweets t
            where t.season_id = %s
              and t.is_quote = true
              and t.quoted_tweet_id = %s
              {twitter_comment_active_filter.replace("is_missing", "t.is_missing")}
            order by t.likes desc nulls last, t.created_at asc
            """,
            [season_id, source_id],
        )

        # Thread replies: direct replies to root go under root, deeper replies nest
        threaded: list[dict[str, Any]] = []
        by_id: dict[str, dict[str, Any]] = {}
        for r in reply_rows:
            node = {**r, "replies": []}
            by_id[r["id"]] = node
        for r in reply_rows:
            node = by_id[r["id"]]
            parent_id = r.get("parent_comment_id")
            if parent_id == source_id or parent_id not in by_id:
                threaded.append(node)
            else:
                by_id[parent_id]["replies"].append(node)

        reposts = max(0, int(post.get("reposts") or post["retweets"] or 0))
        engagement = post["likes"] + post["retweets"] + post["replies_count"] + post["quotes"] + post["views"]
        author = str(post.get("author") or "").strip().lstrip("@")
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = _select_thumbnail_candidate(source_media_urls) if source_media_urls else None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        return {
            "platform": "twitter",
            "source_id": source_id,
            "author": post["author"] or "",
            "user": {
                "id": post.get("user_id"),
                "username": post["author"] or "",
                "display_name": post.get("display_name") or "",
                "url": post.get("user_url"),
                "avatar_url": post.get("user_avatar_url"),
            },
            "display_name": post.get("display_name") or "",
            "text": post.get("text") or "",
            "url": f"https://x.com/{author}/status/{source_id}" if author and source_id else "",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(post.get("raw_data")),
            "stats": {
                "likes": post["likes"],
                "retweets": post["retweets"],
                "reposts": reposts,
                "replies_count": post["replies_count"],
                "quotes": post["quotes"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(reply_rows),
            "total_quotes_in_db": len(quote_rows),
            "comments": [_serialize_comment_tree(node) for node in threaded],
            "quotes": [
                {
                    "comment_id": q["comment_id"],
                    "author": q["author"] or "",
                    "display_name": q.get("display_name") or "",
                    "url": (
                        f"https://x.com/{str(q.get('author') or '').strip().lstrip('@')}/status/{q['comment_id']}"
                        if str(q.get("author") or "").strip().lstrip("@") and q.get("comment_id")
                        else ""
                    ),
                    "text": q["text"] or "",
                    "likes": q["likes"],
                    "retweets": q["retweets"],
                    "reposts": max(0, int(q.get("reposts") or q["retweets"] or 0)),
                    "reply_count": q["reply_count"],
                    "quotes": q["quotes"],
                    "views": q["views"],
                    "is_reply": False,
                    "media_urls": q.get("media_urls"),
                    "hosted_media_urls": q.get("hosted_media_urls"),
                    "thumbnail_url": q.get("thumbnail_url"),
                    "created_at": _iso(q["created_at"]),
                    "user": {
                        "id": q.get("user_id"),
                        "username": q["author"] or "",
                        "display_name": q.get("display_name") or "",
                        "url": q.get("user_url"),
                        "avatar_url": q.get("user_avatar_url"),
                    },
                }
                for q in quote_rows
            ],
        }

    if platform == "facebook":
        thumbnail_expr = _platform_thumbnail_expr("p", "facebook")
        hashtags_expr = (
            "coalesce(p.hashtags, '[]'::jsonb)" if _platform_posts_has_column("facebook", "hashtags") else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)" if _platform_posts_has_column("facebook", "mentions") else "'[]'::jsonb"
        )
        post = pg.fetch_one(
            f"""
            select
              p.id,
              p.post_id as source_id,
              coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
              p.caption as text,
              coalesce(p.post_type, 'feed') as post_type,
              coalesce(p.likes, 0) as likes,
              coalesce(p.comments_count, 0) as comments_count,
              coalesce(p.shares, 0) as shares,
              coalesce(p.views, 0) as views,
              {hashtags_expr} as hashtags,
              {mentions_expr} as mentions,
              {thumbnail_expr} as thumbnail_url,
              coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
              coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
              coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
              coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
              p.posted_at as ts
            from social.facebook_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        comments = pg.fetch_all(
            """
            select
              c.id,
              c.comment_id,
              c.parent_comment_id,
              c.username as author,
              c.text,
              coalesce(c.likes, 0) as likes,
              coalesce(c.is_reply, false) as is_reply,
              coalesce(c.reply_count, 0) as reply_count,
              c.created_at
            from social.facebook_comments c
            where c.post_id = %s
              and coalesce(c.is_missing, false) = false
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )
        engagement = post["likes"] + post["comments_count"] + post["shares"] + post["views"]
        raw_data = post.get("raw_data") if isinstance(post.get("raw_data"), dict) else {}
        url_candidates = [
            str((raw_data or {}).get("url") or "").strip(),
            str((raw_data or {}).get("permalink_url") or "").strip(),
            str((raw_data or {}).get("permalink") or "").strip(),
            f"https://www.facebook.com/{post['author'] or ''}/posts/{source_id}" if post.get("author") else "",
            f"https://www.facebook.com/reel/{source_id}",
        ]
        url = next((candidate for candidate in url_candidates if candidate), "")
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        return {
            "platform": "facebook",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": url,
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(raw_data),
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": post["shares"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "threads":
        thumbnail_expr = _platform_thumbnail_expr("p", "threads")
        hashtags_expr = (
            "coalesce(p.hashtags, '[]'::jsonb)" if _platform_posts_has_column("threads", "hashtags") else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)" if _platform_posts_has_column("threads", "mentions") else "'[]'::jsonb"
        )
        threads_comment_active_filter = (
            "and coalesce(c.is_missing, false) = false" if _comment_lifecycle_supported("meta_threads_comments") else ""
        )
        post = pg.fetch_one(
            f"""
            select
              p.id,
              p.post_id as source_id,
              coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
              p.text,
              coalesce(p.likes, 0) as likes,
              coalesce(p.replies_count, 0) as replies_count,
              coalesce(p.reposts, 0) as reposts,
              coalesce(p.quotes, 0) as quotes,
              coalesce(p.views, 0) as views,
              {hashtags_expr} as hashtags,
              {mentions_expr} as mentions,
              {thumbnail_expr} as thumbnail_url,
              coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
              coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
              coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
              coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
              p.posted_at as ts
            from social.meta_threads_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        context = get_season_context(season_id)
        if _threads_should_enforce_rhoslc_relevance(context=context):
            hashtags, keywords = _threads_build_relevance_terms(
                season_id,
                source_scope="network",
                context=context,
            )
            if not _threads_text_topic_match(
                text=post.get("text"),
                raw_data=post.get("raw_data"),
                hashtags=hashtags,
                keywords=keywords,
            ):
                raise ValueError("Post not found")
        comment_rows = pg.fetch_all(
            f"""
            select
              c.id,
              c.comment_id,
              c.parent_comment_id,
              c.username as author,
              c.text,
              coalesce(c.likes, 0) as likes,
              coalesce(c.is_reply, true) as is_reply,
              coalesce(c.reply_count, 0) as reply_count,
              c.created_at,
              coalesce(to_jsonb(c) -> 'raw_data', '{{}}'::jsonb) as raw_data,
              coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
              coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status
            from social.meta_threads_comments c
            where c.post_id = %s
              {threads_comment_active_filter}
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )
        replies_flat: list[dict[str, Any]] = []
        quotes_flat: list[dict[str, Any]] = []
        for row in comment_rows:
            if _threads_is_quote_interaction(is_reply=row.get("is_reply"), raw_data=row.get("raw_data")):
                quotes_flat.append(row)
            else:
                replies_flat.append(row)
        interaction_counts = _count_stored_threads_interactions([str(post["id"])])
        interaction_summary = interaction_counts.get(str(post["id"]), {})
        total_replies_in_db = int(interaction_summary.get("replies") or len(replies_flat))
        total_quotes_in_db = int(interaction_summary.get("quotes") or len(quotes_flat))
        engagement = post["likes"] + post["replies_count"] + post["reposts"] + post["quotes"] + post["views"]
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        topic = _threads_extract_topic(post.get("raw_data"), text=post.get("text"))
        return {
            "platform": "threads",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "topic": topic,
            "hashtags": hashtags,
            "mentions": mentions,
            "url": f"https://www.threads.com/@{post['author'] or ''}/post/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "media_asset_meta": _extract_media_asset_meta_from_raw_data(post.get("raw_data")),
            "stats": {
                "likes": post["likes"],
                "replies_count": post["replies_count"],
                "reposts": post["reposts"],
                "quotes": post["quotes"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": total_replies_in_db,
            "total_replies_in_db": total_replies_in_db,
            "total_quotes_in_db": total_quotes_in_db,
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(replies_flat)],
            "quotes": [_serialize_comment_tree(node) for node in quotes_flat],
        }

    raise ValueError(f"Unsupported platform: {platform}")


def build_csv(snapshot: dict[str, Any]) -> str:
    _sync_core_overrides()
    rows = snapshot.get("rows") or []
    summary = snapshot.get("summary") or {}
    data_quality = summary.get("data_quality") if isinstance(summary.get("data_quality"), dict) else {}
    weekly_flags = snapshot.get("weekly_flags") if isinstance(snapshot.get("weekly_flags"), list) else []
    benchmark = snapshot.get("benchmark") if isinstance(snapshot.get("benchmark"), dict) else {}
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "week_index",
            "platform",
            "kind",
            "source_id",
            "timestamp",
            "author",
            "url",
            "engagement",
            "sentiment",
            "text",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("week_index"),
                row.get("platform"),
                row.get("kind"),
                row.get("source_id"),
                row.get("timestamp"),
                row.get("author"),
                row.get("url"),
                row.get("engagement"),
                row.get("sentiment"),
                row.get("text"),
            ]
        )
    writer.writerow([])
    writer.writerow(["section", "key", "value"])

    if data_quality:
        writer.writerow(["data_quality", "comments_saved_pct_overall", data_quality.get("comments_saved_pct_overall")])
        writer.writerow(["data_quality", "data_freshness_minutes", data_quality.get("data_freshness_minutes")])
        writer.writerow(["data_quality", "last_post_at", data_quality.get("last_post_at")])
        writer.writerow(["data_quality", "last_comment_at", data_quality.get("last_comment_at")])

    for flag in weekly_flags:
        if not isinstance(flag, dict):
            continue
        writer.writerow(
            [
                "weekly_flags",
                f"week_{flag.get('week_index')}_{flag.get('code')}",
                flag.get("message"),
            ]
        )

    if benchmark:
        current = benchmark.get("current") if isinstance(benchmark.get("current"), dict) else {}
        prev = benchmark.get("previous_week") if isinstance(benchmark.get("previous_week"), dict) else {}
        trailing = (
            benchmark.get("trailing_3_week_avg") if isinstance(benchmark.get("trailing_3_week_avg"), dict) else {}
        )
        writer.writerow(["benchmark", "week_index", benchmark.get("week_index")])
        writer.writerow(["benchmark", "current_posts", current.get("posts")])
        writer.writerow(["benchmark", "current_comments", current.get("comments")])
        writer.writerow(["benchmark", "current_engagement", current.get("engagement")])
        writer.writerow(["benchmark", "previous_week_index", prev.get("week_index")])
        writer.writerow(["benchmark", "trailing_window_size", trailing.get("window_size")])
    return output.getvalue()


def build_pdf(snapshot: dict[str, Any]) -> bytes:
    _sync_core_overrides()
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("PDF export requires reportlab") from exc

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=LETTER,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()

    summary = snapshot.get("summary") or {}
    window = snapshot.get("window") or {}
    weekly = snapshot.get("weekly") or []
    platform_breakdown = snapshot.get("platform_breakdown") or []
    themes = snapshot.get("themes") or {}
    discussions = (snapshot.get("leaderboards") or {}).get("viewer_discussion") or []
    data_quality = summary.get("data_quality") if isinstance(summary.get("data_quality"), dict) else {}
    weekly_flags = snapshot.get("weekly_flags") if isinstance(snapshot.get("weekly_flags"), list) else []
    benchmark = snapshot.get("benchmark") if isinstance(snapshot.get("benchmark"), dict) else {}

    story = []
    story.append(Paragraph("Season Social Analytics Report", styles["Title"]))
    story.append(
        Paragraph(
            (
                f"Show: {summary.get('show_name') or 'Unknown'} | "
                f"Season: {summary.get('season_number') or 'N/A'} | "
                f"Window: {window.get('start')} to {window.get('end')} | "
                f"Timezone: {window.get('timezone') or 'America/New_York'}"
            ),
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    summary_table = Table(
        [
            ["Metric", "Value"],
            ["Total Posts", str(summary.get("total_posts") or 0)],
            ["Total Comments", str(summary.get("total_comments") or 0)],
            ["Total Engagement", str(summary.get("total_engagement") or 0)],
            [
                "Sentiment Mix",
                (
                    f"P {((summary.get('sentiment_mix') or {}).get('positive') or 0):.1%} | "
                    f"N {((summary.get('sentiment_mix') or {}).get('neutral') or 0):.1%} | "
                    f"Neg {((summary.get('sentiment_mix') or {}).get('negative') or 0):.1%}"
                ),
            ],
        ],
        hAlign="LEFT",
        colWidths=[2.1 * inch, 4.6 * inch],
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d4d4d8")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 0.2 * inch))

    if data_quality:
        story.append(Paragraph("Data Quality", styles["Heading2"]))
        quality_rows = [
            ["Metric", "Value"],
            ["Coverage (Overall)", str(data_quality.get("comments_saved_pct_overall"))],
            ["Freshness (Minutes)", str(data_quality.get("data_freshness_minutes"))],
            ["Last Post", str(data_quality.get("last_post_at") or "-")],
            ["Last Comment", str(data_quality.get("last_comment_at") or "-")],
        ]
        quality_table = Table(quality_rows, hAlign="LEFT", colWidths=[2.2 * inch, 4.5 * inch])
        quality_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3f3f46")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(quality_table)
        story.append(Spacer(1, 0.2 * inch))

    if weekly:
        story.append(Paragraph("Weekly Trend", styles["Heading2"]))
        weekly_table_data = [["Week", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in weekly[:16]:
            sentiment = row.get("sentiment") or {}
            weekly_table_data.append(
                [
                    row.get("label"),
                    row.get("post_volume"),
                    row.get("comment_volume"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        weekly_table = Table(weekly_table_data, hAlign="LEFT")
        weekly_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#334155")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(weekly_table)
        story.append(Spacer(1, 0.2 * inch))

    if weekly_flags:
        story.append(Paragraph("Weekly Flags", styles["Heading2"]))
        flags_data = [["Week", "Code", "Severity", "Message"]]
        for flag in weekly_flags[:40]:
            if not isinstance(flag, dict):
                continue
            flags_data.append(
                [
                    str(flag.get("week_index")),
                    str(flag.get("code") or ""),
                    str(flag.get("severity") or ""),
                    str(flag.get("message") or ""),
                ]
            )
        flags_table = Table(flags_data, hAlign="LEFT")
        flags_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#52525b")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(flags_table)
        story.append(Spacer(1, 0.2 * inch))

    if benchmark:
        story.append(Paragraph("Benchmark", styles["Heading2"]))
        benchmark_rows = [
            ["Metric", "Value"],
            ["Week Index", str(benchmark.get("week_index"))],
            ["Current Posts", str((benchmark.get("current") or {}).get("posts"))],
            ["Current Comments", str((benchmark.get("current") or {}).get("comments"))],
            ["Current Engagement", str((benchmark.get("current") or {}).get("engagement"))],
            ["Prev Week Index", str((benchmark.get("previous_week") or {}).get("week_index"))],
            ["Trailing Window", str((benchmark.get("trailing_3_week_avg") or {}).get("window_size"))],
        ]
        benchmark_table = Table(benchmark_rows, hAlign="LEFT", colWidths=[2.2 * inch, 4.5 * inch])
        benchmark_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(benchmark_table)
        story.append(Spacer(1, 0.2 * inch))

    if platform_breakdown:
        story.append(Paragraph("Platform Breakdown", styles["Heading2"]))
        platform_table_data = [["Platform", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in platform_breakdown:
            sentiment = row.get("sentiment") or {}
            platform_table_data.append(
                [
                    row.get("platform"),
                    row.get("posts"),
                    row.get("comments"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        platform_table = Table(platform_table_data, hAlign="LEFT")
        platform_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1d4ed8")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(platform_table)

    story.append(PageBreak())
    story.append(Paragraph("Appendix", styles["Title"]))

    positive_themes = themes.get("positive") or []
    negative_themes = themes.get("negative") or []

    story.append(Paragraph("Top Positive Drivers", styles["Heading2"]))
    if positive_themes:
        for theme in positive_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No positive drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Top Negative Drivers", styles["Heading2"]))
    if negative_themes:
        for theme in negative_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No negative drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("Viewer Discussion Highlights", styles["Heading2"]))
    discussion_table_data = [["Platform", "Sentiment", "Engagement", "Comment Excerpt"]]
    for row in discussions[:20]:
        text = str(row.get("text") or "").strip()
        excerpt = text if len(text) <= 140 else f"{text[:137]}..."
        discussion_table_data.append(
            [
                row.get("platform"),
                row.get("sentiment"),
                row.get("engagement"),
                excerpt,
            ]
        )

    if len(discussion_table_data) == 1:
        discussion_table_data.append(["-", "-", "-", "No comments available in this filter window."])

    discussion_table = Table(
        discussion_table_data,
        hAlign="LEFT",
        colWidths=[0.9 * inch, 0.9 * inch, 0.9 * inch, 4.9 * inch],
    )
    discussion_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f766e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(discussion_table)

    doc.build(story)
    return buffer.getvalue()


def pdf_filename(show_id: str, season_number: int, generated_at: datetime | None = None) -> str:
    _sync_core_overrides()
    ts = (generated_at or _now_utc()).strftime("%Y%m%d")
    return f"social_report_{show_id}_s{season_number}_{ts}.pdf"


_LOCAL_ROOM_NAMES = {
    "get_week_live_health_snapshot",
    "get_analytics",
    "get_comments_coverage",
    "get_mirror_coverage",
    "get_week_detail",
    "get_week_detail_summary_fast",
    "get_week_detail_summary",
    "get_tiktok_overview",
    "get_tiktok_cast_members",
    "get_tiktok_hashtags",
    "get_tiktok_sounds",
    "get_tiktok_content_health",
    "get_tiktok_sound_detail",
    "get_tiktok_sound_posts",
    "get_tiktok_post_detail",
    "get_tiktok_sentiment_trends",
    "get_post_comments",
    "build_csv",
    "build_pdf",
    "pdf_filename",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "get_week_live_health_snapshot",
    "get_analytics",
    "get_comments_coverage",
    "get_mirror_coverage",
    "get_week_detail",
    "get_week_detail_summary_fast",
    "get_week_detail_summary",
    "get_tiktok_overview",
    "get_tiktok_cast_members",
    "get_tiktok_hashtags",
    "get_tiktok_sounds",
    "get_tiktok_content_health",
    "get_tiktok_sound_detail",
    "get_tiktok_sound_posts",
    "get_tiktok_post_detail",
    "get_tiktok_sentiment_trends",
    "get_post_comments",
    "build_csv",
    "build_pdf",
    "pdf_filename",
]
