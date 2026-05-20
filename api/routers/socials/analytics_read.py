"""Request-shaping helpers for season analytics reads."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AnalyticsIncludeOptions:
    include_rows: bool
    include_flags: bool
    include_schedule: bool
    include_benchmark: bool


def parse_analytics_include(include: str | None) -> AnalyticsIncludeOptions:
    include_set: set[str] | None = None
    if include and include.strip():
        include_set = {item.strip().lower() for item in include.split(",") if item.strip()}

    return AnalyticsIncludeOptions(
        include_rows=bool(include_set and "rows" in include_set),
        include_flags=include_set is None or "flags" in include_set,
        include_schedule=include_set is None or "schedule" in include_set,
        include_benchmark=include_set is None or "benchmark" in include_set,
    )


def analytics_read_path_extra(
    *,
    cache: str,
    source_scope: str,
    week: int | None,
    platforms: list[str] | None,
) -> dict[str, Any]:
    return {
        "cache": cache,
        "source_scope": source_scope,
        "week": week,
        "platforms": ",".join(platforms) if platforms else "all",
    }


def _coerce_week_detail_numeric(value: Any) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def sort_week_detail_posts(
    posts: list[tuple[str, str, dict[str, Any]]],
    *,
    sort_field: str,
    sort_dir: str,
) -> None:
    reverse = sort_dir == "desc"
    if sort_field == "posted_at":
        posts.sort(
            key=lambda item: str((item[2] if isinstance(item[2], dict) else {}).get("posted_at") or ""),
            reverse=reverse,
        )
        return
    posts.sort(
        key=lambda item: (
            _coerce_week_detail_numeric((item[2] if isinstance(item[2], dict) else {}).get(sort_field)),
            str((item[2] if isinstance(item[2], dict) else {}).get("posted_at") or ""),
        ),
        reverse=reverse,
    )


def week_detail_cached_post_counts(payload: dict[str, Any]) -> tuple[int, int]:
    cached_posts = 0
    cached_total = 0
    for platform_payload in (payload.get("platforms") or {}).values():
        platform_posts = platform_payload.get("posts") if isinstance(platform_payload, dict) else []
        cached_posts += len(platform_posts) if isinstance(platform_posts, list) else 0
        fallback_count = len(platform_posts) if isinstance(platform_posts, list) else 0
        cached_total += int(platform_payload.get("total_posts", fallback_count) or 0)
    return cached_posts, cached_total


def page_week_detail_payload(
    payload: dict[str, Any],
    *,
    post_limit: int,
    post_offset: int,
    sort_field: str,
    sort_dir: str,
) -> dict[str, Any]:
    paged_payload = copy.deepcopy(payload)
    total_posts = 0
    all_posts: list[tuple[str, str, dict[str, Any]]] = []
    source_index_cache: dict[str, set[str]] = {}

    for platform_name, platform_payload in (paged_payload.get("platforms") or {}).items():
        platform_posts = platform_payload.get("posts") if isinstance(platform_payload, dict) else []
        if isinstance(platform_posts, list):
            for post in platform_posts:
                if isinstance(post, dict):
                    source_id = str(post.get("source_id") or "").strip()
                    post_key = f"{platform_name}:{source_id}"
                    if post_key in source_index_cache.get(platform_name, set()):
                        continue
                    source_index_cache.setdefault(platform_name, set()).add(post_key)
                    all_posts.append((str(post.get("posted_at") or ""), platform_name, post))
        fallback_total_posts = len(platform_posts) if isinstance(platform_posts, list) else 0
        total_posts += int(platform_payload.get("total_posts", fallback_total_posts) or 0)

    sort_week_detail_posts(all_posts, sort_field=sort_field, sort_dir=sort_dir)
    page_end = post_offset + post_limit
    page_posts = all_posts[post_offset:page_end]
    posts_by_platform: dict[str, list[dict[str, Any]]] = {}
    for page_index, (_, platform_name, post) in enumerate(page_posts, start=post_offset):
        post["sort_rank"] = page_index
        posts_by_platform.setdefault(platform_name, []).append(post)

    if isinstance(paged_payload.get("totals"), dict):
        paged_payload["totals"]["posts"] = total_posts

    for _platform_name, platform_payload in (paged_payload.get("platforms") or {}).items():
        if isinstance(platform_payload, dict):
            platform_payload["totals"] = platform_payload.get("totals") or {}
            platform_posts = platform_payload.get("posts")
            if isinstance(platform_payload.get("total_posts"), int):
                platform_payload["totals"]["posts"] = int(platform_payload["total_posts"] or 0)
            elif isinstance(platform_posts, list):
                platform_payload["totals"]["posts"] = int(len(platform_posts))

    for platform_name, platform_payload in (paged_payload.get("platforms") or {}).items():
        platform_payload["posts"] = posts_by_platform.get(platform_name, [])
        if not platform_payload["posts"]:
            platform_payload["posts"] = []

    paged_payload["pagination"] = {
        "limit": post_limit,
        "offset": post_offset,
        "returned": len(page_posts),
        "total": total_posts,
        "has_more": page_end < total_posts,
    }
    return paged_payload
