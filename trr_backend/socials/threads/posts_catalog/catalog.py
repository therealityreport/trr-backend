"""Posts-only Threads shared catalog orchestration.

This module is intentionally callback-wired so the legacy compatibility wrapper
can supply repository-owned helpers without this package importing repository
surfaces or the claimed-job ``threads/posts_scrapling`` lane.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from trr_backend.socials.threads.scraper import ThreadsScrapeConfig, ThreadsScraper

SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE = "shared_account_catalog_backfill"

ProgressCallback = Callable[[dict[str, Any]], None]


def _empty_cookies() -> dict[str, str]:
    return {}


def _coerce_dt(value: Any) -> Any:
    return value


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _normalize_account_handle(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    candidate = raw
    if "://" in raw or raw.lower().startswith("www."):
        url_value = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(url_value)
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        candidate = path_parts[0] if path_parts else str(parsed.netloc or "")

    return candidate.strip().lstrip("@").split("?")[0].split("#")[0].split("/")[0].strip().lower()


def _platform_profile_url_for_handle(platform: str, handle: Any) -> str | None:
    normalized_handle = _normalize_account_handle(handle)
    if not normalized_handle:
        return None
    if str(platform or "").strip().lower() == "threads":
        return f"https://www.threads.com/@{normalized_handle}"
    return None


def _upgrade_profile_avatar_url_variant(value: Any) -> str | None:
    candidate = str(value or "").strip()
    if not candidate.startswith(("http://", "https://")):
        return None

    parsed = urlparse(candidate)
    host = str(parsed.netloc or "").lower()
    if not host or ("instagram.com" not in host and "fbcdn.net" not in host and "facebook.com" not in host):
        return candidate

    query_items = [(key, val) for key, val in parse_qsl(parsed.query, keep_blank_values=True) if key.lower() != "stp"]
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query_items), parsed.fragment)
    )


def _profile_avatar_quality_score(value: str) -> tuple[int, int]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return (-1, -1)

    score = 0
    if any(
        token in normalized
        for token in (
            "profile_pic",
            "profile_picture",
            "profile_image",
            "profilephoto",
            "avatar",
            "cropcenter:1080",
        )
    ):
        score += 50
    if any(
        token in normalized
        for token in (
            "thumb",
            "thumbnail",
            "small",
            "tiny",
            "s32x32",
            "s48x48",
            "s64x64",
            "s96x96",
            "s100x100",
        )
    ):
        score -= 30

    max_dim = 0
    for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
    for match in re.finditer(r"cropcenter:(\d{2,4}):(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
    return (score + min(max_dim, 4096), len(normalized))


def _best_profile_avatar_url(candidates: list[Any]) -> str | None:
    best_url: str | None = None
    best_score: tuple[int, int] = (-1, -1)
    seen: set[str] = set()
    for candidate in candidates:
        for variant in (candidate, _upgrade_profile_avatar_url_variant(candidate)):
            normalized = str(variant or "").strip()
            if not normalized or not normalized.startswith(("http://", "https://")):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            score = _profile_avatar_quality_score(normalized)
            if score > best_score:
                best_score = score
                best_url = normalized
    return best_url


def _merge_social_profile_snapshots(*snapshots: Mapping[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for snapshot in snapshots:
        metadata = _metadata_dict(snapshot)
        if not metadata:
            continue
        for key in ("username", "display_name", "bio", "avatar_url", "profile_url", "channel_id"):
            value = str(metadata.get(key) or "").strip()
            if value and not str(merged.get(key) or "").strip():
                merged[key] = value
        for key in ("follower_count", "following_count", "total_posts"):
            if metadata.get(key) is None:
                continue
            merged[key] = max(
                _normalize_non_negative_int(merged.get(key)),
                _normalize_non_negative_int(metadata.get(key)),
            )
        if bool(metadata.get("is_verified")):
            merged["is_verified"] = True
    return merged


def _shared_catalog_mode(config: Mapping[str, Any] | None) -> bool:
    return (
        str((config or {}).get("pipeline_ingest_mode") or "").strip().lower()
        == SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
    )


def _shared_stage_post_limit(config: Mapping[str, Any] | None, *, default: int = 100) -> int | None:
    raw_limit = None if config is None else config.get("max_posts_per_target")
    if raw_limit is None:
        return default
    try:
        parsed = int(raw_limit)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return None
    return parsed


_EMPTY_SOFT_BLOCK_MARKERS = (
    "soft_block",
    "soft-block",
    "blocked",
    "checkpoint",
    "challenge",
    "login",
    "auth_failed",
    "forbidden",
    "unauthorized",
    "rate_limited",
    "empty_response",
    "empty_payload",
    "temporarily_unavailable",
)


def _empty_soft_block_reason(retrieval_meta: Mapping[str, Any]) -> str | None:
    meta = _metadata_dict(retrieval_meta)
    if _normalize_non_negative_int(meta.get("matched_posts")) > 0:
        return None
    stop_reason = str(meta.get("stop_reason") or "").strip()
    if (
        stop_reason == "no_edges"
        and str(meta.get("source") or "").strip() == "threads_graphql_api"
        and _normalize_non_negative_int(meta.get("posts_checked")) == 0
    ):
        return stop_reason
    for key in (
        "empty_result_reason",
        "stop_reason",
        "fetch_reason",
        "block_reason",
        "session_block_reason",
        "profile_fetch_mode",
        "error_message",
        "redirect_target",
    ):
        value = str(meta.get(key) or "").strip()
        normalized = value.lower()
        if value and any(marker in normalized for marker in _EMPTY_SOFT_BLOCK_MARKERS):
            return value
    return None


def _classify_empty_soft_block_result(retrieval_meta: MutableMapping[str, Any], *, rows: Sequence[Any]) -> None:
    if rows or retrieval_meta.get("error_code"):
        return
    reason = _empty_soft_block_reason(retrieval_meta)
    if not reason:
        return
    retrieval_meta["empty_result_reason"] = reason
    retrieval_meta["error_code"] = "threads_empty_soft_block"
    retrieval_meta["error_class"] = "ThreadsEmptySoftBlock"
    retrieval_meta["retryable"] = True
    retrieval_meta["complete"] = False


def _resolve_threads_delay_seconds() -> float:
    return max(0.5, float(os.getenv("SOCIAL_THREADS_DELAY_SEC", "1.0")))


@dataclass(slots=True)
class ThreadsPostsCatalogDependencies:
    scraper_factory: Callable[..., Any] = ThreadsScraper
    scrape_config_factory: Callable[..., Any] = ThreadsScrapeConfig
    load_cookies: Callable[[], Mapping[str, str]] = _empty_cookies
    coerce_dt: Callable[[Any], Any] = _coerce_dt
    shared_stage_post_limit: Callable[..., int | None] = _shared_stage_post_limit
    shared_catalog_mode: Callable[[Mapping[str, Any] | None], bool] = _shared_catalog_mode
    metadata_dict: Callable[[Any], dict[str, Any]] = _metadata_dict
    normalize_account_handle: Callable[[Any], str] = _normalize_account_handle
    merge_social_profile_snapshots: Callable[..., dict[str, Any]] = _merge_social_profile_snapshots
    best_profile_avatar_url: Callable[[list[Any]], str | None] = _best_profile_avatar_url
    platform_profile_url_for_handle: Callable[[str, Any], str | None] = _platform_profile_url_for_handle
    persist_shared_catalog_posts_with_progress: Callable[..., list[dict[str, Any]]] | None = None
    upsert_shared_catalog_post: Callable[..., dict[str, Any] | None] | None = None
    upsert_threads_post: Callable[..., dict[str, Any] | None] | None = None


def _first_non_empty_attr(items: Sequence[Any], attr: str) -> str | None:
    for item in items:
        value = str(getattr(item, attr, "") or "").strip()
        if value:
            return value
    return None


def _threads_profile_snapshot_from_posts(
    posts: Sequence[Any],
    account_handle: str,
    *,
    deps: ThreadsPostsCatalogDependencies,
) -> dict[str, Any]:
    username = deps.normalize_account_handle(_first_non_empty_attr(posts, "username") or account_handle)
    display_name = next(
        (
            str(deps.metadata_dict(getattr(post, "raw_data", {})).get("full_name") or "").strip()
            for post in posts
            if str(deps.metadata_dict(getattr(post, "raw_data", {})).get("full_name") or "").strip()
        ),
        None,
    )
    avatar_url = deps.best_profile_avatar_url([_first_non_empty_attr(posts, "user_avatar_url")])
    snapshot = {
        "username": username or deps.normalize_account_handle(account_handle),
        "display_name": display_name,
        "avatar_url": avatar_url,
        "is_verified": any(
            bool(deps.metadata_dict(getattr(post, "raw_data", {})).get("is_verified")) for post in posts
        ),
        "profile_url": deps.platform_profile_url_for_handle("threads", account_handle),
    }
    return {key: value for key, value in snapshot.items() if value is not None and value != ""}


def _persist_shared_catalog_posts(
    *,
    deps: ThreadsPostsCatalogDependencies,
    run_id: str | None,
    account_handle: str,
    posts: Sequence[Any],
    retrieval_meta: MutableMapping[str, Any],
    progress_cb: ProgressCallback | None,
) -> list[dict[str, Any]]:
    if deps.persist_shared_catalog_posts_with_progress is None:
        raise RuntimeError("persist_shared_catalog_posts_with_progress dependency is required in shared catalog mode")
    upsert_shared_catalog_post = deps.upsert_shared_catalog_post
    if upsert_shared_catalog_post is None:
        raise RuntimeError("upsert_shared_catalog_post dependency is required in shared catalog mode")

    return deps.persist_shared_catalog_posts_with_progress(
        platform="threads",
        run_id=run_id,
        account_handle=account_handle,
        items=posts,
        retrieval_meta=retrieval_meta,
        progress_cb=progress_cb,
        upsert_item=lambda post: upsert_shared_catalog_post(
            platform="threads",
            run_id=run_id,
            account_handle=account_handle,
            post=post,
        ),
    )


def _persist_threads_posts(
    *,
    deps: ThreadsPostsCatalogDependencies,
    job_id: str,
    account_handle: str,
    posts: Sequence[Any],
) -> list[dict[str, Any]]:
    if deps.upsert_threads_post is None:
        raise RuntimeError("upsert_threads_post dependency is required outside shared catalog mode")

    rows: list[dict[str, Any]] = []
    for post in posts:
        row = deps.upsert_threads_post(None, job_id=job_id, account=account_handle, post=post)
        if row:
            rows.append(row)
    return rows


def scrape_shared_threads_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: ProgressCallback | None = None,
    dependencies: ThreadsPostsCatalogDependencies | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    deps = dependencies or ThreadsPostsCatalogDependencies()
    config = config or {}
    scraper = deps.scraper_factory(cookies=dict(deps.load_cookies() or {}))
    post_limit = deps.shared_stage_post_limit(config, default=0)
    scrape_config = deps.scrape_config_factory(
        username=account_handle,
        date_start=deps.coerce_dt(config.get("date_start")),
        date_end=deps.coerce_dt(config.get("date_end")),
        delay_seconds=_resolve_threads_delay_seconds(),
        max_pages=post_limit if post_limit and post_limit > 0 else None,
    )
    posts = list(scraper.scrape(scrape_config, progress_cb=progress_cb))
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})

    if deps.shared_catalog_mode(config):
        rows = _persist_shared_catalog_posts(
            deps=deps,
            run_id=run_id,
            account_handle=account_handle,
            posts=posts,
            retrieval_meta=retrieval_meta,
            progress_cb=progress_cb,
        )
    else:
        rows = _persist_threads_posts(
            deps=deps,
            job_id=job_id,
            account_handle=account_handle,
            posts=posts,
        )

    retrieval_meta["profile_snapshot"] = deps.merge_social_profile_snapshots(
        deps.metadata_dict(config.get("profile_snapshot")),
        _threads_profile_snapshot_from_posts(posts, account_handle, deps=deps),
    )
    _classify_empty_soft_block_result(retrieval_meta, rows=rows)
    return rows, retrieval_meta
