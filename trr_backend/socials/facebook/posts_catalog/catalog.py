"""Posts-only Facebook shared catalog orchestration.

The module is wired through callbacks for repository-owned helpers so the
Facebook package can be imported without importing the legacy monolith back.
"""

from __future__ import annotations

import inspect
import os
import re
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from trr_backend.socials.facebook.document_fetch import FacebookDocumentFetcher
from trr_backend.socials.facebook.scraper import FacebookScrapeConfig, FacebookScraper

SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE = "shared_account_catalog_backfill"
GENERIC_ACCOUNT_HANDLE_RE = re.compile(r"^[a-z0-9._-]{1,64}$")

ProgressCallback = Callable[[dict[str, Any]], None]


def _empty_cookie_loader() -> dict[str, str]:
    return {}


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

    candidate = candidate.strip().lstrip("@")
    candidate = candidate.split("?")[0].split("#")[0].split("/")[0].strip().lower()
    if not candidate or not GENERIC_ACCOUNT_HANDLE_RE.fullmatch(candidate):
        return ""
    return candidate


def _platform_profile_url_for_handle(platform: str, handle: Any) -> str | None:
    normalized_handle = _normalize_account_handle(handle)
    if not normalized_handle:
        return None
    if str(platform or "").strip().lower() == "facebook":
        return f"https://www.facebook.com/{normalized_handle}"
    return None


def _upgrade_profile_avatar_url_variant(value: Any) -> str | None:
    candidate = str(value or "").strip()
    if not candidate.startswith(("http://", "https://")):
        return None

    upgraded = candidate
    parsed = urlparse(upgraded)
    host = str(parsed.netloc or "").lower()
    if host and ("fbcdn.net" in host or "facebook.com" in host):
        query_items = [
            (key, item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() != "stp"
        ]
        upgraded = urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query_items), parsed.fragment)
        )
    return upgraded


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
            "profile_images",
            "profile_image",
            "profilephoto",
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
            "s50x50",
            "s64x64",
            "s96x96",
            "s100x100",
            "s150x150",
            "s200x200",
        )
    ):
        score -= 30

    max_dim = 0
    for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
    for match in re.finditer(r"cropcenter:(\d{2,4}):(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)), int(match.group(2)))
    for match in re.finditer(r"profile_pic[^0-9]{0,8}(\d{2,4})", normalized):
        max_dim = max(max_dim, int(match.group(1)))
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


def _facebook_profile_snapshot_from_posts(posts: Sequence[Any], account_handle: str) -> dict[str, Any]:
    normalized_account = _normalize_account_handle(account_handle) or account_handle
    display_name = next(
        (
            str(getattr(post, "username", "") or "").strip()
            for post in posts
            if str(getattr(post, "username", "") or "").strip()
        ),
        None,
    )
    avatar_url = _best_profile_avatar_url(
        [
            next(
                (
                    str(getattr(post, "user_avatar_url", "") or "").strip()
                    for post in posts
                    if str(getattr(post, "user_avatar_url", "") or "").strip()
                ),
                None,
            )
        ]
    )
    snapshot = {
        "username": normalized_account,
        "display_name": display_name,
        "avatar_url": avatar_url,
        "profile_url": _platform_profile_url_for_handle("facebook", normalized_account),
    }
    return {key: item for key, item in snapshot.items() if item}


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


def _coerce_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


@dataclass(slots=True)
class FacebookPostsCatalogDependencies:
    scraper_factory: Callable[..., Any] = FacebookScraper
    document_fetcher_factory: Callable[..., Any] | None = FacebookDocumentFetcher
    scrape_config_factory: Callable[..., Any] = FacebookScrapeConfig
    load_cookies: Callable[[], Mapping[str, str] | None] = _empty_cookie_loader
    coerce_dt: Callable[[Any], datetime | None] = _coerce_dt
    shared_stage_post_limit: Callable[..., int | None] = _shared_stage_post_limit
    shared_catalog_mode: Callable[[Mapping[str, Any] | None], bool] = _shared_catalog_mode
    metadata_dict: Callable[[Any], dict[str, Any]] = _metadata_dict
    merge_social_profile_snapshots: Callable[..., dict[str, Any]] = _merge_social_profile_snapshots
    facebook_profile_snapshot_from_posts: Callable[[Sequence[Any], str], dict[str, Any]] = (
        _facebook_profile_snapshot_from_posts
    )
    persist_shared_catalog_posts_with_progress: Callable[..., list[dict[str, Any]]] | None = None
    upsert_shared_catalog_post: Callable[..., dict[str, Any] | None] | None = None
    upsert_facebook_post: Callable[..., dict[str, Any] | None] | None = None


def _factory_accepts_keyword(factory: Callable[..., Any], keyword: str) -> bool:
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter.name == keyword:
            return True
    return False


def _build_scraper(deps: FacebookPostsCatalogDependencies) -> Any:
    cookies = dict(deps.load_cookies() or {})
    kwargs: dict[str, Any] = {"cookies": cookies}
    if deps.document_fetcher_factory is not None and _factory_accepts_keyword(
        deps.scraper_factory,
        "document_fetcher_factory",
    ):
        kwargs["document_fetcher_factory"] = deps.document_fetcher_factory
    return deps.scraper_factory(**kwargs)


def _persist_shared_catalog_posts(
    *,
    deps: FacebookPostsCatalogDependencies,
    run_id: str | None,
    account_handle: str,
    posts: Sequence[Any],
    retrieval_meta: MutableMapping[str, Any],
    progress_cb: ProgressCallback | None,
) -> list[dict[str, Any]]:
    if deps.persist_shared_catalog_posts_with_progress is None:
        raise RuntimeError("persist_shared_catalog_posts_with_progress dependency is required in shared catalog mode")
    if deps.upsert_shared_catalog_post is None:
        raise RuntimeError("upsert_shared_catalog_post dependency is required in shared catalog mode")

    return deps.persist_shared_catalog_posts_with_progress(
        platform="facebook",
        run_id=run_id,
        account_handle=account_handle,
        items=posts,
        retrieval_meta=retrieval_meta,
        progress_cb=progress_cb,
        upsert_item=lambda post: deps.upsert_shared_catalog_post(
            platform="facebook",
            run_id=run_id,
            account_handle=account_handle,
            post=post,
        ),
    )


def _persist_facebook_posts(
    *,
    deps: FacebookPostsCatalogDependencies,
    job_id: str,
    account_handle: str,
    posts: Sequence[Any],
) -> list[dict[str, Any]]:
    if deps.upsert_facebook_post is None:
        raise RuntimeError("upsert_facebook_post dependency is required outside shared catalog mode")

    rows: list[dict[str, Any]] = []
    for post in posts:
        row = deps.upsert_facebook_post(None, job_id=job_id, account=account_handle, post=post)
        if row:
            rows.append(row)
    return rows


def scrape_shared_facebook_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: ProgressCallback | None = None,
    dependencies: FacebookPostsCatalogDependencies | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    deps = dependencies or FacebookPostsCatalogDependencies()
    scraper = _build_scraper(deps)
    scrape_config = deps.scrape_config_factory(
        page_handle=account_handle,
        date_start=deps.coerce_dt(config.get("date_start")),
        date_end=deps.coerce_dt(config.get("date_end")),
        delay_seconds=max(0.5, float(os.getenv("SOCIAL_FACEBOOK_DELAY_SEC", "1.0"))),
        max_pages=deps.shared_stage_post_limit(config, default=5),
        include_feed=True,
        include_reels=True,
        include_photos=True,
        max_scroll_iterations=int(os.getenv("SOCIAL_FACEBOOK_MAX_SCROLL_ITERATIONS", "50")),
    )
    posts = list(scraper.scrape(scrape_config, progress_cb=progress_cb))
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    retrieval_meta["profile_snapshot"] = deps.merge_social_profile_snapshots(
        deps.metadata_dict(config.get("profile_snapshot")),
        deps.facebook_profile_snapshot_from_posts(posts, account_handle),
    )

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
        rows = _persist_facebook_posts(
            deps=deps,
            job_id=job_id,
            account_handle=account_handle,
            posts=posts,
        )
    return rows, retrieval_meta
