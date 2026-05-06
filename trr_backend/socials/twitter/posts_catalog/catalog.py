"""Posts-only Twitter/X shared catalog orchestration.

The module is intentionally wired through callbacks for monolith-owned helpers,
auth, scraper construction, and persistence so platform code can be imported
without pulling repository surfaces back into the Twitter package.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from trr_backend.socials.twitter.scraper import TwitterScrapeConfig, TwitterScraper

SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE = "shared_account_catalog_backfill"

ProgressCallback = Callable[[dict[str, Any]], None]


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


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
    return candidate


def _first_non_empty_str(*values: Any) -> str | None:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def _platform_profile_url_for_handle(platform: str, handle: Any) -> str | None:
    normalized_handle = _normalize_account_handle(handle)
    if not normalized_handle:
        return None
    if str(platform or "").strip().lower() == "twitter":
        return f"https://x.com/{normalized_handle}"
    return None


def _upgrade_profile_avatar_url_variant(value: Any) -> str | None:
    candidate = str(value or "").strip()
    if not candidate.startswith(("http://", "https://")):
        return None

    upgraded = candidate
    if "pbs.twimg.com/profile_images/" in upgraded:
        upgraded = re.sub(r"_(normal|bigger|mini)(\.[A-Za-z0-9]+)$", r"_400x400\2", upgraded, flags=re.IGNORECASE)

    parsed = urlparse(upgraded)
    host = str(parsed.netloc or "").lower()
    if host and ("instagram.com" in host or "fbcdn.net" in host or "facebook.com" in host):
        query_items = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() != "stp"]
        upgraded = urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query_items), parsed.fragment)
        )
    return upgraded


def _profile_avatar_quality_score(value: str) -> tuple[int, int]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return (-1, -1)

    score = 0
    if "profile_images" in normalized or "profile_image" in normalized or "avatar" in normalized:
        score += 50
    if any(token in normalized for token in ("thumb", "thumbnail", "_mini", "_normal", "_bigger")):
        score -= 30

    max_dim = 0
    for match in re.finditer(r"(?:s)?(\d{2,4})x(\d{2,4})", normalized):
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


def _shared_catalog_progress_pages_scanned(retrieval_meta: Mapping[str, Any] | None) -> int:
    meta = _metadata_dict(retrieval_meta)
    direct = _normalize_non_negative_int(meta.get("pages_scanned"))
    surface_sum = _normalize_non_negative_int(meta.get("videos_pages_scanned")) + _normalize_non_negative_int(
        meta.get("shorts_pages_scanned")
    )
    return max(direct, surface_sum)


def _shared_catalog_progress_posts_checked(retrieval_meta: Mapping[str, Any] | None, *, matched_posts: int) -> int:
    meta = _metadata_dict(retrieval_meta)
    return max(
        _normalize_non_negative_int(meta.get("posts_checked")),
        _normalize_non_negative_int(meta.get("checked_renderers")),
        max(0, int(matched_posts)),
    )


def _load_twitter_auth() -> tuple[dict[str, str], str | None]:
    return {}, None


def _load_twikit_credentials(_twitter_cookies: Mapping[str, Any] | None = None) -> dict[str, str] | None:
    return None


@dataclass(slots=True)
class TwitterPostsCatalogDependencies:
    scraper_factory: Callable[..., Any] = TwitterScraper
    scrape_config_factory: Callable[..., Any] = TwitterScrapeConfig
    load_twitter_auth: Callable[[], tuple[dict[str, str], str | None]] = _load_twitter_auth
    load_twikit_credentials: Callable[[Mapping[str, Any] | None], dict[str, str] | None] = _load_twikit_credentials
    coerce_dt: Callable[[Any], datetime | None] = _coerce_dt
    now_utc: Callable[[], datetime] = _now_utc
    shared_stage_post_limit: Callable[..., int | None] = _shared_stage_post_limit
    shared_catalog_mode: Callable[[Mapping[str, Any] | None], bool] = _shared_catalog_mode
    metadata_dict: Callable[[Any], dict[str, Any]] = _metadata_dict
    normalize_account_handle: Callable[[Any], str] = _normalize_account_handle
    merge_social_profile_snapshots: Callable[..., dict[str, Any]] = _merge_social_profile_snapshots
    best_profile_avatar_url: Callable[[list[Any]], str | None] = _best_profile_avatar_url
    platform_profile_url_for_handle: Callable[[str, Any], str | None] = _platform_profile_url_for_handle
    first_non_empty_str: Callable[..., str | None] = _first_non_empty_str
    shared_catalog_progress_posts_checked: Callable[..., int] = _shared_catalog_progress_posts_checked
    shared_catalog_progress_pages_scanned: Callable[[Mapping[str, Any] | None], int] = (
        _shared_catalog_progress_pages_scanned
    )
    persist_shared_catalog_posts_with_progress: Callable[..., list[dict[str, Any]]] | None = None
    upsert_shared_catalog_post: Callable[..., dict[str, Any] | None] | None = None
    upsert_tweet: Callable[..., dict[str, Any] | None] | None = None


def _build_scraper(deps: TwitterPostsCatalogDependencies) -> Any:
    twitter_cookies, twitter_bearer = deps.load_twitter_auth()
    return deps.scraper_factory(
        cookies=twitter_cookies,
        bearer_token=twitter_bearer,
        twikit_credentials=deps.load_twikit_credentials(twitter_cookies),
    )


def _resolve_window(
    *,
    deps: TwitterPostsCatalogDependencies,
    config: Mapping[str, Any],
) -> tuple[datetime, datetime, bool]:
    requested_date_start = deps.coerce_dt(config.get("date_start"))
    requested_date_end = deps.coerce_dt(config.get("date_end"))
    catalog_action_scope = str(config.get("catalog_action_scope") or "").strip().lower()
    full_history_requested = (
        deps.shared_catalog_mode(config)
        and requested_date_start is None
        and requested_date_end is None
        and catalog_action_scope in {"", "full_history"}
    )

    date_start = requested_date_start or (
        datetime(2006, 1, 1, tzinfo=UTC) if full_history_requested else (deps.now_utc() - timedelta(days=30))
    )
    date_end = requested_date_end or (
        (deps.now_utc() - timedelta(days=1)) if full_history_requested else deps.now_utc()
    )
    return date_start, date_end, full_history_requested


def _resolve_max_pages(
    *,
    deps: TwitterPostsCatalogDependencies,
    config: Mapping[str, Any],
    full_history_requested: bool,
) -> int | None:
    if full_history_requested and config.get("max_posts_per_target") is None:
        return None
    if full_history_requested:
        return deps.shared_stage_post_limit(config, default=0)
    return min(deps.shared_stage_post_limit(config, default=10) or 10, 20)


def _persist_shared_catalog_posts(
    *,
    deps: TwitterPostsCatalogDependencies,
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
        platform="twitter",
        run_id=run_id,
        account_handle=account_handle,
        items=posts,
        retrieval_meta=retrieval_meta,
        progress_cb=progress_cb,
        upsert_item=lambda tweet: deps.upsert_shared_catalog_post(
            platform="twitter",
            run_id=run_id,
            account_handle=account_handle,
            post=tweet,
        ),
    )


def _persist_tweets(
    *,
    deps: TwitterPostsCatalogDependencies,
    job_id: str,
    account_handle: str,
    posts: Sequence[Any],
) -> list[dict[str, Any]]:
    if deps.upsert_tweet is None:
        raise RuntimeError("upsert_tweet dependency is required outside shared catalog mode")

    rows: list[dict[str, Any]] = []
    for tweet in posts:
        row = deps.upsert_tweet(None, job_id=job_id, run_id=None, account=account_handle, tweet=tweet)
        if row:
            rows.append(row)
    return rows


def _twitter_profile_snapshot_from_posts(
    *,
    deps: TwitterPostsCatalogDependencies,
    posts: Sequence[Any],
    account_handle: str,
) -> dict[str, Any]:
    return {
        "username": deps.normalize_account_handle(
            next(
                (
                    str(getattr(tweet, "username", "") or "").strip()
                    for tweet in posts
                    if str(getattr(tweet, "username", "") or "").strip()
                ),
                account_handle,
            )
        ),
        "display_name": next(
            (
                str(getattr(tweet, "display_name", "") or "").strip()
                for tweet in posts
                if str(getattr(tweet, "display_name", "") or "").strip()
            ),
            None,
        ),
        "avatar_url": deps.best_profile_avatar_url(
            [
                next(
                    (
                        str(getattr(tweet, "user_avatar_url", "") or "").strip()
                        for tweet in posts
                        if str(getattr(tweet, "user_avatar_url", "") or "").strip()
                    ),
                    None,
                )
            ]
        ),
        "is_verified": any(bool(getattr(tweet, "user_verified", False)) for tweet in posts),
        "profile_url": deps.first_non_empty_str(
            next(
                (
                    str(getattr(tweet, "user_profile_url", "") or "").strip()
                    for tweet in posts
                    if str(getattr(tweet, "user_profile_url", "") or "").strip()
                ),
                None,
            ),
            deps.platform_profile_url_for_handle("twitter", account_handle),
        ),
    }


def scrape_shared_twitter_posts(
    *,
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: ProgressCallback | None = None,
    dependencies: TwitterPostsCatalogDependencies | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    deps = dependencies or TwitterPostsCatalogDependencies()
    scraper = _build_scraper(deps)
    date_start, date_end, full_history_requested = _resolve_window(deps=deps, config=config)
    max_pages = _resolve_max_pages(deps=deps, config=config, full_history_requested=full_history_requested)

    scrape_config = deps.scrape_config_factory(
        query=f"from:{account_handle}",
        date_start=date_start,
        date_end=date_end,
        include_replies=False,
        include_links=True,
        delay_seconds=0.35,
        max_pages=max_pages,
    )
    posts = list(scraper.scrape(scrape_config, progress_cb=progress_cb))
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    catalog_posts = [tweet for tweet in posts if not bool(getattr(tweet, "is_reply", False))]

    if deps.shared_catalog_mode(config):
        rows = _persist_shared_catalog_posts(
            deps=deps,
            run_id=run_id,
            account_handle=account_handle,
            posts=catalog_posts,
            retrieval_meta=retrieval_meta,
            progress_cb=progress_cb,
        )
        retrieval_meta["persist_counters"] = {
            "posts_upserted": len(rows),
            "comments_upserted": 0,
        }
        retrieval_meta["posts_checked"] = deps.shared_catalog_progress_posts_checked(
            retrieval_meta,
            matched_posts=len(catalog_posts),
        )
        retrieval_meta["pages_scanned"] = deps.shared_catalog_progress_pages_scanned(retrieval_meta)
    else:
        rows = _persist_tweets(
            deps=deps,
            job_id=job_id,
            account_handle=account_handle,
            posts=catalog_posts,
        )

    retrieval_meta["profile_snapshot"] = deps.merge_social_profile_snapshots(
        deps.metadata_dict(config.get("profile_snapshot")),
        _twitter_profile_snapshot_from_posts(deps=deps, posts=posts, account_handle=account_handle),
    )
    return rows, retrieval_meta
