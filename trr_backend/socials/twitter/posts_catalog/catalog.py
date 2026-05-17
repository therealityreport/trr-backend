"""Twitter/X shared catalog orchestration.

The module is intentionally wired through callbacks for monolith-owned helpers,
auth, scraper construction, and persistence so platform code can be imported
without pulling repository surfaces back into the Twitter package.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import UUID

from trr_backend.socials.twitter.scraper import TwitterScrapeConfig, TwitterScraper

SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE = "shared_account_catalog_backfill"
TWITTER_FALLBACK_PAGE_SIZE = 10
TWITTER_COMMENT_MIN_PAGE_BUDGET = 5
TWITTER_COMMENT_MAX_PAGE_BUDGET = 120

logger = logging.getLogger(__name__)

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


def _resolve_positive_int_env(name: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        parsed = int(str(os.getenv(name, str(default))).strip())
    except (TypeError, ValueError):
        parsed = int(default)
    parsed = max(int(minimum), parsed)
    if maximum is not None:
        parsed = min(int(maximum), parsed)
    return parsed


def _resolve_non_negative_float_env(name: str, default: float) -> float:
    try:
        parsed = float(str(os.getenv(name, str(default))).strip())
    except (TypeError, ValueError):
        parsed = float(default)
    return max(0.0, parsed)


def _shared_catalog_comments_requested(config: Mapping[str, Any] | None) -> bool:
    metadata = _metadata_dict(config)
    if bool(metadata.get("twitter_comments_in_posts_stage")):
        return True
    selected = metadata.get("effective_selected_tasks")
    if not isinstance(selected, list):
        selected = metadata.get("selected_tasks")
    return any(str(task or "").strip().lower() == "comments" for task in (selected or []))


def _shared_catalog_comment_anchor_source_ids(config: Mapping[str, Any] | None) -> tuple[str, ...]:
    metadata = _metadata_dict(config)
    raw_value = metadata.get("comment_anchor_source_ids")
    if isinstance(raw_value, Mapping):
        platform_value = None
        for key, value in raw_value.items():
            if str(key or "").strip().lower() in {"twitter", "x"}:
                platform_value = value
                break
        raw_value = platform_value
    if isinstance(raw_value, str):
        candidates: Sequence[Any] = [raw_value]
    elif isinstance(raw_value, Sequence):
        candidates = raw_value
    else:
        candidates = []
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        source_id = str(candidate or "").strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        normalized.append(source_id)
    return tuple(normalized)


def _shared_catalog_comment_limit(config: Mapping[str, Any] | None) -> int | None:
    metadata = _metadata_dict(config)
    for key in ("max_comments_per_post", "catalog_max_comments_per_post"):
        if metadata.get(key) is None:
            continue
        try:
            parsed = int(metadata.get(key) or 0)
        except (TypeError, ValueError):
            continue
        return None if parsed <= 0 else parsed
    return None


def _twitter_page_budget_for_expected_count(expected_count: int) -> int:
    expected = max(0, int(expected_count or 0))
    estimated_pages = ((expected + TWITTER_FALLBACK_PAGE_SIZE - 1) // TWITTER_FALLBACK_PAGE_SIZE) + 2
    return min(TWITTER_COMMENT_MAX_PAGE_BUDGET, max(TWITTER_COMMENT_MIN_PAGE_BUDGET, estimated_pages))


def _twitter_context_role(*, account_handle: str, tweet: Any) -> str | None:
    if bool(getattr(tweet, "is_quote", False)):
        return "quote"
    normalized_account = _normalize_account_handle(account_handle)
    normalized_username = _normalize_account_handle(getattr(tweet, "username", ""))
    if bool(getattr(tweet, "is_reply", False)) and normalized_account and normalized_username == normalized_account:
        return "account_reply"
    if bool(getattr(tweet, "is_reply", False)):
        return "audience_reply"
    if normalized_account and normalized_username == normalized_account:
        return "account_post"
    return None


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


def _normalize_uuid(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except (TypeError, ValueError):
        return None


def _upsert_twitter_interaction_fetch_state(
    *,
    source_account: str,
    root_source_id: str,
    interaction_kind: str,
    strategy: str = "default",
    reported_count: int = 0,
    saved_count_before: int = 0,
    saved_count_after: int = 0,
    unique_saved_delta: int = 0,
    duplicate_count: int = 0,
    off_root_count: int = 0,
    pages_scanned: int = 0,
    last_cursor: str | None = None,
    last_ranking: str | None = None,
    consecutive_no_new_pages: int = 0,
    status: str = "running",
    exhaustion_reason: str | None = None,
    last_job_id: str | None = None,
    last_error_code: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    from psycopg2.extras import Json

    from trr_backend.db import pg

    account = str(source_account or "").strip()
    root_id = str(root_source_id or "").strip()
    kind = str(interaction_kind or "").strip().lower()
    normalized_strategy = str(strategy or "default").strip() or "default"
    normalized_status = str(status or "running").strip().lower() or "running"
    if not account or not root_id or kind not in {"reply", "quote"}:
        return None

    payload = dict(metadata or {})
    raw_job_id = str(last_job_id or "").strip()
    normalized_job_id = _normalize_uuid(raw_job_id)
    if raw_job_id and normalized_job_id is None:
        payload.setdefault("last_job_id", raw_job_id)

    sql = """
        INSERT INTO social.twitter_interaction_fetch_state (
            source_account,
            root_source_id,
            interaction_kind,
            strategy,
            reported_count,
            saved_count_before,
            saved_count_after,
            unique_saved_delta,
            duplicate_count,
            off_root_count,
            pages_scanned,
            last_cursor,
            last_ranking,
            consecutive_no_new_pages,
            status,
            exhaustion_reason,
            last_job_id,
            last_error_code,
            metadata,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s::uuid, %s, %s, now()
        )
        ON CONFLICT (lower(source_account), root_source_id, interaction_kind, strategy)
        DO UPDATE SET
            reported_count = EXCLUDED.reported_count,
            saved_count_before = EXCLUDED.saved_count_before,
            saved_count_after = EXCLUDED.saved_count_after,
            unique_saved_delta = EXCLUDED.unique_saved_delta,
            duplicate_count = EXCLUDED.duplicate_count,
            off_root_count = EXCLUDED.off_root_count,
            pages_scanned = GREATEST(
                social.twitter_interaction_fetch_state.pages_scanned,
                EXCLUDED.pages_scanned
            ),
            last_cursor = COALESCE(EXCLUDED.last_cursor, social.twitter_interaction_fetch_state.last_cursor),
            last_ranking = COALESCE(EXCLUDED.last_ranking, social.twitter_interaction_fetch_state.last_ranking),
            consecutive_no_new_pages = EXCLUDED.consecutive_no_new_pages,
            status = EXCLUDED.status,
            exhaustion_reason = EXCLUDED.exhaustion_reason,
            last_job_id = COALESCE(EXCLUDED.last_job_id, social.twitter_interaction_fetch_state.last_job_id),
            last_error_code = EXCLUDED.last_error_code,
            metadata = social.twitter_interaction_fetch_state.metadata || EXCLUDED.metadata,
            updated_at = now()
        RETURNING *
    """
    rows = pg.execute_returning(
        sql,
        [
            account,
            root_id,
            kind,
            normalized_strategy,
            max(0, int(reported_count or 0)),
            max(0, int(saved_count_before or 0)),
            max(0, int(saved_count_after or 0)),
            max(0, int(unique_saved_delta or 0)),
            max(0, int(duplicate_count or 0)),
            max(0, int(off_root_count or 0)),
            max(0, int(pages_scanned or 0)),
            str(last_cursor or "").strip() or None,
            str(last_ranking or "").strip() or None,
            max(0, int(consecutive_no_new_pages or 0)),
            normalized_status,
            str(exhaustion_reason or "").strip() or None,
            normalized_job_id,
            str(last_error_code or "").strip() or None,
            Json(payload),
        ],
    )
    return rows[0] if rows else None


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
    load_existing_catalog_posts: Callable[..., Sequence[Any]] | None = None
    persist_shared_catalog_posts_with_progress: Callable[..., list[dict[str, Any]]] | None = None
    upsert_shared_catalog_post: Callable[..., dict[str, Any] | None] | None = None
    upsert_tweet: Callable[..., dict[str, Any] | None] | None = None
    upsert_twitter_interaction_fetch_state: Callable[..., dict[str, Any] | None] | None = (
        _upsert_twitter_interaction_fetch_state
    )


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
    run_id: str | None,
    account_handle: str,
    posts: Sequence[Any],
    progress_cb: ProgressCallback | None = None,
    retrieval_meta: Mapping[str, Any] | None = None,
    phase: str = "materialize_catalog_posts",
) -> list[dict[str, Any]]:
    if deps.upsert_tweet is None:
        raise RuntimeError("upsert_tweet dependency is required outside shared catalog mode")

    rows: list[dict[str, Any]] = []
    total_posts = max(0, int(len(posts)))
    for index, tweet in enumerate(posts, start=1):
        row = deps.upsert_tweet(None, job_id=job_id, run_id=run_id, account=account_handle, tweet=tweet)
        if row:
            rows.append(row)
        if progress_cb and (index == 1 or index % 25 == 0 or index == total_posts):
            progress_cb(
                {
                    "phase": phase,
                    "pages_scanned": _shared_catalog_progress_pages_scanned(retrieval_meta),
                    "posts_checked": index,
                    "matched_posts": total_posts,
                    "materialized_posts": len(rows),
                }
            )
    return rows


def _fetch_tweet_replies(
    *,
    scraper: Any,
    tweet_id: str,
    page_budget_count: int,
    delay: float,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[Any]:
    fetch = getattr(scraper, "fetch_tweet_replies", None)
    if not callable(fetch):
        raise AttributeError("fetch_tweet_replies")
    page_budget = _twitter_page_budget_for_expected_count(page_budget_count)
    try:
        return list(
            fetch(
                tweet_id,
                delay=delay,
                search_max_pages=page_budget,
                twikit_max_pages=page_budget,
                progress_callback=progress_callback,
            )
            or []
        )
    except TypeError as exc:
        message = str(exc)
        if "progress_callback" in message:
            try:
                return list(
                    fetch(
                        tweet_id,
                        delay=delay,
                        search_max_pages=page_budget,
                        twikit_max_pages=page_budget,
                    )
                    or []
                )
            except TypeError as fallback_exc:
                message = str(fallback_exc)
        if "search_max_pages" in message or "twikit_max_pages" in message:
            return list(fetch(tweet_id, delay=delay) or [])
        raise


def _fetch_tweet_quotes(
    *,
    scraper: Any,
    tweet_id: str,
    page_budget_count: int,
    delay: float,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[Any]:
    fetch = getattr(scraper, "fetch_tweet_quotes", None)
    if not callable(fetch):
        return []
    page_budget = _twitter_page_budget_for_expected_count(page_budget_count)
    try:
        return list(
            fetch(
                tweet_id,
                delay=delay,
                max_pages=page_budget,
                progress_callback=progress_callback,
            )
            or []
        )
    except TypeError as exc:
        if "progress_callback" in str(exc):
            return list(fetch(tweet_id, delay=delay, max_pages=page_budget) or [])
        raise


def _interaction_page_budget_count(*, expected_count: int, comment_limit: int | None) -> int:
    expected = max(0, int(expected_count or 0))
    if comment_limit is None:
        return expected
    return min(expected, max(0, int(comment_limit or 0)))


def _interaction_status_from_failure(*, fail_reason: str | None, fetched_count: int, expected_count: int) -> str:
    normalized = str(fail_reason or "").strip().lower()
    if not normalized:
        return "completed"
    if "429" in normalized or "rate" in normalized:
        return "rate_limited"
    if any(marker in normalized for marker in ("auth", "login", "401", "403", "forbidden", "cookie")):
        return "auth_blocked"
    if max(0, int(fetched_count or 0)) <= 0 and max(0, int(expected_count or 0)) > 0:
        return "exhausted"
    return "completed"


def _record_interaction_fetch_state(
    *,
    deps: TwitterPostsCatalogDependencies,
    source_account: str,
    root_source_id: str,
    interaction_kind: str,
    reported_count: int = 0,
    saved_count_before: int = 0,
    saved_count_after: int = 0,
    unique_saved_delta: int = 0,
    duplicate_count: int = 0,
    off_root_count: int = 0,
    pages_scanned: int = 0,
    status: str = "running",
    exhaustion_reason: str | None = None,
    last_error_code: str | None = None,
    job_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    writer = deps.upsert_twitter_interaction_fetch_state
    if writer is None:
        return
    try:
        writer(
            source_account=source_account,
            root_source_id=root_source_id,
            interaction_kind=interaction_kind,
            reported_count=reported_count,
            saved_count_before=saved_count_before,
            saved_count_after=saved_count_after,
            unique_saved_delta=unique_saved_delta,
            duplicate_count=duplicate_count,
            off_root_count=off_root_count,
            pages_scanned=pages_scanned,
            status=status,
            exhaustion_reason=exhaustion_reason,
            last_job_id=job_id,
            last_error_code=last_error_code,
            metadata=metadata or {},
        )
    except Exception:
        logger.warning(
            "Twitter interaction fetch-state write failed for %s %s",
            interaction_kind,
            root_source_id,
            exc_info=True,
        )


def _persist_tweet_interactions(
    *,
    deps: TwitterPostsCatalogDependencies,
    scraper: Any,
    job_id: str,
    run_id: str | None,
    account_handle: str,
    posts: Sequence[Any],
    config: Mapping[str, Any],
    retrieval_meta: MutableMapping[str, Any],
    progress_cb: ProgressCallback | None,
) -> dict[str, int]:
    if deps.upsert_tweet is None:
        raise RuntimeError("upsert_tweet dependency is required to persist Twitter comments")
    if not _shared_catalog_comments_requested(config):
        return {
            "comments_fetched": 0,
            "comments_upserted": 0,
            "quotes_fetched": 0,
            "quotes_upserted": 0,
            "comment_errors": 0,
            "quote_errors": 0,
        }

    comment_limit = _shared_catalog_comment_limit(config)
    comment_delay = _resolve_non_negative_float_env("SOCIAL_TWITTER_COMMENT_DELAY_SEC", 0.5)
    stats = {
        "comments_fetched": 0,
        "comments_upserted": 0,
        "quotes_fetched": 0,
        "quotes_upserted": 0,
        "comment_errors": 0,
        "quote_errors": 0,
    }
    comment_fail_reasons: set[str] = set()
    quote_fail_reasons: set[str] = set()
    total_posts = max(0, int(len(posts)))

    def _emit_interaction_progress(
        *,
        index: int,
        tweet_id: str,
        phase: str,
        current_comments_fetched: int = 0,
        current_quotes_fetched: int = 0,
    ) -> None:
        if not progress_cb:
            return
        progress_cb(
            {
                "phase": phase,
                "pages_scanned": _shared_catalog_progress_pages_scanned(retrieval_meta),
                "posts_checked": index,
                "matched_posts": total_posts,
                "scraped_comments": (
                    stats["comments_fetched"]
                    + stats["quotes_fetched"]
                    + max(0, int(current_comments_fetched or 0))
                    + max(0, int(current_quotes_fetched or 0))
                ),
                "comments_upserted": stats["comments_upserted"] + stats["quotes_upserted"],
                "current_source_id": tweet_id,
            }
        )

    for index, post in enumerate(posts, start=1):
        tweet_id = str(getattr(post, "tweet_id", "") or "").strip()
        if not tweet_id:
            continue
        expected_replies = _normalize_non_negative_int(getattr(post, "replies", 0))
        expected_quotes = _normalize_non_negative_int(getattr(post, "quotes", 0))
        if expected_replies <= 0 and expected_quotes <= 0:
            continue
        _emit_interaction_progress(index=index, tweet_id=tweet_id, phase="comments_fetch")
        if expected_replies > 0:
            _record_interaction_fetch_state(
                deps=deps,
                source_account=account_handle,
                root_source_id=tweet_id,
                interaction_kind="reply",
                reported_count=expected_replies,
                status="running",
                job_id=job_id,
                metadata={"phase": "comments_fetch", "run_id": run_id},
            )

        replies: list[Any] = []
        reply_fetch_exception = False
        reply_fail_reason = ""
        reply_pages_scanned = 0
        try:

            def _on_reply_fetch_progress(
                payload: dict[str, Any],
                *,
                _index: int = index,
                _tweet_id: str = tweet_id,
            ) -> None:
                nonlocal reply_pages_scanned
                reply_pages_scanned = max(
                    reply_pages_scanned,
                    _normalize_non_negative_int(payload.get("pages_scanned")),
                )
                _emit_interaction_progress(
                    index=_index,
                    tweet_id=_tweet_id,
                    phase=str(payload.get("phase") or "tweet_detail_replies_page"),
                    current_comments_fetched=_normalize_non_negative_int(payload.get("comments_fetched")),
                )
                _record_interaction_fetch_state(
                    deps=deps,
                    source_account=account_handle,
                    root_source_id=_tweet_id,
                    interaction_kind="reply",
                    reported_count=expected_replies,
                    pages_scanned=reply_pages_scanned,
                    status="running",
                    job_id=job_id,
                    metadata={
                        "phase": str(payload.get("phase") or "tweet_detail_replies_page"),
                        "comments_fetched": _normalize_non_negative_int(payload.get("comments_fetched")),
                        "run_id": run_id,
                    },
                )

            replies = _fetch_tweet_replies(
                scraper=scraper,
                tweet_id=tweet_id,
                page_budget_count=_interaction_page_budget_count(
                    expected_count=expected_replies,
                    comment_limit=comment_limit,
                ),
                delay=comment_delay,
                progress_callback=_on_reply_fetch_progress,
            )
            if comment_limit is not None:
                replies = replies[:comment_limit]
            _emit_interaction_progress(
                index=index,
                tweet_id=tweet_id,
                phase="twitter_replies_fetch_done",
                current_comments_fetched=len(replies),
            )
            reply_fail_reason = str(getattr(scraper, "last_reply_fetch_reason", "") or "").strip()
            if reply_fail_reason:
                comment_fail_reasons.add(reply_fail_reason)
                if not replies:
                    stats["comment_errors"] += 1
        except Exception:
            reply_fetch_exception = True
            reply_fail_reason = "fetch_exception"
            stats["comment_errors"] += 1
            comment_fail_reasons.add("fetch_exception")
            replies = []

        reply_upserted = 0
        for reply_index, reply in enumerate(replies, start=1):
            if not getattr(reply, "reply_to_tweet_id", None):
                reply.reply_to_tweet_id = tweet_id
            reply.is_reply = True
            if not str(getattr(reply, "thread_root_tweet_id", "") or "").strip():
                reply.thread_root_tweet_id = tweet_id
            reply.is_thread_part = True
            if not str(getattr(reply, "twitter_context_role", "") or "").strip():
                reply.twitter_context_role = _twitter_context_role(account_handle=account_handle, tweet=reply)
            row = deps.upsert_tweet(
                None,
                job_id=job_id,
                run_id=run_id,
                account=account_handle,
                tweet=reply,
                persist_stats=None,
            )
            stats["comments_fetched"] += 1
            if row:
                stats["comments_upserted"] += 1
                reply_upserted += 1
            if reply_index == 1 or reply_index % 25 == 0 or reply_index == len(replies):
                _emit_interaction_progress(
                    index=index,
                    tweet_id=tweet_id,
                    phase="persist_twitter_replies",
                )
        if expected_replies > 0:
            reply_status = (
                "failed"
                if reply_fetch_exception
                else _interaction_status_from_failure(
                    fail_reason=reply_fail_reason,
                    fetched_count=len(replies),
                    expected_count=expected_replies,
                )
            )
            _record_interaction_fetch_state(
                deps=deps,
                source_account=account_handle,
                root_source_id=tweet_id,
                interaction_kind="reply",
                reported_count=expected_replies,
                saved_count_after=reply_upserted,
                unique_saved_delta=reply_upserted,
                duplicate_count=max(0, len(replies) - reply_upserted),
                pages_scanned=max(reply_pages_scanned, _shared_catalog_progress_pages_scanned(retrieval_meta)),
                status=reply_status,
                exhaustion_reason=reply_fail_reason if reply_status == "exhausted" else None,
                last_error_code=reply_fail_reason or None,
                job_id=job_id,
                metadata={
                    "phase": "persist_twitter_replies",
                    "run_id": run_id,
                    "fetched_count": len(replies),
                },
            )

        quotes: list[Any] = []
        quote_fetch_exception = False
        quote_fail_reason = ""
        quote_pages_scanned = 0
        quote_off_root_count = 0
        try:
            _emit_interaction_progress(index=index, tweet_id=tweet_id, phase="twitter_quotes_fetch")
            if expected_quotes > 0:
                _record_interaction_fetch_state(
                    deps=deps,
                    source_account=account_handle,
                    root_source_id=tweet_id,
                    interaction_kind="quote",
                    reported_count=expected_quotes,
                    status="running",
                    job_id=job_id,
                    metadata={"phase": "twitter_quotes_fetch", "run_id": run_id},
                )

            def _on_quote_fetch_progress(
                payload: dict[str, Any],
                *,
                _index: int = index,
                _tweet_id: str = tweet_id,
            ) -> None:
                nonlocal quote_pages_scanned
                quote_pages_scanned = max(
                    quote_pages_scanned,
                    _normalize_non_negative_int(payload.get("pages_scanned")),
                )
                _emit_interaction_progress(
                    index=_index,
                    tweet_id=_tweet_id,
                    phase=str(payload.get("phase") or "twitter_quotes_fetch"),
                    current_quotes_fetched=_normalize_non_negative_int(payload.get("quotes_fetched")),
                )
                _record_interaction_fetch_state(
                    deps=deps,
                    source_account=account_handle,
                    root_source_id=_tweet_id,
                    interaction_kind="quote",
                    reported_count=expected_quotes,
                    pages_scanned=quote_pages_scanned,
                    status="running",
                    job_id=job_id,
                    metadata={
                        "phase": str(payload.get("phase") or "twitter_quotes_fetch"),
                        "quotes_fetched": _normalize_non_negative_int(payload.get("quotes_fetched")),
                        "run_id": run_id,
                    },
                )

            quotes = _fetch_tweet_quotes(
                scraper=scraper,
                tweet_id=tweet_id,
                page_budget_count=_interaction_page_budget_count(
                    expected_count=expected_quotes,
                    comment_limit=comment_limit,
                ),
                delay=comment_delay,
                progress_callback=_on_quote_fetch_progress,
            )
            if comment_limit is not None:
                quotes = quotes[:comment_limit]
            _emit_interaction_progress(
                index=index,
                tweet_id=tweet_id,
                phase="twitter_quotes_fetch_done",
                current_quotes_fetched=len(quotes),
            )
            quote_fail_reason = str(getattr(scraper, "last_quote_fetch_reason", "") or "").strip()
            if quote_fail_reason:
                quote_fail_reasons.add(quote_fail_reason)
                if not quotes:
                    stats["quote_errors"] += 1
        except Exception:
            quote_fetch_exception = True
            quote_fail_reason = "fetch_exception"
            stats["quote_errors"] += 1
            quote_fail_reasons.add("fetch_exception")
            quotes = []

        quote_upserted = 0
        for quote_index, quote in enumerate(quotes, start=1):
            quote.is_reply = False
            quote.reply_to_tweet_id = None
            quote.is_quote = True
            if not getattr(quote, "quoted_tweet_id", None):
                quote.quoted_tweet_id = tweet_id
            should_emit_quote_progress = quote_index == 1 or quote_index % 25 == 0 or quote_index == len(quotes)
            if str(getattr(quote, "quoted_tweet_id", "") or "") != tweet_id:
                if should_emit_quote_progress:
                    _emit_interaction_progress(
                        index=index,
                        tweet_id=tweet_id,
                        phase="filter_twitter_quotes",
                    )
                quote_off_root_count += 1
                continue
            if not str(getattr(quote, "twitter_context_role", "") or "").strip():
                quote.twitter_context_role = "quote"
            row = deps.upsert_tweet(
                None,
                job_id=job_id,
                run_id=run_id,
                account=account_handle,
                tweet=quote,
                persist_stats=None,
            )
            stats["quotes_fetched"] += 1
            if row:
                stats["quotes_upserted"] += 1
                quote_upserted += 1
            if should_emit_quote_progress:
                _emit_interaction_progress(
                    index=index,
                    tweet_id=tweet_id,
                    phase="persist_twitter_quotes",
                )
        if expected_quotes > 0:
            quote_saved_total = max(0, len(quotes) - quote_off_root_count)
            quote_status = (
                "failed"
                if quote_fetch_exception
                else _interaction_status_from_failure(
                    fail_reason=quote_fail_reason,
                    fetched_count=quote_saved_total,
                    expected_count=expected_quotes,
                )
            )
            _record_interaction_fetch_state(
                deps=deps,
                source_account=account_handle,
                root_source_id=tweet_id,
                interaction_kind="quote",
                reported_count=expected_quotes,
                saved_count_after=quote_upserted,
                unique_saved_delta=quote_upserted,
                duplicate_count=max(0, quote_saved_total - quote_upserted),
                off_root_count=quote_off_root_count,
                pages_scanned=max(quote_pages_scanned, _shared_catalog_progress_pages_scanned(retrieval_meta)),
                status=quote_status,
                exhaustion_reason=quote_fail_reason if quote_status == "exhausted" else None,
                last_error_code=quote_fail_reason or None,
                job_id=job_id,
                metadata={
                    "phase": "persist_twitter_quotes",
                    "run_id": run_id,
                    "fetched_count": len(quotes),
                    "saved_candidate_count": quote_saved_total,
                },
            )

    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if quote_fail_reasons:
        retrieval_meta["quote_fail_reasons"] = sorted(quote_fail_reasons)
    return stats


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


def _catalog_seeded_fallback_needed(
    *,
    deps: TwitterPostsCatalogDependencies,
    config: Mapping[str, Any],
    catalog_posts: Sequence[Any],
    retrieval_meta: Mapping[str, Any],
) -> bool:
    if not deps.shared_catalog_mode(config):
        return False
    if catalog_posts:
        return False
    if deps.load_existing_catalog_posts is None:
        return False

    error_code = str(retrieval_meta.get("error_code") or retrieval_meta.get("last_error_code") or "").strip().lower()
    stop_reason = (
        str(
            retrieval_meta.get("stop_reason")
            or retrieval_meta.get("retrieval_stop_reason")
            or retrieval_meta.get("playwright_failure_reason")
            or ""
        )
        .strip()
        .lower()
    )
    return error_code == "twitter_search_fallback_exhausted" or stop_reason in {
        "playwright_no_search_payload",
        "playwright_no_search_payload_retry",
        "playwright_no_tweet_entries",
        "no_tweet_entries",
    }


def _load_catalog_seeded_posts(
    *,
    deps: TwitterPostsCatalogDependencies,
    run_id: str | None,
    account_handle: str,
    date_start: datetime,
    date_end: datetime,
    config: Mapping[str, Any],
    retrieval_meta: MutableMapping[str, Any],
    progress_cb: ProgressCallback | None,
) -> list[Any]:
    if deps.load_existing_catalog_posts is None:
        return []

    original_error_code = str(retrieval_meta.get("error_code") or "").strip() or None
    original_error_class = str(retrieval_meta.get("error_class") or "").strip() or None
    original_stop_reason = str(retrieval_meta.get("stop_reason") or "").strip() or None
    try:
        seeded_posts = list(
            deps.load_existing_catalog_posts(
                run_id=run_id,
                account_handle=account_handle,
                date_start=date_start,
                date_end=date_end,
                config=config,
                retrieval_meta=retrieval_meta,
            )
            or []
        )
    except Exception:  # noqa: BLE001
        retrieval_meta["catalog_seeded_fallback_error"] = "load_existing_catalog_posts_failed"
        logger.warning(
            "Twitter catalog-seeded fallback failed for @%s window=%s..%s",
            account_handle,
            date_start,
            date_end,
            exc_info=True,
        )
        return []

    if not seeded_posts:
        retrieval_meta["catalog_seeded_fallback_checked"] = True
        retrieval_meta["catalog_seeded_post_count"] = 0
        return []

    retrieval_meta["catalog_seeded_fallback"] = True
    retrieval_meta["catalog_seeded_post_count"] = len(seeded_posts)
    retrieval_meta["catalog_seeded_original_error_code"] = original_error_code
    retrieval_meta["catalog_seeded_original_error_class"] = original_error_class
    retrieval_meta["catalog_seeded_original_stop_reason"] = original_stop_reason
    retrieval_meta["retrieval_mode"] = "catalog_seeded_window"
    retrieval_meta["stop_reason"] = "catalog_seeded_window"
    retrieval_meta["error_code"] = None
    retrieval_meta["error_class"] = None
    retrieval_meta["retryable"] = False
    retrieval_meta["complete"] = True
    retrieval_meta["posts_checked"] = max(
        _normalize_non_negative_int(retrieval_meta.get("posts_checked")),
        len(seeded_posts),
    )
    retrieval_meta["tweet_count"] = max(
        _normalize_non_negative_int(retrieval_meta.get("tweet_count")),
        len(seeded_posts),
    )
    if progress_cb:
        progress_cb(
            {
                "phase": "catalog_seeded_fallback",
                "pages_scanned": _shared_catalog_progress_pages_scanned(retrieval_meta),
                "posts_checked": len(seeded_posts),
                "matched_posts": len(seeded_posts),
            }
        )
    return seeded_posts


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
    targeted_anchor_ids = _shared_catalog_comment_anchor_source_ids(config)

    if (
        deps.shared_catalog_mode(config)
        and _shared_catalog_comments_requested(config)
        and targeted_anchor_ids
        and deps.load_existing_catalog_posts is not None
    ):
        retrieval_meta: dict[str, Any] = {
            "retrieval_mode": "catalog_seeded_anchor_targets",
            "stop_reason": "catalog_seeded_anchor_targets",
            "catalog_seeded_anchor_source_ids": list(targeted_anchor_ids),
            "complete": True,
            "retryable": False,
        }
        seeded_posts = _load_catalog_seeded_posts(
            deps=deps,
            run_id=run_id,
            account_handle=account_handle,
            date_start=date_start,
            date_end=date_end,
            config=config,
            retrieval_meta=retrieval_meta,
            progress_cb=progress_cb,
        )
        posts = list(seeded_posts)
        catalog_posts = list(seeded_posts)
        retrieval_meta["catalog_seeded_anchor_source_ids"] = list(targeted_anchor_ids)
        if seeded_posts:
            retrieval_meta["catalog_seeded_targeted_anchors"] = True
            retrieval_meta["retrieval_mode"] = "catalog_seeded_anchor_targets"
            retrieval_meta["stop_reason"] = "catalog_seeded_anchor_targets"
        elif retrieval_meta.get("catalog_seeded_fallback_checked"):
            retrieval_meta["catalog_seeded_empty_anchor_targets"] = True
            retrieval_meta["retrieval_mode"] = "catalog_seeded_empty_anchor_targets"
            retrieval_meta["stop_reason"] = "catalog_seeded_empty_anchor_targets"
    else:
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
        if _catalog_seeded_fallback_needed(
            deps=deps,
            config=config,
            catalog_posts=catalog_posts,
            retrieval_meta=retrieval_meta,
        ):
            seeded_posts = _load_catalog_seeded_posts(
                deps=deps,
                run_id=run_id,
                account_handle=account_handle,
                date_start=scrape_config.date_start,
                date_end=scrape_config.date_end,
                config=config,
                retrieval_meta=retrieval_meta,
                progress_cb=progress_cb,
            )
            if seeded_posts:
                posts = seeded_posts
                catalog_posts = seeded_posts
            elif retrieval_meta.get("catalog_seeded_fallback_checked"):
                retrieval_meta["catalog_seeded_empty_window"] = True
                retrieval_meta["retrieval_mode"] = "catalog_seeded_empty_window"
                retrieval_meta["stop_reason"] = "catalog_seeded_empty_window"
                retrieval_meta["error_code"] = None
                retrieval_meta["error_class"] = None
                retrieval_meta["retryable"] = False
                retrieval_meta["complete"] = True

    if deps.shared_catalog_mode(config):
        rows = _persist_shared_catalog_posts(
            deps=deps,
            run_id=run_id,
            account_handle=account_handle,
            posts=catalog_posts,
            retrieval_meta=retrieval_meta,
            progress_cb=progress_cb,
        )
        materialized_rows = (
            _persist_tweets(
                deps=deps,
                job_id=job_id,
                run_id=run_id,
                account_handle=account_handle,
                posts=catalog_posts,
                progress_cb=progress_cb,
                retrieval_meta=retrieval_meta,
            )
            if deps.upsert_tweet is not None
            else []
        )
        interaction_stats = {
            "comments_fetched": 0,
            "comments_upserted": 0,
            "quotes_fetched": 0,
            "quotes_upserted": 0,
            "comment_errors": 0,
            "quote_errors": 0,
        }
        if _shared_catalog_comments_requested(config):
            interaction_stats = _persist_tweet_interactions(
                deps=deps,
                scraper=scraper,
                job_id=job_id,
                run_id=run_id,
                account_handle=account_handle,
                posts=catalog_posts,
                config=config,
                retrieval_meta=retrieval_meta,
                progress_cb=progress_cb,
            )
        retrieval_meta["persist_counters"] = {
            "posts_upserted": len(rows),
            "catalog_posts_upserted": len(rows),
            "materialized_posts_upserted": len(materialized_rows),
            "comments_upserted": interaction_stats["comments_upserted"],
        }
        if _shared_catalog_comments_requested(config):
            retrieval_meta["comment_stats"] = {
                "comments_fetched": interaction_stats["comments_fetched"],
                "comments_upserted": interaction_stats["comments_upserted"],
                "comment_errors": interaction_stats["comment_errors"],
            }
            retrieval_meta["quote_stats"] = {
                "quotes_fetched": interaction_stats["quotes_fetched"],
                "quotes_upserted": interaction_stats["quotes_upserted"],
                "quote_errors": interaction_stats["quote_errors"],
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
            run_id=None,
            account_handle=account_handle,
            posts=catalog_posts,
            progress_cb=progress_cb,
            retrieval_meta=retrieval_meta,
        )

    retrieval_meta["profile_snapshot"] = deps.merge_social_profile_snapshots(
        deps.metadata_dict(config.get("profile_snapshot")),
        _twitter_profile_snapshot_from_posts(deps=deps, posts=posts, account_handle=account_handle),
    )
    return rows, retrieval_meta
