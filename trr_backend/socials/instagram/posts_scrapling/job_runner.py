from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
from trr_backend.socials.post_persist_truthfulness import apply_post_persist_truthfulness_metadata

try:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsWarmupError
except ImportError:  # pragma: no cover - removed once the fetcher worker lands.

    class InstagramPostsWarmupError(RuntimeError):
        error_code = "instagram_posts_warmup_failed"
        retryable = False


from trr_backend.socials.instagram import auth_cooldown
from trr_backend.socials.instagram.posts_scrapling.persistence import persist_instagram_posts
from trr_backend.socials.instagram.posts_scrapling.proxy import posts_proxy_feature_flags, select_posts_proxy
from trr_backend.socials.instagram.posts_scrapling.session import (
    build_posts_identity_provider,
    resolve_posts_scrapling_session,
)

_POSTS_PLATFORM = "instagram"
# Bounded rotate-on-block budget (A2): authenticated sessions default to no
# rotation so reputation is preserved; anonymous canary mode may rotate a proxy
# identity without writing account auth cooldown rows.
_POSTS_AUTHENTICATED_ROTATE_ON_BLOCK_MAX_RETRIES_DEFAULT = 0
_POSTS_ANONYMOUS_ROTATE_ON_BLOCK_MAX_RETRIES_DEFAULT = 2
_INSTAGRAM_SCRAPE_MODE_DEFAULT = "public_first"
_INSTAGRAM_SCRAPE_MODE_ENV = "SOCIAL_INSTAGRAM_SCRAPE_MODE"
_INSTAGRAM_SCRAPE_MODE_ALIASES = {
    "": _INSTAGRAM_SCRAPE_MODE_DEFAULT,
    "public": "public_first",
    "public-first": "public_first",
    "public_first": "public_first",
    "no_login": "public_first",
    "nologin": "public_first",
    "anonymous": "anonymous",
    "authenticated": "authenticated",
    "auth": "authenticated",
    "login": "authenticated",
}

logger = logging.getLogger("socials.instagram.posts_scrapling.job_runner")


class _LifecycleProxy:
    def __getattr__(self, name: str) -> Any:
        import trr_backend.socials.control_plane.run_lifecycle as lifecycle

        return getattr(lifecycle, name)


lifecycle = _LifecycleProxy()


@dataclass(slots=True)
class PostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class ScraplingJobCancelledError(Exception):
    message: str
    cancel_scope: str
    job_status: str | None = None
    run_status: str | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


ScraplingJobCancelled = ScraplingJobCancelledError


@dataclass(slots=True)
class PostsAuthCooldownActive(Exception):  # noqa: N818 - established soft-stop control signal
    """Soft-stop signal: an account-scoped auth cooldown is active.

    Raised when ``auth_cooldown.get_active_cooldown`` reports a future deadline at
    job start or before a page request. The job runner catches this and requeues
    the job with ``available_at = cooldown_until`` (mirroring the frontier
    cooldown pattern) rather than burning attempts against a blocked account.
    """

    message: str
    cooldown_until: Any
    error_code: str
    blocker_kind: str
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def _posts_anonymous_enabled(config: dict[str, Any]) -> bool:
    if _instagram_scrape_mode(config) == "anonymous":
        return True
    raw = config.get("anonymous_enabled")
    if raw in (None, ""):
        raw = os.getenv("SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED")
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _instagram_scrape_mode(config: dict[str, Any] | None = None) -> str:
    metadata = config or {}
    raw = (
        metadata.get("instagram_scrape_mode")
        or metadata.get("scrape_mode")
        or os.getenv(_INSTAGRAM_SCRAPE_MODE_ENV)
        or _INSTAGRAM_SCRAPE_MODE_DEFAULT
    )
    normalized = str(raw or "").strip().lower()
    return _INSTAGRAM_SCRAPE_MODE_ALIASES.get(normalized, _INSTAGRAM_SCRAPE_MODE_DEFAULT)


def _posts_public_first_enabled(config: dict[str, Any] | None = None) -> bool:
    return _instagram_scrape_mode(config) == "public_first"


def _rotate_on_block_max_retries(*, anonymous: bool) -> int:
    if anonymous:
        env_name = "SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ROTATE_ON_BLOCK_MAX_RETRIES"
        default = _POSTS_ANONYMOUS_ROTATE_ON_BLOCK_MAX_RETRIES_DEFAULT
    else:
        env_name = "SOCIAL_INSTAGRAM_POSTS_ROTATE_ON_BLOCK_MAX_RETRIES"
        default = _POSTS_AUTHENTICATED_ROTATE_ON_BLOCK_MAX_RETRIES_DEFAULT
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(0, min(5, value))


def _normalize_proxy_session_part(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().lstrip("@")
    return normalized or None


def _coerce_proxy_session_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _posts_pagination_timeout_guard_seconds(config: dict[str, Any]) -> float | None:
    raw = config.get("pagination_timeout_guard_seconds")
    if raw in (None, ""):
        raw = os.getenv("SOCIAL_INSTAGRAM_POSTS_PAGINATION_TIMEOUT_GUARD_SECONDS") or ""
    if raw in (None, ""):
        return 105 * 60.0
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return 105 * 60.0
    return parsed if parsed > 0 else None


def _posts_bidirectional_walk_enabled() -> bool:
    return str(os.getenv("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _posts_rate_per_second(count: int, elapsed_ms: int) -> float | None:
    if count <= 0 or elapsed_ms <= 0:
        return None
    return round(count / (elapsed_ms / 1000.0), 3)


def _posts_pagination_doc_ids_attempted(metadata: dict[str, Any]) -> list[str]:
    for key in ("doc_ids_attempted", "profile_posts_doc_ids_attempted", "profile_posts_doc_ids"):
        value = metadata.get(key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    return []


def _posts_runtime_proxy_fingerprint(metadata: dict[str, Any]) -> str | None:
    for key in ("proxy_fingerprint", "selected_proxy_fingerprint"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    proxy_identity = metadata.get("proxy_identity")
    if isinstance(proxy_identity, dict):
        value = str(proxy_identity.get("configured_fingerprint") or "").strip()
        if value:
            return value
    return None


def _posts_node_identity(post: dict[str, Any]) -> str | None:
    if not isinstance(post, dict):
        return None
    for key in ("id", "pk", "media_id", "code", "shortcode"):
        value = str(post.get(key) or "").strip()
        if value:
            return value
    return None


def _posts_pagination_stop_reason(result: Any) -> str | None:
    reason = str(getattr(result, "fetch_reason", "") or "").strip().lower()
    if reason in {"pagination_doc_id_stale", "graphql_no_doc_id_succeeded"}:
        return reason
    if "cursor" in reason and ("expired" in reason or "stale" in reason or "invalid" in reason):
        return "cursor_expired_restart_required"
    return None


def _public_graphql_connection(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if not isinstance(data, dict):
        return {}
    connection = data.get("xdt_api__v1__feed__user_timeline_graphql_connection")
    return connection if isinstance(connection, dict) else {}


def _public_graphql_page_posts(payload: dict[str, Any] | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    connection = _public_graphql_connection(payload)
    edges = connection.get("edges") if isinstance(connection, dict) else []
    posts = [
        node
        for edge in (edges or [])
        if isinstance(edge, dict)
        for node in [edge.get("node")]
        if isinstance(node, dict) and node
    ]
    page_info = connection.get("page_info") if isinstance(connection.get("page_info"), dict) else {}
    return posts, dict(page_info or {})


def _public_scraper_runtime_metadata(scraper: Any | None = None) -> dict[str, Any]:
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {}) if scraper is not None else {}
    doc_ids_attempted = [
        str(value).strip()
        for value in (retrieval_meta.get("profile_posts_doc_ids_attempted") or [])
        if str(value).strip()
    ]
    doc_id_used = str(retrieval_meta.get("doc_id_used") or retrieval_meta.get("profile_posts_doc_id") or "").strip()
    return {
        "scrape_mode": "public_first",
        "auth_state": "public",
        "proxy_state": "none",
        "selected_proxy_fingerprint": "none",
        "proxy_session_mode": "none",
        "proxy_identity": {
            "configured_fingerprint": "none",
            "observed_identity": None,
            "observed_fingerprint": None,
            "pacing_identity": "instagram:global",
            "redacted_api_proxy_url": None,
            "redacted_browser_proxy": None,
            "session_mode": "none",
        },
        "proxy_session_key": None,
        "transport": retrieval_meta.get("retrieval_transport") or retrieval_meta.get("transport") or "requests_enriched",  # noqa: E501
        "retrieval_transport": retrieval_meta.get("retrieval_transport") or retrieval_meta.get("transport"),
        "graphql_cursor": retrieval_meta.get("graphql_cursor"),
        "request_count": int(getattr(scraper, "_request_count", 0) or 0) if scraper is not None else 0,
        "profile_posts_doc_id": doc_id_used or None,
        "doc_id_used": doc_id_used or None,
        "profile_posts_doc_ids_attempted": doc_ids_attempted,
        "doc_ids_attempted": doc_ids_attempted,
        "profile_posts_doc_ids": {
            "attempted": doc_ids_attempted,
            "used": doc_id_used or None,
            "final_selected": doc_id_used or None,
        },
        "fallback_policy": {
            "auth_fallback": "requires_approval",
            "proxy_fallback": "requires_approval",
            "decodo_fallback": "requires_approval",
        },
        "retrieval_meta": retrieval_meta,
    }


def _raise_if_pagination_state_persist_failed(
    pagination_state: dict[str, Any] | None,
    *,
    account_handle: str,
    direction: str,
    runtime_metadata: dict[str, Any] | None = None,
    listing_progress: dict[str, Any] | None = None,
) -> None:
    if not isinstance(pagination_state, dict):
        return
    reason = str(pagination_state.get("reason") or "").strip().lower()
    if not bool(pagination_state.get("skipped")) or reason != "pagination_state_persist_failed":
        return
    metadata = {
        **dict(runtime_metadata or {}),
        "pagination_state": dict(pagination_state),
        "pagination_checkpoint": {
            "direction": direction,
            "reason": reason,
            "retryable": True,
        },
    }
    if listing_progress is not None:
        metadata["listing_progress"] = dict(listing_progress)
    raise PostsScraplingRuntimeError(
        f"Instagram posts pagination checkpoint could not be saved for @{account_handle}.",
        error_code=reason,
        retryable=True,
        runtime_metadata=metadata,
    )


def _apply_proxy_session_generation(base_key: str, *, rotation_generation: int) -> str:
    """Suffix a rotation generation onto a proxy session key.

    Rotating the session_key re-hashes into a new Decodo sticky session id, i.e.
    a new residential IP (see _proxy_sessions.apply_decodo_session_affinity).
    Generation 0 is the unsuffixed base key so steady-state behaviour and any
    persisted proxy_session_key values are unchanged; only an explicit
    rotate-on-block bumps the generation.
    """
    try:
        generation = int(rotation_generation)
    except (TypeError, ValueError):
        generation = 0
    if generation <= 0:
        return base_key
    return f"{base_key}:gen{generation}"


def _posts_proxy_session_key(
    *,
    account_handle: str,
    stage: str,
    config: dict[str, Any],
    job_metadata: dict[str, Any],
    browser_account_id: str | None,
    rotation_generation: int = 0,
) -> str:
    account = (
        _normalize_proxy_session_part(account_handle) or _normalize_proxy_session_part(browser_account_id) or "unknown"
    )
    normalized_stage = _normalize_proxy_session_part(stage) or "posts_scrapling"

    detail_shard_count = max(1, _coerce_proxy_session_int(config.get("details_refresh_shard_count"), 1))
    detail_shard_index = config.get("details_refresh_shard_index")
    if detail_shard_count > 1 and detail_shard_index not in (None, ""):
        base_key = f"{account}:{normalized_stage}:details:{_coerce_proxy_session_int(detail_shard_index)}"
        return _apply_proxy_session_generation(base_key, rotation_generation=rotation_generation)

    shard_count = max(1, _coerce_proxy_session_int(config.get("shard_count") or config.get("posts_shard_count"), 1))
    shard_index = config.get("shard_index", config.get("posts_shard_index"))
    if shard_count > 1 and shard_index not in (None, ""):
        base_key = f"{account}:{normalized_stage}:posts:{_coerce_proxy_session_int(shard_index)}"
        return _apply_proxy_session_generation(base_key, rotation_generation=rotation_generation)

    worker_lane = _normalize_proxy_session_part(
        config.get("runner_lane") or config.get("worker_lane") or job_metadata.get("worker_lane")
    )
    if worker_lane:
        base_key = f"{account}:{normalized_stage}:lane:{worker_lane}"
        return _apply_proxy_session_generation(base_key, rotation_generation=rotation_generation)

    base_key = str(browser_account_id or account).strip().lower().lstrip("@") or account
    return _apply_proxy_session_generation(base_key, rotation_generation=rotation_generation)


def _raise_if_cancelled(
    *,
    job_id: str,
    run_id: str,
    runtime_metadata: dict[str, Any] | None = None,
    conn: Any | None = None,
) -> None:
    if not job_id:
        return
    started_at = time.perf_counter()
    try:
        job_state = pg.fetch_one("select status from social.scrape_jobs where id = %s", [job_id], conn=conn) or {}
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Skipping posts cancellation check after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return
    job_status = str(job_state.get("status") or "").strip().lower() or None
    run_status: str | None = None
    if run_id:
        try:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id], conn=conn) or {}
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Skipping posts run cancellation check after database saturation: run_id=%s error=%s",
                run_id,
                exc,
            )
            return
        run_status = str(run_state.get("status") or "").strip().lower() or None
    cancel_scope = "job" if job_status == "cancelled" else "run" if run_status == "cancelled" else None
    if not cancel_scope:
        return

    metadata = dict(runtime_metadata or {})
    logger.info(
        "instagram_posts_scrapling cancellation_detected",
        extra={
            "event": "scrapling_job_cancelled",
            "job_id": job_id,
            "run_id": run_id or None,
            "cancel_scope": cancel_scope,
            "job_status": job_status,
            "run_status": run_status,
            "check_latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "request_count": metadata.get("request_count"),
            "warmup_cookie_count": metadata.get("warmup_cookie_count"),
        },
    )
    raise ScraplingJobCancelledError(
        "Instagram posts Scrapling job was cancelled.",
        cancel_scope=cancel_scope,
        job_status=job_status,
        run_status=run_status,
        runtime_metadata=metadata,
    )


def _raise_if_auth_cooldown_active(
    *,
    account_handle: str,
    phase: str,
    runtime_metadata: dict[str, Any] | None = None,
) -> None:
    """A4 READ: soft-stop the job when an account-scoped auth cooldown is active.

    Reads the cross-process cooldown (Postgres-backed, social_control pool). When
    a future deadline is set, raise PostsAuthCooldownActive so the outer handler
    requeues with available_at = cooldown_until instead of issuing more
    authenticated requests. Fails open: any read error means "no cooldown".
    """
    cooldown = auth_cooldown.get_active_cooldown(_POSTS_PLATFORM, account_handle)
    if cooldown is None:
        return
    metadata = dict(runtime_metadata or {})
    metadata["auth_cooldown"] = cooldown.to_metadata()
    logger.warning(
        "instagram_posts_scrapling auth_cooldown_active",
        extra={
            "event": "auth_cooldown_active",
            "phase": phase,
            "account": account_handle,
            "blocker_kind": cooldown.blocker_kind,
            "consecutive_auth_failures": cooldown.consecutive_auth_failures,
            "cooldown_until": cooldown.cooldown_until.isoformat(),
            "last_error_code": cooldown.last_error_code,
        },
    )
    raise PostsAuthCooldownActive(
        f"Instagram posts auth cooldown active for @{account_handle} until {cooldown.cooldown_until.isoformat()}.",
        cooldown_until=cooldown.cooldown_until,
        error_code=cooldown.last_error_code or "instagram_posts_auth_cooldown_active",
        blocker_kind=cooldown.blocker_kind,
        runtime_metadata=metadata,
    )


def _posts_per_identity_session_rotation_budget() -> int:
    """How many sticky-session rotations (A2) to try before advancing identity (A3).

    With a single identity the pool yields one entry, so advancing is a no-op and
    this budget only governs the labelling/telemetry of which rotation tier we are
    in. Defaults to 1: rotate the sticky session once, then (if a real second
    identity exists) advance to it on the next block.
    """
    raw = (os.getenv("SOCIAL_INSTAGRAM_POSTS_PER_IDENTITY_SESSION_ROTATION_BUDGET") or "").strip()
    if not raw:
        return 1
    try:
        value = int(raw)
    except ValueError:
        return 1
    return max(1, min(5, value))


async def _rotate_after_auth_block(
    *,
    fetcher: InstagramPostsScraplingFetcher,
    account_handle: str,
    stage: str,
    config: dict[str, Any],
    job_metadata: dict[str, Any],
    browser_account_id: str | None,
    rotation_generation: int,
    block_error_code: str,
) -> dict[str, Any]:
    """A2 + A3: rotate the proxy/identity after a classified hard 401/403.

    Always selects a fresh Decodo sticky session via a bumped generation suffix on
    the proxy session key (A2 — new residential IP). When the per-identity
    session-rotation budget is exhausted and the identity pool is enabled, also
    advances to the next pool identity via ``fetcher.rotate_session`` (A3 —
    mirrors scraper._maybe_rotate_identity_after_failure). Returns the new proxy
    session key and whether an identity rotation occurred.
    """
    new_proxy_session_key = _posts_proxy_session_key(
        account_handle=account_handle,
        stage=stage,
        config=config,
        job_metadata=job_metadata,
        browser_account_id=browser_account_id,
        rotation_generation=rotation_generation,
    )
    new_proxy_config = select_posts_proxy(session_key=new_proxy_session_key or account_handle)

    identity_rotated = False
    advance_identity = rotation_generation > _posts_per_identity_session_rotation_budget()
    if advance_identity and hasattr(fetcher, "rotate_session"):
        try:
            identity_rotated = bool(
                await fetcher.rotate_session(
                    proxy_config=new_proxy_config,
                    reason=f"rotate_on_block:{block_error_code}",
                )
            )
        except Exception as exc:  # noqa: BLE001 - rotation is best-effort
            logger.warning(
                "instagram_posts_scrapling rotate_session_failed account=%s error=%s",
                account_handle,
                exc,
                exc_info=True,
            )
            identity_rotated = False
    if not identity_rotated:
        # A2 only: swap the direct GraphQL proxy route (fresh sticky session)
        # without rerunning browser warmup or re-resolving the identity.
        await fetcher.set_api_proxy_config(new_proxy_config, reason="rotate_on_block")
    return {
        "proxy_session_key": new_proxy_session_key,
        "identity_rotated": identity_rotated,
    }


def run_instagram_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.socials.control_plane.dispatch_runtime import legacy as repo

    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    job_metadata = dict(job.get("metadata") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or "posts_scrapling").strip().lower()
    fast_mode = bool(config.get("fast_mode", False))
    source_scope = str(config.get("source_scope") or "network").strip().lower() or "network"
    season_id = str(config.get("season_id") or "").strip() or None
    instagram_scrape_mode = _instagram_scrape_mode(config)
    public_first_enabled = instagram_scrape_mode == "public_first"
    anonymous_enabled = _posts_anonymous_enabled(config)

    if not account_handle:
        raise PostsScraplingRuntimeError(
            "Instagram posts Scrapling job is missing an account handle.",
            error_code="instagram_posts_account_missing",
            retryable=False,
        )

    progress_state = lifecycle.new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}
    pages_fetched = 0
    reverse_posts_fetched = 0
    reverse_posts_upserted = 0
    reverse_pages_fetched = 0
    reverse_stop_reason: str | None = None
    fetcher_metadata: dict[str, Any] = {}
    pagination_state: dict[str, Any] = {}
    stop_reason: str | None = None
    bidirectional_probe_done = False
    bidirectional_reverse_started = False
    bidirectional_reverse_error: str | None = None
    timeout_guard_seconds = _posts_pagination_timeout_guard_seconds(config)
    started_monotonic = time.monotonic()
    warmup_duration_ms = 0
    listing_duration_ms = 0
    persistence_duration_ms = 0

    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal posts_fetched, posts_upserted, posts_skipped, posts_skipped_by_reason
        nonlocal pages_fetched, fetcher_metadata, bidirectional_probe_done
        nonlocal reverse_posts_fetched, reverse_posts_upserted, reverse_pages_fetched, reverse_stop_reason
        nonlocal bidirectional_reverse_started, bidirectional_reverse_error
        nonlocal pagination_state, stop_reason
        nonlocal warmup_duration_ms, listing_duration_ms, persistence_duration_ms

        if public_first_enabled:
            from trr_backend.socials.instagram.scraper import InstagramScraper

            scraper = InstagramScraper(cookies={}, browser_account_id=None, attach_auth_session=False)
            auth_metadata = {
                "source": "public",
                "browser_account_id": None,
                "validation_category": "public",
            }
            fetcher_metadata = _public_scraper_runtime_metadata(scraper)
            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
            restart_requested = bool(config.get("restart") or config.get("restart_pagination"))
            cursor: str | None = (
                str(config.get("resume_cursor") or config.get("resume_frontier_cursor") or "").strip() or None
            )
            if not cursor and not restart_requested:
                pagination_state = repo.latest_instagram_profile_pagination_state(
                    account_handle=account_handle,
                    source_scope=source_scope,
                    run_id=run_id or None,
                    direction="forward",
                )
                cursor = str(pagination_state.get("end_cursor") or "").strip() or None
            seen_cursors: set[str] = set()
            unique_post_ids: set[str] = set()

            while True:
                timeout_guard_elapsed = (
                    timeout_guard_seconds is not None
                    and (time.monotonic() - started_monotonic) >= timeout_guard_seconds
                )
                if timeout_guard_elapsed:
                    stop_reason = "timeout_guard"
                    pagination_state = repo.persist_instagram_profile_pagination_state(
                        run_id=run_id or None,
                        job_id=job_id,
                        account_handle=account_handle,
                        source_scope=source_scope,
                        direction="forward",
                        cursor_in=cursor,
                        end_cursor=cursor,
                        page_index=pages_fetched,
                        posts_seen=posts_fetched,
                        posts_upserted=posts_upserted,
                        doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                        doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                        proxy_fingerprint="none",
                        proxy_session_key=None,
                        stop_reason=stop_reason,
                        partial=True,
                        completed=False,
                        metadata={
                            "reason": stop_reason,
                            "listing_progress": True,
                            "instagram_scrape_mode": "public_first",
                            "auth_state": "public",
                            "proxy_state": "none",
                        },
                    )
                    _raise_if_pagination_state_persist_failed(
                        pagination_state,
                        account_handle=account_handle,
                        direction="forward",
                        runtime_metadata=fetcher_metadata,
                        listing_progress={
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "stop_reason": stop_reason,
                            "partial": True,
                        },
                    )
                    break

                lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(
                    job_id=job_id,
                    run_id=run_id,
                    runtime_metadata=fetcher_metadata,
                )
                phase_started = time.monotonic()
                payload = await asyncio.to_thread(
                    scraper.fetch_posts_graphql,
                    account_handle,
                    cursor=cursor,
                    delay=float(os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "0.15"),
                    fast_mode=fast_mode,
                    allow_browser_fallback=False,
                    allow_recovery=False,
                )
                listing_duration_ms += int((time.monotonic() - phase_started) * 1000)
                fetcher_metadata = _public_scraper_runtime_metadata(scraper)
                page_posts, page_info = _public_graphql_page_posts(payload if isinstance(payload, dict) else None)
                retrieval_meta = dict(fetcher_metadata.get("retrieval_meta") or {})
                error_code = str(
                    retrieval_meta.get("error_code")
                    or retrieval_meta.get("request_error_code")
                    or "public_graphql_no_connection"
                ).strip()

                if not page_posts and not _public_graphql_connection(payload if isinstance(payload, dict) else None):
                    if posts_fetched > 0:
                        stop_reason = (
                            "public_pagination_rate_limited"
                            if "rate_limited" in error_code or retrieval_meta.get("error_status_code") == 429
                            else "public_account_pagination_requires_approval"
                        )
                        pagination_state = repo.persist_instagram_profile_pagination_state(
                            run_id=run_id or None,
                            job_id=job_id,
                            account_handle=account_handle,
                            source_scope=source_scope,
                            direction="forward",
                            cursor_in=cursor,
                            end_cursor=cursor,
                            page_index=pages_fetched,
                            posts_seen=posts_fetched,
                            posts_upserted=posts_upserted,
                            doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                            doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                            proxy_fingerprint="none",
                            proxy_session_key=None,
                            stop_reason=stop_reason,
                            partial=True,
                            completed=False,
                            metadata={
                                "fetch_reason": error_code,
                                "instagram_scrape_mode": "public_first",
                                "auth_state": "public",
                                "proxy_state": "none",
                                "fallback_requires_approval": True,
                            },
                        )
                        break
                    raise PostsScraplingRuntimeError(
                        f"Public Instagram posts pagination failed for @{account_handle}: {error_code}.",
                        error_code=error_code or "public_graphql_no_connection",
                        retryable=bool(retrieval_meta.get("retryable") or retrieval_meta.get("request_error_retryable")),  # noqa: E501
                        runtime_metadata={
                            **fetcher_metadata,
                            "fallback_requires_approval": True,
                        },
                    )

                if page_posts:
                    phase_started = time.monotonic()
                    persisted = persist_instagram_posts(
                        account_handle=account_handle,
                        post_nodes=page_posts,
                        run_id=run_id or None,
                        job_id=job_id,
                        season_id=season_id,
                        source_scope=source_scope,
                    )
                    persistence_duration_ms += int((time.monotonic() - phase_started) * 1000)
                    posts_fetched += len(page_posts)
                    posts_upserted += persisted.posts_upserted
                    posts_skipped += persisted.posts_skipped
                    for reason, count in persisted.posts_skipped_by_reason.items():
                        posts_skipped_by_reason[reason] = posts_skipped_by_reason.get(reason, 0) + int(count or 0)
                    unique_post_ids.update(
                        identity for post in page_posts if (identity := _posts_node_identity(post))
                    )

                pages_fetched += 1
                has_next = bool(page_info.get("has_next_page"))
                next_cursor = str(page_info.get("end_cursor") or "").strip() or None
                if next_cursor and next_cursor in seen_cursors:
                    stop_reason = "repeating_cursor"
                elif not has_next or not next_cursor:
                    stop_reason = "public_pagination_ok"
                else:
                    stop_reason = None

                pagination_state = repo.persist_instagram_profile_pagination_state(
                    run_id=run_id or None,
                    job_id=job_id,
                    account_handle=account_handle,
                    source_scope=source_scope,
                    direction="forward",
                    cursor_in=cursor,
                    end_cursor=next_cursor,
                    page_index=pages_fetched,
                    posts_seen=posts_fetched,
                    posts_upserted=posts_upserted,
                    doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                    doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                    proxy_fingerprint="none",
                    proxy_session_key=None,
                    stop_reason=stop_reason,
                    partial=stop_reason not in {None, "public_pagination_ok"},
                    completed=stop_reason == "public_pagination_ok",
                    metadata={
                        "fetch_reason": None,
                        "has_next_page": has_next,
                        "instagram_scrape_mode": "public_first",
                        "auth_state": "public",
                        "proxy_state": "none",
                        "page_number": pages_fetched,
                        "cursor_in": cursor,
                        "end_cursor": next_cursor,
                        "doc_id": str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                        "transport": fetcher_metadata.get("transport"),
                        "unique_shortcode_count": len(unique_post_ids),
                    },
                )
                _raise_if_pagination_state_persist_failed(
                    pagination_state,
                    account_handle=account_handle,
                    direction="forward",
                    runtime_metadata=fetcher_metadata,
                    listing_progress={
                        "page_index": pages_fetched,
                        "posts_seen": posts_fetched,
                        "posts_upserted": posts_upserted,
                        "stop_reason": stop_reason,
                        "partial": stop_reason not in {None, "public_pagination_ok"},
                    },
                )
                lifecycle.emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="instagram",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={
                        "phase": "posts_public_first_running",
                        "pages_fetched": pages_fetched,
                        "listing_progress": {
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "end_cursor": next_cursor,
                            "stop_reason": stop_reason,
                            "auth_state": "public",
                            "proxy_state": "none",
                        },
                    },
                    progress_state=progress_state,
                    force=not has_next,
                )
                if stop_reason:
                    break
                if next_cursor:
                    seen_cursors.add(next_cursor)
                cursor = next_cursor

            fetcher_metadata = _public_scraper_runtime_metadata(scraper)
            return auth_metadata, fetcher_metadata

        # A4 READ (job start): authenticated runs honor account cooldowns before
        # warmup. Anonymous canaries have no account session to burn, so they do
        # not read or write account auth cooldown state.
        if not anonymous_enabled:
            _raise_if_auth_cooldown_active(
                account_handle=account_handle,
                phase="job_start",
                runtime_metadata={"proxy_session_key": None},
            )

        session = (
            None
            if anonymous_enabled
            else resolve_posts_scrapling_session(
                browser_account_id=account_handle,
                caller_context=f"posts_scrapling:{account_handle}",
            )
        )
        # A2: rotation generation suffixed onto the proxy session key. Bumped only
        # on a classified hard 401/403 so a fresh Decodo sticky session (new
        # residential IP) is selected for the bounded same-cursor retry.
        rotation_generation = 0
        proxy_session_key = _posts_proxy_session_key(
            account_handle=account_handle,
            stage=stage,
            config=config,
            job_metadata=job_metadata,
            browser_account_id=session.browser_account_id if session is not None else None,
            rotation_generation=rotation_generation,
        )
        proxy_config = select_posts_proxy(session_key=proxy_session_key or account_handle)
        # A3: identity-pool seam. None when SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED
        # is off (or only one identity exists), in which case rotate_session only
        # swaps the proxy sticky session. Acquired at session-resolve.
        identity_provider = build_posts_identity_provider(session) if session is not None else None
        fetcher = InstagramPostsScraplingFetcher(
            cookies=[] if anonymous_enabled else session.cookies,
            raw_cookies={} if anonymous_enabled else session.auth_session.cookies,
            browser_account_id=None if anonymous_enabled else session.browser_account_id,
            proxy_config=proxy_config,
            fast_mode=fast_mode,
            identity_provider=identity_provider,
            auth_state="anonymous" if anonymous_enabled else "authenticated",
        )
        proxy_flags = posts_proxy_feature_flags()
        forward_seen_post_ids: set[str] = set()
        reverse_seen_post_ids: set[str] = set()
        reverse_task: asyncio.Task[None] | None = None

        def _fetcher_runtime_metadata() -> dict[str, Any]:
            return {**dict(fetcher.runtime_metadata), "proxy_session_key": proxy_session_key}

        async def _run_reverse_listing(snapshot: dict[str, Any]) -> None:
            nonlocal reverse_posts_fetched, reverse_posts_upserted, reverse_pages_fetched, reverse_stop_reason
            nonlocal bidirectional_reverse_error
            nonlocal listing_duration_ms, persistence_duration_ms

            reverse_proxy_session_key = f"{proxy_session_key}:reverse"
            reverse_proxy_config = select_posts_proxy(session_key=reverse_proxy_session_key)
            reverse_fetcher = InstagramPostsScraplingFetcher(
                cookies=[] if anonymous_enabled else session.cookies,
                raw_cookies={} if anonymous_enabled else session.auth_session.cookies,
                browser_account_id=None if anonymous_enabled else session.browser_account_id,
                proxy_config=reverse_proxy_config,
                fast_mode=fast_mode,
                auth_state="anonymous" if anonymous_enabled else "authenticated",
            )
            try:
                await reverse_fetcher.apply_warmup_snapshot(snapshot)
                reverse_cursor: str | None = None
                while True:
                    if proxy_flags["page_proxy_rotation_enabled"]:
                        page_proxy_config = select_posts_proxy(
                            session_key=reverse_proxy_session_key,
                            page_index=reverse_pages_fetched,
                        )
                        await reverse_fetcher.set_api_proxy_config(
                            page_proxy_config,
                            reason=f"reverse_page_{reverse_pages_fetched}",
                        )
                    phase_started = time.monotonic()
                    result = await reverse_fetcher.fetch_posts_page(
                        account_handle,
                        cursor=reverse_cursor,
                        direction="reverse",
                    )
                    listing_duration_ms += int((time.monotonic() - phase_started) * 1000)
                    if result.auth_failed or (result.fetch_failed and not result.posts):
                        reverse_stop_reason = str(result.fetch_reason or "reverse_fetch_failed").strip()
                        bidirectional_reverse_error = reverse_stop_reason
                        break

                    page_ids = {identity for post in result.posts if (identity := _posts_node_identity(post))}
                    overlap = bool(page_ids & forward_seen_post_ids)
                    if result.posts:
                        phase_started = time.monotonic()
                        persisted = persist_instagram_posts(
                            account_handle=account_handle,
                            post_nodes=result.posts,
                            run_id=run_id or None,
                            job_id=job_id,
                            season_id=season_id,
                            source_scope=source_scope,
                        )
                        persistence_duration_ms += int((time.monotonic() - phase_started) * 1000)
                        reverse_posts_fetched += len(result.posts)
                        reverse_posts_upserted += persisted.posts_upserted
                        reverse_seen_post_ids.update(page_ids)

                    reverse_pages_fetched += 1
                    reverse_stop_reason = "bidirectional_overlap" if overlap else None
                    if not reverse_stop_reason and (not result.has_next_page or not result.end_cursor):
                        reverse_stop_reason = "completed"

                    reverse_pagination_state = repo.persist_instagram_profile_pagination_state(
                        run_id=run_id or None,
                        job_id=job_id,
                        account_handle=account_handle,
                        source_scope=source_scope,
                        direction="reverse",
                        cursor_in=reverse_cursor,
                        end_cursor=result.end_cursor,
                        page_index=reverse_pages_fetched,
                        posts_seen=reverse_posts_fetched,
                        posts_upserted=reverse_posts_upserted,
                        doc_id_used=str(reverse_fetcher.runtime_metadata.get("doc_id_used") or "").strip() or None,
                        doc_ids_attempted=_posts_pagination_doc_ids_attempted(dict(reverse_fetcher.runtime_metadata)),
                        proxy_fingerprint=_posts_runtime_proxy_fingerprint(dict(reverse_fetcher.runtime_metadata)),
                        proxy_session_key=reverse_proxy_session_key,
                        stop_reason=reverse_stop_reason,
                        partial=reverse_stop_reason != "completed",
                        completed=reverse_stop_reason == "completed",
                        metadata={
                            "bidirectional_reverse_walker": True,
                            "overlap_with_forward": overlap,
                        },
                    )
                    _raise_if_pagination_state_persist_failed(
                        reverse_pagination_state,
                        account_handle=account_handle,
                        direction="reverse",
                        runtime_metadata={
                            **dict(reverse_fetcher.runtime_metadata),
                            "proxy_session_key": reverse_proxy_session_key,
                        },
                        listing_progress={
                            "page_index": reverse_pages_fetched,
                            "posts_seen": reverse_posts_fetched,
                            "posts_upserted": reverse_posts_upserted,
                            "stop_reason": reverse_stop_reason,
                            "partial": reverse_stop_reason != "completed",
                        },
                    )
                    if reverse_stop_reason:
                        break
                    reverse_cursor = result.end_cursor
            except PostsScraplingRuntimeError:
                raise
            except Exception as exc:  # noqa: BLE001
                bidirectional_reverse_error = str(exc)
                reverse_stop_reason = "reverse_walker_failed"
                logger.warning(
                    "instagram_posts_scrapling reverse_walker_failed account=%s error=%s",
                    account_handle,
                    exc,
                    exc_info=True,
                )
            finally:
                await reverse_fetcher.aclose()

        try:
            try:
                phase_started = time.monotonic()
                await fetcher.warmup(account_handle)
                warmup_duration_ms += int((time.monotonic() - phase_started) * 1000)
            except InstagramPostsWarmupError as exc:
                warmup_duration_ms += int((time.monotonic() - phase_started) * 1000)
                raise PostsScraplingRuntimeError(
                    str(exc),
                    error_code=str(getattr(exc, "error_code", "") or "instagram_posts_warmup_failed"),
                    retryable=bool(getattr(exc, "retryable", False)),
                    runtime_metadata=_fetcher_runtime_metadata(),
                ) from exc
            auth_metadata = {} if session is None else dict(session.auth_session.metadata or {})
            fetcher_metadata = _fetcher_runtime_metadata()

            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
            restart_requested = bool(config.get("restart") or config.get("restart_pagination"))
            cursor: str | None = (
                str(config.get("resume_cursor") or config.get("resume_frontier_cursor") or "").strip() or None
            )
            if not cursor and not restart_requested:
                pagination_state = repo.latest_instagram_profile_pagination_state(
                    account_handle=account_handle,
                    source_scope=source_scope,
                    run_id=run_id or None,
                    direction="forward",
                )
                cursor = str(pagination_state.get("end_cursor") or "").strip() or None

            while True:
                timeout_guard_elapsed = (
                    timeout_guard_seconds is not None
                    and (time.monotonic() - started_monotonic) >= timeout_guard_seconds
                )
                if timeout_guard_elapsed:
                    stop_reason = "timeout_guard"
                    pagination_state = repo.persist_instagram_profile_pagination_state(
                        run_id=run_id or None,
                        job_id=job_id,
                        account_handle=account_handle,
                        source_scope=source_scope,
                        direction="forward",
                        cursor_in=cursor,
                        end_cursor=cursor,
                        page_index=pages_fetched,
                        posts_seen=posts_fetched,
                        posts_upserted=posts_upserted,
                        doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                        doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                        proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                        proxy_session_key=proxy_session_key,
                        stop_reason=stop_reason,
                        partial=True,
                        completed=False,
                        metadata={"reason": stop_reason, "listing_progress": True},
                    )
                    _raise_if_pagination_state_persist_failed(
                        pagination_state,
                        account_handle=account_handle,
                        direction="forward",
                        runtime_metadata=_fetcher_runtime_metadata(),
                        listing_progress={
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "stop_reason": stop_reason,
                            "partial": True,
                        },
                    )
                    break
                lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(
                    job_id=job_id,
                    run_id=run_id,
                    runtime_metadata=_fetcher_runtime_metadata(),
                )
                # A4 READ (per-page): another container may have parked the
                # account on a cooldown since this job started. Soft-stop and
                # requeue at cooldown_until rather than issuing the next page.
                if not anonymous_enabled:
                    _raise_if_auth_cooldown_active(
                        account_handle=account_handle,
                        phase="pre_page",
                        runtime_metadata=_fetcher_runtime_metadata(),
                    )
                if proxy_flags["page_proxy_rotation_enabled"]:
                    page_proxy_config = select_posts_proxy(
                        session_key=proxy_session_key or account_handle,
                        page_index=pages_fetched,
                    )
                    await fetcher.set_api_proxy_config(
                        page_proxy_config,
                        reason=f"page_{pages_fetched}",
                    )
                    fetcher_metadata = _fetcher_runtime_metadata()
                phase_started = time.monotonic()
                result = await fetcher.fetch_posts_page(account_handle, cursor=cursor)
                listing_duration_ms += int((time.monotonic() - phase_started) * 1000)
                fetcher_metadata = _fetcher_runtime_metadata()

                if result.auth_failed:
                    # Converged auth-block handler (A2 + A3 + A4). Classify the
                    # block: a checkpoint/challenge is non-clearing and must NOT
                    # auto-rotate-retry; a plain 401/403 gets a bounded
                    # rotate-on-block retry (fresh sticky session => new IP, and
                    # the next pool identity when the per-identity budget is
                    # exhausted) before we record a cooldown and fail.
                    block_error_code = str(result.fetch_reason or "instagram_posts_auth_failed").strip()
                    is_checkpoint = auth_cooldown.is_checkpoint_error_code(block_error_code)
                    if not is_checkpoint and rotation_generation < _rotate_on_block_max_retries(
                        anonymous=anonymous_enabled
                    ):
                        rotation_generation += 1
                        rotated = await _rotate_after_auth_block(
                            fetcher=fetcher,
                            account_handle=account_handle,
                            stage=stage,
                            config=config,
                            job_metadata=job_metadata,
                            browser_account_id=session.browser_account_id if session is not None else None,
                            rotation_generation=rotation_generation,
                            block_error_code=block_error_code,
                        )
                        proxy_session_key = rotated["proxy_session_key"]
                        fetcher_metadata = _fetcher_runtime_metadata()
                        logger.warning(
                            "instagram_posts_scrapling rotate_on_block",
                            extra={
                                "event": "rotate_on_block",
                                "account": account_handle,
                                "rotation_generation": rotation_generation,
                                "block_error_code": block_error_code,
                                "identity_rotated": rotated["identity_rotated"],
                                "proxy_session_key": proxy_session_key,
                            },
                        )
                        # Retry the SAME cursor on the fresh session/identity.
                        continue
                    if anonymous_enabled:
                        raise PostsScraplingRuntimeError(
                            f"Anonymous Instagram posts retry budget exhausted for @{account_handle}.",
                            error_code="anonymous_retry_exhausted",
                            retryable=True,
                            runtime_metadata={
                                **_fetcher_runtime_metadata(),
                                "fetch_reason": result.fetch_reason,
                                "auth_block_kind": "anonymous",
                                "rotation_generation": rotation_generation,
                            },
                        )
                    # Either a checkpoint (no rotate) or rotation budget exhausted:
                    # record the cross-process cooldown and fail this attempt.
                    cooldown = auth_cooldown.record_auth_block(
                        _POSTS_PLATFORM,
                        account_handle,
                        block_error_code,
                    )
                    raise PostsScraplingRuntimeError(
                        (
                            f"Instagram posts checkpoint required for @{account_handle}."
                            if is_checkpoint
                            else f"Instagram auth failed while fetching posts for @{account_handle}."
                        ),
                        error_code=(
                            "instagram_posts_checkpoint_required"
                            if is_checkpoint
                            else "instagram_posts_auth_failed"
                        ),
                        retryable=False,
                        runtime_metadata={
                            **_fetcher_runtime_metadata(),
                            "fetch_reason": result.fetch_reason,
                            "auth_block_kind": "checkpoint" if is_checkpoint else "auth",
                            "rotation_generation": rotation_generation,
                            "auth_cooldown": cooldown.to_metadata() if cooldown is not None else None,
                        },
                    )
                if result.fetch_failed and not result.posts:
                    stop_reason = _posts_pagination_stop_reason(result)
                    if stop_reason == "cursor_expired_restart_required":
                        pagination_state = repo.persist_instagram_profile_pagination_state(
                            run_id=run_id or None,
                            job_id=job_id,
                            account_handle=account_handle,
                            source_scope=source_scope,
                            direction="forward",
                            cursor_in=cursor,
                            end_cursor=cursor,
                            page_index=pages_fetched,
                            posts_seen=posts_fetched,
                            posts_upserted=posts_upserted,
                            doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                            doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                            proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                            proxy_session_key=proxy_session_key,
                            stop_reason=stop_reason,
                            partial=True,
                            completed=False,
                            metadata={"fetch_reason": getattr(result, "fetch_reason", None)},
                        )
                        _raise_if_pagination_state_persist_failed(
                            pagination_state,
                            account_handle=account_handle,
                            direction="forward",
                            runtime_metadata={**_fetcher_runtime_metadata(), "fetch_reason": result.fetch_reason},
                            listing_progress={
                                "page_index": pages_fetched,
                                "posts_seen": posts_fetched,
                                "posts_upserted": posts_upserted,
                                "stop_reason": stop_reason,
                                "partial": True,
                            },
                        )
                    raise PostsScraplingRuntimeError(
                        f"Instagram posts fetch failed for @{account_handle}.",
                        error_code=stop_reason or str(result.fetch_reason or "instagram_posts_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={**_fetcher_runtime_metadata(), "fetch_reason": result.fetch_reason},
                    )

                # A4 reset: a clean page fetch (no auth failure, no fetch failure)
                # proves the account is healthy again. Clear the cross-process
                # cooldown so sibling containers stop deferring. Checkpoint
                # blockers are non-clearing inside clear_cooldown.
                if not anonymous_enabled:
                    auth_cooldown.clear_cooldown(_POSTS_PLATFORM, account_handle)

                if result.posts:
                    if not bidirectional_probe_done and _posts_bidirectional_walk_enabled():
                        probe_metadata = await fetcher.probe_bidirectional_walk(
                            account_handle,
                            forward_posts=result.posts,
                        )
                        bidirectional_probe_done = True
                        if probe_metadata.get("passed") and reverse_task is None:
                            bidirectional_reverse_started = True
                            reverse_task = asyncio.create_task(_run_reverse_listing(fetcher.warmup_snapshot()))
                        fetcher_metadata = _fetcher_runtime_metadata()
                    phase_started = time.monotonic()
                    persisted = persist_instagram_posts(
                        account_handle=account_handle,
                        post_nodes=result.posts,
                        run_id=run_id or None,
                        job_id=job_id,
                        season_id=season_id,
                        source_scope=source_scope,
                    )
                    persistence_duration_ms += int((time.monotonic() - phase_started) * 1000)
                    posts_fetched += len(result.posts)
                    posts_upserted += persisted.posts_upserted
                    posts_skipped += persisted.posts_skipped
                    for reason, count in persisted.posts_skipped_by_reason.items():
                        posts_skipped_by_reason[reason] = posts_skipped_by_reason.get(reason, 0) + int(count or 0)
                    forward_seen_post_ids.update(
                        identity for post in result.posts if (identity := _posts_node_identity(post))
                    )

                pages_fetched += 1
                stop_reason = _posts_pagination_stop_reason(result)
                if not stop_reason and (not result.has_next_page or not result.end_cursor):
                    stop_reason = "completed"
                pagination_state = repo.persist_instagram_profile_pagination_state(
                    run_id=run_id or None,
                    job_id=job_id,
                    account_handle=account_handle,
                    source_scope=source_scope,
                    direction="forward",
                    cursor_in=cursor,
                    end_cursor=result.end_cursor,
                    page_index=pages_fetched,
                    posts_seen=posts_fetched,
                    posts_upserted=posts_upserted,
                    doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                    doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                    proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                    proxy_session_key=proxy_session_key,
                    stop_reason=stop_reason,
                    partial=stop_reason != "completed",
                    completed=stop_reason == "completed",
                    metadata={
                        "fetch_reason": getattr(result, "fetch_reason", None),
                        "has_next_page": bool(result.has_next_page),
                    },
                )
                _raise_if_pagination_state_persist_failed(
                    pagination_state,
                    account_handle=account_handle,
                    direction="forward",
                    runtime_metadata=_fetcher_runtime_metadata(),
                    listing_progress={
                        "page_index": pages_fetched,
                        "posts_seen": posts_fetched,
                        "posts_upserted": posts_upserted,
                        "stop_reason": stop_reason,
                        "partial": stop_reason != "completed",
                    },
                )
                lifecycle.emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="instagram",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={
                        "phase": "posts_scrapling_running",
                        "pages_fetched": pages_fetched,
                        "listing_progress": {
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "end_cursor": result.end_cursor,
                            "stop_reason": stop_reason,
                        },
                    },
                    progress_state=progress_state,
                    force=not result.has_next_page,
                )

                if stop_reason == "pagination_doc_id_stale":
                    break
                if not result.has_next_page or not result.end_cursor:
                    break
                cursor = result.end_cursor

            if reverse_task is not None:
                await reverse_task
                fetcher_metadata = _fetcher_runtime_metadata()

            if stop_reason in {"timeout_guard", "pagination_doc_id_stale"}:
                raise PostsScraplingRuntimeError(
                    f"Instagram posts pagination stopped for @{account_handle}: {stop_reason}.",
                    error_code=stop_reason,
                    retryable=stop_reason == "timeout_guard",
                    runtime_metadata={
                        **_fetcher_runtime_metadata(),
                        "pagination_state": pagination_state,
                        "listing_progress": {
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "stop_reason": stop_reason,
                            "partial": True,
                        },
                    },
                )

            fetcher_metadata = _fetcher_runtime_metadata()
            return auth_metadata, fetcher_metadata
        finally:
            if reverse_task is not None and not reverse_task.done():
                reverse_task.cancel()
                try:
                    await reverse_task
                except asyncio.CancelledError:
                    pass
            fetcher_metadata = _fetcher_runtime_metadata()
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        total_posts_fetched = posts_fetched + reverse_posts_fetched
        total_pages_fetched = pages_fetched + reverse_pages_fetched
        elapsed_ms = int((time.monotonic() - started_monotonic) * 1000)
        doc_ids_attempted = _posts_pagination_doc_ids_attempted(fetcher_metadata)
        proxy_pacing = (
            fetcher_metadata.get("proxy_pacing") if isinstance(fetcher_metadata.get("proxy_pacing"), dict) else {}
        )
        performance_metadata = {
            "elapsed_ms": elapsed_ms,
            "warmup_duration_ms": warmup_duration_ms,
            "listing_duration_ms": listing_duration_ms,
            "persistence_duration_ms": persistence_duration_ms,
            "pages_per_second": _posts_rate_per_second(total_pages_fetched, listing_duration_ms),
            "posts_per_second": _posts_rate_per_second(
                total_posts_fetched,
                listing_duration_ms + persistence_duration_ms,
            ),
            "doc_id_attempts": len(doc_ids_attempted),
            "doc_ids_attempted": doc_ids_attempted,
            "warmup_pool": fetcher_metadata.get("warmup_pool") if isinstance(fetcher_metadata, dict) else {},
            "bytes_total": int(fetcher_metadata.get("bytes_total") or proxy_pacing.get("bytes_total") or 0),
            "bytes_by_host": fetcher_metadata.get("bytes_by_host") or proxy_pacing.get("bytes_by_host") or {},
        }
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "fast_mode": fast_mode,
            "instagram_scrape_mode": instagram_scrape_mode,
            "auth_state": fetcher_metadata.get("auth_state"),
            "proxy_state": fetcher_metadata.get("proxy_state")
            or ("none" if fetcher_metadata.get("selected_proxy_fingerprint") == "none" else "configured"),
            "source_scope": source_scope,
            "stage_counters": {
                "posts": posts_fetched + reverse_posts_fetched,
                "pages": pages_fetched + reverse_pages_fetched,
                "forward_posts": posts_fetched,
                "forward_pages": pages_fetched,
                "reverse_posts": reverse_posts_fetched,
                "reverse_pages": reverse_pages_fetched,
            },
            "persist_counters": {
                "posts_upserted": posts_upserted,
                "posts_skipped": posts_skipped,
                "posts_skipped_by_reason": posts_skipped_by_reason,
            },
            "posts_scrapling_persist_diagnostics": {
                "posts_upserted": posts_upserted,
                "posts_skipped": posts_skipped,
                "posts_skipped_by_reason": posts_skipped_by_reason,
            },
            "performance": performance_metadata,
            "pagination_state": pagination_state,
            "listing_progress": {
                "page_index": pages_fetched,
                "posts_seen": posts_fetched,
                "posts_upserted": posts_upserted,
                "stop_reason": stop_reason,
                "partial": stop_reason not in {None, "completed", "public_pagination_ok"},
            },
            "bidirectional_listing": {
                "reverse_started": bidirectional_reverse_started,
                "reverse_pages_fetched": reverse_pages_fetched,
                "reverse_posts_seen": reverse_posts_fetched,
                "reverse_posts_upserted": reverse_posts_upserted,
                "reverse_stop_reason": reverse_stop_reason,
                "reverse_error": bidirectional_reverse_error,
            },
            "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
            "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
            "activity": {
                "phase": "posts_scrapling_end",
                "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
            },
            "auth_context": {
                "session_source": auth_metadata.get("source"),
                "browser_account_id": auth_metadata.get("browser_account_id"),
                "validation_category": auth_metadata.get("validation_category"),
                "proxy_session_key": fetcher_metadata.get("proxy_session_key"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        metadata = apply_post_persist_truthfulness_metadata(
            metadata,
            platform="instagram",
            account=account_handle,
            status="completed",
            posts_checked=posts_fetched + reverse_posts_fetched,
            posts_upserted=posts_upserted + reverse_posts_upserted,
            posts_skipped=posts_skipped,
            posts_skipped_by_reason=posts_skipped_by_reason,
        )
        lifecycle.finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched + reverse_posts_fetched,
            metadata=metadata,
        )
        terminal_status = "completed"
        terminal_error_message: str | None = None
    except ScraplingJobCancelledError as exc:
        lifecycle.finish_job(
            job_id,
            status="cancelled",
            items_found=posts_fetched + reverse_posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "activity": {"phase": "cancelled", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": {
                    "posts": posts_fetched + reverse_posts_fetched,
                    "pages": pages_fetched + reverse_pages_fetched,
                    "forward_posts": posts_fetched,
                    "forward_pages": pages_fetched,
                    "reverse_posts": reverse_posts_fetched,
                    "reverse_pages": reverse_pages_fetched,
                },
                "persist_counters": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "pagination_state": pagination_state,
                "listing_progress": {
                    "page_index": pages_fetched,
                    "posts_seen": posts_fetched,
                    "posts_upserted": posts_upserted,
                    "stop_reason": stop_reason,
                    "partial": stop_reason not in {None, "completed"},
                },
                "bidirectional_listing": {
                    "reverse_started": bidirectional_reverse_started,
                    "reverse_pages_fetched": reverse_pages_fetched,
                    "reverse_posts_seen": reverse_posts_fetched,
                    "reverse_posts_upserted": reverse_posts_upserted,
                    "reverse_stop_reason": reverse_stop_reason,
                    "reverse_error": bidirectional_reverse_error,
                },
                "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
                "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
                "runtime_metadata": exc.runtime_metadata,
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code="instagram_posts_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
        )
        terminal_status = "cancelled"
        terminal_error_message = str(exc)
    except PostsAuthCooldownActive as exc:
        # A4 soft-stop: an account-scoped auth cooldown is active. Requeue the job
        # with available_at = cooldown_until (mirrors the frontier cooldown
        # pattern) so a sibling container picks it up after the cooldown rather
        # than burning attempts now. This is NOT a hard failure.
        cooldown_available_at = exc.cooldown_until if isinstance(exc.cooldown_until, datetime) else None
        lifecycle.finish_job(
            job_id,
            status="retrying",
            items_found=posts_fetched + reverse_posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "error_code": exc.error_code,
                "auth_cooldown_active": True,
                "auth_block_kind": exc.blocker_kind,
                "available_at": cooldown_available_at.isoformat() if cooldown_available_at else None,
                "activity": {
                    "phase": "auth_cooldown_deferred",
                    "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
                },
                "persist_counters": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "pagination_state": pagination_state,
                "listing_progress": {
                    "page_index": pages_fetched,
                    "posts_seen": posts_fetched,
                    "posts_upserted": posts_upserted,
                    "stop_reason": "auth_cooldown_active",
                    "partial": True,
                },
                "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
                "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
                "runtime_metadata": exc.runtime_metadata,
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code=exc.error_code,
            last_error_class=exc.__class__.__name__,
            next_available_at=cooldown_available_at,
        )
        terminal_status = "retrying"
        terminal_error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "instagram_posts_scrapling_failed"
        error_class = str(getattr(exc, "error_class", "") or exc.__class__.__name__).strip()
        retryable = bool(getattr(exc, "retryable", False))
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = retryable and attempt_count < max_attempts
        next_available_at = (
            lifecycle.now_utc() + timedelta(seconds=lifecycle.retry_backoff_seconds(attempt_count))
            if can_retry
            else None
        )
        lifecycle.finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=posts_fetched + reverse_posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "persist_counters": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "pagination_state": pagination_state,
                "listing_progress": {
                    "page_index": pages_fetched,
                    "posts_seen": posts_fetched,
                    "posts_upserted": posts_upserted,
                    "stop_reason": stop_reason or error_code,
                    "partial": True,
                },
                "bidirectional_listing": {
                    "reverse_started": bidirectional_reverse_started,
                    "reverse_pages_fetched": reverse_pages_fetched,
                    "reverse_posts_seen": reverse_posts_fetched,
                    "reverse_posts_upserted": reverse_posts_upserted,
                    "reverse_stop_reason": reverse_stop_reason,
                    "reverse_error": bidirectional_reverse_error,
                },
                "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
                "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
                "runtime_metadata": getattr(exc, "runtime_metadata", None),
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
        )
        terminal_status = "retrying" if can_retry else "failed"
        terminal_error_message = str(exc)
    finally:
        if run_id:
            try:
                lifecycle.finalize_run_status(run_id)
            except pg.DatabaseServiceUnavailableError as exc:
                logger.warning(
                    "Deferred final posts run-status reconciliation after database saturation: run_id=%s error=%s",
                    run_id,
                    exc,
                )

    try:
        return (
            pg.fetch_one(
                """
                select
                  id::text,
                  run_id::text as run_id,
                  platform,
                  job_type,
                  status,
                  items_found,
                  error_message,
                  metadata
                from social.scrape_jobs
                where id = %s
                """,
                [job_id],
            )
            or {}
        )
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Returning degraded posts job summary after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return {
            "id": job_id,
            "run_id": run_id or None,
            "platform": "instagram",
            "job_type": str(job.get("job_type") or "posts").strip() or "posts",
            "status": terminal_status or "unknown",
            "items_found": posts_fetched + reverse_posts_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
            },
        }
