"""Hybrid fetcher for the Instagram posts Scrapling lane.

Architecture:
  warmup()      →  _fetch_page()          →  StealthyFetcher (Patchright browser)
  posts page    →  _fetch_json_response() →  httpx.AsyncClient (GraphQL POST)

The browser handles session establishment, challenge solving, and extraction
of runtime tokens (LSD, bloks_version, spin_r/b/t, hsi) from the profile HTML.
All subsequent GraphQL POSTs go through httpx with the cookies and tokens
bridged from warmup.

This module is self-contained: header construction, form data shape, and
token extraction live here — no imports from ``scraper.py``.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from trr_backend.socials._scrapling_http_utils import (
    env_truthy as _env_truthy,
)
from trr_backend.socials._scrapling_http_utils import (
    extract_response_cookies as _extract_response_cookies,
)
from trr_backend.socials._scrapling_http_utils import (
    resolve_positive_float_env as _resolve_positive_float_env,
)
from trr_backend.socials._scrapling_http_utils import (
    response_text as _response_text,
)
from trr_backend.socials._scrapling_http_utils import (
    safe_location as _safe_location,
)
from trr_backend.socials._scrapling_http_utils import (
    status_code as _status_code,
)
from trr_backend.socials._scrapling_http_utils import (
    transient_backoff_seconds as _transient_backoff_seconds,
)
from trr_backend.socials._scrapling_http_utils import (
    transport_failure_reason as _transport_failure_reason,
)
from trr_backend.socials.instagram.constants import (
    GRAPHQL_URL,
    PROFILE_POSTS_DOC_IDS,
    PROFILE_POSTS_FAST_PAGE_SIZE,
    PROFILE_POSTS_PAGE_SIZE,
    WEB_X_ASBD_ID,
)
from trr_backend.socials.instagram.posts_scrapling.proxy import (
    PostsProxyConfig,
    build_posts_proxy_identity,
    posts_proxy_feature_flags,
)

logger = logging.getLogger("socials.instagram.posts_scrapling.fetcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_IG_APP_ID = "936619743392459"
_FRIENDLY_NAME = "PolarisProfilePostsTabContentQuery_connection"
_POSTS_REQUEST_DELAY_DEFAULT = 0.15
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# Regex patterns for extracting runtime tokens from profile HTML.
_LSD_RE = re.compile(r'"LSD",\[\],\{"token":"(?P<token>[^"]+)"\}')
_BLOKS_RE = re.compile(r"bloks_version[^0-9a-fA-F]+(?P<token>[0-9a-fA-F]{32,})")
_SPIN_R_RE = re.compile(r'"__spin_r":(?P<token>\d+)')
_SPIN_B_RE = re.compile(r'"__spin_b":"(?P<token>[^"]+)"')
_SPIN_T_RE = re.compile(r'"__spin_t":(?P<token>\d+)')
_HSI_RE = re.compile(r'"hsi":"?(?P<token>\d+)"?')


@dataclass(slots=True)
class _WarmupPoolEntry:
    raw_cookies: dict[str, str]
    page_tokens: dict[str, str]
    created_at: float
    use_count: int = 0


_POSTS_WARMUP_POOL: dict[str, _WarmupPoolEntry] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge", "accounts/login"))


def _safe_rate_limit_key(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "instagram_posts"))


def _global_rate_limit_key(
    proxy_config: PostsProxyConfig | None,
    *,
    observed_identity: str | None = None,
    observed_fingerprint: str | None = None,
) -> str:
    identity = build_posts_proxy_identity(
        proxy_config,
        observed_identity=observed_identity,
        observed_fingerprint=observed_fingerprint,
    ).pacing_identity
    normalized = str(identity or "instagram:global").strip().lower() or "instagram:global"
    digest = hashlib.sha256(f"instagram-posts:{normalized}".encode()).hexdigest()[:24]
    return f"instagram-posts-{digest}"


def _global_rate_limit_path(key: str) -> str:
    directory = os.path.join(tempfile.gettempdir(), "trr-instagram-posts-rate")
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"{_safe_rate_limit_key(key)}.lock")


def _global_rate_cooldown_path(key: str) -> str:
    directory = os.path.join(tempfile.gettempdir(), "trr-instagram-posts-rate")
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"{_safe_rate_limit_key(key)}.cooldown")


def _read_monotonic_timestamp(handle: Any) -> float:
    handle.seek(0)
    raw_value = handle.read().strip()
    try:
        timestamp = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    now = time.monotonic()
    if timestamp > now + 3_600:
        return 0.0
    return max(0.0, timestamp)


def _record_global_api_cooldown(*, key: str, delay_seconds: float) -> None:
    delay = max(0.0, float(delay_seconds or 0))
    if delay <= 0:
        return
    cooldown_until = time.monotonic() + delay
    path = _global_rate_cooldown_path(key)
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            existing_until = _read_monotonic_timestamp(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(f"{max(existing_until, cooldown_until):.6f}")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


_RATE_LIMIT_ADVISORY_LOCK_NAMESPACE = 0x49_47_50_53  # "IGPS" - IG posts lane.
_RATE_LIMIT_MODE_ENV = "SOCIAL_INSTAGRAM_POSTS_GLOBAL_RATE_LIMIT_MODE"
_RATE_LIMIT_MODE_DEFAULT = "advisory"
_RATE_LIMIT_VALID_MODES = frozenset({"advisory", "file_lock"})


def _resolve_rate_limit_mode(raw_value: str | None = None) -> str:
    raw = raw_value if raw_value is not None else os.getenv(_RATE_LIMIT_MODE_ENV)
    value = str(raw or _RATE_LIMIT_MODE_DEFAULT).strip().lower()
    return value if value in _RATE_LIMIT_VALID_MODES else _RATE_LIMIT_MODE_DEFAULT


def _advisory_lock_keys_for(key: str) -> tuple[int, int]:
    digest = hashlib.sha256(str(key or "instagram-posts").encode("utf-8")).digest()
    key_int = int.from_bytes(digest[:4], byteorder="big", signed=True)
    return _RATE_LIMIT_ADVISORY_LOCK_NAMESPACE, key_int


def _try_advisory_lock_pace(*, key: str, delay_seconds: float) -> dict[str, Any]:
    delay = max(0.0, float(delay_seconds or 0))
    namespace, lock_key = _advisory_lock_keys_for(key)
    started_at = time.monotonic()
    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        return {"acquired": False, "paced": True, "wait_ms": 0, "error": f"pg_import_failed:{exc}"}
    try:
        with pg.db_connection(label="instagram-posts-rate-limit-advisory") as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute("select pg_advisory_lock(%s::int, %s::int)", (namespace, lock_key))
                wait_ms = int((time.monotonic() - started_at) * 1000)
                try:
                    if delay > 0:
                        time.sleep(delay)
                    return {"acquired": True, "paced": True, "wait_ms": wait_ms, "error": None}
                finally:
                    cur.execute("select pg_advisory_unlock(%s::int, %s::int)", (namespace, lock_key))
    except Exception as exc:  # noqa: BLE001
        return {"acquired": False, "paced": True, "wait_ms": 0, "error": str(exc)}


def _pace_global_api_request(*, key: str, delay_seconds: float) -> bool:
    delay = max(0.0, float(delay_seconds or 0))
    cooldown_path = _global_rate_cooldown_path(key)
    with open(cooldown_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            cooldown_until = _read_monotonic_timestamp(handle)
            remaining = cooldown_until - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    if delay <= 0:
        return True

    path = _global_rate_limit_path(key)
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            previous_started_at = _read_monotonic_timestamp(handle)
            remaining = (previous_started_at + delay) - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            handle.seek(0)
            handle.truncate()
            handle.write(f"{time.monotonic():.6f}")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return True


def _feature_flags_metadata() -> dict[str, bool]:
    proxy_flags = posts_proxy_feature_flags()
    return {
        "bidirectional_walk_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", False),
        "per_ip_pacing_enabled": bool(proxy_flags["per_ip_pacing_enabled"]),
        "page_proxy_rotation_enabled": bool(proxy_flags["page_proxy_rotation_enabled"]),
        "shared_warmup_enabled": _env_truthy("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", False),
    }


def _positive_int_env(name: str, default: int, *, minimum: int = 1, maximum: int = 10_000) -> int:
    try:
        value = int(str(os.getenv(name) or "").strip() or default)
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _warmup_pool_enabled() -> bool:
    return _env_truthy("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", False)


def _warmup_pool_ttl_seconds() -> float:
    return _resolve_positive_float_env(
        "SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_TTL_SECONDS",
        15 * 60.0,
        minimum=30.0,
        maximum=6 * 60 * 60.0,
    )


def _warmup_pool_max_uses() -> int:
    return _positive_int_env("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_MAX_USES", 8, minimum=1, maximum=50)


def _cookie_identity_hash(raw_cookies: dict[str, str]) -> str:
    material = "|".join(
        f"{name}={raw_cookies.get(name) or ''}"
        for name in ("sessionid", "ds_user_id", "csrftoken", "mid")
        if raw_cookies.get(name)
    )
    if not material:
        return "no-cookie-identity"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _warmup_pool_key(
    *,
    browser_account_id: str | None,
    proxy_fingerprint: str,
    raw_cookies: dict[str, str],
) -> str:
    account = str(browser_account_id or "").strip().lower().lstrip("@") or "unknown"
    return f"{account}:{proxy_fingerprint or 'none'}:{_cookie_identity_hash(raw_cookies)}"


def _post_identity(post: dict[str, Any]) -> str | None:
    for key in ("id", "pk", "media_id", "code", "shortcode"):
        value = str(post.get(key) or "").strip()
        if value:
            return value
    return None


def _observed_proxy_identity_from_response(response: Any) -> tuple[str | None, str | None]:
    headers = getattr(response, "headers", None) or {}
    observed_identity: str | None = None
    for header_name in (
        "x-trr-proxy-ip",
        "x-proxy-ip",
        "x-upstream-proxy-ip",
        "x-decodo-proxy-ip",
        "x-forwarded-for",
    ):
        try:
            raw_value = headers.get(header_name) if hasattr(headers, "get") else None
        except Exception:  # noqa: BLE001
            raw_value = None
        value = str(raw_value or "").split(",")[0].strip()
        if value:
            observed_identity = value
            break
    if not observed_identity:
        return None, None
    return observed_identity, hashlib.sha256(observed_identity.encode()).hexdigest()[:16]


@dataclass(slots=True)
class InstagramPostsBidirectionalProbeResult:
    enabled: bool
    passed: bool
    reason: str
    request_shape: dict[str, Any] = field(default_factory=dict)
    response_order: list[str] = field(default_factory=list)
    overlap_count: int = 0
    cursor_fields: dict[str, Any] = field(default_factory=dict)

    def to_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "passed": self.passed,
            "reason": self.reason,
            "request_shape": dict(self.request_shape),
            "response_order": list(self.response_order),
            "overlap_count": self.overlap_count,
            "cursor_fields": dict(self.cursor_fields),
        }


def build_bidirectional_probe_metadata(
    *,
    request_shape: dict[str, Any],
    forward_posts: list[dict[str, Any]],
    reverse_posts: list[dict[str, Any]],
    cursor_fields: dict[str, Any] | None = None,
    enabled: bool | None = None,
    failure_reason: str | None = None,
) -> InstagramPostsBidirectionalProbeResult:
    """Build disabled-by-default reverse-walk probe metadata.

    This does not enable reverse walkers. It gives tests and future live probes
    one redaction-safe shape for request/response evidence before the risky
    path is wired into job execution.
    """
    flag_enabled = (
        _env_truthy("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", False) if enabled is None else bool(enabled)
    )
    forward_ids = [_post_identity(post) for post in forward_posts if isinstance(post, dict)]
    reverse_ids = [_post_identity(post) for post in reverse_posts if isinstance(post, dict)]
    forward_clean = [value for value in forward_ids if value]
    reverse_clean = [value for value in reverse_ids if value]
    overlap_count = len(set(forward_clean) & set(reverse_clean))
    response_order = reverse_clean
    reason = str(failure_reason or "").strip()
    if not flag_enabled:
        reason = reason or "bidirectional_walk_disabled"
        passed = False
    elif reason:
        passed = False
    elif not reverse_clean:
        reason = "reverse_probe_empty"
        passed = False
    elif overlap_count >= len(reverse_clean):
        reason = "reverse_probe_duplicate_forward_page"
        passed = False
    else:
        reason = "reverse_probe_passed"
        passed = True
    return InstagramPostsBidirectionalProbeResult(
        enabled=flag_enabled,
        passed=passed,
        reason=reason,
        request_shape=dict(request_shape or {}),
        response_order=response_order,
        overlap_count=overlap_count,
        cursor_fields=dict(cursor_fields or {}),
    )


class InstagramPostsWarmupError(RuntimeError):
    error_code: str
    retryable: bool

    def __init__(self, message: str, *, error_code: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable


def _extract_page_tokens(html: str) -> dict[str, str]:
    """Extract LSD / bloks_version / spin_r / spin_b / spin_t / hsi from profile HTML.

    Returns keys matching the form-data field names used by Instagram's web
    client: ``lsd``, ``bloks_version``, ``__spin_r``, ``__spin_b``, ``__spin_t``,
    ``hsi``. Missing tokens are simply absent from the result.
    """
    tokens: dict[str, str] = {}
    if not html:
        return tokens

    lsd_match = _LSD_RE.search(html)
    if lsd_match:
        token = str(lsd_match.group("token") or "").strip()
        if token:
            tokens["lsd"] = token

    bloks_match = _BLOKS_RE.search(html)
    if bloks_match:
        token = str(bloks_match.group("token") or "").strip()
        if token:
            tokens["bloks_version"] = token

    spin_r_match = _SPIN_R_RE.search(html)
    if spin_r_match:
        token = str(spin_r_match.group("token") or "").strip()
        if token:
            tokens["__spin_r"] = token

    spin_b_match = _SPIN_B_RE.search(html)
    if spin_b_match:
        token = str(spin_b_match.group("token") or "").strip()
        if token:
            tokens["__spin_b"] = token

    spin_t_match = _SPIN_T_RE.search(html)
    if spin_t_match:
        token = str(spin_t_match.group("token") or "").strip()
        if token:
            tokens["__spin_t"] = token

    hsi_match = _HSI_RE.search(html)
    if hsi_match:
        token = str(hsi_match.group("token") or "").strip()
        if token:
            tokens["hsi"] = token

    return tokens


def _build_nav_headers(referer: str) -> dict[str, str]:
    """Stripped-down navigation headers for full-page browser fetches.

    Used by warmup only. Excludes the XHR/GraphQL-specific markers that would
    not appear on a real document navigation request.
    """
    return {
        "accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
        "user-agent": _USER_AGENT,
    }


def _build_graphql_headers(
    *,
    referer: str,
    csrftoken: str,
    lsd_token: str | None = None,
    bloks_version: str | None = None,
) -> dict[str, str]:
    """Build headers for the GraphQL POST.

    Self-contained — does not import from scraper.py. Mirrors the header set
    the Instagram web client sends for the PolarisProfilePostsTabContentQuery.
    """
    headers: dict[str, str] = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.instagram.com",
        "referer": referer,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": _USER_AGENT,
        "x-asbd-id": str(os.getenv("INSTAGRAM_WEB_X_ASBD_ID") or WEB_X_ASBD_ID),
        "x-fb-friendly-name": _FRIENDLY_NAME,
        "x-ig-app-id": _IG_APP_ID,
        "x-requested-with": "XMLHttpRequest",
    }
    csrf = str(csrftoken or "").strip()
    if csrf:
        headers["x-csrftoken"] = csrf
    lsd = str(lsd_token or "").strip()
    if lsd:
        headers["x-fb-lsd"] = lsd
    bloks = str(os.getenv("INSTAGRAM_WEB_BLOKS_VERSION_ID") or bloks_version or "").strip()
    if bloks:
        headers["x-bloks-version-id"] = bloks
    return headers


def _build_graphql_form_data(
    *,
    username: str,
    cursor: str | None,
    page_size: int,
    viewer_id: str,
    page_tokens: dict[str, str],
    doc_id: str,
    direction: str = "forward",
) -> dict[str, str]:
    """Build the x-www-form-urlencoded payload for the GraphQL posts POST.

    Shape mirrors what the Instagram web client sends for the
    PolarisProfilePostsTabContentQuery_connection operation. Runtime tokens
    (``lsd``, ``__spin_r``, ``__spin_b``, ``__spin_t``, ``hsi``) are merged in
    only when present.
    """
    reverse = str(direction or "").strip().lower() == "reverse"
    variables = {
        "after": None if reverse else cursor,
        "before": cursor if reverse else None,
        "data": {
            "count": page_size,
            "include_reel_media_seen_timestamp": True,
            "include_relationship_info": True,
            "latest_besties_reel_media": True,
            "latest_reel_media": True,
        },
        "first": None if reverse else page_size,
        "last": page_size if reverse else None,
        "username": username,
    }
    data: dict[str, str] = {
        "av": viewer_id,
        "__d": "www",
        "__user": viewer_id,
        "__a": "1",
        "__req": "1",
        "__comet_req": "7",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": _FRIENDLY_NAME,
        "variables": json.dumps(variables),
        "server_timestamps": "true",
        "doc_id": doc_id,
    }
    for key in ("lsd", "__spin_r", "__spin_b", "__spin_t", "hsi"):
        value = str(page_tokens.get(key) or "").strip()
        if value:
            data[key] = value
    return data


def _requests_fallback_enabled() -> bool:
    raw = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_REQUESTS_FALLBACK_ENABLED") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not bool(os.getenv("PYTEST_CURRENT_TEST"))


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class InstagramPostsFetchResult:
    posts: list[dict[str, Any]] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    fetch_reason: str | None = None
    request_count: int = 0
    retryable: bool = False
    has_next_page: bool = False
    end_cursor: str | None = None
    has_previous_page: bool = False
    start_cursor: str | None = None
    direction: str = "forward"


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class InstagramPostsScraplingFetcher:
    """Hybrid fetcher: Patchright for warmup + token extraction, httpx for GraphQL."""

    # Retry policy for transient errors (429 / 5xx / transport timeout).
    _MAX_TRANSIENT_RETRIES: int = 3
    _BASE_BACKOFF_SECONDS: float = 1.0

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        browser_account_id: str | None,
        proxy_config: PostsProxyConfig | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
        page_size: int | None = None,
        fast_mode: bool = False,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = raw_cookies if isinstance(raw_cookies, dict) else dict(raw_cookies or {})
        self._browser_account_id = str(browser_account_id or "").strip() or None
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_INSTAGRAM_POSTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._fast_mode = bool(fast_mode)
        resolved_page_size = int(
            page_size
            if page_size is not None
            else (PROFILE_POSTS_FAST_PAGE_SIZE if self._fast_mode else PROFILE_POSTS_PAGE_SIZE)
        )
        self._page_size = max(1, resolved_page_size)
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint: str = proxy_config.fingerprint if proxy_config else "none"
        self._proxy_session_mode: str = proxy_config.session_mode if proxy_config else "none"
        self._page_tokens: dict[str, str] = {}
        self._retry_reason_counts: dict[str, int] = {}
        self._consecutive_auth_failures = 0
        self._warmup_pool_key = _warmup_pool_key(
            browser_account_id=self._browser_account_id,
            proxy_fingerprint=self._selected_proxy_fingerprint,
            raw_cookies=self._raw_cookies,
        )
        self._warmup_pool_metadata: dict[str, Any] = {
            "enabled": _warmup_pool_enabled(),
            "hit": False,
            "miss": False,
            "age_seconds": None,
            "refresh_reason": None,
            "key_fingerprint": self._warmup_pool_key,
        }
        self._proxy_request_attempts: dict[str, int] = {}
        self._proxy_status_counts: dict[str, dict[str, int]] = {}
        self._proxy_auth_failures: dict[str, int] = {}
        self._proxy_rate_limit_failures: dict[str, int] = {}
        self._proxy_rotation_events: list[dict[str, Any]] = []
        self._observed_proxy_identity: str | None = None
        self._observed_proxy_fingerprint: str | None = None
        # Phase 4.2: doc-ID rotation observability — record which doc IDs were
        # tried this run and which one ultimately succeeded. Operators can
        # cross-reference http_400 / non_json_response spikes with rotation
        # events and decide when to update SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS.
        self._doc_ids_configured: tuple[str, ...] = tuple(PROFILE_POSTS_DOC_IDS)
        self._doc_ids_attempted: list[str] = []
        self._doc_id_used: str | None = None
        self._doc_id_attempt_counts: dict[str, int] = {}
        self._doc_id_success_counts: dict[str, int] = {}
        self._doc_id_empty_counts: dict[str, int] = {}
        self._doc_id_4xx_counts: dict[str, int] = {}
        self._pagination_doc_id_stale_count = 0
        self._last_doc_id_failure_reason: str | None = None
        self._api_delay_seconds = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_DELAY_SEC",
            _POSTS_REQUEST_DELAY_DEFAULT,
            minimum=0.0,
            maximum=30.0,
        )
        self._global_rate_limit_key = _global_rate_limit_key(self._proxy_config)
        self._global_rate_limit_mode_configured = _resolve_rate_limit_mode()
        self._global_rate_limit_mode_last: str | None = None
        self._global_rate_limit_advisory_attempts = 0
        self._global_rate_limit_advisory_acquires = 0
        self._global_rate_limit_advisory_fallback_count = 0
        self._global_rate_limit_advisory_total_wait_ms = 0
        self._global_rate_limit_advisory_last_error: str | None = None
        self._last_api_request_started_at = 0.0
        self._bidirectional_probe_metadata = build_bidirectional_probe_metadata(
            request_shape={},
            forward_posts=[],
            reverse_posts=[],
        ).to_metadata()
        self._requests_fallback_active = False
        self._requests_fallback_reason: str | None = None
        self._requests_fallback_metadata: dict[str, Any] = {}
        self._requests_fallback_scraper: Any | None = None

        # Browser fetcher (for warmup only).
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc
        self._fetcher = StealthyFetcher()

        # httpx client (for GraphQL POSTs). Created lazily after warmup bridges cookies.
        self._http_client: httpx.AsyncClient | None = None

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        """Postmortem data for job metadata. The only way job_runner should
        read internal fetcher state."""
        proxy_identity = build_posts_proxy_identity(
            self._proxy_config,
            observed_identity=self._observed_proxy_identity,
            observed_fingerprint=self._observed_proxy_fingerprint,
        )
        return {
            "warmup_cookie_names": sorted(self._warmup_cookie_delta.keys()),
            "warmup_cookie_count": len(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "proxy_session_mode": self._proxy_session_mode,
            "proxy_identity": proxy_identity.to_metadata(),
            "proxy_pacing": {
                "enabled": bool(_feature_flags_metadata()["per_ip_pacing_enabled"]),
                "identity": proxy_identity.to_metadata(),
                "global_rate_limit_key": self._global_rate_limit_key,
                "mode_configured": self._global_rate_limit_mode_configured,
                "mode_last_used": self._global_rate_limit_mode_last,
                "advisory_attempts": self._global_rate_limit_advisory_attempts,
                "advisory_acquires": self._global_rate_limit_advisory_acquires,
                "advisory_fallback_count": self._global_rate_limit_advisory_fallback_count,
                "advisory_total_wait_ms": self._global_rate_limit_advisory_total_wait_ms,
                "advisory_last_error": self._global_rate_limit_advisory_last_error,
                "attempts": dict(sorted(self._proxy_request_attempts.items())),
                "status_counts": {
                    key: dict(sorted(value.items())) for key, value in sorted(self._proxy_status_counts.items())
                },
                "auth_failures": dict(sorted(self._proxy_auth_failures.items())),
                "rate_limit_failures": dict(sorted(self._proxy_rate_limit_failures.items())),
                "rotation_events": list(self._proxy_rotation_events[-20:]),
            },
            "page_tokens_found": list(self._page_tokens.keys()),
            "api_delay_seconds": self._api_delay_seconds,
            "request_count": self._request_count,
            "transport": "httpx_after_browser_warmup",
            "requests_fallback": {
                "active": bool(self._requests_fallback_active),
                "reason": self._requests_fallback_reason,
                **dict(self._requests_fallback_metadata),
            },
            "retry_reason_counts": dict(sorted(self._retry_reason_counts.items())),
            "feature_flags": _feature_flags_metadata(),
            "bidirectional_probe": dict(self._bidirectional_probe_metadata),
            "warmup_pool": dict(self._warmup_pool_metadata),
            "consecutive_auth_failures": self._consecutive_auth_failures,
            # Phase 4.2: doc-ID rotation telemetry.
            "profile_posts_doc_ids": {
                "configured": list(self._doc_ids_configured),
                "attempted": list(self._doc_ids_attempted),
                "used": self._doc_id_used,
                "final_selected": self._doc_id_used,
                "attempts": dict(sorted(self._doc_id_attempt_counts.items())),
                "successes": dict(sorted(self._doc_id_success_counts.items())),
                "empty_connection_count": dict(sorted(self._doc_id_empty_counts.items())),
                "http_4xx_count": dict(sorted(self._doc_id_4xx_counts.items())),
                "pagination_doc_id_stale_count": self._pagination_doc_id_stale_count,
                "last_failure_reason": self._last_doc_id_failure_reason,
            },
        }

    async def warmup(self, username: str) -> None:
        """Navigate to the profile page via Patchright to establish the session,
        extract runtime tokens from the HTML, and bridge cookies into the
        httpx client."""
        if await self._try_apply_warmup_pool_entry():
            return
        profile_url = f"https://www.instagram.com/{username}/"
        response = await self._fetch_page(profile_url, referer=profile_url)
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            if self._activate_requests_fallback("warmup_auth_failed"):
                return
            self._consecutive_auth_failures += 1
            self._warmup_pool_metadata.update({"miss": True, "refresh_reason": "auth_failure"})
            raise InstagramPostsWarmupError(
                "Instagram posts warmup failed because the session appears logged out or challenged.",
                error_code="instagram_posts_warmup_auth_failed",
                retryable=False,
            )
        self._page_tokens = _extract_page_tokens(text)
        self._merge_warmup_cookies(response)
        if not self._warmup_cookie_delta and not str(self._raw_cookies.get("sessionid") or "").strip():
            self._consecutive_auth_failures += 1
            self._warmup_pool_metadata.update({"miss": True, "refresh_reason": "no_cookies"})
            raise InstagramPostsWarmupError(
                "Instagram posts warmup did not bridge cookies and no prior sessionid exists.",
                error_code="instagram_posts_warmup_no_cookies",
                retryable=True,
            )
        await self._rebuild_http_client()
        self._consecutive_auth_failures = 0
        self._store_warmup_pool_entry()
        logger.info(
            "instagram_posts_scrapling warmup_success",
            extra={
                "event": "warmup_success",
                "account": username,
                "cookie_count": len(self._warmup_cookie_delta),
                "page_tokens_count": len(self._page_tokens),
                "proxy_fingerprint": self._selected_proxy_fingerprint,
            },
        )

    def warmup_snapshot(self) -> dict[str, Any]:
        """Return enough warmup state for another posts fetcher in the same shard."""
        return {
            "raw_cookies": dict(self._raw_cookies),
            "page_tokens": dict(self._page_tokens),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
        }

    async def apply_warmup_snapshot(self, snapshot: dict[str, Any]) -> None:
        raw_cookies = snapshot.get("raw_cookies")
        page_tokens = snapshot.get("page_tokens")
        if isinstance(raw_cookies, dict):
            self._raw_cookies.update({str(key): str(value) for key, value in raw_cookies.items()})
        if isinstance(page_tokens, dict):
            self._page_tokens = {str(key): str(value) for key, value in page_tokens.items() if str(value)}
        self._warmup_pool_metadata.update(
            {
                "enabled": _warmup_pool_enabled(),
                "hit": True,
                "miss": False,
                "refresh_reason": "snapshot_reuse",
            }
        )
        await self._rebuild_http_client()

    async def _try_apply_warmup_pool_entry(self) -> bool:
        if not _warmup_pool_enabled():
            self._warmup_pool_metadata.update({"enabled": False, "hit": False, "miss": False})
            return False
        entry = _POSTS_WARMUP_POOL.get(self._warmup_pool_key)
        if entry is None:
            self._warmup_pool_metadata.update({"enabled": True, "hit": False, "miss": True, "refresh_reason": "miss"})
            return False
        age_seconds = max(0.0, time.monotonic() - entry.created_at)
        if age_seconds > _warmup_pool_ttl_seconds():
            _POSTS_WARMUP_POOL.pop(self._warmup_pool_key, None)
            self._warmup_pool_metadata.update(
                {
                    "enabled": True,
                    "hit": False,
                    "miss": True,
                    "age_seconds": round(age_seconds, 3),
                    "refresh_reason": "ttl_expired",
                }
            )
            return False
        if entry.use_count >= _warmup_pool_max_uses():
            _POSTS_WARMUP_POOL.pop(self._warmup_pool_key, None)
            self._warmup_pool_metadata.update(
                {
                    "enabled": True,
                    "hit": False,
                    "miss": True,
                    "age_seconds": round(age_seconds, 3),
                    "refresh_reason": "max_uses",
                }
            )
            return False
        if self._consecutive_auth_failures >= 3:
            _POSTS_WARMUP_POOL.pop(self._warmup_pool_key, None)
            self._warmup_pool_metadata.update(
                {
                    "enabled": True,
                    "hit": False,
                    "miss": True,
                    "age_seconds": round(age_seconds, 3),
                    "refresh_reason": "auth_failure_threshold",
                }
            )
            return False

        entry.use_count += 1
        self._raw_cookies.update(entry.raw_cookies)
        self._page_tokens = dict(entry.page_tokens)
        self._warmup_cookie_delta = {}
        await self._rebuild_http_client()
        self._warmup_pool_metadata.update(
            {
                "enabled": True,
                "hit": True,
                "miss": False,
                "age_seconds": round(age_seconds, 3),
                "refresh_reason": None,
                "use_count": entry.use_count,
            }
        )
        return True

    def _store_warmup_pool_entry(self) -> None:
        if not _warmup_pool_enabled():
            return
        _POSTS_WARMUP_POOL[self._warmup_pool_key] = _WarmupPoolEntry(
            raw_cookies=dict(self._raw_cookies),
            page_tokens=dict(self._page_tokens),
            created_at=time.monotonic(),
            use_count=1,
        )
        self._warmup_pool_metadata.update(
            {
                "enabled": True,
                "hit": False,
                "miss": True,
                "age_seconds": 0.0,
                "refresh_reason": "stored",
                "use_count": 1,
            }
        )

    def _activate_requests_fallback(self, reason: str) -> bool:
        if not _requests_fallback_enabled():
            return False
        if not str(self._raw_cookies.get("sessionid") or "").strip():
            return False
        try:
            from trr_backend.socials.instagram.scraper import InstagramScraper
        except Exception as exc:  # noqa: BLE001
            self._requests_fallback_metadata = {
                "available": False,
                "error_class": type(exc).__name__,
            }
            return False
        self._requests_fallback_scraper = InstagramScraper(
            cookies=dict(self._raw_cookies),
            browser_account_id=self._browser_account_id,
        )
        self._requests_fallback_active = True
        self._requests_fallback_reason = reason
        self._requests_fallback_metadata = {
            "available": True,
            "transport": "requests_enriched",
        }
        self._warmup_pool_metadata.update(
            {
                "enabled": _warmup_pool_enabled(),
                "hit": False,
                "miss": True,
                "refresh_reason": f"requests_fallback:{reason}",
            }
        )
        return True

    def _fetch_posts_page_via_requests(
        self,
        username: str,
        *,
        cursor: str | None,
        direction: str,
    ) -> InstagramPostsFetchResult:
        if direction != "forward":
            return InstagramPostsFetchResult(
                posts=[],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="requests_fallback_reverse_unsupported",
                request_count=self._request_count,
                retryable=False,
                direction=direction,
            )
        scraper = self._requests_fallback_scraper
        if scraper is None:
            self._activate_requests_fallback("fetch_without_warmup")
            scraper = self._requests_fallback_scraper
        if scraper is None:
            return InstagramPostsFetchResult(
                posts=[],
                fetch_failed=True,
                auth_failed=True,
                fetch_reason="requests_fallback_unavailable",
                request_count=self._request_count,
                retryable=False,
                direction=direction,
            )

        payload = scraper.fetch_posts_graphql(
            username,
            cursor=cursor,
            delay=self._api_delay_seconds,
            fast_mode=self._fast_mode,
            allow_browser_fallback=False,
            page_size=self._page_size,
        )
        self._request_count += 1
        metadata = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
        self._requests_fallback_metadata.update(
            {
                "transport": metadata.get("retrieval_transport") or metadata.get("transport") or "requests_enriched",
                "last_error_code": metadata.get("error_code") or metadata.get("request_error_code"),
                "graphql_cursor": metadata.get("graphql_cursor"),
            }
        )
        connection = ((payload or {}).get("data") or {}).get(
            "xdt_api__v1__feed__user_timeline_graphql_connection",
            {},
        )
        if not isinstance(connection, dict) or not connection:
            reason = str(
                metadata.get("error_code") or metadata.get("request_error_code") or "requests_fallback_no_connection"
            ).strip()
            auth_failed = reason in {
                "forbidden",
                "unauthorized",
                "redirect_login",
                "checkpoint_required",
                "challenge_required",
                "login_required",
                "instagram_graphql_initial_unauthorized",
                "instagram_graphql_initial_forbidden",
            }
            return InstagramPostsFetchResult(
                posts=[],
                fetch_failed=True,
                auth_failed=auth_failed,
                fetch_reason=reason,
                request_count=self._request_count,
                retryable=bool(metadata.get("retryable") or metadata.get("request_error_retryable")),
                direction=direction,
            )

        edges = connection.get("edges") or []
        posts = [
            node
            for edge in edges
            if isinstance(edge, dict)
            for node in [edge.get("node")]
            if isinstance(node, dict) and node
        ]
        page_info = connection.get("page_info") if isinstance(connection.get("page_info"), dict) else {}
        start_cursor = str(page_info.get("start_cursor")) if page_info.get("start_cursor") else None
        end_cursor = str(page_info.get("end_cursor")) if page_info.get("end_cursor") else None
        return InstagramPostsFetchResult(
            posts=posts,
            fetch_failed=False,
            auth_failed=False,
            fetch_reason=None,
            request_count=self._request_count,
            retryable=False,
            has_next_page=bool(page_info.get("has_next_page", False)),
            end_cursor=end_cursor,
            has_previous_page=bool(page_info.get("has_previous_page", False)),
            start_cursor=start_cursor,
            direction=direction,
        )

    def _doc_ids_for_page(self) -> tuple[str, ...]:
        selected = str(self._doc_id_used or "").strip()
        if not selected:
            return self._doc_ids_configured
        ordered = [selected]
        ordered.extend(doc_id for doc_id in self._doc_ids_configured if doc_id != selected)
        return tuple(ordered)

    def _record_doc_id_attempt(self, doc_id: str) -> None:
        if doc_id not in self._doc_ids_attempted:
            self._doc_ids_attempted.append(doc_id)
        self._doc_id_attempt_counts[doc_id] = self._doc_id_attempt_counts.get(doc_id, 0) + 1

    def _record_doc_id_success(self, doc_id: str) -> None:
        self._doc_id_success_counts[doc_id] = self._doc_id_success_counts.get(doc_id, 0) + 1
        self._doc_id_used = doc_id
        self._last_doc_id_failure_reason = None

    def _record_doc_id_empty(self, doc_id: str, reason: str) -> None:
        self._doc_id_empty_counts[doc_id] = self._doc_id_empty_counts.get(doc_id, 0) + 1
        self._last_doc_id_failure_reason = reason

    def _record_doc_id_4xx(self, doc_id: str) -> None:
        self._doc_id_4xx_counts[doc_id] = self._doc_id_4xx_counts.get(doc_id, 0) + 1

    @staticmethod
    def _is_http_4xx_reason(reason: str | None) -> bool:
        normalized = str(reason or "").strip().lower()
        if not normalized.startswith("http_"):
            return False
        try:
            status_code = int(normalized.removeprefix("http_"))
        except ValueError:
            return False
        return 400 <= status_code < 500

    @classmethod
    def _is_doc_id_fallback_reason(cls, reason: str | None, *, auth_failed: bool) -> bool:
        if auth_failed:
            return False
        normalized = str(reason or "").strip().lower()
        return cls._is_http_4xx_reason(normalized) or normalized in {
            "non_json_response",
            "graphql_empty_connection",
            "pagination_doc_id_stale",
        }

    @staticmethod
    def _is_stale_empty_page(*, cursor: str | None, page_info: dict[str, Any]) -> bool:
        if cursor:
            return True
        if not isinstance(page_info, dict) or not page_info:
            return False
        return (
            bool(page_info.get("has_next_page"))
            or bool(page_info.get("has_previous_page"))
            or bool(page_info.get("end_cursor"))
            or bool(page_info.get("start_cursor"))
        )

    async def fetch_posts_page(
        self,
        username: str,
        *,
        cursor: str | None = None,
        direction: str = "forward",
    ) -> InstagramPostsFetchResult:
        """Fetch a single page of posts via GraphQL.

        Iterates through ``PROFILE_POSTS_DOC_IDS`` until one returns a populated
        ``xdt_api__v1__feed__user_timeline_graphql_connection``. Returns on
        first success; on full exhaustion returns a failure result carrying
        the last observed reason.
        """
        referer = f"https://www.instagram.com/{username}/"
        if self._requests_fallback_active:
            return await asyncio.to_thread(
                self._fetch_posts_page_via_requests,
                username,
                cursor=cursor,
                direction=direction,
            )
        viewer_id = str(self._raw_cookies.get("ds_user_id") or "0")
        csrftoken = str(self._raw_cookies.get("csrftoken") or "")
        headers = _build_graphql_headers(
            referer=referer,
            csrftoken=csrftoken,
            lsd_token=self._page_tokens.get("lsd"),
            bloks_version=self._page_tokens.get("bloks_version"),
        )

        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        stale_empty_seen = False

        normalized_direction = str(direction or "forward").strip().lower()
        if normalized_direction not in {"forward", "reverse"}:
            normalized_direction = "forward"

        for doc_id in self._doc_ids_for_page():
            self._record_doc_id_attempt(doc_id)
            data = _build_graphql_form_data(
                username=username,
                cursor=cursor,
                page_size=self._page_size,
                viewer_id=viewer_id,
                page_tokens=self._page_tokens,
                doc_id=doc_id,
                direction=normalized_direction,
            )
            response = await self._fetch_json_response(
                GRAPHQL_URL,
                referer=referer,
                data=data,
                headers=headers,
            )
            payload = response.get("payload")
            current_reason = response.get("reason")
            current_failed = bool(response.get("failed"))
            current_auth = bool(response.get("auth_failed"))
            current_retryable = bool(response.get("retryable"))

            if current_failed:
                auth_failed = auth_failed or current_auth
                retryable = retryable or current_retryable
                if self._is_http_4xx_reason(str(current_reason or "")):
                    self._record_doc_id_4xx(doc_id)
                if current_reason:
                    self._last_doc_id_failure_reason = str(current_reason)
                if current_reason and not fetch_reason:
                    fetch_reason = current_reason
                fallback_reason = self._is_doc_id_fallback_reason(current_reason, auth_failed=current_auth)
                if doc_id == self._doc_id_used and not fallback_reason:
                    break
                if current_auth or (cursor and not current_retryable):
                    if not fallback_reason:
                        # Auth failure or non-doc-id cursor failure — stop trying
                        # remaining doc_ids.
                        break
                continue

            if not isinstance(payload, dict):
                self._last_doc_id_failure_reason = str(current_reason or "non_json_response")
                if current_reason and not fetch_reason:
                    fetch_reason = current_reason
                continue

            connection = payload.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
            if not connection:
                self._record_doc_id_empty(doc_id, "graphql_empty_connection")
                if not fetch_reason:
                    fetch_reason = "graphql_empty_connection"
                logger.warning(
                    "Instagram GraphQL doc_id %s returned no connection data; trying fallback",
                    doc_id,
                )
                continue

            edges = connection.get("edges") or []
            page_info = connection.get("page_info") or {}
            posts: list[dict[str, Any]] = []
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                node = edge.get("node") or {}
                if isinstance(node, dict) and node:
                    posts.append(node)

            if not posts and self._is_stale_empty_page(cursor=cursor, page_info=page_info):
                stale_empty_seen = True
                self._pagination_doc_id_stale_count += 1
                self._record_doc_id_empty(doc_id, "pagination_doc_id_stale")
                fetch_reason = "pagination_doc_id_stale"
                logger.warning(
                    "Instagram GraphQL doc_id %s returned an empty page with pagination state; trying fallback",
                    doc_id,
                )
                continue

            # Record the doc_id that produced a valid profile-post connection
            # so later pages try it first instead of paying the fallback chain.
            self._record_doc_id_success(doc_id)
            start_cursor = str(page_info.get("start_cursor")) if page_info.get("start_cursor") else None
            end_cursor = str(page_info.get("end_cursor")) if page_info.get("end_cursor") else None
            return InstagramPostsFetchResult(
                posts=posts,
                fetch_failed=False,
                auth_failed=False,
                fetch_reason=None,
                request_count=self._request_count,
                retryable=False,
                has_next_page=(
                    bool(page_info.get("has_previous_page", False))
                    if normalized_direction == "reverse"
                    else bool(page_info.get("has_next_page", False))
                ),
                end_cursor=start_cursor if normalized_direction == "reverse" else end_cursor,
                has_previous_page=bool(page_info.get("has_previous_page", False)),
                start_cursor=start_cursor,
                direction=normalized_direction,
            )

        return InstagramPostsFetchResult(
            posts=[],
            fetch_failed=True,
            auth_failed=auth_failed,
            fetch_reason=(
                "pagination_doc_id_stale" if stale_empty_seen else fetch_reason or "graphql_no_doc_id_succeeded"
            ),
            request_count=self._request_count,
            retryable=retryable,
            has_next_page=False,
            end_cursor=None,
            direction=normalized_direction,
        )

    async def probe_bidirectional_walk(
        self,
        username: str,
        *,
        forward_posts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Empirically test whether IG honors reverse Relay pagination for posts.

        The feature stays disabled unless SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED
        is true. A passing probe does not rely on fabricated cursors: it asks for
        ``last=N`` with no ``before`` boundary and checks that the response is not
        just a duplicate of the forward first page.
        """
        enabled = _env_truthy("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", False)
        request_shape = {
            "variables": {
                "after": None,
                "before": None,
                "first": None,
                "last": self._page_size,
            },
            "doc_id": self._doc_id_used or (self._doc_ids_configured[0] if self._doc_ids_configured else None),
        }
        if not enabled:
            self._bidirectional_probe_metadata = build_bidirectional_probe_metadata(
                enabled=False,
                request_shape=request_shape,
                forward_posts=forward_posts,
                reverse_posts=[],
            ).to_metadata()
            return dict(self._bidirectional_probe_metadata)

        result = await self.fetch_posts_page(username, cursor=None, direction="reverse")
        cursor_fields = {
            "has_previous_page": bool(result.has_previous_page),
            "has_next_page": bool(result.has_next_page),
            "start_cursor_present": bool(result.start_cursor),
            "end_cursor_present": bool(result.end_cursor),
        }
        self._bidirectional_probe_metadata = build_bidirectional_probe_metadata(
            enabled=True,
            request_shape=request_shape,
            forward_posts=forward_posts,
            reverse_posts=result.posts,
            cursor_fields=cursor_fields,
            failure_reason=str(result.fetch_reason or "").strip() if result.fetch_failed else None,
        ).to_metadata()
        return dict(self._bidirectional_probe_metadata)

    async def aclose(self) -> None:
        """Close the httpx client. Called by job_runner in finally."""
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._http_client = None

    # -------------------------------------------------------------------
    # Cookie bridge
    # -------------------------------------------------------------------

    def _merge_warmup_cookies(self, response: Any) -> None:
        """Record warmup cookie delta and sync future request headers."""
        new_cookies = _extract_response_cookies(response)
        self._warmup_cookie_delta = dict(new_cookies)
        self._sync_response_cookies(response)

    def _sync_response_cookies(self, response: Any) -> None:
        """Mirror response cookies into the header-building state.

        The httpx client updates its own cookie jar automatically, but the
        GraphQL headers read from `self._raw_cookies`. Keep both aligned so a
        rotated `csrftoken` or `ds_user_id` is visible on the next request.
        """
        for name, value in _extract_response_cookies(response).items():
            self._raw_cookies[name] = value

    async def _rebuild_http_client(self) -> None:
        """Create or recreate the httpx client with current cookies and proxy."""
        existing_client = self._http_client
        self._http_client = None
        if existing_client is not None:
            try:
                await existing_client.aclose()
            except Exception:  # noqa: BLE001
                pass
        self._http_client = httpx.AsyncClient(
            cookies=dict(self._raw_cookies),
            timeout=httpx.Timeout(self._timeout_ms / 1000),
            proxy=self._api_proxy_url,
            follow_redirects=False,
            trust_env=False,
        )

    async def set_api_proxy_config(self, proxy_config: PostsProxyConfig | None, *, reason: str) -> None:
        """Switch the direct GraphQL proxy route without rerunning browser warmup."""
        current_url = self._api_proxy_url
        next_url = proxy_config.api_proxy_url if proxy_config else None
        if current_url == next_url:
            return
        previous_fingerprint = self._selected_proxy_fingerprint
        self._proxy_config = proxy_config
        self._api_proxy_url = next_url
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"
        self._proxy_session_mode = proxy_config.session_mode if proxy_config else "none"
        self._observed_proxy_identity = None
        self._observed_proxy_fingerprint = None
        self._global_rate_limit_key = _global_rate_limit_key(self._proxy_config)
        self._proxy_rotation_events.append(
            {
                "from": previous_fingerprint,
                "to": self._selected_proxy_fingerprint,
                "reason": str(reason or "proxy_change").strip() or "proxy_change",
                "rotation_index": proxy_config.rotation_index if proxy_config else None,
            }
        )
        await self._rebuild_http_client()

    # -------------------------------------------------------------------
    # Transport: browser (warmup only)
    # -------------------------------------------------------------------

    async def _fetch_page(
        self,
        url: str,
        *,
        referer: str,
    ) -> Any:
        """Full page navigation via Patchright. Used ONLY by warmup().
        Emits a document request with stripped-down nav headers so that the
        profile HTML comes back with the runtime tokens intact.
        """
        self._request_count += 1
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=_build_nav_headers(referer),
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    # -------------------------------------------------------------------
    # Transport: httpx (GraphQL POSTs)
    # -------------------------------------------------------------------

    async def _fetch_graphql(
        self,
        url: str,
        *,
        data: dict[str, str],
        headers: dict[str, str],
    ) -> httpx.Response:
        """Plain HTTP POST via httpx. Used for GraphQL posts-page calls."""
        if self._http_client is None:
            await self._rebuild_http_client()
        await self._pace_api_requests()
        self._request_count += 1
        response = await self._http_client.post(url, data=data, headers=headers)  # type: ignore[union-attr]
        self._record_proxy_response(response)
        self._sync_response_cookies(response)
        return response

    async def _pace_api_requests(self) -> None:
        if self._api_delay_seconds <= 0:
            return
        if self._global_rate_limit_mode_configured == "advisory":
            self._global_rate_limit_advisory_attempts += 1
            advisory_result = await asyncio.to_thread(
                _try_advisory_lock_pace,
                key=self._global_rate_limit_key,
                delay_seconds=self._api_delay_seconds,
            )
            self._global_rate_limit_advisory_total_wait_ms += int(advisory_result.get("wait_ms") or 0)
            if advisory_result.get("acquired"):
                self._global_rate_limit_advisory_acquires += 1
                self._global_rate_limit_mode_last = "advisory"
            else:
                self._global_rate_limit_advisory_fallback_count += 1
                self._global_rate_limit_advisory_last_error = advisory_result.get("error")
                self._global_rate_limit_mode_last = "file_lock_fallback"
                await asyncio.to_thread(
                    _pace_global_api_request,
                    key=self._global_rate_limit_key,
                    delay_seconds=self._api_delay_seconds,
                )
        else:
            self._global_rate_limit_mode_last = "file_lock"
            await asyncio.to_thread(
                _pace_global_api_request,
                key=self._global_rate_limit_key,
                delay_seconds=self._api_delay_seconds,
            )
        self._last_api_request_started_at = time.monotonic()

    def _record_proxy_response(self, response: Any) -> None:
        observed_identity, observed_fingerprint = _observed_proxy_identity_from_response(response)
        if observed_identity:
            self._observed_proxy_identity = observed_identity
            self._observed_proxy_fingerprint = observed_fingerprint
            self._global_rate_limit_key = _global_rate_limit_key(
                self._proxy_config,
                observed_identity=self._observed_proxy_identity,
                observed_fingerprint=self._observed_proxy_fingerprint,
            )
        fingerprint = self._selected_proxy_fingerprint or "none"
        self._proxy_request_attempts[fingerprint] = self._proxy_request_attempts.get(fingerprint, 0) + 1
        status = str(_status_code(response) or "unknown")
        status_counts = self._proxy_status_counts.setdefault(fingerprint, {})
        status_counts[status] = status_counts.get(status, 0) + 1
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            self._proxy_auth_failures[fingerprint] = self._proxy_auth_failures.get(fingerprint, 0) + 1
            self._consecutive_auth_failures += 1
        elif _status_code(response) == 429:
            self._proxy_rate_limit_failures[fingerprint] = self._proxy_rate_limit_failures.get(fingerprint, 0) + 1
        else:
            self._consecutive_auth_failures = 0

    async def _recover_homepage_redirect(self, *, referer: str) -> bool:
        recovery_url = str(referer or "").strip() or "https://www.instagram.com/"
        self._record_retry_reason("homepage_redirect_recovery")
        try:
            recovery_response = await self._fetch_page(recovery_url, referer=recovery_url)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Instagram posts homepage redirect recovery warmup failed for %s", recovery_url, exc_info=True
            )
            return False
        status_code = _status_code(recovery_response)
        text = _response_text(recovery_response)
        if status_code >= 400 or 300 <= status_code < 400 or _auth_failure_text(text):
            return False
        self._page_tokens = _extract_page_tokens(text)
        self._merge_warmup_cookies(recovery_response)
        await self._rebuild_http_client()
        return True

    def _record_retry_reason(self, reason: str | None) -> None:
        normalized = str(reason or "").strip()
        if not normalized:
            return
        self._retry_reason_counts[normalized] = self._retry_reason_counts.get(normalized, 0) + 1

    # -------------------------------------------------------------------
    # JSON response handling with retry/backoff
    # -------------------------------------------------------------------

    @staticmethod
    def _is_transient_status(status_code: int) -> bool:
        return status_code == 429 or (500 <= status_code < 600)

    @staticmethod
    def _retry_after_seconds(response: Any) -> float | None:
        headers = getattr(response, "headers", None) or {}
        raw = None
        try:
            raw = headers.get("retry-after") if hasattr(headers, "get") else None
        except Exception:  # noqa: BLE001
            raw = None
        if raw is None:
            return None
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return None

    async def _fetch_json_response(
        self,
        url: str,
        *,
        referer: str,
        data: dict[str, str],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """JSON fetch via httpx POST with bounded exponential backoff on
        transient failures (429 / 5xx / transport timeout).
        """
        attempt = 0
        homepage_redirect_recovery_attempted = False
        last_transient_reason: str | None = None
        while True:
            attempt += 1
            try:
                response = await self._fetch_graphql(url, data=data, headers=headers)
            except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as exc:
                last_transient_reason = _transport_failure_reason(exc)
                self._record_retry_reason(last_transient_reason)
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                    }
                await asyncio.sleep(_transient_backoff_seconds(attempt, self._BASE_BACKOFF_SECONDS))
                continue

            status_code = _status_code(response)
            text = _response_text(response)
            auth_failed = status_code in {401, 403} or _auth_failure_text(text)

            # 3xx: explicit redirect handling.
            if 300 <= status_code < 400:
                location = _safe_location(response)
                reason = (
                    "redirect_to_login"
                    if "/accounts/login" in location
                    else "redirect_to_checkpoint"
                    if ("/challenge" in location or "/checkpoint" in location)
                    else "redirect_to_homepage"
                )
                logger.warning(
                    "Instagram GraphQL redirected (%d) to %s — reason=%s",
                    status_code,
                    location,
                    reason,
                )
                auth_redirect = any(token in location for token in ("login", "challenge", "checkpoint"))
                if reason == "redirect_to_homepage":
                    if not homepage_redirect_recovery_attempted:
                        homepage_redirect_recovery_attempted = True
                        if await self._recover_homepage_redirect(referer=referer):
                            continue
                    auth_redirect = True
                return {
                    "failed": True,
                    "auth_failed": auth_redirect,
                    "reason": reason,
                    "retryable": False,
                    "payload": None,
                }

            # Transient 429 / 5xx: retry with backoff.
            if self._is_transient_status(status_code):
                last_transient_reason = f"http_{status_code}"
                self._record_retry_reason(last_transient_reason)
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                    }
                retry_after = self._retry_after_seconds(response)
                sleep_seconds = _transient_backoff_seconds(
                    attempt,
                    self._BASE_BACKOFF_SECONDS,
                    retry_after=retry_after,
                )
                if status_code == 429:
                    _record_global_api_cooldown(
                        key=self._global_rate_limit_key,
                        delay_seconds=max(sleep_seconds, self._api_delay_seconds),
                    )
                await asyncio.sleep(sleep_seconds)
                continue

            # Permanent 4xx.
            if status_code >= 400:
                return {
                    "failed": True,
                    "auth_failed": auth_failed,
                    "reason": f"http_{status_code}",
                    "retryable": False,
                    "payload": None,
                }

            # HTML response (challenge page, not JSON).
            if text and text.lstrip().startswith("<"):
                return {
                    "failed": True,
                    "auth_failed": auth_failed or _auth_failure_text(text),
                    "reason": "html_challenge_or_auth_required",
                    "retryable": False,
                    "payload": None,
                }

            # Parse JSON.
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001
                try:
                    payload = json.loads(text)
                except Exception:  # noqa: BLE001
                    return {
                        "failed": True,
                        "auth_failed": auth_failed,
                        "reason": "non_json_response",
                        "retryable": False,
                        "payload": None,
                    }

            # Check IG API-level status.
            if isinstance(payload, dict):
                status_value = str(payload.get("status") or "").strip().lower()
                message = str(payload.get("message") or payload.get("error_message") or "").strip().lower()
                if status_value and status_value != "ok":
                    return {
                        "failed": True,
                        "auth_failed": auth_failed
                        or any(
                            token in f"{status_value} {message}"
                            for token in ("login", "checkpoint", "challenge", "unauthorized")
                        ),
                        "reason": status_value or "api_status_fail",
                        "retryable": False,
                        "payload": payload,
                    }

            return {
                "failed": False,
                "auth_failed": auth_failed,
                "reason": None,
                "retryable": False,
                "payload": payload,
            }
