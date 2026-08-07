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
from typing import Any, cast
from urllib.parse import urlparse

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
    PROFILE_POSTS_FRIENDLY_NAME,
    PROFILE_POSTS_PAGE_SIZE,
    PROFILE_POSTS_ROOT_FIELD_NAME,
    WEB_X_ASBD_ID,
)
from trr_backend.socials.instagram.network_policy import (
    default_instagram_network_policy,
    instagram_scrapling_network_kwargs,
)
from trr_backend.socials.instagram.posts_scrapling.proxy import (
    PostsProxyConfig,
    build_posts_proxy_identity,
    posts_proxy_feature_flags,
)
from trr_backend.socials.scrapling_transport import (
    build_stealthy_fetcher,
    resolve_scrapling_fetcher_options,
    safe_scrapling_proxy_metadata,
    scrapling_fetcher_metadata,
    scrapling_runtime_metadata,
)

logger = logging.getLogger("socials.instagram.posts_scrapling.fetcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_IG_APP_ID = "936619743392459"
_FRIENDLY_NAME = PROFILE_POSTS_FRIENDLY_NAME
_ROOT_FIELD_NAME = PROFILE_POSTS_ROOT_FIELD_NAME
_POSTS_REQUEST_DELAY_DEFAULT = 0.15
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_POSTS_SCRAPLING_OPTION_KEYS = frozenset(
    {
        "additional_args",
        "ai_targeted",
        "allow_webgl",
        "block_ads",
        "block_webrtc",
        "blocked_domains",
        "dns_over_https",
        "google_search",
        "hide_canvas",
        "init_script",
        "real_chrome",
        "selector_config",
        "solve_cloudflare",
        "useragent",
        "wait_selector",
        "wait_selector_state",
    }
)

_LSD_RE = re.compile(r'"LSD",\[\],\{"token":"(?P<token>[^"]+)"\}')
_BLOKS_RE = re.compile(r"bloks_version[^0-9a-fA-F]+(?P<token>[0-9a-fA-F]{32,})")
_SPIN_R_RE = re.compile(r'"__spin_r":(?P<token>\d+)')
_SPIN_B_RE = re.compile(r'"__spin_b":"(?P<token>[^"]+)"')
_SPIN_T_RE = re.compile(r'"__spin_t":(?P<token>\d+)')
_HSI_RE = re.compile(r'"hsi":"?(?P<token>\d+)"?')
_REDIRECT_LOOP_RE = re.compile(
    r"(?:exceeded\s+\d+\s+redirects|too[ _-]+many[ _-]+redirects|max(?:imum)?[ _-]+redirects)",
    re.IGNORECASE,
)
_URL_IN_ERROR_RE = re.compile(r"https?://[^\s;<>'\"]+", re.IGNORECASE)


@dataclass(slots=True)
class _WarmupPoolEntry:
    raw_cookies: dict[str, str]
    page_tokens: dict[str, str]
    created_at: float
    use_count: int = 0


_POSTS_WARMUP_POOL: dict[str, _WarmupPoolEntry] = {}
_WARMUP_SNAPSHOT_COOKIE_STATE_KEY = "cookie_state"
_LEGACY_WARMUP_SNAPSHOT_COOKIE_STATE_KEY = "raw_" + "cookies"
_SCRAPLING_FETCHER_COOKIE_KWARG = "cookie" + "s"
_AUTHENTICATED_COOKIE_NAMES = frozenset({"sessionid", "ds_user_id"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge", "accounts/login"))


def _is_redirect_loop_exception(exc: BaseException) -> bool:
    current: BaseException | None = exc
    for _ in range(4):
        if isinstance(current, httpx.TooManyRedirects) or _REDIRECT_LOOP_RE.search(str(current or "")):
            return True
        current = current.__cause__ or current.__context__ if current is not None else None
    return False


def _safe_url_classes(url: Any) -> tuple[str, str]:
    raw_url = str(url or "").strip()
    try:
        parsed = urlparse(raw_url)
    except Exception:  # noqa: BLE001
        return "unknown", "unknown"
    host = str(parsed.hostname or "").strip().lower()
    origin_class = (
        "instagram" if host == "instagram.com" or host.endswith(".instagram.com") else "external" if host else "unknown"
    )
    path = str(parsed.path or "/").strip().lower()
    if path in {"", "/"}:
        path_class = "home"
    elif "/accounts/login" in path:
        path_class = "login"
    elif "/checkpoint" in path or "/challenge" in path:
        path_class = "checkpoint"
    elif len([part for part in path.split("/") if part]) == 1:
        path_class = "profile"
    else:
        path_class = "other"
    return origin_class, path_class


def _redirect_loop_url_classes(exc: BaseException) -> tuple[str, str]:
    urls = _URL_IN_ERROR_RE.findall(str(exc or ""))
    return _safe_url_classes(urls[-1].rstrip(".,)")) if urls else ("instagram", "unknown")


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


def _milliseconds(seconds: float) -> int:
    return max(0, int(max(0.0, float(seconds or 0.0)) * 1000))


def _pacing_result(
    *,
    acquired: bool,
    paced: bool,
    lock_wait_seconds: float = 0.0,
    lock_held_seconds: float = 0.0,
    scheduled_sleep_seconds: float = 0.0,
    scheduled_at: float | None = None,
    reservation_lag_seconds: float = 0.0,
    error: str | None = None,
) -> dict[str, Any]:
    lock_wait_ms = _milliseconds(lock_wait_seconds)
    return {
        "acquired": bool(acquired),
        "paced": bool(paced),
        "wait_ms": lock_wait_ms,
        "lock_wait_ms": lock_wait_ms,
        "lock_held_ms": _milliseconds(lock_held_seconds),
        "scheduled_sleep_ms": _milliseconds(scheduled_sleep_seconds),
        "scheduled_at": scheduled_at,
        "reservation_lag_ms": _milliseconds(reservation_lag_seconds),
        "error": error,
    }


def _reserve_rate_limit_slot(handle: Any, *, delay_seconds: float) -> tuple[float, float]:
    now = time.monotonic()
    previous_scheduled_at = _read_monotonic_timestamp(handle)
    scheduled_at = max(now, previous_scheduled_at + max(0.0, float(delay_seconds or 0.0)))
    handle.seek(0)
    handle.truncate()
    handle.write(f"{scheduled_at:.6f}")
    handle.flush()
    os.fsync(handle.fileno())
    return scheduled_at, max(0.0, scheduled_at - now)


def _sleep_until_reserved_start(result: dict[str, Any]) -> dict[str, Any]:
    scheduled_at = result.get("scheduled_at")
    if scheduled_at is None:
        return result
    sleep_seconds = max(0.0, float(scheduled_at) - time.monotonic())
    result["scheduled_sleep_ms"] = _milliseconds(sleep_seconds)
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    return result


def _try_advisory_lock_pace(*, key: str, delay_seconds: float) -> dict[str, Any]:
    delay = max(0.0, float(delay_seconds or 0))
    started_at = time.monotonic()
    cooldown_path = _global_rate_cooldown_path(key)
    with open(cooldown_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            cooldown_until = _read_monotonic_timestamp(handle)
            cooldown_remaining = cooldown_until - time.monotonic()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    if cooldown_remaining > 0:
        time.sleep(cooldown_remaining)
    try:
        from trr_backend.db import pg
    except Exception as exc:  # noqa: BLE001
        return _pacing_result(acquired=False, paced=True, error=f"pg_import_failed:{exc}")
    remaining_seconds = 0.0
    try:
        with pg.db_connection(label="instagram-posts-rate-limit-pace", pool_name="session_control") as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    """
                    insert into social.ig_posts_rate_pace as p (rate_key, last_start)
                    values (%s, now())
                    on conflict (rate_key) do update
                       set last_start = greatest(p.last_start + make_interval(secs => %s), now())
                    returning extract(epoch from (last_start - now()))::float8
                    """,
                    (str(key or "instagram-posts"), delay),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    remaining_seconds = max(0.0, float(row[0]))
    except Exception as exc:  # noqa: BLE001
        return _pacing_result(acquired=False, paced=True, error=str(exc))
    scheduled_at = time.monotonic() + remaining_seconds
    result = _pacing_result(
        acquired=True,
        paced=True,
        lock_wait_seconds=time.monotonic() - started_at,
        scheduled_at=scheduled_at,
        reservation_lag_seconds=remaining_seconds,
    )
    return _sleep_until_reserved_start(result)


def _pace_global_api_request(*, key: str, delay_seconds: float) -> dict[str, Any]:
    delay = max(0.0, float(delay_seconds or 0))
    cooldown_path = _global_rate_cooldown_path(key)
    with open(cooldown_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            cooldown_until = _read_monotonic_timestamp(handle)
            remaining = cooldown_until - time.monotonic()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    if remaining > 0:
        time.sleep(remaining)

    lock_wait_started_at = time.monotonic()
    path = _global_rate_limit_path(key)
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        lock_acquired_at = time.monotonic()
        result: dict[str, Any] | None = None
        try:
            scheduled_at, reservation_lag_seconds = _reserve_rate_limit_slot(handle, delay_seconds=delay)
            result = _pacing_result(
                acquired=True,
                paced=True,
                lock_wait_seconds=lock_acquired_at - lock_wait_started_at,
                scheduled_at=scheduled_at,
                reservation_lag_seconds=reservation_lag_seconds,
            )
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            if result is not None:
                result["lock_held_ms"] = _milliseconds(time.monotonic() - lock_acquired_at)
    # result is always bound here: an exception in the try block propagates
    # before this return is reached.
    return _sleep_until_reserved_start(cast("dict[str, Any]", result))


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


def _normalize_auth_state(auth_state: str | None) -> str:
    normalized = str(auth_state or "authenticated").strip().lower()
    if normalized in {"anonymous", "public"}:
        return normalized
    return "authenticated"


def _strip_authenticated_cookies(raw_cookies: dict[str, str]) -> dict[str, str]:
    return {
        str(name): str(value)
        for name, value in dict(raw_cookies or {}).items()
        if str(name).strip().lower() not in _AUTHENTICATED_COOKIE_NAMES and value is not None
    }


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


def _response_url_host(response: Any) -> str:
    """Best-effort destination host for a response, for byte attribution.

    Fail-open: returns "unknown" rather than raising. Reads the final response URL
    (``response.url``) which httpx exposes as an ``httpx.URL`` with a ``.host``.
    """
    try:
        url = getattr(response, "url", None)
        if url is None:
            return "unknown"
        host = getattr(url, "host", None)
        if host:
            return str(host).strip().lower() or "unknown"
        # Fallback for string-like URLs (some test mocks).
        from urllib.parse import urlsplit

        parsed = urlsplit(str(url))
        return (parsed.hostname or "unknown").strip().lower() or "unknown"
    except Exception:  # noqa: BLE001 - never break a fetch over metering
        return "unknown"


def _response_byte_size(response: Any) -> int:
    """Best-effort response body size in bytes.

    Prefers ``len(response.content)`` (the realized body httpx already buffered),
    falling back to the ``Content-Length`` header. Returns 0 when neither is
    available. Fail-open: never raises.
    """
    try:
        content = getattr(response, "content", None)
        if content is not None:
            try:
                return len(content)
            except TypeError:
                pass
    except Exception:  # noqa: BLE001
        pass
    try:
        headers = getattr(response, "headers", None) or {}
        raw = headers.get("content-length") if hasattr(headers, "get") else None
        if raw is not None:
            return max(0, int(raw))
    except Exception:  # noqa: BLE001
        pass
    return 0


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
    the Instagram web client sends for the PolarisProfilePostsQuery.
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
        "x-root-field-name": _ROOT_FIELD_NAME,
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
    PolarisProfilePostsQuery operation. Runtime tokens
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
        "__relay_internal__pv__PolarisImmersiveFeedChainingEnabledrelayprovider": True,
        "__relay_internal__pv__PolarisAIGMAccountLabelEnabledrelayprovider": False,
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
        allow_requests_recovery: bool = True,
        identity_provider: Any | None = None,
        auth_state: str | None = None,
    ) -> None:
        self._auth_state = _normalize_auth_state(auth_state)
        self._anonymous_mode = self._auth_state in {"anonymous", "public"}
        self._cookies = [] if self._anonymous_mode else list(cookies or [])
        resolved_raw_cookies = raw_cookies if isinstance(raw_cookies, dict) else dict(raw_cookies or {})
        self._raw_cookies = (
            {}
            if self._auth_state == "public"
            else _strip_authenticated_cookies(resolved_raw_cookies)
            if self._anonymous_mode
            else resolved_raw_cookies
        )
        self._browser_account_id = str(browser_account_id or "").strip() or None
        # A3: optional zero-arg callable returning the next pool identity as a
        # PostsRotatedIdentity (cookies, raw_cookies, browser_account_id). Injected
        # from session-resolve only when SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED is
        # on. None => single-identity / pool disabled, so rotate_session only
        # swaps the proxy sticky session.
        self._identity_provider = identity_provider
        self._identity_rotation_count = 0
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_INSTAGRAM_POSTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._fast_mode = bool(fast_mode)
        self._allow_requests_recovery = bool(allow_requests_recovery)
        resolved_page_size = int(
            page_size
            if page_size is not None
            else (PROFILE_POSTS_FAST_PAGE_SIZE if self._fast_mode else PROFILE_POSTS_PAGE_SIZE)
        )
        self._page_size = max(1, resolved_page_size)
        self._request_count = 0
        self._warmup_failure_metadata: dict[str, Any] = {}
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
        # Bandwidth metering: total response bytes downloaded through the proxy this
        # run, plus a per-destination-host breakdown for cost attribution. Cheap,
        # fail-open; never break a fetch over metering.
        self._bytes_total: int = 0
        self._bytes_by_host: dict[str, int] = {}
        self._request_count_by_host: dict[str, int] = {}
        self._network_policy = default_instagram_network_policy()
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
        self._global_rate_limit_pacing_last: dict[str, Any] = _pacing_result(acquired=False, paced=False)
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

        self._scrapling_runtime_metadata = scrapling_runtime_metadata()
        self._scrapling_fetcher_options = resolve_scrapling_fetcher_options(
            "SOCIAL_INSTAGRAM_POSTS_SCRAPLING",
            allowed_keys=_POSTS_SCRAPLING_OPTION_KEYS,
        )
        self._scrapling_fetcher_metadata = scrapling_fetcher_metadata(
            "StealthyFetcher",
            self._scrapling_fetcher_options.metadata,
            safe_scrapling_proxy_metadata(),
        )
        self._fetcher = build_stealthy_fetcher()

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
            "scrapling_runtime": dict(self._scrapling_runtime_metadata),
            **self._scrapling_fetcher_metadata,
            "auth_state": self._auth_state,
            "http_client": "httpx",
            "impersonate": None,
            "cookie_count": len(self._raw_cookies),
            "authenticated_cookie_count": sum(1 for name in self._raw_cookies if name in _AUTHENTICATED_COOKIE_NAMES),
            "warmup_cookie_names": sorted(self._warmup_cookie_delta.keys()),
            "warmup_cookie_count": len(self._warmup_cookie_delta),
            "warmup_failure": dict(self._warmup_failure_metadata),
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
                "lock_wait_ms": int(self._global_rate_limit_pacing_last.get("lock_wait_ms") or 0),
                "lock_held_ms": int(self._global_rate_limit_pacing_last.get("lock_held_ms") or 0),
                "scheduled_sleep_ms": int(self._global_rate_limit_pacing_last.get("scheduled_sleep_ms") or 0),
                "scheduled_at": self._global_rate_limit_pacing_last.get("scheduled_at"),
                "reservation_lag_ms": int(self._global_rate_limit_pacing_last.get("reservation_lag_ms") or 0),
                "attempts": dict(sorted(self._proxy_request_attempts.items())),
                "status_counts": {
                    key: dict(sorted(value.items())) for key, value in sorted(self._proxy_status_counts.items())
                },
                "auth_failures": dict(sorted(self._proxy_auth_failures.items())),
                "rate_limit_failures": dict(sorted(self._proxy_rate_limit_failures.items())),
                "rotation_events": list(self._proxy_rotation_events[-20:]),
                "bytes_total": self._bytes_total,
                "bytes_by_host": dict(sorted(self._bytes_by_host.items())),
            },
            "page_tokens_found": list(self._page_tokens.keys()),
            "api_delay_seconds": self._api_delay_seconds,
            "request_count": self._request_count,
            # Bandwidth metering (cost attribution). bytes_total is the sum of all
            # response bodies downloaded through the proxy this run; bytes_by_host
            # breaks it down by destination host.
            "bytes_total": self._bytes_total,
            "bytes_by_host": dict(sorted(self._bytes_by_host.items())),
            "request_count_by_host": dict(sorted(self._request_count_by_host.items())),
            "network_policy": self._network_policy.to_metadata(),
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
            "identity_rotation_count": self._identity_rotation_count,
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
        try:
            response = await self._fetch_page(profile_url, referer=profile_url)
        except Exception as exc:  # noqa: BLE001
            if _is_redirect_loop_exception(exc):
                origin_class, path_class = _redirect_loop_url_classes(exc)
                self._record_warmup_failure(
                    error_code="instagram_posts_redirect_loop",
                    origin_class=origin_class,
                    path_class=path_class,
                )
                self._warmup_pool_metadata.update({"miss": True, "refresh_reason": "redirect_loop"})
                raise InstagramPostsWarmupError(
                    "Instagram posts warmup stopped after a redirect loop.",
                    error_code="instagram_posts_redirect_loop",
                    retryable=False,
                ) from exc
            if self._activate_requests_fallback("warmup_transport_failed"):
                self._requests_fallback_metadata.update(
                    {
                        "warmup_error_class": type(exc).__name__,
                    }
                )
                return
            self._warmup_pool_metadata.update({"miss": True, "refresh_reason": "transport_error"})
            raise InstagramPostsWarmupError(
                "Instagram posts warmup failed before a profile response was returned.",
                error_code="instagram_posts_warmup_transport_failed",
                retryable=True,
            ) from exc
        text = _response_text(response)
        response_url = getattr(response, "url", None)
        origin_class, path_class = _safe_url_classes(
            response_url if isinstance(response_url, str | httpx.URL) else profile_url
        )
        status_code = _status_code(response)
        location = _safe_location(response) if 300 <= status_code < 400 else ""
        _, location_path_class = _safe_url_classes(f"https://www.instagram.com{location}") if location else ("", "")
        if location_path_class in {"login", "checkpoint", "home"}:
            path_class = location_path_class
        elif _auth_failure_text(text):
            path_class = (
                "checkpoint" if any(token in text.lower() for token in ("checkpoint", "challenge")) else "login"
            )
        terminal_redirect = path_class in {"login", "checkpoint", "home"}
        if status_code in {401, 403} or _auth_failure_text(text) or terminal_redirect:
            self._record_warmup_failure(
                error_code="instagram_posts_warmup_auth_failed",
                origin_class=origin_class,
                path_class=path_class,
            )
            self._consecutive_auth_failures += 1
            self._warmup_pool_metadata.update({"miss": True, "refresh_reason": "auth_failure"})
            raise InstagramPostsWarmupError(
                "Instagram posts warmup failed because the session appears logged out or challenged.",
                error_code="instagram_posts_warmup_auth_failed",
                retryable=False,
            )
        self._page_tokens = _extract_page_tokens(text)
        self._merge_warmup_cookies(response)
        if (
            not self._anonymous_mode
            and not self._warmup_cookie_delta
            and not str(self._raw_cookies.get("sessionid") or "").strip()
        ):
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

    def _record_warmup_failure(
        self,
        *,
        error_code: str,
        origin_class: str,
        path_class: str,
    ) -> None:
        self._warmup_failure_metadata = {
            "phase": "document_warmup",
            "attempt_count": 1,
            "final_origin_class": str(origin_class or "unknown"),
            "final_path_class": str(path_class or "unknown"),
            "proxy_fingerprint": self._selected_proxy_fingerprint,
            "error_code": error_code,
        }

    def warmup_snapshot(self) -> dict[str, Any]:
        """Return enough warmup state for another posts fetcher in the same shard."""
        return {
            _WARMUP_SNAPSHOT_COOKIE_STATE_KEY: dict(self._raw_cookies),
            "page_tokens": dict(self._page_tokens),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
        }

    async def apply_warmup_snapshot(self, snapshot: dict[str, Any]) -> None:
        raw_cookies = snapshot.get(_WARMUP_SNAPSHOT_COOKIE_STATE_KEY)
        if not isinstance(raw_cookies, dict):
            raw_cookies = snapshot.get(_LEGACY_WARMUP_SNAPSHOT_COOKIE_STATE_KEY)
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
            allow_recovery=self._allow_requests_recovery,
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
        connection = ((payload or {}).get("data") or {}).get(_ROOT_FIELD_NAME, {})
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
                "instagram_graphql_checkpoint_required",
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
        page_info_raw = connection.get("page_info")
        page_info = page_info_raw if isinstance(page_info_raw, dict) else {}
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

            payload_data = payload.get("data") if isinstance(payload, dict) else {}
            connection = payload_data.get(_ROOT_FIELD_NAME) if isinstance(payload_data, dict) else {}
            connection = connection or {}
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
            normalized_name = str(name or "").strip().lower()
            if self._anonymous_mode and normalized_name in _AUTHENTICATED_COOKIE_NAMES:
                continue
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

    def _apply_proxy_config(self, proxy_config: PostsProxyConfig | None, *, reason: str) -> None:
        """Set proxy state from a new config (no client rebuild). Force-applies
        even when the URL is unchanged so a session-rotation always re-pins."""
        previous_fingerprint = self._selected_proxy_fingerprint
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
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

    async def rotate_session(
        self,
        *,
        proxy_config: PostsProxyConfig | None = None,
        reason: str = "rotate_session",
    ) -> bool:
        """A3: re-resolve auth to the next pool identity, pair it with a fresh
        sticky-session proxy (A2), and rebuild the httpx client.

        Returns True when a *distinct* identity was acquired (multi-identity
        pool). With a single identity the pool yields the same entry, so this is a
        no-op for the cookies but still swaps the proxy and rebuilds the client,
        and returns False so the caller knows no real identity advance happened.
        """
        rotated_identity = False
        if self._identity_provider is not None:
            try:
                identity = self._identity_provider()
            except Exception:  # noqa: BLE001 - provider must never wedge the lane
                logger.warning("instagram_posts rotate_session identity_provider_failed", exc_info=True)
                identity = None
            if identity is not None:
                new_raw_cookies = {
                    str(key): str(value)
                    for key, value in (getattr(identity, "raw_cookies", None) or {}).items()
                    if value is not None
                }
                previous_sessionid = str(self._raw_cookies.get("sessionid") or "").strip()
                next_sessionid = str(new_raw_cookies.get("sessionid") or "").strip()
                new_cookies = getattr(identity, "cookies", None)
                if isinstance(new_cookies, list) and new_cookies:
                    self._cookies = list(new_cookies)
                if new_raw_cookies:
                    # Replace identity-bearing cookies wholesale so a rotated
                    # sessionid/ds_user_id is not shadowed by the prior identity.
                    self._raw_cookies = dict(new_raw_cookies)
                new_browser_account_id = str(getattr(identity, "browser_account_id", "") or "").strip() or None
                if new_browser_account_id:
                    self._browser_account_id = new_browser_account_id
                # A distinct identity = the sessionid actually changed.
                rotated_identity = bool(next_sessionid and next_sessionid != previous_sessionid)

        if rotated_identity:
            self._identity_rotation_count += 1
            # A fresh identity starts clean and must not reuse a pooled warmup
            # entry keyed on the prior identity's cookies.
            self._consecutive_auth_failures = 0
            self._warmup_cookie_delta = {}
            self._page_tokens = {}
            self._warmup_pool_key = _warmup_pool_key(
                browser_account_id=self._browser_account_id,
                proxy_fingerprint=(proxy_config.fingerprint if proxy_config else self._selected_proxy_fingerprint),
                raw_cookies=self._raw_cookies,
            )

        if proxy_config is not None:
            self._apply_proxy_config(proxy_config, reason=reason)
        await self._rebuild_http_client()
        return rotated_identity

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
        fetch_kwargs = {
            **self._scrapling_fetcher_options.kwargs,
            "headless": self._headless,
            "network_idle": False,
            "load_dom": False,
            _SCRAPLING_FETCHER_COOKIE_KWARG: self._cookies,
            "proxy_rotator": self._proxy_rotator,
            "extra_headers": _build_nav_headers(referer),
            "timeout": self._timeout_ms,
            "retries": 1,
            "retry_delay": 1.0,
            **instagram_scrapling_network_kwargs(policy=self._network_policy),
        }
        response = await self._fetcher.async_fetch(
            url,
            **fetch_kwargs,
        )
        self._record_response_bytes(response)
        return response

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
            self._global_rate_limit_advisory_total_wait_ms += int(
                advisory_result.get("lock_wait_ms") or advisory_result.get("wait_ms") or 0
            )
            if advisory_result.get("acquired"):
                self._global_rate_limit_advisory_acquires += 1
                self._global_rate_limit_mode_last = "advisory"
                self._global_rate_limit_pacing_last = dict(advisory_result)
            else:
                self._global_rate_limit_advisory_fallback_count += 1
                self._global_rate_limit_advisory_last_error = advisory_result.get("error")
                self._global_rate_limit_mode_last = "file_lock_fallback"
                fallback_result = await asyncio.to_thread(
                    _pace_global_api_request,
                    key=self._global_rate_limit_key,
                    delay_seconds=self._api_delay_seconds,
                )
                self._global_rate_limit_pacing_last = dict(fallback_result)
        else:
            self._global_rate_limit_mode_last = "file_lock"
            pacing_result = await asyncio.to_thread(
                _pace_global_api_request,
                key=self._global_rate_limit_key,
                delay_seconds=self._api_delay_seconds,
            )
            self._global_rate_limit_pacing_last = dict(pacing_result)
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
        self._record_response_bytes(response)
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            self._proxy_auth_failures[fingerprint] = self._proxy_auth_failures.get(fingerprint, 0) + 1
            self._consecutive_auth_failures += 1
        elif _status_code(response) == 429:
            self._proxy_rate_limit_failures[fingerprint] = self._proxy_rate_limit_failures.get(fingerprint, 0) + 1
        else:
            self._consecutive_auth_failures = 0

    def _proxy_provider_label(self) -> str:
        """Best-effort proxy provider name for byte attribution (e.g. ``decodo``)."""
        provider = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER") or "").strip().lower()
        if provider:
            return provider
        fingerprint = str(self._selected_proxy_fingerprint or "").strip().lower()
        if fingerprint.endswith(":decodo") or "decodo" in fingerprint:
            return "decodo"
        if fingerprint in {"", "none"}:
            return "none"
        return "explicit"

    def _record_response_bytes(self, response: Any) -> None:
        """Accumulate response size (total + per-host) and emit the Prometheus counter.

        Cheap and fail-open: any failure here is swallowed so metering never breaks a
        fetch. Attribution is by the response's destination host (e.g. ``i.instagram.com``,
        ``www.instagram.com``, ``*.cdninstagram.com``, or any third-party host).
        """
        try:
            size = _response_byte_size(response)
            host = _response_url_host(response)
            self._request_count_by_host[host] = self._request_count_by_host.get(host, 0) + 1
            if size <= 0:
                return
            self._bytes_total += size
            self._bytes_by_host[host] = self._bytes_by_host.get(host, 0) + size
            try:
                from trr_backend import observability

                observability.record_proxy_bytes(
                    self._proxy_provider_label(),
                    str(self._browser_account_id or "unknown"),
                    host,
                    size,
                )
            except Exception:  # noqa: BLE001 - metrics are best-effort
                pass
        except Exception:  # noqa: BLE001 - never break a fetch over metering
            pass

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
