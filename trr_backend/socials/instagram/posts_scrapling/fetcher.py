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
import json
import logging
import os
import re
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
from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge", "accounts/login"))


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
) -> dict[str, str]:
    """Build the x-www-form-urlencoded payload for the GraphQL posts POST.

    Shape mirrors what the Instagram web client sends for the
    PolarisProfilePostsTabContentQuery_connection operation. Runtime tokens
    (``lsd``, ``__spin_r``, ``__spin_b``, ``__spin_t``, ``hsi``) are merged in
    only when present.
    """
    variables = {
        "after": cursor,
        "before": None,
        "data": {
            "count": page_size,
            "include_reel_media_seen_timestamp": True,
            "include_relationship_info": True,
            "latest_besties_reel_media": True,
            "latest_reel_media": True,
        },
        "first": page_size,
        "last": None,
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
        # Phase 4.2: doc-ID rotation observability — record which doc IDs were
        # tried this run and which one ultimately succeeded. Operators can
        # cross-reference http_400 / non_json_response spikes with rotation
        # events and decide when to update SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS.
        self._doc_ids_configured: tuple[str, ...] = tuple(PROFILE_POSTS_DOC_IDS)
        self._doc_ids_attempted: list[str] = []
        self._doc_id_used: str | None = None
        self._api_delay_seconds = _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_DELAY_SEC",
            _POSTS_REQUEST_DELAY_DEFAULT,
            minimum=0.0,
            maximum=30.0,
        )
        self._last_api_request_started_at = 0.0

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
        return {
            "warmup_cookie_names": sorted(self._warmup_cookie_delta.keys()),
            "warmup_cookie_count": len(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "proxy_session_mode": self._proxy_session_mode,
            "page_tokens_found": list(self._page_tokens.keys()),
            "api_delay_seconds": self._api_delay_seconds,
            "request_count": self._request_count,
            "transport": "httpx_after_browser_warmup",
            "retry_reason_counts": dict(sorted(self._retry_reason_counts.items())),
            # Phase 4.2: doc-ID rotation telemetry.
            "profile_posts_doc_ids": {
                "configured": list(self._doc_ids_configured),
                "attempted": list(self._doc_ids_attempted),
                "used": self._doc_id_used,
            },
        }

    async def warmup(self, username: str) -> None:
        """Navigate to the profile page via Patchright to establish the session,
        extract runtime tokens from the HTML, and bridge cookies into the
        httpx client."""
        profile_url = f"https://www.instagram.com/{username}/"
        response = await self._fetch_page(profile_url, referer=profile_url)
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            raise InstagramPostsWarmupError(
                "Instagram posts warmup failed because the session appears logged out or challenged.",
                error_code="instagram_posts_warmup_auth_failed",
                retryable=False,
            )
        self._page_tokens = _extract_page_tokens(text)
        self._merge_warmup_cookies(response)
        if not self._warmup_cookie_delta and not str(self._raw_cookies.get("sessionid") or "").strip():
            raise InstagramPostsWarmupError(
                "Instagram posts warmup did not bridge cookies and no prior sessionid exists.",
                error_code="instagram_posts_warmup_no_cookies",
                retryable=True,
            )
        await self._rebuild_http_client()
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

    async def fetch_posts_page(
        self,
        username: str,
        *,
        cursor: str | None = None,
    ) -> InstagramPostsFetchResult:
        """Fetch a single page of posts via GraphQL.

        Iterates through ``PROFILE_POSTS_DOC_IDS`` until one returns a populated
        ``xdt_api__v1__feed__user_timeline_graphql_connection``. Returns on
        first success; on full exhaustion returns a failure result carrying
        the last observed reason.
        """
        referer = f"https://www.instagram.com/{username}/"
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

        for doc_id in PROFILE_POSTS_DOC_IDS:
            # Phase 4.2: track every doc_id this run actually tried.
            if doc_id not in self._doc_ids_attempted:
                self._doc_ids_attempted.append(doc_id)
            data = _build_graphql_form_data(
                username=username,
                cursor=cursor,
                page_size=self._page_size,
                viewer_id=viewer_id,
                page_tokens=self._page_tokens,
                doc_id=doc_id,
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
                if current_reason and not fetch_reason:
                    fetch_reason = current_reason
                if current_auth or (cursor and not current_retryable):
                    # Auth failure or non-retryable cursor fetch — stop trying
                    # remaining doc_ids.
                    break
                continue

            if not isinstance(payload, dict):
                if current_reason and not fetch_reason:
                    fetch_reason = current_reason
                continue

            connection = payload.get("data", {}).get("xdt_api__v1__feed__user_timeline_graphql_connection") or {}
            if not connection:
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

            # Phase 4.2: record the doc_id that produced data so operators can
            # see which IDs are healthy without grepping logs.
            self._doc_id_used = doc_id
            return InstagramPostsFetchResult(
                posts=posts,
                fetch_failed=False,
                auth_failed=False,
                fetch_reason=None,
                request_count=self._request_count,
                retryable=False,
                has_next_page=bool(page_info.get("has_next_page", False)),
                end_cursor=(str(page_info.get("end_cursor")) if page_info.get("end_cursor") else None),
            )

        return InstagramPostsFetchResult(
            posts=[],
            fetch_failed=True,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason or "graphql_no_doc_id_succeeded",
            request_count=self._request_count,
            retryable=retryable,
            has_next_page=False,
            end_cursor=None,
        )

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
        self._sync_response_cookies(response)
        return response

    async def _pace_api_requests(self) -> None:
        if self._api_delay_seconds <= 0:
            return
        remaining = (self._last_api_request_started_at + self._api_delay_seconds) - time.monotonic()
        if remaining > 0:
            await asyncio.sleep(remaining)
        self._last_api_request_started_at = time.monotonic()

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
