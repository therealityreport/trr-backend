"""Hybrid fetcher for the TikTok posts Scrapling lane.

Architecture:
  warmup()     ->  StealthyFetcher (Patchright) -> tiktok.com/@{username}
  user_detail  ->  httpx GET /api/user/detail/
  posts        ->  httpx GET /api/post/item_list/ (paginated)

The browser handles challenge solving. All API calls go through httpx
with cookies bridged from warmup.

RISK: TikTok API may require X-Bogus/_signature JS-generated params. If
the API returns challenge pages after warmup, this lane returns
``challenge_or_blocked``. The lane is explicitly experimental.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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
    response_text as _response_text,
)
from trr_backend.socials._scrapling_http_utils import (
    safe_location as _safe_location,
)
from trr_backend.socials._scrapling_http_utils import (
    status_code as _status_code,
)
from trr_backend.socials.scrapling_transport import (
    build_stealthy_fetcher,
    cookies_to_scrapling,
    merge_response_cookies,
    resolve_scrapling_fetcher_options,
    safe_cookie_metadata,
    safe_scrapling_proxy_metadata,
    scrapling_fetcher_metadata,
    scrapling_runtime_metadata,
)
from trr_backend.socials.tiktok.posts_scrapling.proxy import TikTokPostsProxyConfig

logger = logging.getLogger("socials.tiktok.posts_scrapling.fetcher")

TIKTOK_USER_DETAIL_URL = "https://www.tiktok.com/api/user/detail/"
TIKTOK_POST_LIST_URL = "https://www.tiktok.com/api/post/item_list/"
TIKTOK_POST_PAGE_SIZE = 30
TIKTOK_POST_PAGE_SIZE_MIN = 10
TIKTOK_POST_PAGE_SIZE_MAX = 50
_SEC_UID_RE = re.compile(r'\\?"secUid\\?"\s*:\s*\\?"(?P<sec_uid>[^"\\]+)')
_TIKTOK_SCRAPLING_OPTION_KEYS = frozenset(
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


# ---------------------------------------------------------------------------
# Pure helpers (copy pattern from Instagram fetcher)
# ---------------------------------------------------------------------------


def _build_tiktok_headers(referer: str) -> dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.tiktok.com",
        "referer": referer,
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
    }


def _classify_challenge_response(text: str) -> str | None:
    body = str(text or "").strip().lower()[:1024]
    if not body:
        return None
    if any(token in body for token in ("x-bogus", "_signature", "invalid signature", "signature verification")):
        return "js_generated_params_required"
    if any(token in body for token in ("captcha", "verify", "challenge")):
        return "captcha_or_challenge"
    if any(token in body for token in ("login", "sign in", "signin")):
        return "login_required"
    if "<html" in body:
        return "html_response"
    return None


def _captured_xhr_paths(response: Any) -> list[str]:
    candidates: list[Any] = []
    for attr_name in ("captured_xhr", "xhr", "xhr_requests", "requests"):
        value = getattr(response, attr_name, None)
        if isinstance(value, list):
            candidates.extend(value)
    paths: list[str] = []
    for item in candidates:
        raw_url = ""
        if isinstance(item, dict):
            raw_url = str(item.get("url") or item.get("request_url") or "")
        else:
            raw_url = str(getattr(item, "url", "") or getattr(item, "request_url", "") or "")
        if not raw_url:
            continue
        try:
            from urllib.parse import urlparse

            parsed = urlparse(raw_url)
            path = parsed.path or raw_url
        except Exception:  # noqa: BLE001
            path = raw_url
        if path:
            paths.append(path)
    return sorted(set(paths))


def tiktok_posts_scrapling_page_size() -> int:
    raw_value = str(os.getenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE") or "").strip()
    if not raw_value:
        return TIKTOK_POST_PAGE_SIZE
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return TIKTOK_POST_PAGE_SIZE
    return min(TIKTOK_POST_PAGE_SIZE_MAX, max(TIKTOK_POST_PAGE_SIZE_MIN, parsed))


def _extract_sec_uid_from_text(text: str) -> str | None:
    normalized = str(text or "")
    for candidate in (normalized, normalized.replace(r"\"", '"')):
        match = _SEC_UID_RE.search(candidate)
        if match:
            return str(match.group("sec_uid") or "").strip() or None
    return None


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TikTokPostsFetchResult:
    posts: list[dict[str, Any]] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    fetch_reason: str | None = None
    request_count: int = 0
    retryable: bool = False
    has_more: bool = False
    cursor: str | None = None


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class TikTokPostsScraplingFetcher:
    """Hybrid fetcher: Patchright warmup, httpx API pagination."""

    _MAX_TRANSIENT_RETRIES: int = 3
    _BASE_BACKOFF_SECONDS: float = 1.0

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        proxy_config: TikTokPostsProxyConfig | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = raw_cookies if isinstance(raw_cookies, dict) else dict(raw_cookies or {})
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_TIKTOK_POSTS_HEADLESS", True)
        self._capture_xhr = _env_truthy("SOCIAL_TIKTOK_POSTS_CAPTURE_XHR", False)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._request_count = 0
        self._seed_raw_cookies = dict(self._raw_cookies)
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"
        self._sec_uid: str | None = None
        self._sec_uid_source: str | None = None
        self._warmup_sec_uid: str | None = None
        self._warmup_status_code: int | None = None
        self._warmup_challenge_classification: str | None = None
        self._last_challenge_classification: str | None = None
        self._captured_xhr_paths: list[str] = []
        self._scrapling_runtime_metadata = scrapling_runtime_metadata()
        self._scrapling_fetcher_options = resolve_scrapling_fetcher_options(
            "SOCIAL_TIKTOK_POSTS_SCRAPLING",
            allowed_keys=_TIKTOK_SCRAPLING_OPTION_KEYS,
        )
        self._scrapling_fetcher_metadata = scrapling_fetcher_metadata(
            "StealthyFetcher",
            self._scrapling_fetcher_options.metadata,
            safe_scrapling_proxy_metadata(),
        )
        self._fetcher = build_stealthy_fetcher()
        self._http_client: httpx.AsyncClient | None = None

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        cookie_metadata = safe_cookie_metadata(self._seed_raw_cookies, self._warmup_cookie_delta, prefix="")
        return {
            **cookie_metadata,
            "scrapling_runtime": dict(self._scrapling_runtime_metadata),
            **self._scrapling_fetcher_metadata,
            "cookie_sync_count": len(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "sec_uid_resolved": bool(self._sec_uid),
            "sec_uid_source": self._sec_uid_source,
            "warmup_sec_uid_resolved": bool(self._warmup_sec_uid),
            "warmup_status_code": self._warmup_status_code,
            "warmup_challenge_classification": self._warmup_challenge_classification,
            "last_challenge_classification": self._last_challenge_classification,
            "capture_xhr_enabled": self._capture_xhr,
            "captured_xhr_count": len(self._captured_xhr_paths),
            "captured_xhr_paths": list(self._captured_xhr_paths),
            "api_signature_limitation": "tiktok_api_may_require_js_generated_params",
            "request_count": self._request_count,
            "transport": "httpx_after_browser_warmup",
        }

    async def warmup(self, username: str) -> None:
        """Navigate to profile via Patchright to establish session."""
        profile_url = f"https://www.tiktok.com/@{username}"
        response = await self._fetch_page(profile_url, referer="https://www.tiktok.com/")
        text = _response_text(response)
        status = _status_code(response)
        self._warmup_status_code = status
        self._captured_xhr_paths = _captured_xhr_paths(response)
        self._warmup_challenge_classification = _classify_challenge_response(text)
        if (status in {401, 403}) or (
            self._warmup_challenge_classification is not None and status not in range(200, 300)
        ):
            raise RuntimeError("TikTok warmup hit challenge page or auth failure; cookies may be invalid.")
        self._warmup_sec_uid = _extract_sec_uid_from_text(text)
        self._merge_warmup_cookies(response)
        self._rebuild_http_client()
        logger.info(
            "tiktok_posts_scrapling warmup_success",
            extra={
                "event": "warmup_success",
                "account": username,
                "cookie_count": len(self._warmup_cookie_delta),
                "cookie_sync_count": len(self._warmup_cookie_delta),
                "proxy_fingerprint": self._selected_proxy_fingerprint,
                "warmup_status_code": self._warmup_status_code,
                "captured_xhr_count": len(self._captured_xhr_paths),
            },
        )

    async def resolve_sec_uid(self, username: str) -> str:
        """Fetch secUid via user detail API. Required for post pagination."""
        response = await self._fetch_api_json(
            TIKTOK_USER_DETAIL_URL,
            params={"uniqueId": username},
            referer=f"https://www.tiktok.com/@{username}",
        )
        if response.get("failed"):
            if self._warmup_sec_uid:
                self._sec_uid = self._warmup_sec_uid
                self._sec_uid_source = "warmup_html"
                return self._warmup_sec_uid
            raise RuntimeError(f"TikTok user detail failed: {response.get('reason')}")
        payload = response.get("payload") or {}
        user_info = (payload.get("userInfo") or {}).get("user") or {}
        sec_uid = str(user_info.get("secUid") or "").strip()
        if not sec_uid and self._warmup_sec_uid:
            self._sec_uid = self._warmup_sec_uid
            self._sec_uid_source = "warmup_html"
            return self._warmup_sec_uid
        if not sec_uid:
            raise RuntimeError(f"TikTok secUid not found for @{username}")
        self._sec_uid = sec_uid
        self._sec_uid_source = "user_detail_api"
        return sec_uid

    async def fetch_posts_page(
        self,
        *,
        sec_uid: str,
        cursor: str | None = None,
        count: int | None = None,
    ) -> TikTokPostsFetchResult:
        """Fetch one page of posts via the post list API."""
        effective_count = count if count is not None else tiktok_posts_scrapling_page_size()
        params: dict[str, str] = {
            "secUid": sec_uid,
            "count": str(effective_count),
            "aid": "1988",
        }
        if cursor:
            params["cursor"] = cursor

        response = await self._fetch_api_json(
            TIKTOK_POST_LIST_URL,
            params=params,
            referer="https://www.tiktok.com/",
        )

        if response.get("failed"):
            return TikTokPostsFetchResult(
                fetch_failed=True,
                auth_failed=bool(response.get("auth_failed", False)),
                fetch_reason=response.get("reason"),
                request_count=self._request_count,
                retryable=bool(response.get("retryable", False)),
            )

        payload = response.get("payload") or {}
        items = payload.get("itemList") or []
        has_more = bool(payload.get("hasMore", False))
        next_cursor = str(payload.get("cursor") or "").strip() or None

        return TikTokPostsFetchResult(
            posts=list(items) if isinstance(items, list) else [],
            request_count=self._request_count,
            has_more=has_more,
            cursor=next_cursor,
        )

    async def aclose(self) -> None:
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
        new_cookies = _extract_response_cookies(response)
        self._warmup_cookie_delta = dict(new_cookies)
        self._raw_cookies = merge_response_cookies(self._raw_cookies, response)
        self._sync_browser_cookies(new_cookies)

    def _sync_browser_cookies(self, new_cookies: dict[str, str]) -> None:
        if not new_cookies:
            return
        by_name = {
            str(cookie.get("name") or "").strip(): dict(cookie)
            for cookie in self._cookies
            if isinstance(cookie, dict) and str(cookie.get("name") or "").strip()
        }
        for cookie in cookies_to_scrapling(new_cookies, domain=".tiktok.com"):
            by_name[str(cookie.get("name") or "").strip()] = cookie
        self._cookies = list(by_name.values())

    def _rebuild_http_client(self) -> None:
        if self._http_client is not None:
            self._http_client = None
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

    async def _fetch_page(self, url: str, *, referer: str) -> Any:
        self._request_count += 1
        fetch_kwargs = {
            **self._scrapling_fetcher_options.kwargs,
            "headless": self._headless,
            "network_idle": False,
            "load_dom": False,
            "cookies": self._cookies,
            "proxy_rotator": self._proxy_rotator,
            "extra_headers": _build_tiktok_headers(referer),
            "timeout": self._timeout_ms,
            "retries": 1,
            "retry_delay": 1.0,
        }
        if self._capture_xhr:
            fetch_kwargs["capture_xhr"] = True
        return await self._fetcher.async_fetch(url, **fetch_kwargs)

    # -------------------------------------------------------------------
    # Transport: httpx (API calls)
    # -------------------------------------------------------------------

    async def _fetch_api_get(self, url: str, *, params: dict[str, str], headers: dict[str, str]) -> httpx.Response:
        if self._http_client is None:
            self._rebuild_http_client()
        self._request_count += 1
        return await self._http_client.get(url, params=params, headers=headers)  # type: ignore[union-attr]

    # -------------------------------------------------------------------
    # JSON response handling with retry/backoff
    # (same pattern as Instagram fetcher, GET-based)
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

    async def _fetch_api_json(
        self,
        url: str,
        *,
        params: dict[str, str],
        referer: str,
    ) -> dict[str, Any]:
        """GET with bounded retry/backoff. Same pattern as Instagram fetcher."""
        headers = _build_tiktok_headers(referer)
        attempt = 0
        last_transient_reason: str | None = None
        while True:
            attempt += 1
            try:
                response = await self._fetch_api_get(url, params=params, headers=headers)
            except (TimeoutError, httpx.TimeoutException):
                last_transient_reason = "transport_timeout"
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                    }
                await asyncio.sleep(self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue

            status = _status_code(response)
            text = _response_text(response)

            # 3xx redirect -> classify
            if 300 <= status < 400:
                location = _safe_location(response)
                challenge_classification = (
                    "login_required"
                    if "/login" in location
                    else "captcha_or_challenge"
                    if ("/challenge" in location or "/captcha" in location)
                    else None
                )
                self._last_challenge_classification = challenge_classification
                reason = (
                    "redirect_to_login"
                    if "/login" in location
                    else "redirect_to_challenge"
                    if ("/challenge" in location or "/captcha" in location)
                    else "redirect"
                )
                return {
                    "failed": True,
                    "auth_failed": any(t in location for t in ("login", "challenge", "captcha")),
                    "reason": reason,
                    "retryable": False,
                    "challenge_classification": challenge_classification,
                    "payload": None,
                }

            # Transient 429/5xx
            if self._is_transient_status(status):
                last_transient_reason = f"http_{status}"
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": last_transient_reason,
                        "retryable": True,
                        "payload": None,
                    }
                retry_after = self._retry_after_seconds(response)
                sleep_seconds = (
                    retry_after if retry_after is not None else self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                )
                await asyncio.sleep(sleep_seconds)
                continue

            # Permanent 4xx
            if status >= 400:
                return {
                    "failed": True,
                    "auth_failed": status in {401, 403},
                    "reason": f"http_{status}",
                    "retryable": False,
                    "payload": None,
                }

            # HTML/challenge detection
            challenge_classification = _classify_challenge_response(text)
            if challenge_classification is not None:
                self._last_challenge_classification = challenge_classification
                return {
                    "failed": True,
                    "auth_failed": True,
                    "reason": "challenge_or_blocked",
                    "retryable": False,
                    "challenge_classification": challenge_classification,
                    "payload": None,
                }

            # Parse JSON
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001
                try:
                    payload = json.loads(text)
                except Exception:  # noqa: BLE001
                    return {
                        "failed": True,
                        "auth_failed": False,
                        "reason": "non_json_response",
                        "retryable": False,
                        "payload": None,
                    }

            # TikTok API status code (note: TikTok uses "statusCode" not "status")
            if isinstance(payload, dict):
                status_code_value = payload.get("statusCode")
                if status_code_value is not None and int(status_code_value) != 0:
                    status_msg = str(payload.get("statusMsg") or "").strip().lower()
                    challenge_classification = _classify_challenge_response(status_msg)
                    if challenge_classification is not None:
                        self._last_challenge_classification = challenge_classification
                    return {
                        "failed": True,
                        "auth_failed": any(
                            t in status_msg for t in ("login", "auth", "challenge", "captcha", "blocked")
                        ),
                        "reason": f"tiktok_status_{status_code_value}",
                        "retryable": False,
                        "challenge_classification": challenge_classification,
                        "payload": payload,
                    }

            return {
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
                "challenge_classification": None,
                "payload": payload,
            }
