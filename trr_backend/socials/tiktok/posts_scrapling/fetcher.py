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
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

from trr_backend.socials.tiktok.posts_scrapling.proxy import TikTokPostsProxyConfig

logger = logging.getLogger("socials.tiktok.posts_scrapling.fetcher")

TIKTOK_USER_DETAIL_URL = "https://www.tiktok.com/api/user/detail/"
TIKTOK_POST_LIST_URL = "https://www.tiktok.com/api/post/item_list/"
TIKTOK_POST_PAGE_SIZE = 30


# ---------------------------------------------------------------------------
# Pure helpers (copy pattern from Instagram fetcher)
# ---------------------------------------------------------------------------


def _env_truthy(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


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


def _is_challenge_response(text: str) -> bool:
    body = str(text or "").strip().lower()[:512]
    return any(token in body for token in ("<html", "captcha", "verify", "challenge"))


def _response_text(response: Any) -> str:
    text = getattr(response, "text", "")
    if callable(text):
        try:
            return str(text() or "")
        except Exception:  # noqa: BLE001
            return ""
    return str(text or "")


def _status_code(response: Any) -> int:
    raw = getattr(response, "status_code", getattr(response, "status", 0))
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def _safe_location(response: Any) -> str:
    headers = getattr(response, "headers", None) or {}
    try:
        raw = str(headers.get("location") or headers.get("Location") or "")
    except Exception:  # noqa: BLE001
        return ""
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        return str(parsed.path or "/").lower()
    except Exception:  # noqa: BLE001
        return raw.split("?")[0].lower()


def _extract_response_cookies(response: Any) -> dict[str, str]:
    cookies_attr = getattr(response, "cookies", None)
    if cookies_attr is None:
        return {}
    result: dict[str, str] = {}
    try:
        if isinstance(cookies_attr, dict):
            for k, v in cookies_attr.items():
                result[str(k)] = str(v)
        elif hasattr(cookies_attr, "items"):
            for k, v in cookies_attr.items():
                result[str(k)] = str(v)
        elif hasattr(cookies_attr, "jar"):
            for cookie in cookies_attr.jar:
                result[str(cookie.name)] = str(cookie.value)
    except Exception:  # noqa: BLE001
        pass
    return result


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
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"
        self._sec_uid: str | None = None

        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc
        self._fetcher = StealthyFetcher()
        self._http_client: httpx.AsyncClient | None = None

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        return {
            "warmup_cookie_delta": dict(self._warmup_cookie_delta),
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "sec_uid_resolved": bool(self._sec_uid),
            "request_count": self._request_count,
            "transport": "httpx_after_browser_warmup",
        }

    async def warmup(self, username: str) -> None:
        """Navigate to profile via Patchright to establish session."""
        profile_url = f"https://www.tiktok.com/@{username}"
        response = await self._fetch_page(profile_url, referer="https://www.tiktok.com/")
        text = _response_text(response)
        status = _status_code(response)
        if (status in {401, 403}) or (_is_challenge_response(text) and status not in range(200, 300)):
            raise RuntimeError("TikTok warmup hit challenge page or auth failure; cookies may be invalid.")
        self._merge_warmup_cookies(response)
        self._rebuild_http_client()

    async def resolve_sec_uid(self, username: str) -> str:
        """Fetch secUid via user detail API. Required for post pagination."""
        response = await self._fetch_api_json(
            TIKTOK_USER_DETAIL_URL,
            params={"uniqueId": username},
            referer=f"https://www.tiktok.com/@{username}",
        )
        if response.get("failed"):
            raise RuntimeError(f"TikTok user detail failed: {response.get('reason')}")
        payload = response.get("payload") or {}
        user_info = (payload.get("userInfo") or {}).get("user") or {}
        sec_uid = str(user_info.get("secUid") or "").strip()
        if not sec_uid:
            raise RuntimeError(f"TikTok secUid not found for @{username}")
        self._sec_uid = sec_uid
        return sec_uid

    async def fetch_posts_page(
        self,
        *,
        sec_uid: str,
        cursor: str | None = None,
        count: int = TIKTOK_POST_PAGE_SIZE,
    ) -> TikTokPostsFetchResult:
        """Fetch one page of posts via the post list API."""
        params: dict[str, str] = {
            "secUid": sec_uid,
            "count": str(count),
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
        for name, value in new_cookies.items():
            self._raw_cookies[name] = value

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
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=_build_tiktok_headers(referer),
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

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
            if _is_challenge_response(text):
                return {
                    "failed": True,
                    "auth_failed": True,
                    "reason": "challenge_or_blocked",
                    "retryable": False,
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
                    return {
                        "failed": True,
                        "auth_failed": any(
                            t in status_msg for t in ("login", "auth", "challenge", "captcha", "blocked")
                        ),
                        "reason": f"tiktok_status_{status_code_value}",
                        "retryable": False,
                        "payload": payload,
                    }

            return {"failed": False, "auth_failed": False, "reason": None, "retryable": False, "payload": payload}
