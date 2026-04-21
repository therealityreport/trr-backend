"""Hybrid fetcher for the Instagram comments Scrapling lane.

Architecture:
  warmup()  →  _fetch_page()  →  StealthyFetcher (Patchright browser)
  comments  →  _fetch_api()   →  httpx.AsyncClient (plain HTTP + XHR headers)

The browser handles session establishment and challenge solving. All JSON
API calls go through httpx with the cookies bridged from warmup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

from trr_backend.socials.instagram.comments_scrapling.proxy import CommentsProxyConfig
from trr_backend.socials.instagram.constants import COMMENT_REPLIES_URL, COMMENTS_URL
from trr_backend.socials.instagram.permalink_metadata import _shortcode_to_media_id
from trr_backend.socials.instagram.scraper import InstagramComment, InstagramScraper

logger = logging.getLogger("socials.instagram.comments_scrapling.fetcher")

_COMMENT_PAGINATION_MAX_PAGES_DEFAULT = 25
_REPLY_PAGINATION_MAX_PAGES_DEFAULT = 10
_COMMENT_PAGINATION_MAX_SECONDS_DEFAULT = 30.0
_REPLY_PAGINATION_MAX_SECONDS_DEFAULT = 20.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_API_HEADER_KEYS_TO_STRIP = frozenset(
    {
        "x-requested-with",
        "x-ig-app-id",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
    }
)


def _env_truthy(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _resolve_positive_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _resolve_positive_float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _auth_failure_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    return any(token in normalized for token in ("login", "checkpoint", "challenge", "accounts/login"))


def _response_text(response: Any) -> str:
    text = getattr(response, "text", "")
    if callable(text):
        try:
            return str(text() or "")
        except Exception:  # noqa: BLE001
            return ""
    return str(text or "")


def _status_code(response: Any) -> int:
    """Read status from both httpx (.status_code) and Scrapling (.status)."""
    raw = getattr(response, "status_code", getattr(response, "status", 0))
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def _safe_location(response: Any) -> str:
    """Extract sanitized Location header path (no query string, no credentials)."""
    headers = getattr(response, "headers", None) or {}
    raw = ""
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
    """Pull cookies from a Scrapling/httpx response object."""
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
class InstagramCommentsFetchResult:
    comments: list[InstagramComment] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    fetch_reason: str | None = None
    request_count: int = 0
    retryable: bool = False


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class InstagramCommentsScraplingFetcher:
    """Hybrid fetcher: Patchright for warmup, httpx for API calls."""

    # Retry policy for transient errors (429 / 5xx / transport timeout).
    _MAX_TRANSIENT_RETRIES: int = 3
    _BASE_BACKOFF_SECONDS: float = 1.0

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        browser_account_id: str | None,
        proxy_config: CommentsProxyConfig | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = raw_cookies if isinstance(raw_cookies, dict) else dict(raw_cookies or {})
        self._browser_account_id = str(browser_account_id or "").strip() or None
        self._proxy_config = proxy_config
        self._proxy_rotator = proxy_config.proxy_rotator if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_INSTAGRAM_COMMENTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._parser = InstagramScraper(cookies=self._raw_cookies, browser_account_id=self._browser_account_id)
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._selected_proxy_fingerprint: str = proxy_config.fingerprint if proxy_config else "none"

        # Browser fetcher (for warmup only).
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc
        self._fetcher = StealthyFetcher()

        # httpx client (for API calls). Created lazily after warmup bridges cookies.
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
            "transport": "httpx_after_browser_warmup",
            "request_count": self._request_count,
        }

    async def warmup(self) -> None:
        """Navigate to instagram.com via Patchright to establish the session,
        solve challenges, and bridge cookies into the httpx client."""
        response = await self._fetch_page(
            "https://www.instagram.com/",
            referer="https://www.instagram.com/",
        )
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            raise RuntimeError("Instagram auth warm-up failed; session appears logged out or challenged.")
        self._merge_warmup_cookies(response)
        await self._rebuild_http_client()

    async def fetch_comments_for_shortcode(
        self,
        shortcode: str,
        *,
        max_comments: int,
        fetch_replies: bool,
    ) -> InstagramCommentsFetchResult:
        try:
            media_id = _shortcode_to_media_id(shortcode)
        except Exception as exc:  # noqa: BLE001
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason=f"invalid_shortcode:{exc.__class__.__name__}",
                request_count=self._request_count,
            )

        post_url = f"https://www.instagram.com/p/{shortcode}/"
        comments: list[InstagramComment] = []
        cursor: str | None = None
        comments_fetched = 0
        fetch_failed = False
        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        pages_seen = 0
        seen_cursors: set[str] = set()
        deadline = time.monotonic() + _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_SECONDS",
            _COMMENT_PAGINATION_MAX_SECONDS_DEFAULT,
            minimum=1.0,
            maximum=300.0,
        )
        page_cap = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES",
            _COMMENT_PAGINATION_MAX_PAGES_DEFAULT,
            minimum=1,
            maximum=250,
        )

        while True:
            if time.monotonic() >= deadline:
                fetch_failed = True
                fetch_reason = "pagination_deadline_exceeded"
                retryable = True
                logger.warning("Instagram comments pagination deadline exceeded for shortcode=%s", shortcode)
                break
            response = await self._fetch_json_response(
                COMMENTS_URL.format(media_id=media_id),
                referer=post_url,
                params={
                    "can_support_threading": "true",
                    "permalink_enabled": "false",
                    **({"min_id": cursor} if cursor else {}),
                },
            )
            payload = response.get("payload")
            page_fetch_reason = response.get("reason")
            page_fetch_failed = bool(response.get("failed"))
            page_auth_failed = bool(response.get("auth_failed"))
            page_retryable = bool(response.get("retryable"))
            fetch_failed = fetch_failed or page_fetch_failed
            auth_failed = auth_failed or page_auth_failed
            retryable = retryable or page_retryable
            if page_fetch_reason and not fetch_reason:
                fetch_reason = page_fetch_reason
            if page_fetch_failed or not isinstance(payload, (dict, list)):
                break
            pages_seen += 1

            comment_rows = payload if isinstance(payload, list) else list(payload.get("comments") or [])
            for comment_data in comment_rows:
                if not isinstance(comment_data, dict):
                    continue
                comment = self._parser._parse_comment(comment_data, shortcode, post_url)
                if fetch_replies and comment.reply_count > 0 and not comment.replies:
                    replies_result = await self._fetch_comment_replies(
                        media_id=media_id,
                        comment_id=comment.comment_id,
                        shortcode=shortcode,
                        post_url=post_url,
                    )
                    comment.replies = replies_result.comments
                    fetch_failed = fetch_failed or replies_result.fetch_failed
                    auth_failed = auth_failed or replies_result.auth_failed
                    retryable = retryable or replies_result.retryable
                    if replies_result.fetch_reason and not fetch_reason:
                        fetch_reason = replies_result.fetch_reason
                comments.append(comment)
                comments_fetched += 1
                if max_comments > 0 and comments_fetched >= max_comments:
                    break

            if max_comments > 0 and comments_fetched >= max_comments:
                break
            if not isinstance(payload, dict):
                break
            has_more = bool(payload.get("has_more_comments", False)) or bool(payload.get("has_more_headload_comments"))
            next_cursor = payload.get("next_min_id") or payload.get("next_max_id")
            if not has_more or not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor == cursor or next_cursor in seen_cursors:
                fetch_failed = True
                fetch_reason = "pagination_repeated_cursor"
                logger.warning(
                    "Instagram comments pagination repeated cursor for shortcode=%s cursor=%s",
                    shortcode,
                    next_cursor,
                )
                break
            if pages_seen >= page_cap:
                fetch_failed = True
                fetch_reason = "pagination_page_cap_reached"
                logger.warning(
                    "Instagram comments pagination page cap reached for shortcode=%s page_cap=%d",
                    shortcode,
                    page_cap,
                )
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return InstagramCommentsFetchResult(
            comments=comments,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            request_count=self._request_count,
            retryable=retryable,
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
    # Reply fetching
    # -------------------------------------------------------------------

    async def _fetch_comment_replies(
        self,
        *,
        media_id: str,
        comment_id: str,
        shortcode: str,
        post_url: str,
    ) -> InstagramCommentsFetchResult:
        replies: list[InstagramComment] = []
        cursor: str | None = None
        fetch_failed = False
        auth_failed = False
        fetch_reason: str | None = None
        retryable = False
        pages_seen = 0
        seen_cursors: set[str] = set()
        deadline = time.monotonic() + _resolve_positive_float_env(
            "SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_SECONDS",
            _REPLY_PAGINATION_MAX_SECONDS_DEFAULT,
            minimum=1.0,
            maximum=300.0,
        )
        page_cap = _resolve_positive_int_env(
            "SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_PAGES",
            _REPLY_PAGINATION_MAX_PAGES_DEFAULT,
            minimum=1,
            maximum=250,
        )

        while True:
            if time.monotonic() >= deadline:
                fetch_failed = True
                fetch_reason = "pagination_deadline_exceeded"
                retryable = True
                logger.warning("Instagram reply pagination deadline exceeded for comment_id=%s", comment_id)
                break
            response = await self._fetch_json_response(
                COMMENT_REPLIES_URL.format(media_id=media_id, comment_id=comment_id),
                referer=post_url,
                params={"min_id": cursor} if cursor else None,
            )
            payload = response.get("payload")
            fetch_reason = response.get("reason")
            fetch_failed = bool(response.get("failed"))
            auth_failed = bool(response.get("auth_failed"))
            retryable = retryable or bool(response.get("retryable"))
            if fetch_failed or not isinstance(payload, (dict, list)):
                break
            pages_seen += 1

            if isinstance(payload, dict):
                reply_rows = payload.get("child_comments") or payload.get("replies") or []
            else:
                reply_rows = payload
            for reply_data in reply_rows:
                if not isinstance(reply_data, dict):
                    continue
                replies.append(
                    self._parser._parse_comment(
                        reply_data,
                        shortcode,
                        post_url,
                        is_reply=True,
                        parent_id=comment_id,
                    )
                )

            if not isinstance(payload, dict):
                break
            if not bool(payload.get("has_more_tail_child_comments", False)):
                break
            next_cursor = payload.get("next_min_child_cursor")
            if not next_cursor:
                break
            next_cursor = str(next_cursor)
            if next_cursor == cursor or next_cursor in seen_cursors:
                fetch_failed = True
                fetch_reason = "pagination_repeated_cursor"
                logger.warning(
                    "Instagram reply pagination repeated cursor for comment_id=%s cursor=%s",
                    comment_id,
                    next_cursor,
                )
                break
            if pages_seen >= page_cap:
                fetch_failed = True
                fetch_reason = "pagination_page_cap_reached"
                logger.warning(
                    "Instagram reply pagination page cap reached for comment_id=%s page_cap=%d",
                    comment_id,
                    page_cap,
                )
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return InstagramCommentsFetchResult(
            comments=replies,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            request_count=self._request_count,
            retryable=retryable,
        )

    # -------------------------------------------------------------------
    # Cookie bridge
    # -------------------------------------------------------------------

    def _merge_warmup_cookies(self, response: Any) -> None:
        """In-place mutation of self._raw_cookies + parser sync.

        Critical: must NOT rebind self._raw_cookies — the parser holds a
        shared reference from __init__. In-place update ensures the parser
        sees updated csrftoken when _get_headers() runs for API calls.
        """
        new_cookies = _extract_response_cookies(response)
        self._warmup_cookie_delta = dict(new_cookies)
        for name, value in new_cookies.items():
            self._raw_cookies[name] = value
            if hasattr(self._parser, "cookies") and isinstance(self._parser.cookies, dict):
                self._parser.cookies[name] = value
            if hasattr(self._parser, "session") and hasattr(self._parser.session, "cookies"):
                try:
                    self._parser.session.cookies.set(name, value)
                except Exception:  # noqa: BLE001
                    pass

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
        Strips API-specific headers (x-ig-app-id, x-requested-with, sec-fetch-*)
        that don't belong on document navigation.
        """
        self._request_count += 1
        all_headers = self._parser._get_headers(referer)
        nav_headers = {k: v for k, v in all_headers.items() if k.lower() not in _API_HEADER_KEYS_TO_STRIP}
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=nav_headers,
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    # -------------------------------------------------------------------
    # Transport: httpx (API calls)
    # -------------------------------------------------------------------

    async def _fetch_api(
        self,
        url: str,
        *,
        referer: str,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Plain HTTP GET via httpx. Used for comments/replies JSON API calls."""
        if self._http_client is None:
            await self._rebuild_http_client()
        self._request_count += 1
        headers = self._parser._get_headers(referer)
        clean_params = {k: v for k, v in (params or {}).items() if v is not None} or None
        return await self._http_client.get(url, params=clean_params, headers=headers)  # type: ignore[union-attr]

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
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """JSON fetch via httpx with bounded exponential backoff on transient
        failures (429 / 5xx / transport timeout).
        """
        attempt = 0
        last_transient_reason: str | None = None
        while True:
            attempt += 1
            try:
                response = await self._fetch_api(url, referer=referer, params=params)
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
                    "Instagram API redirected (%d) to %s — reason=%s",
                    status_code,
                    location,
                    reason,
                )
                return {
                    "failed": True,
                    "auth_failed": any(token in location for token in ("login", "challenge", "checkpoint")),
                    "reason": reason,
                    "retryable": False,
                    "payload": None,
                }

            # Transient 429 / 5xx: retry with backoff.
            if self._is_transient_status(status_code):
                last_transient_reason = f"http_{status_code}"
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
