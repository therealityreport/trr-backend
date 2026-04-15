from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from trr_backend.socials.instagram.constants import COMMENT_REPLIES_URL, COMMENTS_URL
from trr_backend.socials.instagram.permalink_metadata import _shortcode_to_media_id
from trr_backend.socials.instagram.scraper import InstagramComment, InstagramScraper


def _env_truthy(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


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
    try:
        return int(getattr(response, "status", 0) or 0)
    except (TypeError, ValueError):
        return 0


@dataclass(slots=True)
class InstagramCommentsFetchResult:
    comments: list[InstagramComment] = field(default_factory=list)
    fetch_failed: bool = False
    auth_failed: bool = False
    fetch_reason: str | None = None
    request_count: int = 0
    # P1-5: True when the last failure was transient (429 / 5xx / transport
    # timeout). The job runner maps this into CommentsScraplingRuntimeError's
    # retryable flag so the queue requeues the job rather than marking it
    # terminally failed. False when the failure is permanent (auth, 4xx
    # validation, parse error).
    retryable: bool = False


class InstagramCommentsScraplingFetcher:
    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        browser_account_id: str | None,
        proxy_rotator: Any | None = None,
        headless: bool | None = None,
        timeout_ms: int = 45_000,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = dict(raw_cookies or {})
        self._browser_account_id = str(browser_account_id or "").strip() or None
        self._proxy_rotator = proxy_rotator
        self._headless = headless if headless is not None else _env_truthy("SOCIAL_INSTAGRAM_COMMENTS_HEADLESS", True)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._parser = InstagramScraper(cookies=self._raw_cookies, browser_account_id=self._browser_account_id)
        self._request_count = 0
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc
        self._fetcher = StealthyFetcher()

    async def warmup(self) -> None:
        response = await self._fetch(
            "https://www.instagram.com/",
            referer="https://www.instagram.com/",
            capture_xhr=False,
        )
        text = _response_text(response)
        if _status_code(response) in {401, 403} or _auth_failure_text(text):
            raise RuntimeError("Instagram auth warm-up failed; session appears logged out or challenged.")

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
        retryable = False  # P1-5: propagate transient-failure signal.

        while True:
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
            fetch_reason = response.get("reason")
            fetch_failed = bool(response.get("failed"))
            auth_failed = bool(response.get("auth_failed"))
            retryable = bool(response.get("retryable"))
            if fetch_failed or not isinstance(payload, (dict, list)):
                break

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
            if not has_more or not next_cursor or next_cursor == cursor:
                break
            cursor = str(next_cursor)

        return InstagramCommentsFetchResult(
            comments=comments,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            request_count=self._request_count,
            retryable=retryable,
        )

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
        retryable = False  # P1-5

        while True:
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
            if not next_cursor or next_cursor == cursor:
                break
            cursor = str(next_cursor)

        return InstagramCommentsFetchResult(
            comments=replies,
            fetch_failed=fetch_failed,
            auth_failed=auth_failed,
            fetch_reason=fetch_reason,
            request_count=self._request_count,
            retryable=retryable,
        )

    # P1-5: transient-error retry policy.
    _MAX_TRANSIENT_RETRIES: int = 3
    _BASE_BACKOFF_SECONDS: float = 1.0

    @staticmethod
    def _is_transient_status(status_code: int) -> bool:
        """HTTP status codes that warrant retry with backoff."""
        return status_code == 429 or (500 <= status_code < 600)

    @staticmethod
    def _retry_after_seconds(response: Any) -> float | None:
        """Honor Retry-After when Instagram returns one. Falls back to None."""
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
        """Perform a JSON fetch with bounded exponential backoff on transient
        failures (429 / 5xx / transport timeout). Returns a result dict with
        a `retryable` flag so callers can surface the right queue semantics.
        """
        request_url = url
        if params:
            request_url = f"{url}?{urlencode({key: value for key, value in params.items() if value is not None})}"

        attempt = 0
        last_transient_reason: str | None = None
        while True:
            attempt += 1
            try:
                response = await self._fetch(
                    request_url,
                    referer=referer,
                    capture_xhr=False,
                )
            except TimeoutError:
                # Transport timeout — retry with backoff.
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

            if self._is_transient_status(status_code):
                # Transient HTTP status: 429 or 5xx. Retry with exponential
                # backoff, respecting Retry-After if present.
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

            if status_code >= 400:
                # Permanent 4xx (not 429): auth failure or validation error.
                # Never retryable — retrying would burn proxy budget and
                # likely get us further flagged.
                return {
                    "failed": True,
                    "auth_failed": auth_failed,
                    "reason": f"http_{status_code}",
                    "retryable": False,
                    "payload": None,
                }
            if text and text.lstrip().startswith("<"):
                return {
                    "failed": True,
                    "auth_failed": auth_failed or _auth_failure_text(text),
                    "reason": "html_challenge_or_auth_required",
                    "retryable": False,
                    "payload": None,
                }
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

    async def _fetch(
        self,
        url: str,
        *,
        referer: str,
        capture_xhr: bool,
    ) -> Any:
        self._request_count += 1
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=False,
            cookies=self._cookies,
            proxy_rotator=self._proxy_rotator,
            extra_headers=self._parser._get_headers(referer),
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
            capture_xhr=capture_xhr,
        )


def fetch_comments_for_shortcode_sync(
    *,
    fetcher: InstagramCommentsScraplingFetcher,
    shortcode: str,
    max_comments: int,
    fetch_replies: bool,
) -> InstagramCommentsFetchResult:
    return asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            shortcode,
            max_comments=max_comments,
            fetch_replies=fetch_replies,
        )
    )
