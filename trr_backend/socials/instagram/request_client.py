from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import requests

from trr_backend.socials.instagram.constants import AUTH_FATAL_MESSAGES


class InstagramRequestFailure(RuntimeError):  # noqa: N818
    def __init__(
        self,
        error_code: str,
        *,
        status_code: int | None,
        retryable: bool,
        response_text: str | None = None,
        redirect_target: str | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.status_code = status_code
        self.retryable = retryable
        # Provider bodies and query strings may contain credentials or identifiers.
        self.response_text = None
        destination = urlsplit(redirect_target or "")
        self.redirect_target = (destination.hostname or "") + destination.path if redirect_target else None
        self.retry_after_seconds = retry_after_seconds
        self.next_attempt_at: datetime | None = None


class InstagramRequestClient:
    def __init__(self, *, session: requests.Session) -> None:
        self.session = session

    @staticmethod
    def _html_auth_failure_code(body_text: str) -> str | None:
        body_lower = str(body_text or "").lower()
        if "checkpoint_required" in body_lower or "checkpoint" in body_lower:
            return "checkpoint_required"
        if "challenge_required" in body_lower or "challenge" in body_lower or "two_factor" in body_lower:
            return "challenge_required"
        if "feedback_required" in body_lower or "feedback required" in body_lower:
            return "feedback_required"
        if "/accounts/login" in body_lower or "accounts/login" in body_lower:
            return "redirect_login"
        if "login_required" in body_lower:
            return "login_required"
        return None

    def _classify_response(self, response: requests.Response) -> None:
        headers = getattr(response, "headers", {}) or {}
        location = str(headers.get("location") or headers.get("Location") or "").strip()
        content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
        body_text = str(getattr(response, "text", "") or "")
        is_redirect = bool(getattr(response, "is_redirect", False))

        if is_redirect and "/accounts/login" in location:
            raise InstagramRequestFailure(
                "redirect_login",
                status_code=response.status_code,
                retryable=False,
                response_text=body_text,
                redirect_target=location,
            )

        if response.status_code == 200 and ("text/html" in content_type or body_text.lstrip().startswith("<")):
            html_error_code = self._html_auth_failure_code(body_text)
            if html_error_code:
                raise InstagramRequestFailure(
                    html_error_code,
                    status_code=response.status_code,
                    retryable=False,
                    response_text=body_text,
                    redirect_target=location or None,
                )

        if response.status_code == 429:
            retry_after = str(headers.get("Retry-After") or headers.get("retry-after") or "")
            try:
                retry_seconds = max(0.0, float(retry_after))
            except ValueError:
                try:
                    retry_seconds = max(0.0, (parsedate_to_datetime(retry_after) - datetime.now(UTC)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    retry_seconds = None
            raise InstagramRequestFailure(
                "rate_limited",
                status_code=429,
                retryable=True,
                response_text=body_text,
                retry_after_seconds=retry_seconds,
            )

        if response.status_code == 401:
            # Auth failure is terminal for a given cookie bundle; retrying
            # won't flip credentials back to valid and burns identity-pool
            # reputation. Caller triggers cookie refresh / identity rotation.
            raise InstagramRequestFailure(
                "unauthorized",
                status_code=401,
                retryable=False,
                response_text=body_text,
            )

        if response.status_code == 403:
            # 403 indicates account-level block (shadowban / restriction).
            # Retrying accelerates suppression; caller should retire the
            # identity and move on.
            raise InstagramRequestFailure(
                "forbidden",
                status_code=403,
                retryable=False,
                response_text=body_text,
            )

        if response.status_code == 400 and "application/json" in content_type:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            message = str(payload.get("message") or "").strip().lower()
            if message in AUTH_FATAL_MESSAGES:
                raise InstagramRequestFailure(
                    message,
                    status_code=400,
                    retryable=False,
                    response_text=body_text,
                )

        if 300 <= response.status_code < 400:
            code = self._html_auth_failure_code(location) or "endpoint_redirect"
            raise InstagramRequestFailure(
                code, status_code=response.status_code, retryable=False, redirect_target=location
            )

        if response.status_code >= 400:
            raise InstagramRequestFailure(
                "request_failed",
                status_code=response.status_code,
                retryable=response.status_code >= 500 or response.status_code == 408,
                response_text=body_text,
            )

    def get_json(
        self,
        url: str,
        *,
        query_type: str,
        headers: dict[str, str],
        cookies: dict[str, str],
        params: dict[str, Any],
        timeout: tuple[int, int] | float | None = None,
        sender: Callable[..., requests.Response] | None = None,
        proxies: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        del query_type
        request_fn = sender or self.session.get
        response = request_fn(
            url,
            headers=headers,
            cookies=cookies,
            params=params,
            timeout=timeout,
            proxies=proxies,
            allow_redirects=False,
        )
        self._classify_response(response)
        return self._json(response)

    @staticmethod
    def _json(response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise InstagramRequestFailure("parse_error", status_code=response.status_code, retryable=True) from exc
        if not isinstance(payload, dict):
            raise InstagramRequestFailure("parse_error", status_code=response.status_code, retryable=True)
        message = str(payload.get("message") or "").strip().lower()
        if message in AUTH_FATAL_MESSAGES:
            raise InstagramRequestFailure(message, status_code=response.status_code, retryable=False)
        return payload

    def post_form_json(
        self,
        url: str,
        *,
        query_type: str,
        headers: dict[str, str],
        cookies: dict[str, str],
        data: dict[str, Any],
        timeout: tuple[int, int] | float | None = None,
        sender: Callable[..., requests.Response] | None = None,
        proxies: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        del query_type
        request_fn = sender or self.session.post
        response = request_fn(
            url,
            headers=headers,
            cookies=cookies,
            data=data,
            timeout=timeout,
            proxies=proxies,
            allow_redirects=False,
        )
        self._classify_response(response)
        return self._json(response)

    def get_text(
        self,
        url: str,
        *,
        query_type: str,
        headers: dict[str, str],
        cookies: dict[str, str],
        timeout: tuple[int, int] | float | None = None,
        sender: Callable[..., requests.Response] | None = None,
        proxies: dict[str, str] | None = None,
    ) -> str:
        del query_type
        request_fn = sender or self.session.get
        response = request_fn(
            url,
            headers=headers,
            cookies=cookies,
            timeout=timeout,
            proxies=proxies,
            allow_redirects=False,
        )
        self._classify_response(response)
        return str(response.text or "")
