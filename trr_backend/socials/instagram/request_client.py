from __future__ import annotations

from collections.abc import Callable
from typing import Any

import requests

from trr_backend.socials.instagram.constants import AUTH_FATAL_MESSAGES


class InstagramRequestFailure(RuntimeError):
    def __init__(
        self,
        error_code: str,
        *,
        status_code: int | None,
        retryable: bool,
        response_text: str | None = None,
        redirect_target: str | None = None,
    ) -> None:
        super().__init__(error_code)
        self.error_code = error_code
        self.status_code = status_code
        self.retryable = retryable
        self.response_text = response_text
        self.redirect_target = redirect_target


class InstagramRequestClient:
    def __init__(self, *, session: requests.Session) -> None:
        self.session = session

    def _classify_response(self, response: requests.Response) -> None:
        headers = getattr(response, "headers", {}) or {}
        location = str(headers.get("location") or "").strip()
        content_type = str(headers.get("content-type") or "").lower()
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

        if response.status_code == 429:
            raise InstagramRequestFailure(
                "rate_limited",
                status_code=429,
                retryable=True,
                response_text=body_text,
            )

        if response.status_code == 401:
            raise InstagramRequestFailure(
                "unauthorized",
                status_code=401,
                retryable=True,
                response_text=body_text,
            )

        if response.status_code == 403:
            raise InstagramRequestFailure(
                "forbidden",
                status_code=403,
                retryable=True,
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

        if response.status_code >= 400:
            raise InstagramRequestFailure(
                "request_failed",
                status_code=response.status_code,
                retryable=True,
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
        return response.json()

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
        return response.json()

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
