"""Pluggable async HTTP transport for the Instagram comments lane.

The comments fetcher historically built ``httpx.AsyncClient`` instances inline.
This factory lets an operator opt into a ``curl_cffi`` transport (TLS/JA3
browser impersonation) for the logged-out public-relay lane via
``SOCIAL_INSTAGRAM_COMMENTS_HTTP_CLIENT=curl_cffi`` without changing call sites.

Design notes:
- Default is ``httpx`` so behavior is byte-for-byte unchanged unless the flag is
  set.
- Responses from either transport are consumed through the fetcher's duck-typed
  response helpers (``status_code``/``text``/``content``/``headers``/``cookies``),
  so the downstream parsing path is transport-agnostic.
- ``curl_cffi`` is imported lazily; if it is requested but not installed, the
  factory logs a warning and falls back to ``httpx`` so a mis-set flag can never
  take the lane down.

Reuses the lazy-import + dynamic-exception-discovery pattern from the TikTok
``curl_cffi`` client (``trr_backend/socials/tiktok/http_client.py``).
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger("socials.instagram.comments_scrapling.async_http_client")

HTTP_CLIENT_ENV = "SOCIAL_INSTAGRAM_COMMENTS_HTTP_CLIENT"
_HTTPX = "httpx"
_CURL_CFFI = "curl_cffi"
_VALID_CLIENTS = frozenset({_HTTPX, _CURL_CFFI})
_DEFAULT_CURL_IMPERSONATE = "chrome"


def resolve_comments_http_client_name() -> str:
    """Resolve the configured transport; defaults to ``httpx`` on unset/garbage."""
    value = str(os.getenv(HTTP_CLIENT_ENV) or "").strip().lower()
    return value if value in _VALID_CLIENTS else _HTTPX


def _curl_cffi_requests() -> Any | None:
    try:
        return importlib.import_module("curl_cffi.requests")
    except Exception:  # noqa: BLE001 - ImportError or transitive import failure
        return None


def curl_cffi_available() -> bool:
    return _curl_cffi_requests() is not None


def _curl_cffi_exception_types() -> tuple[type[BaseException], ...]:
    mod = _curl_cffi_requests()
    if mod is None:
        return ()
    exceptions_mod = getattr(mod, "exceptions", None)
    request_error = getattr(exceptions_mod, "RequestException", None)
    return (request_error,) if isinstance(request_error, type) else ()


# Transport-failure exception tuple covering httpx and (when installed) curl_cffi.
# Catch sites that previously hardcoded the httpx tuple can widen to this.
TRANSPORT_EXC_TYPES: tuple[type[BaseException], ...] = (
    TimeoutError,
    httpx.TimeoutException,
    httpx.TransportError,
    httpx.DecodingError,
    OSError,
    *_curl_cffi_exception_types(),
)


def _timeout_seconds(timeout: Any, default: float = 30.0) -> float:
    """Coerce an httpx.Timeout / number / None into a single float of seconds."""
    if timeout is None:
        return default
    if isinstance(timeout, (int, float)):
        return float(timeout)
    # httpx.Timeout exposes per-phase floats; pick the read/connect ceiling.
    for attr in ("read", "connect", "pool", "write"):
        value = getattr(timeout, attr, None)
        if value:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return default


class _CurlCffiAsyncClient:
    """Async-context adapter over ``curl_cffi.requests.AsyncSession`` mirroring the
    subset of the ``httpx.AsyncClient`` interface the comments lane uses (async
    ``get``/``post`` returning a duck-typed response)."""

    def __init__(
        self,
        *,
        cookies: dict[str, str] | None,
        timeout: Any,
        proxy: str | None,
        follow_redirects: bool,
        headers: dict[str, str] | None,
        impersonate: str,
    ) -> None:
        requests_mod = _curl_cffi_requests()
        if requests_mod is None:  # pragma: no cover - guarded by factory
            raise RuntimeError(
                "curl_cffi transport requested but the dependency is not installed. "
                "Add curl_cffi to requirements and reinstall dependencies."
            )
        self._timeout_default = _timeout_seconds(timeout)
        self._allow_redirects = bool(follow_redirects)
        session_kwargs: dict[str, Any] = {
            "headers": dict(headers or {}),
            "timeout": self._timeout_default,
            "impersonate": impersonate,
        }
        if cookies:
            session_kwargs["cookies"] = dict(cookies)
        if proxy:
            session_kwargs["proxies"] = {"http": proxy, "https": proxy}
        self._session = requests_mod.AsyncSession(**session_kwargs)

    async def __aenter__(self) -> _CurlCffiAsyncClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        close = getattr(self._session, "close", None)
        if close is None:
            return
        try:
            result = close()
            if hasattr(result, "__await__"):
                await result
        except Exception:  # noqa: BLE001
            pass

    async def get(self, url: str, *, headers: Any = None, timeout: Any = None, **kwargs: Any) -> Any:
        return await self._session.get(
            url,
            headers=dict(headers or {}),
            timeout=_timeout_seconds(timeout, self._timeout_default),
            allow_redirects=self._allow_redirects,
            **kwargs,
        )

    async def post(self, url: str, *, data: Any = None, headers: Any = None, timeout: Any = None, **kwargs: Any) -> Any:
        return await self._session.post(
            url,
            data=data,
            headers=dict(headers or {}),
            timeout=_timeout_seconds(timeout, self._timeout_default),
            allow_redirects=self._allow_redirects,
            **kwargs,
        )


def build_comments_async_client(
    *,
    client_name: str | None = None,
    cookies: dict[str, str] | None = None,
    timeout: Any = None,
    proxy: str | None = None,
    follow_redirects: bool = False,
    trust_env: bool = False,
    headers: dict[str, str] | None = None,
    impersonate: str | None = None,
) -> Any:
    """Build an async HTTP client for the comments lane.

    Returns an ``httpx.AsyncClient`` (default) or a curl_cffi-backed adapter,
    both usable as ``async with ... as client``. Falls back to httpx (with a
    warning) when curl_cffi is requested but unavailable, so a mis-set flag is
    never fatal.
    """
    name = (client_name or resolve_comments_http_client_name()).strip().lower()
    if name == _CURL_CFFI:
        if curl_cffi_available():
            return _CurlCffiAsyncClient(
                cookies=cookies,
                timeout=timeout,
                proxy=proxy,
                follow_redirects=follow_redirects,
                headers=headers,
                impersonate=impersonate or _DEFAULT_CURL_IMPERSONATE,
            )
        logger.warning(
            "[comments-http] curl_cffi transport requested via %s but the dependency "
            "is not installed; falling back to httpx.",
            HTTP_CLIENT_ENV,
        )
    # httpx path: only pass kwargs that were provided so the constructed client is
    # identical to the prior inline httpx.AsyncClient(...) construction.
    httpx_kwargs: dict[str, Any] = {
        "follow_redirects": follow_redirects,
        "trust_env": trust_env,
    }
    if cookies is not None:
        httpx_kwargs["cookies"] = cookies
    if timeout is not None:
        httpx_kwargs["timeout"] = timeout
    if proxy is not None:
        httpx_kwargs["proxy"] = proxy
    if headers is not None:
        httpx_kwargs["headers"] = headers
    return httpx.AsyncClient(**httpx_kwargs)
