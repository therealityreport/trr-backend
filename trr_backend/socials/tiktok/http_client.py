"""TikTok-specific HTTP client abstraction for transport experiments.

Direct TikTok HTTP scraping is no longer the production-default path. Keep this
transport layer available for explicit experiments only while `yt-dlp` remains
the active posts path.
"""

from __future__ import annotations

import importlib
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

DEFAULT_HTTP_CLIENT = "requests"
DEFAULT_CURL_CFFI_IMPERSONATE = "chrome"
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class TikTokHttpResponse(Protocol):
    status_code: int
    headers: Any
    text: str
    content: bytes

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...


class TikTokHttpRequestError(RuntimeError):
    """Transport-agnostic request failure."""

    def __init__(self, message: str, *, response: Any | None = None) -> None:
        super().__init__(message)
        self.response = response


@dataclass(frozen=True)
class _ClientConfig:
    retry_total: int = 3
    backoff_factor: float = 1.5
    proxy_url: str | None = None
    impersonate: str | None = None


def _proxy_label(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(str(proxy_url))
    return parsed.hostname or str(proxy_url)


class _TikTokHttpClientBase:
    client_name = ""
    request_exception_types: tuple[type[BaseException], ...] = (Exception,)

    def __init__(self, *, config: _ClientConfig) -> None:
        self.retry_total = max(1, int(config.retry_total))
        self.backoff_factor = max(0.0, float(config.backoff_factor))
        self.proxy_url = str(config.proxy_url or "").strip() or None
        self.proxy_label = _proxy_label(self.proxy_url)
        self.impersonate = str(config.impersonate or "").strip() or None

    def _sleep_before_retry(self, attempt: int) -> None:
        if attempt >= self.retry_total:
            return
        delay = self.backoff_factor * attempt
        if delay > 0:
            time.sleep(delay)

    def _perform_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None,
        params: dict[str, Any] | None,
        cookies: dict[str, str] | None,
        timeout: float | tuple[float, float] | tuple[int, int] | int,
    ) -> TikTokHttpResponse:
        raise NotImplementedError

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        cookies: dict[str, str] | None = None,
        timeout: float | tuple[float, float] | tuple[int, int] | int = 30,
    ) -> TikTokHttpResponse:
        last_error: BaseException | None = None
        for attempt in range(1, self.retry_total + 1):
            try:
                response = self._perform_get(
                    url,
                    headers=headers,
                    params=params,
                    cookies=cookies,
                    timeout=timeout,
                )
                if (
                    int(getattr(response, "status_code", 0) or 0) in RETRYABLE_STATUS_CODES
                    and attempt < self.retry_total
                ):
                    self._sleep_before_retry(attempt)
                    continue
                response.raise_for_status()
                return response
            except self.request_exception_types as exc:
                last_error = exc
                response = getattr(exc, "response", None)
                if (
                    int(getattr(response, "status_code", 0) or 0) in RETRYABLE_STATUS_CODES
                    and attempt < self.retry_total
                ):
                    self._sleep_before_retry(attempt)
                    continue
                if attempt < self.retry_total and response is None:
                    self._sleep_before_retry(attempt)
                    continue
                raise TikTokHttpRequestError(
                    f"tiktok_http_get_failed:{self.client_name}:{type(exc).__name__}",
                    response=response,
                ) from exc
        raise TikTokHttpRequestError(
            f"tiktok_http_get_failed:{self.client_name}:{type(last_error).__name__ if last_error else 'unknown'}",
            response=getattr(last_error, "response", None),
        )


class RequestsTikTokHttpClient(_TikTokHttpClientBase):
    client_name = "requests"
    request_exception_types = (requests.exceptions.RequestException,)

    def __init__(self, *, config: _ClientConfig) -> None:
        super().__init__(config=config)
        self._session = requests.Session()

    def _perform_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None,
        params: dict[str, Any] | None,
        cookies: dict[str, str] | None,
        timeout: float | tuple[float, float] | tuple[int, int] | int,
    ) -> TikTokHttpResponse:
        proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
        return self._session.get(
            url,
            headers=headers,
            params=params,
            cookies=cookies,
            timeout=timeout,
            proxies=proxies,
        )


class CurlCffiTikTokHttpClient(_TikTokHttpClientBase):
    client_name = "curl_cffi"

    def __init__(self, *, config: _ClientConfig) -> None:
        if not config.impersonate:
            config = _ClientConfig(
                retry_total=config.retry_total,
                backoff_factor=config.backoff_factor,
                proxy_url=config.proxy_url,
                impersonate=DEFAULT_CURL_CFFI_IMPERSONATE,
            )
        super().__init__(config=config)
        try:
            curl_requests = importlib.import_module("curl_cffi.requests")
        except ImportError as exc:
            raise RuntimeError(
                "curl_cffi transport requested but dependency is not installed. "
                "Add curl_cffi to requirements and reinstall dependencies."
            ) from exc
        self._session = curl_requests.Session()
        exceptions_mod = getattr(curl_requests, "exceptions", None)
        request_error = getattr(exceptions_mod, "RequestException", Exception)
        self.request_exception_types = (request_error,) if isinstance(request_error, type) else (Exception,)

    def _perform_get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None,
        params: dict[str, Any] | None,
        cookies: dict[str, str] | None,
        timeout: float | tuple[float, float] | tuple[int, int] | int,
    ) -> TikTokHttpResponse:
        proxies = {"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None
        return self._session.get(
            url,
            headers=headers,
            params=params,
            cookies=cookies,
            timeout=timeout,
            proxies=proxies,
            impersonate=self.impersonate or DEFAULT_CURL_CFFI_IMPERSONATE,
        )


def build_tiktok_http_client(
    client_name: str | None = None,
    *,
    retry_total: int = 3,
    backoff_factor: float = 1.5,
    proxy_url: str | None = None,
    impersonate: str | None = None,
) -> _TikTokHttpClientBase:
    normalized_name = str(client_name or DEFAULT_HTTP_CLIENT).strip().lower() or DEFAULT_HTTP_CLIENT
    config = _ClientConfig(
        retry_total=retry_total,
        backoff_factor=backoff_factor,
        proxy_url=proxy_url,
        impersonate=impersonate,
    )
    if normalized_name == "requests":
        return RequestsTikTokHttpClient(config=config)
    if normalized_name == "curl_cffi":
        return CurlCffiTikTokHttpClient(config=config)
    raise RuntimeError(f"Unsupported TikTok HTTP client: {normalized_name}")
