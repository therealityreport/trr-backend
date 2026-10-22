"""TikTok error types shared by the direct scraper, HTTP client, and posts Scrapling lane."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


class TikTokHttpRequestError(requests.exceptions.RequestException, RuntimeError):
    """Transport-agnostic request failure.

    Subclasses ``requests.exceptions.RequestException`` so scraper call sites
    that catch the requests exception family (``fetch_user_detail``,
    ``fetch_posts``, ``fetch_comments``, replies, profile HTML) record a
    ``request_error`` reason instead of letting the wrapper escape. It also
    stays a ``RuntimeError`` for callers that relied on the original base.
    """

    def __init__(self, message: str, *, response: Any | None = None) -> None:
        super().__init__(message, response=response)
        self.response = response


@dataclass(slots=True)
class TikTokPostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class TikTokPostsScraplingCancelledError(Exception):
    message: str
    cancel_scope: str
    job_status: str | None = None
    run_status: str | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


TikTokPostsScraplingCancelled = TikTokPostsScraplingCancelledError

__all__ = [
    "TikTokHttpRequestError",
    "TikTokPostsScraplingCancelled",
    "TikTokPostsScraplingCancelledError",
    "TikTokPostsScraplingRuntimeError",
]
