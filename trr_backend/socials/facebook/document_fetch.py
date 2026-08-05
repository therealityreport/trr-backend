from __future__ import annotations

from typing import Any

import requests

from trr_backend.socials.pipelines.facebook_cookie_loader import load_facebook_cookies as _load_facebook_cookies


class FacebookDocumentFetcher:
    def __init__(
        self,
        *,
        cookies: dict[str, str] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._cookies = dict(cookies or _load_facebook_cookies())
        self._session = session or requests.Session()
        self._request_count = 0
        self._transport = "authenticated_document_fetch"

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        return {
            "request_count": self._request_count,
            "transport": self._transport,
        }

    def fetch(
        self,
        url: str,
        *,
        headers: dict[str, str],
        referer: str | None = None,
        timeout: tuple[int, int] = (10, 45),
    ) -> str:
        request_headers = dict(headers or {})
        if referer:
            request_headers["referer"] = str(referer)
        self._request_count += 1
        response = self._session.get(
            url,
            timeout=timeout,
            headers=request_headers,
            cookies=self._cookies,
        )
        response.raise_for_status()
        return response.text or ""
