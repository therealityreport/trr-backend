"""Unit tests for Threads media resolver URL validation."""

from __future__ import annotations

from typing import Any, cast

from trr_backend.socials.threads import media_resolver


class _ProbeResponse:
    def __init__(self, *, status_code: int, content_type: str, url: str) -> None:
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self.url = url


class _ProbeSession:
    def __init__(self, *, status_code: int = 200, content_type: str = "image/jpeg") -> None:
        self.status_code = status_code
        self.content_type = content_type
        self.probed_urls: list[str] = []

    def head(self, url: str, **_kwargs) -> _ProbeResponse:
        self.probed_urls.append(url)
        return _ProbeResponse(status_code=self.status_code, content_type=self.content_type, url=url)


def test_resolve_threads_media_accepts_image_probe_and_records_evidence() -> None:
    session = _ProbeSession(content_type="image/jpeg")
    post_data = {
        "media_type": 1,
        "image_versions2": {
            "candidates": [
                {"url": "https://threads-cdn.test/image-no-extension", "width": 1080, "height": 1080},
            ]
        },
    }

    result = media_resolver.resolve_threads_media(post_data, session=cast(Any, session), validate_urls=True)

    assert result.source == "threads_graphql_post_data"
    assert result.media_urls == ["https://threads-cdn.test/image-no-extension"]
    assert result.media_asset_meta["probe_evidence"][0]["content_type"] == "image/jpeg"
    assert result.media_asset_meta["source_assets"][0]["probe"]["http_status"] == 200


def test_resolve_threads_media_rejects_html_probe_response() -> None:
    session = _ProbeSession(content_type="text/html; charset=utf-8")
    post_data = {
        "media_type": 2,
        "video_versions": [
            {"url": "https://threads-cdn.test/video-no-extension", "width": 1080, "height": 1920},
        ],
    }

    result = media_resolver.resolve_threads_media(post_data, session=cast(Any, session), validate_urls=True)

    assert result.media_urls == []
    assert result.attempts[0]["reason_code"] == "threads_media_urls_not_accessible"
    assert result.media_asset_meta["probe_evidence"][0]["content_type"] == "text/html"
    assert result.media_asset_meta["probe_evidence"][0]["http_status"] == 200
