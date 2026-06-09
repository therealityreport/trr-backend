"""Unit tests for TikTok media resolver fallbacks."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from trr_backend.socials.tiktok import media_resolver


class _FakeResponse:
    def __init__(self, *, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise media_resolver.requests.HTTPError(response=SimpleNamespace(status_code=self.status_code))


class _FakeSession:
    def __init__(self, *, html: str, status_code: int = 200) -> None:
        self._html = html
        self._status_code = status_code

    def get(self, *_args, **_kwargs) -> _FakeResponse:
        return _FakeResponse(text=self._html, status_code=self._status_code)


class _ProbeResponse:
    def __init__(self, *, status_code: int, content_type: str, url: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self.url = url or "https://video.test/stream"

    def close(self) -> None:
        return None


def test_resolve_tiktok_media_prefers_ytdlp_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        media_resolver.shutil,
        "which",
        lambda name: "/usr/local/bin/yt-dlp" if name == "yt-dlp" else None,
    )

    payload = {"url": "https://video.test/main.mp4", "thumbnail": "https://img.test/thumb.jpg"}
    monkeypatch.setattr(
        media_resolver.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
    )

    assert result.source == "yt_dlp_manifest"
    assert result.media_urls == ["https://video.test/main.mp4"]
    assert result.thumbnail_url == "https://img.test/thumb.jpg"
    assert result.attempts[0]["success"] is True


def test_resolve_tiktok_media_skips_ytdlp_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        media_resolver.shutil,
        "which",
        lambda name: "/usr/local/bin/yt-dlp" if name == "yt-dlp" else None,
    )

    def _unexpected_subprocess(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("yt-dlp should be skipped when allow_ytdlp=False")

    monkeypatch.setattr(media_resolver.subprocess, "run", _unexpected_subprocess)
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "12345",
                        "video": {
                            "bitrateInfo": [
                                {
                                    "Bitrate": 2000,
                                    "PlayAddr": {
                                        "DataSize": 200,
                                        "Height": 1080,
                                        "Width": 1920,
                                        "UrlList": ["https://video.test/1080.mp4"],
                                    },
                                }
                            ],
                            "cover": "https://img.test/cover.jpg",
                        },
                    }
                }
            }
        }
    }
    html = (
        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">' + json.dumps(payload) + "</script>"
    )

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html=html),
        allow_ytdlp=False,
    )

    assert result.source == "watch_page_json"
    assert result.media_urls == ["https://video.test/1080.mp4"]
    assert result.thumbnail_url == "https://img.test/cover.jpg"
    assert result.attempts[0]["source"] == "yt_dlp_manifest"
    assert result.attempts[0]["reason_code"] == "tiktok_ytdlp_skipped"


def test_resolve_tiktok_media_rejects_unsafe_canonical_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="http://127.0.0.1/video/12345",
        session=_FakeSession(html=""),
    )

    assert result.media_urls == []
    assert result.attempts[0]["reason_code"] == "tiktok_unsafe_video_url"


def test_resolve_tiktok_media_falls_back_to_watch_page_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "12345",
                        "video": {
                            "bitrateInfo": [
                                {
                                    "Bitrate": 1200,
                                    "PlayAddr": {
                                        "DataSize": 100,
                                        "Height": 720,
                                        "Width": 1280,
                                        "UrlList": ["https://video.test/720.mp4"],
                                    },
                                },
                                {
                                    "Bitrate": 2000,
                                    "PlayAddr": {
                                        "DataSize": 200,
                                        "Height": 1080,
                                        "Width": 1920,
                                        "UrlList": ["https://video.test/1080.mp4"],
                                    },
                                },
                            ],
                            "cover": "https://img.test/cover.jpg",
                            "dynamicCover": "https://img.test/dynamic.gif",
                        },
                    }
                }
            }
        }
    }
    html = (
        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">' + json.dumps(payload) + "</script>"
    )

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html=html),
    )

    assert result.source == "watch_page_json"
    assert result.media_urls == ["https://video.test/1080.mp4"]
    assert result.thumbnail_url == "https://img.test/cover.jpg"
    assert any(attempt["source"] == "watch_page_json" and attempt["success"] for attempt in result.attempts)


def test_resolve_tiktok_media_falls_back_to_unofficial_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)

    class _FakeApiResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        @staticmethod
        def json() -> dict[str, object]:
            return {
                "msg": "success",
                "data": {
                    "hdplay": "https://video.test/hd.mp4",
                    "origin_cover": "https://img.test/cover.jpg",
                },
            }

    monkeypatch.setattr(media_resolver.requests, "get", lambda *args, **kwargs: _FakeApiResponse())

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html="<html><body>no-json</body></html>"),
    )

    assert result.source == "unofficial_api"
    assert result.media_urls == ["https://video.test/hd.mp4"]
    assert result.thumbnail_url == "https://img.test/cover.jpg"
    assert result.attempts[-1]["source"] == "unofficial_api"
    assert result.attempts[-1]["success"] is True


def test_resolve_tiktok_media_uses_unofficial_when_watch_url_not_downloadable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "12345",
                        "video": {
                            "bitrateInfo": [
                                {
                                    "Bitrate": 1200,
                                    "PlayAddr": {
                                        "DataSize": 100,
                                        "Height": 720,
                                        "Width": 1280,
                                        "UrlList": ["https://video.test/watch.mp4"],
                                    },
                                }
                            ],
                            "cover": "https://img.test/cover.jpg",
                        },
                    }
                }
            }
        }
    }
    html = (
        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">' + json.dumps(payload) + "</script>"
    )

    monkeypatch.setattr(
        media_resolver,
        "_resolve_with_unofficial_api",
        lambda **_kwargs: (
            ["https://video.test/unofficial.mp4"],
            "https://img.test/unofficial.jpg",
            media_resolver._build_attempt(  # noqa: SLF001
                source="unofficial_api",
                success=True,
                selected_url_count=1,
            ),
        ),
    )

    class _ProbeResponse:
        def __init__(self, status_code: int, content_type: str = "video/mp4") -> None:
            self.status_code = status_code
            self.headers = {"content-type": content_type}

        def close(self) -> None:
            return None

    def _fake_probe_get(url: str, **_kwargs):  # noqa: ANN001
        if "watch.mp4" in url:
            return _ProbeResponse(403, "text/html")
        if "unofficial.mp4" in url:
            return _ProbeResponse(206)
        return _ProbeResponse(404, "text/html")

    monkeypatch.setattr(media_resolver.requests, "get", _fake_probe_get)

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html=html),
        allow_ytdlp=False,
        validate_download_url=True,
    )

    assert result.source == "unofficial_api"
    assert result.media_urls == ["https://video.test/unofficial.mp4"]
    assert any(attempt["source"] == "watch_page_json_probe" and not attempt["success"] for attempt in result.attempts)


def test_resolve_tiktok_media_accepts_no_suffix_video_probe_and_records_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    stream_url = "https://v16-webapp-prime.tiktokcdn-us.com/tos-useast5-pve/no-extension-token"
    monkeypatch.setattr(
        media_resolver,
        "_TIKTOK_MEDIA_URL_POLICY",
        media_resolver.MediaUrlSafetyPolicy(media_resolver.allowed_hosts_for_platform("tiktok"), resolve_dns=False),
    )
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "12345",
                        "video": {
                            "playAddr": {"UrlList": [stream_url]},
                            "cover": "https://img.test/cover.jpg",
                        },
                    }
                }
            }
        }
    }
    html = (
        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">' + json.dumps(payload) + "</script>"
    )

    monkeypatch.setattr(
        media_resolver.requests,
        "get",
        lambda url, **_kwargs: _ProbeResponse(status_code=206, content_type="video/mp4", url=url),
    )

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html=html),
        allow_ytdlp=False,
        validate_download_url=True,
    )

    assert result.source == "watch_page_json"
    assert result.media_urls == [stream_url]
    assert result.media_asset_meta["probe_evidence"][0]["content_type"] == "video/mp4"
    assert result.media_asset_meta["source_assets"][0]["type"] == "video"
    assert result.media_asset_meta["source_assets"][0]["probe"]["http_status"] == 206


def test_resolve_tiktok_media_rejects_html_probe_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    stream_url = "https://v16-webapp-prime.tiktokcdn-us.com/tos-useast5-pve/no-extension-token"
    monkeypatch.setattr(
        media_resolver,
        "_TIKTOK_MEDIA_URL_POLICY",
        media_resolver.MediaUrlSafetyPolicy(media_resolver.allowed_hosts_for_platform("tiktok"), resolve_dns=False),
    )
    payload = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "12345",
                        "video": {
                            "playAddr": {"UrlList": [stream_url]},
                            "cover": "https://img.test/cover.jpg",
                        },
                    }
                }
            }
        }
    }
    html = (
        '<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">' + json.dumps(payload) + "</script>"
    )
    monkeypatch.setattr(
        media_resolver,
        "_resolve_with_unofficial_api",
        lambda **_kwargs: (
            [],
            None,
            media_resolver._build_attempt(  # noqa: SLF001
                source="unofficial_api",
                success=False,
                reason_code="tiktok_media_not_found",
                selected_url_count=0,
            ),
        ),
    )
    monkeypatch.setattr(
        media_resolver.requests,
        "get",
        lambda url, **_kwargs: _ProbeResponse(status_code=200, content_type="text/html; charset=utf-8", url=url),
    )

    result = media_resolver.resolve_tiktok_media(
        "12345",
        canonical_url="https://www.tiktok.com/@bravotv/video/12345",
        session=_FakeSession(html=html),
        allow_ytdlp=False,
        validate_download_url=True,
    )

    assert result.media_urls == []
    probe_attempt = next(attempt for attempt in result.attempts if attempt["source"] == "watch_page_json_probe")
    assert probe_attempt["success"] is False
    assert probe_attempt["reason_code"] == "download_bad_content_type"
    assert probe_attempt["http_status"] == 200
    assert probe_attempt["content_type"] == "text/html"
