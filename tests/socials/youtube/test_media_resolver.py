"""Unit tests for YouTube media resolver fallbacks."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest

from trr_backend.socials.youtube import media_resolver


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


def test_resolve_youtube_media_prefers_ytdlp_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        media_resolver.shutil,
        "which",
        lambda name: "/usr/local/bin/yt-dlp" if name == "yt-dlp" else None,
    )

    payload = {"url": "https://video.test/stream.mp4", "thumbnail": "https://img.test/thumb.jpg"}
    monkeypatch.setattr(
        media_resolver.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )

    result = media_resolver.resolve_youtube_media("abc123")

    assert result.source == "yt_dlp_manifest"
    assert result.media_urls == ["https://video.test/stream.mp4"]
    assert result.thumbnail_url == "https://img.test/thumb.jpg"
    assert result.attempts[0]["success"] is True


def test_resolve_youtube_media_falls_back_to_watch_page_streaming_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    html = """
    <html><body>
    <script>
    var ytInitialPlayerResponse = {"streamingData":{"formats":[
      {"url":"https://video.test/360.mp4","width":640,"height":360,"bitrate":1000,
       "mimeType":"video/mp4; codecs=\\"avc1.42001E, mp4a.40.2\\""},
      {"url":"https://video.test/1080.mp4","width":1920,"height":1080,"bitrate":4000,
       "mimeType":"video/mp4; codecs=\\"avc1.640028, mp4a.40.2\\""}
    ]},"videoDetails":{"thumbnail":{"thumbnails":[
      {"url":"https://img.test/s.jpg","width":120},
      {"url":"https://img.test/l.jpg","width":1280}
    ]}}};
    </script>
    </body></html>
    """

    result = media_resolver.resolve_youtube_media("abc123", session=cast(Any, _FakeSession(html=html)))

    assert result.source == "watch_page_streaming_data"
    assert result.media_urls[0] == "https://video.test/1080.mp4"
    assert result.thumbnail_url == "https://img.test/l.jpg"
    assert any(attempt["source"] == "watch_page_streaming_data" and attempt["success"] for attempt in result.attempts)


def test_resolve_youtube_media_accepts_videoplayback_mime_query_without_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    stream_url = "https://rr2---sn.test/videoplayback?expire=1767225600&mime=video%2Fmp4&gir=yes"
    html = f"""
    <html><body>
    <script>
    var ytInitialPlayerResponse = {{"streamingData":{{"adaptiveFormats":[
      {{"url":"{stream_url}","bitrate":2000}}
    ]}},"videoDetails":{{"thumbnail":{{"thumbnails":[
      {{"url":"https://img.test/thumb.jpg","width":480,"height":270}}
    ]}}}}}};
    </script>
    </body></html>
    """

    result = media_resolver.resolve_youtube_media("abc123", session=cast(Any, _FakeSession(html=html)))

    assert result.source == "watch_page_streaming_data"
    assert result.media_urls == [stream_url]
    assert result.media_asset_meta["source_assets"][0]["type"] == "video"


def test_resolve_youtube_media_falls_back_to_og_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(media_resolver.shutil, "which", lambda _name: None)
    html = """
    <html><head>
      <meta property="og:video" content="https://cdn.test/fallback.mp4" />
      <meta property="og:image" content="https://cdn.test/thumb.jpg" />
    </head></html>
    """

    result = media_resolver.resolve_youtube_media("abc123", session=cast(Any, _FakeSession(html=html)))

    assert result.source == "og_fallback"
    assert result.media_urls == ["https://cdn.test/fallback.mp4"]
    assert result.thumbnail_url == "https://cdn.test/thumb.jpg"
    assert result.attempts[-1]["source"] == "og_fallback"
    assert result.attempts[-1]["success"] is True
