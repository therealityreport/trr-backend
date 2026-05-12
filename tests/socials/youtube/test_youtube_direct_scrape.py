from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

import trr_backend.socials.youtube as youtube_package
from trr_backend.socials.youtube import direct_scrape


def _request() -> SimpleNamespace:
    return SimpleNamespace(
        channel_handle="bravo",
        keywords=["RHOSLC", "Salt Lake City"],
        date_start=datetime(2025, 8, 14, tzinfo=UTC),
        date_end=datetime(2026, 2, 4, tzinfo=UTC),
        delay_seconds=0.5,
        max_results=25,
        show_id="show-1",
        season_number=6,
        person_id="person-1",
    )


def _video(**overrides: Any) -> SimpleNamespace:
    values = {
        "video_id": "abc123",
        "title": "Preview clip",
        "description": "x" * 510,
        "date_time": "2025-08-14 12:00:00",
        "channel_title": "Bravo",
        "duration": "PT1M",
        "duration_seconds": 60,
        "views": 1234,
        "likes": 56,
        "comments": 7,
        "url": "https://www.youtube.com/watch?v=abc123",
        "thumbnail_url": "https://img.test/thumb.jpg",
        "keywords_matched": ["RHOSLC"],
        "comment_list": [SimpleNamespace(comment_id="comment-1")],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_video_to_payload_preserves_route_fields_and_description_limit() -> None:
    payload = direct_scrape.video_to_payload(_video())

    assert payload == {
        "video_id": "abc123",
        "title": "Preview clip",
        "description": "x" * 500,
        "date_time": "2025-08-14 12:00:00",
        "channel_title": "Bravo",
        "duration": "PT1M",
        "duration_seconds": 60,
        "views": 1234,
        "likes": 56,
        "comments": 7,
        "url": "https://www.youtube.com/watch?v=abc123",
        "thumbnail_url": "https://img.test/thumb.jpg",
        "keywords_matched": ["RHOSLC"],
    }
    assert "comment_list" not in payload


def test_scrape_youtube_uses_package_imports_for_existing_monkeypatch_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()

    class _FakeConfig:
        kwargs: dict[str, Any]

        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    class _FakeScraper:
        last_config: _FakeConfig | None = None

        def scrape(self, config: _FakeConfig) -> list[SimpleNamespace]:
            self.__class__.last_config = config
            return [_video()]

    monkeypatch.setattr(youtube_package, "YouTubeScrapeConfig", _FakeConfig)
    monkeypatch.setattr(youtube_package, "YouTubeScraper", _FakeScraper)

    response = direct_scrape.scrape_youtube(request)

    assert _FakeScraper.last_config is not None
    assert _FakeScraper.last_config.kwargs == {
        "channel_handle": "bravo",
        "keywords": ["RHOSLC", "Salt Lake City"],
        "date_start": request.date_start,
        "date_end": request.date_end,
        "delay_seconds": 0.5,
        "max_results": 25,
        "show_id": "show-1",
        "season_number": 6,
        "person_id": "person-1",
    }
    assert response["success"] is True
    assert response["channel_handle"] == "bravo"
    assert response["videos_found"] == 1
    assert response["videos"] == [direct_scrape.video_to_payload(_video())]
    assert response["filters_applied"] == {
        "keywords": ["RHOSLC", "Salt Lake City"],
        "date_start": "2025-08-14T00:00:00+00:00",
        "date_end": "2026-02-04T00:00:00+00:00",
    }
    assert response["error"] is None


def test_scrape_youtube_returns_existing_error_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeConfig:
        def __init__(self, **_kwargs: Any) -> None:
            pass

    class _FakeScraper:
        def scrape(self, _config: _FakeConfig) -> list[Any]:
            raise RuntimeError("youtube unavailable")

    class _FakeLogger(logging.Logger):
        calls: list[tuple[Any, ...]]

        def __init__(self) -> None:
            super().__init__("fake-youtube-direct-scrape-test")
            self.calls = []

        def error(self, *args: Any, **_kwargs: Any) -> None:
            self.calls.append(args)

    monkeypatch.setattr(youtube_package, "YouTubeScrapeConfig", _FakeConfig)
    monkeypatch.setattr(youtube_package, "YouTubeScraper", _FakeScraper)
    fake_logger = _FakeLogger()

    response = direct_scrape.scrape_youtube(_request(), logger=fake_logger)

    assert response == {
        "success": False,
        "channel_handle": "bravo",
        "videos_found": 0,
        "videos": [],
        "filters_applied": {},
        "error": "youtube unavailable",
    }
    assert fake_logger.calls


def test_scrape_youtube_preserves_http_exception_passthrough(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeConfig:
        def __init__(self, **_kwargs: Any) -> None:
            pass

    class _FakeScraper:
        def scrape(self, _config: _FakeConfig) -> list[Any]:
            raise HTTPException(status_code=418, detail="teapot")

    monkeypatch.setattr(youtube_package, "YouTubeScrapeConfig", _FakeConfig)
    monkeypatch.setattr(youtube_package, "YouTubeScraper", _FakeScraper)

    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.scrape_youtube(_request())

    assert exc_info.value.status_code == 418
    assert exc_info.value.detail == "teapot"


def test_direct_scrape_module_does_not_import_legacy_repository() -> None:
    source = Path(direct_scrape.__file__).read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.youtube.scraper import" not in source
