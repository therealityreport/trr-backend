from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from trr_backend.socials import threads as threads_package
from trr_backend.socials.threads import direct_scrape
from trr_backend.socials.threads import scraper as threads_scraper


class _FakeConfig:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _FakeScraper:
    def __init__(self, *, cookies: Any, posts: list[Any] | None = None, error: Exception | None = None) -> None:
        self.cookies = cookies
        self.posts = list(posts or [])
        self.error = error
        self.config: Any = None
        self.last_retrieval_meta = {"source": "threads_graphql_api", "pages_scanned": 1}

    def scrape(self, config: Any) -> list[Any]:
        self.config = config
        if self.error is not None:
            raise self.error
        return list(self.posts)


def test_post_to_payload_preserves_threads_route_shape() -> None:
    post = SimpleNamespace(
        post_id="post-1",
        username="bravotv",
        text="A #RHOSLC post",
        likes=10,
        replies=2,
        reposts=3,
        quotes=4,
        views=500,
        url="https://www.threads.com/@bravotv/post/post-1",
        thumbnail_url="https://images.test/thumb.jpg",
        media_urls=["https://images.test/a.jpg", "", None],
        posted_at=1_767_225_600,
    )

    assert direct_scrape.post_to_payload(post) == {
        "post_id": "post-1",
        "username": "bravotv",
        "text": "A #RHOSLC post",
        "likes": 10,
        "replies": 2,
        "reposts": 3,
        "quotes": 4,
        "views": 500,
        "url": "https://www.threads.com/@bravotv/post/post-1",
        "thumbnail_url": "https://images.test/thumb.jpg",
        "media_urls": ["https://images.test/a.jpg"],
        "posted_at": "2026-01-01T00:00:00+00:00",
    }


def test_threads_package_facade_preserves_canonical_scraper_identity() -> None:
    assert threads_package.ThreadsScraper is threads_scraper.ThreadsScraper
    assert threads_package.ThreadsScrapeConfig is threads_scraper.ThreadsScrapeConfig


def test_scrape_threads_uses_injected_cookie_surface_and_shapes_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts = [
        SimpleNamespace(
            post_id="post-1",
            username="bravotv",
            text="A #RHOSLC reunion keyword",
            likes=10,
            replies=2,
            reposts=3,
            quotes=4,
            views=500,
            url="https://www.threads.com/@bravotv/post/post-1",
            thumbnail_url=None,
            media_urls=[],
            posted_at=None,
        ),
        SimpleNamespace(
            post_id="post-2",
            username="bravotv",
            text="Below filter",
            likes=1,
            replies=0,
            reposts=0,
            quotes=0,
            views=5,
            url="https://www.threads.com/@bravotv/post/post-2",
            thumbnail_url=None,
            media_urls=[],
            posted_at=None,
        ),
    ]
    scraper = _FakeScraper(cookies={}, posts=posts)
    surfaces: list[str] = []

    _forbid_runtime_package_facade(monkeypatch)
    monkeypatch.setattr(threads_scraper, "ThreadsScraper", lambda *, cookies: _capture_scraper(scraper, cookies))
    monkeypatch.setattr(threads_scraper, "ThreadsScrapeConfig", _FakeConfig)

    request = SimpleNamespace(
        username="bravotv",
        hashtags=["RHOSLC"],
        keywords=["reunion"],
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        delay_seconds=0.5,
        max_pages=3,
    )

    result = direct_scrape.scrape_threads(
        request,
        load_cookies=lambda surface: surfaces.append(surface) or {"sessionid": "cookie"},
    )

    assert surfaces == ["scrape"]
    assert scraper.cookies == {"sessionid": "cookie"}
    assert scraper.config.username == "bravotv"
    assert scraper.config.date_start == datetime(2026, 1, 1, tzinfo=UTC)
    assert scraper.config.date_end == datetime(2026, 2, 1, tzinfo=UTC)
    assert scraper.config.delay_seconds == 0.5
    assert scraper.config.max_pages == 3
    assert result["success"] is True
    assert result["username"] == "bravotv"
    assert result["posts_found"] == 1
    assert result["posts"][0]["post_id"] == "post-1"
    assert result["filters_applied"] == {
        "hashtags": ["RHOSLC"],
        "keywords": ["reunion"],
        "date_start": "2026-01-01T00:00:00+00:00",
        "date_end": "2026-02-01T00:00:00+00:00",
    }
    assert result["retrieval_meta"] == {"source": "threads_graphql_api", "pages_scanned": 1}


def test_scrape_threads_preserves_failure_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = _FakeScraper(cookies={}, error=RuntimeError("scrape failed"))

    _forbid_runtime_package_facade(monkeypatch)
    monkeypatch.setattr(threads_scraper, "ThreadsScraper", lambda *, cookies: _capture_scraper(scraper, cookies))
    monkeypatch.setattr(threads_scraper, "ThreadsScrapeConfig", _FakeConfig)

    result = direct_scrape.scrape_threads(
        SimpleNamespace(username="bravotv", hashtags=[], keywords=[], date_start=None, date_end=None),
        load_cookies=lambda _surface: {"sessionid": "cookie"},
    )

    assert result == {
        "success": False,
        "username": "bravotv",
        "posts_found": 0,
        "posts": [],
        "filters_applied": {},
        "error": "scrape failed",
    }


def test_preview_threads_profile_uses_direct_interface_and_latest_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scraper = _FakeScraper(
        cookies={},
        posts=[
            SimpleNamespace(
                post_id="latest",
                url="https://www.threads.com/@bravotv/post/latest",
                text="Latest post",
            )
        ],
    )
    surfaces: list[str] = []

    _forbid_runtime_package_facade(monkeypatch)
    monkeypatch.setattr(threads_scraper, "ThreadsScraper", lambda *, cookies: _capture_scraper(scraper, cookies))
    monkeypatch.setattr(threads_scraper, "ThreadsScrapeConfig", _FakeConfig)

    result = direct_scrape.preview_threads_profile(
        "bravotv",
        load_cookies=lambda surface: surfaces.append(surface) or {"sessionid": "cookie"},
    )

    assert surfaces == ["preview"]
    assert scraper.cookies == {"sessionid": "cookie"}
    assert scraper.config.username == "bravotv"
    assert scraper.config.max_pages == 1
    assert result == {
        "username": "bravotv",
        "posts_discovered": 1,
        "latest_post": {
            "post_id": "latest",
            "url": "https://www.threads.com/@bravotv/post/latest",
            "text": "Latest post",
        },
        "retrieval_meta": {"source": "threads_graphql_api", "pages_scanned": 1},
    }


def test_preview_threads_profile_wraps_errors_as_http_500() -> None:
    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.preview_threads_profile(
            "bravotv",
            load_cookies=lambda _surface: (_ for _ in ()).throw(RuntimeError("cookie load failed")),
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "cookie load failed"


def test_direct_scrape_module_does_not_import_repository_or_threads_lanes() -> None:
    source = Path(direct_scrape.__file__).read_text()
    tree = ast.parse(source)
    imported_modules: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    assert "trr_backend.repositories.social_season_analytics" not in imported_modules
    assert all("trr_backend.socials.threads.posts_catalog" not in name for name in imported_modules)
    assert all("trr_backend.socials.threads.posts_scrapling" not in name for name in imported_modules)


def _capture_scraper(scraper: _FakeScraper, cookies: Any) -> _FakeScraper:
    scraper.cookies = cookies
    return scraper


def _forbid_runtime_package_facade(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("direct scrape runtime must import the canonical Threads scraper leaf")

    monkeypatch.setattr(threads_package, "ThreadsScraper", fail)
    monkeypatch.setattr(threads_package, "ThreadsScrapeConfig", fail)
