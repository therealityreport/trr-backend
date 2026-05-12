from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from trr_backend.socials.tiktok import direct_scrape


def _request(**overrides: Any) -> SimpleNamespace:
    values = {
        "username": "bravotv",
        "hashtags": ["RHOSLC"],
        "date_start": datetime(2025, 1, 1, tzinfo=UTC),
        "date_end": datetime(2025, 1, 2, tzinfo=UTC),
        "delay_seconds": 1.0,
        "max_pages": 3,
        "show_id": "show-1",
        "season_number": 6,
        "person_id": "person-1",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_build_scrape_diagnostics_returns_safe_subset() -> None:
    diagnostics = direct_scrape.build_scrape_diagnostics(
        {
            "retrieval_mode": "browser_intercept",
            "http_client": "curl_cffi",
            "fallback_chain": ["browser_intercept", "ytdlp"],
            "stop_reason": "path_degraded",
            "error_code": "tiktok_posts_path_degraded",
            "risk_state": "critical",
            "operator_summary": "TikTok posts path degraded.",
            "operator_action": "Refresh auth.",
            "triage_bucket": "manual_review",
            "profile_enrichment_status": "partial",
            "cookie": "raw-cookie-value",
            "proxy_url": "https://user:pass@example.test",
            "msToken": "signed-token",
        }
    )

    assert diagnostics == {
        "retrieval_mode": "browser_intercept",
        "http_client": "curl_cffi",
        "fallback_chain": ["browser_intercept", "ytdlp"],
        "stop_reason": "path_degraded",
        "error_code": "tiktok_posts_path_degraded",
        "risk_state": "critical",
        "operator_summary": "TikTok posts path degraded.",
        "operator_action": "Refresh auth.",
        "triage_bucket": "manual_review",
        "profile_enrichment_status": "partial",
    }


def test_scrape_tiktok_uses_loader_and_shapes_route_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            captured["cookies"] = cookies
            self.last_retrieval_meta = {
                "risk_state": "critical",
                "operator_summary": "TikTok posts path degraded.",
                "ignored": "not-route-visible",
            }

        def scrape(self, config):  # noqa: ANN001
            captured["config"] = config
            return [
                SimpleNamespace(
                    video_id="72899887766",
                    date_time="2025-01-01T00:00:00+00:00",
                    description="Finale #RHOSLC",
                    hashtags=["RHOSLC"],
                    mentions=["bravotv"],
                    likes=10,
                    comments=2,
                    shares=3,
                    views=400,
                    url="https://www.tiktok.com/@bravotv/video/72899887766",
                    username="bravotv",
                    author_nickname="Bravo",
                    duration=30,
                    music_title="Original Sound",
                    music_author="Bravo",
                )
            ]

    def _load_cookies(surface: str) -> dict[str, str]:
        captured["surface"] = surface
        return {"sessionid": "cookie"}

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)

    request = _request()
    payload = direct_scrape.scrape_tiktok(request, load_cookies=_load_cookies)

    assert captured["surface"] == "scrape"
    assert captured["cookies"] == {"sessionid": "cookie"}
    config = captured["config"]
    assert config.username == "bravotv"
    assert config.hashtags == ["RHOSLC"]
    assert config.date_start == request.date_start
    assert config.date_end == request.date_end
    assert config.delay_seconds == 1.0
    assert config.max_pages == 3
    assert config.show_id == "show-1"
    assert config.season_number == 6
    assert config.person_id == "person-1"
    assert payload == {
        "success": True,
        "username": "bravotv",
        "posts_found": 1,
        "posts": [
            {
                "video_id": "72899887766",
                "date_time": "2025-01-01T00:00:00+00:00",
                "description": "Finale #RHOSLC",
                "hashtags": ["RHOSLC"],
                "mentions": ["bravotv"],
                "likes": 10,
                "comments": 2,
                "shares": 3,
                "views": 400,
                "url": "https://www.tiktok.com/@bravotv/video/72899887766",
                "username": "bravotv",
                "author_nickname": "Bravo",
                "duration": 30,
                "music_title": "Original Sound",
                "music_author": "Bravo",
            }
        ],
        "filters_applied": {
            "hashtags": ["RHOSLC"],
            "date_start": "2025-01-01T00:00:00+00:00",
            "date_end": "2025-01-02T00:00:00+00:00",
        },
        "diagnostics": {
            "risk_state": "critical",
            "operator_summary": "TikTok posts path degraded.",
        },
    }


def test_scrape_tiktok_returns_error_payload_on_unexpected_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FailingScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            pass

        def scrape(self, config):  # noqa: ANN001
            raise RuntimeError("scrape exploded")

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FailingScraper)

    payload = direct_scrape.scrape_tiktok(_request(), load_cookies=lambda _surface: {"sessionid": "cookie"})

    assert payload == {
        "success": False,
        "username": "bravotv",
        "posts_found": 0,
        "posts": [],
        "filters_applied": {},
        "error": "scrape exploded",
    }


def test_scrape_tiktok_reraises_http_exception_from_loader() -> None:
    def _load_cookies(_surface: str) -> None:
        raise HTTPException(status_code=503, detail="TikTok auth unavailable")

    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.scrape_tiktok(_request(), load_cookies=_load_cookies)

    assert exc_info.value.status_code == 503


def test_preview_tiktok_profile_uses_loader_and_shapes_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            captured["cookies"] = cookies

        def fetch_user_detail(self, username: str, *, delay: float) -> dict[str, Any]:
            captured["username"] = username
            captured["delay"] = delay
            return {
                "userInfo": {
                    "user": {
                        "uniqueId": username,
                        "nickname": "Bravo",
                        "signature": "bio",
                        "verified": True,
                        "privateAccount": False,
                        "avatarMedium": "https://example.test/avatar.jpg",
                    },
                    "stats": {
                        "followerCount": 10,
                        "followingCount": 20,
                        "heart": 30,
                        "heartCount": 999,
                        "videoCount": 40,
                    },
                }
            }

    def _load_cookies(surface: str) -> dict[str, str]:
        captured["surface"] = surface
        return {"sessionid": "cookie"}

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)

    payload = direct_scrape.preview_tiktok_profile("creator", load_cookies=_load_cookies)

    assert captured == {
        "surface": "preview",
        "cookies": {"sessionid": "cookie"},
        "username": "creator",
        "delay": 0,
    }
    assert payload == {
        "username": "creator",
        "nickname": "Bravo",
        "bio": "bio",
        "is_verified": True,
        "is_private": False,
        "followers": 10,
        "following": 20,
        "likes": 30,
        "video_count": 40,
        "profile_pic_url": "https://example.test/avatar.jpg",
    }


def test_preview_tiktok_profile_raises_404_when_profile_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            pass

        def fetch_user_detail(self, username: str, *, delay: float) -> dict[str, Any]:
            return {"userInfo": {"stats": {"followerCount": 10}}}

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FakeScraper)

    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.preview_tiktok_profile("missing", load_cookies=lambda _surface: {"sessionid": "cookie"})

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Profile not found: @missing"


def test_preview_tiktok_profile_raises_500_on_unexpected_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FailingScraper:
        def __init__(self, *, cookies=None):  # noqa: ANN003
            pass

        def fetch_user_detail(self, username: str, *, delay: float) -> dict[str, Any]:
            raise RuntimeError("preview exploded")

    monkeypatch.setattr("trr_backend.socials.tiktok.TikTokScraper", _FailingScraper)

    with pytest.raises(HTTPException) as exc_info:
        direct_scrape.preview_tiktok_profile("creator", load_cookies=lambda _surface: {"sessionid": "cookie"})

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "preview exploded"


def test_direct_scrape_imports_stay_out_of_repository_and_posts_scrapling_lanes() -> None:
    source = Path(direct_scrape.__file__).read_text()
    tree = ast.parse(source)
    imported_modules = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported_modules.update(
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    )

    assert "trr_backend.repositories.social_season_analytics" not in imported_modules
    assert all("posts_scrapling" not in module for module in imported_modules)
