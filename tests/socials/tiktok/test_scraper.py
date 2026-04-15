"""Unit tests for TikTok scraper path-health and browser-intercept triage."""

import builtins
from datetime import datetime

from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper


def _config() -> TikTokScrapeConfig:
    return TikTokScrapeConfig(
        username="bravotv",
        hashtags=["RHOBH"],
        date_start=datetime.fromisoformat("2026-03-31T00:00:00+00:00"),
        date_end=datetime.fromisoformat("2026-04-10T00:00:00+00:00"),
        max_pages=2,
    )


def test_ytdlp_zero_posts_marks_single_path_degraded(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})
    monkeypatch.setattr(scraper, "_scrape_via_ytdlp", lambda *args, **kwargs: [])

    posts = scraper.scrape(_config())

    assert posts == []
    assert scraper.last_retrieval_meta["retrieval_mode"] == "ytdlp"
    assert scraper.last_retrieval_meta["path_role"] == "primary"
    assert scraper.last_retrieval_meta["topology_state"] == "single_path_ytdlp"
    assert scraper.last_retrieval_meta["risk_state"] == "critical"
    assert scraper.last_retrieval_meta["operator_summary"] == (
        "TikTok posts path degraded: yt-dlp returned zero posts while browser_intercept is not proven live."
    )


def test_ytdlp_timeout_falls_back_to_api(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})
    api_calls = 0

    def _fake_ytdlp(*_args, **_kwargs):
        scraper._set_retrieval_meta(  # noqa: SLF001
            context_mode="ytdlp",
            auth_mode="none",
            retrieval_mode="ytdlp",
            error_code="ytdlp_timeout",
            stop_reason="timeout",
            fallback_chain=["yt_dlp"],
        )
        return []

    def _fake_api(*_args, **_kwargs):
        nonlocal api_calls
        api_calls += 1
        scraper._set_retrieval_meta(  # noqa: SLF001
            retrieval_mode="api",
            fallback_chain=["yt_dlp", "api"],
        )
        return ["api-post"]

    monkeypatch.setattr(scraper, "_scrape_via_ytdlp", _fake_ytdlp)
    monkeypatch.setattr(scraper, "_scrape_api", _fake_api)

    posts = scraper.scrape(_config())

    assert posts == ["api-post"]
    assert api_calls == 1


def test_ytdlp_nonzero_exit_falls_back_to_api(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})
    api_calls = 0

    def _fake_ytdlp(*_args, **_kwargs):
        scraper._set_retrieval_meta(  # noqa: SLF001
            context_mode="ytdlp",
            auth_mode="none",
            retrieval_mode="ytdlp",
            error_code="ytdlp_nonzero_exit",
            stop_reason="nonzero_exit",
            fallback_chain=["yt_dlp"],
        )
        return []

    def _fake_api(*_args, **_kwargs):
        nonlocal api_calls
        api_calls += 1
        scraper._set_retrieval_meta(  # noqa: SLF001
            retrieval_mode="api",
            fallback_chain=["yt_dlp", "api"],
        )
        return ["api-post"]

    monkeypatch.setattr(scraper, "_scrape_via_ytdlp", _fake_ytdlp)
    monkeypatch.setattr(scraper, "_scrape_api", _fake_api)

    posts = scraper.scrape(_config())

    assert posts == ["api-post"]
    assert api_calls == 1


def test_ytdlp_fallback_updates_path_health_topology(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    def _fake_ytdlp(*_args, **_kwargs):
        scraper._set_retrieval_meta(  # noqa: SLF001
            context_mode="ytdlp",
            auth_mode="none",
            retrieval_mode="ytdlp",
            error_code="ytdlp_unavailable",
            stop_reason="ytdlp_unavailable",
            fallback_chain=["yt_dlp"],
        )
        return []

    def _fake_api(*_args, **_kwargs):
        scraper._set_retrieval_meta(  # noqa: SLF001
            retrieval_mode="api",
            fallback_chain=["yt_dlp", "api"],
        )
        return ["api-post"]

    monkeypatch.setattr(scraper, "_scrape_via_ytdlp", _fake_ytdlp)
    monkeypatch.setattr(scraper, "_scrape_api", _fake_api)

    posts = scraper.scrape(_config())

    assert posts == ["api-post"]
    assert scraper.last_retrieval_meta["retrieval_mode"] == "api"
    assert scraper.last_retrieval_meta["path_role"] == "fallback"
    assert scraper.last_retrieval_meta["topology_state"] == "ytdlp_with_api_fallback"


def test_browser_intercept_zero_posts_classifies_target_drift() -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    bucket = scraper._classify_browser_intercept_failure(  # noqa: SLF001
        posts_found=0,
        intercepted_post_responses=0,
        intercepted_user_detail_responses=1,
        dom_cards_seen=0,
        scroll_iterations=5,
        authenticated=True,
        playwright_error=None,
    )

    assert bucket == "interception_target_drift"


def test_browser_intercept_zero_posts_classifies_scroll_or_pagination_drift() -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    bucket = scraper._classify_browser_intercept_failure(  # noqa: SLF001
        posts_found=0,
        intercepted_post_responses=2,
        intercepted_user_detail_responses=1,
        dom_cards_seen=0,
        scroll_iterations=3,
        authenticated=True,
        playwright_error=None,
    )

    assert bucket == "scroll_or_pagination_drift"


def test_browser_intercept_zero_posts_classifies_playwright_runtime_change() -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    bucket = scraper._classify_browser_intercept_failure(  # noqa: SLF001
        posts_found=0,
        intercepted_post_responses=0,
        intercepted_user_detail_responses=0,
        dom_cards_seen=0,
        scroll_iterations=0,
        authenticated=True,
        playwright_error="TimeoutError",
    )

    assert bucket == "playwright_runtime_change"


def test_browser_intercept_zero_posts_preserves_triage_bucket_after_structured_failure(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})
    scraper.last_retrieval_meta.update(
        {
            "intercepted_post_responses": 0,
            "intercepted_user_detail_responses": 1,
            "dom_cards_seen": 0,
            "scroll_iterations": 5,
            "auth_mode": "with_cookies",
            "playwright_error": "TimeoutError",
        }
    )
    monkeypatch.setattr(scraper, "_scrape_browser_intercept", lambda *args, **kwargs: [])

    posts = scraper.scrape(TikTokScrapeConfig(username="bravotv", scrape_mode="browser_intercept"))

    assert posts == []
    assert scraper.last_retrieval_meta["error_code"] == "browser_intercept_zero_posts"
    assert scraper.last_retrieval_meta["stop_reason"] == "browser_intercept_zero_posts"
    assert scraper.last_retrieval_meta["triage_bucket"] == "playwright_runtime_change"
    assert scraper.last_retrieval_meta["intercepted_post_responses"] == 0
    assert scraper.last_retrieval_meta["intercepted_user_detail_responses"] == 1
    assert scraper.last_retrieval_meta["dom_cards_seen"] == 0
    assert scraper.last_retrieval_meta["scroll_iterations"] == 5
    assert scraper.last_retrieval_meta["playwright_error"] == "TimeoutError"


def test_browser_intercept_playwright_unavailable_preserves_runtime_triage(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "playwright.sync_api":
            raise ModuleNotFoundError("playwright unavailable")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    posts = scraper.scrape(TikTokScrapeConfig(username="bravotv", scrape_mode="browser_intercept"))

    assert posts == []
    assert scraper.last_retrieval_meta["error_code"] == "playwright_unavailable"
    assert scraper.last_retrieval_meta["playwright_error"] == "ModuleNotFoundError"
    assert scraper.last_retrieval_meta["triage_bucket"] == "playwright_runtime_change"


def test_browser_intercept_direct_preflight_does_not_count_as_intercepted_user_detail(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    class _FakeLocator:
        def count(self) -> int:
            return 0

    class _FakePage:
        def __init__(self) -> None:
            self._response_handler = None

        def goto(self, *_args, **_kwargs) -> None:
            return None

        def wait_for_timeout(self, *_args, **_kwargs) -> None:
            return None

        def on(self, event: str, handler) -> None:
            assert event == "response"
            self._response_handler = handler

        def evaluate(self, *_args, **_kwargs) -> None:
            return None

        def locator(self, _selector: str) -> _FakeLocator:
            return _FakeLocator()

    class _FakeContext:
        def add_cookies(self, _cookies) -> None:
            return None

        def new_page(self) -> _FakePage:
            return _FakePage()

    class _FakeBrowser:
        def new_context(self, **_kwargs) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakePlaywrightContextManager:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr("playwright.sync_api.sync_playwright", lambda: _FakePlaywrightContextManager())
    monkeypatch.setattr(
        "trr_backend.socials.browser_cookie_refresh.launch_browser",
        lambda *_args, **_kwargs: _FakeBrowser(),
    )
    monkeypatch.setattr(
        scraper,
        "fetch_user_detail",
        lambda *_args, **_kwargs: {"userInfo": {"user": {"secUid": "sec"}}},
    )
    monkeypatch.setattr(scraper, "build_profile_snapshot", lambda *_args, **_kwargs: {"avatar_url": None})

    posts = scraper.scrape(TikTokScrapeConfig(username="bravotv", scrape_mode="browser_intercept"))

    assert posts == []
    assert scraper.last_retrieval_meta["intercepted_user_detail_responses"] == 0
    assert scraper.last_retrieval_meta["intercepted_post_responses"] == 0
    assert scraper.last_retrieval_meta["triage_bucket"] == "scroll_or_pagination_drift"


def test_browser_intercept_launch_failure_preserves_runtime_triage(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    class _FakePlaywrightContextManager:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr("playwright.sync_api.sync_playwright", lambda: _FakePlaywrightContextManager())
    monkeypatch.setattr(
        "trr_backend.socials.browser_cookie_refresh.launch_browser",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("launch failed")),
    )

    posts = scraper.scrape(TikTokScrapeConfig(username="bravotv", scrape_mode="browser_intercept"))

    assert posts == []
    assert scraper.last_retrieval_meta["error_code"] == "browser_intercept_error"
    assert scraper.last_retrieval_meta["playwright_error"] == "RuntimeError"
    assert scraper.last_retrieval_meta["triage_bucket"] == "playwright_runtime_change"


def test_browser_intercept_failed_browser_user_detail_does_not_classify_target_drift(monkeypatch) -> None:
    scraper = TikTokScraper(cookies={"sessionid": "cookie"})

    class _FakeResponse:
        url = "https://www.tiktok.com/api/user/detail/"
        ok = True

        def json(self):
            raise ValueError("non-json")

    class _FakeLocator:
        def count(self) -> int:
            return 0

    class _FakePage:
        def __init__(self) -> None:
            self._response_handler = None

        def goto(self, *_args, **_kwargs) -> None:
            return None

        def wait_for_timeout(self, *_args, **_kwargs) -> None:
            return None

        def on(self, event: str, handler) -> None:
            assert event == "response"
            self._response_handler = handler

        def evaluate(self, *_args, **_kwargs) -> None:
            if self._response_handler is not None:
                self._response_handler(_FakeResponse())

        def locator(self, _selector: str) -> _FakeLocator:
            return _FakeLocator()

    class _FakeContext:
        def add_cookies(self, _cookies) -> None:
            return None

        def new_page(self) -> _FakePage:
            return _FakePage()

    class _FakeBrowser:
        def new_context(self, **_kwargs) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakePlaywrightContextManager:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr("playwright.sync_api.sync_playwright", lambda: _FakePlaywrightContextManager())
    monkeypatch.setattr(
        "trr_backend.socials.browser_cookie_refresh.launch_browser",
        lambda *_args, **_kwargs: _FakeBrowser(),
    )
    monkeypatch.setattr(
        scraper,
        "fetch_user_detail",
        lambda *_args, **_kwargs: {"userInfo": {"user": {"secUid": "sec"}}},
    )
    monkeypatch.setattr(scraper, "build_profile_snapshot", lambda *_args, **_kwargs: {"avatar_url": None})

    posts = scraper.scrape(TikTokScrapeConfig(username="bravotv", scrape_mode="browser_intercept"))

    assert posts == []
    assert scraper.last_retrieval_meta["intercepted_user_detail_responses"] == 0
    assert scraper.last_retrieval_meta["triage_bucket"] != "interception_target_drift"
