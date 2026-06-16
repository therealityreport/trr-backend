from __future__ import annotations

import json
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

_FIXTURE_DIR = Path(__file__).parents[3] / "fixtures" / "instagram" / "scrapling"


def _fixture_text(name: str) -> str:
    return (_FIXTURE_DIR / name).read_text(encoding="utf-8")


@pytest.fixture
def _mock_scrapling(monkeypatch):
    """Prevent real Scrapling import."""
    mock_fetcher_cls = MagicMock()
    mock_module = MagicMock()
    mock_module.StealthyFetcher = mock_fetcher_cls
    mock_module.ProxyRotator = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "scrapling.fetchers", mock_module)
    return mock_fetcher_cls


def test_fetcher_constructs_without_error(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "xyz", "ds_user_id": "123"},
        browser_account_id="test",
    )
    assert fetcher._request_count == 0


def test_anonymous_fetcher_strips_authenticated_cookies(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={
            "sessionid": "abc",
            "csrftoken": "csrf",
            "ds_user_id": "123",
            "mid": "mid-token",
        },
        browser_account_id="test",
        auth_state="anonymous",
    )

    metadata = fetcher.runtime_metadata
    assert fetcher._cookies == []
    assert fetcher._raw_cookies == {"csrftoken": "csrf", "mid": "mid-token"}
    assert metadata["auth_state"] == "anonymous"
    assert metadata["authenticated_cookie_count"] == 0


def test_public_fetcher_strips_authenticated_cookies(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={
            "sessionid": "abc",
            "csrftoken": "csrf",
            "ds_user_id": "123",
            "mid": "mid-token",
        },
        browser_account_id="test",
        auth_state="public",
    )

    metadata = fetcher.runtime_metadata
    assert fetcher._cookies == []
    assert fetcher._raw_cookies == {}
    assert metadata["auth_state"] == "public"
    assert metadata["cookie_count"] == 0
    assert metadata["authenticated_cookie_count"] == 0


def test_requests_fallback_can_disable_graphql_recovery(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    captured: dict[str, object] = {}

    class _FallbackScraper:
        last_retrieval_meta = {"error_code": "instagram_graphql_checkpoint_required"}

        def fetch_posts_graphql(self, username: str, **kwargs: object) -> None:
            captured["username"] = username
            captured["kwargs"] = dict(kwargs)
            return None

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[{"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"}],
        raw_cookies={"sessionid": "abc", "csrftoken": "xyz", "ds_user_id": "123"},
        browser_account_id="test",
        allow_requests_recovery=False,
    )
    fetcher._requests_fallback_scraper = _FallbackScraper()

    result = fetcher._fetch_posts_page_via_requests("bravotv", cursor=None, direction="forward")

    assert result.fetch_failed is True
    assert result.auth_failed is True
    assert result.fetch_reason == "instagram_graphql_checkpoint_required"
    assert captured["username"] == "bravotv"
    assert captured["kwargs"]["allow_browser_fallback"] is False
    assert captured["kwargs"]["allow_recovery"] is False


def test_extract_page_tokens():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    html = """
    "LSD",[],{"token":"abc123_lsd"}
    bloks_version":"deadbeef1234567890abcdef12345678"
    "__spin_r":1234
    "__spin_b":"spin_b_val"
    "__spin_t":5678
    "hsi":"9999"
    """
    tokens = _extract_page_tokens(html)
    assert tokens["lsd"] == "abc123_lsd"
    assert tokens["bloks_version"] == "deadbeef1234567890abcdef12345678"
    assert tokens["__spin_r"] == "1234"
    assert tokens["__spin_b"] == "spin_b_val"
    assert tokens["__spin_t"] == "5678"
    assert tokens["hsi"] == "9999"


def test_extract_page_tokens_missing_values():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    tokens = _extract_page_tokens("<html><body>nothing here</body></html>")
    assert tokens == {}


def test_advisory_api_pacing_uses_social_control_pool(monkeypatch):
    from trr_backend.db import pg
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _try_advisory_lock_pace

    calls: list[tuple[str, str]] = []

    class FakeCursor:
        def execute(self, _sql: str, _params: Any = None) -> None:
            return None

    @contextmanager
    def fake_db_connection(*, label: str, pool_name: str = "default"):
        calls.append((label, pool_name))
        yield object()

    @contextmanager
    def fake_db_cursor(*, conn: Any = None, label: str = "write-cursor"):
        del conn, label
        yield FakeCursor()

    monkeypatch.setattr(pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(pg, "db_cursor", fake_db_cursor)

    result = _try_advisory_lock_pace(key="advisory-pool", delay_seconds=0.0)

    assert result["acquired"] is True
    assert result["paced"] is True
    assert result["error"] is None
    assert calls == [("instagram-posts-rate-limit-advisory", "social_control")]


def test_file_lock_pacing_reserves_scheduled_starts_without_holding_sleep(monkeypatch, tmp_path):
    import trr_backend.socials.instagram.posts_scrapling.fetcher as fetcher_module

    class FakeClock:
        def __init__(self) -> None:
            self.current = 1_000.0
            self.sleeps: list[float] = []

        def monotonic(self) -> float:
            return self.current

        def sleep(self, seconds: float) -> None:
            self.sleeps.append(seconds)
            self.current += seconds

    clock = FakeClock()
    monkeypatch.setattr(fetcher_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(fetcher_module.time, "sleep", clock.sleep)
    monkeypatch.setattr(fetcher_module, "_global_rate_limit_path", lambda key: str(tmp_path / f"{key}.lock"))
    monkeypatch.setattr(fetcher_module, "_global_rate_cooldown_path", lambda key: str(tmp_path / f"{key}.cooldown"))

    delay = 0.25
    results = [
        fetcher_module._pace_global_api_request(key="proxy-a", delay_seconds=delay),
        fetcher_module._pace_global_api_request(key="proxy-a", delay_seconds=delay),
        fetcher_module._pace_global_api_request(key="proxy-a", delay_seconds=delay),
    ]
    request_start_times = [result["scheduled_at"] for result in results]

    assert request_start_times[1] - request_start_times[0] == pytest.approx(delay)
    assert request_start_times[2] - request_start_times[1] == pytest.approx(delay)
    assert clock.current == pytest.approx(request_start_times[-1])
    assert results[1]["scheduled_sleep_ms"] >= 200
    assert results[1]["reservation_lag_ms"] >= 200
    assert results[1]["lock_held_ms"] < results[1]["scheduled_sleep_ms"]
    assert results[2]["lock_held_ms"] < results[2]["scheduled_sleep_ms"]


def test_advisory_pacing_releases_lock_before_scheduled_sleep(monkeypatch, tmp_path):
    import trr_backend.socials.instagram.posts_scrapling.fetcher as fetcher_module
    from trr_backend.db import pg

    class FakeClock:
        def __init__(self) -> None:
            self.current = 2_000.0

        def monotonic(self) -> float:
            return self.current

        def sleep(self, seconds: float) -> None:
            events.append(("sleep", self.current, seconds))
            self.current += seconds

    events: list[tuple[str, float, float | None]] = []
    clock = FakeClock()

    class FakeCursor:
        def execute(self, sql: str, _params: Any = None) -> None:
            if "pg_advisory_lock" in sql:
                events.append(("lock", clock.current, None))
            if "pg_advisory_unlock" in sql:
                events.append(("unlock", clock.current, None))

    @contextmanager
    def fake_db_connection(*, label: str, pool_name: str = "default"):
        assert (label, pool_name) == ("instagram-posts-rate-limit-advisory", "social_control")
        yield object()

    @contextmanager
    def fake_db_cursor(*, conn: Any = None, label: str = "write-cursor"):
        del conn, label
        yield FakeCursor()

    monkeypatch.setattr(pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(pg, "db_cursor", fake_db_cursor)
    monkeypatch.setattr(fetcher_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(fetcher_module.time, "sleep", clock.sleep)
    monkeypatch.setattr(fetcher_module, "_global_rate_limit_path", lambda key: str(tmp_path / f"{key}.lock"))

    first = fetcher_module._try_advisory_lock_pace(key="advisory-proxy", delay_seconds=0.3)
    second = fetcher_module._try_advisory_lock_pace(key="advisory-proxy", delay_seconds=0.3)

    assert first["acquired"] is True
    assert second["scheduled_sleep_ms"] >= 250
    assert second["lock_held_ms"] < second["scheduled_sleep_ms"]
    assert [event[0] for event in events] == ["lock", "unlock", "lock", "unlock", "sleep"]


def test_build_graphql_headers():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _build_graphql_headers

    headers = _build_graphql_headers(
        referer="https://www.instagram.com/testuser/",
        csrftoken="csrf_val",
        lsd_token="lsd_val",
        bloks_version="bloks_val",
    )
    assert headers["x-csrftoken"] == "csrf_val"
    assert headers["x-fb-lsd"] == "lsd_val"
    assert headers["x-bloks-version-id"] == "bloks_val"
    assert headers["x-fb-friendly-name"] == "PolarisProfilePostsQuery"
    assert headers["x-root-field-name"] == "xdt_api__v1__feed__user_timeline_graphql_connection"
    assert headers["x-requested-with"] == "XMLHttpRequest"
    assert headers["referer"] == "https://www.instagram.com/testuser/"
    assert "x-ig-app-id" in headers


def test_build_graphql_form_data():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _build_graphql_form_data

    data = _build_graphql_form_data(
        username="testuser",
        cursor=None,
        page_size=33,
        viewer_id="123",
        page_tokens={"lsd": "lsd_val", "__spin_r": "1234"},
        doc_id="25645538101792896",
    )
    assert data["doc_id"] == "25645538101792896"
    assert data["fb_api_req_friendly_name"] == "PolarisProfilePostsQuery"
    variables = json.loads(data["variables"])
    assert variables["username"] == "testuser"
    assert variables["first"] == 33
    assert variables["after"] is None
    assert variables["__relay_internal__pv__PolarisImmersiveFeedChainingEnabledrelayprovider"] is True
    assert variables["__relay_internal__pv__PolarisAIGMAccountLabelEnabledrelayprovider"] is False
    assert data["lsd"] == "lsd_val"


def test_result_dataclass():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsFetchResult

    result = InstagramPostsFetchResult(posts=[{"shortcode": "abc"}])
    assert len(result.posts) == 1
    assert result.fetch_failed is False
    assert result.has_next_page is False


def test_runtime_metadata_never_exposes_cookie_values(_mock_scrapling):
    """warmup_cookie_delta previously leaked {name: value} into scrape_jobs.metadata.
    Must now only expose names and counts."""
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="test",
    )
    # Simulate warmup having merged new cookies
    fetcher._warmup_cookie_delta = {"sessionid": "new-sensitive-value", "csrftoken": "secret-token"}

    meta = fetcher.runtime_metadata
    # Cookie values must not appear in the metadata dict, anywhere
    serialized = repr(meta)
    assert "new-sensitive-value" not in serialized
    assert "secret-token" not in serialized
    # Names and count should be reported (sorted alphabetically for deterministic order)
    assert meta.get("warmup_cookie_names") == ["csrftoken", "sessionid"]
    assert meta.get("warmup_cookie_count") == 2
    # The old field must not exist
    assert "warmup_cookie_delta" not in meta


def test_runtime_metadata_reports_delay_and_proxy_session_mode(_mock_scrapling):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="test",
        proxy_config=PostsProxyConfig(
            browser_proxy="http://proxy:8080",
            api_proxy_url="http://proxy:8080",
            proxy_rotator=None,
            fingerprint="proxy:8080:explicit",
            session_mode="explicit",
        ),
    )

    meta = fetcher.runtime_metadata
    assert meta["proxy_session_mode"] == "explicit"
    assert meta["api_delay_seconds"] >= 0


def test_pace_api_requests_records_fallback_pacing_metadata(_mock_scrapling, monkeypatch):
    import asyncio

    import trr_backend.socials.instagram.posts_scrapling.fetcher as fetcher_module
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "sensitive-cookie-value"},
        browser_account_id="test",
    )
    fetcher._api_delay_seconds = 0.1
    fetcher._global_rate_limit_mode_configured = "advisory"

    def fake_advisory_pace(*, key: str, delay_seconds: float) -> dict[str, Any]:
        del key, delay_seconds
        return {
            "acquired": False,
            "paced": True,
            "wait_ms": 12,
            "lock_wait_ms": 12,
            "lock_held_ms": 0,
            "scheduled_sleep_ms": 0,
            "scheduled_at": None,
            "reservation_lag_ms": 0,
            "error": "pg_unavailable",
        }

    def fake_file_pace(*, key: str, delay_seconds: float) -> dict[str, Any]:
        del key, delay_seconds
        return {
            "acquired": True,
            "paced": True,
            "wait_ms": 3,
            "lock_wait_ms": 3,
            "lock_held_ms": 2,
            "scheduled_sleep_ms": 95,
            "scheduled_at": 1234.5,
            "reservation_lag_ms": 98,
            "error": None,
        }

    monkeypatch.setattr(fetcher_module, "_try_advisory_lock_pace", fake_advisory_pace)
    monkeypatch.setattr(fetcher_module, "_pace_global_api_request", fake_file_pace)

    asyncio.run(fetcher._pace_api_requests())

    proxy_pacing = fetcher.runtime_metadata["proxy_pacing"]
    assert proxy_pacing["mode_last_used"] == "file_lock_fallback"
    assert proxy_pacing["advisory_fallback_count"] == 1
    assert proxy_pacing["advisory_last_error"] == "pg_unavailable"
    assert proxy_pacing["advisory_total_wait_ms"] == 12
    assert proxy_pacing["lock_wait_ms"] == 3
    assert proxy_pacing["lock_held_ms"] == 2
    assert proxy_pacing["scheduled_sleep_ms"] == 95
    assert proxy_pacing["scheduled_at"] == 1234.5
    assert proxy_pacing["reservation_lag_ms"] == 98
    assert "sensitive-cookie-value" not in repr(proxy_pacing)


def test_sync_response_cookies_updates_direct_request_state(_mock_scrapling):
    from unittest.mock import MagicMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "old", "ds_user_id": "1"},
        browser_account_id="test",
    )
    response = MagicMock()
    response.cookies = {"csrftoken": "fresh-csrf", "ds_user_id": "2"}

    fetcher._sync_response_cookies(response)

    assert fetcher._raw_cookies["csrftoken"] == "fresh-csrf"
    assert fetcher._raw_cookies["ds_user_id"] == "2"


def test_warmup_emits_structured_log_success(_mock_scrapling, caplog):
    """After a successful warmup, an info log with event=warmup_success should fire."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x"},
        browser_account_id="t",
    )

    # Mock the browser navigation to return a fake response with status 200
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = "<html>LSD token stuff</html>"
    fake_resp.cookies = {"fresh_cookie": "v"}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with caplog.at_level("INFO", logger="socials.instagram.posts_scrapling.fetcher"):
        asyncio.run(fetcher.warmup("bravotv"))

    events = [r for r in caplog.records if getattr(r, "event", None) == "warmup_success"]
    assert len(events) == 1
    assert events[0].account == "bravotv"
    assert events[0].cookie_count >= 1


def test_warmup_raises_auth_error_code(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("auth_failure.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_auth_failed"
    assert exc_info.value.retryable is False


def test_warmup_transport_error_activates_requests_fallback(_mock_scrapling, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    created: dict[str, Any] = {}

    class _FallbackScraper:
        def __init__(self, *, cookies: dict[str, str], browser_account_id: str | None) -> None:
            created["cookies"] = cookies
            created["browser_account_id"] = browser_account_id

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_REQUESTS_FALLBACK_ENABLED", "1")
    monkeypatch.setitem(
        sys.modules,
        "trr_backend.socials.instagram.scraper",
        types.SimpleNamespace(InstagramScraper=_FallbackScraper),
    )

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf"},
        browser_account_id="t",
    )
    fetcher._fetcher.async_fetch = AsyncMock(side_effect=RuntimeError("Page.goto failed"))

    asyncio.run(fetcher.warmup("bravotv"))

    assert fetcher.runtime_metadata["requests_fallback"]["active"] is True
    assert fetcher.runtime_metadata["requests_fallback"]["reason"] == "warmup_transport_failed"
    assert fetcher.runtime_metadata["requests_fallback"]["warmup_error_class"] == "RuntimeError"
    assert created["cookies"]["sessionid"] == "existing"
    assert created["browser_account_id"] == "t"


def test_warmup_raises_no_cookie_when_no_prior_session(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"csrftoken": "csrf"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("no_cookie_warmup.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_no_cookies"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["warmup_cookie_count"] == 0
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_allows_no_new_cookies_when_prior_session_exists(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf"},
        browser_account_id="t",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = _fixture_text("no_cookie_warmup.html")
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup("bravotv"))

    fetcher._rebuild_http_client.assert_awaited_once()


def test_shared_warmup_pool_reuses_tokens_without_second_browser_fetch(_mock_scrapling, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from trr_backend.socials.instagram.posts_scrapling import fetcher as fetcher_mod
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "1")
    fetcher_mod._POSTS_WARMUP_POOL.clear()

    first = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x", "csrftoken": "old", "ds_user_id": "1"},
        browser_account_id="test-account",
    )
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = '"LSD",[],{"token":"pooled_lsd"} bloks_version":"deadbeef1234567890abcdef12345678"'
    fake_resp.cookies = {"csrftoken": "fresh-csrf"}
    first._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    asyncio.run(first.warmup("bravotv"))

    second = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "x", "csrftoken": "old", "ds_user_id": "1"},
        browser_account_id="test-account",
    )
    second._fetcher.async_fetch = AsyncMock(side_effect=AssertionError("warmup should come from pool"))

    asyncio.run(second.warmup("bravotv"))

    assert second.runtime_metadata["warmup_pool"]["hit"] is True
    assert set(second.runtime_metadata["page_tokens_found"]) == {"bloks_version", "lsd"}
    assert second._raw_cookies["csrftoken"] == "fresh-csrf"


def test_doc_id_pin_tries_successful_id_first_after_initial_fallback(_mock_scrapling, monkeypatch):
    import asyncio

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf", "ds_user_id": "123"},
        browser_account_id="t",
    )
    fetcher._doc_ids_configured = ("stale-a", "stale-b", "healthy-c")

    attempted: list[str] = []

    async def fake_fetch_json_response(*_args, **kwargs):
        doc_id = kwargs["data"]["doc_id"]
        attempted.append(doc_id)
        if doc_id != "healthy-c":
            return {
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
                "payload": {"data": {}},
            }
        return {
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
            "payload": {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "1", "code": "AAA"}}],
                        "page_info": {"has_next_page": True, "end_cursor": "cursor-1"},
                    }
                }
            },
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", fake_fetch_json_response)

    first = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    second = asyncio.run(fetcher.fetch_posts_page("bravotv", cursor=first.end_cursor))

    assert [post["code"] for post in first.posts] == ["AAA"]
    assert [post["code"] for post in second.posts] == ["AAA"]
    assert attempted == ["stale-a", "stale-b", "healthy-c", "healthy-c"]
    telemetry = fetcher.runtime_metadata["profile_posts_doc_ids"]
    assert telemetry["final_selected"] == "healthy-c"
    assert telemetry["attempts"] == {"healthy-c": 2, "stale-a": 1, "stale-b": 1}
    assert telemetry["successes"] == {"healthy-c": 2}


def test_empty_page_with_pagination_state_is_doc_id_stale_not_complete(_mock_scrapling, monkeypatch):
    import asyncio

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf", "ds_user_id": "123"},
        browser_account_id="t",
    )
    fetcher._doc_ids_configured = ("stale-doc",)

    async def fake_fetch_json_response(*_args, **_kwargs):
        return {
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
            "payload": {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [],
                        "page_info": {"has_next_page": True, "end_cursor": "cursor-still-present"},
                    }
                }
            },
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", fake_fetch_json_response)

    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))

    assert result.fetch_failed is True
    assert result.fetch_reason == "pagination_doc_id_stale"
    assert result.has_next_page is False
    assert result.end_cursor is None
    telemetry = fetcher.runtime_metadata["profile_posts_doc_ids"]
    assert telemetry["pagination_doc_id_stale_count"] == 1
    assert telemetry["empty_connection_count"] == {"stale-doc": 1}


def test_bidirectional_probe_metadata_records_disabled_failure_shape(monkeypatch):
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", raising=False)
    from trr_backend.socials.instagram.posts_scrapling.fetcher import build_bidirectional_probe_metadata

    result = build_bidirectional_probe_metadata(
        request_shape={"variables": {"before": "cursor-old", "last": 33}},
        forward_posts=[{"id": "1"}, {"id": "2"}],
        reverse_posts=[{"id": "2"}, {"id": "3"}],
        cursor_fields={"before": "cursor-old", "has_previous_page": True},
    )

    assert result.enabled is False
    assert result.passed is False
    assert result.reason == "bidirectional_walk_disabled"
    assert result.request_shape["variables"]["last"] == 33
    assert result.response_order == ["2", "3"]
    assert result.overlap_count == 1
    assert result.cursor_fields["has_previous_page"] is True


def test_bidirectional_probe_metadata_records_pass_and_overlap_failure():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import build_bidirectional_probe_metadata

    passing = build_bidirectional_probe_metadata(
        enabled=True,
        request_shape={"variables": {"before": "cursor-old", "last": 33}},
        forward_posts=[{"id": "1"}, {"id": "2"}],
        reverse_posts=[{"id": "9"}, {"id": "8"}],
    )
    duplicate = build_bidirectional_probe_metadata(
        enabled=True,
        request_shape={"variables": {"before": "cursor-old", "last": 33}},
        forward_posts=[{"id": "1"}, {"id": "2"}],
        reverse_posts=[{"id": "1"}, {"id": "2"}],
    )

    assert passing.passed is True
    assert passing.reason == "reverse_probe_passed"
    assert duplicate.passed is False
    assert duplicate.reason == "reverse_probe_duplicate_forward_page"


def test_rate_limit_key_shards_by_proxy_only_when_per_ip_pacing_enabled(monkeypatch):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _global_rate_limit_key
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

    proxy_a = PostsProxyConfig(
        browser_proxy="http://user:pass@proxy-a:8080",
        api_proxy_url="http://user:pass@proxy-a:8080",
        proxy_rotator=None,
        fingerprint="proxy-a:8080:explicit",
        session_mode="explicit",
    )
    proxy_b = PostsProxyConfig(
        browser_proxy="http://user:pass@proxy-b:8080",
        api_proxy_url="http://user:pass@proxy-b:8080",
        proxy_rotator=None,
        fingerprint="proxy-b:8080:explicit",
        session_mode="explicit",
    )

    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", raising=False)
    assert _global_rate_limit_key(proxy_a) == _global_rate_limit_key(proxy_b)

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", "1")
    assert _global_rate_limit_key(proxy_a) != _global_rate_limit_key(proxy_b)


def test_proxy_pacing_identity_updates_from_observed_response_header(_mock_scrapling, monkeypatch):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", "1")
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="t",
        proxy_config=PostsProxyConfig(
            browser_proxy="http://user:pass@proxy-a:8080",
            api_proxy_url="http://user:pass@proxy-a:8080",
            proxy_rotator=None,
            fingerprint="proxy-a:8080:explicit",
            session_mode="explicit",
        ),
    )
    initial_key = fetcher.runtime_metadata["proxy_pacing"]["global_rate_limit_key"]
    response = MagicMock()
    response.status_code = 200
    response.text = "{}"
    response.headers = {"x-trr-proxy-ip": "203.0.113.10"}

    fetcher._record_proxy_response(response)
    metadata = fetcher.runtime_metadata

    assert metadata["proxy_pacing"]["global_rate_limit_key"] != initial_key
    assert metadata["proxy_pacing"]["identity"]["observed_identity"] == "203.0.113.10"
    assert metadata["proxy_pacing"]["identity"]["observed_fingerprint"]


def test_bidirectional_probe_executes_reverse_request_when_enabled(_mock_scrapling, monkeypatch):
    import asyncio

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", "1")
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf", "ds_user_id": "123"},
        browser_account_id="t",
    )
    fetcher._doc_id_used = "healthy-doc"

    requests: list[dict[str, str]] = []

    async def fake_fetch_json_response(*_args, **kwargs):
        requests.append(kwargs["data"])
        return {
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
            "payload": {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "old-1", "code": "OLD"}}],
                        "page_info": {
                            "has_previous_page": False,
                            "has_next_page": True,
                            "start_cursor": "start-old",
                            "end_cursor": "end-old",
                        },
                    }
                }
            },
        }

    monkeypatch.setattr(fetcher, "_fetch_json_response", fake_fetch_json_response)

    metadata = asyncio.run(fetcher.probe_bidirectional_walk("bravotv", forward_posts=[{"id": "new-1"}]))

    variables = json.loads(requests[0]["variables"])
    assert variables["first"] is None
    assert variables["last"] == fetcher._page_size
    assert metadata["passed"] is True
    assert metadata["reason"] == "reverse_probe_passed"
    assert metadata["response_order"] == ["old-1"]
