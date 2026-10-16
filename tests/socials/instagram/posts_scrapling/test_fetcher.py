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


def _document_context_html(tokens: list[str], *, nested: bool = True) -> str:
    definitions = [["DTSGInitialData", [], {"token": token}, 123] for token in tokens]
    box = {"define": definitions}
    if nested:
        box["require"] = [
            [
                "Nested",
                "call",
                None,
                [
                    {
                        "data": {
                            "__bbox": {
                                "define": [["DTSGInitialData", [], {"token": "nested-secret"}, 456]],
                            }
                        }
                    }
                ],
            ]
        ]
    root = {
        "require": [
            [
                "UnspecifiedWrapper",
                "method",
                {"not_null": True},
                [{"__bbox": box}, {"extra_argument": True}],
                "extra_element",
            ]
        ],
        "haste_session": "document-hs",
        "hsi": "900",
    }
    return '<script type="application/json" data-sjs>' + json.dumps(root, separators=(",", ":")) + "</script>"


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

    captured: dict[str, Any] = {}

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


def test_document_context_selects_unique_direct_definition_without_wrapper_assumptions():
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    tokens = _extract_page_tokens(_document_context_html(["direct-secret"]))
    assert tokens["fb_dtsg"] == "direct-secret"
    assert tokens["__hs"] == "document-hs" and tokens["hsi"] == "900"
    assert "nested-secret" not in tokens.values()


@pytest.mark.parametrize(
    "tokens",
    [
        [],
        ["one", "two"],
        ["same", "same"],
        [""],
        ["placeholder"],
        [" bad "],
        pytest.param(["\ud800"], id="json-escaped-surrogate"),
    ],
)
def test_document_context_missing_or_ambiguous_dtsg_remains_optional(tokens):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    result = _extract_page_tokens(_document_context_html(tokens))
    assert "fb_dtsg" not in result
    assert result["__hs"] == "document-hs" and result["hsi"] == "900"


@pytest.mark.parametrize(
    "html",
    [
        '<script type="application/json" data-sjs>{broken</script>',
        _document_context_html(["one"]) + _document_context_html(["two"]),
        _document_context_html(["one"]).replace("data-sjs", "data-other"),
        _document_context_html(["one"]).replace('"token":"one"', '"token":null'),
        _document_context_html(["one"]).replace('["DTSGInitialData",[],', '["DTSGInitialData",["dependency"],'),
        "\ud800",
    ],
)
def test_document_context_malformed_or_non_document_scope_never_guesses(html):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_document_dtsg

    assert _extract_document_dtsg(html) is None


@pytest.mark.parametrize("bound,value", [("_DOCUMENT_CONTEXT_MAX_BYTES", 8), ("_DOCUMENT_CONTEXT_MAX_ITEMS", 1)])
def test_document_context_input_limits_omit_token(monkeypatch, bound, value):
    from trr_backend.socials.instagram.posts_scrapling import fetcher as module

    monkeypatch.setattr(module, bound, value)
    assert module._extract_document_dtsg(_document_context_html(["one"])) is None


def test_advisory_api_pacing_uses_session_control_pool(monkeypatch):
    from trr_backend.db import pg
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _try_advisory_lock_pace

    calls: list[tuple[str, str]] = []
    queries: list[str] = []

    class FakeCursor:
        def execute(self, sql: str, _params: Any = None) -> None:
            queries.append(sql)
            return None

        def fetchone(self) -> list[float]:
            return [0.0]

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
    assert calls == [("instagram-posts-rate-limit-pace", "session_control")]
    assert any("social.ig_posts_rate_pace" in query for query in queries)
    assert not any("pg_advisory_lock" in query for query in queries)


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


def test_advisory_pacing_returns_connection_before_scheduled_sleep(monkeypatch, tmp_path):
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
    remaining_by_call = [0.0, 0.3]

    class FakeCursor:
        def execute(self, sql: str, _params: Any = None) -> None:
            assert "social.ig_posts_rate_pace" in sql
            assert "pg_advisory_lock" not in sql

        def fetchone(self) -> list[float]:
            return [remaining_by_call.pop(0)]

    @contextmanager
    def fake_db_connection(*, label: str, pool_name: str = "default"):
        assert (label, pool_name) == ("instagram-posts-rate-limit-pace", "session_control")
        events.append(("connect_enter", clock.current, None))
        try:
            yield object()
        finally:
            events.append(("connect_exit", clock.current, None))

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
    assert [event[0] for event in events] == [
        "connect_enter",
        "connect_exit",
        "connect_enter",
        "connect_exit",
        "sleep",
    ]


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
    assert headers["x-fb-friendly-name"] == "PolarisProfilePostsTabContentQuery_connection"
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
    assert data["fb_api_req_friendly_name"] == "PolarisProfilePostsTabContentQuery_connection"
    variables = json.loads(data["variables"])
    assert variables["username"] == "testuser"
    assert variables["first"] == 33
    assert variables["after"] is None
    assert variables["include_multi_captions"] is True
    assert variables["__relay_internal__pv__PolarisMultiCaptionCarouselEnabledrelayprovider"] is True
    assert variables["__relay_internal__pv__PolarisShortDramaEnabledrelayprovider"] is True
    assert variables["__relay_internal__pv__PolarisThreadsStyleCommentsEnabledrelayprovider"] is False
    assert variables["__relay_internal__pv__PolarisReelsRecoDebugOverlayEnabledrelayprovider"] is False
    assert "__relay_internal__pv__PolarisImmersiveFeedChainingEnabledrelayprovider" not in variables
    assert "__relay_internal__pv__PolarisAIGMAccountLabelEnabledrelayprovider" not in variables
    assert set(variables) == {
        "after",
        "before",
        "data",
        "first",
        "last",
        "username",
        "include_multi_captions",
        "__relay_internal__pv__PolarisMultiCaptionCarouselEnabledrelayprovider",
        "__relay_internal__pv__PolarisShortDramaEnabledrelayprovider",
        "__relay_internal__pv__PolarisThreadsStyleCommentsEnabledrelayprovider",
        "__relay_internal__pv__PolarisReelsRecoDebugOverlayEnabledrelayprovider",
    }
    assert data["lsd"] == "lsd_val"
    assert data["av"] == "123" and data["__user"] == "0"
    assert "fb_dtsg" not in data and "jazoest" not in data


@pytest.mark.parametrize(
    "direction,cursor", [("forward", None), ("forward", "next-cursor"), ("reverse", "previous-cursor")]
)
def test_document_context_form_preserves_viewer_and_cursor_with_dtsg_checksum(direction, cursor):
    from trr_backend.socials.instagram.posts_scrapling.fetcher import _build_graphql_form_data, _extract_page_tokens

    tokens = _extract_page_tokens(_document_context_html(["direct-secret"]))
    tokens.update({"lsd": "unrelated-lsd", "__spin_r": "123", "__spin_b": "trunk", "__spin_t": "456"})
    data = _build_graphql_form_data(
        username="bravotv",
        cursor=cursor,
        page_size=10,
        viewer_id="viewer",
        page_tokens=tokens,
        doc_id="unchanged-doc",
        direction=direction,
    )
    assert data["av"] == "viewer" and data["__user"] == "0"
    assert data["fb_dtsg"] == "direct-secret"
    assert data["jazoest"] == "2" + str(sum(map(ord, "direct-secret")))
    assert data["__hs"] == "document-hs" and data["__hsi"] == "900" and "hsi" not in data
    assert all(data[key] == tokens[key] for key in ("lsd", "__spin_r", "__spin_b", "__spin_t"))
    variables = json.loads(data["variables"])
    assert variables["after"] == (cursor if direction == "forward" else None)
    assert variables["before"] == (cursor if direction == "reverse" else None)
    assert variables["first"] == (10 if direction == "forward" else None)
    assert variables["last"] == (10 if direction == "reverse" else None)
    assert data["doc_id"] == "unchanged-doc" and "dpr" not in data


@pytest.mark.parametrize(
    "override,expected",
    [
        (None, ("39229011733413759",)),
        ("", ("39229011733413759",)),
        ("bad,not-a-document", ("39229011733413759",)),
        (" 123,456,123,bad ", ("123", "456")),
    ],
)
def test_connection_contract_default_and_explicit_override_do_not_change_legacy_constants(
    _mock_scrapling, monkeypatch, override, expected
):
    from trr_backend.socials.instagram import constants
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    legacy_ids, legacy_name = constants.PROFILE_POSTS_DOC_IDS, constants.PROFILE_POSTS_FRIENDLY_NAME
    if override is None:
        monkeypatch.delenv("SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS", raising=False)
    else:
        monkeypatch.setenv("SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS", override)
    fetcher = InstagramPostsScraplingFetcher(cookies=[], raw_cookies={}, browser_account_id=None)
    assert fetcher._doc_ids_configured == expected
    assert constants.PROFILE_POSTS_DOC_IDS == legacy_ids and constants.PROFILE_POSTS_FRIENDLY_NAME == legacy_name


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


@pytest.mark.parametrize(
    "body,rejected",
    [
        ("<html>profile</html>", False),
        ('<form action="/accounts/login/"><input name="username"><input type="password"></form>', True),
        ('<script>var login = "private-token"</script>', False),
    ],
)
@pytest.mark.parametrize("callback_fails", [None, RuntimeError, SystemExit])
def test_safe_diagnostic_preserves_warmup_classification_and_single_operation(
    _mock_scrapling, monkeypatch, body, rejected, callback_fails
):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "0")
    events, requests = [], []

    def observe(value):
        events.append(value)
        if callback_fails:
            raise callback_fails("private callback text must not escape")

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "private-token", "ds_user_id": "123"},
        browser_account_id="test",
        consume_credentials_only=True,
        allow_requests_recovery=False,
        before_source_request=requests.append,
        source_diagnostic_observer=observe,
    )
    response = SimpleNamespace(
        status_code=200,
        text=body,
        cookies={},
        headers={},
        url="https://www.instagram.com/bravotv/?secret=private-token",
    )
    fetcher._fetcher.async_fetch = AsyncMock(return_value=response)
    fetcher._fetch_graphql = AsyncMock()
    if rejected:
        with pytest.raises(InstagramPostsWarmupError) as error:
            asyncio.run(fetcher.warmup("bravotv"))
        assert error.value.error_code == "instagram_posts_warmup_auth_failed"
    else:
        asyncio.run(fetcher.warmup("bravotv"))
    assert requests == ["browser_warmup"] and fetcher._request_count == 1
    assert fetcher._fetcher.async_fetch.await_count == 1
    assert fetcher._fetcher.async_fetch.await_args.kwargs["retries"] == 1
    fetcher._fetch_graphql.assert_not_awaited()
    assert len(events) == 2
    assert events[-1] == {
        "phase": "browser_warmup",
        "event": "warmup_classification",
        "http_status": 200,
        "origin_class": "instagram",
        "path_class": "profile",
        "classification_path_class": "login" if rejected else "profile",
        "location_path_class": "none",
        "classification": "auth_rejected" if rejected else "not_rejected",
        "auth_body_marker": rejected,
    }
    serialized = json.dumps(events)
    assert all(secret not in serialized for secret in ("private-token", "bravotv", "https:", "secret=", "<script>"))


def test_safe_diagnostic_restricts_values_and_contains_callback_failure(_mock_scrapling):
    from types import SimpleNamespace

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    events = []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[], raw_cookies={}, browser_account_id=None, source_diagnostic_observer=events.append
    )
    response = SimpleNamespace(status_code=999, url="https://private.invalid/secret/value?token=secret", headers={})
    fetcher._emit_source_diagnostic(
        response,
        phase="private-token",
        event="private-token",
        classification="private-token",
        auth_body_marker="secret",
        classification_path_class="private-token",
    )
    assert events == [
        {
            "phase": "unknown",
            "event": "unknown",
            "http_status": None,
            "origin_class": "external",
            "path_class": "other",
            "classification_path_class": None,
            "location_path_class": "none",
            "classification": "unknown",
            "auth_body_marker": None,
        }
    ]
    assert fetcher._request_count == 0


def test_safe_diagnostic_reports_graphql_response_before_auth_observer(_mock_scrapling):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    events = []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={},
        browser_account_id=None,
        source_diagnostic_observer=lambda value: events.append(value),
        source_failure_observer=lambda code, retry_after: events.append(code),
    )
    response = SimpleNamespace(
        status_code=401,
        url="https://www.instagram.com/graphql/query/?token=private-token",
        headers={},
        cookies={},
        text="private-token",
    )
    fetcher._http_client = SimpleNamespace(post=AsyncMock(return_value=response))
    fetcher._pace_api_requests = AsyncMock()
    assert (
        asyncio.run(fetcher._fetch_graphql("https://www.instagram.com/graphql/query/", data={}, headers={})) is response
    )
    assert events[0]["phase"] == "graphql" and events[0]["http_status"] == 401
    assert events[1] == "source_auth_failed"
    assert fetcher._request_count == 1 and fetcher._http_client.post.await_count == 1
    assert "private-token" not in json.dumps(events)


def _guarded_graphql_fetcher(_mock_scrapling, text, *, status=200):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import httpx

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    events = []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "fixture", "ds_user_id": "123"},
        browser_account_id="test",
        consume_credentials_only=True,
        allow_requests_recovery=False,
        source_payload_observer=lambda kind, payload: events.append(("retained", payload)),
        source_failure_observer=lambda code, retry: events.append(("failure", code)),
    )
    response = httpx.Response(
        status, text=text, request=httpx.Request("POST", "https://www.instagram.com/graphql/query")
    )
    fetcher._http_client = SimpleNamespace(post=AsyncMock(return_value=response), cookies=httpx.Cookies())
    fetcher._pace_api_requests = AsyncMock()
    fetcher._doc_ids_configured = ("fixture-doc",)
    return fetcher, events


def _native_graphql_page():
    return {
        "status": "ok",
        "data": {
            "xdt_api__v1__feed__user_timeline_graphql_connection": {
                "edges": [{"node": {"code": "fixture", "caption": "login checkpoint challenge are incidental words"}}],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        },
    }


@pytest.mark.parametrize("errors,rejected", [([], False), ([{"message": "source query rejected"}], True)])
def test_document_context_normal_parser_uses_error_truth_and_one_request(_mock_scrapling, errors, rejected):
    import asyncio

    from trr_backend.socials.instagram.posts_scrapling.fetcher import _extract_page_tokens

    payload = _native_graphql_page()
    payload["errors"] = errors
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    fetcher._page_tokens = _extract_page_tokens(_document_context_html(["direct-secret"]))
    operations = []
    fetcher._before_source_request = operations.append
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed is rejected and result.auth_failed is False
    assert len(result.posts) == (0 if rejected else 1)
    assert result.fetch_reason == ("graphql_error" if rejected else None)
    assert events == [("retained", payload)]
    assert operations == ["graphql"] and fetcher._request_count == fetcher._http_client.post.await_count == 1
    form = fetcher._http_client.post.await_args.kwargs["data"]
    assert form["fb_dtsg"] == "direct-secret" and form["__user"] == "0"
    assert "hsi" not in form and form["__hsi"] == "900"


def _optional_location_error(index=0):
    return {
        "severity": "ERROR",
        "message": "A server field error occurred",
        "path": [
            "xdt_api__v1__feed__user_timeline_graphql_connection",
            "edges",
            index,
            "node",
            "location",
            "profile_pic_url",
        ],
    }


def test_connection_contract_two_pages_accept_only_ancillary_error_after_retaining_source(_mock_scrapling, monkeypatch):
    import asyncio

    import httpx

    monkeypatch.delenv("SOCIAL_INSTAGRAM_PROFILE_POSTS_DOC_IDS", raising=False)
    first = _native_graphql_page()
    second = _native_graphql_page()
    first["errors"] = [_optional_location_error()]
    second["errors"] = [_optional_location_error()]
    root = "xdt_api__v1__feed__user_timeline_graphql_connection"
    first["data"][root]["page_info"] = {"has_next_page": True, "end_cursor": "next-page"}
    for page in (first, second):
        page["data"][root]["edges"][0]["node"]["location"] = {"id": "place", "profile_pic_url": None}
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "")
    fetcher._doc_ids_configured = ("39229011733413759",)
    fetcher._http_client.post.side_effect = [
        httpx.Response(
            200,
            text="for(;;);" + json.dumps(page),
            request=httpx.Request("POST", "https://www.instagram.com/graphql/query"),
        )
        for page in (first, second)
    ]
    operations = []
    fetcher._before_source_request = operations.append

    async def execute():
        initial = await fetcher.fetch_posts_page("bravotv")
        following = await fetcher.fetch_posts_page("bravotv", cursor=initial.end_cursor)
        return initial, following

    initial, following = asyncio.run(execute())
    assert not initial.fetch_failed and initial.has_next_page and initial.end_cursor == "next-page"
    assert not following.fetch_failed and not following.has_next_page
    assert events == [("retained", first), ("retained", second)]
    assert operations == ["graphql", "graphql"] and fetcher._request_count == 2
    calls = fetcher._http_client.post.await_args_list
    assert len(calls) == 2
    for call in calls:
        assert call.kwargs["data"]["doc_id"] == "39229011733413759"
        assert call.kwargs["headers"]["x-fb-friendly-name"] == call.kwargs["data"]["fb_api_req_friendly_name"]
        assert call.kwargs["data"]["fb_api_req_friendly_name"] == "PolarisProfilePostsTabContentQuery_connection"
    assert json.loads(calls[0].kwargs["data"]["variables"])["after"] is None
    assert json.loads(calls[1].kwargs["data"]["variables"])["after"] == "next-page"


@pytest.mark.parametrize(
    "variant", ["unknown_path", "pathless", "boolean_index", "missing_node", "auth", "nested_auth", "nested_rate"]
)
def test_connection_contract_partial_data_does_not_hide_legitimate_failure(_mock_scrapling, variant):
    import asyncio

    payload = _native_graphql_page()
    payload["errors"] = [_optional_location_error()]
    root = "xdt_api__v1__feed__user_timeline_graphql_connection"
    if variant == "unknown_path":
        payload["errors"][0]["path"][-1] = "name"
    elif variant == "pathless":
        payload["errors"][0].pop("path")
    elif variant == "boolean_index":
        payload["errors"][0]["path"][2] = False
    elif variant == "missing_node":
        payload["data"][root]["edges"][0]["node"] = None
    elif variant == "nested_auth":
        payload["errors"][0]["message"] = "login_required"
    elif variant == "nested_rate":
        payload["errors"][0]["extensions"] = {"code": "rate_limited"}
    else:
        payload["status"] = "fail"
        payload["message"] = "login_required"
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and result.posts == []
    assert result.auth_failed is (variant in {"auth", "nested_auth"})
    assert events[0] == ("retained", payload)
    expected_failures = (
        [("failure", "source_auth_failed")]
        if variant in {"auth", "nested_auth"}
        else [("failure", "rate_limited")]
        if variant == "nested_rate"
        else []
    )
    assert events[1:] == expected_failures
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


def test_connection_contract_reverse_partial_error_requires_reverse_pagination_proof(_mock_scrapling):
    import asyncio

    payload = _native_graphql_page()
    payload["errors"] = [_optional_location_error()]
    root = "xdt_api__v1__feed__user_timeline_graphql_connection"
    payload["data"][root]["page_info"] = {"has_previous_page": False, "start_cursor": None}
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv", cursor="before-cursor", direction="reverse"))
    assert not result.fetch_failed and len(result.posts) == 1
    assert events == [("retained", payload)]
    variables = json.loads(fetcher._http_client.post.await_args.kwargs["data"]["variables"])
    assert variables["before"] == "before-cursor" and variables["after"] is None
    assert variables["first"] is None and variables["last"] == fetcher._page_size
    assert fetcher._request_count == 1


@pytest.mark.parametrize("container", ["error", "extensions"])
@pytest.mark.parametrize(
    "marker,auth,callback",
    [
        ({"permission_denied": True}, True, "source_auth_failed"),
        ({"rate_limited": True}, False, "rate_limited"),
        ({"error": 1357001}, True, "source_auth_failed"),
    ],
)
def test_nested_machine_failure_retained_before_one_callback_and_request(
    _mock_scrapling, container, marker, auth, callback
):
    import asyncio

    payload = _native_graphql_page()
    error = _optional_location_error()
    if container == "extensions":
        error["extensions"] = marker
    else:
        error.update(marker)
    payload["errors"] = [error]
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    operations = []
    fetcher._before_source_request = operations.append

    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))

    assert result.fetch_failed and result.posts == [] and result.auth_failed is auth
    assert events == [("retained", payload), ("failure", callback)]
    assert operations == ["graphql"]
    assert result.request_count == fetcher._request_count == fetcher._http_client.post.await_count == 1


@pytest.mark.parametrize("auth_state", ["authenticated", "public"])
@pytest.mark.parametrize(
    "tokens", [[], ["ambiguous-a", "ambiguous-b"], pytest.param(["\ud800"], id="json-escaped-surrogate")]
)
def test_document_context_absent_dtsg_preserves_warmup_and_request_budget(
    _mock_scrapling, monkeypatch, auth_state, tokens
):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import httpx

    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "0")
    operations = []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "fixture", "ds_user_id": "viewer"},
        browser_account_id="test",
        auth_state=auth_state,
        consume_credentials_only=True,
        allow_requests_recovery=False,
        before_source_request=operations.append,
    )
    fetcher._fetcher.async_fetch = AsyncMock(
        return_value=SimpleNamespace(
            status_code=200,
            text=_document_context_html(tokens),
            cookies={},
            headers={},
            url="https://www.instagram.com/bravotv/",
        )
    )
    response = httpx.Response(
        200, json=_native_graphql_page(), request=httpx.Request("POST", "https://www.instagram.com/graphql/query")
    )
    fetcher._http_client = SimpleNamespace(post=AsyncMock(return_value=response), cookies=httpx.Cookies())
    fetcher._rebuild_http_client = AsyncMock()
    fetcher._pace_api_requests = AsyncMock()
    fetcher._doc_ids_configured = ("fixture-doc",)

    async def execute():
        await fetcher.warmup("bravotv")
        return await fetcher.fetch_posts_page("bravotv")

    result = asyncio.run(execute())
    assert not result.fetch_failed and len(result.posts) == 1
    assert operations == ["browser_warmup", "graphql"] and result.request_count == 2
    assert fetcher._fetcher.async_fetch.await_count == fetcher._http_client.post.await_count == 1
    form = fetcher._http_client.post.await_args.kwargs["data"]
    assert "fb_dtsg" not in form and "jazoest" not in form
    assert form["av"] == ("0" if auth_state == "public" else "viewer") and form["__user"] == "0"


@pytest.mark.parametrize("prefix", ["", "for (;;);", "for(;;);", "\ufeff \nfor(;;);\n"])
def test_guarded_structured_graphql_success_retains_evidence_without_text_auth(_mock_scrapling, prefix):
    import asyncio

    payload = _native_graphql_page()
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, prefix + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert not result.fetch_failed and not result.auth_failed and len(result.posts) == 1
    assert not result.has_next_page
    assert events == [("retained", payload)]
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


@pytest.mark.parametrize("with_connection", [False, True])
def test_guarded_graphql_1357001_retained_before_one_auth_failure(_mock_scrapling, with_connection):
    import asyncio

    payload = {"error": 1357001, "errorSummary": "Log in to continue"}
    if with_connection:
        payload.update(_native_graphql_page())
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and result.auth_failed and result.posts == []
    assert result.fetch_reason == "login_required" and not result.retryable
    assert events == [("retained", payload), ("failure", "source_auth_failed")]
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


@pytest.mark.parametrize(
    "payload,reason,auth",
    [
        ({"error": 42, "errorDescription": "incidental login word"}, "graphql_error", False),
        ({"errors": [{"message": "caption mentions a challenge"}]}, "graphql_error", False),
        ({"status": "fail", "message": "incidental login challenge word"}, "fail", False),
        ({"status": "fail", "message": "login_required"}, "fail", True),
        ({"status": "fail", "error_type": "checkpoint_required"}, "fail", True),
        ({"status": "untrusted raw status text", "message": "login_required"}, "api_status_fail", False),
        ({"status": "ok", "data": {}}, "graphql_empty_connection", False),
    ],
)
def test_guarded_graphql_error_or_missing_connection_never_succeeds(_mock_scrapling, payload, reason, auth):
    import asyncio

    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for (;;);" + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and result.posts == [] and result.auth_failed is auth
    assert result.fetch_reason == reason
    assert events[0] == ("retained", payload)
    assert events[1:] == ([("failure", "source_auth_failed")] if auth else [])
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


@pytest.mark.parametrize(
    "text", ["for(;;);not JSON login challenge", "for (;;);[1,2]", "for(;;);null", 'while(1);{"status":"ok"}']
)
def test_guarded_graphql_invalid_or_unrecognized_payload_stays_unresolved(_mock_scrapling, text):
    import asyncio

    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, text)
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and not result.auth_failed and result.posts == []
    assert result.fetch_reason == "non_json_response" and not result.retryable
    assert events == []
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


@pytest.mark.parametrize(
    "body,auth",
    [
        ('<html><script>const login = "checkpoint challenge"</script></html>', False),
        ('<form action="/accounts/login/"><input name="username"><input type="password"></form>', True),
    ],
)
def test_graphql_html_failure_needs_structured_auth_evidence(_mock_scrapling, body, auth):
    import asyncio

    fetcher, _ = _guarded_graphql_fetcher(_mock_scrapling, body)
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and result.auth_failed is auth and result.posts == []
    assert result.fetch_reason == "html_challenge_or_auth_required"
    assert fetcher._request_count == 1


def test_graphql_http400_exact_checkpoint_remains_auth_failure(_mock_scrapling):
    import asyncio

    fetcher, events = _guarded_graphql_fetcher(
        _mock_scrapling, '{"status":"fail","message":"checkpoint_required"}', status=400
    )
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and result.auth_failed and result.posts == []
    assert result.fetch_reason == "http_400"
    assert events == [("failure", "source_auth_failed")]
    assert fetcher._request_count == 1


@pytest.mark.parametrize(
    "connection",
    [
        [],
        "invalid",
        {"edges": None, "page_info": {"has_next_page": False}},
        {"edges": {}, "page_info": {"has_next_page": False}},
        {"edges": [{"node": None}], "page_info": {"has_next_page": False}},
        {"edges": [None], "page_info": {"has_next_page": False}},
        {"edges": [{"node": {}}], "page_info": {"has_next_page": False}},
        {"edges": [{"node": "invalid"}], "page_info": {"has_next_page": False}},
        {"edges": [{"node": {"code": "valid"}}, {"node": None}], "page_info": {"has_next_page": False}},
        {"edges": []},
        {"edges": [], "page_info": {}},
        {"edges": [], "page_info": {"has_next_page": "false"}},
        {"edges": [], "page_info": {"has_next_page": 0}},
        {"edges": [], "page_info": {"has_next_page": True}},
        {"edges": [], "page_info": {"has_next_page": False, "end_cursor": 23}},
        {"edges": [], "page_info": {"has_next_page": False, "end_cursor": " "}},
    ],
)
def test_guarded_malformed_connection_is_retained_and_fails_without_more_requests(_mock_scrapling, connection):
    import asyncio

    payload = {"data": {"xdt_api__v1__feed__user_timeline_graphql_connection": connection}}
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    fetcher._doc_ids_configured = ("first-doc", "must-not-be-requested")
    result = asyncio.run(fetcher.fetch_posts_page("bravotv"))
    assert result.fetch_failed and not result.auth_failed and result.posts == []
    assert result.fetch_reason == "graphql_invalid_connection"
    assert not result.has_next_page and not result.retryable
    assert events == [("retained", payload)]
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1
    assert fetcher._doc_id_used is None and not fetcher._doc_id_success_counts


@pytest.mark.parametrize("reverse", [False, True])
def test_guarded_valid_empty_connection_requires_explicit_direction_terminal(_mock_scrapling, reverse):
    import asyncio

    info = (
        {"has_previous_page": False, "start_cursor": None} if reverse else {"has_next_page": False, "end_cursor": None}
    )
    payload = {"data": {"xdt_api__v1__feed__user_timeline_graphql_connection": {"edges": [], "page_info": info}}}
    fetcher, events = _guarded_graphql_fetcher(_mock_scrapling, "for(;;);" + json.dumps(payload))
    result = asyncio.run(fetcher.fetch_posts_page("bravotv", direction="reverse" if reverse else "forward"))
    assert not result.fetch_failed and not result.auth_failed and result.posts == []
    assert not result.has_next_page and result.end_cursor is None
    assert events == [("retained", payload)]
    assert fetcher._request_count == fetcher._http_client.post.await_count == 1


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
    fake_resp.text = '<form action="/accounts/login/"><input name="username"><input type="password"></form>'
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_auth_failed"
    assert exc_info.value.retryable is False


@pytest.mark.parametrize(
    "body,body_path",
    [
        ('<form action="/accounts/login/"><input name="username"><input name="password"></form>', "login"),
        (
            '<form action="https://www.instagram.com/accounts/login/ajax/">'
            '<input name="username"><input type="password"></form>',
            "login",
        ),
        ('<form action="/challenge/123/"><input name="security_code"><button>Continue</button></form>', "checkpoint"),
        ('<form action="https://www.instagram.com/checkpoint/"><button>Confirm</button></form>', "checkpoint"),
        ('<meta http-equiv="refresh" content="0;url=/challenge/123/">', "checkpoint"),
        ('{"status":"fail","message":"login_required"}', "login"),
        ('{"status":"fail","error_type":"challenge_required"}', "checkpoint"),
        ('<script>const login = "checkpoint challenge";</script><style>.login {color:red}</style>', None),
        (
            '<script>const template = \'<form action="/accounts/login/">'
            '<input name="username"><input type="password"></form>\';</script>',
            None,
        ),
        (
            '<a href="/accounts/login/">Log in</a><p>Bio: the challenge checkpoint</p><img src="/assets/login.png">',
            None,
        ),
        ('<form action="/search/"><input name="username"><input type="password"></form>', None),
        (
            '<form action="https://external.invalid/accounts/login/">'
            '<input name="username"><input type="password"></form>',
            None,
        ),
        ('<form action="/accounts/login/"><input name="username"></form><form><input type="password"></form>', None),
        ('<form action="/accounts/login-research/"><input name="username"><input type="password"></form>', None),
        (
            '<template><form action="/accounts/login/">'
            '<input name="username"><input type="password"></form></template>',
            None,
        ),
        ('{"status":"ok","message":"login_required"}', None),
        ('{"status":"fail","message":"caption about login and a challenge"}', None),
        ('{"data":{"caption":"login_required","status":"fail"}}', None),
    ],
)
def test_warmup_structured_auth_documents_ignore_incidental_copy(_mock_scrapling, monkeypatch, body, body_path):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "0")
    events, failures, requests = [], [], []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "synthetic", "ds_user_id": "123"},
        browser_account_id="test",
        consume_credentials_only=True,
        allow_requests_recovery=False,
        before_source_request=requests.append,
        source_failure_observer=lambda *args: failures.append(args),
        source_diagnostic_observer=events.append,
    )
    response = SimpleNamespace(
        status_code=200, text=body, cookies={}, headers={}, url="https://www.instagram.com/bravotv/"
    )
    fetcher._fetcher.async_fetch = AsyncMock(return_value=response)
    fetcher._fetch_graphql = AsyncMock()
    fetcher._activate_requests_fallback = MagicMock(return_value=False)
    if body_path:
        with pytest.raises(InstagramPostsWarmupError):
            asyncio.run(fetcher.warmup("bravotv"))
    else:
        asyncio.run(fetcher.warmup("bravotv"))
    assert requests == ["browser_warmup"] and fetcher._request_count == 1
    assert failures == ([("source_auth_failed", None)] if body_path else [])
    assert events[-1]["path_class"] == "profile"
    assert events[-1]["classification_path_class"] == (body_path or "profile")
    assert events[-1]["auth_body_marker"] == bool(body_path)
    assert len(events) == 2
    fetcher._fetch_graphql.assert_not_awaited()
    fetcher._activate_requests_fallback.assert_not_called()


@pytest.mark.parametrize(
    "status,url,location,path",
    [
        (401, "https://www.instagram.com/bravotv/", "", "profile"),
        (403, "https://www.instagram.com/bravotv/", "", "profile"),
        (200, "https://www.instagram.com/accounts/login/", "", "login"),
        (200, "https://www.instagram.com/checkpoint/", "", "checkpoint"),
        (200, "https://www.instagram.com/challenge/", "", "checkpoint"),
        (200, "https://www.instagram.com/", "", "home"),
        (302, "https://www.instagram.com/bravotv/", "/accounts/login/?next=/bravotv/", "login"),
        (
            302,
            "https://www.instagram.com/bravotv/",
            "https://www.instagram.com/challenge/123/?secret=value",
            "checkpoint",
        ),
        (302, "https://www.instagram.com/bravotv/", "https://www.instagram.com/", "home"),
    ],
)
def test_warmup_retains_http_and_relative_absolute_redirect_stops(
    _mock_scrapling, monkeypatch, status, url, location, path
):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "0")
    failures = []
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "synthetic", "ds_user_id": "123"},
        browser_account_id="test",
        consume_credentials_only=True,
        allow_requests_recovery=False,
        source_failure_observer=lambda *args: failures.append(args),
    )
    fetcher._fetcher.async_fetch = AsyncMock(
        return_value=SimpleNamespace(
            status_code=status, url=url, headers={"location": location}, text="<html>profile</html>", cookies={}
        )
    )
    fetcher._fetch_graphql = AsyncMock()
    with pytest.raises(InstagramPostsWarmupError) as error:
        asyncio.run(fetcher.warmup("bravotv"))
    assert error.value.error_code == "instagram_posts_warmup_auth_failed"
    assert failures == [("source_auth_failed", None)]
    assert fetcher._request_count == 1
    assert fetcher.runtime_metadata["warmup_failure"]["final_path_class"] == path
    fetcher._fetch_graphql.assert_not_awaited()


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


def test_warmup_redirect_loop_stops_after_one_document_without_graphql(_mock_scrapling, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_REQUESTS_FALLBACK_ENABLED", "1")
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing", "csrftoken": "csrf"},
        browser_account_id="t",
    )
    fetcher._fetcher.async_fetch = AsyncMock(
        side_effect=RuntimeError(
            "Page.goto: net::ERR_TOO_MANY_REDIRECTS at "
            "https://www.instagram.com/accounts/login/?next=%2Fbravotv%2F; Exceeded 30 redirects"
        )
    )
    fetcher._fetch_graphql = AsyncMock()

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_redirect_loop"
    assert exc_info.value.retryable is False
    assert fetcher._fetcher.async_fetch.await_count == 1
    await_args = fetcher._fetcher.async_fetch.await_args
    assert await_args is not None
    assert await_args.kwargs["retries"] == 1
    fetcher._fetch_graphql.assert_not_awaited()
    metadata = fetcher.runtime_metadata
    assert metadata["requests_fallback"]["active"] is False
    assert metadata["warmup_failure"] == {
        "phase": "document_warmup",
        "attempt_count": 1,
        "final_origin_class": "instagram",
        "final_path_class": "login",
        "proxy_fingerprint": "none",
        "error_code": "instagram_posts_redirect_loop",
    }
    assert "next=" not in json.dumps(metadata)


@pytest.mark.parametrize(
    ("final_url", "path_class"),
    [
        ("https://www.instagram.com/accounts/login/?next=%2Fbravotv%2F", "login"),
        ("https://www.instagram.com/challenge/", "checkpoint"),
        ("https://www.instagram.com/", "home"),
    ],
)
def test_warmup_terminal_redirect_signals_do_not_fallback_or_fetch_graphql(
    _mock_scrapling,
    monkeypatch,
    final_url,
    path_class,
):
    import asyncio
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_REQUESTS_FALLBACK_ENABLED", "1")
    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={"sessionid": "existing"},
        browser_account_id="t",
    )
    response = MagicMock(status_code=200, text="<html></html>", cookies={})
    response.url = final_url
    fetcher._fetcher.async_fetch = AsyncMock(return_value=response)
    fetcher._fetch_graphql = AsyncMock()

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.retryable is False
    assert fetcher._fetcher.async_fetch.await_count == 1
    fetcher._fetch_graphql.assert_not_awaited()
    assert fetcher.runtime_metadata["requests_fallback"]["active"] is False
    assert fetcher.runtime_metadata["warmup_failure"]["final_path_class"] == path_class


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


def test_document_context_pool_snapshot_and_fresh_warmup_keep_token_handoff_private(
    _mock_scrapling, monkeypatch, caplog
):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from trr_backend.socials.instagram.posts_scrapling import fetcher as module

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "1")
    module._POSTS_WARMUP_POOL.clear()

    def construct():
        return module.InstagramPostsScraplingFetcher(
            cookies=[],
            raw_cookies={"sessionid": "private-session", "ds_user_id": "1"},
            browser_account_id="handoff",
        )

    first, pooled, snapshot = construct(), construct(), construct()
    response = SimpleNamespace(
        status_code=200,
        text=_document_context_html(["private-direct-dtsg"]),
        cookies={"csrftoken": "private-fresh-csrf"},
        headers={},
        url="https://www.instagram.com/bravotv/",
    )
    first._fetcher = SimpleNamespace(async_fetch=AsyncMock(return_value=response))
    pooled._fetcher = SimpleNamespace(async_fetch=AsyncMock(side_effect=AssertionError("pool should avoid source")))
    snapshot._fetcher = SimpleNamespace(
        async_fetch=AsyncMock(side_effect=AssertionError("snapshot should avoid source"))
    )

    async def execute():
        try:
            await first.warmup("bravotv")
            await pooled.warmup("bravotv")
            await snapshot.apply_warmup_snapshot(first.warmup_snapshot())
            for handed_off in (pooled, snapshot):
                assert handed_off._page_tokens == first._page_tokens
                assert handed_off._page_tokens["fb_dtsg"] == "private-direct-dtsg"
                assert handed_off._raw_cookies["csrftoken"] == "private-fresh-csrf"
                assert handed_off._request_count == 0
            monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_SHARED_WARMUP_ENABLED", "0")
            response.text = _document_context_html(["ambiguous-a", "ambiguous-b"])
            await first.warmup("bravotv")
            assert "fb_dtsg" not in first._page_tokens
            assert first._request_count == 2
        finally:
            await first.aclose()
            await pooled.aclose()
            await snapshot.aclose()
            module._POSTS_WARMUP_POOL.clear()

    asyncio.run(execute())
    serialized = repr([first.runtime_metadata, pooled.runtime_metadata, snapshot.runtime_metadata]) + caplog.text
    assert all(secret not in serialized for secret in ("private-direct-dtsg", "private-session", "private-fresh-csrf"))
    pooled._fetcher.async_fetch.assert_not_awaited()
    snapshot._fetcher.async_fetch.assert_not_awaited()


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
