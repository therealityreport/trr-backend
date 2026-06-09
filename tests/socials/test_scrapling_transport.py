from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest


def test_import_does_not_import_scrapling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "scrapling", raising=False)
    monkeypatch.delitem(sys.modules, "scrapling.fetchers", raising=False)

    import trr_backend.socials.scrapling_transport as transport

    assert transport.DEFAULT_TIMEOUT_MS == 45_000
    assert "scrapling" not in sys.modules


def test_cookies_to_scrapling_converts_mapping_to_browser_cookie_records() -> None:
    from trr_backend.socials.scrapling_transport import cookies_to_scrapling

    assert cookies_to_scrapling(
        {"sessionid": "abc", "empty": "", " csrftoken ": " xyz "},
        ".instagram.com",
    ) == [
        {"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"},
        {"name": "csrftoken", "value": "xyz", "domain": ".instagram.com", "path": "/"},
    ]


def test_cookies_to_scrapling_requires_domain() -> None:
    from trr_backend.socials.scrapling_transport import cookies_to_scrapling

    with pytest.raises(ValueError, match="domain is required"):
        cookies_to_scrapling({"sessionid": "abc"}, "")


def test_build_stealthy_fetcher_lazily_imports_scrapling_fetchers(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.scrapling_transport import build_stealthy_fetcher

    mock_scrapling = ModuleType("scrapling")
    mock_fetchers = ModuleType("scrapling.fetchers")
    mock_fetcher_cls = MagicMock(return_value="fetcher")
    mock_fetchers.StealthyFetcher = mock_fetcher_cls
    monkeypatch.setitem(sys.modules, "scrapling", mock_scrapling)
    monkeypatch.setitem(sys.modules, "scrapling.fetchers", mock_fetchers)

    assert build_stealthy_fetcher(headless=True) == "fetcher"
    mock_fetcher_cls.assert_called_once_with(headless=True)


def test_build_fetchers_raise_clear_runtime_error_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.scrapling_transport import build_dynamic_fetcher

    monkeypatch.delitem(sys.modules, "scrapling", raising=False)
    monkeypatch.setitem(sys.modules, "scrapling.fetchers", None)

    with pytest.raises(RuntimeError, match=r"Scrapling fetchers are unavailable\. Install scrapling\[fetchers\]\."):
        build_dynamic_fetcher()


def test_build_proxy_rotator_uses_scrapling_fetchers_path(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.scrapling_transport import build_proxy_rotator

    mock_fetchers = ModuleType("scrapling.fetchers")
    mock_rotator_cls = MagicMock(return_value="rotator")
    mock_fetchers.ProxyRotator = mock_rotator_cls
    monkeypatch.setitem(sys.modules, "scrapling.fetchers", mock_fetchers)

    assert build_proxy_rotator(["http://one", "", "http://two"]) == "rotator"
    mock_rotator_cls.assert_called_once_with(["http://one", "http://two"])


def test_build_proxy_rotator_supports_browser_proxy_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.scrapling_transport import build_proxy_rotator

    mock_fetchers = ModuleType("scrapling.fetchers")
    mock_rotator_cls = MagicMock(return_value="rotator")
    mock_fetchers.ProxyRotator = mock_rotator_cls
    monkeypatch.setitem(sys.modules, "scrapling.fetchers", mock_fetchers)

    proxy = {"server": "http://gate.example:7000", "username": "user", "password": "secret"}
    assert build_proxy_rotator(proxy) == "rotator"
    mock_rotator_cls.assert_called_once_with([proxy])


def test_build_proxy_rotator_returns_none_for_empty_values() -> None:
    from trr_backend.socials.scrapling_transport import build_proxy_rotator

    assert build_proxy_rotator(None) is None
    assert build_proxy_rotator("") is None
    assert build_proxy_rotator([]) is None


def test_merge_response_cookies_keeps_existing_and_applies_response_delta() -> None:
    from trr_backend.socials.scrapling_transport import merge_response_cookies

    response = MagicMock(cookies={"sessionid": "new", "csrftoken": "xyz"})

    assert merge_response_cookies({"sessionid": "old", "ig_did": "seed"}, response) == {
        "sessionid": "new",
        "ig_did": "seed",
        "csrftoken": "xyz",
    }


def test_safe_cookie_metadata_reports_names_and_counts_without_values() -> None:
    from trr_backend.socials.scrapling_transport import safe_cookie_metadata

    metadata = safe_cookie_metadata(
        {"sessionid": "abc", "csrftoken": "xyz"},
        [{"name": "ttwid", "value": "secret"}, {"name": "msToken", "value": "secret"}],
        prefix="scrapling",
    )

    assert metadata == {
        "scrapling_seed_cookie_names": ["csrftoken", "sessionid"],
        "scrapling_seed_cookie_count": 2,
        "scrapling_warmup_cookie_names": ["msToken", "ttwid"],
        "scrapling_warmup_cookie_count": 2,
    }
    assert "abc" not in repr(metadata)
    assert "secret" not in repr(metadata)


def test_transport_defaults_are_available_without_scrapling() -> None:
    from trr_backend.socials.scrapling_transport import DEFAULT_TRANSPORT, ScraplingTransportDefaults

    assert DEFAULT_TRANSPORT == ScraplingTransportDefaults(
        timeout_ms=45_000,
        max_transient_retries=3,
        base_backoff_seconds=1.0,
    )


def test_scrapling_runtime_metadata_reports_known_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.socials.scrapling_transport as transport

    def fake_version(package: str) -> str:
        return {
            "scrapling": "0.4.9",
            "patchright": "1.60.1",
            "playwright": "1.60.0",
        }[package]

    monkeypatch.setattr(transport, "version", fake_version)

    assert transport.scrapling_runtime_metadata() == {
        "scrapling_version": "0.4.9",
        "patchright_version": "1.60.1",
        "playwright_version": "1.60.0",
    }


def test_scrapling_runtime_metadata_handles_missing_packages(monkeypatch: pytest.MonkeyPatch) -> None:
    import trr_backend.socials.scrapling_transport as transport

    def missing_version(_package: str) -> str:
        raise transport.PackageNotFoundError

    monkeypatch.setattr(transport, "version", missing_version)

    assert transport.scrapling_runtime_metadata() == {
        "scrapling_version": None,
        "patchright_version": None,
        "playwright_version": None,
    }


def test_apply_decodo_session_affinity_defaults_to_rotating_and_supports_sticky() -> None:
    from trr_backend.socials.scrapling_transport import apply_decodo_session_affinity

    rotating_username, rotating_mode = apply_decodo_session_affinity(
        "decodo-user",
        use_sticky_proxy=False,
        session_ttl_seconds=600,
        session_id="bravotv",
    )
    sticky_username, sticky_mode = apply_decodo_session_affinity(
        "decodo-user",
        use_sticky_proxy=True,
        session_ttl_seconds=600,
        session_id="bravotv",
    )
    preconfigured_username, preconfigured_mode = apply_decodo_session_affinity(
        "decodo-user-session-existing-sessionduration-30",
        use_sticky_proxy=False,
        session_ttl_seconds=600,
    )

    assert rotating_username == "decodo-user"
    assert rotating_mode == "rotating"
    assert sticky_username.startswith("decodo-user-session-")
    assert "-sessionduration-10" in sticky_username
    assert sticky_mode == "sticky"
    assert preconfigured_username == "decodo-user-session-existing-sessionduration-30"
    assert preconfigured_mode == "sticky_preconfigured"


def test_proxy_conflict_guard_allows_single_proxy_mode() -> None:
    from trr_backend.socials.scrapling_transport import assert_no_conflicting_scrapling_proxies

    assert_no_conflicting_scrapling_proxies()
    assert_no_conflicting_scrapling_proxies(session_proxy="http://proxy.example")
    assert_no_conflicting_scrapling_proxies(request_proxy="http://proxy.example")
    assert_no_conflicting_scrapling_proxies(request_proxies={"http": "http://proxy.example"})


def test_proxy_conflict_guard_rejects_mixed_proxy_modes() -> None:
    from trr_backend.socials.scrapling_transport import (
        SCRAPLING_PROXY_CONFLICT_REASON,
        assert_no_conflicting_scrapling_proxies,
    )

    with pytest.raises(ValueError, match=SCRAPLING_PROXY_CONFLICT_REASON):
        assert_no_conflicting_scrapling_proxies(
            session_proxy="http://session-proxy.example",
            request_proxy="http://request-proxy.example",
        )

    with pytest.raises(ValueError, match=SCRAPLING_PROXY_CONFLICT_REASON):
        assert_no_conflicting_scrapling_proxies(
            session_proxy="http://session-proxy.example",
            request_proxies={"http": "http://request-proxy.example"},
        )
