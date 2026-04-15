from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from trr_backend.socials.tiktok.http_client import DEFAULT_CURL_CFFI_IMPERSONATE, build_tiktok_http_client
from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper


@dataclass
class _FakeResponse:
    status_code: int = 200
    headers: dict[str, str] | None = None
    text: str = ""
    content: bytes = b"{}"

    def json(self) -> dict[str, str]:
        return {}

    def raise_for_status(self) -> None:
        return None


def test_build_tiktok_http_client_defaults_to_requests() -> None:
    client = build_tiktok_http_client()

    assert client.client_name == "requests"
    assert client.proxy_url is None
    assert client.proxy_label is None
    assert client.impersonate is None


def test_tiktok_scraper_prefers_explicit_proxy_url_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TIKTOK_PROXY_URLS", "http://env-user:env-pass@env-proxy:8080")
    monkeypatch.setenv("SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK", "http://crawlee-proxy:8080")

    scraper = TikTokScraper(proxy_urls=["http://explicit-user:explicit-pass@explicit-proxy:9090"])

    assert scraper._http_client.proxy_url == "http://explicit-user:explicit-pass@explicit-proxy:9090"  # noqa: SLF001
    assert scraper._http_client.proxy_label == "explicit-proxy"  # noqa: SLF001
    assert scraper.last_retrieval_meta["proxy_source"] == "constructor"


def test_tiktok_scraper_uses_tiktok_proxy_env_before_crawlee_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TIKTOK_PROXY_URLS", "http://env-user:env-pass@env-proxy:8080")
    monkeypatch.setenv("SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK", "http://crawlee-proxy:8080")

    scraper = TikTokScraper()

    assert scraper._http_client.proxy_url == "http://env-user:env-pass@env-proxy:8080"  # noqa: SLF001
    assert scraper._http_client.proxy_label == "env-proxy"  # noqa: SLF001
    assert scraper.last_retrieval_meta["proxy_source"] == "SOCIAL_TIKTOK_PROXY_URLS"


def test_tiktok_scraper_falls_back_to_crawlee_proxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_TIKTOK_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK", "http://crawlee-user:crawlee-pass@crawlee-proxy:8080")

    scraper = TikTokScraper()

    assert scraper._http_client.proxy_url == "http://crawlee-user:crawlee-pass@crawlee-proxy:8080"  # noqa: SLF001
    assert scraper._http_client.proxy_label == "crawlee-proxy"  # noqa: SLF001
    assert scraper.last_retrieval_meta["proxy_source"] == "SOCIAL_CRAWLEE_PROXY_URLS_TIKTOK"


def test_tiktok_scraper_uses_curl_cffi_impersonation_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TIKTOK_HTTP_CLIENT", "curl_cffi")
    monkeypatch.setenv("SOCIAL_TIKTOK_CURL_CFFI_IMPERSONATE", "chrome131")

    original_import_module = importlib.import_module

    class _FakeCurlSession:
        def get(self, *args: object, **kwargs: object) -> _FakeResponse:
            return _FakeResponse(headers={})

    def _fake_import_module(name: str, package: str | None = None):  # noqa: ANN001
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=_FakeCurlSession, exceptions=SimpleNamespace(RequestException=Exception))
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _fake_import_module)

    scraper = TikTokScraper()

    assert scraper._http_client.client_name == "curl_cffi"  # noqa: SLF001
    assert scraper.last_retrieval_meta["http_client"] == "curl_cffi"
    assert scraper.last_retrieval_meta["curl_cffi_impersonate"] == "chrome131"
    assert scraper._http_client.impersonate == "chrome131"  # noqa: SLF001


def test_tiktok_scraper_defaults_curl_cffi_impersonation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TIKTOK_HTTP_CLIENT", "curl_cffi")
    monkeypatch.delenv("SOCIAL_TIKTOK_CURL_CFFI_IMPERSONATE", raising=False)

    original_import_module = importlib.import_module

    class _FakeCurlSession:
        def get(self, *args: object, **kwargs: object) -> _FakeResponse:
            return _FakeResponse(headers={})

    def _fake_import_module(name: str, package: str | None = None):  # noqa: ANN001
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=_FakeCurlSession, exceptions=SimpleNamespace(RequestException=Exception))
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _fake_import_module)

    scraper = TikTokScraper()

    assert scraper.last_retrieval_meta["curl_cffi_impersonate"] == DEFAULT_CURL_CFFI_IMPERSONATE


def test_build_tiktok_http_client_raises_clear_error_when_curl_cffi_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import_module = importlib.import_module

    def _fake_import_module(name: str, package: str | None = None):  # noqa: ANN001
        if name == "curl_cffi.requests":
            raise ImportError("curl_cffi missing")
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _fake_import_module)

    with pytest.raises(RuntimeError, match="curl_cffi transport requested"):
        build_tiktok_http_client(client_name="curl_cffi")


def test_record_endpoint_response_accepts_transport_agnostic_response() -> None:
    scraper = TikTokScraper()

    scraper._record_endpoint_response(  # noqa: SLF001
        endpoint="fetch_posts",
        response=_FakeResponse(
            status_code=200,
            headers={
                "content-type": "application/json",
                "content-length": "2",
                "x-tt-logid": "agnostic-logid",
            },
            text="{}",
            content=b"{}",
        ),
    )

    assert scraper.last_retrieval_meta["endpoint_responses"]["fetch_posts"] == {
        "endpoint": "fetch_posts",
        "http_status": 200,
        "content_type": "application/json",
        "content_length": 2,
        "request_id": "agnostic-logid",
    }


def test_scrape_api_preserves_transport_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TIKTOK_HTTP_CLIENT", "curl_cffi")
    monkeypatch.setenv("SOCIAL_TIKTOK_CURL_CFFI_IMPERSONATE", "chrome131")

    original_import_module = importlib.import_module

    class _FakeCurlSession:
        def get(self, *args: object, **kwargs: object) -> _FakeResponse:
            return _FakeResponse(headers={})

    def _fake_import_module(name: str, package: str | None = None):  # noqa: ANN001
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=_FakeCurlSession, exceptions=SimpleNamespace(RequestException=Exception))
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", _fake_import_module)

    scraper = TikTokScraper()
    scraper.last_retrieval_meta.update(
        {
            "proxy_source": "constructor",
            "endpoint_responses": {"fetch_user_detail": {"endpoint": "fetch_user_detail", "http_status": 200}},
        }
    )

    scraper.fetch_user_detail = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "userInfo": {"user": {"secUid": "sec-1", "nickname": "Bravo"}}
    }
    scraper.fetch_posts = lambda *_args, **_kwargs: {"itemList": [], "hasMore": False, "cursor": 0}  # type: ignore[method-assign]
    scraper.build_profile_snapshot = lambda *_args, **_kwargs: {"total_posts": 0, "avatar_url": None}  # type: ignore[method-assign]
    scraper._has_ytdlp = lambda: False  # type: ignore[method-assign]

    posts = scraper._scrape_api(TikTokScrapeConfig(username="bravotv"))  # type: ignore[attr-defined]

    assert posts == []
    assert scraper.last_retrieval_meta["http_client"] == "curl_cffi"
    assert scraper.last_retrieval_meta["proxy_source"] == "constructor"
    assert scraper.last_retrieval_meta["curl_cffi_impersonate"] == "chrome131"
    assert scraper.last_retrieval_meta["endpoint_responses"]["fetch_user_detail"]["http_status"] == 200
