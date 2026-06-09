"""Focused tests for Decodo cost observability.

Covers:
  * Per-request byte aggregation by destination host in the Instagram posts fetcher,
    plus surfacing in ``runtime_metadata``.
  * ``observability.record_proxy_bytes`` fail-open no-op behavior.
  * Decodo usage poll: no-op without creds; threshold alert fires / does-not-fire.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from trr_backend import observability
from trr_backend.socials import decodo_usage
from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher


class _FakeURL:
    def __init__(self, host: str) -> None:
        self.host = host


class _FakeResponse:
    """Minimal httpx-like response for metering tests."""

    def __init__(self, *, host: str, content: bytes = b"", status: int = 200, headers: dict[str, str] | None = None):
        self.url = _FakeURL(host)
        self.content = content
        self.status_code = status
        self.text = ""
        self.headers = headers or {}


def _make_fetcher(browser_account_id: str = "@trr_test") -> InstagramPostsScraplingFetcher:
    return InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={},
        browser_account_id=browser_account_id,
        proxy_config=None,
    )


# ---------------------------------------------------------------------------
# Part 1: byte aggregation by host
# ---------------------------------------------------------------------------


def test_byte_aggregation_by_host_accumulates_total_and_breakdown() -> None:
    f = _make_fetcher()
    f._record_proxy_response(_FakeResponse(host="i.instagram.com", content=b"a" * 1000))
    f._record_proxy_response(_FakeResponse(host="i.instagram.com", content=b"b" * 500))
    f._record_proxy_response(_FakeResponse(host="scontent.cdninstagram.com", content=b"c" * 2048))
    f._record_proxy_response(_FakeResponse(host="www.instagram.com", content=b"d" * 64))

    assert f._bytes_total == 1000 + 500 + 2048 + 64
    assert f._bytes_by_host == {
        "i.instagram.com": 1500,
        "scontent.cdninstagram.com": 2048,
        "www.instagram.com": 64,
    }


def test_byte_aggregation_surfaced_in_runtime_metadata() -> None:
    f = _make_fetcher()
    f._record_proxy_response(_FakeResponse(host="i.instagram.com", content=b"x" * 4096))

    metadata = f.runtime_metadata
    assert metadata["bytes_total"] == 4096
    assert metadata["bytes_by_host"] == {"i.instagram.com": 4096}
    # Also mirrored inside proxy_pacing so the progress payload can carry it.
    assert metadata["proxy_pacing"]["bytes_total"] == 4096
    assert metadata["proxy_pacing"]["bytes_by_host"] == {"i.instagram.com": 4096}


def test_byte_aggregation_falls_back_to_content_length_header() -> None:
    f = _make_fetcher()
    # content is None -> must fall back to the Content-Length header.
    resp = _FakeResponse(host="i.instagram.com", headers={"content-length": "777"})
    resp.content = None  # type: ignore[assignment]
    f._record_proxy_response(resp)
    assert f._bytes_total == 777
    assert f._bytes_by_host == {"i.instagram.com": 777}


def test_byte_metering_is_fail_open_on_bad_response() -> None:
    f = _make_fetcher()

    class _Broken:
        @property
        def content(self) -> Any:  # noqa: D401 - raises on access
            raise RuntimeError("boom")

        url = None
        status_code = 200
        text = ""
        headers: dict[str, str] = {}

    # Must not raise, and must not record anything.
    f._record_proxy_response(_Broken())
    assert f._bytes_total == 0
    assert f._bytes_by_host == {}


def test_provider_label_detects_decodo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")
    f = _make_fetcher()
    assert f._proxy_provider_label() == "decodo"
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", raising=False)
    # No proxy configured -> "none".
    assert _make_fetcher()._proxy_provider_label() == "none"


# ---------------------------------------------------------------------------
# observability.record_proxy_bytes
# ---------------------------------------------------------------------------


def test_record_proxy_bytes_is_noop_without_prometheus() -> None:
    # In the local/test env prometheus_client is absent, so the counter is None and the
    # helper must be a safe no-op (no raise) for any input, including bad values.
    observability.record_proxy_bytes("decodo", "@acct", "i.instagram.com", 123)
    observability.record_proxy_bytes("", "", "", 0)
    observability.record_proxy_bytes("decodo", "@acct", "host", -5)
    observability.record_proxy_bytes("decodo", "@acct", "host", "not-an-int")  # type: ignore[arg-type]


def test_record_response_bytes_invokes_observability(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str, int]] = []

    def _spy(provider: str, account: str, host: str, n: int) -> None:
        calls.append((provider, account, host, n))

    monkeypatch.setattr(observability, "record_proxy_bytes", _spy)
    f = _make_fetcher("@spyacct")
    f._record_proxy_response(_FakeResponse(host="i.instagram.com", content=b"z" * 321))
    assert calls == [("none", "@spyacct", "i.instagram.com", 321)]


# ---------------------------------------------------------------------------
# Part 2: Decodo usage poll + threshold alert
# ---------------------------------------------------------------------------


def _clear_decodo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "DECODO_API_TOKEN",
        "DECODO_API_USERNAME",
        "DECODO_API_PASSWORD",
        "DECODO_API_AUTH_SCHEME",
        "DECODO_API_PROXY_TYPE",
        "DECODO_DAILY_BUDGET_USD",
        "DECODO_DAILY_BUDGET_GB",
        "DECODO_USD_PER_GB",
    ):
        monkeypatch.delenv(key, raising=False)


def test_poll_is_noop_without_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    result = decodo_usage.poll_decodo_usage()
    assert result == {"status": "skipped", "reason": "no_credentials", "alert": False}


def test_resolve_auth_header_token_and_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    assert decodo_usage._resolve_auth_header() is None

    monkeypatch.setenv("DECODO_API_TOKEN", "abc123")
    assert decodo_usage._resolve_auth_header() == "abc123"

    monkeypatch.setenv("DECODO_API_AUTH_SCHEME", "Bearer")
    assert decodo_usage._resolve_auth_header() == "Bearer abc123"

    monkeypatch.delenv("DECODO_API_TOKEN")
    monkeypatch.delenv("DECODO_API_AUTH_SCHEME")
    monkeypatch.setenv("DECODO_API_USERNAME", "user")
    monkeypatch.setenv("DECODO_API_PASSWORD", "pass")
    header = decodo_usage._resolve_auth_header()
    assert header is not None and header.startswith("Basic ")


def test_threshold_alert_fires_on_gb_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    monkeypatch.setenv("DECODO_API_TOKEN", "tok")
    monkeypatch.setenv("DECODO_DAILY_BUDGET_GB", "1.0")

    # 2 GB used (decimal) -> over the 1 GB budget.
    def _fake_fetch(*, auth_header: str, timeout_seconds: float = 15.0, now: datetime | None = None) -> dict[str, Any]:
        return {"ok": True, "bytes": 2 * 1_000_000_000, "requests": 42, "raw": {}, "error": None}

    sent: list[str] = []
    monkeypatch.setattr(decodo_usage, "_fetch_decodo_usage", _fake_fetch)
    monkeypatch.setattr(decodo_usage, "_emit_sentry_message", lambda msg, *, extra: sent.append(msg) or True)

    result = decodo_usage.poll_decodo_usage()
    assert result["status"] == "ok"
    assert result["alert"] is True
    assert result["used_gb"] == pytest.approx(2.0)
    assert result["requests"] == 42
    assert result.get("sentry_sent") is True
    assert sent and "budget exceeded" in sent[0].lower()


def test_threshold_alert_does_not_fire_below_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    monkeypatch.setenv("DECODO_API_TOKEN", "tok")
    monkeypatch.setenv("DECODO_DAILY_BUDGET_GB", "5.0")

    def _fake_fetch(*, auth_header: str, timeout_seconds: float = 15.0, now: datetime | None = None) -> dict[str, Any]:
        return {"ok": True, "bytes": 1_000_000_000, "requests": 7, "raw": {}, "error": None}

    sent: list[str] = []
    monkeypatch.setattr(decodo_usage, "_fetch_decodo_usage", _fake_fetch)
    monkeypatch.setattr(decodo_usage, "_emit_sentry_message", lambda msg, *, extra: sent.append(msg) or True)

    result = decodo_usage.poll_decodo_usage()
    assert result["status"] == "ok"
    assert result["alert"] is False
    assert "sentry_sent" not in result
    assert sent == []


def test_usd_budget_requires_price_per_gb(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    monkeypatch.setenv("DECODO_API_TOKEN", "tok")
    monkeypatch.setenv("DECODO_DAILY_BUDGET_USD", "1.00")
    # No DECODO_USD_PER_GB -> cannot derive USD -> no USD alert even with heavy usage.

    def _fake_fetch(*, auth_header: str, timeout_seconds: float = 15.0, now: datetime | None = None) -> dict[str, Any]:
        return {"ok": True, "bytes": 100 * 1_000_000_000, "requests": 0, "raw": {}, "error": None}

    monkeypatch.setattr(decodo_usage, "_fetch_decodo_usage", _fake_fetch)
    result = decodo_usage.poll_decodo_usage()
    assert result["alert"] is False
    assert result["used_usd"] == 0.0

    # With a price-per-GB set, the USD budget is breached.
    monkeypatch.setenv("DECODO_USD_PER_GB", "0.50")  # 100 GB * 0.50 = $50 >= $1
    monkeypatch.setattr(decodo_usage, "_emit_sentry_message", lambda msg, *, extra: True)
    result2 = decodo_usage.poll_decodo_usage()
    assert result2["alert"] is True
    assert result2["used_usd"] == pytest.approx(50.0)


def test_poll_reports_error_on_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_decodo_env(monkeypatch)
    monkeypatch.setenv("DECODO_API_TOKEN", "tok")
    monkeypatch.setenv("DECODO_DAILY_BUDGET_GB", "1.0")

    def _fake_fetch(*, auth_header: str, timeout_seconds: float = 15.0, now: datetime | None = None) -> dict[str, Any]:
        return {"ok": False, "bytes": 0, "requests": 0, "raw": None, "error": "http_503"}

    monkeypatch.setattr(decodo_usage, "_fetch_decodo_usage", _fake_fetch)
    result = decodo_usage.poll_decodo_usage()
    assert result == {"status": "error", "reason": "http_503", "alert": False}


def test_combined_bytes_extraction_variants() -> None:
    assert decodo_usage._combined_bytes_from_payload({"metadata": {"total_rx_tx": 12345}}) == 12345
    assert decodo_usage._combined_bytes_from_payload({"metadata": {"total_rx": 10, "total_tx": 5}}) == 15
    assert (
        decodo_usage._combined_bytes_from_payload({"data": [{"rx_bytes": 10, "tx_bytes": 5}, {"rx_tx_bytes": 100}]})
        == 115
    )
    assert decodo_usage._combined_bytes_from_payload({}) == 0
    assert decodo_usage._combined_bytes_from_payload("garbage") == 0


def test_today_range_format() -> None:
    fixed = datetime(2026, 6, 8, 13, 45, 30, tzinfo=UTC)
    start, end = decodo_usage._today_range_utc(fixed)
    assert start == "2026-06-08 00:00:00"
    assert end == "2026-06-08 13:45:30"
