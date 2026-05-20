from __future__ import annotations

from trr_backend.integrations import getty_transport


def test_select_getty_proxy_prefers_explicit_urls(monkeypatch) -> None:
    monkeypatch.setenv("TRR_GETTY_PROXY_URLS", "http://user:pass@proxy.example:8080")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proxy = getty_transport.select_getty_proxy()

    assert proxy is not None
    assert proxy.http_proxy_url == "http://user:pass@proxy.example:8080"
    assert proxy.proxy_fingerprint == "proxy.example:8080:explicit"
    assert proxy.provider == "explicit"


def test_select_getty_proxy_builds_decodo_config(monkeypatch) -> None:
    monkeypatch.delenv("TRR_GETTY_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proxy = getty_transport.select_getty_proxy()

    assert proxy is not None
    assert (
        proxy.http_proxy_url
        == "http://decodo-user-session-gettyremote-sessionduration-10:decodo-pass@gate.decodo.com:7000"
    )
    assert proxy.proxy_fingerprint == "gate.decodo.com:7000:decodo:sticky"
    assert proxy.provider == "decodo"
    assert proxy.browser_proxy == {
        "server": "http://gate.decodo.com:7000",
        "username": "decodo-user-session-gettyremote-sessionduration-10",
        "password": "decodo-pass",
    }


def test_select_getty_proxy_uses_getty_specific_decodo_overrides(monkeypatch) -> None:
    monkeypatch.delenv("TRR_GETTY_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "global.decodo.example:7000")
    monkeypatch.setenv("TRR_GETTY_PROXY_USERNAME", "getty-user")
    monkeypatch.setenv("TRR_GETTY_PROXY_PASSWORD", "getty-pass")
    monkeypatch.setenv("TRR_GETTY_PROXY_GATEWAY", "getty.decodo.example:7000")
    monkeypatch.setenv("TRR_GETTY_USE_STICKY_PROXY", "false")

    proxy = getty_transport.select_getty_proxy()

    assert proxy is not None
    assert proxy.http_proxy_url == "http://getty-user:getty-pass@getty.decodo.example:7000"
    assert proxy.proxy_fingerprint == "getty.decodo.example:7000:decodo:rotating"
    assert proxy.browser_proxy == {
        "server": "http://getty.decodo.example:7000",
        "username": "getty-user",
        "password": "getty-pass",
    }


def test_build_remote_getty_session_returns_disabled_metadata_when_unconfigured(monkeypatch) -> None:
    monkeypatch.delenv("TRR_GETTY_PROXY_URLS", raising=False)
    monkeypatch.delenv("TRR_GETTY_PROXY_USERNAME", raising=False)
    monkeypatch.delenv("TRR_GETTY_PROXY_PASSWORD", raising=False)
    monkeypatch.delenv("TRR_GETTY_PROXY_GATEWAY", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.delenv("DECODO_GATEWAY", raising=False)

    session, metadata = getty_transport.build_remote_getty_session()

    assert session is None
    assert metadata["getty_runtime_probe_status"] == "disabled"
    assert metadata["getty_runtime_probe_reason"] == "proxy_unconfigured"
    assert metadata["getty_primary_failure_reason"] == "proxy_unconfigured"


def test_classify_getty_transport_failure_maps_stable_reason_codes() -> None:
    assert (
        getty_transport.classify_getty_transport_failure(
            {
                "termination_reason": "request_exception",
                "request_exception_class": "ProxyError",
                "request_exception_message": "Tunnel connection failed: 407 Proxy Authentication Required",
            }
        )
        == "proxy_auth_failed"
    )
    assert (
        getty_transport.classify_getty_transport_failure(
            {
                "termination_reason": "request_exception",
                "request_exception_class": "ProxyError",
                "request_exception_message": "Tunnel connection failed: 302 Found",
            }
        )
        == "proxy_tunnel_failed"
    )
    assert (
        getty_transport.classify_getty_transport_failure({"termination_reason": "challenge_page"}) == "challenge_page"
    )
    assert (
        getty_transport.classify_getty_transport_failure({"pagination_rewrite_detected": True}) == "pagination_rewrite"
    )
    assert (
        getty_transport.classify_getty_transport_failure(
            {
                "termination_reason": None,
                "fetched_candidates_total": 0,
                "site_image_total": None,
            }
        )
        == "zero_results_block_indicators"
    )
    assert (
        getty_transport.classify_getty_transport_failure(
            {
                "termination_reason": "natural_exhaustion",
                "fetched_candidates_total": 0,
                "site_image_total": 0,
            }
        )
        is None
    )
