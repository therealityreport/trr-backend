from __future__ import annotations

import trr_backend.socials.socialblade.auth as auth_module


def test_shared_chrome_cdp_url_prefers_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("SOCIALBLADE_SHARED_CHROME_CDP_URL", "http://127.0.0.1:9555")
    monkeypatch.setattr(auth_module, "_chrome_cdp_endpoint_reachable", lambda _url: False)

    assert auth_module._socialblade_shared_chrome_cdp_url() == "http://127.0.0.1:9555"


def test_shared_chrome_cdp_url_prefers_live_managed_port(monkeypatch) -> None:
    monkeypatch.delenv("SOCIALBLADE_SHARED_CHROME_CDP_URL", raising=False)
    monkeypatch.setattr(
        auth_module,
        "_chrome_cdp_endpoint_reachable",
        lambda url: url == "http://127.0.0.1:9422",
    )

    assert auth_module._socialblade_shared_chrome_cdp_url() == "http://127.0.0.1:9422"


def test_shared_chrome_cdp_url_falls_back_to_visible_manual_port(monkeypatch) -> None:
    monkeypatch.delenv("SOCIALBLADE_SHARED_CHROME_CDP_URL", raising=False)
    monkeypatch.setattr(
        auth_module,
        "_chrome_cdp_endpoint_reachable",
        lambda url: url == "http://127.0.0.1:9222",
    )

    assert auth_module._socialblade_shared_chrome_cdp_url() == "http://127.0.0.1:9222"


def test_visible_chrome_cdp_url_defaults_to_manual_port(monkeypatch) -> None:
    monkeypatch.delenv("SOCIALBLADE_VISIBLE_CHROME_CDP_URL", raising=False)

    assert auth_module._socialblade_visible_chrome_cdp_url() == "http://127.0.0.1:9222"


def test_refresh_socialblade_cookies_prefers_visible_manual_port(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_export_socialblade_cookies_from_shared_chrome(*, cdp_url: str | None = None) -> dict[str, str]:
        captured["cdp_url"] = cdp_url or ""
        return {"cf_clearance": "token"}

    monkeypatch.setattr(
        auth_module,
        "export_socialblade_cookies_from_shared_chrome",
        fake_export_socialblade_cookies_from_shared_chrome,
    )
    monkeypatch.setattr(auth_module, "_socialblade_visible_chrome_cdp_url", lambda: "http://127.0.0.1:9222")

    cookies = auth_module.refresh_socialblade_cookies("test-refresh", allow_headless_fallback=False)

    assert cookies == {"cf_clearance": "token"}
    assert captured == {"cdp_url": "http://127.0.0.1:9222"}
