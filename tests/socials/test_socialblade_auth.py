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


def test_refresh_socialblade_cookies_auto_launches_visible_managed_chrome(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(auth_module, "_socialblade_visible_chrome_cdp_url", lambda: "http://127.0.0.1:9222")
    monkeypatch.setattr(auth_module, "_chrome_cdp_endpoint_reachable", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        auth_module,
        "_visible_managed_chrome_workspace_script",
        lambda: auth_module.Path("/tmp/fake-script"),
    )
    monkeypatch.setattr(auth_module.Path, "is_file", lambda self: str(self) == "/tmp/fake-script")

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["cwd"] = kwargs.get("cwd")
        captured["env"] = {
            "CODEX_CHROME_MODE": kwargs["env"]["CODEX_CHROME_MODE"],
            "CODEX_CHROME_SHARED_PORT": kwargs["env"]["CODEX_CHROME_SHARED_PORT"],
            "CHROME_AGENT_DEBUG_PORT": kwargs["env"]["CHROME_AGENT_DEBUG_PORT"],
            "CHROME_AGENT_HEADLESS": kwargs["env"]["CHROME_AGENT_HEADLESS"],
        }
        return None

    monkeypatch.setattr(auth_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        auth_module,
        "export_socialblade_cookies_from_shared_chrome",
        lambda *, cdp_url=None: {"cf_clearance": "token", "cdp_url": cdp_url},
    )

    reachable_sequence = iter([False, True])
    monkeypatch.setattr(
        auth_module,
        "_chrome_cdp_endpoint_reachable",
        lambda *_args, **_kwargs: next(reachable_sequence),
    )

    cookies = auth_module.refresh_socialblade_cookies("test-refresh", allow_headless_fallback=False)

    assert cookies["cf_clearance"] == "token"
    assert cookies["cdp_url"] == "http://127.0.0.1:9222"
    assert captured["cmd"] == ["bash", "/tmp/fake-script"]
    assert captured["env"] == {
        "CODEX_CHROME_MODE": "shared",
        "CODEX_CHROME_SHARED_PORT": "9222",
        "CHROME_AGENT_DEBUG_PORT": "9222",
        "CHROME_AGENT_HEADLESS": "0",
    }


def test_refresh_socialblade_cookies_opens_repair_tab_when_clearance_missing(monkeypatch) -> None:
    monkeypatch.setattr(auth_module, "_socialblade_visible_chrome_cdp_url", lambda: "http://127.0.0.1:9222")
    monkeypatch.setattr(auth_module, "_ensure_visible_managed_chrome_available", lambda _cdp_url: True)
    monkeypatch.setattr(
        auth_module,
        "export_socialblade_cookies_from_shared_chrome",
        lambda *, cdp_url=None: (_ for _ in ()).throw(
            RuntimeError("Managed Chrome does not have a usable SocialBlade Cloudflare clearance cookie")
        ),
    )
    opened: dict[str, str] = {}
    monkeypatch.setattr(
        auth_module,
        "_open_socialblade_repair_tab",
        lambda cdp_url: opened.setdefault("cdp_url", cdp_url) == cdp_url,
    )

    try:
        auth_module.refresh_socialblade_cookies("test-refresh", allow_headless_fallback=False)
    except RuntimeError as exc:
        assert str(exc) == (
            "Opened SocialBlade in the visible shared Chrome window, but Cloudflare clearance is still missing. "
            "Complete the challenge there and retry."
        )
    else:
        raise AssertionError("Expected refresh_socialblade_cookies to raise")

    assert opened == {"cdp_url": "http://127.0.0.1:9222"}
