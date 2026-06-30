from __future__ import annotations

import pytest

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


def test_socialblade_chrome_profile_preflight_rejects_retired_managed_profile(monkeypatch) -> None:
    retired_profile = "codex" + "-agent"
    monkeypatch.setenv("CODEX_CHROME_SEED_PROFILE_DIR", f"/Users/test/.chrome-profiles/{retired_profile}")

    with pytest.raises(auth_module.VisibleManagedChromeProfileError, match="retired profile"):
        auth_module.preflight_socialblade_chrome_profile()


def test_socialblade_chrome_profile_preflight_accepts_openai_agent(monkeypatch) -> None:
    monkeypatch.setenv("CODEX_CHROME_SEED_PROFILE_DIR", "/Users/test/.chrome-profiles/openai-agent")
    monkeypatch.setenv("CODEX_CHROME_PROFILE_DIR", "/Users/test/.chrome-profiles/openai-agent-devtools")

    auth_module.preflight_socialblade_chrome_profile()


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
    monkeypatch.setattr(auth_module, "_ensure_visible_managed_chrome_available", lambda _cdp_url: False)

    cookies = auth_module.refresh_socialblade_cookies("test-refresh", allow_headless_fallback=False)

    assert cookies == {"cf_clearance": "token"}
    assert captured == {"cdp_url": "http://127.0.0.1:9222"}


def test_visible_managed_chrome_reachable_validates_codex_profile(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(auth_module, "_chrome_cdp_endpoint_reachable", lambda *_args, **_kwargs: True)
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

    assert auth_module._ensure_visible_managed_chrome_available("http://127.0.0.1:9222") is False
    assert captured["cmd"] == ["bash", "/tmp/fake-script"]
    assert captured["env"] == {
        "CODEX_CHROME_MODE": "shared",
        "CODEX_CHROME_SHARED_PORT": "9222",
        "CHROME_AGENT_DEBUG_PORT": "9222",
        "CHROME_AGENT_HEADLESS": "0",
    }


def test_visible_managed_chrome_profile_error_blocks_headless_fallback(monkeypatch) -> None:
    def fake_ensure(_cdp_url: str) -> bool:
        raise auth_module.VisibleManagedChromeProfileError("wrong profile")

    monkeypatch.setattr(auth_module, "_socialblade_visible_chrome_cdp_url", lambda: "http://127.0.0.1:9222")
    monkeypatch.setattr(auth_module, "_ensure_visible_managed_chrome_available", fake_ensure)
    monkeypatch.setattr(
        auth_module,
        "export_socialblade_cookies_from_shared_chrome",
        lambda *, cdp_url=None: (_ for _ in ()).throw(AssertionError("must not export with wrong profile")),
    )

    with pytest.raises(auth_module.VisibleManagedChromeProfileError, match="wrong profile"):
        auth_module.refresh_socialblade_cookies("test-refresh", allow_headless_fallback=True)


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


def test_socialblade_cookie_health_report_redacts_values(monkeypatch, tmp_path) -> None:
    cookie_file = tmp_path / "socialblade_cookies.json"
    cookie_file.write_text('{"cf_clearance":"secret-clearance","session":"secret-session"}', encoding="utf-8")

    monkeypatch.setattr(auth_module, "socialblade_cookie_file_path", lambda: cookie_file)
    monkeypatch.setattr(
        auth_module,
        "load_socialblade_cookies_from_sources",
        lambda: {"cf_clearance": "secret-clearance", "session": "secret-session"},
    )
    monkeypatch.setattr(
        auth_module,
        "validate_socialblade_cookie_health",
        lambda _cookies, *, validation_handle=None: (True, None),
    )

    report = auth_module.socialblade_cookie_health_report(validate=True)

    assert report["healthy"] is True
    assert report["cookieNames"] == ["cf_clearance", "session"]
    assert report["cookieFile"]["exists"] is True
    assert "secret-clearance" not in str(report)
    assert "secret-session" not in str(report)


def test_socialblade_cookie_health_report_missing_session(monkeypatch) -> None:
    monkeypatch.setattr(auth_module, "socialblade_cookie_file_path", lambda: auth_module.Path("/tmp/missing.json"))
    monkeypatch.setattr(auth_module, "load_socialblade_cookies_from_sources", lambda: {"cf_clearance": "token"})

    report = auth_module.socialblade_cookie_health_report(validate=True)

    assert report["healthy"] is False
    assert report["reason"] == "missing_required_cookie:session"
    assert report["validation"]["checked"] is False


def test_socialblade_cookie_loader_preserves_browser_signal_cookies(monkeypatch, tmp_path) -> None:
    cookie_file = tmp_path / "socialblade_cookies.json"
    cookie_file.write_text(
        auth_module.json.dumps(
            {
                "_ga": "analytics",
                "_sharedID": "shared",
                "cf_clearance": "clearance",
                "session": "session-token",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("SOCIALBLADE_COOKIES_JSON", raising=False)
    monkeypatch.setenv("SOCIALBLADE_COOKIES_FILE", str(cookie_file))

    cookies = auth_module.load_socialblade_cookies_from_sources()

    assert cookies == {
        "_ga": "analytics",
        "_sharedID": "shared",
        "cf_clearance": "clearance",
        "session": "session-token",
    }
