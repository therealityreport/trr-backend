from __future__ import annotations

from pathlib import Path

import pytest

from scripts.dev import smoke_decodo_residential_proxy as smoke


def clear_decodo_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "DECODO_PROXY_URL",
        "DECODO_USERNAME",
        "DECODO_PASSWORD",
        "DECODO_GATEWAY",
        "SCRAPER_API_TOKEN",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(smoke, "launchctl_getenv", lambda _key: "")


def test_dry_run_uses_proxy_url_without_web_scraping_api_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_decodo_env(monkeypatch)
    monkeypatch.setenv("DECODO_PROXY_URL", "http://user:password@gate.decodo.com:10001")
    monkeypatch.setenv("SCRAPER_API_TOKEN", "not-used-by-trr")

    args = smoke.parse_args(["--dry-run", "--json", "--env-file", str(tmp_path / ".env")])
    report = smoke.build_report(args)

    assert report["state"] == "ok"
    assert report["web_scraping_api_required"] is False
    assert report["required_decodo_product"] == "residential_proxy"
    assert report["proxy"]["url"] == "http://<redacted>:<redacted>@gate.decodo.com:10001"
    assert "SCRAPER_API_TOKEN" in report["custom_scraper_env"]["not_required"]


def test_dry_run_accepts_username_password_gateway_from_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_decodo_env(monkeypatch)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DECODO_USERNAME=user\nDECODO_PASSWORD=secret\nDECODO_GATEWAY=gate.decodo.com:10001\n",
        encoding="utf-8",
    )

    args = smoke.parse_args(["--dry-run", "--json", "--env-file", str(env_file)])
    report = smoke.build_report(args)

    assert report["state"] == "ok"
    assert report["proxy_configured"] is True
    assert report["proxy"]["host"] == "gate.decodo.com"
    assert report["proxy"]["port"] == 10001


def test_missing_proxy_config_does_not_accept_scraper_api_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_decodo_env(monkeypatch)
    monkeypatch.setenv("SCRAPER_API_TOKEN", "not-used-by-trr")

    args = smoke.parse_args(["--dry-run", "--json", "--env-file", str(tmp_path / ".env")])
    report = smoke.build_report(args)

    assert report["state"] == "advisory"
    assert report["reason"] == "decodo_proxy_unconfigured"
    assert report["proxy_configured"] is False


def test_classifies_exhausted_decodo_traffic() -> None:
    result = smoke.classify_proxy_response(
        407,
        "HTTP/1.1 407 Proxy Authentication Required\r\nX-Error: traffic limit exceeded",
    )

    assert result == "traffic_exhausted_or_plan_limit"


def test_classifies_plain_407_as_proxy_auth() -> None:
    result = smoke.classify_proxy_response(407, "HTTP/1.1 407 Proxy Authentication Required")

    assert result == "proxy_auth_407"
