from __future__ import annotations

import pytest

from scripts.dev import verify_external_runtime_contracts as cli


def test_verify_render_contract_flags_blueprint_drift(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    render_yaml = tmp_path / "render.yaml"
    render_doc = tmp_path / "render.md"
    render_yaml.write_text("services:\n  - type: web\n", encoding="utf-8")
    render_doc.write_text("Render docs\n", encoding="utf-8")
    monkeypatch.setattr(cli, "RENDER_BLUEPRINT_PATH", render_yaml)
    monkeypatch.setattr(cli, "RENDER_DOC_PATH", render_doc)

    result = cli.verify_render_contract()

    assert result["state"] == "advisory"
    assert result["reason"] == "render_contract_mismatch"


def test_verify_decodo_contract_warns_when_credentials_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.delenv("SCRAPER_API_TOKEN", raising=False)
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))

    result = cli.verify_decodo_contract()

    assert result["state"] == "advisory"
    assert result["reason"] == "decodo_unconfigured"
    assert result["mcp_token_configured"] is False
    assert result["proxy_configured"] is False


def test_verify_decodo_contract_accepts_configured_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
    assert result["gateway"] == "gate.decodo.com:7000"
    assert result["proxy_configured"] is True


def test_verify_decodo_contract_accepts_launchctl_mcp_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.delenv("SCRAPER_API_TOKEN", raising=False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda key: key == "SCRAPER_API_TOKEN")

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
    assert result["mcp_token_configured"] is True
    assert result["proxy_configured"] is False
    assert result["token_sources"] == ["launchctl:SCRAPER_API_TOKEN"]


def test_verify_decodo_contract_falls_back_to_repo_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text("DECODO_USERNAME=user\nDECODO_PASSWORD=secret\n", encoding="utf-8")
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", source_env)

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
