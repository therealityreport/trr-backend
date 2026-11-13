from __future__ import annotations

import pytest

from scripts.dev import verify_external_runtime_contracts as cli


@pytest.mark.parametrize("state,reason", [("ok", None), ("advisory", "decodo_unconfigured")])
def test_external_runtime_contracts_only_reports_supported_auxiliary_services(
    monkeypatch: pytest.MonkeyPatch, state: str, reason: str | None,
) -> None:
    component = {"state": state, "reason": reason, "verify_only": True}
    monkeypatch.setattr(cli, "verify_decodo_contract", lambda: component)

    assert cli.verify_external_runtime_contracts() == {
        "state": state, "reason": reason, "decodo": component,
    }


def test_verify_decodo_contract_warns_when_credentials_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.delenv("SCRAPER_API_TOKEN", raising=False)
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))

    result = cli.verify_decodo_contract()

    assert result["state"] == "advisory"
    assert result["reason"] == "decodo_unconfigured"
    assert result["proxy_configured"] is False
    assert result["usage_api_configured"] is False
    assert result["required_decodo_product"] == "residential_proxy"
    assert result["web_scraping_api_required"] is False


def test_verify_decodo_contract_accepts_configured_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:10001")
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
    assert result["gateway"] == "gate.decodo.com:10001"
    assert result["proxy_configured"] is True
    assert result["credential_proxy_configured"] is True


def test_verify_decodo_contract_accepts_launchctl_proxy_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.delenv("SCRAPER_API_TOKEN", raising=False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda key: key == "DECODO_PROXY_URL")

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
    assert result["proxy_configured"] is True
    assert result["proxy_url_configured"] is True


def test_verify_decodo_contract_ignores_scraper_api_token_for_trr_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.setenv("SCRAPER_API_TOKEN", "not-used-by-trr-custom-scrapers")
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", cli.Path("/tmp/does-not-exist.env"))
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)

    result = cli.verify_decodo_contract()

    assert result["state"] == "advisory"
    assert result["reason"] == "decodo_unconfigured"
    assert result["proxy_configured"] is False
    assert "SCRAPER_API_TOKEN" in result["custom_scraper_env"]["not_required"]


def test_verify_decodo_contract_falls_back_to_repo_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_env = tmp_path / ".env"
    source_env.write_text("DECODO_USERNAME=user\nDECODO_PASSWORD=secret\n", encoding="utf-8")
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    monkeypatch.setattr(cli, "_launchctl_has_env_value", lambda _key: False)
    monkeypatch.setattr(cli, "SOURCE_ENV_PATH", source_env)

    result = cli.verify_decodo_contract()

    assert result["state"] == "ok"
