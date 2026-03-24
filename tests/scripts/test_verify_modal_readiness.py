from __future__ import annotations

import json

import pytest

from scripts.modal import verify_modal_readiness as cli


class _StubFunctionHandle:
    def __init__(self, *, web_url: str | None = None, hydrate_error: str | None = None) -> None:
        self._web_url = web_url
        self._hydrate_error = hydrate_error

    def hydrate(self) -> None:
        if self._hydrate_error:
            raise RuntimeError(self._hydrate_error)

    def get_web_url(self) -> str | None:
        return self._web_url


def test_expected_function_names_includes_reddit_runtime_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_MODAL_REDDIT_RUNTIME_PROBE_FUNCTION", raising=False)

    function_names = cli.expected_function_names()

    assert "probe_reddit_refresh_runtime" in function_names


def test_verify_modal_readiness_passes_when_all_resources_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli,
        "list_secret_names",
        lambda *, modal_environment="": {"trr-backend-runtime", "trr-social-auth"},
    )
    monkeypatch.setattr(cli, "list_app_descriptions", lambda *, modal_environment="": {"trr-backend-jobs"})
    monkeypatch.setattr(
        cli,
        "get_app_function_handles",
        lambda *, app_name, modal_environment="": {
            "serve_backend_api": _StubFunctionHandle(web_url="https://workspace--trr-backend-api.modal.run"),
            "run_admin_operation": _StubFunctionHandle(),
            "run_social_job": _StubFunctionHandle(),
            "run_socialblade_scrape": _StubFunctionHandle(),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "run_admin_operation", "run_social_job", "run_socialblade_scrape"),
    )

    assert summary["ok"] is True
    assert summary["missing_secrets"] == []
    assert summary["missing_functions"] == []
    assert summary["app_found"] is True
    assert summary["api_web_url"] == "https://workspace--trr-backend-api.modal.run"
    assert summary["missing_web_endpoints"] == []


def test_verify_modal_readiness_reports_missing_secret_and_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "list_secret_names", lambda *, modal_environment="": {"trr-backend-runtime"})
    monkeypatch.setattr(cli, "list_app_descriptions", lambda *, modal_environment="": {"trr-backend-jobs"})
    monkeypatch.setattr(
        cli,
        "get_app_function_handles",
        lambda *, app_name, modal_environment="": {
            "serve_backend_api": _StubFunctionHandle(web_url=None),
            "run_admin_operation": _StubFunctionHandle(),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "run_admin_operation", "run_social_job", "run_socialblade_scrape"),
    )

    assert summary["ok"] is False
    assert summary["missing_secrets"] == ["trr-social-auth"]
    assert summary["missing_functions"] == ["run_social_job", "run_socialblade_scrape"]
    assert summary["missing_web_endpoints"] == ["serve_backend_api"]


def test_verify_modal_readiness_handles_missing_modal_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli,
        "list_secret_names",
        lambda *, modal_environment="": {"trr-backend-runtime", "trr-social-auth"},
    )
    monkeypatch.setattr(cli, "list_app_descriptions", lambda *, modal_environment="": {"trr-backend-jobs"})

    def _raise_modal_error(*, app_name: str, modal_environment: str = "") -> dict[str, object]:
        raise RuntimeError("Modal experimental helpers are unavailable")

    monkeypatch.setattr(cli, "get_app_function_handles", _raise_modal_error)

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api",),
    )

    assert summary["ok"] is False
    assert summary["app_found"] is True
    assert summary["missing_functions"] == ["serve_backend_api"]


def test_main_emits_json_and_returns_nonzero_when_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "app_name": "trr-backend-jobs",
                "runtime_secret_name": "trr-backend-runtime",
                "social_secret_name": "trr-social-auth",
                "env": "main",
                "json": True,
            },
        )(),
    )
    monkeypatch.setattr(cli, "expected_function_names", lambda: ("run_admin_operation",))
    monkeypatch.setattr(
        cli,
        "verify_modal_readiness",
        lambda **kwargs: {
            "ok": False,
            "app_name": kwargs["app_name"],
            "modal_environment": kwargs["modal_environment"],
            "app_found": True,
            "runtime_secret_name": kwargs["runtime_secret_name"],
            "social_secret_name": kwargs["social_secret_name"],
            "missing_secrets": ["trr-social-auth"],
            "function_results": [{"name": "run_admin_operation", "resolved": True, "error": None}],
            "missing_functions": [],
            "api_function_name": "serve_backend_api",
            "api_web_url": None,
            "missing_web_endpoints": ["serve_backend_api"],
        },
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["missing_secrets"] == ["trr-social-auth"]
    assert payload["modal_environment"] == "main"
    assert payload["missing_web_endpoints"] == ["serve_backend_api"]
