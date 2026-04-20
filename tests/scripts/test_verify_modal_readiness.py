from __future__ import annotations

import json

import pytest

from scripts.modal import verify_modal_readiness as cli


class _StubFunctionHandle:
    def __init__(
        self,
        *,
        web_url: str | None = None,
        hydrate_error: str | None = None,
        remote_payload: dict[str, object] | None = None,
        remote_error: str | None = None,
    ) -> None:
        self._web_url = web_url
        self._hydrate_error = hydrate_error
        self._remote_payload = remote_payload
        self._remote_error = remote_error

    def hydrate(self) -> None:
        if self._hydrate_error:
            raise RuntimeError(self._hydrate_error)

    def get_web_url(self) -> str | None:
        return self._web_url

    def remote(self, *_args: object, **_kwargs: object) -> dict[str, object]:
        if self._remote_error:
            raise RuntimeError(self._remote_error)
        return dict(self._remote_payload or {})


def test_expected_function_names_includes_reddit_runtime_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_MODAL_REDDIT_RUNTIME_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION", raising=False)

    function_names = cli.expected_function_names()

    assert "probe_reddit_refresh_runtime" in function_names
    assert "probe_social_remote_auth" in function_names
    assert "probe_getty_remote_access" in function_names


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
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": True, "reason": None}
            ),
            "probe_getty_remote_access": _StubFunctionHandle(
                remote_payload={"platform": "getty", "ready": True, "reason": None}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=(
            "serve_backend_api",
            "run_admin_operation",
            "run_social_job",
            "run_socialblade_scrape",
            "probe_social_remote_auth",
            "probe_getty_remote_access",
        ),
        probe_remote_auth_platform="instagram",
        probe_getty_remote_access=True,
    )

    assert summary["ok"] is True
    assert summary["missing_secrets"] == []
    assert summary["missing_functions"] == []
    assert summary["app_found"] is True
    assert summary["app_lookup_error"] is None
    assert summary["api_web_url"] == "https://workspace--trr-backend-api.modal.run"
    assert summary["missing_web_endpoints"] == []
    assert summary["remote_auth_probe"] == {"platform": "instagram", "ready": True, "reason": None}
    assert summary["getty_remote_probe"] == {"platform": "getty", "ready": True, "reason": None}


def test_verify_modal_readiness_reports_missing_secret_and_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "list_secret_names", lambda *, modal_environment="": {"trr-backend-runtime"})
    monkeypatch.setattr(cli, "list_app_descriptions", lambda *, modal_environment="": {"trr-backend-jobs"})
    monkeypatch.setattr(
        cli,
        "get_app_function_handles",
        lambda *, app_name, modal_environment="": {
            "serve_backend_api": _StubFunctionHandle(web_url=None),
            "run_admin_operation": _StubFunctionHandle(),
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": False, "reason": "checkpoint_required"}
            ),
            "probe_getty_remote_access": _StubFunctionHandle(
                remote_payload={"platform": "getty", "ready": False, "reason": "challenge_page"}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=(
            "serve_backend_api",
            "run_admin_operation",
            "run_social_job",
            "run_socialblade_scrape",
            "probe_social_remote_auth",
            "probe_getty_remote_access",
        ),
        probe_remote_auth_platform="instagram",
        probe_getty_remote_access=True,
    )

    assert summary["ok"] is False
    assert summary["missing_secrets"] == ["trr-social-auth"]
    assert summary["missing_functions"] == ["run_social_job", "run_socialblade_scrape"]
    assert summary["missing_web_endpoints"] == ["serve_backend_api"]
    assert summary["remote_auth_probe"] == {
        "platform": "instagram",
        "ready": False,
        "reason": "checkpoint_required",
    }
    assert summary["getty_remote_probe"] == {
        "platform": "getty",
        "ready": False,
        "reason": "challenge_page",
    }


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
    monkeypatch.setattr(
        cli,
        "get_named_function_handles",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("Modal Function helpers are unavailable")),
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api",),
        probe_remote_auth_platform="instagram",
        probe_getty_remote_access=True,
    )

    assert summary["ok"] is False
    assert summary["app_found"] is True
    assert summary["app_lookup_error"] == "Modal Function helpers are unavailable"
    assert summary["missing_functions"] == ["serve_backend_api"]
    assert summary["remote_auth_probe"] == {
        "platform": "instagram",
        "ready": False,
        "reason": "probe_function_unavailable",
    }
    assert summary["getty_remote_probe"] == {
        "platform": "getty",
        "ready": False,
        "reason": "probe_function_unavailable",
    }


def test_verify_modal_readiness_reports_remote_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
            "probe_social_remote_auth": _StubFunctionHandle(remote_error="remote auth probe exploded"),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "probe_social_remote_auth"),
        probe_remote_auth_platform="instagram",
    )

    assert summary["ok"] is False
    assert summary["remote_auth_probe"]["platform"] == "instagram"
    assert summary["remote_auth_probe"]["ready"] is False
    assert summary["remote_auth_probe"]["reason"] == "probe_invocation_failed"
    assert summary["remote_auth_probe"]["detail"]["exception_class"] == "RuntimeError"


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
                "probe_remote_auth": "instagram",
                "probe_getty_remote_access": True,
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
            "app_lookup_error": None,
            "runtime_secret_name": kwargs["runtime_secret_name"],
            "social_secret_name": kwargs["social_secret_name"],
            "missing_secrets": ["trr-social-auth"],
            "function_results": [{"name": "run_admin_operation", "resolved": True, "error": None}],
            "missing_functions": [],
            "api_function_name": "serve_backend_api",
            "api_web_url": None,
            "missing_web_endpoints": ["serve_backend_api"],
            "remote_auth_probe": {"platform": "instagram", "ready": False, "reason": "checkpoint_required"},
            "getty_remote_probe": {"platform": "getty", "ready": False, "reason": "challenge_page"},
        },
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["missing_secrets"] == ["trr-social-auth"]
    assert payload["modal_environment"] == "main"
    assert payload["missing_web_endpoints"] == ["serve_backend_api"]
    assert payload["remote_auth_probe"]["reason"] == "checkpoint_required"
    assert payload["getty_remote_probe"]["reason"] == "challenge_page"


def test_main_applies_workspace_runtime_env_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    monkeypatch.setattr(
        cli,
        "apply_workspace_runtime_env",
        lambda *, repo_root, environ=None: calls.append(repo_root) or {},
        raising=False,
    )
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
                "env": "",
                "json": True,
                "probe_remote_auth": "",
                "probe_getty_remote_access": False,
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "verify_modal_readiness",
        lambda **_kwargs: {
            "ok": True,
            "app_found": True,
            "app_lookup_error": None,
            "missing_secrets": [],
            "function_results": [],
            "missing_functions": [],
            "api_function_name": "serve_backend_api",
            "api_web_url": "https://workspace--trr-backend-api.modal.run",
            "missing_web_endpoints": [],
            "remote_auth_probe": None,
        },
    )

    assert cli.main() == 0
    assert calls
