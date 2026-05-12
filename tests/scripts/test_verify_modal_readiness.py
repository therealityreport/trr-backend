from __future__ import annotations

import json
import sys
import types

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
    monkeypatch.delenv("TRR_MODAL_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_INSTAGRAM_COMMENTS_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION", raising=False)
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "true")

    function_names = cli.expected_function_names()

    assert "probe_reddit_refresh_runtime" in function_names
    assert "probe_social_remote_auth" in function_names
    assert "probe_instagram_posts_auth" in function_names
    assert "probe_instagram_comments_auth" in function_names
    assert "probe_getty_remote_access" in function_names
    assert "run_social_posts_job" in function_names
    assert "run_social_media_job" in function_names
    assert "run_social_comments_job" in function_names
    assert cli.required_social_function_names() == (
        "run_social_job",
        "run_social_posts_job",
        "run_social_media_job",
        "run_social_comments_job",
    )


def test_expected_function_names_skips_social_functions_when_queue_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
    monkeypatch.delenv("TRR_MODAL_REDDIT_RUNTIME_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_INSTAGRAM_POSTS_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_INSTAGRAM_COMMENTS_AUTH_PROBE_FUNCTION", raising=False)
    monkeypatch.delenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION", raising=False)

    function_names = cli.expected_function_names()

    assert "run_social_job" not in function_names
    assert "run_social_comments_job" not in function_names
    assert cli.required_social_function_names(enabled=False) == ()


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
            "run_social_posts_job": _StubFunctionHandle(),
            "run_social_media_job": _StubFunctionHandle(),
            "run_social_comments_job": _StubFunctionHandle(),
            "run_socialblade_scrape": _StubFunctionHandle(),
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": True, "reason": None}
            ),
            "probe_instagram_posts_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "instagram",
                    "account_handle": "thetraitorsus",
                    "status": "valid",
                    "ready": True,
                    "reason": None,
                    "execution_backend": "modal",
                }
            ),
            "probe_instagram_comments_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "instagram",
                    "account_handle": "thetraitorsus",
                    "shortcode": "DSfwXnYAaEs",
                    "status": "valid",
                    "ready": True,
                    "reason": None,
                    "execution_backend": "modal",
                }
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
            "run_social_posts_job",
            "run_social_media_job",
            "run_social_comments_job",
            "run_socialblade_scrape",
            "probe_social_remote_auth",
            "probe_instagram_posts_auth",
            "probe_instagram_comments_auth",
            "probe_getty_remote_access",
        ),
        probe_remote_auth_platform="instagram",
        probe_instagram_posts_auth_handle="thetraitorsus",
        probe_instagram_comments_auth_handle="thetraitorsus",
        probe_instagram_comments_auth_shortcode="DSfwXnYAaEs",
        probe_getty_remote_access=True,
    )

    assert summary["ok"] is True
    assert summary["missing_secrets"] == []
    assert summary["missing_functions"] == []
    assert summary["app_found"] is True
    assert summary["app_lookup_error"] is None
    assert summary["social_jobs_enabled"] is True
    assert summary["required_social_function_names"] == [
        "run_social_job",
        "run_social_posts_job",
        "run_social_media_job",
        "run_social_comments_job",
    ]
    assert summary["configured_social_function_names"] == [
        "run_social_job",
        "run_social_posts_job",
        "run_social_media_job",
        "run_social_comments_job",
    ]
    assert summary["api_web_url"] == "https://workspace--trr-backend-api.modal.run"
    assert summary["missing_web_endpoints"] == []
    assert summary["core_ok"] is True
    assert summary["blocking_probe_failures"] == []
    assert summary["advisory_probe_failures"] == []
    assert summary["remote_auth_probe"] == {"platform": "instagram", "ready": True, "reason": None}
    assert summary["instagram_posts_auth_probe"] == {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "status": "valid",
        "ready": True,
        "reason": None,
        "execution_backend": "modal",
    }
    assert summary["instagram_comments_auth_probe"] == {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "shortcode": "DSfwXnYAaEs",
        "status": "valid",
        "ready": True,
        "reason": None,
        "execution_backend": "modal",
    }
    assert summary["getty_remote_probe"] == {"platform": "getty", "ready": True, "reason": None}


def test_verify_modal_readiness_accepts_tiktok_remote_auth_probe(monkeypatch: pytest.MonkeyPatch) -> None:
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
            "run_social_posts_job": _StubFunctionHandle(),
            "run_social_media_job": _StubFunctionHandle(),
            "run_social_comments_job": _StubFunctionHandle(),
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "tiktok",
                    "ready": True,
                    "reason": None,
                    "has_sessionid": True,
                    "has_sid_tt": False,
                    "has_ms_token": True,
                }
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
            "run_social_posts_job",
            "run_social_media_job",
            "run_social_comments_job",
            "probe_social_remote_auth",
        ),
        probe_remote_auth_platform="tiktok",
    )

    assert summary["ok"] is True
    assert summary["remote_auth_probe"] == {
        "platform": "tiktok",
        "ready": True,
        "reason": None,
        "has_sessionid": True,
        "has_sid_tt": False,
        "has_ms_token": True,
    }
    assert summary["blocking_probe_failures"] == []


def test_parse_args_accepts_tiktok_remote_auth_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["verify_modal_readiness.py", "--probe-remote-auth", "tiktok", "--json"],
    )

    args = cli._parse_args()

    assert args.probe_remote_auth == "tiktok"
    assert args.json is True


def test_parse_args_accepts_instagram_comments_auth_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_modal_readiness.py",
            "--probe-instagram-comments-auth",
            "thetraitorsus",
            "--probe-instagram-comments-shortcode",
            "DSfwXnYAaEs",
            "--json",
        ],
    )

    args = cli._parse_args()

    assert args.probe_instagram_comments_auth == "thetraitorsus"
    assert args.probe_instagram_comments_shortcode == "DSfwXnYAaEs"
    assert args.json is True


@pytest.mark.parametrize("platform", ["twitter", "facebook", "threads"])
def test_parse_args_accepts_additional_remote_auth_probes(
    monkeypatch: pytest.MonkeyPatch,
    platform: str,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["verify_modal_readiness.py", "--probe-remote-auth", platform, "--json"],
    )

    args = cli._parse_args()

    assert args.probe_remote_auth == platform
    assert args.json is True


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
    assert summary["core_ok"] is False
    assert summary["missing_secrets"] == ["trr-social-auth"]
    assert summary["missing_functions"] == ["run_social_job", "run_socialblade_scrape"]
    assert summary["missing_web_endpoints"] == ["serve_backend_api"]
    assert summary["blocking_probe_failures"] == ["checkpoint_required"]
    assert summary["advisory_probe_failures"] == ["challenge_page"]
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


def test_verify_modal_readiness_reports_missing_social_comments_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "true")
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
            "run_social_job": _StubFunctionHandle(),
            "run_social_posts_job": _StubFunctionHandle(),
            "run_social_media_job": _StubFunctionHandle(),
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": True, "reason": None}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=(
            "serve_backend_api",
            "run_social_job",
            "run_social_posts_job",
            "run_social_media_job",
            "run_social_comments_job",
            "probe_social_remote_auth",
        ),
        probe_remote_auth_platform="instagram",
    )

    assert summary["ok"] is False
    assert "run_social_comments_job" in summary["missing_functions"]
    assert summary["required_social_function_names"][-1] == "run_social_comments_job"


def test_verify_modal_readiness_requires_missing_social_comments_function_even_if_caller_omits_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "true")
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
            "run_social_job": _StubFunctionHandle(),
            "run_social_posts_job": _StubFunctionHandle(),
            "run_social_media_job": _StubFunctionHandle(),
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": True, "reason": None}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=(
            "serve_backend_api",
            "run_social_job",
            "run_social_posts_job",
            "run_social_media_job",
            "probe_social_remote_auth",
        ),
        probe_remote_auth_platform="instagram",
    )

    assert summary["ok"] is False
    assert summary["required_social_function_names"][-1] == "run_social_comments_job"
    assert summary["missing_required_social_functions"] == ["run_social_comments_job"]


def test_diagnose_instagram_comments_remote_uses_persisted_run_id_and_cleans_up_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_modal = types.ModuleType("modal")
    fake_modal.Function = types.SimpleNamespace(from_name=lambda *_args, **_kwargs: None)
    monkeypatch.setitem(sys.modules, "modal", fake_modal)

    from scripts.modal import diagnose_instagram_comments_remote as diagnose

    created_runs: list[dict[str, object]] = []
    created_jobs: list[dict[str, object]] = []
    cleanup_calls: list[tuple[str, tuple[object, ...]]] = []

    monkeypatch.setattr(
        diagnose.social_repo,
        "_create_run",
        lambda *_args, **kwargs: created_runs.append(kwargs) or "persisted-run-123",
    )

    def fake_create_job(*_args: object, **kwargs: object) -> str:
        created_jobs.append(kwargs)
        return "job-456"

    monkeypatch.setattr(diagnose.social_repo, "_create_job", fake_create_job)

    def fake_fetch_one(sql: str, params: list[object]) -> None:
        cleanup_calls.append((sql, tuple(params)))

    monkeypatch.setattr(diagnose.pg, "fetch_one", fake_fetch_one)

    run_id, job_id = diagnose._create_probe_job(shortcode="abc123", account_handle="thetraitorsus")
    diagnose._cleanup_probe_job(run_id=run_id, job_id=job_id)

    assert run_id == "persisted-run-123"
    assert job_id == "job-456"
    assert created_runs[0]["config"]["launch_group_id"].startswith("diagnostic:")
    assert created_jobs[0]["run_id"] == "persisted-run-123"
    assert cleanup_calls == [
        (
            """
            delete from social.scrape_jobs
             where id = %s
            returning id::text
            """,
            ("job-456",),
        ),
        (
            """
            delete from social.scrape_runs
             where id = %s
            returning id::text
            """,
            ("persisted-run-123",),
        ),
    ]


def test_verify_modal_readiness_ignores_missing_social_comments_function_when_queue_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
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
            "probe_social_remote_auth": _StubFunctionHandle(
                remote_payload={"platform": "instagram", "ready": True, "reason": None}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "run_admin_operation", "probe_social_remote_auth"),
        probe_remote_auth_platform="instagram",
    )

    assert summary["ok"] is True
    assert summary["social_jobs_enabled"] is False
    assert summary["required_social_function_names"] == []
    assert "run_social_comments_job" not in summary["missing_functions"]


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
    assert summary["blocking_probe_failures"] == ["probe_invocation_failed"]


def test_verify_modal_readiness_keeps_core_ready_when_only_getty_probe_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
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
            "probe_getty_remote_access": _StubFunctionHandle(
                remote_payload={"platform": "getty", "ready": False, "reason": "challenge_page"}
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "probe_getty_remote_access"),
        probe_getty_remote_access=True,
    )

    assert summary["ok"] is True
    assert summary["core_ok"] is True
    assert summary["blocking_probe_failures"] == []
    assert summary["advisory_probe_failures"] == ["challenge_page"]
    assert summary["getty_remote_probe"] == {
        "platform": "getty",
        "ready": False,
        "reason": "challenge_page",
    }


def test_verify_modal_readiness_blocks_comments_html_challenge_with_rendered_fallback_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_RENDERED_FALLBACK", "1")
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
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
            "probe_instagram_comments_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "instagram",
                    "account_handle": "thetraitorsus",
                    "shortcode": "SHORT1",
                    "status": "auth_blocked",
                    "ready": False,
                    "reason": "html_challenge_or_auth_required",
                    "execution_backend": "modal",
                }
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "probe_instagram_comments_auth"),
        probe_instagram_comments_auth_handle="thetraitorsus",
        probe_instagram_comments_auth_shortcode="SHORT1",
    )

    assert summary["ok"] is False
    assert summary["blocking_probe_failures"] == ["html_challenge_or_auth_required"]
    assert summary["advisory_probe_failures"] == []
    assert "advisory_continue" not in summary["instagram_comments_auth_probe"]


def test_verify_modal_readiness_blocks_comments_html_challenge_when_rendered_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_RENDERED_FALLBACK", "0")
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
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
            "probe_instagram_comments_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "instagram",
                    "account_handle": "thetraitorsus",
                    "shortcode": "SHORT1",
                    "status": "auth_blocked",
                    "ready": False,
                    "reason": "html_challenge_or_auth_required",
                    "execution_backend": "modal",
                }
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "probe_instagram_comments_auth"),
        probe_instagram_comments_auth_handle="thetraitorsus",
        probe_instagram_comments_auth_shortcode="SHORT1",
    )

    assert summary["ok"] is False
    assert summary["blocking_probe_failures"] == ["html_challenge_or_auth_required"]
    assert summary["advisory_probe_failures"] == []


def test_verify_modal_readiness_blocks_browser_session_invalidation_even_with_rendered_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_RENDERED_FALLBACK", "1")
    monkeypatch.setenv("SOCIAL_QUEUE_ENABLED", "false")
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
            "probe_instagram_comments_auth": _StubFunctionHandle(
                remote_payload={
                    "platform": "instagram",
                    "account_handle": "thetraitorsus",
                    "shortcode": "SHORT1",
                    "status": "auth_blocked",
                    "ready": False,
                    "reason": cli.BROWSER_SESSION_INVALIDATED_REASON,
                    "execution_backend": "modal",
                }
            ),
        },
    )

    summary = cli.verify_modal_readiness(
        app_name="trr-backend-jobs",
        runtime_secret_name="trr-backend-runtime",
        social_secret_name="trr-social-auth",
        function_names=("serve_backend_api", "probe_instagram_comments_auth"),
        probe_instagram_comments_auth_handle="thetraitorsus",
        probe_instagram_comments_auth_shortcode="SHORT1",
    )

    assert summary["ok"] is False
    assert summary["blocking_probe_failures"] == [cli.BROWSER_SESSION_INVALIDATED_REASON]
    assert summary["advisory_probe_failures"] == []
    assert "advisory_continue" not in summary["instagram_comments_auth_probe"]


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
                "probe_instagram_posts_auth": "",
                "probe_instagram_comments_auth": "",
                "probe_instagram_comments_shortcode": "",
                "probe_getty_remote_access": True,
                "strict_probes": False,
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
            "core_ok": False,
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
            "blocking_probe_failures": ["checkpoint_required"],
            "advisory_probe_failures": ["challenge_page"],
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


def test_main_returns_zero_when_only_advisory_probe_fails_without_strict_mode(
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
                "env": "",
                "json": True,
                "probe_remote_auth": "",
                "probe_instagram_posts_auth": "",
                "probe_instagram_comments_auth": "",
                "probe_instagram_comments_shortcode": "",
                "probe_getty_remote_access": True,
                "strict_probes": False,
            },
        )(),
    )
    monkeypatch.setattr(cli, "expected_function_names", lambda: ("serve_backend_api",))
    monkeypatch.setattr(
        cli,
        "verify_modal_readiness",
        lambda **_kwargs: {
            "ok": True,
            "core_ok": True,
            "app_name": "trr-backend-jobs",
            "modal_environment": None,
            "app_found": True,
            "app_lookup_error": None,
            "runtime_secret_name": "trr-backend-runtime",
            "social_secret_name": "trr-social-auth",
            "missing_secrets": [],
            "function_results": [{"name": "serve_backend_api", "resolved": True, "error": None}],
            "missing_functions": [],
            "api_function_name": "serve_backend_api",
            "api_web_url": "https://workspace--trr-backend-api.modal.run",
            "missing_web_endpoints": [],
            "remote_auth_probe": None,
            "getty_remote_probe": {"platform": "getty", "ready": False, "reason": "challenge_page"},
            "blocking_probe_failures": [],
            "advisory_probe_failures": ["challenge_page"],
        },
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["ok"] is True
    assert payload["advisory_probe_failures"] == ["challenge_page"]


def test_main_returns_nonzero_when_only_advisory_probe_fails_in_strict_mode(
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
                "env": "",
                "json": True,
                "probe_remote_auth": "",
                "probe_instagram_posts_auth": "",
                "probe_instagram_comments_auth": "",
                "probe_instagram_comments_shortcode": "",
                "probe_getty_remote_access": True,
                "strict_probes": True,
            },
        )(),
    )
    monkeypatch.setattr(cli, "expected_function_names", lambda: ("serve_backend_api",))
    monkeypatch.setattr(
        cli,
        "verify_modal_readiness",
        lambda **_kwargs: {
            "ok": True,
            "core_ok": True,
            "app_name": "trr-backend-jobs",
            "modal_environment": None,
            "app_found": True,
            "app_lookup_error": None,
            "runtime_secret_name": "trr-backend-runtime",
            "social_secret_name": "trr-social-auth",
            "missing_secrets": [],
            "function_results": [{"name": "serve_backend_api", "resolved": True, "error": None}],
            "missing_functions": [],
            "api_function_name": "serve_backend_api",
            "api_web_url": "https://workspace--trr-backend-api.modal.run",
            "missing_web_endpoints": [],
            "remote_auth_probe": None,
            "getty_remote_probe": {"platform": "getty", "ready": False, "reason": "challenge_page"},
            "blocking_probe_failures": [],
            "advisory_probe_failures": ["challenge_page"],
        },
    )

    rc = cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["ok"] is True
    assert payload["advisory_probe_failures"] == ["challenge_page"]


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
                "probe_instagram_posts_auth": "",
                "probe_instagram_comments_auth": "",
                "probe_instagram_comments_shortcode": "",
                "probe_getty_remote_access": False,
                "strict_probes": False,
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
