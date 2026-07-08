"""Targeted tests for Modal app secret resolution helpers."""

from __future__ import annotations

import importlib
import os
import sys
import types

import pytest

from trr_backend import modal_jobs


class _FakeImage:
    def __init__(self) -> None:
        self.operations: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    @classmethod
    def debian_slim(cls, **kwargs):
        image = cls()
        image.operations.append(("debian_slim", (), dict(kwargs)))
        return image

    def pip_install_from_requirements(self, *args, **kwargs):
        self.operations.append(("pip_install_from_requirements", args, dict(kwargs)))
        return self

    def pip_install(self, *args, **kwargs):
        self.operations.append(("pip_install", args, dict(kwargs)))
        return self

    def add_local_python_source(self, *args, **kwargs):
        self.operations.append(("add_local_python_source", args, dict(kwargs)))
        return self

    def add_local_file(self, *args, **kwargs):
        self.operations.append(("add_local_file", args, dict(kwargs)))
        return self

    def add_local_dir(self, *args, **kwargs):
        self.operations.append(("add_local_dir", args, dict(kwargs)))
        return self

    def apt_install(self, *args, **kwargs):
        self.operations.append(("apt_install", args, dict(kwargs)))
        return self

    def run_commands(self, *args, **kwargs):
        self.operations.append(("run_commands", args, dict(kwargs)))
        return self


def _ops_for(image: _FakeImage, name: str) -> list[tuple[object, ...]]:
    return [args for op_name, args, _kwargs in image.operations if op_name == name]


def _clear_modal_owner_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", raising=False)
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)


def test_resolve_modal_secrets_uses_named_secrets_when_both_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    named: list[str] = []
    dotenv_paths: list[object] = []

    monkeypatch.setenv("TRR_MODAL_RUNTIME_SECRET_NAME", "trr-backend-runtime")
    monkeypatch.setenv("TRR_MODAL_SOCIAL_SECRET_NAME", "trr-social-auth")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_name",
        lambda name: named.append(name) or {"named": name},
    )
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_dotenv",
        lambda path: dotenv_paths.append(path) or {"dotenv": str(path)},
    )

    secrets = modal_jobs._resolve_modal_secrets()

    assert secrets == [
        {"named": "trr-backend-runtime"},
        {"named": "trr-social-auth"},
    ]
    assert named == ["trr-backend-runtime", "trr-social-auth"]
    assert dotenv_paths == []


def test_resolve_modal_secrets_rejects_partial_named_secret_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SECRET_NAME", "trr-backend-runtime")
    monkeypatch.delenv("TRR_MODAL_SOCIAL_SECRET_NAME", raising=False)

    with pytest.raises(RuntimeError, match="Modal secret configuration is partial"):
        modal_jobs._resolve_modal_secrets()


def test_resolve_modal_secrets_requires_named_secrets_outside_local_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    named: list[str] = []

    monkeypatch.delenv("TRR_MODAL_RUNTIME_SECRET_NAME", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_SECRET_NAME", raising=False)
    monkeypatch.setattr(modal_jobs, "_is_local_or_dev_runtime", lambda: False)
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_name",
        lambda name: named.append(name) or {"named": name},
    )

    secrets = modal_jobs._resolve_modal_secrets()

    assert secrets == [
        {"named": "trr-backend-runtime"},
        {"named": "trr-social-auth"},
    ]
    assert named == ["trr-backend-runtime", "trr-social-auth"]


def test_resolve_modal_secrets_uses_named_secrets_for_enabled_modal_job_plane_in_local_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    named: list[str] = []
    dotenv_paths: list[object] = []

    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SECRET_NAME", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_SECRET_NAME", raising=False)
    monkeypatch.delenv("TRR_MODAL_ALLOW_DOTENV_FALLBACK", raising=False)
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_name",
        lambda name: named.append(name) or {"named": name},
    )
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_dotenv",
        lambda path: dotenv_paths.append(path) or {"dotenv": str(path)},
    )

    secrets = modal_jobs._resolve_modal_secrets()

    assert secrets == [
        {"named": "trr-backend-runtime"},
        {"named": "trr-social-auth"},
    ]
    assert named == ["trr-backend-runtime", "trr-social-auth"]
    assert dotenv_paths == []


def test_resolve_modal_secrets_keeps_explicit_dotenv_fallback_for_local_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dotenv_paths: list[object] = []

    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("TRR_MODAL_ALLOW_DOTENV_FALLBACK", "1")
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SECRET_NAME", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_SECRET_NAME", raising=False)
    monkeypatch.setattr(
        modal_jobs.modal.Secret,
        "from_dotenv",
        lambda path: dotenv_paths.append(path) or {"dotenv": str(path)},
    )

    secrets = modal_jobs._resolve_modal_secrets()

    assert secrets == [{"dotenv": str(modal_jobs._BACKEND_ROOT)}]
    assert dotenv_paths == [modal_jobs._BACKEND_ROOT]


def test_api_custom_domains_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRR_MODAL_API_CUSTOM_DOMAINS", raising=False)

    assert modal_jobs._api_custom_domains() is None


def test_api_custom_domains_splits_comma_delimited_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "TRR_MODAL_API_CUSTOM_DOMAINS",
        "api.therealityreport.com, api-staging.therealityreport.com ",
    )

    assert modal_jobs._api_custom_domains() == [
        "api.therealityreport.com",
        "api-staging.therealityreport.com",
    ]


def test_inject_modal_runtime_defaults_sets_canonical_modal_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in modal_jobs._CANONICAL_MODAL_RUNTIME_DEFAULTS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_JOB_PLANE_MODE"] == "remote"
    assert os.environ["TRR_REMOTE_EXECUTOR"] == "modal"
    assert os.environ["TRR_MODAL_MAINTENANCE_OWNER_REQUIRED"] == "1"
    assert "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED" not in os.environ
    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED" not in os.environ
    assert os.environ["TRR_DB_POOL_MINCONN"] == "1"
    assert os.environ["TRR_DB_POOL_MAXCONN"] == "2"
    assert os.environ["TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN"] == "1"
    assert os.environ["TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN"] == "1"
    assert os.environ["TRR_DB_POOL_CLOSE_AFTER_RETURN"] == "1"
    assert os.environ["TRR_DB_POOL_ACQUIRE_ATTEMPTS"] == "30"
    assert os.environ["TRR_DB_POOL_ACQUIRE_SLEEP_MS"] == "200"
    assert os.environ["TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT"] == "8"
    assert os.environ["TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT"] == "4"
    assert os.environ["TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT"] == "4"
    assert os.environ["TRR_MODAL_SOCIAL_MEDIA_JOB_CONCURRENCY_LIMIT"] == "1"
    assert os.environ["TRR_MODAL_SOCIAL_RECOVERY_CONCURRENCY_LIMIT"] == "1"
    assert os.environ["TRR_MODAL_CAST_SCREENTIME_FUNCTION"] == "run_cast_screentime_analysis"
    assert os.environ["TRR_MODAL_CAST_SCREENTIME_CONCURRENCY_LIMIT"] == "2"
    assert os.environ["SOCIAL_MODAL_DISPATCH_LIMIT"] == "12"
    assert os.environ["SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY"] == "true"
    assert os.environ["SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED"] == "false"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"] == "decodo"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY"] == "true"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY"] == "false"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS"] == "600"
    # Throughput Phase 1: public-relay GraphQL page size pinned to the clamp
    # ceiling (downgrade-protected in the fetcher).
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_PAGE_SIZE"] == "50"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE"] == "50"
    assert os.environ["INSTAGRAM_BROWSER_NETWORK_POLICY_ENABLED"] == "true"
    assert os.environ["INSTAGRAM_BROWSER_BLOCK_STATIC_ASSETS"] == "true"
    assert os.environ["INSTAGRAM_BROWSER_DISABLE_EXTRA_RESOURCES"] == "true"
    assert os.environ["INSTAGRAM_BROWSER_NETWORK_POLICY_REPORT_ONLY"] == "false"
    assert os.environ["SOCIAL_WORKER_POOL_COMMENTS"] == "4"
    assert os.environ["SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY"] == "3"
    assert os.environ["SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS"] == "8"
    assert os.environ["SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM"] == "2"
    assert os.environ["SOCIAL_WORKER_POOL_MEDIA_MIRROR"] == "1"
    assert os.environ["SOCIAL_MIRROR_PLATFORM_CAP"] == "1"
    assert os.environ["SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP"] == "8"
    assert os.environ["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "4"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT"] == "8"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_MAX_SHARD_COUNT"] == "1000"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE"] == "advisory"
    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_PER_POST_CONCURRENCY"] == "1"
    assert os.environ["SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED"] == "true"
    assert os.environ["SOCIAL_THREADS_POSTS_PROXY_PROVIDER"] == "decodo"
    assert os.environ["SOCIALBLADE_PROXY_PROVIDER"] == "decodo"
    assert os.environ["SOCIALBLADE_USE_STICKY_PROXY"] == "false"
    assert os.environ["SOCIALBLADE_PROXY_SESSION_TTL_SECONDS"] == "600"
    assert os.environ["SOCIALBLADE_SCRAPLING_SOLVE_CLOUDFLARE"] == "true"
    assert os.environ["SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS"] == "180"
    assert os.environ["SOCIAL_QUEUE_ENABLED"] == "true"


def test_inject_modal_runtime_defaults_preserves_owner_selection_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "0")

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_MODAL_MAINTENANCE_OWNER_REQUIRED"] == "1"
    assert os.environ["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "1"
    assert os.environ["TRR_MODAL_RUNTIME_SCHEDULER_ENABLED"] == "0"


def test_inject_modal_runtime_defaults_overrides_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "custom")
    monkeypatch.setenv("TRR_DB_POOL_MAXCONN", "1")

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_JOB_PLANE_MODE"] == "remote"
    assert os.environ["TRR_DB_POOL_MAXCONN"] == "2"


def test_inject_modal_runtime_defaults_preserves_operator_tunable_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "none")

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"] == "none"


def test_inject_modal_runtime_defaults_applies_operator_tunable_default_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOCIALBLADE_PROXY_PROVIDER", raising=False)

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["SOCIALBLADE_PROXY_PROVIDER"] == "decodo"


def test_inject_modal_runtime_defaults_keeps_safety_clamps_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", "999")

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT"] == "4"


def test_operator_tunable_runtime_default_keys_exist_in_canonical_defaults() -> None:
    assert (
        modal_jobs._OPERATOR_TUNABLE_RUNTIME_DEFAULT_KEYS
        <= modal_jobs._CANONICAL_MODAL_RUNTIME_DEFAULTS.keys()
    )


def test_inject_modal_runtime_defaults_clears_object_storage_profile_when_static_creds_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_PROFILE", "trr")
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")

    modal_jobs._inject_modal_runtime_defaults()

    assert "OBJECT_STORAGE_PROFILE" not in os.environ


def test_reddit_runtime_probe_payload_reports_missing_oauth_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("REDDIT_USER_AGENT", raising=False)

    payload = modal_jobs._reddit_runtime_probe_payload()

    assert payload["healthy"] is False
    assert payload["reason"] == "reddit_oauth_missing"
    assert payload["missing_env"] == ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"]
    assert payload["warnings"] == ["REDDIT_USER_AGENT"]
    assert payload["supports_oauth"] is False
    assert payload["user_agent_configured"] is False
    assert payload["uses_default_user_agent"] is True


def test_reddit_runtime_probe_payload_reports_healthy_oauth_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "client-id")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("REDDIT_USER_AGENT", "TRRTest/1.0")

    payload = modal_jobs._reddit_runtime_probe_payload()

    assert payload["healthy"] is True
    assert payload["reason"] == "ok"
    assert payload["missing_env"] == []
    assert payload["warnings"] == []
    assert payload["supports_oauth"] is True
    assert payload["user_agent_configured"] is True
    assert payload["uses_default_user_agent"] is False
    assert payload["effective_user_agent"] == "TRRTest/1.0"


def test_probe_instagram_public_history_scrubs_auth_proxy_and_decodo_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from trr_backend.socials.instagram import public_probe

    for name in (
        *public_probe.COOKIE_ENV_VARS,
        *public_probe.DECODO_ENV_VARS,
        *public_probe.PROXY_ENV_VARS,
        *public_probe.AUTH_ENV_VARS,
        *public_probe.PROXY_PROVIDER_ENV_VARS,
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_JSON", "{}")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", "http://proxy.example")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "trr")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")

    def fake_run_public_probe(config):
        assert (
            public_probe.validate_public_environment(
                strict_public=config.strict_public,
                fail_if_cookies=config.fail_if_cookies,
                fail_if_decodo=config.fail_if_decodo,
            )
            == []
        )
        assert config.target_years == (2025, 2026)
        assert config.continue_after_boundary is True
        return public_probe.PublicProbeResult(
            account=config.account,
            historical_boundary=config.until_date.isoformat(),
            target_years=list(config.target_years),
            continue_after_boundary=config.continue_after_boundary,
            stop_reason="account_exhausted",
            account_exhausted=True,
        )

    monkeypatch.setattr(public_probe, "run_public_probe", fake_run_public_probe)

    payload = modal_jobs.probe_instagram_public_history.local(
        account_handle="BravoTV",
        until_date="2025-01-01",
        target_years="2025,2026",
        max_pages=1,
        state_file=str(tmp_path / "state.json"),
        output_file=str(tmp_path / "output.json"),
    )

    assert payload["account"] == "bravotv"
    assert payload["stop_reason"] == "account_exhausted"
    assert payload["execution_backend"] == "modal"
    assert payload["auth_state"] == "public"
    assert payload["proxy_state"] == "none"
    assert payload["decodo_state"] == "none"
    assert payload["target_years"] == [2025, 2026]
    assert payload["continue_after_boundary"] is True
    assert payload["output_file"] == str(tmp_path / "output.json")
    assert payload["modal_public_env_scrubbed"] == [
        "DECODO_USERNAME",
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        "SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER",
        "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS",
        "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID",
    ]
    assert os.environ["DECODO_USERNAME"] == "decodo-user"


def test_probe_instagram_public_history_accepts_and_returns_resume_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from trr_backend.socials.instagram import public_probe

    for name in (
        *public_probe.COOKIE_ENV_VARS,
        *public_probe.DECODO_ENV_VARS,
        *public_probe.PROXY_ENV_VARS,
        *public_probe.AUTH_ENV_VARS,
        *public_probe.PROXY_PROVIDER_ENV_VARS,
    ):
        monkeypatch.delenv(name, raising=False)

    state_path = tmp_path / "state.json"
    incoming_state = {
        "account": "bravotv",
        "cursor": "cursor-2",
        "pages_recovered": 1,
        "posts": [],
        "seen_cursors": ["cursor-2"],
        "unique_shortcodes": [],
    }

    def fake_run_public_probe(config):
        loaded = public_probe._load_state(config.state_file)  # noqa: SLF001
        assert loaded["cursor"] == "cursor-2"
        loaded["cursor"] = "cursor-3"
        loaded["pages_recovered"] = 2
        config.state_file.write_text(__import__("json").dumps(loaded), encoding="utf-8")
        return public_probe.PublicProbeResult(
            account=config.account,
            historical_boundary=config.until_date.isoformat(),
            target_years=list(config.target_years),
            stop_reason="public_graphql_403_backoff_required",
            next_retry_after_seconds=7200,
        )

    monkeypatch.setattr(public_probe, "run_public_probe", fake_run_public_probe)

    payload = modal_jobs.probe_instagram_public_history.local(
        account_handle="BravoTV",
        until_date="2025-01-01",
        target_years="2025,2026",
        max_pages=5,
        state_file=str(state_path),
        state_payload=incoming_state,
        scrub_public_env=True,
    )

    assert payload["stop_reason"] == "public_graphql_403_backoff_required"
    assert payload["next_retry_after_seconds"] == 7200
    assert payload["state_payload"]["cursor"] == "cursor-3"
    assert payload["state_payload"]["pages_recovered"] == 2


def test_social_concurrency_limit_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "17")
    monkeypatch.delenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", raising=False)

    reloaded = importlib.reload(modal_jobs)
    try:
        assert reloaded._SOCIAL_CONCURRENCY_LIMIT == 17
        assert reloaded._SOCIAL_COMMENTS_CONCURRENCY_LIMIT == 4
        assert reloaded._SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT == 4
    finally:
        monkeypatch.delenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", raising=False)
        _clear_modal_owner_env(monkeypatch)
        importlib.reload(modal_jobs)


def test_social_comments_concurrency_limit_reads_comments_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "17")
    monkeypatch.setenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", "11")
    monkeypatch.setenv("TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT", "3")

    reloaded = importlib.reload(modal_jobs)
    try:
        assert reloaded._SOCIAL_CONCURRENCY_LIMIT == 17
        assert reloaded._SOCIAL_COMMENTS_CONCURRENCY_LIMIT == 11
        assert reloaded._SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT == 3
    finally:
        monkeypatch.delenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", raising=False)
        monkeypatch.delenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", raising=False)
        monkeypatch.delenv("TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT", raising=False)
        _clear_modal_owner_env(monkeypatch)
        importlib.reload(modal_jobs)


def test_execute_social_job_closes_db_pool_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.socials import control_plane

    close_calls: list[str] = []

    monkeypatch.setattr(
        control_plane,
        "claim_and_process_social_job",
        lambda *, job_id, worker_id: {"claimed": True, "job": {"id": job_id, "worker_id": worker_id}},
    )
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    result = modal_jobs._execute_social_job("job-1", worker_prefix="modal:social-posts")

    assert result["job_id"] == "job-1"
    assert result["claimed"] is True
    assert result["worker_family"] == "social"
    assert close_calls == ["closed"]


def test_execute_social_job_closes_db_pool_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.socials import control_plane

    close_calls: list[str] = []

    def _raise_db_error(*, job_id: str, worker_id: str) -> None:
        raise RuntimeError(f"claim failed {job_id} {worker_id}")

    monkeypatch.setattr(control_plane, "claim_and_process_social_job", _raise_db_error)
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    with pytest.raises(RuntimeError, match="claim failed job-1"):
        modal_jobs._execute_social_job("job-1", worker_prefix="modal:social-posts")

    assert close_calls == ["closed"]


def test_execute_admin_operation_closes_db_pool_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.pipeline import admin_operations

    close_calls: list[str] = []

    monkeypatch.setattr(admin_operations, "wait_for_sub_operation_dependencies", lambda _operation_id: True)
    monkeypatch.setattr(
        admin_operations,
        "claim_and_execute_operation",
        lambda *, operation_id, worker_id, operation_types: True,
    )
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    result = modal_jobs._execute_admin_operation("operation-1", "admin_show_refresh")

    assert result["claimed"] is True
    assert result["worker_family"] == "admin_operations"
    assert close_calls == ["closed"]


def test_run_google_news_sync_closes_db_pool_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from api.routers import admin_show_news
    from trr_backend.db import pg

    close_calls: list[str] = []
    monkeypatch.setattr(admin_show_news, "claim_and_execute_google_news_sync_job", lambda *, job_id, worker_id: True)
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    payload = modal_jobs.run_google_news_sync.local("job-1")

    assert payload["claimed"] is True
    assert payload["worker_family"] == "google_news"
    assert close_calls == ["closed"]


def test_run_reddit_refresh_closes_db_pool_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import reddit_refresh

    close_calls: list[str] = []
    monkeypatch.setattr(reddit_refresh, "execute_refresh_run", lambda run_id, *, worker_id: {"status": "completed"})
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    payload = modal_jobs.run_reddit_refresh.local("run-1")

    assert payload["status"] == "completed"
    assert payload["worker_family"] == "reddit_refresh"
    assert close_calls == ["closed"]


def test_sweep_social_dispatch_queue_closes_db_pool_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg

    close_calls: list[str] = []
    monkeypatch.setitem(
        sys.modules,
        "trr_backend.socials.control_plane",
        types.SimpleNamespace(
            recover_and_dispatch_due_social_jobs=lambda: {
                "status": "completed",
                "recovered": 2,
                "dispatched": 1,
            },
        ),
    )
    monkeypatch.setattr(
        modal_jobs,
        "_recover_stale_pending_social_catalog_launches",
        lambda: {"scanned": 0, "recovered": 0, "recovered_run_ids": [], "failed_run_ids": []},
    )
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    payload = modal_jobs.sweep_social_dispatch_queue.local()

    assert payload == {
        "status": "completed",
        "recovered": 2,
        "dispatched": 1,
        "pending_launch_recovery": {
            "scanned": 0,
            "recovered": 0,
            "recovered_run_ids": [],
            "failed_run_ids": [],
        },
    }
    assert close_calls == ["closed"]


def test_sweep_social_dispatch_queue_invokes_pending_launch_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg

    recovery_calls: list[str] = []
    monkeypatch.setitem(
        sys.modules,
        "trr_backend.socials.control_plane",
        types.SimpleNamespace(
            recover_and_dispatch_due_social_jobs=lambda: {
                "status": "completed",
                "recovered": 0,
                "dispatched": 0,
            },
        ),
    )
    monkeypatch.setattr(
        modal_jobs,
        "_recover_stale_pending_social_catalog_launches",
        lambda: recovery_calls.append("recovered")
        or {"scanned": 1, "recovered": 1, "recovered_run_ids": ["run-1"], "failed_run_ids": []},
    )
    monkeypatch.setattr(pg, "close_pool", lambda: None)

    payload = modal_jobs.sweep_social_dispatch_queue.local()

    assert recovery_calls == ["recovered"]
    assert payload["pending_launch_recovery"] == {
        "scanned": 1,
        "recovered": 1,
        "recovered_run_ids": ["run-1"],
        "failed_run_ids": [],
    }


def test_sweep_social_dispatch_queue_pending_launch_recovery_error_does_not_fail_sweep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg

    monkeypatch.setitem(
        sys.modules,
        "trr_backend.socials.control_plane",
        types.SimpleNamespace(
            recover_and_dispatch_due_social_jobs=lambda: {
                "status": "completed",
                "recovered": 3,
                "dispatched": 2,
            },
        ),
    )

    def _boom() -> dict[str, object]:
        raise RuntimeError("recovery scan blew up")

    monkeypatch.setattr(modal_jobs, "_recover_stale_pending_social_catalog_launches", _boom)
    monkeypatch.setattr(pg, "close_pool", lambda: None)

    payload = modal_jobs.sweep_social_dispatch_queue.local()

    assert payload["status"] == "completed"
    assert payload["recovered"] == 3
    assert payload["dispatched"] == 2
    assert payload["pending_launch_recovery"] == {"status": "error"}


def test_recover_stale_pending_social_catalog_launches_recovers_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.social_season_analytics_impl as social_core

    fetched: list[tuple[str, list[object]]] = []
    recover_calls: list[dict[str, str]] = []

    def _fake_fetch_all(query: str, params=None, **_kwargs):
        fetched.append((query, list(params or [])))
        return [
            {
                "run_id": "run-1",
                "config": {"platforms": ["instagram"], "accounts_override": ["bravotv"]},
            },
            {
                "run_id": "run-2",
                "config": {"platforms": ["tiktok"], "accounts_override": ["bravotv"]},
            },
            {
                # Missing platform/account: not addressable per-account, must be skipped.
                "run_id": "run-3",
                "config": {},
            },
        ]

    def _fake_recover(*, platform: str, account_handle: str, run_id: str) -> dict[str, object]:
        recover_calls.append({"platform": platform, "account_handle": account_handle, "run_id": run_id})
        if run_id == "run-2":
            raise RuntimeError("finalize failed")
        return {"recovered": True, "reason": "finalized", "run_id": run_id}

    monkeypatch.setattr(social_core.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_core, "recover_pending_social_account_catalog_launch", _fake_recover)

    summary = modal_jobs._recover_stale_pending_social_catalog_launches(limit=7)

    assert len(fetched) == 1
    query, params = fetched[0]
    assert "launch_state" in query
    assert "scrape_runs" in query
    assert params[0] == social_core.SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE
    assert params[-1] == 7
    assert recover_calls == [
        {"platform": "instagram", "account_handle": "bravotv", "run_id": "run-1"},
        {"platform": "tiktok", "account_handle": "bravotv", "run_id": "run-2"},
    ]
    assert summary == {
        "scanned": 3,
        "recovered": 1,
        "recovered_run_ids": ["run-1"],
        "failed_run_ids": ["run-2"],
    }


def test_recover_stale_pending_social_catalog_launches_skips_unrecovered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.social_season_analytics_impl as social_core

    monkeypatch.setattr(
        social_core.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "run_id": "run-1",
                "config": {"platforms": ["instagram"], "accounts_override": ["bravotv"]},
            }
        ],
    )
    monkeypatch.setattr(
        social_core,
        "recover_pending_social_account_catalog_launch",
        lambda **_kwargs: {"recovered": False, "reason": "recovery_lock_busy", "run_id": "run-1"},
    )

    summary = modal_jobs._recover_stale_pending_social_catalog_launches()

    assert summary == {
        "scanned": 1,
        "recovered": 0,
        "recovered_run_ids": [],
        "failed_run_ids": [],
    }


def test_modal_deploy_schedules_are_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    partial_modal = types.ModuleType("modal")
    monkeypatch.setitem(sys.modules, "modal", partial_modal)
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", raising=False)
    monkeypatch.delenv("TRR_MODAL_API_MIN_CONTAINERS", raising=False)
    monkeypatch.delenv("TRR_MODAL_ADMIN_KEEP_WARM", raising=False)

    reloaded = importlib.reload(modal_jobs)
    try:
        assert "schedule" not in reloaded.sweep_social_dispatch_queue._modal_function_options
        assert "schedule" not in reloaded.heartbeat_remote_executors._modal_function_options
        assert "schedule" not in reloaded.sync_nbcumv_official_images._modal_function_options
        assert "schedule" not in reloaded.purge_stale_social_worker_heartbeats._modal_function_options
        assert reloaded.serve_backend_api._modal_function_options["min_containers"] == 0
        assert reloaded.run_admin_operation_v2._modal_function_options["min_containers"] == 0
    finally:
        monkeypatch.delitem(sys.modules, "modal", raising=False)
        _clear_modal_owner_env(monkeypatch)
        importlib.reload(modal_jobs)


def test_modal_deploy_schedules_can_be_enabled_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    partial_modal = types.ModuleType("modal")
    monkeypatch.setitem(sys.modules, "modal", partial_modal)
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "0")

    reloaded = importlib.reload(modal_jobs)
    try:
        heartbeat_schedule = reloaded.heartbeat_remote_executors._modal_function_options["schedule"]
        social_recovery_schedule = reloaded.sweep_social_dispatch_queue._modal_function_options["schedule"]
        nbcumv_schedule = reloaded.sync_nbcumv_official_images._modal_function_options["schedule"]
        cleanup_schedule = reloaded.purge_stale_social_worker_heartbeats._modal_function_options["schedule"]
        assert heartbeat_schedule.expression == "* * * * *"
        assert social_recovery_schedule.expression == "*/2 * * * *"
        assert nbcumv_schedule.expression == "15 14 * * *"
        assert cleanup_schedule.expression == "17 4 * * *"
    finally:
        monkeypatch.delitem(sys.modules, "modal", raising=False)
        _clear_modal_owner_env(monkeypatch)
        importlib.reload(modal_jobs)


def test_modal_maintenance_owner_required_rejects_no_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    with pytest.raises(RuntimeError, match="no active owner") as excinfo:
        modal_jobs._validate_modal_maintenance_owner_config()
    message = str(excinfo.value)
    assert "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=1" in message
    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1" in message
    assert "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1" in message
    assert "scripts/modal/prepare_named_secrets.py --apply" in message


def test_modal_maintenance_owner_required_fails_closed_without_required_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_jobs, "_is_local_or_dev_runtime", lambda: False)
    monkeypatch.delenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", raising=False)
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    with pytest.raises(RuntimeError, match="no active owner"):
        modal_jobs._validate_modal_maintenance_owner_config()


def test_modal_maintenance_owner_required_fails_closed_when_disabled_outside_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_jobs, "_is_local_or_dev_runtime", lambda: False)
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "0")
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    with pytest.raises(RuntimeError, match="no active owner"):
        modal_jobs._validate_modal_maintenance_owner_config()


def test_modal_maintenance_owner_required_allows_explicit_local_dev_bypass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(modal_jobs, "_is_local_or_dev_runtime", lambda: True)
    monkeypatch.delenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", raising=False)
    monkeypatch.delenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", raising=False)
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    assert modal_jobs._validate_modal_maintenance_owner_config() is None


def test_modal_maintenance_owner_required_rejects_duplicate_owners(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.setenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", "1")

    with pytest.raises(RuntimeError, match="duplicate active owners") as excinfo:
        modal_jobs._validate_modal_maintenance_owner_config()
    message = str(excinfo.value)
    assert "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED='1'" in message
    assert "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED='1'" in message
    assert "scripts/modal/prepare_named_secrets.py --apply" in message


def test_modal_maintenance_owner_required_accepts_modal_singleton_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_MAINTENANCE_OWNER_REQUIRED", "1")
    monkeypatch.setenv("TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED", "1")
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SCHEDULER_ENABLED", raising=False)

    assert modal_jobs._validate_modal_maintenance_owner_config() == "modal_singleton_cron"


def test_build_social_image_base_includes_shared_script_payloads() -> None:
    image = modal_jobs._build_social_image_base(image_factory=_FakeImage)

    added_files = {
        args[0]: kwargs["remote_path"] for op_name, args, kwargs in image.operations if op_name == "add_local_file"
    }
    added_dirs = {
        args[0]: kwargs["remote_path"] for op_name, args, kwargs in image.operations if op_name == "add_local_dir"
    }

    assert _ops_for(image, "add_local_python_source") == [("api", "trr_backend")]
    assert added_files == dict(modal_jobs._SOCIAL_IMAGE_LOCAL_FILES)
    assert added_dirs == dict(modal_jobs._SOCIAL_IMAGE_LOCAL_DIRS)
    assert _ops_for(image, "pip_install_from_requirements") == [(str(modal_jobs._MODAL_BROWSER_REQUIREMENTS),)]
    assert _ops_for(image, "pip_install") == []
    assert _ops_for(image, "apt_install") == []
    assert _ops_for(image, "run_commands") == []


def test_build_social_image_base_adds_browser_runtime_when_requested() -> None:
    image = modal_jobs._build_social_image_base(include_browser_runtime=True, image_factory=_FakeImage)

    assert _ops_for(image, "apt_install") == [modal_jobs._SOCIAL_BROWSER_APT_PACKAGES]
    assert _ops_for(image, "run_commands") == [modal_jobs._SOCIAL_BROWSER_SETUP_COMMANDS]


def test_build_lean_image_base_omits_social_browser_payloads() -> None:
    image = modal_jobs._build_lean_image_base(image_factory=_FakeImage)

    added_files = {
        args[0]: kwargs["remote_path"] for op_name, args, kwargs in image.operations if op_name == "add_local_file"
    }
    added_dirs = {
        args[0]: kwargs["remote_path"] for op_name, args, kwargs in image.operations if op_name == "add_local_dir"
    }

    assert _ops_for(image, "add_local_python_source") == [("api", "trr_backend")]
    assert _ops_for(image, "pip_install_from_requirements") == [(str(modal_jobs._MODAL_LEAN_REQUIREMENTS),)]
    assert _ops_for(image, "pip_install") == []
    assert _ops_for(image, "apt_install") == []
    assert added_files == dict(modal_jobs._LEAN_IMAGE_LOCAL_FILES)
    assert added_dirs == dict(modal_jobs._LEAN_IMAGE_LOCAL_DIRS)


def test_run_social_job_uses_browser_capable_image_binding() -> None:
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_admin_operation"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_admin_operation_v2"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_google_news_sync"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_reddit_refresh"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["probe_reddit_refresh_runtime"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_posts_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_media_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_comments_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_comments_recovery_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_socialblade_scrape"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["probe_socialblade_runtime"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["heartbeat_remote_executors"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["purge_stale_social_worker_heartbeats"] is modal_jobs._image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_admin_vision"] is modal_jobs._vision_image


def test_run_socialblade_scrape_persists_payload_with_following_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.socialblade.auth as auth_module
    import trr_backend.socials.socialblade.scraper as scraper_module
    import trr_backend.socials.socialblade.service as service_module

    captured: dict[str, object] = {}

    monkeypatch.setattr(auth_module, "load_socialblade_cookies_from_sources", lambda: {"cf_clearance": "token"})

    def fake_scrape_socialblade(
        handle: str,
        cookies,
        *,
        platform: str = "instagram",
        allow_login_fallback: bool = True,
        allow_visible_browser_retry: bool = True,
    ):
        captured["scrape"] = {
            "handle": handle,
            "cookies": cookies,
            "platform": platform,
            "allow_login_fallback": allow_login_fallback,
            "allow_visible_browser_retry": allow_visible_browser_retry,
        }
        return {"username": handle, "platform": platform, "stats_refreshed": True}

    def fake_attach(payload, *, handle: str, platform: str, source: str, source_scope: str, enabled: bool):
        captured["sidecar"] = {
            "handle": handle,
            "platform": platform,
            "source": source,
            "source_scope": source_scope,
            "enabled": enabled,
        }
        return {**payload, "instagram_following_scrape": {"status": "completed"}}

    def fake_refresh_and_persist_socialblade(**kwargs):
        captured["refresh_kwargs"] = kwargs
        return kwargs["scraper"](service_module.sanitize_socialblade_handle(kwargs["handle"]))

    monkeypatch.setattr(scraper_module, "scrape_socialblade", fake_scrape_socialblade)
    monkeypatch.setattr(service_module, "attach_instagram_following_scrape", fake_attach)
    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)

    runner = getattr(modal_jobs.run_socialblade_scrape, "local", modal_jobs.run_socialblade_scrape)
    payload = runner(
        "NetworkOfficial",
        person_id="person-1",
        source="season_run",
        force=True,
        platform="instagram",
        scrape_following=True,
        source_scope="creator",
    )

    assert payload["instagram_following_scrape"] == {"status": "completed"}
    assert captured["refresh_kwargs"] == {
        "person_id": "person-1",
        "handle": "NetworkOfficial",
        "scraper": captured["refresh_kwargs"]["scraper"],
        "source": "season_run",
        "force": True,
        "platform": "instagram",
    }
    assert captured["scrape"] == {
        "handle": "networkofficial",
        "cookies": {"cf_clearance": "token"},
        "platform": "instagram",
        "allow_login_fallback": False,
        "allow_visible_browser_retry": False,
    }
    assert captured["sidecar"] == {
        "handle": "networkofficial",
        "platform": "instagram",
        "source": "season_run",
        "source_scope": "creator",
        "enabled": True,
    }


def test_run_socialblade_scrape_persists_account_scoped_payload_without_person(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.socialblade.auth as auth_module
    import trr_backend.socials.socialblade.scraper as scraper_module
    import trr_backend.socials.socialblade.service as service_module

    captured: dict[str, object] = {}

    monkeypatch.setattr(auth_module, "load_socialblade_cookies_from_sources", lambda: {})
    monkeypatch.setattr(scraper_module, "scrape_socialblade", lambda handle, _cookies, **_kwargs: {"username": handle})
    monkeypatch.setattr(service_module, "attach_instagram_following_scrape", lambda payload, **_kwargs: payload)

    def fake_refresh_and_persist_socialblade(**kwargs):
        captured["refresh_kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(service_module, "refresh_and_persist_socialblade", fake_refresh_and_persist_socialblade)

    runner = getattr(modal_jobs.run_socialblade_scrape, "local", modal_jobs.run_socialblade_scrape)
    payload = runner("NetworkOfficial", source="all_saved_instagram_backfill", force=True, platform="instagram")

    assert payload == {"ok": True}
    assert captured["refresh_kwargs"] == {
        "person_id": None,
        "handle": "NetworkOfficial",
        "scraper": captured["refresh_kwargs"]["scraper"],
        "source": "all_saved_instagram_backfill",
        "force": True,
        "platform": "instagram",
    }


def test_heartbeat_remote_executors_reports_social_auth_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg

    close_calls: list[str] = []
    recorded: list[dict[str, object]] = []

    def _fake_record_dispatcher_heartbeat(**kwargs):
        recorded.append(kwargs)

    monkeypatch.setitem(
        sys.modules,
        "trr_backend.modal_dispatch",
        types.SimpleNamespace(_record_dispatcher_heartbeat=_fake_record_dispatcher_heartbeat),
    )
    monkeypatch.setitem(
        sys.modules,
        "trr_backend.socials.control_plane",
        types.SimpleNamespace(
            is_queue_enabled=lambda: True,
            get_worker_auth_capabilities=lambda: {"instagram_authenticated": True, "twitter_authenticated": False},
        ),
    )
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    payload = modal_jobs.heartbeat_remote_executors.local()

    assert payload == {
        "ok": True,
        "social_auth_capabilities": {"instagram_authenticated": True, "twitter_authenticated": False},
    }
    social_call = next(call for call in recorded if call["dispatcher_name"] == "social")
    assert social_call["metadata_updates"]["auth_capabilities"] == {
        "instagram_authenticated": True,
        "twitter_authenticated": False,
    }
    assert social_call["metadata_updates"]["modal_capacity"]["modal_function"] == "run_social_job"
    assert social_call["metadata_updates"]["modal_capacity_by_function"]
    assert any(
        item["modal_function"] == "run_social_comments_recovery_job"
        and item["max_containers"] == modal_jobs._SOCIAL_COMMENTS_RECOVERY_CONCURRENCY_LIMIT
        for item in social_call["metadata_updates"]["modal_capacity_by_function"]
    )
    admin_call = next(call for call in recorded if call["dispatcher_name"] == "admin")
    assert admin_call["metadata_updates"]["modal_capacity"] == {
        "worker_family": "admin_operations",
        "modal_app": modal_jobs._APP_NAME,
        "modal_function": "run_admin_operation_v2",
        "image_family": "lean",
        "timeout_seconds": modal_jobs._ADMIN_OPERATION_TIMEOUT_SECONDS,
        "min_containers": modal_jobs._ADMIN_KEEP_WARM,
        "max_containers": modal_jobs._ADMIN_CONCURRENCY_LIMIT,
    }
    assert social_call["supported_platforms"] == list(modal_jobs.SOCIAL_SUPPORTED_PLATFORMS)
    assert close_calls == ["closed"]


def test_modal_completion_evidence_contract_is_explicit() -> None:
    contract = modal_jobs.modal_completion_evidence_contract()

    assert contract["modal_update_status_required"] is True
    assert contract["blocker_required_when_not_updated"] is True
    assert "modal_update_status" in contract["required_completion_fields"]
    assert "blocker" in contract["required_completion_fields"]
    assert "verify_modal_readiness.py" in contract["readiness_command"]
    assert any(
        "tests/api/test_health.py tests/test_modal_jobs.py" in command for command in contract["local_verification"]
    )


def test_cast_screentime_modal_function_uses_vision_image() -> None:
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_cast_screentime_analysis"] is modal_jobs._vision_image


def test_run_cast_screentime_analysis_delegates_to_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.services import retained_cast_screentime_runtime

    worker_events: list[tuple[str, dict[str, object]]] = []
    close_calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        retained_cast_screentime_runtime,
        "run_screentime_analysis",
        lambda run_id: {"run_id": run_id, "status": "success"},
    )
    monkeypatch.setattr(
        modal_jobs,
        "_worker_started",
        lambda worker_family, **kwargs: worker_events.append(("started", {"worker_family": worker_family, **kwargs}))
        or "started-at",
    )
    monkeypatch.setattr(
        modal_jobs,
        "_worker_finished",
        lambda worker_family, _started_at, **kwargs: worker_events.append(
            ("finished", {"worker_family": worker_family, **kwargs})
        ),
    )
    monkeypatch.setattr(
        modal_jobs,
        "_close_db_pools_after_worker",
        lambda worker_family, **kwargs: close_calls.append((worker_family, kwargs)),
    )

    result = modal_jobs.run_cast_screentime_analysis.local("run-123")

    assert result == {"run_id": "run-123", "status": "success"}
    assert worker_events == [
        (
            "started",
            {
                "worker_family": "cast_screentime",
                "function_name": "run_cast_screentime_analysis",
                "run_id": "run-123",
            },
        ),
        (
            "finished",
            {
                "worker_family": "cast_screentime",
                "result_status": "success",
                "run_id": "run-123",
            },
        ),
    ]
    assert close_calls == [("cast_screentime", {"run_id": "run-123"})]


def test_purge_stale_social_worker_heartbeats_uses_seven_day_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg

    close_calls: list[str] = []
    captured: dict[str, object] = {"params": []}

    def _fake_fetch_one(_sql, params=None):
        captured["fetch_sql"] = _sql
        captured["params"].append(params)
        return {"active_workers": 3, "total_workers": 23}

    def _fake_execute_returning(sql, params=None):
        captured["delete_sql"] = sql
        captured["params"].append(params)
        return [{"worker_id": "old-1", "status": "stopped"}, {"worker_id": "old-2", "status": "idle"}]

    monkeypatch.setattr(pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(pg, "execute_returning", _fake_execute_returning)
    monkeypatch.setattr(pg, "close_pool", lambda: close_calls.append("closed"))

    payload = modal_jobs.purge_stale_social_worker_heartbeats.local()

    assert captured["params"] == [[7 * 24 * 60 * 60], [7 * 24 * 60 * 60]]
    assert "last_seen_at < now()" in str(captured["delete_sql"])
    assert "where not" not in str(captured["delete_sql"]).lower()
    assert payload["deleted_workers"] == 2
    assert payload["deleted_by_status"] == {"idle": 1, "stopped": 1}
    assert payload["cleanup_policy"] == "delete_rows_older_than_threshold"
    assert payload["worker_family"] == "social_worker_heartbeat_cleanup"
    assert close_calls == ["closed"]


def test_reload_falls_back_to_stub_when_modal_module_is_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial_modal = types.ModuleType("modal")
    monkeypatch.setitem(sys.modules, "modal", partial_modal)

    reloaded = importlib.reload(modal_jobs)
    try:
        assert hasattr(reloaded.modal, "Image")
        assert hasattr(reloaded.modal, "Secret")
        assert hasattr(reloaded.modal, "App")
        assert hasattr(reloaded.modal, "asgi_app")
    finally:
        monkeypatch.delitem(sys.modules, "modal", raising=False)
        _clear_modal_owner_env(monkeypatch)
        importlib.reload(modal_jobs)


def test_comments_db_session_budget_status(monkeypatch):
    # Default container caps: comments(4) + recovery(4) = 8 workers; per-worker
    # sessions = default pool(2) + social_control(1) = 3; demand = 24.
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET", raising=False)
    status = modal_jobs.comments_db_session_budget_status()
    assert status["sessions_per_worker"] == 3
    assert status["demand"] == status["worker_cap"] * status["sessions_per_worker"]
    # Budget 0/unset disables the check.
    assert status["within_budget"] is True

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET", str(status["demand"] - 1))
    assert modal_jobs.comments_db_session_budget_status()["within_budget"] is False

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET", str(status["demand"]))
    assert modal_jobs.comments_db_session_budget_status()["within_budget"] is True

    # Garbage budget is treated as disabled.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET", "not-a-number")
    assert modal_jobs.comments_db_session_budget_status()["within_budget"] is True
