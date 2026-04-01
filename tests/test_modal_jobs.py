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


def test_resolve_modal_secrets_keeps_dotenv_fallback_for_local_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dotenv_paths: list[object] = []

    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "local")
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

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_JOB_PLANE_MODE"] == "remote"
    assert os.environ["TRR_REMOTE_EXECUTOR"] == "modal"
    assert os.environ["SOCIAL_QUEUE_ENABLED"] == "true"


def test_inject_modal_runtime_defaults_overrides_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRR_JOB_PLANE_MODE", "custom")

    modal_jobs._inject_modal_runtime_defaults()

    assert os.environ["TRR_JOB_PLANE_MODE"] == "remote"


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


def test_social_concurrency_limit_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "17")

    reloaded = importlib.reload(modal_jobs)
    try:
        assert reloaded._SOCIAL_CONCURRENCY_LIMIT == 17
    finally:
        monkeypatch.delenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", raising=False)
        importlib.reload(modal_jobs)


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
    assert _ops_for(image, "pip_install") == [modal_jobs._SOCIAL_IMAGE_PIP_PACKAGES]
    assert _ops_for(image, "apt_install") == []
    assert _ops_for(image, "run_commands") == []


def test_build_social_image_base_adds_browser_runtime_when_requested() -> None:
    image = modal_jobs._build_social_image_base(include_browser_runtime=True, image_factory=_FakeImage)

    assert _ops_for(image, "apt_install") == [modal_jobs._SOCIAL_BROWSER_APT_PACKAGES]
    assert _ops_for(image, "run_commands") == [modal_jobs._SOCIAL_BROWSER_SETUP_COMMANDS]


def test_run_social_job_uses_browser_capable_image_binding() -> None:
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_socialblade_scrape"] is modal_jobs._browser_image


def test_heartbeat_remote_executors_reports_social_auth_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        "trr_backend.repositories.social_season_analytics",
        types.SimpleNamespace(
            is_queue_enabled=lambda: True,
            get_worker_auth_capabilities=lambda: {"instagram_authenticated": True, "twitter_authenticated": False},
        ),
    )

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
    assert social_call["supported_platforms"] == list(modal_jobs.SOCIAL_SUPPORTED_PLATFORMS)


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
        importlib.reload(modal_jobs)
