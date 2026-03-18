"""Targeted tests for Modal app secret resolution helpers."""

from __future__ import annotations

import importlib
import os

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
        args[0]: kwargs["remote_path"]
        for op_name, args, kwargs in image.operations
        if op_name == "add_local_file"
    }
    added_dirs = {
        args[0]: kwargs["remote_path"]
        for op_name, args, kwargs in image.operations
        if op_name == "add_local_dir"
    }

    assert _ops_for(image, "add_local_python_source") == [("api", "trr_backend")]
    assert added_files == dict(modal_jobs._SOCIAL_IMAGE_LOCAL_FILES)
    assert added_dirs == dict(modal_jobs._SOCIAL_IMAGE_LOCAL_DIRS)
    assert _ops_for(image, "apt_install") == []
    assert _ops_for(image, "run_commands") == []


def test_build_social_image_base_adds_browser_runtime_when_requested() -> None:
    image = modal_jobs._build_social_image_base(include_browser_runtime=True, image_factory=_FakeImage)

    assert _ops_for(image, "apt_install") == [modal_jobs._SOCIAL_BROWSER_APT_PACKAGES]
    assert _ops_for(image, "run_commands") == [modal_jobs._SOCIAL_BROWSER_SETUP_COMMANDS]


def test_run_social_job_uses_browser_capable_image_binding() -> None:
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_social_job"] is modal_jobs._browser_image
    assert modal_jobs._FUNCTION_IMAGE_BINDINGS["run_socialblade_scrape"] is modal_jobs._browser_image
