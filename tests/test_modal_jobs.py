"""Targeted tests for Modal app secret resolution helpers."""

from __future__ import annotations

import os

import pytest

from trr_backend import modal_jobs


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
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("TRR_MODAL_RUNTIME_SECRET_NAME", raising=False)
    monkeypatch.delenv("TRR_MODAL_SOCIAL_SECRET_NAME", raising=False)
    monkeypatch.setattr(modal_jobs, "_is_local_or_dev_runtime", lambda: False)

    with pytest.raises(RuntimeError, match="require named secrets"):
        modal_jobs._resolve_modal_secrets()


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
