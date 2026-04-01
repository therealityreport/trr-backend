from __future__ import annotations

import pytest

from api import main as api_main


def _clear_runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "TRR_LOCAL_DEV",
        "APP_ENV",
        "ENVIRONMENT",
        "TRR_ENV",
        "TRR_ENVIRONMENT",
        "PYTHON_ENV",
        "SCREENALYTICS_API_URL",
        "TRR_INTERNAL_ADMIN_SHARED_SECRET",
        "SCREENALYTICS_SERVICE_TOKEN",
        "SUPABASE_JWT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)


def test_validate_startup_config_allows_local_workspace_without_deployed_only_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("TRR_LOCAL_DEV", "1")
    monkeypatch.setattr(api_main, "log_database_resolution_summary", lambda: None)
    monkeypatch.setattr(api_main, "resolve_database_url_candidate_details", lambda: ())

    api_main._validate_startup_config()


def test_validate_startup_config_requires_deployed_only_secrets_for_deployed_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setattr(api_main, "log_database_resolution_summary", lambda: None)
    monkeypatch.setattr(api_main, "resolve_database_url_candidate_details", lambda: ())

    with pytest.raises(RuntimeError) as excinfo:
        api_main._validate_startup_config()

    message = str(excinfo.value)
    assert "TRR_INTERNAL_ADMIN_SHARED_SECRET" in message
    assert "SCREENALYTICS_SERVICE_TOKEN" in message
    assert "SUPABASE_JWT_SECRET" in message
