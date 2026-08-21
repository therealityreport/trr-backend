from __future__ import annotations

import os
from pathlib import Path

import pytest

from trr_backend.utils import env


def test_load_env_does_not_read_local_or_repo_dotenv_when_disabled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (cwd / ".env").write_text("CUSTOM_VALUE=from-local-dotenv\n", encoding="utf-8")
    (repo_root / ".env").write_text("CUSTOM_VALUE=from-repo-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(env, "REPO_ROOT", repo_root)
    monkeypatch.delenv("CUSTOM_VALUE", raising=False)
    monkeypatch.setenv("TRR_TEST_DISABLE_DOTENV", "1")
    monkeypatch.setattr(
        env,
        "dotenv_values",
        lambda path: pytest.fail(f"dotenv must not be read while disabled: {path}"),
    )

    assert env.load_env() is None
    assert os.getenv("CUSTOM_VALUE") is None


def test_load_env_preserves_existing_environment_by_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("TRR_TEST_DISABLE_DOTENV", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("OBJECT_STORAGE_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OBJECT_STORAGE_PROFILE", "runtime")
    monkeypatch.delenv("CUSTOM_VALUE", raising=False)

    loaded = env.load_env()

    assert loaded == env_file
    assert os.getenv("OBJECT_STORAGE_PROFILE") == "runtime"
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"


def test_load_env_falls_back_to_repo_root_when_cwd_has_no_env(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("TRR_TEST_DISABLE_DOTENV", raising=False)
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    repo_env = repo_root / ".env"
    repo_env.write_text("CUSTOM_VALUE=from-repo-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CUSTOM_VALUE", raising=False)
    monkeypatch.setattr(env, "REPO_ROOT", repo_root)

    loaded = env.load_env()

    assert loaded == repo_env
    assert os.getenv("CUSTOM_VALUE") == "from-repo-dotenv"


def test_load_env_skips_object_storage_profile_from_dotenv_when_static_credentials_present(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("TRR_TEST_DISABLE_DOTENV", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OBJECT_STORAGE_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("OBJECT_STORAGE_PROFILE", raising=False)

    loaded = env.load_env()

    assert loaded == env_file
    assert os.getenv("OBJECT_STORAGE_PROFILE") is None
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"
