from __future__ import annotations

import os
from pathlib import Path

from trr_backend.utils.env import load_env


def test_load_env_preserves_existing_environment_by_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("OBJECT_STORAGE_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OBJECT_STORAGE_PROFILE", "runtime")
    monkeypatch.delenv("CUSTOM_VALUE", raising=False)

    loaded = load_env()

    assert loaded == env_file
    assert os.getenv("OBJECT_STORAGE_PROFILE") == "runtime"
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"


def test_load_env_falls_back_to_repo_root_when_cwd_has_no_env(
    monkeypatch,
) -> None:
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    monkeypatch.chdir(Path(__file__).resolve().parent)

    loaded = load_env()

    assert loaded == repo_env


def test_load_env_skips_object_storage_profile_from_dotenv_when_static_credentials_present(
    monkeypatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OBJECT_STORAGE_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("OBJECT_STORAGE_PROFILE", raising=False)

    loaded = load_env()

    assert loaded == env_file
    assert os.getenv("OBJECT_STORAGE_PROFILE") is None
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"
