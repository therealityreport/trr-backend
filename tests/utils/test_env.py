from __future__ import annotations

import os
from pathlib import Path

from trr_backend.utils.env import load_env


def test_load_env_preserves_existing_environment_by_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("AWS_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AWS_PROFILE", "runtime")
    monkeypatch.delenv("CUSTOM_VALUE", raising=False)

    loaded = load_env()

    assert loaded == env_file
    assert os.getenv("AWS_PROFILE") == "runtime"
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"


def test_load_env_falls_back_to_repo_root_when_cwd_has_no_env(
    monkeypatch,
) -> None:
    repo_env = Path(__file__).resolve().parents[2] / ".env"
    monkeypatch.chdir(Path(__file__).resolve().parent)

    loaded = load_env()

    assert loaded == repo_env


def test_load_env_skips_aws_profile_from_dotenv_when_static_credentials_present(
    monkeypatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "AWS_PROFILE=trr\nAWS_DEFAULT_PROFILE=trr\nCUSTOM_VALUE=from-dotenv\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)

    loaded = load_env()

    assert loaded == env_file
    assert os.getenv("AWS_PROFILE") is None
    assert os.getenv("AWS_DEFAULT_PROFILE") is None
    assert os.getenv("CUSTOM_VALUE") == "from-dotenv"
