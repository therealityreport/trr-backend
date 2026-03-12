from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values

_AWS_PROFILE_ENV_KEYS = frozenset({"AWS_PROFILE", "AWS_DEFAULT_PROFILE"})


def _has_explicit_aws_credentials() -> bool:
    return bool(
        str(os.getenv("AWS_ACCESS_KEY_ID") or "").strip()
        and str(os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip()
    )


def load_env(*, override: bool = False) -> Path | None:
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        Path.cwd() / ".env",
        repo_root / ".env",
    ]
    for path in candidates:
        if path.is_file():
            skip_profile_env = _has_explicit_aws_credentials()
            for key, value in dotenv_values(path).items():
                if value is None:
                    continue
                if skip_profile_env and key in _AWS_PROFILE_ENV_KEYS:
                    continue
                if override or key not in os.environ:
                    os.environ[key] = value
            return path
    return None
