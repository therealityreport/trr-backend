from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_env(*, override: bool = False) -> Path | None:
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        repo_root / ".env",
        Path.cwd() / ".env",
    ]
    for path in candidates:
        if path.is_file():
            load_dotenv(dotenv_path=path, override=override)
            return path
    return None
