"""Import-neutral cookie source path and candidate helpers."""

from __future__ import annotations

import os
from pathlib import Path


def _default_platform_cookie_file_path(platform: str) -> Path:
    return Path(__file__).resolve().parents[2] / "scripts" / "socials" / platform / f"{platform}_cookies.json"


def _platform_cookie_file_candidates(default_path: Path, *env_keys: str) -> list[Path]:
    raw_candidates = [str(os.getenv(key) or "").strip() for key in env_keys]
    raw_candidates.append(str(default_path))
    return [Path(raw_path).expanduser() for raw_path in raw_candidates if raw_path]


def _platform_cookie_refresh_target_path(default_path: Path, *env_keys: str) -> Path:
    candidates = _platform_cookie_file_candidates(default_path, *env_keys)
    return candidates[0] if candidates else default_path


def _select_preferred_cookie_candidate(
    candidates: list[dict[str, str]],
    *,
    required_cookie_names_any: tuple[str, ...] = (),
    required_cookie_names_all: tuple[str, ...] = (),
) -> dict[str, str]:
    if not candidates:
        return {}
    if not required_cookie_names_any and not required_cookie_names_all:
        return dict(candidates[0])

    def _score(candidate: dict[str, str]) -> tuple[int, int]:
        has_all = int(
            all(str(candidate.get(name) or "").strip() for name in required_cookie_names_all)
            if required_cookie_names_all
            else True
        )
        has_any = int(
            any(str(candidate.get(name) or "").strip() for name in required_cookie_names_any)
            if required_cookie_names_any
            else True
        )
        return (has_all, has_any)

    best_score: tuple[int, int] | None = None
    best_candidate: dict[str, str] | None = None
    for candidate in candidates:
        score = _score(candidate)
        if best_score is None or score > best_score:
            best_score = score
            best_candidate = candidate
    return dict(best_candidate or {})


__all__ = [
    "_default_platform_cookie_file_path",
    "_platform_cookie_file_candidates",
    "_platform_cookie_refresh_target_path",
    "_select_preferred_cookie_candidate",
]
