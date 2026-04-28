from __future__ import annotations

import ast
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[2]
_METADATA_FILES = [
    _ROOT / "trr_backend/socials/instagram/posts_scrapling/job_runner.py",
    _ROOT / "trr_backend/socials/instagram/comments_scrapling/job_runner.py",
    _ROOT / "trr_backend/socials/instagram/posts_scrapling/fetcher.py",
    _ROOT / "trr_backend/socials/instagram/comments_scrapling/fetcher.py",
]
_FORBIDDEN_METADATA_NAMES = {
    "cookies",
    "raw_cookies",
    "_raw_cookies",
    "sessionid",
    "csrftoken",
    "ds_user_id",
    "warmup_cookie_delta",
}


def _literal_dict_keys(tree: ast.AST) -> set[str]:
    keys: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key in node.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.add(key.value)
    return keys


def test_instagram_scrapling_metadata_uses_cookie_names_and_counts_not_values() -> None:
    offenders: list[str] = []
    for path in _METADATA_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for key in _literal_dict_keys(tree):
            normalized = key.strip().lower()
            if normalized in _FORBIDDEN_METADATA_NAMES:
                offenders.append(f"{path.relative_to(_ROOT)}:{key}")

    assert offenders == []
