"""Offline contracts for the standalone Instagram comment-rollup rebuild operator."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_MODULE = "scripts.socials.instagram.rebuild_comment_rollups"
COMMON_MODULE = "trr_backend.socials.read_models.account_profile.common"
PROVIDER_MODULE = "trr_backend.socials.social_season_analytics_impl"


def _operator_module() -> Any:
    return importlib.import_module(SCRIPT_MODULE)


def test_rebuild_comment_rollups_import_establishes_provider_before_export_capture() -> None:
    source = f"""
import importlib
import importlib.abc
import sys

common_name = {COMMON_MODULE!r}
provider_name = {PROVIDER_MODULE!r}
script_name = {SCRIPT_MODULE!r}
order = []

class TraceFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {{common_name, provider_name}}:
            order.append(fullname)
        return None

sys.meta_path.insert(0, TraceFinder())
script = importlib.import_module(script_name)
common = sys.modules[common_name]
provider = sys.modules[provider_name]
assert order.index(provider_name) < order.index(common_name)
assert common._PROVIDER_STATE == "READY"
assert common._PROVIDER_NAMESPACE is provider.__dict__
assert script._account_profile_provider is provider
assert (
    script.rebuild_instagram_post_comment_rollups
    is common.rebuild_instagram_post_comment_rollups
)
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", source],
        cwd=BACKEND_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_rebuild_comment_rollups_main_forwards_arguments_and_prints_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    operator = _operator_module()
    calls: list[dict[str, Any]] = []

    def fake_rebuild(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "ok": True,
            "dry_run": False,
            "target_count": 2,
            "refreshed_count": 2,
            "account_handle": kwargs["account_handle"],
            "post_ids": kwargs["post_ids"],
            "limit": kwargs["limit"],
        }

    monkeypatch.setattr(
        operator,
        "parse_args",
        lambda: SimpleNamespace(
            account_handle="BravoTV",
            post_id=["post-a"],
            post_ids="post-b, post-c",
            limit=4,
            dry_run=False,
            json=False,
        ),
    )
    monkeypatch.setattr(operator, "rebuild_instagram_post_comment_rollups", fake_rebuild)

    assert operator.main() == 0
    assert calls == [
        {
            "account_handle": "BravoTV",
            "post_ids": ["post-a", "post-b", "post-c"],
            "limit": 4,
            "dry_run": False,
        }
    ]
    assert capsys.readouterr().out == (
        "Instagram comment rollup rebuild: scope=3 explicit post(s) target_count=2 refreshed_count=2 dry_run=no\n"
    )


def test_rebuild_comment_rollups_main_prints_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    operator = _operator_module()
    calls: list[dict[str, Any]] = []

    def fake_rebuild(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "ok": True,
            "dry_run": True,
            "target_count": 2,
            "refreshed_count": 0,
            "account_handle": None,
            "post_ids": [],
            "limit": None,
        }

    monkeypatch.setattr(
        operator,
        "parse_args",
        lambda: SimpleNamespace(
            account_handle=None,
            post_id=[],
            post_ids=None,
            limit=None,
            dry_run=True,
            json=True,
        ),
    )
    monkeypatch.setattr(operator, "rebuild_instagram_post_comment_rollups", fake_rebuild)

    assert operator.main() == 0
    assert calls == [
        {
            "account_handle": None,
            "post_ids": [],
            "limit": None,
            "dry_run": True,
        }
    ]
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "dry_run": True,
        "target_count": 2,
        "refreshed_count": 0,
        "account_handle": None,
        "post_ids": [],
        "limit": None,
    }
