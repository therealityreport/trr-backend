"""Unit tests for verify_shared_account_catalog script argument parsing."""

from __future__ import annotations

import sys
from types import SimpleNamespace

from scripts.socials.verify_shared_account_catalog import parse_args
from scripts.socials import verify_shared_account_catalog as cli


def test_parse_args_defaults() -> None:
    args = parse_args(["--platform", "instagram", "--account", "bravotv"])

    assert args.platform == "instagram"
    assert args.account == "bravotv"
    assert args.run_id is None
    assert args.expected_total_posts is None
    assert args.pretty is False


def test_parse_args_with_optional_flags() -> None:
    args = parse_args(
        [
            "--platform",
            "instagram",
            "--account",
            "bravotv",
            "--run-id",
            "run-123",
            "--expected-total-posts",
            "16454",
            "--pretty",
        ]
    )

    assert args.platform == "instagram"
    assert args.account == "bravotv"
    assert args.run_id == "run-123"
    assert args.expected_total_posts == 16454
    assert args.pretty is True


def test_main_applies_workspace_runtime_env_before_loading_backend_repo(
    monkeypatch,
    capsys,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        cli,
        "apply_workspace_runtime_env",
        lambda *, repo_root, environ=None: calls.append("env") or {},
        raising=False,
    )
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda argv=None: SimpleNamespace(
            platform="instagram",
            account="bravotv",
            run_id=None,
            expected_total_posts=None,
            pretty=False,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "trr_backend.repositories.social_season_analytics",
        SimpleNamespace(
            get_social_account_catalog_verification=lambda *args, **kwargs: {
                "verified": True,
                "catalog_posts": 1,
                "caption_rows": 1,
                "hashtag_counts_match": True,
            }
        ),
    )

    assert cli.main() == 0
    assert calls == ["env"]
    assert '"verified": true' in capsys.readouterr().out
