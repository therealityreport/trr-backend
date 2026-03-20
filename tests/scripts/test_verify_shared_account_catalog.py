"""Unit tests for verify_shared_account_catalog script argument parsing."""

from __future__ import annotations

from scripts.socials.verify_shared_account_catalog import parse_args


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
