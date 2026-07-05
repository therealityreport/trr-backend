from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from scripts.socials.instagram import bravo_straggler_recovery as cli


def _args(**overrides: Any) -> SimpleNamespace:
    values = {
        "account": "@BravoTV",
        "limit": 50,
        "approved_shortcode": None,
        "approved_shortcodes_file": None,
        "show_filter": None,
        "retry_tail": False,
        "batch_size": 1,
        "comments_worker_count": None,
        "safe_preset": None,
        "confirm_safe_12": None,
        "max_comments_per_post": 0,
        "comments_load_strategy": "public_relay",
        "date_start": None,
        "date_end": None,
        "skip_launch_auth_probe": False,
        "force_rerun_existing": False,
        "enqueue": False,
        "confirm_enqueue": None,
        "json": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_delegate_argv_safe_12_preserves_batch_size_and_normalizes_account() -> None:
    args = _args(
        approved_shortcode=["ABC123"],
        safe_preset="12",
        confirm_safe_12=cli.CONFIRM_SAFE_12,
        date_start="2026-01-01",
        date_end="2027-01-01",
    )

    delegate = cli._delegate_argv(args)  # noqa: SLF001

    assert delegate[delegate.index("--account") + 1] == "bravotv"
    assert delegate[delegate.index("--batch-size") + 1] == "1"
    assert delegate[delegate.index("--max-comments-per-post") + 1] == "0"
    assert delegate[delegate.index("--comments-load-strategy") + 1] == "public_relay"
    assert delegate[delegate.index("--date-start") + 1] == "2026-01-01"
    assert delegate[delegate.index("--date-end") + 1] == "2027-01-01"
    assert delegate[delegate.index("--comments-worker-count") + 1] == "12"
    assert delegate[delegate.index("--shortcode") + 1] == "ABC123"


def test_safe_12_requires_exact_confirmation() -> None:
    payload = cli.build_payload(_args(approved_shortcode=["ABC123"], safe_preset="12", confirm_safe_12="wrong"))

    assert payload["ok"] is False
    assert payload["failure_reason"] == "confirm_safe_12_required"


def test_enqueue_requires_approved_shortcodes() -> None:
    payload = cli.build_payload(_args(enqueue=True, confirm_enqueue=cli.CONFIRM_BRAVO_ENQUEUE))

    assert payload["ok"] is False
    assert payload["failure_reason"] == "approved_shortcodes_required_for_enqueue"
    assert payload["enqueue"] == {"requested": True, "performed": False}


def test_build_payload_delegates_to_existing_enqueue_cli(monkeypatch) -> None:
    calls: list[list[str]] = []

    def _parse(delegate: list[str]) -> SimpleNamespace:
        calls.append(delegate)
        return SimpleNamespace(delegate=delegate)

    monkeypatch.setattr(cli.enqueue_cli, "_parse_args", _parse)
    monkeypatch.setattr(
        cli.enqueue_cli,
        "_build_payload",
        lambda args: {"ok": True, "mode": "dry_run", "delegate_seen": args.delegate},
    )

    payload = cli.build_payload(_args(approved_shortcode=["ABC123"], safe_preset="8"))

    assert payload["ok"] is True
    assert calls
    assert "--comments-worker-count" in calls[0]
    assert calls[0][calls[0].index("--comments-worker-count") + 1] == "8"
    assert payload["bravo_recovery"]["batch_size"] == 1
    assert "delegate_command" in payload["bravo_recovery"]


def test_main_prints_json_payload(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setenv("BRAVO_RECOVERY_ARGS", "--approved-shortcode ABC123 --safe-preset 8")
    monkeypatch.setattr(
        cli.enqueue_cli,
        "_build_payload",
        lambda _args: {"ok": True, "mode": "dry_run", "enqueue": {"requested": False, "performed": False}},
    )

    assert cli.main([]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["bravo_recovery"]["comments_worker_count"] == 8
