from __future__ import annotations

from typing import Any

from scripts.socials.instagram import enqueue_comments_audit_cursor_retries as cli


def test_build_payload_dry_run_selects_eligible_audit_rows(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli,
        "enqueue_instagram_comments_audit_cursor_retries",
        lambda **kwargs: calls.append(kwargs)
        or {
            "ok": True,
            "mode": "dry_run",
            "selected_target_source_ids": ["SHORT1"],
            "selected_rows": [{"shortcode": "SHORT1", "has_top_level_cursor": True}],
            "enqueue": {"requested": False, "performed": False},
        },
    )

    args = cli._parse_args(["--account", "bravotv", "--json"])
    payload = cli._build_payload(args)

    assert payload["mode"] == "dry_run"
    assert payload["selected_target_source_ids"] == ["SHORT1"]
    assert payload["selected_rows"][0]["has_top_level_cursor"] is True
    assert payload["enqueue"] == {"requested": False, "performed": False}
    assert calls[0]["dry_run"] is True
    assert calls[0]["batch_size"] == 1
    assert calls[0]["attach_to_active_run"] is True


def test_build_payload_enqueue_threads_batch_size(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli,
        "enqueue_instagram_comments_audit_cursor_retries",
        lambda **kwargs: calls.append(kwargs)
        or {
            "ok": True,
            "mode": "enqueue",
            "enqueue": {"requested": True, "performed": True, "result": {"run_id": "run-1"}},
        },
    )

    args = cli._parse_args(
        [
            "--account",
            "bravotv",
            "--enqueue",
            "--confirm-enqueue",
            cli.CONFIRM_ENQUEUE,
            "--batch-size",
            "1",
            "--json",
        ]
    )
    payload = cli._build_payload(args)

    assert payload["ok"] is True
    assert payload["enqueue"]["performed"] is True
    assert calls[0]["account_handle"] == "bravotv"
    assert calls[0]["batch_size"] == 1
    assert calls[0]["max_comments_per_post"] == 0
    assert calls[0]["dry_run"] is False
