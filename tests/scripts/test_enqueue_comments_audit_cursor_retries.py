from __future__ import annotations

from typing import Any

from scripts.socials.instagram import enqueue_comments_audit_cursor_retries as cli


def test_build_payload_dry_run_selects_eligible_audit_rows(monkeypatch) -> None:
    rows = [
        {
            "post_id": "post-1",
            "shortcode": "SHORT1",
            "source_account": "bravotv",
            "cursor_stop_reason": "pagination_deadline_exceeded",
            "cursor_payload": {
                "top_level_checkpoint": {
                    "target_shortcode": "SHORT1",
                    "stop_reason": "pagination_deadline_exceeded",
                    "next_top_level_cursor": "cursor-2",
                    "next_top_level_cursor_param": "max_id",
                }
            },
            "created_at": "2026-06-12T00:00:00+00:00",
        },
        {
            "post_id": "post-2",
            "shortcode": "SHORT2",
            "source_account": "bravotv",
            "cursor_stop_reason": "pagination_repeated_cursor",
            "cursor_payload": {
                "top_level_checkpoint": {
                    "target_shortcode": "SHORT2",
                    "stop_reason": "pagination_repeated_cursor",
                    "last_top_level_cursor": "stuck",
                    "last_top_level_cursor_param": "max_id",
                }
            },
            "created_at": "2026-06-12T00:01:00+00:00",
        },
    ]
    monkeypatch.setattr(cli.pg, "fetch_all", lambda *_args, **_kwargs: rows)

    args = cli._parse_args(["--account", "bravotv", "--json"])
    payload = cli._build_payload(args)

    assert payload["mode"] == "dry_run"
    assert payload["selected_target_source_ids"] == ["SHORT1"]
    assert payload["selected_rows"][0]["has_top_level_cursor"] is True
    assert payload["enqueue"] == {"requested": False, "performed": False}


def test_build_payload_enqueue_threads_batch_size(monkeypatch) -> None:
    rows = [
        {
            "post_id": "post-1",
            "shortcode": "SHORT1",
            "source_account": "bravotv",
            "cursor_stop_reason": "pagination_page_cap_reached",
            "cursor_payload": {
                "reply_checkpoint_summary": {
                    "items": [
                        {
                            "target_shortcode": "SHORT1",
                            "parent_comment_id": "parent-1",
                            "stop_reason": "pagination_page_cap_reached",
                            "next_reply_cursor": "reply-cursor",
                            "next_reply_cursor_param": "max_id",
                        }
                    ]
                }
            },
            "created_at": "2026-06-12T00:00:00+00:00",
        }
    ]
    start_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(cli.pg, "fetch_all", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(
        cli,
        "start_social_account_comments_scrape",
        lambda *args, **kwargs: start_calls.append({"args": args, "kwargs": kwargs})
        or {"run_id": "run-1", "status": "queued"},
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
    assert start_calls[0]["args"] == ("instagram", "bravotv")
    assert start_calls[0]["kwargs"]["target_source_ids"] == ["SHORT1"]
    assert start_calls[0]["kwargs"]["comments_target_batch_size"] == 1
    assert start_calls[0]["kwargs"]["max_comments_per_post"] == 0
