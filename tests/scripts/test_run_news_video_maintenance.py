from __future__ import annotations

from unittest.mock import patch

import scripts.backfill.run_news_video_maintenance as mod


def test_parse_args_supports_phase_and_continue_on_error() -> None:
    args = mod._parse_args(["--phase", "google", "--continue-on-error", "--limit", "3"])
    assert args.phase == "google"
    assert args.continue_on_error is True
    assert args.limit == 3


def test_phase_order_expands_all() -> None:
    assert mod._phase_order("all") == ["bootstrap", "thumbnails", "google"]
    assert mod._phase_order("google") == ["google"]


def test_run_thumbnail_phase_filters_to_requested_show_ids() -> None:
    with patch.object(mod.thumbnail_backfill, "_list_show_ids_with_bravo_snapshot", return_value=["a", "b"]):
        with patch.object(
            mod.thumbnail_backfill,
            "_process_show",
            return_value={
                "show_id": "b",
                "status": "ok",
                "pending_before": 1,
                "pending_after": 0,
                "sync": {"attempted": 1, "synced": 1, "failed": 0, "missing_source": 0},
            },
        ) as process_show:
            summary = mod._run_thumbnail_phase(
                db=object(),
                show_ids=["b"],
                limit=0,
                dry_run=False,
                continue_on_error=True,
            )

    assert process_show.call_count == 1
    assert summary["totals"]["shows_scanned"] == 1
    assert summary["totals"]["thumbnail_synced"] == 1


def test_run_google_phase_dry_run_counts_targets() -> None:
    with patch.object(
        mod,
        "_list_google_news_targets",
        return_value=[
            {"show_id": "show-1", "topic_url": "https://news.google.com/topics/a"},
            {"show_id": "show-2", "topic_url": "https://news.google.com/topics/b"},
        ],
    ):
        summary = mod._run_google_phase(
            db=object(),
            show_ids=[],
            limit=0,
            dry_run=True,
            continue_on_error=True,
        )

    assert summary["totals"]["shows_scanned"] == 2
    assert summary["totals"]["shows_processed"] == 2
    assert summary["totals"]["shows_succeeded"] == 2
    assert summary["totals"]["google_synced"] == 0


def test_run_google_phase_counts_stale_guard_as_skipped() -> None:
    with patch.object(
        mod,
        "_list_google_news_targets",
        return_value=[{"show_id": "show-1", "topic_url": "https://news.google.com/topics/a"}],
    ):
        with patch.object(
            mod,
            "_run_google_news_sync_impl",
            return_value={"synced": False, "stale_guard_skipped": True, "count": 0},
        ):
            summary = mod._run_google_phase(
                db=object(),
                show_ids=[],
                limit=0,
                dry_run=False,
                continue_on_error=True,
            )

    assert summary["totals"]["shows_processed"] == 1
    assert summary["totals"]["shows_succeeded"] == 0
    assert summary["totals"]["shows_skipped"] == 1
    assert summary["totals"]["google_stale_guard_skipped"] == 1
    assert summary["skip_reasons"]["stale_guard_skipped"] == 1


def test_main_returns_nonzero_on_hard_failure() -> None:
    with patch.object(mod, "load_env"):
        with patch.object(mod, "create_supabase_admin_client", return_value=object()):
            with patch.object(mod, "_run_bootstrap_phase", side_effect=RuntimeError("boom")):
                code = mod.main(["--phase", "bootstrap"])

    assert code == 1
