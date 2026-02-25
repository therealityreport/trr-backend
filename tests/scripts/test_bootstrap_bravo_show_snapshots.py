from __future__ import annotations

from unittest.mock import patch

from fastapi import HTTPException

import scripts.backfill.bootstrap_bravo_show_snapshots as mod


def test_parse_args_supports_sync_toggle_and_json_summary() -> None:
    args = mod._parse_args(
        [
            "--show-id",
            "show-1",
            "--limit",
            "5",
            "--dry-run",
            "--no-sync-thumbnails",
            "--continue-on-error",
            "--json-summary",
            "-",
        ]
    )
    assert args.show_id == ["show-1"]
    assert args.limit == 5
    assert args.dry_run is True
    assert args.sync_thumbnails is False
    assert args.continue_on_error is True
    assert args.json_summary == "-"


def test_list_bootstrap_targets_parameter_order_keeps_limit_last() -> None:
    with patch.object(mod.pg, "fetch_all", return_value=[]) as fetch_all:
        mod.list_bootstrap_targets(show_ids=["show-1"], limit=2)

    params = fetch_all.call_args.args[1]
    assert params == [["show-1"], "bravo", "default", 2]


def test_bootstrap_single_show_skips_when_sync_readiness_unmet() -> None:
    with patch.object(mod, "_show_exists", return_value=True):
        with patch.object(
            mod,
            "_assert_show_sync_ready_for_bravo",
            side_effect=HTTPException(status_code=409, detail="missing: seasons"),
        ):
            result = mod._bootstrap_single_show(
                db=object(),
                show_id="show-1",
                show_url="https://www.bravotv.com/the-valley",
                show_name="The Valley",
                link_kind="official",
                link_status="approved",
                dry_run=False,
                sync_thumbnails=True,
                actor="test",
            )

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "sync_readiness_unmet"
    assert "missing" in str(result["error"])


def test_run_bootstrap_accumulates_status_counters() -> None:
    with patch.object(
        mod,
        "list_bootstrap_targets",
        return_value=[
            {"show_id": "show-1", "show_name": "A", "show_url": "https://www.bravotv.com/a"},
            {"show_id": "show-2", "show_name": "B", "show_url": "https://www.bravotv.com/b"},
        ],
    ):
        with patch.object(
            mod,
            "_bootstrap_single_show",
            side_effect=[
                {
                    "show_id": "show-1",
                    "status": "created",
                    "counts": {
                        "video_thumbnail_attempted": 2,
                        "video_thumbnail_synced": 1,
                        "video_thumbnail_failed": 0,
                        "video_thumbnail_missing_source": 1,
                    },
                },
                {
                    "show_id": "show-2",
                    "status": "skipped",
                    "skip_reason": "show_not_found",
                    "counts": {},
                },
            ],
        ):
            summary = mod.run_bootstrap(db=object(), dry_run=False, sync_thumbnails=True)

    assert summary["totals"]["shows_scanned"] == 2
    assert summary["totals"]["shows_processed"] == 2
    assert summary["totals"]["shows_succeeded"] == 1
    assert summary["totals"]["shows_skipped"] == 1
    assert summary["totals"]["bootstrap_created"] == 1
    assert summary["totals"]["video_thumbnail_attempted"] == 2
    assert summary["skip_reasons"]["show_not_found"] == 1
