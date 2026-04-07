from __future__ import annotations

import json

import scripts.backfill.backfill_screenalytics_stage6 as mod


def test_main_dry_run_reports_ready_and_failed_candidates(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env_and_db", lambda: object())
    monkeypatch.setattr(
        mod.screenalytics_runs,
        "list_all_result_sync_candidates",
        lambda limit=None: [
            {
                "run": {"id": "run-ready", "result_contract_version": "trr-screenalytics/v1"},
                "artifacts": [{"artifact_key": "leaderboard.json"}],
                "person_metrics": [{"person_id": "person-1", "screen_time_seconds": 10}],
                "leaderboard": [{"person_id": "person-1", "screen_time_seconds": 10}],
            },
            {
                "run": {"id": "run-bad", "result_contract_version": "trr-screenalytics/v1"},
                "artifacts": [],
                "person_metrics": [],
                "leaderboard": [],
            },
        ],
    )

    exit_code = mod.main(["--all-pending"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["ingested_run_ids"] == ["run-ready"]
    assert payload["failed_runs"] == [
        {
            "run_id": "run-bad",
            "error": "incomplete result bundle: missing artifacts",
        }
    ]


def test_main_apply_marks_ingested_and_failed_runs(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env_and_db", lambda: object())
    monkeypatch.setattr(
        mod.screenalytics_runs,
        "list_result_bundles",
        lambda run_ids: [
            {
                "run": {"id": "run-1", "result_contract_version": "trr-screenalytics/v1"},
                "artifacts": [{"artifact_key": "leaderboard.json"}],
                "person_metrics": [{"person_id": "person-1", "screen_time_seconds": 12}],
                "leaderboard": [{"person_id": "person-1", "screen_time_seconds": 12}],
            },
            {
                "run": {"id": "run-2", "result_contract_version": "trr-screenalytics/v1"},
                "artifacts": [],
                "person_metrics": [],
                "leaderboard": [],
            },
        ],
    )

    status_updates: list[tuple[str, str, str | None]] = []
    monkeypatch.setattr(
        mod.screenalytics_runs,
        "mark_result_ingest_status",
        lambda run_id, *, status, error=None: status_updates.append((run_id, status, error)),
    )

    exit_code = mod.main(["--run-id", "run-1", "--run-id", "run-2", "--apply", "--verbose"])

    assert exit_code == 1
    assert status_updates == [
        ("run-1", "ingested", None),
        ("run-2", "failed", "incomplete result bundle: missing artifacts"),
    ]
    captured = capsys.readouterr().out
    assert "INGESTED run_id=run-1" in captured
    assert "FAILED run_id=run-2 error=incomplete result bundle: missing artifacts" in captured
    payload = json.loads(captured[captured.index("{") :])
    assert payload["dry_run"] is False
    assert payload["ingested_run_ids"] == ["run-1"]
    assert payload["failed_runs"] == [
        {
            "run_id": "run-2",
            "error": "incomplete result bundle: missing artifacts",
        }
    ]
