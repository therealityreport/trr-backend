from __future__ import annotations

import argparse
import json
from types import SimpleNamespace
from typing import Any, cast

from scripts.socials.instagram import media_mirror_recovery as cli


def _args(**overrides: Any) -> argparse.Namespace:
    values = {
        "run_id": "11111111-1111-1111-1111-111111111111",
        "stage": "media_mirror",
        "account": "bravotv",
        "stale_after_seconds": 900,
        "recover_limit": 5,
        "dispatch_limit": 8,
        "skip_recover": False,
        "skip_dispatch": False,
        "apply": False,
        "confirm_apply": None,
        "json": True,
    }
    values.update(overrides)
    return cast(argparse.Namespace, SimpleNamespace(**values))


def test_fetch_status_snapshot_reports_counts_and_stale_jobs(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def _fetch_all(query: str, params: list[Any]) -> list[dict[str, Any]]:
        calls.append({"query": query, "params": params})
        if "group by status" in query.lower():
            return [{"status": "queued", "jobs": 2, "worker_ids": []}]
        return [{"id": "job-1", "status": "running"}]

    monkeypatch.setattr(cli.pg, "fetch_all", _fetch_all)

    snapshot = cli.fetch_status_snapshot(
        run_id="run-1",
        stage="media_mirror",
        account="bravotv",
        stale_after_seconds=900,
    )

    assert snapshot["status_counts"] == [{"status": "queued", "jobs": 2, "worker_ids": []}]
    assert snapshot["stale_running_jobs"] == [{"id": "job-1", "status": "running"}]
    assert snapshot["stale_running_count"] == 1
    assert all("bravotv" in call["params"] for call in calls)


def test_dry_run_does_not_recover_or_dispatch(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "fetch_status_snapshot",
        lambda **kwargs: {"run_id": kwargs["run_id"], "stage": kwargs["stage"], "stale_running_count": 1},
    )
    monkeypatch.setattr(
        cli.social_repo,
        "recover_stale_running_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dry run must not recover")),
    )
    monkeypatch.setattr(
        cli.social_repo,
        "dispatch_due_social_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dry run must not dispatch")),
    )

    payload = cli.recover_and_dispatch(_args(), "bravotv")

    assert payload["dry_run"] is True
    assert payload["recovered_job_ids"] == []
    assert payload["dispatch"] == {"dispatched_job_ids": [], "dispatch_attempts": 0, "skipped": True}


def test_apply_requires_exact_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(cli, "fetch_status_snapshot", lambda **_kwargs: {"stale_running_count": 1})

    payload = cli.recover_and_dispatch(_args(apply=True, confirm_apply="wrong"), "bravotv")

    assert payload["ok"] is False
    assert payload["failure_reason"] == "confirm_apply_required"
    assert payload["confirm_required"] == cli.CONFIRM_APPLY


def test_apply_recovers_and_dispatches(monkeypatch) -> None:
    snapshots: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli,
        "fetch_status_snapshot",
        lambda **kwargs: snapshots.append(kwargs) or {"run_id": kwargs["run_id"], "stage": kwargs["stage"]},
    )
    recover_calls: list[dict[str, Any]] = []
    dispatch_calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli.social_repo,
        "recover_stale_running_jobs",
        lambda **kwargs: recover_calls.append(kwargs) or [{"id": "job-1"}],
    )
    monkeypatch.setattr(
        cli.social_repo,
        "dispatch_due_social_jobs",
        lambda **kwargs: dispatch_calls.append(kwargs) or {"dispatched_job_ids": ["job-2"], "dispatch_attempts": 1},
    )

    payload = cli.recover_and_dispatch(_args(apply=True, confirm_apply=cli.CONFIRM_APPLY), "bravotv")

    assert payload["dry_run"] is False
    assert payload["recovered_job_ids"] == ["job-1"]
    assert payload["dispatch"]["dispatched_job_ids"] == ["job-2"]
    assert recover_calls == [
        {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "stage": "media_mirror",
            "platform": "instagram",
            "stale_after_seconds": 900,
            "limit": 5,
        }
    ]
    assert dispatch_calls == [{"run_id": "11111111-1111-1111-1111-111111111111", "limit": 8}]
    assert len(snapshots) == 2


def test_main_emits_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.pg, "close_pool", lambda: None)
    monkeypatch.setattr(
        cli,
        "recover_and_dispatch",
        lambda _args, account: {"ok": True, "account": account, "dry_run": True},
    )

    assert cli.main(["--run-id", "run-1", "--account", "@BravoTV", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"ok": True, "account": "bravotv", "dry_run": True}
