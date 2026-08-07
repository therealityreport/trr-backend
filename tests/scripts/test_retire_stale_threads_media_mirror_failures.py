from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import scripts.socials.retire_stale_threads_media_mirror_failures as mod


def _base_args(**overrides):
    values = {
        "season_id": [],
        "show_id": [],
        "dry_run": False,
        "apply": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_main_dry_run_reports_matching_rows_without_retiring(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv: _base_args(dry_run=True, season_id=["season-1"], show_id=["show-1"]),
    )
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [
            {"id": "job-1", "season_id": "season-1", "show_id": "show-1"},
            {"id": "job-2", "season_id": "season-1", "show_id": "show-1"},
        ],
    )
    retire_called = False

    def _unexpected_retire(**_kwargs):
        nonlocal retire_called
        retire_called = True
        raise AssertionError("dry-run must not retire rows")

    monkeypatch.setattr(mod, "_retire_matches", _unexpected_retire)

    assert mod.main([]) == 0
    assert retire_called is False

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is True
    assert payload["season_ids"] == ["season-1"]
    assert payload["show_ids"] == ["show-1"]
    assert payload["totals"] == {"matched_rows": 2, "retired_rows": 0}
    assert payload["preview_job_ids"] == ["job-1", "job-2"]


def test_main_apply_retires_matching_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(apply=True, season_id=["season-1"]))
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [
            {"id": "job-1", "season_id": "season-1", "show_id": "show-1"},
            {"id": "job-2", "season_id": "season-1", "show_id": "show-1"},
        ],
    )
    retire_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        mod,
        "_retire_matches",
        lambda **kwargs: retire_calls.append(kwargs) or [{"id": "job-1"}, {"id": "job-2"}],
    )

    assert mod.main([]) == 0
    assert retire_calls == [{"season_ids": ["season-1"], "show_ids": []}]

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is False
    assert payload["totals"] == {"matched_rows": 2, "retired_rows": 2}
    assert payload["replacement_error_message"] == mod.OBSOLETE_ERROR_MESSAGE


def test_retire_matches_updates_only_stale_threads_failures(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        mod.pg,
        "execute_returning",
        lambda query, params=None: calls.append({"query": query, "params": list(params or [])}) or [{"id": "job-1"}],
    )

    rows = mod._retire_matches(season_ids=["season-1"], show_ids=["show-1"])

    assert rows == [{"id": "job-1"}]
    assert len(calls) == 1
    query = calls[0]["query"]
    params = calls[0]["params"]
    assert "status = 'cancelled'" in query
    assert "platform = 'threads'" in query
    assert "status = 'failed'" in query
    assert "coalesce(config->>'stage', metadata->>'stage', job_type) = 'media_mirror'" in query
    assert params[0] == mod.OBSOLETE_ERROR_MESSAGE
    assert params[1] == mod.OBSOLETE_ERROR_CODE
    assert params[2] == mod.OBSOLETE_ERROR_CLASS
    assert json.loads(params[3]) == {
        "obsolete_historical_failure": True,
        "obsolete_failure_reason": mod.STALE_THREADS_MEDIA_MIRROR_ERROR,
        "obsolete_failure_resolution": "threads_media_mirror_supported_now",
    }
    assert params[4] == mod.STALE_THREADS_MEDIA_MIRROR_ERROR
    assert params[5] == ["season-1"]
    assert params[6] == ["show-1"]
