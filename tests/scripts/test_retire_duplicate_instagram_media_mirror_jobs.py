from __future__ import annotations

import json
from types import SimpleNamespace

import scripts.socials.retire_duplicate_instagram_media_mirror_jobs as mod


def _base_args(**overrides):
    values = {"season_id": [], "show_id": [], "dry_run": False, "apply": False}
    values.update(overrides)
    return SimpleNamespace(**values)


def test_main_dry_run_reports_duplicate_rows_without_retiring(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(dry_run=True, season_id=["season-1"]))
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [{"id": "job-1", "post_id": "post-1"}, {"id": "job-2", "post_id": "post-1"}],
    )
    monkeypatch.setattr(mod, "_retire_matches", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("no retire")))

    assert mod.main([]) == 0

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is True
    assert payload["season_ids"] == ["season-1"]
    assert payload["totals"] == {"matched_rows": 2, "retired_rows": 0}
    assert payload["preview_job_ids"] == ["job-1", "job-2"]


def test_retire_matches_marks_duplicate_jobs_cancelled(monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [{"id": "job-1", "post_id": "post-1"}, {"id": "job-2", "post_id": "post-1"}],
    )
    calls: list[dict[str, object]] = []
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
    assert "and status = any(%s)" in query.lower()
    assert params[0] == mod.DUPLICATE_ERROR_MESSAGE
    assert params[1] == mod.DUPLICATE_ERROR_CODE
    assert params[2] == mod.DUPLICATE_ERROR_CLASS
    assert json.loads(params[3]) == {"duplicate_active_media_mirror_job": True}
    assert params[4] == ["job-1", "job-2"]
    assert params[5] == list(mod.ACTIVE_DUPLICATE_STATUSES)
