from __future__ import annotations

import json
from types import SimpleNamespace

import scripts.socials.retire_duplicate_instagram_comment_media_mirror_jobs as mod


def _base_args(**overrides):
    values = {
        "season_id": [],
        "show_id": [],
        "account": [],
        "dry_run": False,
        "apply": False,
        "confirm_destructive": False,
        "confirm_account": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_main_apply_retires_duplicate_comment_media_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv: _base_args(
            apply=True,
            show_id=["show-1"],
            account=["@thetraitorsus"],
            confirm_destructive=True,
            confirm_account="thetraitorsus",
        ),
    )
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [{"id": "job-1", "identity_key": "post-1:comment-1"}],
    )
    retire_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        mod,
        "_retire_matches",
        lambda **kwargs: retire_calls.append(kwargs) or [{"id": "job-1"}],
    )

    assert mod.main([]) == 0
    assert retire_calls == [{"season_ids": [], "show_ids": ["show-1"], "accounts": ["thetraitorsus"]}]

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is False
    assert payload["accounts"] == ["thetraitorsus"]
    assert payload["show_ids"] == ["show-1"]
    assert payload["totals"] == {"matched_rows": 1, "retired_rows": 1}


def test_main_apply_refuses_without_destructive_confirmation(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv: _base_args(apply=True, account=["thetraitorsus"], confirm_account="thetraitorsus"),
    )
    monkeypatch.setattr(mod, "_fetch_matches", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("queried")))

    assert mod.main([]) == 2

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["status"] == "refused"
    assert payload["dry_run"] is True
    assert payload["accounts"] == ["thetraitorsus"]
    assert "missing --confirm-destructive" in payload["refusal_reasons"]


def test_retire_matches_marks_duplicate_comment_media_jobs_cancelled(monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "_fetch_matches",
        lambda **_kwargs: [{"id": "job-1", "identity_key": "post-1:comment-1"}],
    )
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        mod.pg,
        "execute_returning",
        lambda query, params=None: calls.append({"query": query, "params": list(params or [])}) or [{"id": "job-1"}],
    )

    rows = mod._retire_matches(season_ids=["season-1"], show_ids=[], accounts=["thetraitorsus"])

    assert rows == [{"id": "job-1"}]
    assert len(calls) == 1
    assert "status = 'cancelled'" in calls[0]["query"]
    assert calls[0]["params"][0] == mod.DUPLICATE_ERROR_MESSAGE
    assert calls[0]["params"][1] == mod.DUPLICATE_ERROR_CODE
    assert calls[0]["params"][2] == mod.DUPLICATE_ERROR_CLASS
    assert json.loads(calls[0]["params"][3]) == {"duplicate_active_comment_media_mirror_job": True}
    assert calls[0]["params"][4] == ["job-1"]
    assert "concat(config->>'post_id', ':', config->>'comment_id')" in mod.IDENTITY_SQL
    assert "owner_username" in mod.ACCOUNT_SQL
