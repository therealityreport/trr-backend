from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import scripts.socials.repair_youtube_short_timestamps as mod


def _base_args(**overrides):
    values = {
        "season_id": [],
        "show_id": [],
        "season_number": [],
        "limit": 100,
        "delay_seconds": 0.0,
        "dry_run": False,
        "apply": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_main_dry_run_reports_repairable_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda _argv: _base_args(dry_run=True, season_id=["season-1"], season_number=["6"]),
    )
    monkeypatch.setattr(
        mod,
        "_fetch_epoch_short_rows",
        lambda **_kwargs: [
            {"id": "row-1", "video_id": "short-1"},
            {"id": "row-2", "video_id": "short-2"},
        ],
    )

    class _FakeScraper:
        def _fetch_precise_publish_timestamp(self, video_id: str, delay: float = 0.0) -> int:  # noqa: SLF001
            del delay
            return 100 if video_id == "short-1" else 0

    monkeypatch.setattr(mod, "YouTubeScraper", _FakeScraper)

    assert mod.main([]) == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is True
    assert payload["season_ids"] == ["season-1"]
    assert payload["season_numbers"] == [6]
    assert payload["totals"] == {
        "examined_rows": 2,
        "epoch_rows_found": 2,
        "rows_repaired": 1,
        "rows_unresolved": 1,
    }
    assert payload["repaired_preview"][0]["video_id"] == "short-1"
    assert payload["unresolved_video_ids_preview"] == ["short-2"]


def test_main_apply_updates_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(apply=True, show_id=["show-1"]))
    monkeypatch.setattr(
        mod,
        "_fetch_epoch_short_rows",
        lambda **_kwargs: [{"id": "row-1", "video_id": "short-1"}],
    )

    class _FakeScraper:
        def _fetch_precise_publish_timestamp(self, video_id: str, delay: float = 0.0) -> int:  # noqa: SLF001
            del video_id, delay
            return int(datetime(2025, 11, 18, 12, 0, tzinfo=UTC).timestamp())

    repair_calls: list[dict[str, object]] = []
    monkeypatch.setattr(mod, "YouTubeScraper", _FakeScraper)
    monkeypatch.setattr(
        mod,
        "_repair_row",
        lambda **kwargs: repair_calls.append(kwargs) or [{"id": "row-1", "published_at": "2025-11-18T12:00:00+00:00"}],
    )

    assert mod.main([]) == 0
    assert repair_calls == [{"row_id": "row-1", "published_at_iso": "2025-11-18T12:00:00+00:00"}]
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is False
    assert payload["totals"]["rows_repaired"] == 1


def test_repair_row_updates_only_epochish_rows(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        mod.pg,
        "execute_returning",
        lambda query, params=None: calls.append({"query": query, "params": list(params or [])}) or [{"id": "row-1"}],
    )

    rows = mod._repair_row(row_id="row-1", published_at_iso="2025-11-18T12:00:00+00:00")

    assert rows == [{"id": "row-1"}]
    assert len(calls) == 1
    assert "update social.youtube_videos" in " ".join(calls[0]["query"].lower().split())
    assert calls[0]["params"] == ["2025-11-18T12:00:00+00:00", "row-1", mod.EPOCHISH_CUTOFF_SQL]
