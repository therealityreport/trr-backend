from __future__ import annotations

import pytest

from trr_backend.repositories import screenalytics_runs


def test_screenalytics_update_run_rejects_non_identifier_column() -> None:
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        screenalytics_runs.update_run("run-1", {"status = 'x'; --": "y"})


def test_screenalytics_update_run_accepts_safe_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _execute_returning(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured.update(sql=sql, params=params)
        return [{"id": "run-1", "status": "complete"}]

    monkeypatch.setattr(screenalytics_runs.pg, "execute_returning", _execute_returning)

    result = screenalytics_runs.update_run("run-1", {"status": "complete", "result_ingest_error": None})

    assert result == {"id": "run-1", "status": "complete"}
    assert captured["sql"] == (
        "UPDATE ml.screentime_runs SET status = %s, result_ingest_error = %s WHERE id = %s RETURNING *"
    )
    assert captured["params"] == ["complete", None, "run-1"]
