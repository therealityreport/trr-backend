from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from scripts.db import cleanup_stale_social_advisory_locks as subject


@dataclass
class FakePg:
    rows: list[dict[str, object]]
    writes: list[tuple[str, list[object]]] = field(default_factory=list)

    def fetch_all(self, query: str, params: list[object]) -> list[dict[str, object]]:
        assert "from pg_stat_activity" in query
        assert params == [30]
        return self.rows

    def execute(self, query: str, params: list[object]) -> None:
        self.writes.append((" ".join(query.lower().split()), params))


def _rows() -> list[dict[str, object]]:
    return [
        {
            "pid": 101,
            "state": "idle",
            "query": "select pg_try_advisory_lock(9001) as locked",
            "idle_age_seconds": 3600,
        },
        {
            "pid": 202,
            "state": "idle in transaction",
            "query": "select pg_try_advisory_lock(1234) as locked",
            "idle_age_seconds": 3600,
        },
        {
            "pid": 303,
            "state": "idle",
            "query": "select 1",
            "idle_age_seconds": 3600,
        },
    ]


def test_cleanup_dry_run_filters_to_allowed_lock_keys_without_terminating(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pg = FakePg(_rows())
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.cleanup_stale_advisory_sessions(30, [9001], execute=False)

    assert result["dry_run"] is True
    assert result["terminated_pids"] == []
    assert [session["pid"] for session in result["stale_advisory_sessions"]] == [101]
    assert fake_pg.writes == []


def test_cleanup_execute_terminates_allowed_stale_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pg = FakePg(_rows())
    monkeypatch.setattr(subject, "pg", fake_pg)

    result = subject.cleanup_stale_advisory_sessions(30, [9001], execute=True)

    assert result["dry_run"] is False
    assert result["terminated_pids"] == [101]
    assert fake_pg.writes == [("select pg_terminate_backend(%s)", [101])]


def test_find_stale_advisory_sessions_refuses_empty_allowlist() -> None:
    with pytest.raises(subject.AdvisoryCleanupRefused):
        subject.find_stale_advisory_sessions(30, [])
