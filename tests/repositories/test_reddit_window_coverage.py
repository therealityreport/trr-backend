import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.repositories import reddit_window_coverage as coverage

NOW = datetime(2026, 10, 22, tzinfo=UTC)


def iso(day):
    return datetime(2026, 10, day, tzinfo=UTC).isoformat()


def proof(start=1, end=8, run_id="manual"):
    return {
        "run_id": run_id,
        "created_at": NOW,
        "configuration": {"requested_phases": ["discovery"], "period_start": iso(start), "period_end": iso(end)},
        "coverage": {
            "scope": "window",
            "state": "complete",
            "chronological_boundary_proven": True,
            "requested_start": iso(start),
            "requested_end": iso(end),
        },
    }


def window(start=1, end=8, key="episode-1"):
    return {"key": key, "start": iso(start), "end": iso(end)}


def test_manual_capture_classifies_actual_intersections_not_run_keys():
    windows = [
        window(1, 8),
        window(8, 15, "episode-2"),
        window(15, 22, "episode-3"),
        window(22, 29, "episode-4"),
        window(1, 2, "period-preseason"),
    ]
    result = coverage.classify_windows(windows, [proof(2, 20)], now=NOW)
    assert [row["coverage_state"] for row in result.values()] == [
        "partial",
        "covered",
        "partial",
        "upcoming",
        "uncovered",
    ]
    assert result["episode-3"]["covered_through"] == "2026-10-20T00:00:00Z"
    assert result["episode-2"]["evidence_run_ids"] == ["manual"]


def test_interval_union_accepts_adjacency_but_never_bridges_gap():
    rows = [proof(1, 4, "a"), proof(4, 8, "b")]
    result = coverage.classify_windows([window()], rows, now=NOW)["episode-1"]
    assert result["coverage_state"] == "covered"
    assert result["evidence_run_ids"] == ["a", "b"]
    rows[1] = proof(5, 8, "b")
    result = coverage.classify_windows([window()], rows, now=NOW)["episode-1"]
    assert result["coverage_state"] == "partial"
    assert result["covered_through"] == "2026-10-04T00:00:00Z"
    assert len(result["covered_intervals"]) == 2


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p["configuration"].update(selected_post_ids=["abc"]),
        lambda p: p["configuration"].update(requested_phases=["comments", "media"]),
        lambda p: p["coverage"].update(scope="selected_posts"),
        lambda p: p["coverage"].update(state="partial"),
        lambda p: p["coverage"].update(chronological_boundary_proven=False),
        lambda p: p["coverage"].update(requested_start=iso(2)),
    ],
)
def test_non_interval_or_incomplete_evidence_cannot_clear_window(mutation):
    row = deepcopy(proof())
    mutation(row)
    assert coverage.classify_windows([window()], [row], now=NOW)["episode-1"]["coverage_state"] == "uncovered"


def test_future_request_end_is_capped_at_recorded_capture_start():
    row = proof(1, 29)
    row["created_at"] = datetime(2026, 10, 10, tzinfo=UTC)
    result = coverage.classify_windows([window(8, 15)], [row], now=NOW)["episode-1"]
    assert result["coverage_state"] == "partial"
    assert result["covered_through"] == "2026-10-10T00:00:00Z"


def test_rhoslc_cutoff_yields_five_covered_two_partial_seven_upcoming():
    start = datetime(2026, 9, 16, 23, tzinfo=UTC)
    windows = [{"key": "period-preseason", "start": (start - timedelta(days=7)).isoformat(), "end": start.isoformat()}]
    windows += [
        {
            "key": f"episode-{n + 1}",
            "start": (start + timedelta(days=n * 7)).isoformat(),
            "end": (start + timedelta(days=(n + 1) * 7)).isoformat(),
        }
        for n in range(12)
    ]
    windows += [
        {
            "key": "period-postseason",
            "start": (start + timedelta(days=84)).isoformat(),
            "end": (start + timedelta(days=91)).isoformat(),
        }
    ]
    row = proof()
    row["configuration"].update(period_start="2026-09-16T04:00:00Z", period_end="2026-10-21T23:06:47.895435Z")
    row["coverage"].update(
        requested_start=row["configuration"]["period_start"], requested_end=row["configuration"]["period_end"]
    )
    result = coverage.classify_windows(windows, [row], now=NOW)
    states = [row["coverage_state"] for row in result.values()]
    assert states.count("covered") == 5
    assert states.count("partial") == 2
    assert states.count("upcoming") == 7


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        "[]",
        "[{}]",
        "not json",
        json.dumps([window(), window()]),
        json.dumps([window(key="debug")]),
        json.dumps([window(8, 1)]),
        json.dumps([window()] * 65),
        " " * 12001,
        json.dumps([{"key": "episode-1", "start": "2026-10-01", "end": iso(8)}]),
    ],
)
def test_windows_input_is_bounded_and_validated(raw):
    with pytest.raises(ValueError):
        coverage.parse_windows(raw)


def test_equivalent_timezone_bounds_preserve_user_interval():
    assert coverage.parse_windows(
        '[{"key":"episode-1","start":"2026-10-01T19:01:01-04:00","end":"2026-10-08T19:01:10-04:00"}]'
    ) == [{"key": "episode-1", "start": "2026-10-01T23:01:01Z", "end": "2026-10-08T23:01:10Z"}]


def test_proof_query_requires_committed_discovery_targets(monkeypatch):
    calls = []
    monkeypatch.setattr(coverage.pg, "fetch_all", lambda sql, params: calls.append((sql, params)) or [])
    assert coverage.fetch_proofs("community", "season") == []
    sql, params = calls[0]
    assert params == ["community", "season"]
    assert "t.kind='discovery' and t.required and t.state='committed'" in sql
    assert "t.kind='discovery' and t.required and t.state<>'committed'" in sql
    assert "m.requested_phases ? 'discovery'" in sql
