from __future__ import annotations

from datetime import UTC, datetime

from scripts._sync_common import should_sync_show


def _base_show(marker: str = "S1E1", total_seasons: int | None = 2) -> dict[str, object]:
    return {
        "most_recent_episode": marker,
        "show_total_seasons": total_seasons,
        "external_ids": {},
    }


def _base_state(marker: str = "S1E1", status: str = "success") -> dict[str, object]:
    return {
        "status": status,
        "last_seen_most_recent_episode": marker,
        "last_success_at": "2024-01-01T00:00:00+00:00",
    }


def test_should_sync_when_no_state() -> None:
    show = _base_show()
    should_sync, reason = should_sync_show(
        show=show,
        state=None,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=False,
        derived_total_seasons=None,
    )
    assert should_sync is True
    assert reason == "no-sync-state"


def test_should_skip_when_unchanged() -> None:
    show = _base_show(marker="S1E1", total_seasons=2)
    state = _base_state(marker="S1E1", status="success")
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=True,
        derived_total_seasons=2,
    )
    assert should_sync is False
    assert reason == "up-to-date"


def test_should_sync_when_marker_changes() -> None:
    show = _base_show(marker="S1E2", total_seasons=2)
    state = _base_state(marker="S1E1", status="success")
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=False,
        derived_total_seasons=None,
    )
    assert should_sync is True
    assert reason == "episode-marker-changed"


def test_should_sync_failed_when_resume_enabled() -> None:
    show = _base_show()
    state = _base_state(marker="S1E1", status="failed")
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=False,
        derived_total_seasons=None,
    )
    assert should_sync is True
    assert reason == "resume-failed"


def test_should_skip_failed_when_resume_disabled() -> None:
    show = _base_show()
    state = _base_state(marker="S1E1", status="failed")
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=False,
        force=False,
        check_total_seasons=False,
        derived_total_seasons=None,
    )
    assert should_sync is False
    assert reason == "up-to-date"


def test_should_sync_on_since_cutoff() -> None:
    show = _base_show()
    state = _base_state(marker="S1E1", status="success")
    since = datetime(2024, 2, 1, tzinfo=UTC)
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=False,
        derived_total_seasons=None,
        since=since,
    )
    assert should_sync is True
    assert reason == "since"


def test_should_sync_on_season_mismatch() -> None:
    show = _base_show(marker="S1E1", total_seasons=3)
    state = _base_state(marker="S1E1", status="success")
    should_sync, reason = should_sync_show(
        show=show,
        state=state,
        incremental=True,
        resume=True,
        force=False,
        check_total_seasons=True,
        derived_total_seasons=2,
    )
    assert should_sync is True
    assert reason == "season-mismatch"
