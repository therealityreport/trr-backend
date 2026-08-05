from __future__ import annotations

import subprocess
import sys
import textwrap
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials import windowing
from trr_backend.socials.control_plane import windowing as control_plane_windowing
from trr_backend.socials.model_types import SeasonContext, WeekWindow

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _run_fresh_interpreter(source: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        cwd=_PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_week_window_leaf_is_import_neutral_and_unconfigured_in_fresh_interpreter() -> None:
    result = _run_fresh_interpreter(
        """
        import sys

        from trr_backend.socials import windowing

        assert "trr_backend.socials.social_season_analytics_impl" not in sys.modules
        assert windowing._week_window_resolver is None
        try:
            windowing.resolve_week_window("season-1", week_index=1)
        except RuntimeError as exc:
            assert str(exc) == "Social week-window resolver is not configured"
        else:
            raise AssertionError("unconfigured windowing port did not fail")
        """
    )

    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.parametrize(
    "composition_import",
    [
        """
        from trr_backend.repositories import social_season_analytics as repo

        assert windowing._week_window_resolver is repo.resolve_week_window
        """,
        """
        from trr_backend.socials.control_plane import resolve_week_window

        assert windowing._week_window_resolver is not None
        assert resolve_week_window is windowing.resolve_week_window
        """,
    ],
    ids=["legacy-repository", "public-control-plane"],
)
def test_production_composition_imports_register_resolver_in_fresh_interpreter(
    composition_import: str,
) -> None:
    result = _run_fresh_interpreter("from trr_backend.socials import windowing\n" + textwrap.dedent(composition_import))

    assert result.returncode == 0, result.stderr or result.stdout


def test_week_window_resolver_requires_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(windowing, "_week_window_resolver", None)

    with pytest.raises(RuntimeError, match="Social week-window resolver is not configured"):
        windowing.resolve_week_window("season-1", week_index=1)


def test_week_window_resolver_preserves_arguments_and_result_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = {"week_index": 2, "label": "Week 2"}
    captured: dict[str, Any] = {}
    now_utc = datetime(2026, 7, 16, 12, 30, tzinfo=UTC)

    def fake_resolver(
        season_id: str,
        *,
        week_index: int,
        timezone: str = "America/New_York",
        source_scope: str = "network",
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        captured.update(
            season_id=season_id,
            week_index=week_index,
            timezone=timezone,
            source_scope=source_scope,
            now_utc=now_utc,
        )
        return expected

    monkeypatch.setattr(windowing, "_week_window_resolver", None)
    windowing.configure_week_window_resolver(fake_resolver)

    result = windowing.resolve_week_window(
        "season-2",
        week_index=2,
        timezone="UTC",
        source_scope="community",
        now_utc=now_utc,
    )

    assert result is expected
    assert captured == {
        "season_id": "season-2",
        "week_index": 2,
        "timezone": "UTC",
        "source_scope": "community",
        "now_utc": now_utc,
    }


def test_monolith_composition_root_registers_compatibility_function() -> None:
    assert windowing._week_window_resolver is repo.resolve_week_window


def test_week_window_port_uses_canonical_monolith_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    start = datetime(2026, 1, 7, tzinfo=UTC)
    context = SeasonContext(
        season_id="season-3",
        show_id="show-3",
        show_name="Test Show",
        season_number=3,
        anchor_date=date(2026, 1, 1),
    )
    resolved = WeekWindow(
        week_index=2,
        start_local=start,
        end_local=start + timedelta(days=7),
        episode_number=2,
    )
    monkeypatch.setattr(repo, "get_season_context", lambda _season_id: context)
    monkeypatch.setattr(repo, "_resolve_week_windows", lambda *_args, **_kwargs: ([resolved], start))

    result = windowing.resolve_week_window("season-3", week_index=2, timezone="UTC")

    assert result == {
        "week_index": 2,
        "label": "Week 2",
        "start": "2026-01-07T00:00:00+00:00",
        "end": "2026-01-13T23:59:59.999999+00:00",
        "week_type": "episode",
        "episode_number": 2,
        "timezone": "UTC",
    }


def test_control_plane_windowing_reexports_import_neutral_resolver() -> None:
    assert control_plane_windowing is windowing
    assert control_plane_windowing.resolve_week_window is windowing.resolve_week_window


def test_control_plane_windowing_monkeypatches_legacy_leaf(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_resolver(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"patched": True}

    monkeypatch.setattr(control_plane_windowing, "resolve_week_window", fake_resolver)

    assert windowing.resolve_week_window is fake_resolver
    assert control_plane_windowing.resolve_week_window("season-4", week_index=4) == {"patched": True}
