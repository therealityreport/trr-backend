from __future__ import annotations

from types import SimpleNamespace

import pytest

from trr_backend.socials.analytics import periods


def test_periods_delegate_only_to_canonical_calendar_and_keep_inclusive_ends(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def calendar(season_id, **kwargs):
        calls.append((season_id, kwargs))
        return [
            {
                "week_index": 0,
                "label": "Pre-Season",
                "week_type": "preseason",
                "episode_number": None,
                "date_start": "2026-09-10T00:00:00+00:00",
                "date_end": "2026-09-17T00:00:00+00:00",
            },
            {
                "week_index": 7,
                "label": "Week 7",
                "week_type": "episode",
                "episode_number": 7,
                "date_start": "2026-10-29T00:00:00+00:00",
                "date_end": "2026-11-05T01:00:00+00:00",
            },
        ]

    # No analytics, sentiment or job methods exist on this provider.
    monkeypatch.setattr(periods, "_provider", SimpleNamespace(list_canonical_week_windows=calendar))
    result = periods.get_periods("season-7")
    assert calls == [("season-7", {"timezone": "America/New_York", "source_scope": "network"})]
    assert result["weekly"][0] == {
        "week_index": 0,
        "label": "Pre-Season",
        "week_type": "preseason",
        "episode_number": None,
        "start": "2026-09-10T00:00:00+00:00",
        "end": "2026-09-16T23:59:59.999999Z",
    }
    assert result["weekly"][1]["end"] == "2026-11-05T00:59:59.999999Z"


def test_periods_fail_closed_before_provider_publication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(periods, "_provider", None)
    with pytest.raises(RuntimeError, match="PROVIDER_UNCONFIGURED"):
        periods.get_periods("season-7")


def test_periods_reject_invalid_timezone_before_reading_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(periods, "_provider", SimpleNamespace())
    with pytest.raises(ValueError, match="valid IANA timezone"):
        periods.get_periods("season-7", timezone="Invalid/Zone")


def test_periods_resolve_published_calendar_at_call_time(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = SimpleNamespace(list_canonical_week_windows=lambda *_args, **_kwargs: [])
    monkeypatch.setattr(periods, "_provider", provider)
    assert periods.get_periods("season-7")["weekly"] == []
    provider.list_canonical_week_windows = lambda *_args, **_kwargs: [
        {
            "week_index": 1,
            "label": "Week 1",
            "week_type": "episode",
            "episode_number": 1,
            "date_start": "2026-09-17T00:00:00Z",
            "date_end": "2026-09-24T00:00:00Z",
        }
    ]
    assert periods.get_periods("season-7")["weekly"][0]["episode_number"] == 1
