from __future__ import annotations

from scripts.socials import import_socialblade_seed as cli


def test_normalize_payload_preserves_repository_shape() -> None:
    payload = cli._normalize_payload(
        {
            "username": "lisabarlow14",
            "scraped_at": "2026-03-16T07:29:43Z",
            "stats_refreshed": True,
            "profile_stats": {"followers": 123},
            "rankings": {"grade": "B+"},
            "daily_channel_metrics_60day": {"row_count": 60},
            "daily_total_followers_chart": {"data": [{"date": "2026-03-15", "followers": 123}]},
        },
        "lisabarlow14",
    )

    assert payload == {
        "username": "lisabarlow14",
        "platform": "instagram",
        "scraped_at": "2026-03-16T07:29:43Z",
        "stats_refreshed": True,
        "profile_stats": {"followers": 123},
        "rankings": {"grade": "B+"},
        "daily_channel_metrics_60day": {"row_count": 60},
        "daily_total_followers_chart": {"data": [{"date": "2026-03-15", "followers": 123}]},
    }


def test_resolve_person_id_requires_unique_match(monkeypatch) -> None:
    monkeypatch.setattr(
        cli.pg,
        "fetch_all",
        lambda query, params: [{"id": "person-1"}] if params == ["lisabarlow14"] else [],
    )

    assert cli._resolve_person_id("lisabarlow14") == "person-1"
