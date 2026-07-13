from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
from trr_backend.socials.social_season_analytics_impl import (
    _normalize_catalog_backfill_window,
    resolve_social_account_catalog_action_seed,
)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"date_start": "2026-01-01T00:00:00Z"},
        {"date_start": "invalid", "date_end": "2026-01-02T00:00:00Z"},
        {"date_start": "2026-01-02T00:00:00Z", "date_end": "2026-01-01T00:00:00Z"},
        {"catalog_action_scope": "bounded_window"},
        {
            "catalog_action_scope": "full_history",
            "date_start": "2026-01-01T00:00:00Z",
            "date_end": "2026-01-02T00:00:00Z",
        },
    ],
)
def test_catalog_action_seed_rejects_ambiguous_windows(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        resolve_social_account_catalog_action_seed(**kwargs)


def test_catalog_action_seed_allows_full_history_without_dates() -> None:
    assert resolve_social_account_catalog_action_seed() == {
        "date_start": None,
        "date_end": None,
        "resume_frontier_cursor": None,
        "catalog_action": "backfill",
        "catalog_action_scope": "full_history",
    }


def test_catalog_window_preserves_exclusive_midnight_end() -> None:
    start = datetime(2026, 6, 1, tzinfo=UTC)
    exclusive_end = datetime(2026, 8, 1, tzinfo=UTC)

    assert _normalize_catalog_backfill_window(date_start=start, date_end=exclusive_end) == (
        start,
        exclusive_end,
    )


def test_bounded_catalog_counts_and_enumeration_use_half_open_end(monkeypatch: pytest.MonkeyPatch) -> None:
    queries: list[str] = []
    start = datetime(2026, 6, 1, tzinfo=UTC)
    exclusive_end = datetime(2026, 8, 1, tzinfo=UTC)

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda sql, _params=None: queries.append(" ".join(sql.split()).lower()) or {"total": 0},
    )
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda sql, _params=None: queries.append(" ".join(sql.split()).lower()) or [],
    )

    social_repo._shared_catalog_total_posts_for_window(
        "instagram",
        "bravotv",
        date_start=start,
        date_end=exclusive_end,
    )
    social_repo._materialized_social_account_total_posts(
        "instagram",
        "bravotv",
        date_start=start,
        date_end=exclusive_end,
    )
    social_repo._load_existing_social_account_posts("instagram", "bravotv", start, exclusive_end)

    assert len(queries) == 3
    assert all("posted_at < %s" in query for query in queries)
    assert all("posted_at <= %s" not in query for query in queries)
    selected, _crossed_start = social_repo._filter_shared_instagram_posts_for_window(
        [
            SimpleNamespace(taken_at=int(start.timestamp())),
            SimpleNamespace(taken_at=int(exclusive_end.timestamp())),
        ],
        date_start=start,
        date_end=exclusive_end,
    )
    assert [post.taken_at for post in selected] == [int(start.timestamp())]


def test_catalog_shards_are_contiguous_half_open_windows() -> None:
    start = datetime(2026, 6, 1, tzinfo=UTC)
    exclusive_end = datetime(2026, 6, 7, tzinfo=UTC)

    shards = social_repo._build_catalog_backfill_shards(
        platform="tiktok",
        date_start=start,
        date_end=exclusive_end,
        runner_count=2,
    )

    assert [(shard.window_start, shard.window_end) for shard in shards] == [
        (datetime(2026, 6, 1, tzinfo=UTC), datetime(2026, 6, 3, tzinfo=UTC)),
        (datetime(2026, 6, 3, tzinfo=UTC), datetime(2026, 6, 5, tzinfo=UTC)),
        (datetime(2026, 6, 5, tzinfo=UTC), datetime(2026, 6, 7, tzinfo=UTC)),
    ]


@pytest.mark.parametrize("bounded_total", [360, 0])
def test_bounded_target_preview_is_read_only_and_preserves_zero(
    monkeypatch: pytest.MonkeyPatch,
    bounded_total: int,
) -> None:
    monkeypatch.setattr(
        social_repo,
        "_shared_catalog_total_posts_for_window",
        lambda *_args, **_kwargs: bounded_total,
    )
    monkeypatch.setattr(
        social_repo,
        "_materialized_social_account_total_posts",
        lambda *_args, **_kwargs: bounded_total,
    )
    monkeypatch.setattr(
        social_repo,
        "_create_run",
        lambda *_args, **_kwargs: pytest.fail("bounded preview must not create a run"),
    )
    monkeypatch.setattr(
        social_repo,
        "_create_job",
        lambda *_args, **_kwargs: pytest.fail("bounded preview must not create a job"),
    )
    monkeypatch.setattr(
        social_repo,
        "_best_known_social_account_total_posts",
        lambda *_args, **_kwargs: pytest.fail("bounded preview must not use profile history"),
    )

    preview = social_repo.preview_social_account_catalog_backfill_target(
        "instagram",
        "bravotv",
        date_start=datetime(2026, 6, 1, tzinfo=UTC),
        date_end=datetime(2026, 8, 1, tzinfo=UTC),
    )

    assert preview == {
        "catalog_total": bounded_total,
        "materialized_total": bounded_total,
        "completion_target_posts": bounded_total,
        "completion_target_source": "bounded_catalog",
    }
