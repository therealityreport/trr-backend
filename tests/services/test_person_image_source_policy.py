from __future__ import annotations

from trr_backend.services.person_images import source_policy


def test_canonicalize_refresh_sources_maps_getty_alias() -> None:
    assert source_policy.canonicalize_refresh_sources(["imdb", "getty", "nbcumv"]) == ["imdb", "nbcumv"]


def test_apply_show_source_policy_blocks_fandom_for_non_housewives_show() -> None:
    sources, fandom_skipped = source_policy.apply_show_source_policy(
        show_name="Top Chef",
        sources=["imdb", "fandom", "fandom-gallery", "bravotv"],
    )

    assert fandom_skipped is True
    assert sources == ["imdb", "bravotv"]


def test_normalize_operational_refresh_sources_keeps_getty_pipeline_when_prefetched() -> None:
    normalized = source_policy.normalize_operational_refresh_sources(
        sources=["imdb"],
        requested_sources=["imdb"],
        has_getty_prefetched_assets=True,
        has_getty_prefetched_events=False,
        has_getty_prefetched_queries=False,
    )

    assert normalized == ["imdb", "nbcumv"]


def test_ordered_source_progress_snapshot_uses_declared_order() -> None:
    snapshot = source_policy.ordered_source_progress_snapshot(
        {
            "bravotv": {"status": "pending"},
            "imdb": {"status": "completed"},
            "custom": {"status": "running"},
        },
        ("imdb", "bravotv"),
    )

    assert list(snapshot.keys()) == ["imdb", "bravotv", "custom"]
