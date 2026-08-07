from __future__ import annotations

from typing import Any, cast

from trr_backend.repositories import tagging_references as refs


def _selected_context(rank: int, *, computed_at: str) -> dict[str, Any]:
    return {
        "tagging_reference": {
            "selected": True,
            "rank": rank,
            "reasons": ["seeded", "solo"],
            "profile_version": "v1",
            "computed_at": computed_at,
        }
    }


def test_build_owner_tagging_reference_profile_pins_existing_selected(monkeypatch) -> None:
    rows = [
        {
            "link_id": "link-1",
            "media_asset_id": "asset-1",
            "hosted_url": "https://cdn.example.com/ref-1.jpg",
            "source_url": "https://origin.example.com/ref-1.jpg",
            "context": _selected_context(1, computed_at="2026-03-01T00:00:00+00:00"),
            "metadata": {"people_count": 1},
            "facebank_seed": True,
            "link_updated_at": "2026-03-02T00:00:00+00:00",
            "asset_updated_at": "2026-03-02T00:00:00+00:00",
        }
    ]

    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)

    def _rank_should_not_run(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("candidate ranking should not run when selected references are pinned")

    monkeypatch.setattr(refs, "_rank_candidates", _rank_should_not_run)

    profile = refs.build_owner_tagging_reference_profile(db=None, person_id="person-1")
    assert profile["cache_hit"] is True
    assert profile["accepted"] == 1
    used = cast("list[dict[str, Any]]", profile["used"])
    assert used[0]["link_id"] == "link-1"


def test_sync_owner_tagging_reference_usage_preserves_existing_selected_by_default(monkeypatch) -> None:
    rows = [
        {
            "link_id": "link-1",
            "media_asset_id": "asset-1",
            "hosted_url": "https://cdn.example.com/ref-1.jpg",
            "source_url": "https://origin.example.com/ref-1.jpg",
            "context": _selected_context(1, computed_at="2026-03-01T00:00:00+00:00"),
        },
        {
            "link_id": "link-2",
            "media_asset_id": "asset-2",
            "hosted_url": "https://cdn.example.com/ref-2.jpg",
            "source_url": "https://origin.example.com/ref-2.jpg",
            "context": _selected_context(2, computed_at="2026-03-01T00:00:00+00:00"),
        },
    ]
    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)
    captured: dict[str, Any] = {}

    def _capture_apply(_db, _rows, *, selected, computed_at_iso, profile_version):  # noqa: ANN001
        captured["selected"] = selected
        captured["computed_at_iso"] = computed_at_iso
        captured["profile_version"] = profile_version

    monkeypatch.setattr(refs, "_apply_selection_to_context", _capture_apply)

    output = refs.sync_owner_tagging_reference_usage(
        db=None,
        person_id="person-1",
        used_references=[
            {
                "url": "https://cdn.example.com/ref-1.jpg",
                "media_asset_id": "asset-1",
                "link_id": "link-1",
                "rank": 1,
                "reasons": ["seeded", "solo"],
            }
        ],
    )

    assert [entry.get("link_id") for entry in output] == ["link-1", "link-2"]
    assert [entry.get("rank") for entry in output] == [1, 2]
    assert [entry.get("link_id") for entry in captured["selected"]] == ["link-1", "link-2"]


def test_sync_owner_tagging_reference_usage_can_replace_when_preserve_existing_disabled(monkeypatch) -> None:
    rows = [
        {
            "link_id": "link-1",
            "media_asset_id": "asset-1",
            "hosted_url": "https://cdn.example.com/ref-1.jpg",
            "source_url": "https://origin.example.com/ref-1.jpg",
            "context": _selected_context(1, computed_at="2026-03-01T00:00:00+00:00"),
        },
        {
            "link_id": "link-2",
            "media_asset_id": "asset-2",
            "hosted_url": "https://cdn.example.com/ref-2.jpg",
            "source_url": "https://origin.example.com/ref-2.jpg",
            "context": _selected_context(2, computed_at="2026-03-01T00:00:00+00:00"),
        },
    ]
    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)
    monkeypatch.setattr(refs, "_apply_selection_to_context", lambda *_args, **_kwargs: None)

    output = refs.sync_owner_tagging_reference_usage(
        db=None,
        person_id="person-1",
        used_references=[
            {
                "url": "https://cdn.example.com/ref-1.jpg",
                "media_asset_id": "asset-1",
                "link_id": "link-1",
                "rank": 1,
                "reasons": ["seeded", "solo"],
            }
        ],
        preserve_existing=False,
    )

    assert [entry.get("link_id") for entry in output] == ["link-1"]
    assert [entry.get("rank") for entry in output] == [1]


def test_selected_references_prefer_source_url_over_hosted_url(monkeypatch) -> None:
    rows = [
        {
            "link_id": "link-1",
            "media_asset_id": "asset-1",
            "hosted_url": "https://cdn.example.com/ref-1.jpg",
            "source_url": "https://origin.example.com/ref-1.jpg",
            "context": _selected_context(1, computed_at="2026-03-01T00:00:00+00:00"),
            "metadata": {"people_count": 1},
        }
    ]
    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)

    profile = refs.build_owner_tagging_reference_profile(db=None, person_id="person-1")
    assert profile["cache_hit"] is True
    assert profile["accepted"] == 1
    used = cast("list[dict[str, Any]]", profile["used"])
    assert used[0]["url"] == "https://origin.example.com/ref-1.jpg"
    assert used[0]["source_url"] == "https://origin.example.com/ref-1.jpg"
    assert used[0]["hosted_url"] == "https://cdn.example.com/ref-1.jpg"
    assert used[0]["url_candidates"] == [
        "https://origin.example.com/ref-1.jpg",
        "https://cdn.example.com/ref-1.jpg",
    ]


def test_rank_candidates_wwhl_allows_seeded_cross_title_references() -> None:
    rows = [
        {
            "link_id": "link-cross-title-seeded",
            "media_asset_id": "asset-1",
            "source": "imdb",
            "facebank_seed": True,
            "source_url": "https://origin.example.com/traitors-1.jpg",
            "hosted_url": "https://cdn.example.com/traitors-1.jpg",
            "context": {"show_name": "The Traitors", "people_count": 1},
            "metadata": {"show_name": "The Traitors", "people_count": 1},
            "position": 1,
        },
        {
            "link_id": "link-wwhl-unseeded",
            "media_asset_id": "asset-2",
            "source": "imdb",
            "source_url": "https://origin.example.com/wwhl-1.jpg",
            "hosted_url": "https://cdn.example.com/wwhl-1.jpg",
            "context": {"show_name": "Watch What Happens Live with Andy Cohen", "people_count": 1},
            "metadata": {"show_name": "Watch What Happens Live with Andy Cohen", "people_count": 1},
            "position": 2,
        },
    ]

    selected, _skipped = refs._rank_candidates(
        rows,
        show_ids=set(),
        show_name_keys={"watch what happens live with andy cohen"},
        request_show_id=None,
        request_show_name="WWHL",
        max_refs=12,
    )

    assert len(selected) >= 1
    first = cast("dict[str, Any]", selected[0])
    assert first["link_id"] == "link-cross-title-seeded"
    assert "cross_title_wwhl" in list(selected[0].get("reasons") or [])


def test_build_owner_facebank_initial_reference_profile_prefers_show_and_primary_then_caps_event_sources(
    monkeypatch,
) -> None:
    rows = [
        {
            "link_id": "link-seeded-event",
            "media_asset_id": "asset-1",
            "source": "nbcumv",
            "source_url": "https://origin.example.com/event-1.jpg",
            "hosted_url": "https://cdn.example.com/event-1.jpg",
            "facebank_seed": True,
            "is_primary": False,
            "position": 10,
            "width": 1600,
            "height": 1200,
            "context": {"show_id": "11111111-1111-1111-1111-111111111111", "people_count": 1},
            "metadata": {"gallery_bucket_type": "event", "people_count": 1},
            "asset_created_at": "2026-03-03T00:00:00+00:00",
        },
        {
            "link_id": "link-show-solo",
            "media_asset_id": "asset-2",
            "source": "tmdb",
            "source_url": "https://origin.example.com/show-solo.jpg",
            "hosted_url": "https://cdn.example.com/show-solo.jpg",
            "facebank_seed": False,
            "is_primary": False,
            "position": 2,
            "width": 1400,
            "height": 1000,
            "context": {"show_id": "11111111-1111-1111-1111-111111111111", "people_count": 1},
            "metadata": {"people_count": 1},
            "asset_created_at": "2026-03-02T00:00:00+00:00",
        },
        {
            "link_id": "link-primary",
            "media_asset_id": "asset-3",
            "source": "imdb",
            "source_url": "https://origin.example.com/primary.jpg",
            "hosted_url": "https://cdn.example.com/primary.jpg",
            "facebank_seed": False,
            "is_primary": True,
            "position": 1,
            "width": 1200,
            "height": 1600,
            "context": {"people_count": 1},
            "metadata": {"people_count": 1},
            "asset_created_at": "2026-03-01T00:00:00+00:00",
        },
        {
            "link_id": "link-second-event",
            "media_asset_id": "asset-4",
            "source": "nbcumv",
            "source_url": "https://origin.example.com/event-2.jpg",
            "hosted_url": "https://cdn.example.com/event-2.jpg",
            "facebank_seed": False,
            "is_primary": False,
            "position": 11,
            "width": 1600,
            "height": 1200,
            "context": {"people_count": 1},
            "metadata": {"gallery_bucket_type": "event", "people_count": 1},
            "asset_created_at": "2026-03-04T00:00:00+00:00",
        },
    ]
    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)
    monkeypatch.setattr(
        refs,
        "_load_person_show_context",
        lambda _db, _person_id: ({"11111111-1111-1111-1111-111111111111"}, {"demo show"}),
    )

    profile = refs.build_owner_facebank_initial_reference_profile(
        db=None,
        person_id="person-1",
        show_id="11111111-1111-1111-1111-111111111111",
        show_name="Demo Show",
        max_refs=5,
    )

    selected = cast("list[dict[str, Any]]", profile["used"])
    assert [entry["link_id"] for entry in selected[:2]] == ["link-show-solo", "link-primary"]
    assert [entry["link_id"] for entry in selected].count("link-seeded-event") == 1
    assert [entry["link_id"] for entry in selected].count("link-second-event") == 0


def test_build_owner_facebank_initial_reference_profile_uses_non_solo_only_as_last_resort(monkeypatch) -> None:
    rows = [
        {
            "link_id": "link-solo-1",
            "media_asset_id": "asset-1",
            "source": "tmdb",
            "source_url": "https://origin.example.com/solo-1.jpg",
            "hosted_url": "https://cdn.example.com/solo-1.jpg",
            "is_primary": False,
            "position": 1,
            "width": 900,
            "height": 1100,
            "context": {"people_count": 1},
            "metadata": {"people_count": 1},
        },
        {
            "link_id": "link-solo-2",
            "media_asset_id": "asset-2",
            "source": "imdb",
            "source_url": "https://origin.example.com/solo-2.jpg",
            "hosted_url": "https://cdn.example.com/solo-2.jpg",
            "is_primary": True,
            "position": 2,
            "width": 1000,
            "height": 1400,
            "context": {"people_count": 1},
            "metadata": {"people_count": 1},
        },
        {
            "link_id": "link-group-fallback",
            "media_asset_id": "asset-3",
            "source": "fandom-gallery",
            "source_url": "https://origin.example.com/group.jpg",
            "hosted_url": "https://cdn.example.com/group.jpg",
            "is_primary": False,
            "position": 3,
            "width": 1200,
            "height": 900,
            "context": {"people_count": 2},
            "metadata": {"people_count": 2},
        },
    ]
    monkeypatch.setattr(refs, "_list_gallery_rows", lambda _db, _person_id: rows)
    monkeypatch.setattr(refs, "_load_person_show_context", lambda _db, _person_id: (set(), set()))

    profile = refs.build_owner_facebank_initial_reference_profile(db=None, person_id="person-1", max_refs=3)

    used = cast("list[dict[str, Any]]", profile["used"])
    assert [entry["link_id"] for entry in used] == [
        "link-solo-2",
        "link-solo-1",
        "link-group-fallback",
    ]
