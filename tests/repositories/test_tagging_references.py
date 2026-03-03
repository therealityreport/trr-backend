from __future__ import annotations

from typing import Any

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
    assert profile["used"][0]["link_id"] == "link-1"


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
    assert profile["used"][0]["url"] == "https://origin.example.com/ref-1.jpg"
    assert profile["used"][0]["source_url"] == "https://origin.example.com/ref-1.jpg"
    assert profile["used"][0]["hosted_url"] == "https://cdn.example.com/ref-1.jpg"
    assert profile["used"][0]["url_candidates"] == [
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
    assert selected[0]["link_id"] == "link-cross-title-seeded"
    assert "cross_title_wwhl" in list(selected[0].get("reasons") or [])
