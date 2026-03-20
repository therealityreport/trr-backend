"""Tests for admin show Getty/NBCUMV import endpoints."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from api.routers import admin_show_images
from trr_backend.media.getty_replacement import ResolvedPublicReplacement


def _make_getty_asset() -> dict[str, object]:
    return {
        "detail_url": "https://www.gettyimages.com/detail/news-photo/bravo-show/1",
        "editorial_id": "show-getty-1",
        "object_name": "SHOW_GETTY.JPG",
        "title": "Bravo Show Promo",
        "event_name": "Bravo Show Premiere",
        "caption": "A Bravo cast photo.",
        "preview_image_url": "https://media.gettyimages.com/show-comp.jpg",
        "assetDimensions": {"width": 1600, "height": 900},
        "people": [{"text": "Lisa Barlow"}],
        "keyword_texts": ["Season 2"],
    }


def _build_media_assets_db(inserted_rows: list[dict[str, object]]) -> MagicMock:
    db = MagicMock()
    existing_query = (
        db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value
    )
    existing_query.execute.return_value = SimpleNamespace(data=[], error=None)

    def _insert(row: dict[str, object]):
        inserted_rows.append(row)
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=[row], error=None))

    db.schema.return_value.table.return_value.insert.side_effect = _insert
    return db


def test_show_is_bravo_family_checks_networks() -> None:
    assert admin_show_images._show_is_bravo_family({"networks": ["Bravo"]}) is True
    assert admin_show_images._show_is_bravo_family({"networks": ["NBCU Bravo"]}) is True
    assert admin_show_images._show_is_bravo_family({"networks": ["NBC"]}) is False
    assert admin_show_images._show_is_bravo_family({"networks": None}) is False


@pytest.mark.parametrize(
    ("show_is_bravo", "expected_resolution"),
    [(True, "auto_picdetective_bravo"), (False, "getty_watermark_fallback")],
)
def test_import_show_images_uses_public_replacement_only_for_bravo_family(
    monkeypatch: pytest.MonkeyPatch,
    show_is_bravo: bool,
    expected_resolution: str,
) -> None:
    from api.routers import admin_nbcumv
    from trr_backend.integrations import getty as getty_integration
    from trr_backend.integrations import nbcumv as nbcumv_integration

    inserted_rows: list[dict[str, object]] = []
    db = _build_media_assets_db(inserted_rows)
    resolved_person_id = str(uuid4())

    monkeypatch.setattr(admin_nbcumv, "_ensure_sources", lambda db: None)
    monkeypatch.setattr(admin_nbcumv, "_load_eligible_people_index", lambda db: [])
    monkeypatch.setattr(
        admin_nbcumv,
        "_match_people_names",
        lambda people_index, tagged_people: {
            "resolved": [{"person_id": resolved_person_id}],
            "unmatched": [],
            "ambiguous": [],
        },
    )
    monkeypatch.setattr(getty_integration, "search_editorial_assets", lambda *args, **kwargs: [_make_getty_asset()])
    monkeypatch.setattr(nbcumv_integration, "resolve_show_by_title", lambda title: None)
    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.asset_id_for",
        lambda source, source_asset_id, source_url: uuid4(),
    )
    monkeypatch.setattr(
        "trr_backend.repositories.web_scrape_images.create_media_link_for_entity",
        lambda *args, **kwargs: {"id": str(uuid4())},
    )

    replacement_calls: list[dict[str, object]] = []

    def _resolve_replacement(*args, **kwargs):
        replacement_calls.append({"args": args, "kwargs": kwargs})
        return ResolvedPublicReplacement(
            page_url="https://www.bravotv.com/show/gallery",
            source_domain="bravotv.com",
            image_url="https://www.bravotv.com/sites/bravo/files/show-gallery-01.jpg",
            width=1825,
            height=1217,
        )

    monkeypatch.setattr(admin_show_images, "resolve_best_public_replacement", _resolve_replacement)

    result = admin_show_images._import_show_images(
        db,
        show_id=str(uuid4()),
        show_name="The Real Housewives of Salt Lake City",
        show_is_bravo=show_is_bravo,
        limit=10,
        getty_limit=10,
    )

    assert result["getty_imported"] == 1
    assert len(inserted_rows) == 1
    row = inserted_rows[0]
    metadata = row["metadata"]
    assert metadata["source_resolution"] == expected_resolution

    if show_is_bravo:
        assert len(replacement_calls) == 1
        assert row["source_url"] == "https://www.bravotv.com/sites/bravo/files/show-gallery-01.jpg"
        assert row["width"] == 1825
        assert row["height"] == 1217
        assert metadata["source_domain"] == "bravotv.com"
        assert metadata["source_page_url"] == "https://www.bravotv.com/show/gallery"
        assert metadata["original_source_url"] == "https://media.gettyimages.com/show-comp.jpg"
        assert metadata["getty_only_fallback"] is False
    else:
        assert replacement_calls == []
        assert row["source_url"] == "https://media.gettyimages.com/show-comp.jpg"
        assert row["width"] == 1600
        assert row["height"] == 900
        assert metadata["getty_only_fallback"] is True
