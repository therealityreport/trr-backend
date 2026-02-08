from __future__ import annotations

import pytest

from api.routers.admin_scrape import ImportImageItem


def test_import_image_item_accepts_new_kinds() -> None:
    # Pydantic should accept expanded ImageKind literals.
    item = ImportImageItem(
        candidate_id="abc",
        url="https://example.com/x.jpg",
        kind="promo",
        caption=None,
        person_ids=None,
    )
    assert item.kind == "promo"


def test_import_image_item_rejects_unknown_kind() -> None:
    with pytest.raises(Exception):
        ImportImageItem(
            candidate_id="abc",
            url="https://example.com/x.jpg",
            kind="not-a-kind",
        )

