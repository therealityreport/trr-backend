from __future__ import annotations

import pytest
from pydantic import ValidationError

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
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="abc",
            url="https://example.com/x.jpg",
            kind="not-a-kind",
        )


def test_import_image_item_accepts_logo_target_fields() -> None:
    item = ImportImageItem(
        candidate_id="logo-1",
        url="https://example.com/logo.png",
        kind="logo",
        logo_target_type="publication",
        logo_target_key="deadline.com",
        logo_target_label="Deadline",
        logo_set_primary=True,
    )
    assert item.kind == "logo"
    assert item.logo_target_type == "publication"
    assert item.logo_target_key == "deadline.com"
    assert item.logo_target_label == "Deadline"
    assert item.logo_set_primary is True


def test_import_image_item_rejects_unknown_logo_target_type() -> None:
    with pytest.raises(ValidationError):
        ImportImageItem(
            candidate_id="logo-2",
            url="https://example.com/logo.png",
            kind="logo",
            logo_target_type="invalid-target",  # type: ignore[arg-type]
            logo_target_key="foo",
            logo_target_label="Foo",
        )
