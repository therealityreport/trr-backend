from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.person_gallery import (
    parse_imdb_person_mediaindex_images,
    parse_imdb_person_mediaviewer_details,
)


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[2] / "fixtures" / "imdb"
    return (base / name).read_text(encoding="utf-8")


def test_parse_person_mediaindex_images_picks_largest_srcset() -> None:
    html = _read_fixture("person_mediaindex_nm11883948_sample.html")
    images = parse_imdb_person_mediaindex_images(html, "nm11883948")

    assert images, "Expected at least one image"
    assert len(images) == len({row["source_image_id"] for row in images})

    primary = next(row for row in images if row["viewer_id"] == "rm1679992066")
    assert primary["url"].endswith("_UX776_.jpg")
    assert primary["width"] == 776


def test_parse_person_mediaviewer_details_extracts_people_titles() -> None:
    html = _read_fixture("person_mediaviewer_nm11883948_rm1679992066_sample.html")
    details = parse_imdb_person_mediaviewer_details(html, viewer_id="rm1679992066")

    assert details["gallery_index"] == 1
    assert details["gallery_total"] == 46
    assert details["people_imdb_ids"]
    assert "nm11883948" in details["people_imdb_ids"]
    assert details["title_imdb_ids"] == ["tt36951580"]
    assert details["url"].endswith("_V1_.jpg")
    assert details["width"] == 640
    assert "Lisa Barlow" in (details["caption"] or "")
