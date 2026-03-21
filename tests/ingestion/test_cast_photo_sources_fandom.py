from __future__ import annotations

from pathlib import Path

from trr_backend.ingestion.cast_photo_sources import (
    fetch_fandom_gallery_cast_photos,
    fetch_fandom_person_cast_photos,
)
from trr_backend.integrations.fandom import FandomGalleryImage, FandomGalleryResult


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "fandom"
    return (base / name).read_text(encoding="utf-8")


def test_fetch_fandom_person_cast_photos_keeps_only_real_housewives_confessionals_and_intros(monkeypatch) -> None:
    html = _read_fixture("andy_cohen_gallery_sample.html")

    monkeypatch.setattr(
        "trr_backend.ingestion.fandom_person_scraper.fetch_fandom_person_html",
        lambda url: (html, "https://real-housewives.fandom.com/wiki/Andy_Cohen"),
    )

    rows = fetch_fandom_person_cast_photos(
        person_name="Andy Cohen",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=20,
    )

    assert len(rows) == 1

    rhoslc_row = next(row for row in rows if row["metadata"]["content_type"] == "CONFESSIONAL")
    rhoslc_metadata = rhoslc_row.get("metadata") or {}
    assert rhoslc_row["season"] == 5
    assert rhoslc_metadata["show_name"] == "The Real Housewives of Salt Lake City"
    assert rhoslc_metadata["show_short_code"] == "RHOSLC"
    assert rhoslc_metadata["content_type"] == "CONFESSIONAL"
    assert rhoslc_metadata["season_number"] == 5
    assert rhoslc_row["context_type"] == "confessional"


def test_fetch_fandom_gallery_cast_photos_keeps_only_real_housewives_confessionals_and_intros(monkeypatch) -> None:
    gallery = FandomGalleryResult(
        source="fandom",
        url="https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
        person_name="Lisa Barlow",
        error=None,
        images=[
            FandomGalleryImage(
                url="https://static.wikia.nocookie.net/real-housewives/images/1/11/Lisa_Barlow_S5_Confession_1.png",
                thumb_url=None,
                caption="Season 5 ( RHOSLC )",
                source_page_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
                file_page_url=None,
                section_label="Confessional Interview Looks",
            ),
            FandomGalleryImage(
                url="https://static.wikia.nocookie.net/real-housewives/images/2/22/Lisa_Barlow_S6_Intro_Card_1.jpeg",
                thumb_url=None,
                caption="Season 6 ( RHOSLC )",
                source_page_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
                file_page_url=None,
                section_label="Intro Cards",
            ),
            FandomGalleryImage(
                url="https://static.wikia.nocookie.net/real-housewives/images/3/33/Lisa_Barlow_S5_Promotional_Portrait_1.webp",
                thumb_url=None,
                caption="Season 5 ( RHOSLC )",
                source_page_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
                file_page_url=None,
                section_label="Promotional Portraits",
            ),
            FandomGalleryImage(
                url="https://static.wikia.nocookie.net/real-housewives/images/4/44/Lisa_Barlow_S6_Title_Card_1.jpeg",
                thumb_url=None,
                caption="Season 6 ( RHOSLC )",
                source_page_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
                file_page_url=None,
                section_label="Title Cards",
            ),
        ],
    )

    monkeypatch.setattr("trr_backend.integrations.fandom.fetch_fandom_gallery", lambda *_args, **_kwargs: gallery)

    rows = fetch_fandom_gallery_cast_photos(
        person_name="Lisa Barlow",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=20,
        resolve_file_pages=False,
    )

    assert len(rows) == 3
    assert {row["metadata"]["content_type"] for row in rows} == {"CONFESSIONAL", "INTRO"}
    assert {row["context_type"] for row in rows} == {"confessional", "intro"}
    assert any((row.get("metadata") or {}).get("fandom_section_label") == "Title Cards" for row in rows)
