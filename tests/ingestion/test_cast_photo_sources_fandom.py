from __future__ import annotations

from pathlib import Path

from trr_backend.ingestion.cast_photo_sources import fetch_fandom_person_cast_photos


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "fandom"
    return (base / name).read_text(encoding="utf-8")


def test_fetch_fandom_person_cast_photos_applies_gallery_show_season_and_content_type(monkeypatch) -> None:
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

    assert len(rows) == 3

    rhoc_row = next(
        row
        for row in rows
        if row.get("context_section") == "The Real Housewives of Orange County Season 18 Reunion"
    )
    rhoc_metadata = rhoc_row.get("metadata") or {}
    assert rhoc_row["season"] == 18
    assert rhoc_metadata["show_name"] == "The Real Housewives of Orange County"
    assert rhoc_metadata["show_short_code"] == "RHOC"
    assert rhoc_metadata["content_type"] == "REUNION"
    assert rhoc_metadata["season_number"] == 18
    assert rhoc_metadata["source_page_title"] == "Andy Cohen"
    assert rhoc_row["title_names"] == ["The Real Housewives of Orange County"]
    assert rhoc_row["people_names"] == ["Andy Cohen"]

    rhoslc_row = next(
        row
        for row in rows
        if row.get("context_section") == "The Real Housewives of Salt Lake City Season 5 Confessional"
    )
    rhoslc_metadata = rhoslc_row.get("metadata") or {}
    assert rhoslc_row["season"] == 5
    assert rhoslc_metadata["show_name"] == "The Real Housewives of Salt Lake City"
    assert rhoslc_metadata["show_short_code"] == "RHOSLC"
    assert rhoslc_metadata["content_type"] == "CONFESSIONAL"
    assert rhoslc_metadata["season_number"] == 5
