from __future__ import annotations

from pathlib import Path

from trr_backend.ingestion.fandom_person_scraper import parse_fandom_person_html


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[2] / "fixtures" / "fandom"
    return (base / name).read_text(encoding="utf-8")


def test_fandom_person_parsing_infobox_taglines_reunion_images() -> None:
    html = _read_fixture("lisa_barlow_person_sample.html")
    payload, photos = parse_fandom_person_html(html, source_url="https://real-housewives.fandom.com/wiki/Lisa_Barlow")

    assert payload["full_name"] == "Lisa Deanna Barlow"
    assert payload["gender"] == "Female"
    assert payload["resides_in"] == "Salt Lake City, Utah"
    assert payload["birthdate"] == "1974-12-14"
    assert payload["installment"] == "The Real Housewives of Salt Lake City"
    assert payload["main_seasons_display"] == "1-4"

    taglines = payload.get("taglines") or []
    assert len(taglines) == 2
    assert taglines[0]["season"] == 1
    assert taglines[0]["opening_order"] == "2/6"

    reunion = payload.get("reunion_seating") or []
    assert reunion
    assert reunion[0]["season"] == 1
    assert reunion[0]["side"] == "Left"
    assert reunion[0]["seat_order"] == "2/3"

    assert payload.get("summary")
    assert payload.get("infobox_raw")

    canonical_urls = [photo.get("image_url_canonical") for photo in photos]
    canonical_urls = [url for url in canonical_urls if url]
    assert len(photos) == len(set(canonical_urls))
    assert len(photos) == 5

    reunion_photo = next(
        photo for photo in photos if photo.get("context_section") == "reunion_seating"
    )
    assert reunion_photo["image_url"].endswith("Lisa_reunion_s1.jpg/revision/latest?cb=444")

    contexts = {(photo.get("context_section"), photo.get("context_type")) for photo in photos}
    assert ("infobox", "hero") in contexts
    assert ("taglines", "intro_card") in contexts
    assert ("reunion_seating", "reunion_look") in contexts
    assert ("article", "inline") in contexts
