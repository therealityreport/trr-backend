from __future__ import annotations

from pathlib import Path

from trr_backend.scraping.url_image_scraper import extract_images_from_html


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "scraping"
    return (base / name).read_text(encoding="utf-8")


def test_extracts_pinterest_media_and_skips_icons() -> None:
    html = _read_fixture("eonline_pinterest_sample.html")
    base_url = "https://www.eonline.com/photos/34966/sample"

    candidates = extract_images_from_html(html, base_url, min_width=200, limit=50)
    urls = [candidate.best_url for candidate in candidates]

    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160858-rhoslc6.jpg" in urls
    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160858-rhoslc.jpg" in urls
    assert "https://akns-images.eonline.com/eol_images/Entire_Site/2022730/rs_634x707-220830160857-rhoslc3.jpg" in urls

    assert "https://assets.pinterest.com/images/pidgets/pinit_fg_en_round_red_32.png" not in urls

    alt_texts = [candidate.alt_text for candidate in candidates if candidate.alt_text]
    assert any("Jen Shah" in text for text in alt_texts)
    assert any("Lisa Barlow" in text for text in alt_texts)
