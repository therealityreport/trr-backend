from __future__ import annotations

from pathlib import Path

from trr_backend.scraping.url_image_scraper import ImageCandidate, extract_images_from_html, scrape_url_for_images


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


def test_scrape_populates_candidate_bytes_from_head(monkeypatch) -> None:
    import trr_backend.scraping.url_image_scraper as scraper

    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_MAX", "5")
    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_TIMEOUT_S", "0.1")
    monkeypatch.setenv("SCRAPE_PREVIEW_HEAD_WORKERS", "2")

    monkeypatch.setattr(scraper, "fetch_page_html", lambda url: ("<html></html>", "Example"))
    monkeypatch.setattr(
        scraper,
        "extract_images_from_html",
        lambda html, base_url, min_width=200, limit=50: [
            ImageCandidate(
                id="1",
                original_url="https://example.com/img.jpg",
                best_url="https://example.com/img.jpg",
                width=800,
                height=600,
            )
        ],
    )

    class FakeHeadResponse:
        status_code = 200
        headers = {"Content-Length": "1234"}

    monkeypatch.setattr(scraper.requests, "head", lambda *args, **kwargs: FakeHeadResponse())

    result = scrape_url_for_images("https://example.com/page")
    assert result.error is None
    assert result.total_found == 1
    assert result.images[0].bytes == 1234


def test_direct_image_sets_bytes_from_head(monkeypatch) -> None:
    import trr_backend.scraping.url_image_scraper as scraper

    class FakeHeadResponse:
        status_code = 200
        headers = {"Content-Type": "image/jpeg", "Content-Length": "999"}

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(scraper.requests, "head", lambda *args, **kwargs: FakeHeadResponse())

    result = scrape_url_for_images("https://example.com/direct.jpg")
    assert result.error is None
    assert result.total_found == 1
    assert result.images[0].bytes == 999
