from __future__ import annotations

from pathlib import Path

from trr_backend.ingestion.imdb_images import extract_imdb_image_urls


def _read_fixture(name: str) -> str:
    base = Path(__file__).resolve().parents[2] / "fixtures" / "imdb"
    return (base / name).read_text(encoding="utf-8")


def test_extract_imdb_image_urls_from_section_images() -> None:
    html = _read_fixture("section_images_sample.html")
    urls = extract_imdb_image_urls(html)

    assert urls, "Expected at least one image URL"
    assert all(url.startswith("https://m.media-amazon.com/images/") for url in urls)

    expected_2x = "https://m.media-amazon.com/images/M/AAA._V1_QL75_UX360_CR0,2,360,533_.jpg"
    assert expected_2x in urls

    one_x = "https://m.media-amazon.com/images/M/AAA._V1_QL75_UX180_CR0,2,180,266_.jpg"
    assert one_x not in urls

    wide_url = "https://m.media-amazon.com/images/M/BBB._V1_UX400_.jpg"
    assert wide_url in urls

    assert len(urls) == len(set(urls))


def test_srcset_with_embedded_commas() -> None:
    """Verify URLs with commas (like CR0,2,180,266_) are parsed correctly."""
    html = _read_fixture("section_images_sample.html")
    urls = extract_imdb_image_urls(html)
    # The 2x URL contains commas in the crop params and should be extracted correctly
    assert any("CR0,2,360,533_" in url for url in urls)
