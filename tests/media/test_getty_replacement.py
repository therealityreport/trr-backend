from __future__ import annotations

from trr_backend.integrations.picdetective import ReverseImageCandidate
from trr_backend.media import getty_replacement
from trr_backend.scraping.url_image_scraper import ImageCandidate, ScrapeResult


def test_is_approved_public_domain_excludes_removed_domains() -> None:
    assert getty_replacement.is_approved_public_domain("https://www.bravotv.com/gallery")
    assert getty_replacement.is_approved_public_domain("nbcinsider.com")
    assert not getty_replacement.is_approved_public_domain("peacocktv.com")
    assert not getty_replacement.is_approved_public_domain("eonline.com")


def test_search_public_replacement_candidates_prefers_approved_domains(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        getty_replacement,
        "search_by_image_url",
        lambda *args, **kwargs: [
            ReverseImageCandidate(
                title="Unapproved larger candidate",
                source_domain="example.com",
                page_url="https://example.com/story",
                thumbnail_b64=None,
                width=2400,
                height=1600,
            ),
            ReverseImageCandidate(
                title="NBC candidate",
                source_domain="www.nbc.com",
                page_url="https://www.nbc.com/story",
                thumbnail_b64=None,
                width=1600,
                height=900,
            ),
            ReverseImageCandidate(
                title="Bravo candidate",
                source_domain="bravotv.com",
                page_url="https://www.bravotv.com/story",
                thumbnail_b64=None,
                width=1500,
                height=844,
            ),
            ReverseImageCandidate(
                title="Removed domain",
                source_domain="eonline.com",
                page_url="https://www.eonline.com/story",
                thumbnail_b64=None,
                width=3000,
                height=2000,
            ),
        ],
    )

    candidates = getty_replacement.search_public_replacement_candidates(
        "https://media.gettyimages.com/example.jpg",
        expected_width=1600,
        expected_height=900,
        bravo_only=True,
        limit=5,
    )

    assert [candidate.source_domain for candidate in candidates] == ["bravotv.com", "www.nbc.com"]


def test_resolve_public_replacement_from_page_prefers_closest_ratio_over_larger_image(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        getty_replacement,
        "scrape_url_for_images",
        lambda *args, **kwargs: ScrapeResult(
            url="https://www.bravotv.com/gallery",
            page_title="Gallery",
            domain="bravotv.com",
            images=[
                ImageCandidate(
                    id="largest-wrong-ratio",
                    original_url="https://cdn.example.com/a.jpg",
                    best_url="https://cdn.example.com/a.jpg",
                    width=2400,
                    height=1600,
                ),
                ImageCandidate(
                    id="best-ratio",
                    original_url="https://cdn.example.com/b.jpg",
                    best_url="https://cdn.example.com/b.jpg",
                    width=1600,
                    height=900,
                ),
            ],
            total_found=2,
        ),
    )

    replacement = getty_replacement.resolve_public_replacement_from_page(
        "https://www.bravotv.com/gallery",
        source_domain="bravotv.com",
        expected_width=1200,
        expected_height=675,
        bravo_only=True,
    )

    assert replacement is not None
    assert replacement.image_url == "https://cdn.example.com/b.jpg"
    assert replacement.source_domain == "bravotv.com"
