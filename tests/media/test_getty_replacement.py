from __future__ import annotations

from unittest.mock import MagicMock

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


def test_apply_media_asset_replacement_generates_variants_with_keyword_asset_id(
    monkeypatch,
) -> None:
    """After a successful replacement, variant generation must be invoked with
    the keyword-only asset_id. The fake mirrors the real keyword-only
    signature of generate_media_asset_variants, so a positional call would
    raise TypeError (swallowed by the caller) and leave variant_calls empty."""
    monkeypatch.setattr(
        getty_replacement,
        "download_and_hash_image",
        lambda *args, **kwargs: (b"image-bytes", "sha-1", "image/jpeg"),
    )
    monkeypatch.setattr(getty_replacement, "get_s3_client", lambda: MagicMock())
    monkeypatch.setattr(getty_replacement, "get_s3_bucket", lambda: "test-bucket")
    monkeypatch.setattr(getty_replacement, "build_hosted_url", lambda key: f"https://cdn.test/{key}")

    variant_calls: list[tuple[object, str]] = []

    def _fake_generate_variants(db, *, asset_id, crop=None, force=False):
        variant_calls.append((db, asset_id))
        return []

    monkeypatch.setattr(getty_replacement, "generate_media_asset_variants", _fake_generate_variants)

    db = MagicMock()
    result = getty_replacement.apply_media_asset_replacement(
        db,
        asset_id="asset-1",
        row={"source": "getty", "source_url": "https://gettyimages.com/x", "metadata": {}},
        replacement=getty_replacement.ResolvedPublicReplacement(
            page_url="https://www.bravotv.com/story",
            source_domain="bravotv.com",
            image_url="https://cdn.example.com/b.jpg",
            width=1600,
            height=900,
        ),
    )

    assert result["status"] == "replaced"
    assert variant_calls == [(db, "asset-1")]
