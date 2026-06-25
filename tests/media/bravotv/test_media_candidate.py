from __future__ import annotations

from trr_backend.media.bravotv.media_candidate import candidate_from_normalized_record


def test_getty_candidate_defaults_to_reference_only_display_ineligible() -> None:
    candidate = candidate_from_normalized_record(
        {
            "source": "getty",
            "source_id": "928663262",
            "nup_filename": "NUP_181952_0005.JPG",
            "source_url": "https://media.gettyimages.com/id/928663262/photo/example.jpg",
            "source_page_url": "https://www.gettyimages.com/detail/news-photo/example/928663262",
            "caption": "Pictured: Jane Doe",
            "people_names": ["Jane Doe"],
            "raw": {"object_name": "NUP_181952_0005.JPG"},
        }
    )

    assert candidate.source_role == "reference_metadata"
    assert candidate.display_eligible is False
    assert candidate.bridge_keys["nup_filename"] == "NUP_181952_0005.JPG"
    assert candidate.bridge_keys["nup_set"] == "NUP_181952"
    assert "metadata_only" in candidate.review_reasons


def test_nbcumv_candidate_preserves_lbx_bridge_key_and_display_eligibility() -> None:
    candidate = candidate_from_normalized_record(
        {
            "source": "nbcumv",
            "source_id": "70761487",
            "nup_filename": "NUP_181952_5.JPG",
            "source_url": "https://nbcumv.example/NUP_181952_5.JPG",
            "caption": "Pictured: Jane Doe",
            "width": 3000,
            "height": 2000,
            "raw": {"lbx_id": "70761487", "lbx_filename": "NUP_181952_5.JPG"},
        }
    )

    assert candidate.source_role == "original"
    assert candidate.display_eligible is True
    assert candidate.bridge_keys["lbx_id"] == "70761487"
    assert candidate.width == 3000
    assert candidate.height == 2000


def test_bravo_candidate_keeps_gallery_item_context() -> None:
    candidate = candidate_from_normalized_record(
        {
            "source": "bravo",
            "source_id": "media-1",
            "source_url": "https://www.bravotv.com/sites/bravo/files/example.jpg",
            "source_page_url": "https://www.bravotv.com/watch-what-happens-live/photos/example#123",
            "caption": "Jane Doe backstage.",
            "raw": {"gallery_item_id": "123", "file_url": "https://www.bravotv.com/sites/bravo/files/example.jpg"},
        }
    )

    assert candidate.source_role == "editorial_context"
    assert candidate.display_eligible is True
    assert candidate.bridge_keys["bravo_gallery_item_id"] == "123"
    assert candidate.bridge_keys["file_url"] == "https://www.bravotv.com/sites/bravo/files/example.jpg"


def test_peacock_candidate_is_official_original_and_display_eligible() -> None:
    candidate = candidate_from_normalized_record(
        {
            "source": "peacock",
            "source_id": "peacock-blog:keyon",
            "source_url": "https://www.peacocktv.com/sites/peacock/files/2026/06/keyon.jpg",
            "source_page_url": "https://www.peacocktv.com/blog/love-island-usa-season-8-cast",
            "caption": "Keyon Love Island USA Season 8 Casa Amor bombshell",
            "people_names": ["Keyon Harry"],
            "width": 1080,
            "height": 1350,
            "raw": {"source_image_id": "peacock-blog:keyon"},
        }
    )

    assert candidate.source_role == "official_original"
    assert candidate.display_eligible is True
    assert candidate.source_asset_id == "peacock-blog:keyon"
    assert candidate.width == 1080
    assert candidate.height == 1350
