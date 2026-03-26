from __future__ import annotations

import pytest

from trr_backend.repositories import admin_networks_streaming_reads as repo


def test_get_networks_streaming_summary_shapes_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_one",
        lambda query, params=None: {
            "total_available_shows": 20,
            "total_added_shows": 8,
        },
    )
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "type": "network",
                "name": "Bravo",
                "available_show_count": 12,
                "added_show_count": 5,
                "hosted_logo_url": "https://cdn.example.com/bravo.png",
                "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
                "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
                "wikidata_id": "Q123",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                "tmdb_entity_id": "74",
                "homepage_url": "https://www.bravotv.com",
                "resolution_status": "resolved",
                "resolution_reason": None,
                "last_attempt_at": "2026-03-26T00:00:00Z",
            },
            {
                "type": "production",
                "name": "Shed Media",
                "available_show_count": "4",
                "added_show_count": "1",
                "hosted_logo_url": None,
                "hosted_logo_black_url": None,
                "hosted_logo_white_url": None,
                "wikidata_id": None,
                "wikipedia_url": None,
                "tmdb_entity_id": None,
                "homepage_url": "",
                "resolution_status": "manual_required",
                "resolution_reason": "missing_logo",
                "last_attempt_at": None,
            },
        ],
    )

    payload, query_count = repo.get_networks_streaming_summary()

    assert query_count == 2
    assert payload["totals"] == {
        "total_available_shows": 20,
        "total_added_shows": 8,
    }
    assert payload["rows"] == [
        {
            "type": "network",
            "name": "Bravo",
            "available_show_count": 12,
            "added_show_count": 5,
            "hosted_logo_url": "https://cdn.example.com/bravo.png",
            "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
            "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
            "wikidata_id": "Q123",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
            "tmdb_entity_id": "74",
            "homepage_url": "https://www.bravotv.com",
            "resolution_status": "resolved",
            "resolution_reason": None,
            "last_attempt_at": "2026-03-26T00:00:00Z",
            "has_logo": True,
            "has_bw_variants": True,
            "has_links": True,
        },
        {
            "type": "production",
            "name": "Shed Media",
            "available_show_count": 4,
            "added_show_count": 1,
            "hosted_logo_url": None,
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "wikidata_id": None,
            "wikipedia_url": None,
            "tmdb_entity_id": None,
            "homepage_url": None,
            "resolution_status": "manual_required",
            "resolution_reason": "missing_logo",
            "last_attempt_at": None,
            "has_logo": False,
            "has_bw_variants": False,
            "has_links": False,
        },
    ]
    assert payload["generated_at"].endswith("Z")


def test_get_networks_streaming_detail_shapes_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch_one_calls = {"count": 0}

    def fake_fetch_one(query: str, params=None):
        fetch_one_calls["count"] += 1
        if "to_regclass" in query:
            return {"exists": True}
        return {
            "entity_type": "network",
            "name_key": "bravo",
            "display_name": "Bravo",
            "entity_slug": "bravo",
            "available_show_count": 12,
            "added_show_count": 5,
            "core_entity_id": "74",
            "core_origin_country": "US",
            "core_display_priority": None,
            "core_tmdb_logo_path": "/logo.png",
            "core_logo_path": None,
            "core_hosted_logo_key": "logos/bravo.png",
            "core_hosted_logo_url": "https://cdn.example.com/bravo.png",
            "core_hosted_logo_black_url": None,
            "core_hosted_logo_white_url": None,
            "core_wikidata_id": "Q123",
            "core_wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
            "core_wikimedia_logo_file": None,
            "core_link_enriched_at": "2026-03-26T00:00:00Z",
            "core_link_enrichment_source": "wikidata",
            "core_facebook_id": None,
            "core_instagram_id": None,
            "core_twitter_id": None,
            "core_tiktok_id": None,
            "override_id": None,
            "display_name_override": None,
            "wikidata_id_override": None,
            "wikipedia_url_override": None,
            "logo_source_urls_override": [],
            "source_priority_override": [],
            "aliases_override": [],
            "override_notes": None,
            "override_is_active": False,
            "override_updated_by": None,
            "override_updated_at": None,
            "completion_resolution_status": "resolved",
            "completion_resolution_reason": None,
            "completion_last_attempt_at": None,
        }

    def fake_fetch_all(query: str, params=None):
        if "network_streaming_logo_assets" in query:
            return [
                {
                    "id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.org/logo.png",
                    "source_rank": 1,
                    "hosted_logo_url": "https://cdn.example.com/bravo.png",
                    "hosted_logo_content_type": "image/png",
                    "base_logo_format": "png",
                    "pixel_width": 400,
                    "pixel_height": 200,
                    "mirror_status": "mirrored",
                    "failure_reason": None,
                    "is_primary": True,
                    "updated_at": "2026-03-26T00:00:00Z",
                }
            ]
        return [
            {
                "trr_show_id": "show-1",
                "show_name": "Bravo Show",
                "canonical_slug": "bravo-show",
                "poster_url": "https://cdn.example.com/poster.jpg",
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_networks_streaming_detail(entity_type="network", entity_key="bravo")

    assert query_count == 4
    assert payload is not None
    assert payload["entity_key"] == "bravo"
    assert payload["core"]["entity_id"] == "74"
    assert payload["logo_assets"][0]["mirror_status"] == "mirrored"
    assert payload["shows"][0]["canonical_slug"] == "bravo-show"


def test_get_networks_streaming_suggestions_scores_slug_and_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "entity_type": "network",
                "name": "Bravo",
                "entity_slug": "bravo",
                "available_show_count": "10",
                "added_show_count": "5",
            }
        ],
    )

    payload, query_count = repo.get_networks_streaming_suggestions(entity_type="network", entity_slug="bravo")

    assert query_count == 1
    assert payload == [
        {
            "entity_type": "network",
            "name": "Bravo",
            "entity_slug": "bravo",
            "available_show_count": 10,
            "added_show_count": 5,
        }
    ]
