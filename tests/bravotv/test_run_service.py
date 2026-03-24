from __future__ import annotations

from unittest.mock import MagicMock, patch

from trr_backend.bravotv import run_service


def _sample_getty_record(*, acquisition_status: str = "referenced_only") -> dict[str, object]:
    return {
        "id": "group-1",
        "getty_editorial_id": "928663262",
        "caption": "Pictured: Jane Doe",
        "show_name": "Watch What Happens Live",
        "season_number": 15,
        "persons_pictured": ["Jane Doe"],
        "per_source": {
            "getty": {
                "source_url": "https://media.gettyimages.com/photos/example-preview.jpg",
                "source_page_url": "https://www.gettyimages.com/detail/news-photo/example",
                "width": 3000,
                "height": 2000,
            }
        },
        "acquisition": {
            "status": acquisition_status,
            "source_url": "https://media.gettyimages.com/photos/example-preview.jpg",
            "google_reverse_image_search_url": "https://www.google.com/searchbyimage?image_url=https://media.gettyimages.com/photos/example-preview.jpg",
        },
    }


def test_build_asset_payload_marks_preview_records_for_replacement() -> None:
    payload, preview_only = run_service._build_asset_payload(
        _sample_getty_record(),
        run_id="run-1",
    )

    assert preview_only is True
    assert payload["source"] == "getty"
    assert payload["hosted_url"] == "https://media.gettyimages.com/photos/example-preview.jpg"
    assert payload["metadata"]["run_id"] == "run-1"
    assert payload["metadata"]["replacement_pending"] is True
    assert payload["metadata"]["google_reverse_image_search_url"].startswith("https://www.google.com/searchbyimage")


def test_import_catalog_person_mode_skips_non_deterministic_person_links() -> None:
    db = MagicMock()

    with patch("trr_backend.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.bravotv.run_service._fetch_person_aliases", return_value={"jane doe"}):
            with patch("trr_backend.bravotv.run_service.upsert_media_assets") as upsert_mock:
                with patch("trr_backend.bravotv.run_service.create_media_link_for_entity") as link_mock:
                    with patch("trr_backend.bravotv.run_service.generate_media_asset_variants"):
                        summary, imported, review, replacement = run_service._import_catalog(
                            mode="person",
                            run_id="run-1",
                            target_show_id=None,
                            target_person_id="person-1",
                            merged_catalog=[
                                {
                                    **_sample_getty_record(),
                                    "persons_pictured": ["Someone Else"],
                                }
                            ],
                        )

    assert upsert_mock.call_count == 1
    assert link_mock.call_count == 0
    assert summary["assets_upserted"] == 1
    assert summary["links_created"] == 0
    assert imported[0]["replacement_pending"] is True
    assert review == [
        {
            "group_id": "group-1",
            "reason": "target_person_not_deterministic",
            "persons_pictured": ["Someone Else"],
            "caption": "Pictured: Jane Doe",
            "show_name": "Watch What Happens Live",
        }
    ]
    assert replacement[0]["google_reverse_image_search_url"].startswith("https://www.google.com/searchbyimage")


def test_import_catalog_show_mode_links_resolved_people_and_flags_unresolved_names() -> None:
    db = MagicMock()

    with patch("trr_backend.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.bravotv.run_service._fetch_season_map", return_value={15: "season-15"}):
            with patch("trr_backend.bravotv.run_service._load_people_index", return_value={"jane doe": []}):
                with patch(
                    "trr_backend.bravotv.run_service._match_people",
                    return_value={
                        "resolved": [{"person_id": "person-1"}],
                        "ambiguous": ["John Smith"],
                        "unmatched": [],
                    },
                ):
                    with patch("trr_backend.bravotv.run_service.upsert_media_assets"):
                        with patch("trr_backend.bravotv.run_service.create_media_link_for_entity") as link_mock:
                            with patch("trr_backend.bravotv.run_service.generate_media_asset_variants"):
                                summary, imported, review, replacement = run_service._import_catalog(
                                    mode="show",
                                    run_id="run-2",
                                    target_show_id="show-1",
                                    target_person_id=None,
                                    merged_catalog=[_sample_getty_record(acquisition_status="uploaded")],
                                )

    assert summary["assets_upserted"] == 1
    assert summary["links_created"] == 3
    assert imported[0]["link_targets"] == ["show:show-1", "season:season-15", "person:person-1"]
    assert review == [
        {
            "group_id": "group-1",
            "reason": "person_assignment_needs_review",
            "persons_pictured": ["Jane Doe"],
            "unresolved_names": ["John Smith"],
            "caption": "Pictured: Jane Doe",
            "show_name": "Watch What Happens Live",
        }
    ]
    assert replacement == []
    assert link_mock.call_count == 3


def test_import_supplemental_catalog_preserves_fandom_context_and_generates_variants() -> None:
    db = MagicMock()

    fandom_row = {
        "source_image_id": "fandom-gallery-123",
        "image_url": "https://static.wikia.nocookie.net/real-housewives/images/test.jpg",
        "caption": "Season 6 intro card",
        "position": 1,
        "season": 6,
        "context_type": "intro",
        "hosted": {
            "hosted_url": "https://cdn.test/fandom/test.jpg",
            "hosted_key": "shared-media/test.jpg",
            "hosted_sha256": "abc123",
            "hosted_content_type": "image/jpeg",
            "hosted_bytes": 1234,
        },
        "metadata": {
            "content_type": "INTRO",
            "fandom_section_label": "Title Cards",
            "source_variant": "fandom_gallery",
            "show_name": "The Real Housewives of Salt Lake City",
            "season_number": 6,
            "source_page_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
        },
    }

    with patch("trr_backend.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.bravotv.run_service._fetch_season_map", return_value={6: "season-6"}):
            with patch("trr_backend.bravotv.run_service._fetch_episode_slug_map", return_value={}):
                with patch("trr_backend.bravotv.run_service.upsert_media_assets") as upsert_mock:
                    with patch("trr_backend.bravotv.run_service.create_media_link_for_entity") as link_mock:
                        with patch("trr_backend.bravotv.run_service.generate_media_asset_variants") as variants_mock:
                            summary, imported = run_service._import_supplemental_catalog(
                                run_id="run-3",
                                target_person_id="person-1",
                                target_show_id="show-1",
                                supplemental_catalog={"fandom": [fandom_row]},
                            )

    assert summary == {"supplemental_assets_upserted": 1, "supplemental_links_created": 3}
    assert imported == [
        {
            "media_asset_id": imported[0]["media_asset_id"],
            "source": "fandom",
            "caption": "Season 6 intro card",
            "context_type": "intro",
            "context_section": "Title Cards",
            "episode_id": None,
            "supplemental": True,
        }
    ]
    variants_mock.assert_called_once()
    upsert_payload = upsert_mock.call_args.args[1][0]
    assert upsert_payload["metadata"]["content_type"] == "INTRO"
    assert upsert_payload["metadata"]["fandom_section_tag"] == "INTRO"
    assert upsert_payload["metadata"]["fandom_section_label"] == "Title Cards"
    assert link_mock.call_count == 3
    first_context = link_mock.call_args_list[0].kwargs["context"]
    assert first_context["context_type"] == "intro"
    assert first_context["context_section"] == "Title Cards"


def test_import_supplemental_catalog_respects_person_show_link_flags_and_resolves_episode() -> None:
    db = MagicMock()

    bravo_row = {
        "source_image_id": "bravo-gallery-1",
        "image_url": "https://cdn.test/bravo/test.jpg",
        "caption": "Kyle and Adrienne head out to find the proper gift for young Portia.",
        "position": 2,
        "season": 3,
        "context_type": "bravotv_gallery",
        "context_section": "Portia's Drama Filled Birthday Party",
        "link_person": False,
        "link_show": False,
        "link_season": True,
        "link_episode": True,
        "hosted": {
            "hosted_url": "https://cdn.test/bravo/hosted.jpg",
            "hosted_key": "shared-media/bravo.jpg",
            "hosted_sha256": "def456",
            "hosted_content_type": "image/jpeg",
            "hosted_bytes": 4321,
        },
        "metadata": {
            "source_variant": "bravotv_gallery",
            "show_name": "The Real Housewives of Beverly Hills",
            "season_number": 3,
            "episode_slug": "portias-drama-filled-birthday-party",
            "page_title": "Portia's Drama Filled Birthday Party",
            "source_page_url": (
                "https://www.bravotv.com/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party#9799496"
            ),
        },
    }

    with patch("trr_backend.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.bravotv.run_service._fetch_season_map", return_value={3: "season-3"}):
            with patch(
                "trr_backend.bravotv.run_service._fetch_episode_slug_map",
                return_value={(3, "portias-drama-filled-birthday-party"): "episode-3x01"},
            ):
                with patch("trr_backend.bravotv.run_service.upsert_media_assets"):
                    with patch("trr_backend.bravotv.run_service.create_media_link_for_entity") as link_mock:
                        with patch("trr_backend.bravotv.run_service.generate_media_asset_variants"):
                            summary, imported = run_service._import_supplemental_catalog(
                                run_id="run-4",
                                target_person_id="person-1",
                                target_show_id="show-1",
                                supplemental_catalog={"bravo": [bravo_row]},
                            )

    assert summary == {"supplemental_assets_upserted": 1, "supplemental_links_created": 2}
    assert imported[0]["episode_id"] == "episode-3x01"
    assert [call.kwargs["entity_type"] for call in link_mock.call_args_list] == ["season", "episode"]
