from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.media.bravotv import run_service


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


def test_build_asset_payload_preserves_getty_large_and_thumb_metadata() -> None:
    record = {
        **_sample_getty_record(acquisition_status="uploaded"),
        "per_source": {
            "getty": {
                "source_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048x2048&w=gi&k=20&c=full",
                "preview_image_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=1024x1024&w=gi&k=20&c=preview",
                "thumb_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=612x612&w=0&k=20&c=thumb",
                "source_page_url": "https://www.gettyimages.com/detail/news-photo/example/1435767826",
                "getty_original_image_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048x2048&w=gi&k=20&c=full",
                "getty_thumb_clean_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=612x612&w=0&k=20&c=thumb",
                "getty_preview_image_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=1024x1024&w=gi&k=20&c=preview",
            }
        },
        "acquisition": {
            "status": "uploaded",
            "hosted_url": "https://cdn.example.com/full.jpg",
            "hosted_key": "shared-media/full.jpg",
            "hosted_sha256": "sha-full",
            "hosted_content_type": "image/jpeg",
            "hosted_bytes": 1234,
            "hosted_thumb_url": "https://cdn.example.com/thumb.jpg",
            "hosted_thumb_key": "shared-media/thumb.jpg",
            "hosted_thumb_sha256": "sha-thumb",
        },
    }

    payload, preview_only = run_service._build_asset_payload(record, run_id="run-1")

    assert preview_only is False
    assert payload["hosted_url"] == "https://cdn.example.com/full.jpg"
    assert payload["metadata"]["getty_original_image_url"].endswith("c=full")
    assert payload["metadata"]["getty_thumb_clean_url"].endswith("c=thumb")
    assert payload["metadata"]["hosted_thumb_url"] == "https://cdn.example.com/thumb.jpg"


def test_upsert_media_asset_for_import_reuses_existing_asset_on_duplicate_hosted_sha(monkeypatch) -> None:
    db = MagicMock()
    response = MagicMock()
    response.data = [{"id": "existing-asset"}]
    (
        db.schema.return_value.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value
    ) = response
    monkeypatch.setattr(
        run_service,
        "upsert_media_assets",
        MagicMock(
            side_effect=RuntimeError(
                'Supabase error upserting media_assets: duplicate key value violates unique constraint "media_assets_source_hosted_sha_uq"'
            )
        ),
    )

    asset_id, was_upserted = run_service._upsert_media_asset_for_import(
        db,
        {"id": "new-asset", "source": "getty", "hosted_sha256": "sha-1"},
    )

    assert asset_id == "existing-asset"
    assert was_upserted is False


def test_execute_bravotv_image_run_requires_local_getty_prefetch_for_remote_modal(monkeypatch) -> None:
    monkeypatch.setattr(run_service, "execution_backend_canonical", lambda: "modal")
    monkeypatch.setattr(
        run_service,
        "_fetch_person_row",
        lambda _person_id: {"id": "person-1", "full_name": "Jane Doe"},
    )
    create_run_mock = MagicMock()
    monkeypatch.setattr(run_service, "create_run", create_run_mock)

    with pytest.raises(RuntimeError, match="local Getty prefetch"):
        run_service.execute_bravotv_image_run(
            mode="person",
            person_id="person-1",
            sources=["getty"],
        )

    create_run_mock.assert_not_called()


def test_fetch_person_aliases_uses_full_name(monkeypatch) -> None:
    db = MagicMock()
    overrides_response = MagicMock()
    overrides_response.error = None
    overrides_response.data = [{"full_name_override": "J. Doe"}]
    (
        db.schema.return_value.table.return_value.select.return_value.eq.return_value.execute.return_value
    ) = overrides_response

    monkeypatch.setattr(run_service, "create_supabase_admin_client", lambda: db)
    monkeypatch.setattr(
        run_service,
        "_fetch_person_row",
        lambda _person_id: {"id": "person-1", "full_name": "Jane Doe", "external_ids": {}},
    )

    aliases = run_service._fetch_person_aliases("person-1")

    assert aliases == {"jane doe", "j. doe"}


def test_import_catalog_person_mode_skips_non_deterministic_person_links() -> None:
    db = MagicMock()

    with patch("trr_backend.media.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.media.bravotv.run_service._fetch_person_aliases", return_value={"jane doe"}):
            with patch("trr_backend.media.bravotv.run_service.upsert_media_assets") as upsert_mock:
                with patch("trr_backend.media.bravotv.run_service.create_media_link_for_entity") as link_mock:
                    with patch("trr_backend.media.bravotv.run_service.generate_media_asset_variants"):
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

    with patch("trr_backend.media.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.media.bravotv.run_service._fetch_season_map", return_value={15: "season-15"}):
            with patch("trr_backend.media.bravotv.run_service._load_people_index", return_value={"jane doe": []}):
                with patch(
                    "trr_backend.media.bravotv.run_service._match_people",
                    return_value={
                        "resolved": [{"person_id": "person-1"}],
                        "ambiguous": ["John Smith"],
                        "unmatched": [],
                    },
                ):
                    with patch("trr_backend.media.bravotv.run_service.upsert_media_assets"):
                        with patch("trr_backend.media.bravotv.run_service.create_media_link_for_entity") as link_mock:
                            with patch("trr_backend.media.bravotv.run_service.generate_media_asset_variants"):
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

    with patch("trr_backend.media.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.media.bravotv.run_service._fetch_season_map", return_value={6: "season-6"}):
            with patch("trr_backend.media.bravotv.run_service._fetch_episode_slug_map", return_value={}):
                with patch("trr_backend.media.bravotv.run_service.upsert_media_assets") as upsert_mock:
                    with patch("trr_backend.media.bravotv.run_service.create_media_link_for_entity") as link_mock:
                        with patch("trr_backend.media.bravotv.run_service.generate_media_asset_variants") as variants_mock:
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
            "source_asset_id": "fandom-gallery-123",
            "source_url": "https://static.wikia.nocookie.net/real-housewives/images/test.jpg",
            "source_page_url": "https://real-housewives.fandom.com/wiki/Lisa_Barlow/Gallery",
            "hosted_url": "https://cdn.test/fandom/test.jpg",
            "hosted_key": "shared-media/test.jpg",
            "hosted_sha256": "abc123",
            "asset_reused": False,
            "caption": "Season 6 intro card",
            "context_type": "intro",
            "context_section": "Title Cards",
            "episode_id": None,
            "link_targets": ["person:person-1", "show:show-1", "season:season-6"],
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

    with patch("trr_backend.media.bravotv.run_service.create_supabase_admin_client", return_value=db):
        with patch("trr_backend.media.bravotv.run_service._fetch_season_map", return_value={3: "season-3"}):
            with patch(
                "trr_backend.media.bravotv.run_service._fetch_episode_slug_map",
                return_value={(3, "portias-drama-filled-birthday-party"): "episode-3x01"},
            ):
                with patch("trr_backend.media.bravotv.run_service.upsert_media_assets"):
                    with patch("trr_backend.media.bravotv.run_service.create_media_link_for_entity") as link_mock:
                        with patch("trr_backend.media.bravotv.run_service.generate_media_asset_variants"):
                            summary, imported = run_service._import_supplemental_catalog(
                                run_id="run-4",
                                target_person_id="person-1",
                                target_show_id="show-1",
                                supplemental_catalog={"bravo": [bravo_row]},
                            )

    assert summary == {"supplemental_assets_upserted": 1, "supplemental_links_created": 2}
    assert imported[0]["episode_id"] == "episode-3x01"
    assert imported[0]["link_targets"] == ["season:season-3", "episode:episode-3x01"]
    assert [call.kwargs["entity_type"] for call in link_mock.call_args_list] == ["season", "episode"]


def test_build_cast_source_links_export_groups_r2_examples_by_source() -> None:
    export = run_service._build_cast_source_links_export(
        run_id="run-1",
        mode="person",
        person_name="Jane Doe",
        show_name="Summer House",
        imported_records=[
            {
                "media_asset_id": "asset-1",
                "source": "bravo",
                "hosted_url": "https://r2.example/bravo.jpg",
                "source_url": "https://bravo.example/original.jpg",
                "source_page_url": "https://bravo.example/gallery#1",
                "caption": "Jane smiles.",
                "asset_reused": True,
                "link_targets": ["person:person-1"],
            }
        ],
    )

    assert export["run_id"] == "run-1"
    assert export["person"] == "Jane Doe"
    assert export["sources"]["bravo"]["count"] == 1
    assert export["sources"]["bravo"]["r2_count"] == 1
    assert export["sources"]["bravo"]["examples"][0]["r2_url"] == "https://r2.example/bravo.jpg"
    assert export["sources"]["bravo"]["examples"][0]["asset_reused"] is True
