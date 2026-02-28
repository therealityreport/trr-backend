from __future__ import annotations

from trr_backend.ingestion.cast_photo_sources import fetch_imdb_cast_photos


def test_fetch_imdb_cast_photos_sets_imdb_metadata_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm123456",
                    "viewer_id": "rm123456",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm123456/",
                    "url": "https://m.media-amazon.com/images/M/MV5BBASE._UX640_.jpg",
                    "url_path": "/images/M/MV5BBASE._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                }
            ],
            {"has_next_page": False, "end_cursor": None, "total": 1},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "url": "https://m.media-amazon.com/images/M/MV5BDETAIL._V1_.jpg",
            "url_path": "/images/M/MV5BDETAIL._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Andy Cohen in Watch What Happens Live (2022)",
            "gallery_index": 2,
            "gallery_total": 45,
            "people_imdb_ids": ["nm0000001"],
            "people_names": ["Andy Cohen"],
            "title_imdb_ids": ["tt0000001"],
            "title_names": ["Watch What Happens Live (2022)"],
        },
    )

    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0000001",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
    )

    assert len(rows) == 1
    row = rows[0]
    metadata = row.get("metadata") or {}
    assert metadata["source_variant"] == "imdb_person_gallery"
    assert metadata["source_logo"] == "IMDb"
    assert metadata["source_page_url"] == "https://www.imdb.com/name/nm0000001/mediaviewer/rm123456/"
    assert metadata["source_file_url"] == "https://m.media-amazon.com/images/M/MV5BDETAIL._V1_.jpg"
    assert metadata["source_page_title"] == "Watch What Happens Live (2022)"
    assert metadata["asset_name"] == "Watch What Happens Live (2022)"
    assert metadata["name"] == "Watch What Happens Live (2022)"
    assert row["people_names"] == ["Andy Cohen"]
    assert row["title_names"] == ["Watch What Happens Live (2022)"]


def test_fetch_imdb_cast_photos_filters_titles_and_prioritizes_solo_people(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm111",
                    "viewer_id": "rm111",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm111/",
                    "url": "https://m.media-amazon.com/images/M/MV5B111._UX640_.jpg",
                    "url_path": "/images/M/MV5B111._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                },
                {
                    "source_image_id": "rm222",
                    "viewer_id": "rm222",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm222/",
                    "url": "https://m.media-amazon.com/images/M/MV5B222._UX640_.jpg",
                    "url_path": "/images/M/MV5B222._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                },
                {
                    "source_image_id": "rm333",
                    "viewer_id": "rm333",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm333/",
                    "url": "https://m.media-amazon.com/images/M/MV5B333._UX640_.jpg",
                    "url_path": "/images/M/MV5B333._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                },
                {
                    "source_image_id": "rm444",
                    "viewer_id": "rm444",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm444/",
                    "url": "https://m.media-amazon.com/images/M/MV5B444._UX640_.jpg",
                    "url_path": "/images/M/MV5B444._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                },
            ],
            {"has_next_page": False, "end_cursor": None, "total": 4},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )

    details_by_viewer = {
        "rm111": {
            "url": "https://m.media-amazon.com/images/M/MV5B111._V1_.jpg",
            "url_path": "/images/M/MV5B111._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming and Darren Criss in Episode Name (2024)",
            "gallery_index": 1,
            "gallery_total": 40,
            "people_imdb_ids": ["nm0001086", "nm0149186"],
            "people_names": ["Alan Cumming", "Darren Criss"],
            "title_imdb_ids": ["ttALLOWED01"],
            "title_names": ["Episode Name"],
        },
        "rm222": {
            "url": "https://m.media-amazon.com/images/M/MV5B222._V1_.jpg",
            "url_path": "/images/M/MV5B222._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming in The Traitors (2024)",
            "gallery_index": 2,
            "gallery_total": 40,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["ttOTHER01"],
            "title_names": ["Episode Two"],
        },
        "rm333": {
            "url": "https://m.media-amazon.com/images/M/MV5B333._V1_.jpg",
            "url_path": "/images/M/MV5B333._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming in Watch What Happens Live (2024)",
            "gallery_index": 3,
            "gallery_total": 40,
            "people_imdb_ids": ["nm0001086", "nm1111111", "nm2222222"],
            "people_names": ["Alan Cumming", "Guest A", "Guest B"],
            "title_imdb_ids": ["ttOTHER02"],
            "title_names": ["WWHL Episode"],
        },
        "rm444": {
            "url": "https://m.media-amazon.com/images/M/MV5B444._V1_.jpg",
            "url_path": "/images/M/MV5B444._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming on red carpet",
            "gallery_index": 4,
            "gallery_total": 40,
            "people_imdb_ids": ["nm0001086", "nm3333333"],
            "people_names": ["Alan Cumming", "Someone Else"],
            "title_imdb_ids": ["ttNOTMATCH"],
            "title_names": ["Premiere Night"],
        },
    }

    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: details_by_viewer[str(viewer_id)],
    )

    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0000001",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=2,
        allowed_title_imdb_ids={"ttallowed01"},
        allowed_title_keywords=["traitors", "watch what happens live", "wwhl"],
        prioritize_solo_people=True,
    )

    assert len(rows) == 2
    assert [row["source_image_id"] for row in rows] == ["rm222", "rm111"]


def test_fetch_imdb_cast_photos_expands_pagination_for_show_filters(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm100",
                    "viewer_id": "rm100",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm100/",
                    "url": "https://m.media-amazon.com/images/M/MV5B100._UX640_.jpg",
                    "url_path": "/images/M/MV5B100._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                }
            ],
            {"has_next_page": True, "end_cursor": "cursor-1", "total": 120},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_page",
        lambda imdb_person_id, after_cursor, first=50, session=None: {"mock_cursor": after_cursor},
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_payload",
        lambda payload, imdb_person_id: (
            [
                {
                    "source_image_id": "rm200",
                    "viewer_id": "rm200",
                    "mediaviewer_url_path": "/name/nm0000001/mediaviewer/rm200/",
                    "url": "https://m.media-amazon.com/images/M/MV5B200._UX640_.jpg",
                    "url_path": "/images/M/MV5B200._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "caption": "Episode-only title",
                }
            ],
            {"has_next_page": False, "end_cursor": None, "total": 120},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "url": f"https://m.media-amazon.com/images/M/MV5B{str(viewer_id)[2:]}._V1_.jpg",
            "url_path": f"/images/M/MV5B{str(viewer_id)[2:]}._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "The Traitors still" if viewer_id == "rm200" else "Unrelated still",
            "gallery_index": 1,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["ttTRAITORS01"] if viewer_id == "rm200" else ["ttNOTMATCH"],
            "title_names": ["The Traitors"] if viewer_id == "rm200" else ["Other Title"],
        },
    )

    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0000001",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
        allowed_title_imdb_ids={"tttraitors01"},
        allowed_title_keywords=["traitors", "watch what happens live", "wwhl"],
        prioritize_solo_people=True,
    )

    assert len(rows) == 1
    assert rows[0]["source_image_id"] == "rm200"


def test_fetch_imdb_cast_photos_applies_traitors_strict_filters_and_ranking(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm_solo",
                    "viewer_id": "rm_solo",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_solo/",
                    "url": "https://m.media-amazon.com/images/M/MV5BSOLO._UX640_.jpg",
                    "url_path": "/images/M/MV5BSOLO._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "event",
                },
                {
                    "source_image_id": "rm_cast_group",
                    "viewer_id": "rm_cast_group",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_cast_group/",
                    "url": "https://m.media-amazon.com/images/M/MV5BGROUP._UX640_.jpg",
                    "url_path": "/images/M/MV5BGROUP._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "event",
                },
                {
                    "source_image_id": "rm_episode",
                    "viewer_id": "rm_episode",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_episode/",
                    "url": "https://m.media-amazon.com/images/M/MV5BEPISODE._UX640_.jpg",
                    "url_path": "/images/M/MV5BEPISODE._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "still_frame",
                },
                {
                    "source_image_id": "rm_wrong_type",
                    "viewer_id": "rm_wrong_type",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_wrong_type/",
                    "url": "https://m.media-amazon.com/images/M/MV5BWRONGTYPE._UX640_.jpg",
                    "url_path": "/images/M/MV5BWRONGTYPE._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "poster",
                },
                {
                    "source_image_id": "rm_non_cast",
                    "viewer_id": "rm_non_cast",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_non_cast/",
                    "url": "https://m.media-amazon.com/images/M/MV5BNONCAST._UX640_.jpg",
                    "url_path": "/images/M/MV5BNONCAST._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "event",
                },
                {
                    "source_image_id": "rm_still_not_episode",
                    "viewer_id": "rm_still_not_episode",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_still_not_episode/",
                    "url": "https://m.media-amazon.com/images/M/MV5BNOMATCH._UX640_.jpg",
                    "url_path": "/images/M/MV5BNOMATCH._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "still_frame",
                },
            ],
            {"has_next_page": False, "end_cursor": None, "total": 6},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    details_by_viewer = {
        "rm_solo": {
            "url": "https://m.media-amazon.com/images/M/MV5BSOLO._V1_.jpg",
            "url_path": "/images/M/MV5BSOLO._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming solo event",
            "gallery_index": 10,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["tt1111111"],
            "title_names": ["Event"],
        },
        "rm_cast_group": {
            "url": "https://m.media-amazon.com/images/M/MV5BGROUP._V1_.jpg",
            "url_path": "/images/M/MV5BGROUP._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming and traitors cast",
            "gallery_index": 20,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086", "nmCAST01"],
            "people_names": ["Alan Cumming", "Traitors Cast One"],
            "title_imdb_ids": ["tt2222222"],
            "title_names": ["Premiere"],
        },
        "rm_episode": {
            "url": "https://m.media-amazon.com/images/M/MV5BEPISODE._V1_.jpg",
            "url_path": "/images/M/MV5BEPISODE._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Episode still",
            "gallery_index": 30,
            "gallery_total": 120,
            "people_imdb_ids": [],
            "people_names": [],
            "title_imdb_ids": ["ttEPMATCH1"],
            "title_names": ["Episode Match"],
        },
        "rm_wrong_type": {
            "url": "https://m.media-amazon.com/images/M/MV5BWRONGTYPE._V1_.jpg",
            "url_path": "/images/M/MV5BWRONGTYPE._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Poster",
            "gallery_index": 40,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["tt3333333"],
            "title_names": ["Poster Title"],
        },
        "rm_non_cast": {
            "url": "https://m.media-amazon.com/images/M/MV5BNONCAST._V1_.jpg",
            "url_path": "/images/M/MV5BNONCAST._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Non cast group",
            "gallery_index": 50,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086", "nmOUTSIDER"],
            "people_names": ["Alan Cumming", "Outsider"],
            "title_imdb_ids": ["tt4444444"],
            "title_names": ["Other Event"],
        },
        "rm_still_not_episode": {
            "url": "https://m.media-amazon.com/images/M/MV5BNOMATCH._V1_.jpg",
            "url_path": "/images/M/MV5BNOMATCH._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Still not matching episode",
            "gallery_index": 60,
            "gallery_total": 120,
            "people_imdb_ids": [],
            "people_names": [],
            "title_imdb_ids": ["ttNOTEP"],
            "title_names": ["Non Match Episode"],
        },
    }
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: details_by_viewer[str(viewer_id)],
    )

    diagnostics: dict[str, int] = {}
    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0001086",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
        strict_types={"event", "still_frame"},
        target_person_imdb_id="nm0001086",
        target_person_name="Alan Cumming",
        allowed_cast_imdb_ids={"nmCAST01"},
        allowed_cast_names={"Traitors Cast One"},
        allowed_episode_imdb_ids={"ttEPMATCH1"},
        strict_mode_enabled=True,
        imdb_diagnostics=diagnostics,
    )

    assert [row["source_image_id"] for row in rows] == ["rm_solo", "rm_cast_group", "rm_episode"]
    reasons = [row["metadata"]["imdb_filter_reason"] for row in rows]
    assert reasons == ["solo_self", "traitors_cast_group", "episode_still_frame"]
    for row in rows:
        assert row["metadata"]["imdb_filter_scope"] == "traitors_strict"
    assert diagnostics["imdb_pages_scanned"] == 1
    assert diagnostics["imdb_candidates_seen"] == 6
    assert diagnostics["imdb_kept"] == 3
    assert diagnostics["imdb_filtered_type"] == 1
    assert diagnostics["imdb_filtered_people"] == 1
    assert diagnostics["imdb_filtered_episode"] == 1
    assert diagnostics["imdb_filtered_other"] == 0


def test_fetch_imdb_cast_photos_expands_pagination_in_strict_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm100",
                    "viewer_id": "rm100",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm100/",
                    "url": "https://m.media-amazon.com/images/M/MV5B100._UX640_.jpg",
                    "url_path": "/images/M/MV5B100._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "poster",
                }
            ],
            {"has_next_page": True, "end_cursor": "cursor-1", "total": 120},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_page",
        lambda imdb_person_id, after_cursor, first=50, session=None: {"cursor": after_cursor},
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_payload",
        lambda payload, imdb_person_id: (
            [
                {
                    "source_image_id": "rm200",
                    "viewer_id": "rm200",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm200/",
                    "url": "https://m.media-amazon.com/images/M/MV5B200._UX640_.jpg",
                    "url_path": "/images/M/MV5B200._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                    "image_type": "event",
                }
            ],
            {"has_next_page": False, "end_cursor": None, "total": 120},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "url": f"https://m.media-amazon.com/images/M/MV5B{str(viewer_id)[2:]}._V1_.jpg",
            "url_path": f"/images/M/MV5B{str(viewer_id)[2:]}._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming image",
            "gallery_index": 1,
            "gallery_total": 120,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["tt1000"],
            "title_names": ["The Traitors"],
        },
    )

    diagnostics: dict[str, int] = {}
    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0001086",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
        strict_types={"event", "still_frame"},
        target_person_imdb_id="nm0001086",
        target_person_name="Alan Cumming",
        allowed_cast_imdb_ids={"nm0001086"},
        allowed_cast_names={"Alan Cumming"},
        allowed_episode_imdb_ids={"ttEP1"},
        strict_mode_enabled=True,
        imdb_diagnostics=diagnostics,
    )

    assert len(rows) == 1
    assert rows[0]["source_image_id"] == "rm200"
    assert diagnostics["imdb_pages_scanned"] == 2
    assert diagnostics["imdb_candidates_seen"] == 2
    assert diagnostics["imdb_kept"] == 1
    assert diagnostics["imdb_filtered_type"] == 1


def test_fetch_imdb_cast_photos_uses_mediaviewer_image_type_fallback_for_strict_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaindex_html",
        lambda imdb_person_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaindex_state",
        lambda html, imdb_person_id: (
            [
                {
                    "source_image_id": "rm_fallback_type",
                    "viewer_id": "rm_fallback_type",
                    "mediaviewer_url_path": "/name/nm0001086/mediaviewer/rm_fallback_type/",
                    "url": "https://m.media-amazon.com/images/M/MV5BFALLBACK._UX640_.jpg",
                    "url_path": "/images/M/MV5BFALLBACK._UX640_.jpg",
                    "width": 640,
                    "height": 360,
                }
            ],
            {"has_next_page": False, "end_cursor": None, "total": 1},
        ),
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.fetch_imdb_person_mediaviewer_html",
        lambda imdb_person_id, viewer_id, session=None: "<html></html>",
    )
    monkeypatch.setattr(
        "trr_backend.integrations.imdb.person_gallery.parse_imdb_person_mediaviewer_details",
        lambda html, viewer_id=None: {
            "url": "https://m.media-amazon.com/images/M/MV5BFALLBACK._V1_.jpg",
            "url_path": "/images/M/MV5BFALLBACK._V1_.jpg",
            "width": 1920,
            "height": 1080,
            "caption": "Alan Cumming in fallback image type sample",
            "gallery_index": 1,
            "gallery_total": 1,
            "people_imdb_ids": ["nm0001086"],
            "people_names": ["Alan Cumming"],
            "title_imdb_ids": ["tt123"],
            "title_names": ["The Traitors"],
            "image_type": "event",
        },
    )

    diagnostics: dict[str, int] = {}
    rows = fetch_imdb_cast_photos(
        imdb_person_id="nm0001086",
        person_id="00000000-0000-0000-0000-000000000001",
        limit=10,
        strict_types={"event", "still_frame"},
        target_person_imdb_id="nm0001086",
        target_person_name="Alan Cumming",
        allowed_cast_imdb_ids={"nm0001086"},
        allowed_cast_names={"Alan Cumming"},
        strict_mode_enabled=True,
        imdb_diagnostics=diagnostics,
    )

    assert len(rows) == 1
    assert rows[0]["metadata"]["imdb_image_type"] == "event"
    assert rows[0]["metadata"]["imdb_filter_reason"] == "solo_self"
    assert diagnostics["imdb_kept"] == 1
    assert diagnostics["imdb_filtered_type"] == 0
