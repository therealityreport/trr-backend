from __future__ import annotations

import types

from trr_backend.media.bravotv.get_images_pipeline import (
    _extract_bravo_image_people_names,
    _extract_people_from_text,
    _collect_getty_person,
    _normalize_external_ids,
    _normalize_getty_record,
    _normalize_nup_key,
    _refreshed_artifacts,
    _selected_source_families,
    _split_caption_people,
    acquire_best_image,
    build_bridge_and_catalog,
    run_get_images_pipeline,
)
from trr_backend.media.bravotv import get_images_pipeline
from trr_backend.integrations.bravo_jsonapi import extract_gallery_assets_from_html


def test_selected_source_families_expand_all_for_person_mode() -> None:
    assert _selected_source_families(["all"], mode="person") == ["getty", "imdb", "tmdb", "fandom"]
    assert _refreshed_artifacts(["getty"], mode="person") == ["getty", "nbcumv", "bravo"]
    assert _refreshed_artifacts(["fandom"], mode="person") == ["fandom"]


def test_normalize_nup_key_normalizes_zero_padded_frames() -> None:
    assert _normalize_nup_key("NUP_181952_0005.JPG") == "NUP_181952_5"
    assert _normalize_nup_key("nup_181952_5.jpg") == "NUP_181952_5"


def test_split_caption_people_parses_nbcu_style_caption() -> None:
    caption = 'WATCH WHAT HAPPENS LIVE -- "Brandi" -- Pictured: (l-r) Andy Cohen, Brandi Glanville, Adam Rippon --'
    assert _split_caption_people(caption) == ["Andy Cohen", "Brandi Glanville", "Adam Rippon"]


def test_extract_people_from_text_matches_known_people_in_editorial_caption() -> None:
    people = _extract_people_from_text(
        "Brandi gets lifted by Adam Rippon and Andy Cohen!",
        known_people=["Brandi Glanville", "Adam Rippon", "Andy Cohen"],
    )
    assert people == ["Adam Rippon", "Andy Cohen"]


def test_extract_bravo_image_people_names_does_not_fall_back_to_gallery_cast() -> None:
    people = _extract_bravo_image_people_names(
        {
            "field_caption": "Kyle continues tho struggle with her sister, Kim.",
            "field_media_image_alt": "Kyle and Kim",
            "gallery_people_names": ["Brandi Glanville", "Kyle Richards", "Kim Richards"],
        },
        known_people=["Brandi Glanville", "Kyle Richards", "Kim Richards"],
    )
    assert people == ["Kyle Richards", "Kim Richards"]


def test_collect_bravo_person_filters_show_before_limit(monkeypatch) -> None:
    monkeypatch.setattr(get_images_pipeline, "find_person_uuid", lambda *_args, **_kwargs: "person-1")
    monkeypatch.setattr(
        get_images_pipeline,
        "fetch_person_galleries",
        lambda *_args, **_kwargs: [{"title": "Other"}, {"title": "Summer House"}],
    )

    def fake_fetch_gallery_assets(gallery, **_kwargs):
        if gallery["title"] == "Other":
            return [
                {"id": f"other-{index}", "gallery_show_name": "Watch What Happens Live", "gallery_title": "Other"}
                for index in range(10)
            ]
        return [{"id": "summer-1", "gallery_show_name": "Summer House", "gallery_title": "Summer House photos"}]

    monkeypatch.setattr(get_images_pipeline, "fetch_gallery_assets", fake_fetch_gallery_assets)

    rows = get_images_pipeline._collect_bravo_person("Kyle Cooke", limit=10, show_name="Summer House")

    assert rows == [
        {
            "id": "summer-1",
            "gallery_show_name": "Summer House",
            "gallery_title": "Summer House photos",
            "bravotv_collection_branch": "media_gallery",
        }
    ]


def test_collect_bravo_person_uses_person_image_fallback_when_gallery_tags_are_empty(monkeypatch) -> None:
    monkeypatch.setattr(get_images_pipeline, "find_person_uuid", lambda *_args, **_kwargs: "person-1")
    monkeypatch.setattr(get_images_pipeline, "fetch_person_galleries", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(get_images_pipeline, "find_show_node", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        get_images_pipeline,
        "fetch_person_image_assets",
        lambda *_args, **_kwargs: [
            {
                "file_name": "ciara.jpg",
                "file_url": "https://www.bravotv.com/sites/bravo/files/2025/12/ciara.jpg",
                "gallery_title": "Ciara Miller profile images",
                "bravotv_person_image_field": "field_person_cover_photo",
            }
        ],
    )

    rows = get_images_pipeline._collect_bravo_person("Ciara Miller", limit=10, show_name="Summer House")

    assert rows == [
        {
            "file_name": "ciara.jpg",
            "file_url": "https://www.bravotv.com/sites/bravo/files/2025/12/ciara.jpg",
            "gallery_title": "Ciara Miller profile images",
            "bravotv_person_image_field": "field_person_cover_photo",
            "bravotv_collection_branch": "person_image",
        }
    ]


def test_bravo_row_matches_person_from_newer_profile_filename() -> None:
    assert get_images_pipeline._bravo_row_matches_person(
        {
            "file_name": "ciara.jpg",
            "file_url": "https://www.bravotv.com/sites/bravo/files/2025/12/ciara.jpg",
            "bravotv_person_image_field": "field_person_cover_photo",
        },
        "Ciara Miller",
    )
    assert get_images_pipeline._bravo_row_matches_person(
        {
            "file_name": "ciara-miller-summer-house.jpg",
            "field_caption": "",
            "gallery_title": "Summer House Season 10 Cast Photos",
        },
        "Ciara Miller",
    )


def test_collect_nbcumv_person_falls_back_to_show_name_text_when_show_id_caption_misses(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        get_images_pipeline.nbcumv,
        "resolve_show_by_title",
        lambda title: {"id": "show-uuid", "title": title},
    )

    def fake_search_images(filters, **_kwargs):
        calls.append(
            {
                "show_id": filters.show_id,
                "show_name": filters.show_name,
                "search_text": filters.search_text,
                "search_caption": filters.search_caption,
            }
        )
        if filters.show_id and filters.search_caption:
            return []
        if filters.show_name and filters.search_text == "Ciara Miller":
            return [
                {
                    "lbx_id": "1",
                    "lbx_filename": "NUP_1_0001.JPG",
                    "lbx_showTitle": "Summer House",
                    "lbx_caption": "Pictured: Ciara Miller",
                }
            ]
        return []

    monkeypatch.setattr(get_images_pipeline.nbcumv, "search_images", fake_search_images)
    diagnostics: list[dict[str, object]] = []

    rows = get_images_pipeline._collect_nbcumv_person(
        "Ciara Miller",
        show_name="Summer House",
        limit=10,
        diagnostics=diagnostics,
    )

    assert [row["lbx_id"] for row in rows] == ["1"]
    assert rows[0]["nbcumv_query_branch"] == "search_show_name_person_text"
    assert rows[0]["nbcumv_query_rank"] == 0
    assert calls[0]["show_id"] == "show-uuid"
    assert calls[1]["show_name"] == "Summer House"
    assert [item["stage"] for item in diagnostics if item.get("source") == "nbcumv"][:3] == [
        "resolve_show_by_title",
        "search_show_id_caption_probe",
        "search_show_name_person_text",
    ]


def test_collect_getty_person_runs_bravo_then_name_without_overlap(monkeypatch) -> None:
    calls: list[tuple[str, int, dict[str, str] | None]] = []

    def fake_search(phrase, *, limit=None, query_params=None, **_kwargs):
        calls.append((phrase, limit, query_params))
        if phrase == "Kyle Cooke":
            return [
                {"editorial_id": "2", "object_name": "name-2.jpg"},
                {"editorial_id": "3", "object_name": "bravo-3.jpg"},
                {"editorial_id": "4", "object_name": "name-4.jpg"},
                {"editorial_id": "5", "object_name": "name-5.jpg"},
            ]
        if phrase == "Kyle Cooke Bravo":
            return [
                {"editorial_id": "2", "object_name": "name-2.jpg"},
                {"editorial_id": "3", "object_name": "bravo-3.jpg"},
            ]
        return []

    monkeypatch.setattr(get_images_pipeline.getty, "search_editorial_assets", fake_search)
    summaries: list[dict[str, object]] = []

    rows = _collect_getty_person("Kyle Cooke", limit=2, query_summaries=summaries)

    assert [row["editorial_id"] for row in rows] == ["2", "3", "4", "5"]
    assert calls == [
        ("Kyle Cooke Bravo", 2, {"sort": "newest"}),
        ("Kyle Cooke", 4, {"sort": "newest"}),
    ]
    assert [summary["phrase"] for summary in summaries] == ["Kyle Cooke Bravo", "Kyle Cooke"]
    assert summaries[1]["duplicate_suppressed_count"] == 2
    assert summaries[1]["unique_result_count"] == 2


def test_normalize_external_ids_matches_people_full_name(monkeypatch) -> None:
    db = types.SimpleNamespace()
    response = types.SimpleNamespace(
        data=[{"id": "person-1", "full_name": "Jane Doe", "external_ids": {"imdb": "nm123", "tmdb": "456"}}]
    )
    query = types.SimpleNamespace(execute=lambda: response)
    query = types.SimpleNamespace(limit=lambda _count: query, execute=lambda: response)
    query = types.SimpleNamespace(eq=lambda *_args: query, limit=lambda _count: query, execute=lambda: response)
    query = types.SimpleNamespace(
        select=lambda *_args: query,
        eq=lambda *_args: query,
        limit=lambda _count: query,
        execute=lambda: response,
    )
    db.schema = lambda _name: types.SimpleNamespace(table=lambda _table: query)

    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline.create_supabase_admin_client", lambda: db)

    result = _normalize_external_ids("Jane Doe")

    assert result == {"person_id": "person-1", "imdb_id": "nm123", "tmdb_id": 456}


def test_build_bridge_and_catalog_prefers_exact_nup_matches() -> None:
    raw_payloads = {
        "getty": [
            {
                "editorial_id": "928663262",
                "object_name": "NUP_181952_0005.JPG",
                "caption": "Pictured: Andy Cohen, Brandi Glanville, Adam Rippon",
                "event_name": "Watch What Happens Live",
                "date_created": "2018-02-08T00:00:00Z",
                "preview_image_url": "https://getty.example/5.jpg",
            }
        ],
        "nbcumv": [
            {
                "lbx_id": "70761487",
                "lbx_filename": "NUP_181952_5.JPG",
                "lbx_caption": (
                    'WATCH WHAT HAPPENS LIVE -- "Episode 40" -- Pictured: Andy Cohen, Brandi Glanville, Adam Rippon'
                ),
                "lbx_showTitle": "Watch What Happens Live",
                "lbx_episodeTitle": "Episode 40",
                "lbx_seasonNumber": 15,
                "liveDate": "2018-02-08T00:00:00Z",
                "location": "https://nbcumv.example/5.jpg",
            }
        ],
        "bravo": [],
    }

    bridge_rows, merged_catalog = build_bridge_and_catalog(raw_payloads)

    assert len(merged_catalog) == 1
    merged = merged_catalog[0]
    assert merged["bridge_strategy"] == "A_nup_filename"
    assert set(merged["sources"]) == {"getty", "nbcumv"}
    assert merged["episode_title"] == "Episode 40"
    assert merged["show_name"] == "Watch What Happens Live"
    assert bridge_rows[0]["strategy"] == "A_nup_filename"


def test_build_bridge_and_catalog_sends_ambiguous_caption_matches_to_manual_review() -> None:
    raw_payloads = {
        "getty": [
            {
                "editorial_id": "1",
                "object_name": None,
                "caption": "Pictured: Andy Cohen, Brandi Glanville",
                "event_name": "Watch What Happens Live",
                "date_created": "2018-02-08",
                "preview_image_url": "https://getty.example/1.jpg",
            }
        ],
        "nbcumv": [],
        "bravo": [
            {
                "media_uuid": "a",
                "field_caption": "Brandi celebrates with Andy Cohen backstage.",
                "gallery_show_name": "Watch What Happens Live",
                "gallery_created": "2018-02-08",
                "file_url": "https://www.bravotv.com/sites/bravo/files/example-a.jpg",
            },
            {
                "media_uuid": "b",
                "field_caption": "Andy Cohen and Brandi pose backstage.",
                "gallery_show_name": "Watch What Happens Live",
                "gallery_created": "2018-02-08",
                "file_url": "https://www.bravotv.com/sites/bravo/files/example-b.jpg",
            },
        ],
    }

    bridge_rows, merged_catalog = build_bridge_and_catalog(raw_payloads)

    assert any(row.get("strategy") == "manual_review" for row in bridge_rows)
    assert len(merged_catalog) == 3


def test_normalize_getty_record_preserves_large_and_thumb_urls() -> None:
    asset = {
        "editorial_id": "928663262",
        "object_name": "NUP_181952_0005.JPG",
        "caption": "Pictured: Andy Cohen, Brandi Glanville",
        "event_name": "Watch What Happens Live Season 15",
        "detail_url": "https://www.gettyimages.com/detail/news-photo/example/928663262",
        "thumbUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=170x170&w=gi&k=20&c=thumb",
        "compUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=594x594&w=gi&k=20&c=comp",
        "galleryComp1024Url": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=1024x1024&w=gi&k=20&c=gallery-comp",
        "highResCompUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=1365x2048&w=gi&k=20&c=hires",
        "galleryHighResCompUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=2048x2048&w=gi&k=20&c=gallery-hires",
        "assetDimensions": {"width": 2048, "height": 1365},
    }

    normalized = _normalize_getty_record(asset, known_people=["Andy Cohen", "Brandi Glanville"])

    assert normalized["source_url"] == asset["galleryHighResCompUrl"]
    assert normalized["preview_image_url"] == asset["galleryComp1024Url"]
    assert normalized["getty_original_image_url"] == asset["galleryHighResCompUrl"]
    assert isinstance(normalized["thumb_url"], str)
    assert "612x612" in str(normalized["thumb_url"])
    assert "w=0" in str(normalized["thumb_url"])


def test_acquire_best_image_uploads_getty_large_and_thumb(monkeypatch) -> None:
    mirrored_urls: list[str] = []

    def fake_mirror(url: str):  # noqa: ANN001
        mirrored_urls.append(url)
        suffix = "thumb" if "612x612" in url else "full"
        return types.SimpleNamespace(
            status="mirrored",
            hosted_url=f"https://cdn.example.com/{suffix}.jpg",
            hosted_key=f"shared-media/{suffix}.jpg",
            sha256=f"sha-{suffix}",
            content_type="image/jpeg",
            size_bytes=1234 if suffix == "full" else 123,
            error=None,
        )

    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.mirror_url_to_s3",
        lambda url: fake_mirror(url),
    )

    record = {
        "id": "group-1",
        "per_source": {
            "getty": {
                "source_url": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=2048x2048&w=gi&k=20&c=gallery-hires",
                "thumb_url": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=612x612&w=0&k=20&c=thumb-clean",
                "source_page_url": "https://www.gettyimages.com/detail/news-photo/example/928663262",
            }
        },
    }

    acquisition = acquire_best_image(record)

    assert mirrored_urls == [
        "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=2048x2048&w=gi&k=20&c=gallery-hires",
        "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=612x612&w=0&k=20&c=thumb-clean",
    ]
    assert acquisition["status"] == "uploaded"
    assert acquisition["source"] == "getty"
    assert acquisition["hosted_url"] == "https://cdn.example.com/full.jpg"
    assert acquisition["hosted_thumb_url"] == "https://cdn.example.com/thumb.jpg"
    assert acquisition["source_page_url"] == "https://www.gettyimages.com/detail/news-photo/example/928663262"


def test_run_get_images_pipeline_uses_prefetched_getty_assets_without_live_collect(monkeypatch, tmp_path) -> None:
    prefetched_assets = [
        {
            "editorial_id": "928663262",
            "object_name": "NUP_181952_0005.JPG",
            "caption": "Pictured: Andy Cohen",
            "event_name": "Watch What Happens Live",
            "detail_url": "https://www.gettyimages.com/detail/news-photo/example/928663262",
            "galleryHighResCompUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=2048x2048&w=gi&k=20&c=gallery-hires",
            "galleryComp1024Url": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=1024x1024&w=gi&k=20&c=gallery-comp",
            "thumbUrl": "https://media.gettyimages.com/id/928663262/photo/example.jpg?s=170x170&w=gi&k=20&c=thumb",
            "assetDimensions": {"width": 2048, "height": 1365},
        }
    ]

    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline._collect_getty_person",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("live Getty collection should be skipped")),
    )
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_nbcumv_person", lambda *args, **kwargs: [])
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_bravo_person", lambda *args, **kwargs: [])
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline.nbcumv.fetch_image_by_identity", lambda **kwargs: None)
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.mirror_url_to_s3",
        lambda url: types.SimpleNamespace(
            status="mirrored",
            hosted_url=f"https://cdn.example.com/{'thumb' if '612x612' in url else 'full'}.jpg",
            hosted_key="shared-media/test.jpg",
            sha256="sha-test",
            content_type="image/jpeg",
            size_bytes=1234,
            error=None,
        ),
    )

    result = run_get_images_pipeline(
        person_name="Andy Cohen",
        output_dir=tmp_path,
        sources=["getty"],
        getty_prefetched_assets=prefetched_assets,
        getty_prefetch_mode="full",
    )

    manifest = result["manifest"]
    raw_getty = (tmp_path / "raw" / "getty.json").read_text()
    merged_catalog = (tmp_path / "merged_catalog.json").read_text()

    assert manifest["counts"]["getty"] == 1
    assert any("getty_prefetched_assets" in str(note) for note in manifest["notes"])
    assert "928663262" in raw_getty
    assert "hosted_thumb_url" in merged_catalog


def test_run_get_images_pipeline_backfills_nbcumv_from_getty_nup(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline._collect_getty_person",
        lambda *args, **kwargs: [
            {
                "editorial_id": "928663262",
                "object_name": "NUP_181952_0005.JPG",
                "caption": "Pictured: Andy Cohen",
                "event_name": "Watch What Happens Live",
                "preview_image_url": "https://getty.example/5.jpg",
            }
        ],
    )
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_nbcumv_person", lambda *args, **kwargs: [])
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_bravo_person", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.nbcumv.fetch_image_by_identity",
        lambda **kwargs: {
            "lbx_id": "70761487",
            "lbx_filename": "NUP_181952_5.JPG",
            "lbx_caption": "Pictured: Andy Cohen",
            "lbx_showTitle": "Watch What Happens Live",
            "location": "https://nbcumv.example/5.jpg",
        },
    )
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.acquire_best_image",
        lambda record: {"status": "skipped", "source": "test"},
    )

    result = run_get_images_pipeline(person_name="Andy Cohen", output_dir=tmp_path, sources=["getty"])

    raw_nbcumv = (tmp_path / "raw" / "nbcumv.json").read_text()
    merged_catalog = (tmp_path / "merged_catalog.json").read_text()

    assert result["manifest"]["counts"]["nbcumv"] == 1
    assert result["manifest"]["getty_family_backfill"]["nbcumv_from_getty_nup"]["added"] == 1
    assert '"nbcumv_query_branch": "filename_backfill_from_getty_nup"' in raw_nbcumv
    assert "70761487" in raw_nbcumv
    assert '"strategy": "A_nup_filename"' in (tmp_path / "bridge_table.json").read_text()
    assert '"nbcumv"' in merged_catalog


def test_run_get_images_pipeline_backfills_getty_metadata_for_bravo_nup(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_getty_person", lambda *args, **kwargs: [])
    monkeypatch.setattr("trr_backend.media.bravotv.get_images_pipeline._collect_nbcumv_person", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline._collect_bravo_person",
        lambda *args, **kwargs: [
            {
                "media_uuid": "bravo-1",
                "file_name": "NUP_181952_0005.JPG",
                "file_url": "https://www.bravotv.com/sites/bravo/files/NUP_181952_0005.JPG",
                "field_caption": "Pictured: Andy Cohen",
                "gallery_show_name": "Watch What Happens Live",
            }
        ],
    )
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.getty.resolve_asset_by_object_name",
        lambda filename: {
            "editorial_id": "928663262",
            "object_name": filename,
            "caption": "Pictured: Andy Cohen",
            "event_name": "Watch What Happens Live",
            "preview_image_url": "https://getty.example/5.jpg",
        },
    )
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.nbcumv.fetch_image_by_identity",
        lambda **kwargs: {
            "lbx_id": "70761487",
            "lbx_filename": "NUP_181952_5.JPG",
            "lbx_caption": "Pictured: Andy Cohen",
            "lbx_showTitle": "Watch What Happens Live",
            "location": "https://nbcumv.example/5.jpg",
        },
    )
    monkeypatch.setattr(
        "trr_backend.media.bravotv.get_images_pipeline.acquire_best_image",
        lambda record: {"status": "skipped", "source": "test"},
    )

    result = run_get_images_pipeline(person_name="Andy Cohen", output_dir=tmp_path, sources=["getty"])

    raw_getty = (tmp_path / "raw" / "getty.json").read_text()
    merged_catalog = (tmp_path / "merged_catalog.json").read_text()

    assert result["manifest"]["counts"]["getty"] == 1
    assert result["manifest"]["counts"]["nbcumv"] == 1
    assert result["manifest"]["getty_family_backfill"]["getty_from_nup_sources"]["added"] == 1
    assert "928663262" in raw_getty
    assert '"bravo"' in merged_catalog
    assert '"getty"' in merged_catalog
    assert '"nbcumv"' in merged_catalog


def test_scrapling_fallback_extracts_bravo_gallery_rows_from_html() -> None:
    html = """
    <html>
      <head>
        <script data-drupal-selector="drupal-settings-json" type="application/json">
          {
            "ls_adobe_analytics": {
              "people": "Kyle Richards, Kim Richards",
              "showSite": "The Real Housewives of Beverly Hills",
              "season": "Season 3",
              "pageName": "Portia's Drama Filled Birthday Party"
            }
          }
        </script>
      </head>
      <body>
        <figure class="gallery-item" data-gallery-item-id="9799496" data-media-id="12345">
          <img
            src="/sites/bravo/files/2026/05/portia-party.jpg"
            alt="Kyle and Kim arrive at the party"
            data-file-uuid="file-1"
          />
          <figcaption>Kyle and Kim arrive at Portia's party.</figcaption>
          <span class="credit">Bravo</span>
        </figure>
      </body>
    </html>
    """

    rows = extract_gallery_assets_from_html(
        html,
        gallery={
            "uuid": "gallery-uuid",
            "title": "Portia's Drama Filled Birthday Party",
            "path": "/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party",
        },
    )

    assert rows == [
        {
            "gallery_uuid": "gallery-uuid",
            "gallery_title": "Portia's Drama Filled Birthday Party",
            "gallery_path": "/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party",
            "gallery_item_id": "9799496",
            "media_internal_id": "12345",
            "gallery_anchor_resolved": True,
            "gallery_position": 0,
            "gallery_people_names": ["Kyle Richards", "Kim Richards"],
            "gallery_show_name": "The Real Housewives of Beverly Hills",
            "gallery_season_name": "Season 3",
            "gallery_page_title": "Portia's Drama Filled Birthday Party",
            "season_number": 3,
            "field_caption": "Kyle and Kim arrive at Portia's party.",
            "field_credit": "Bravo",
            "field_media_image_alt": "Kyle and Kim arrive at the party",
            "file_uuid": "file-1",
            "file_url": "https://www.bravotv.com/sites/bravo/files/2026/05/portia-party.jpg",
            "file_name": "portia-party.jpg",
            "source_page_url": "https://www.bravotv.com/the-real-housewives-of-beverly-hills/photos/portias-drama-filled-birthday-party#9799496",
            "bravotv_html_fallback": True,
            "bravotv_html_original_url": True,
        }
    ]
