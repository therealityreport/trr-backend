from __future__ import annotations

import types

from trr_backend.bravotv.get_images_pipeline import (
    _extract_bravo_image_people_names,
    _extract_people_from_text,
    _normalize_getty_record,
    _normalize_nup_key,
    _refreshed_artifacts,
    _selected_source_families,
    _split_caption_people,
    acquire_best_image,
    build_bridge_and_catalog,
    run_get_images_pipeline,
)


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
        "trr_backend.bravotv.get_images_pipeline.mirror_url_to_s3",
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
        "trr_backend.bravotv.get_images_pipeline._collect_getty_person",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("live Getty collection should be skipped")),
    )
    monkeypatch.setattr("trr_backend.bravotv.get_images_pipeline._collect_nbcumv_person", lambda *args, **kwargs: [])
    monkeypatch.setattr("trr_backend.bravotv.get_images_pipeline._collect_bravo_person", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "trr_backend.bravotv.get_images_pipeline.mirror_url_to_s3",
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
