from __future__ import annotations

from trr_backend.bravotv.get_images_pipeline import (
    _extract_bravo_image_people_names,
    _extract_people_from_text,
    _normalize_nup_key,
    _refreshed_artifacts,
    _selected_source_families,
    _split_caption_people,
    build_bridge_and_catalog,
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
