import importlib
import sys
from fractions import Fraction

from trr_backend.integrations import nbcumv


def test_default_public_appsync_key_is_used_when_env_is_unset(monkeypatch) -> None:
    monkeypatch.delenv("NBCUMV_APPSYNC_API_KEY", raising=False)
    sys.modules.pop("trr_backend.integrations.nbcumv", None)
    reloaded = importlib.import_module("trr_backend.integrations.nbcumv")

    assert reloaded.APPSYNC_API_KEY == reloaded.DEFAULT_APPSYNC_API_KEY


def test_find_show_image_by_filename_uses_show_index(monkeypatch) -> None:
    monkeypatch.setattr(
        nbcumv,
        "list_show_images",
        lambda show_id, session=None, limit=None: [
            {"lbx_filename": "NUP_209430_00480.jpg", "lbx_id": "70075355"},
            {"lbx_filename": "NUP_209430_00178.JPG", "lbx_id": "70075342"},
        ],
    )

    image = nbcumv.find_show_image_by_filename("show-1", "NUP_209430_00178.jpg")

    assert image is not None
    assert image["lbx_id"] == "70075342"


def test_json_safe_value_normalizes_fraction_like_values() -> None:
    payload = {
        "fraction": Fraction(3, 2),
        "items": [Fraction(2, 1)],
        "text": "abc\u0000def\u001bghi",
    }

    result = nbcumv._json_safe_value(payload)

    assert result == {
        "fraction": 1.5,
        "items": [2],
        "text": "abcdefghi",
    }


def test_search_person_images_uses_cloudsearch_shape_and_annotates_nup_counts(monkeypatch) -> None:
    payload = {
        "hits": {
            "found": 3,
            "start": 0,
            "hit": [
                {
                    "id": "bio-hit",
                    "fields": {
                        "type": "bio",
                        "title": "Lisa Barlow",
                        "item_number": "bio-item",
                    },
                },
                {
                    "id": "image-1",
                    "fields": {
                        "id": "cloud-1",
                        "type": "image",
                        "title": "NUP_191484_3108.JPG",
                        "item_number": "45047326",
                        "thumbnail": "https://thumb.example.com/1.jpg",
                        "description": "THE REAL HOUSEWIVES OF SALT LAKE CITY -- Pictured: Lisa Barlow",
                        "headline": "The Real Housewives of Salt Lake City - Season 1",
                        "shows": ["The Real Housewives of Salt Lake City"],
                        "show_ids": ["show-rhoslc"],
                        "created": "2020-09-09T19:30:18.749Z",
                        "filesize": "6161613",
                    },
                },
                {
                    "id": "image-2",
                    "fields": {
                        "id": "cloud-2",
                        "type": "image",
                        "title": "NUP_191484_3110.JPG",
                        "item_number": "45047328",
                        "thumbnail": "https://thumb.example.com/2.jpg",
                        "description": "THE REAL HOUSEWIVES OF SALT LAKE CITY -- Pictured: Lisa Barlow",
                        "headline": "The Real Housewives of Salt Lake City - Season 1",
                        "shows": ["The Real Housewives of Salt Lake City"],
                        "show_ids": ["show-rhoslc"],
                        "created": "2020-09-09T19:30:18.749Z",
                        "filesize": "6161613",
                    },
                },
            ],
        }
    }

    monkeypatch.setattr(nbcumv, "_cloudsearch_request", lambda *args, **kwargs: payload)

    images = nbcumv.search_person_images("Lisa Barlow", show_id="show-rhoslc", limit=10)

    assert [image["lbx_filename"] for image in images] == [
        "NUP_191484_3108.JPG",
        "NUP_191484_3110.JPG",
    ]
    assert all(image["lbx_showTitle"] == "The Real Housewives of Salt Lake City" for image in images)
    assert all(image["grouped_image_count"] == 2 for image in images)
    assert all(image["person_match_source"] == "cloudsearch" for image in images)


def test_discover_person_show_titles_dedupes_cloudsearch_titles(monkeypatch) -> None:
    monkeypatch.setattr(
        nbcumv,
        "search_person_images",
        lambda person_name, **kwargs: [
            {"lbx_showTitle": "Watch What Happens Live with Andy Cohen"},
            {"lbx_showTitle": "BravoCon"},
            {"lbx_showTitle": "Watch What Happens Live with Andy Cohen"},
        ],
    )

    titles = nbcumv.discover_person_show_titles("Lisa Barlow")

    assert titles == ["Watch What Happens Live with Andy Cohen", "BravoCon"]


def test_search_person_show_catalog_filters_show_catalog_and_annotates_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        nbcumv,
        "list_show_images",
        lambda show_id, **kwargs: [
            {
                "lbx_id": "1",
                "lbx_filename": "NUP_191484_3108.JPG",
                "lbx_caption": "THE REAL HOUSEWIVES OF SALT LAKE CITY -- Pictured: Lisa Barlow",
            },
            {
                "lbx_id": "2",
                "lbx_filename": "NUP_191484_3110.JPG",
                "lbx_caption": "THE REAL HOUSEWIVES OF SALT LAKE CITY -- Pictured: Lisa Barlow",
            },
            {
                "lbx_id": "3",
                "lbx_filename": "NUP_191500_0001.JPG",
                "lbx_caption": "THE REAL HOUSEWIVES OF SALT LAKE CITY -- Pictured: Meredith Marks",
            },
        ],
    )

    images = nbcumv.search_person_show_catalog("Lisa Barlow", show_id="show-rhoslc", limit=10)

    assert [image["lbx_id"] for image in images] == ["1", "2"]
    assert all(image["grouped_image_count"] == 2 for image in images)
    assert all(image["person_match_source"] == "show_catalog" for image in images)


def test_fetch_image_by_identity_falls_back_to_cloudsearch_for_unpadded_nup_variants(monkeypatch) -> None:
    monkeypatch.setattr(nbcumv, "find_show_image_by_filename", lambda *args, **kwargs: None)
    monkeypatch.setattr(nbcumv, "search_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        nbcumv,
        "_search_cloudsearch_images_by_filename",
        lambda *args, **kwargs: [
            {
                "lbx_id": "45047326",
                "lbx_filename": "NUP_191484_3108.JPG",
                "location": "https://thumb.example.com/1.jpg",
                "showIds": ["show-rhoslc"],
                "lbx_showTitle": "The Real Housewives of Salt Lake City",
            }
        ],
    )

    image = nbcumv.fetch_image_by_identity(filename="nup_191484_03108", show_id="show-rhoslc")

    assert image is not None
    assert image["lbx_id"] == "45047326"
    assert image["lbx_filename"] == "NUP_191484_3108.JPG"
