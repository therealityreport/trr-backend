from __future__ import annotations

import pytest

from trr_backend.repositories import person_external_ids as repo


def test_normalize_person_external_id_value_matches_app_url_cases() -> None:
    assert (
        repo.normalize_person_external_id_value(
            "imdb",
            "https://www.imdb.com/name/nm1234567/?ref_=fn_al_nm_1",
        )
        == "nm1234567"
    )
    assert (
        repo.normalize_person_external_id_value(
            "wikidata",
            "https://www.wikidata.org/wiki/q42?uselang=en",
        )
        == "Q42"
    )
    assert (
        repo.normalize_person_external_id_value(
            "facebook",
            "https://www.facebook.com/people/Jane-Doe/100094321234567/",
        )
        == "100094321234567"
    )
    assert repo.normalize_person_external_id_value("threads", "https://www.threads.net/@andycohen") == "andycohen"
    assert repo.normalize_person_external_id_value("youtube", "https://www.youtube.com/@bravotv/videos") == "@bravotv"


def test_build_legacy_external_ids_from_records_coerces_numeric_sources() -> None:
    legacy = repo._build_legacy_external_ids_from_records(
        [
            {
                "id": 1,
                "source_id": "tmdb",
                "external_id": "1686599",
                "is_primary": True,
                "valid_to": None,
            },
            {
                "id": 2,
                "source_id": "imdb",
                "external_id": "https://www.imdb.com/name/nm1234567/",
                "is_primary": True,
                "valid_to": None,
            },
            {
                "id": 3,
                "source_id": "instagram",
                "external_id": "https://www.instagram.com/bravotv/",
                "is_primary": True,
                "valid_to": None,
            },
        ]
    )

    assert legacy == {
        "tmdb": 1686599,
        "tmdb_id": 1686599,
        "imdb": "nm1234567",
        "imdb_id": "nm1234567",
        "instagram": "bravotv",
        "instagram_id": "bravotv",
    }


def test_normalize_input_rejects_unsupported_source() -> None:
    with pytest.raises(repo.UnsupportedPersonExternalIdSourceError, match="Unsupported source: letterboxd"):
        repo._normalize_input({"source_id": "letterboxd", "external_id": "demo"})


def test_normalize_input_drops_blank_external_id() -> None:
    assert repo._normalize_input({"source_id": "imdb", "external_id": "   "}) is None
