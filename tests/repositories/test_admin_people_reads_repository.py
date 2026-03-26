from __future__ import annotations

from trr_backend.repositories import admin_people_reads as repo


def test_person_detail_uses_tmdb_aliases(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_one(query: str, params: list[object]):
        captured["query"] = query
        captured["params"] = params
        return {
            "id": "person-1",
            "full_name": "Brandi Glanville",
            "known_for": None,
            "external_ids": {"tmdb": 1686599},
            "birthday": {},
            "gender": {},
            "biography": {},
            "place_of_birth": {},
            "homepage": {},
            "profile_image_url": {},
            "alternative_names": ["Brandi", "Brandi Lynn"],
        }

    monkeypatch.setattr(repo.pg, "fetch_one", fake_fetch_one)

    payload, query_count = repo.get_person_detail("person-1")

    assert query_count == 1
    assert payload is not None
    assert payload["alternative_names"] == ["Brandi", "Brandi Lynn"]
    assert "core.cast_tmdb" in str(captured["query"])
    assert "ct.also_known_as" in str(captured["query"])


def test_gallery_page_keeps_zero_people_count_without_extra_queries(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) == 1:
            return []
        return [
            {
                "link_id": "link-1",
                "person_id": "person-1",
                "media_asset_id": "asset-1",
                "source": "imdb",
                "source_url": "https://example.com/source.jpg",
                "hosted_url": "https://cdn.example.com/photo.jpg",
                "hosted_content_type": "image/jpeg",
                "caption": "caption",
                "width": 640,
                "height": 480,
                "resolved_source_url": "https://example.com/source.jpg",
                "thumbnail_crop": {"focus_x": 0.5},
                "context_people_count": "0",
                "context_people_count_source": None,
                "metadata_people_count": None,
                "metadata_people_count_source": None,
                "face_boxes": [],
                "face_crops": [],
                "bucket_type": None,
                "bucket_key": None,
                "bucket_label": None,
                "resolved_show_id": None,
                "resolved_show_name": None,
                "source_page_url": None,
                "gallery_status": None,
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_person_gallery_page(
        "person-1",
        limit=10,
        offset=0,
        include_broken=False,
        sources=None,
    )

    assert query_count == 2
    assert len(calls) == 2
    assert payload["photos"][0]["people_count"] == 0
    assert payload["pagination"] == {
        "limit": 10,
        "offset": 0,
        "count": 1,
        "next_offset": 1,
        "has_more": False,
    }
    first_query = calls[0][0]
    second_query = calls[1][0]
    assert "cp.metadata," not in first_query
    assert " ma.metadata," not in second_query
    assert " ma.metadata\n" not in second_query
