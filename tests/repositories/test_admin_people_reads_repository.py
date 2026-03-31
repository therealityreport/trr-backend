from __future__ import annotations

from uuid import uuid4

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


def test_gallery_page_keeps_zero_people_count_and_returns_total_count(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) == 1:
            return []
        if len(calls) in {2, 4}:
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
        if len(calls) == 3:
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

    assert query_count == 4
    assert len(calls) == 4
    assert payload["photos"][0]["people_count"] == 0
    assert payload["pagination"] == {
        "limit": 10,
        "offset": 0,
        "count": 1,
        "total_count": 1,
        "total_count_status": "exact",
        "next_offset": 1,
        "has_more": False,
    }
    first_query = calls[0][0]
    second_query = calls[1][0]
    assert "cp.metadata," not in first_query
    assert " ma.metadata," not in second_query
    assert " ma.metadata\n" not in second_query


def test_gallery_page_total_count_dedupes_duplicates_and_skips_broken(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) in {1, 3}:
            return [
                {
                    "id": "cast-1",
                    "person_id": "person-1",
                    "source": "imdb",
                    "url": "https://example.com/source-1.jpg",
                    "hosted_url": "https://cdn.example.com/photo-1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "caption": "caption",
                    "width": 640,
                    "height": 480,
                    "source_page_url": None,
                    "thumbnail_crop": None,
                    "metadata_people_count": None,
                    "metadata_people_count_source": None,
                    "face_boxes": [],
                    "face_crops": [],
                    "bucket_type": None,
                    "bucket_key": None,
                    "bucket_label": None,
                    "resolved_show_id": None,
                    "resolved_show_name": None,
                    "gallery_status": None,
                    "people_count": None,
                    "people_count_source": None,
                }
            ]
        return [
            {
                "link_id": "link-1",
                "person_id": "person-1",
                "media_asset_id": "",
                "source": "imdb",
                "source_url": "https://example.com/source-1.jpg",
                "hosted_url": "https://cdn.example.com/photo-1.jpg",
                "hosted_content_type": "image/jpeg",
                "caption": "caption",
                "width": 640,
                "height": 480,
                "resolved_source_url": "https://example.com/source-1.jpg",
                "thumbnail_crop": None,
                "context_people_count": None,
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
            },
            {
                "link_id": "link-2",
                "person_id": "person-1",
                "media_asset_id": "asset-2",
                "source": "imdb",
                "source_url": "https://example.com/source-2.jpg",
                "hosted_url": "https://cdn.example.com/photo-2.jpg",
                "hosted_content_type": "image/jpeg",
                "caption": "caption",
                "width": 640,
                "height": 480,
                "resolved_source_url": "https://example.com/source-2.jpg",
                "thumbnail_crop": None,
                "context_people_count": None,
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
                "gallery_status": "broken_unreachable",
            },
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_person_gallery_page(
        "person-1",
        limit=10,
        offset=0,
        include_broken=False,
        sources=["imdb"],
    )

    assert query_count == 4
    assert payload["pagination"]["count"] == 1
    assert payload["pagination"]["total_count"] == 1
    assert payload["photos"][0]["id"] == "link-1"
    assert calls[0][1][1] == ["imdb"]
    assert calls[1][1][2] == ["imdb"]


def test_gallery_page_can_defer_exact_total_count(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) == 1:
            return [
                {
                    "id": "cast-1",
                    "person_id": "person-1",
                    "source": "imdb",
                    "url": "https://example.com/source-1.jpg",
                    "hosted_url": "https://cdn.example.com/photo-1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "caption": "caption",
                    "width": 640,
                    "height": 480,
                    "source_page_url": None,
                    "thumbnail_crop": None,
                    "metadata_people_count": None,
                    "metadata_people_count_source": None,
                    "face_boxes": [],
                    "face_crops": [],
                    "bucket_type": None,
                    "bucket_key": None,
                    "bucket_label": None,
                    "resolved_show_id": None,
                    "resolved_show_name": None,
                    "gallery_status": None,
                    "people_count": None,
                    "people_count_source": None,
                }
            ]
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_person_gallery_page(
        "person-1",
        limit=10,
        offset=0,
        include_broken=False,
        sources=None,
        include_total_count=False,
    )

    assert query_count == 2
    assert len(calls) == 2
    assert payload["pagination"] == {
        "limit": 10,
        "offset": 0,
        "count": 1,
        "total_count": None,
        "total_count_status": "deferred",
        "next_offset": 1,
        "has_more": False,
    }


def test_gallery_page_uses_lightweight_count_queries_for_exact_total_count(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) == 1:
            return []
        if len(calls) in {2, 4}:
            return [
                {
                    "link_id": "link-1",
                    "person_id": "person-1",
                    "media_asset_id": "asset-1",
                    "source": "imdb",
                    "source_url": "https://example.com/source.jpg",
                    "resolved_source_url": "https://example.com/source.jpg",
                    "hosted_url": "https://cdn.example.com/photo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "caption": "caption",
                    "width": 640,
                    "height": 480,
                    "thumbnail_crop": None,
                    "context_people_count": None,
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
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, query_count = repo.get_person_gallery_page(
        "person-1",
        limit=10,
        offset=0,
        include_broken=False,
        sources=None,
        include_total_count=True,
    )

    assert query_count == 4
    assert payload["pagination"]["total_count"] == 1
    assert len(calls) == 4
    count_cast_query = calls[2][0]
    count_media_query = calls[3][0]
    assert "thumbnail_crop" not in count_cast_query
    assert "face_boxes" not in count_cast_query
    assert "caption" not in count_cast_query
    assert "thumbnail_crop" not in count_media_query
    assert "face_boxes" not in count_media_query
    assert "caption" not in count_media_query


def test_resolve_person_slug_uses_single_ranked_query(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params: list[object]):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "id": str(uuid4()),
                "full_name": "Phaedra Parks",
                "on_show": False,
                "match_rank": 0,
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, resolved_show_id, query_count = repo.resolve_person_slug("phaedra-parks")

    assert query_count == 1
    assert resolved_show_id is None
    assert payload is not None
    assert payload["slug"] == "phaedra-parks"
    assert "match_rank" in str(captured["query"])
    assert "p.full_name = ANY" in str(captured["query"])


def test_resolve_person_slug_uses_single_people_lookup(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        return [
            {
                "id": "person-1",
                "full_name": "Brandi Glanville",
                "on_show": True,
                "match_rank": 0,
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    payload, resolved_show_id, query_count = repo.resolve_person_slug("brandi-glanville", None)

    assert payload == {
        "person_id": "person-1",
        "slug": "brandi-glanville",
        "canonical_slug": "brandi-glanville",
    }
    assert resolved_show_id is None
    assert query_count == 1
    assert len(calls) == 1
    assert "match_rank" in calls[0][0]
