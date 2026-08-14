from __future__ import annotations

import pytest

from trr_backend.integrations import bravo_jsonapi, nbcumv
from trr_backend.media.watchers import sources

NBCUMV_SHOW_ID = "490e731c-d85f-474f-945b-b9681dc1931b"
BRAVO_SHOW_ID = "0e5cd65c-f736-434e-99bd-8943805b1c60"
NOW = "2026-08-06T12:00:00Z"
OLD = "2026-08-01T12:00:00Z"


def _record(record_type: str, record_id: str, *, created: str = NOW, changed: str = NOW, **attributes):
    return {
        "type": record_type,
        "id": record_id,
        "attributes": {"created": created, "changed": changed, **attributes},
    }


def _show_detail() -> dict:
    return {
        "data": {
            "type": "node--tv_show",
            "id": BRAVO_SHOW_ID,
            "attributes": {"drupal_internal__nid": 77, "title": "The Watch"},
            "relationships": {"field_cast": {"data": [{"type": "node--person", "id": "cast-1"}]}},
        },
        "included": [],
    }


def _person_detail() -> dict:
    return {
        "data": {
            "type": "node--person",
            "id": "cast-1",
            "attributes": {"title": "Cast Person", "created": NOW, "changed": NOW},
            "relationships": {
                "field_person_cover_photo": {"data": {"type": "media--image", "id": "profile-image"}},
                "field_person_full_photo": {"data": {"type": "media--image", "id": "profile-image"}},
            },
        },
        "included": [
            {
                "type": "media--image",
                "id": "profile-image",
                "attributes": {"name": "cast.jpg", "created": NOW, "changed": NOW},
                "relationships": {"field_media_image": {"data": {"type": "file--file", "id": "profile-file"}}},
            },
            {
                "type": "file--file",
                "id": "profile-file",
                "attributes": {
                    "filename": "cast.jpg",
                    "filemime": "image/jpeg",
                    "filesize": 321,
                    "uri": {"url": "/sites/bravo/files/styles/hero/public/cast.jpg?itok=discard"},
                    "created": NOW,
                    "changed": NOW,
                },
            },
        ],
    }


def test_nbcumv_look_images_iterates_every_token_page_and_normalizes_provenance(monkeypatch) -> None:
    queries: list[str] = []

    def fake_graphql(query: str, *, session=None):  # noqa: ARG001
        queries.append(query)
        if 'nextToken: "page-2"' in query:
            return {
                "lookImages": {
                    "items": [
                        {
                            "id": "lookup-2",
                            "imgId": "image-2",
                            "showId": NBCUMV_SHOW_ID,
                            "img": {"id": "image-2", "lbx_id": "2", "lbx_filename": "NUP_2.JPG", "modified": NOW},
                        }
                    ],
                    "nextToken": None,
                }
            }
        return {
            "lookImages": {
                "items": [
                    {
                        "id": "lookup-1",
                        "imgId": "image-1",
                        "showId": NBCUMV_SHOW_ID,
                        "img": {"id": "image-1", "lbx_id": "1", "lbx_filename": "NUP_1.JPG", "modified": NOW},
                    }
                ],
                "nextToken": "page-2",
            }
        }

    monkeypatch.setattr(nbcumv, "_graphql_request", fake_graphql)

    result = sources.discover_nbcumv_show_candidates(NBCUMV_SHOW_ID)

    assert [candidate["source_asset_id"] for candidate in result.candidates] == ["image-1", "image-2"]
    assert result.complete is True
    assert result.pages_fetched == 2
    assert result.candidates[0]["provenance"] == {
        "adapter": "nbcumv.lookImages",
        "show_id": NBCUMV_SHOW_ID,
        "source_id": "image-1",
        "resource_type": "image",
    }
    assert result.candidates[0]["raw_record"]["look_image_id"] == "lookup-1"
    assert len(queries) == 2
    assert "lookImages(" in queries[0]
    assert "category: show" in queries[0]
    assert 'id: "490e731c-d85f-474f-945b-b9681dc1931b"' in queries[0]


def test_nbcumv_malformed_and_unknown_show_id_are_distinct(monkeypatch) -> None:
    with pytest.raises(nbcumv.NBCUMVShowIdentityError):
        list(nbcumv.iter_show_look_images("not-a-uuid"))

    monkeypatch.setattr(nbcumv, "_graphql_request", lambda *args, **kwargs: {"lookImages": None})
    with pytest.raises(nbcumv.NBCUMVUnknownShowError):
        list(nbcumv.iter_show_look_images(NBCUMV_SHOW_ID))

    monkeypatch.setattr(
        nbcumv, "_graphql_request", lambda *args, **kwargs: {"lookImages": {"items": [], "nextToken": None}}
    )
    assert list(nbcumv.iter_show_look_images(NBCUMV_SHOW_ID)) == []

    monkeypatch.setattr(
        nbcumv, "_graphql_request", lambda *args, **kwargs: {"lookImages": {"items": [{}], "nextToken": None}}
    )
    with pytest.raises(nbcumv.NBCUMVMalformedPageError):
        list(nbcumv.iter_show_look_images(NBCUMV_SHOW_ID))


def test_bravo_incremental_discovery_follows_next_normalizes_original_and_cast_and_selects_mpx(monkeypatch) -> None:
    calls: list[tuple[str, dict | None]] = []

    image = _record(
        "media--image",
        "image-1",
        relationships={"field_media_image": {"data": {"type": "file--file", "id": "image-file"}}},
        name="watch.jpg",
    )
    # Relationships are JSON:API members, not attributes.
    image["relationships"] = image.pop("attributes").pop("relationships")
    image["attributes"] = {"created": NOW, "changed": NOW, "name": "watch.jpg"}
    image_file = _record(
        "file--file",
        "image-file",
        filename="watch.jpg",
        filemime="image/jpeg",
        filesize=123,
        uri={
            "url": "/sites/bravo/files/styles/media_gallery_computer/public/field_media_items/2026/08/watch.jpg?itok=1"
        },
    )
    video = _record(
        "media--video",
        "video-1",
        name="watch-video",
        renditions=[
            {
                "url": "https://link.theplatform.com/s/bravo/media/low.mp4?sig=low",
                "width": 640,
                "height": 360,
                "bitrate": 300,
            },
            {
                "url": "https://link.theplatform.com/s/bravo/media/high.mp4?sig=high",
                "width": 1920,
                "height": 1080,
                "bitrate": 3000,
            },
        ],
    )
    blog = _record("node--blog", "blog-1", title="Watch story", path={"alias": "/the-watch/watch-story"})

    def fake_get_json(client, url, params=None):  # noqa: ARG001
        calls.append((url, params))
        if url.endswith(f"/node/tv_show/{BRAVO_SHOW_ID}"):
            return _show_detail()
        if url.endswith("/node/person/cast-1"):
            return _person_detail()
        if url == "https://www.bravotv.com/jsonapi/media/image" and params and params.get("sort") == "-created":
            return {
                "data": [image],
                "included": [image_file],
                "links": {"next": "/jsonapi/media/image?page=2"},
            }
        if url == "https://www.bravotv.com/jsonapi/media/image?page=2":
            return {
                "data": [_record("media--image", "image-old", created=OLD, changed=OLD)],
                "included": [],
                "links": {"next": None},
            }
        if url == "https://www.bravotv.com/jsonapi/media/video" and params and params.get("sort") == "-created":
            return {"data": [video], "included": [], "links": {"next": None}}
        if url == "https://www.bravotv.com/jsonapi/node/blog" and params and params.get("sort") == "-created":
            return {"data": [blog], "included": [], "links": {"next": None}}
        return {"data": [], "included": [], "links": {"next": None}}

    monkeypatch.setattr(bravo_jsonapi, "_get_json", fake_get_json)

    result = sources.discover_bravo_incremental_candidates(
        BRAVO_SHOW_ID,
        watermarks={
            "created_at": "2026-08-05T00:00:00Z",
            "created_source_id": "z",
            "changed_at": "2026-08-05T00:00:00Z",
            "changed_source_id": "z",
        },
        overlap=sources.timedelta(0),
    )

    candidates = {candidate["source_asset_id"]: candidate for candidate in result.candidates}
    assert result.complete is True
    assert any(url.endswith("/jsonapi/media/image?page=2") for url, _ in calls)
    assert (
        candidates["image-1"]["original_url"]
        == "https://www.bravotv.com/sites/bravo/files/field_media_items/2026/08/watch.jpg"
    )
    assert candidates["image-1"]["download_url"] == candidates["image-1"]["original_url"]
    assert candidates["profile-image"]["people"] == ["Cast Person"]
    assert (
        candidates["profile-image"]["provenance"]["relationship_path"]
        == "field_person_cover_photo,field_person_full_photo"
    )
    assert candidates["profile-image"]["original_url"] == "https://www.bravotv.com/sites/bravo/files/cast.jpg"
    assert candidates["video-1"]["original_url"] == "https://www.bravotv.com/jsonapi/media/video/video-1"
    assert candidates["video-1"]["download_url"] == "https://link.theplatform.com/s/bravo/media/high.mp4?sig=high"
    assert candidates["blog-1"]["media_type"] == "metadata"
    assert all(
        params is None or params.get("filter[field_tv_shows.show]") == "77"
        for url, params in calls
        if "/jsonapi/" in url and "/node/" not in url
    )


def test_bravo_page_cap_returns_resumable_opaque_continuation(monkeypatch) -> None:
    calls: list[str] = []

    def fake_get_json(client, url, params=None):  # noqa: ARG001
        calls.append(url)
        if url.endswith(f"/node/tv_show/{BRAVO_SHOW_ID}"):
            return _show_detail()
        if url == "https://www.bravotv.com/jsonapi/media/image":
            return {
                "data": [_record("media--image", "image-page-1")],
                "included": [],
                "links": {"next": "/jsonapi/media/image?page=2"},
            }
        if url == "https://www.bravotv.com/jsonapi/media/image?page=2":
            return {
                "data": [_record("media--image", "image-page-2", created=OLD, changed=OLD)],
                "included": [],
                "links": {"next": None},
            }
        return {"data": [], "included": [], "links": {"next": None}}

    monkeypatch.setattr(bravo_jsonapi, "_get_json", fake_get_json)

    result = sources.discover_bravo_incremental_candidates(BRAVO_SHOW_ID, page_cap=2)

    assert result.complete is False
    assert result.continuation is not None
    decoded = sources._continuation_decode(result.continuation, show_uuid=BRAVO_SHOW_ID)
    assert decoded == {
        "cast_person_ids": ["cast-1"],
        "phase": "collection",
        "show_nid": 77,
        "show_uuid": BRAVO_SHOW_ID,
        "source": "bravo",
        "stream_index": 0,
        "person_index": 0,
        "next_url": "https://www.bravotv.com/jsonapi/media/image?page=2",
        "seen_page_urls": ["https://www.bravotv.com/jsonapi/media/image"],
        "version": 1,
    }
    resumed = sources.discover_bravo_incremental_candidates(BRAVO_SHOW_ID, continuation=result.continuation, page_cap=1)
    assert resumed.complete is False
    assert calls.count(f"https://www.bravotv.com/jsonapi/node/tv_show/{BRAVO_SHOW_ID}") == 1
    assert "https://www.bravotv.com/jsonapi/media/image?page=2" in calls


def test_bravo_malformed_page_is_a_hard_failure(monkeypatch) -> None:
    def fake_get_json(client, url, params=None):  # noqa: ARG001
        if url.endswith(f"/node/tv_show/{BRAVO_SHOW_ID}"):
            return _show_detail()
        return {"data": {"not": "a-list"}}

    monkeypatch.setattr(bravo_jsonapi, "_get_json", fake_get_json)

    with pytest.raises(bravo_jsonapi.BravoJSONAPIMalformedPageError):
        sources.discover_bravo_incremental_candidates(BRAVO_SHOW_ID, page_cap=2)


def test_source_url_policy_rejects_unsafe_urls_and_keeps_signed_mpx_url_transient() -> None:
    assert (
        sources.normalize_bravo_original_url(
            "https://www.bravotv.com/sites/bravo/files/styles/card/public/2026/08/photo.jpg?itok=discard"
        )
        == "https://www.bravotv.com/sites/bravo/files/2026/08/photo.jpg"
    )
    assert (
        sources.validate_transient_download_url(
            "https://link.theplatform.com/s/bravo/media/high.mp4?sig=keep", source="mpx"
        )
        == "https://link.theplatform.com/s/bravo/media/high.mp4?sig=keep"
    )
    with pytest.raises(sources.UnsafeSourceURLError):
        sources.normalize_source_url("http://www.bravotv.com/sites/bravo/files/photo.jpg", source="bravo")
    with pytest.raises(sources.UnsafeSourceURLError):
        sources.normalize_source_url("https://www.bravotv.com.evil.test/sites/bravo/files/photo.jpg", source="bravo")
    with pytest.raises(sources.UnsafeSourceURLError):
        sources.normalize_source_url("https://127.0.0.1/photo.jpg", source="bravo")
