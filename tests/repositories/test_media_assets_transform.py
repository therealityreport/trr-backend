from __future__ import annotations

from trr_backend.repositories.media_assets import (
    reconcile_media_asset_id_conflicts,
    transform_cast_photos_to_media,
    transform_episode_images_to_media,
    transform_person_images_to_media,
    transform_season_images_to_media,
    transform_show_images_to_media,
)


class _FakeMediaAssetsQuery:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self._filters: dict[str, object] = {}

    def select(self, _fields: str):
        return self

    def eq(self, key: str, value: object):
        self._filters[key] = value
        return self

    def in_(self, key: str, values: list[str]):
        self._filters[key] = list(values)
        return self

    def execute(self):
        filtered = self._rows
        source = self._filters.get("source")
        if source is not None:
            filtered = [row for row in filtered if row.get("source") == source]
        source_asset_ids = self._filters.get("source_asset_id")
        if isinstance(source_asset_ids, list):
            filtered = [row for row in filtered if row.get("source_asset_id") in source_asset_ids]
        source_urls = self._filters.get("source_url")
        if isinstance(source_urls, list):
            filtered = [row for row in filtered if row.get("source_url") in source_urls]
        return type("Response", (), {"data": filtered})()


class _FakeDb:
    def __init__(self, media_assets_rows: list[dict[str, object]]) -> None:
        self._media_assets_rows = media_assets_rows

    def schema(self, _name: str):
        return self

    def table(self, name: str):
        if name != "media_assets":
            raise AssertionError(f"Unexpected table lookup: {name}")
        return _FakeMediaAssetsQuery(self._media_assets_rows)


def test_transform_show_images_preserves_urls_and_hosted() -> None:
    rows = [
        {
            "show_id": "00000000-0000-0000-0000-000000000001",
            "source": "tmdb",
            "source_image_id": "abc123",
            "url": "https://image.tmdb.org/t/p/original/foo.jpg",
            "hosted_url": "https://cdn.example.com/media/foo.jpg",
            "kind": "poster",
            "width": 1000,
            "height": 1500,
            "caption": "Promo",
            "file_path": "/foo.jpg",
            "iso_639_1": "en",
            "metadata": {"vote_average": 7.5},
        }
    ]

    assets, links = transform_show_images_to_media(rows)

    assert len(assets) == 1
    asset = assets[0]
    assert asset["source_url"] == "https://image.tmdb.org/t/p/original/foo.jpg"
    assert asset["hosted_url"] == "https://cdn.example.com/media/foo.jpg"
    assert asset["metadata"]["vote_average"] == 7.5

    assert len(links) == 1
    link = links[0]
    assert link["entity_type"] == "show"
    assert link["kind"] == "poster"
    assert link["context"]["file_path"] == "/foo.jpg"


def test_transform_show_images_dedup_by_source_url() -> None:
    rows = [
        {
            "show_id": "00000000-0000-0000-0000-000000000001",
            "source": "imdb",
            "source_image_id": None,
            "url": "https://m.media-amazon.com/images/M/abc.jpg",
            "kind": "gallery",
        },
        {
            "show_id": "00000000-0000-0000-0000-000000000002",
            "source": "imdb",
            "source_image_id": None,
            "url": "https://m.media-amazon.com/images/M/abc.jpg",
            "kind": "gallery",
        },
    ]

    assets, links = transform_show_images_to_media(rows)

    assert len(assets) == 1
    assert len(links) == 2


def test_transform_person_images_primary_flag() -> None:
    rows = [
        {
            "person_id": "00000000-0000-0000-0000-000000000010",
            "source": "imdb_graphql",
            "url": "https://m.media-amazon.com/images/M/xyz.jpg",
            "width": 640,
            "height": 480,
            "caption": "Sample",
            "is_primary": False,
        }
    ]

    assets, links = transform_person_images_to_media(rows)

    assert len(assets) == 1
    assert len(links) == 1
    assert links[0]["is_primary"] is False


def test_transform_season_images_uses_canonical_hosted_url_and_context(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.build_hosted_url",
        lambda hosted_key: f"https://pub.example/{hosted_key}",
    )
    rows = [
        {
            "id": "10000000-0000-0000-0000-000000000001",
            "season_id": "20000000-0000-0000-0000-000000000001",
            "season_number": 6,
            "source": "tmdb",
            "source_image_id": "season-poster-1",
            "url": "https://image.tmdb.org/t/p/original/season.jpg",
            "kind": "poster",
            "file_path": "/season.jpg",
            "iso_639_1": "en",
            "hosted_key": "season-images/rhoslc/s6/poster.jpg",
            "hosted_url": "https://legacy.example/season.jpg",
        }
    ]

    assets, links = transform_season_images_to_media(rows)

    assert len(assets) == 1
    assert assets[0]["hosted_url"].endswith("/season-images/rhoslc/s6/poster.jpg")
    assert len(links) == 1
    assert links[0]["entity_type"] == "season"
    assert links[0]["kind"] == "poster"
    assert links[0]["context"]["legacy_table"] == "season_images"
    assert links[0]["context"]["season_number"] == 6


def test_transform_episode_images_preserves_episode_context(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.build_hosted_url",
        lambda hosted_key: f"https://pub.example/{hosted_key}",
    )
    rows = [
        {
            "id": "30000000-0000-0000-0000-000000000001",
            "episode_id": "40000000-0000-0000-0000-000000000001",
            "season_number": 6,
            "episode_number": 12,
            "source": "tmdb",
            "source_image_id": "episode-still-1",
            "url_original": "https://image.tmdb.org/t/p/original/still.jpg",
            "kind": "still",
            "position": 2,
        }
    ]

    assets, links = transform_episode_images_to_media(rows)

    assert len(assets) == 1
    assert assets[0]["source_url"] == "https://image.tmdb.org/t/p/original/still.jpg"
    assert len(links) == 1
    assert links[0]["entity_type"] == "episode"
    assert links[0]["position"] == 2
    assert links[0]["context"]["legacy_table"] == "episode_images"
    assert links[0]["context"]["episode_number"] == 12


def test_transform_cast_photos_maps_gallery_links(monkeypatch) -> None:
    monkeypatch.setattr(
        "trr_backend.repositories.media_assets.build_hosted_url",
        lambda hosted_key: f"https://pub.example/{hosted_key}",
    )
    rows = [
        {
            "id": "50000000-0000-0000-0000-000000000001",
            "person_id": "60000000-0000-0000-0000-000000000001",
            "source": "imdb",
            "source_image_id": "rm123",
            "image_url_canonical": "https://m.media-amazon.com/images/M/cast.jpg",
            "gallery_index": 4,
            "gallery_total": 25,
            "viewer_id": "viewer-1",
        }
    ]

    assets, links = transform_cast_photos_to_media(rows)

    assert len(assets) == 1
    assert assets[0]["source_asset_id"] == "rm123"
    assert len(links) == 1
    assert links[0]["entity_type"] == "person"
    assert links[0]["kind"] == "gallery"
    assert links[0]["position"] == 4
    assert links[0]["context"]["legacy_table"] == "cast_photos"
    assert links[0]["context"]["gallery_total"] == 25


def test_transform_cast_photos_prefers_getty_original_url_from_metadata() -> None:
    rows = [
        {
            "id": "50000000-0000-0000-0000-000000000099",
            "person_id": "60000000-0000-0000-0000-000000000099",
            "source": "getty",
            "source_image_id": "1435767826",
            "url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?p=1&s=594x594&w=gi&k=20&c=preview",
            "metadata": {
                "getty_original_image_url": (
                    "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048x2048&w=gi&k=20&c=full"
                ),
                "getty_preview_image_url": (
                    "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=1024x1024&w=gi&k=20&c=mid"
                ),
            },
        }
    ]

    assets, links = transform_cast_photos_to_media(rows)

    assert len(assets) == 1
    assert assets[0]["source_url"] == rows[0]["metadata"]["getty_original_image_url"]
    assert assets[0]["source_asset_id"] == "1435767826"
    assert len(links) == 1
    assert links[0]["kind"] == "gallery"


def test_transform_cast_photos_ignores_raw_getty_hosted_url_without_hosted_key() -> None:
    rows = [
        {
            "id": "50000000-0000-0000-0000-000000000100",
            "person_id": "60000000-0000-0000-0000-000000000100",
            "source": "getty",
            "source_image_id": "1435767827",
            "url": "https://media.gettyimages.com/id/1435767827/photo/example.jpg?p=1&s=594x594&w=gi&k=20&c=preview",
            "hosted_url": "https://media.gettyimages.com/id/1435767827/photo/example.jpg?s=2048x2048&w=gi&k=20&c=full",
            "metadata": {
                "getty_original_image_url": (
                    "https://media.gettyimages.com/id/1435767827/photo/example.jpg?s=2048x2048&w=gi&k=20&c=full"
                )
            },
        }
    ]

    assets, _links = transform_cast_photos_to_media(rows)

    assert len(assets) == 1
    assert assets[0]["source_url"] == rows[0]["metadata"]["getty_original_image_url"]
    assert assets[0].get("hosted_url") is None


def test_reconcile_media_asset_id_conflicts_reuses_existing_source_asset_row() -> None:
    db = _FakeDb(
        [
            {
                "id": "existing-asset-id",
                "source": "getty",
                "source_asset_id": "1435767826",
                "source_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048",
            }
        ]
    )
    assets = [
        {
            "id": "new-deterministic-id",
            "source": "getty",
            "source_asset_id": "1435767826",
            "source_url": "https://media.gettyimages.com/id/1435767826/photo/example.jpg?s=2048",
        }
    ]
    links = [
        {
            "id": "stale-link-id",
            "entity_type": "person",
            "entity_id": "60000000-0000-0000-0000-000000000099",
            "media_asset_id": "new-deterministic-id",
            "kind": "gallery",
            "position": 1,
            "is_primary": False,
            "context": {"legacy_table": "cast_photos"},
        }
    ]

    reconciled_assets, reconciled_links = reconcile_media_asset_id_conflicts(db, assets, links)

    assert reconciled_assets[0]["id"] == "existing-asset-id"
    assert reconciled_links[0]["media_asset_id"] == "existing-asset-id"
    assert reconciled_links[0]["id"] != "stale-link-id"
