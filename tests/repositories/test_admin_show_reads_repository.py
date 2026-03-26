from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from trr_backend.repositories import admin_show_reads as repo


class _FakeCursorContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def cursor(self, *args, **kwargs):
        return _FakeCursorContext()


def test_search_shows_maps_explicit_show_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "show-1",
                "name": "Bravo Show",
                "slug": "bravo-show",
                "canonical_slug": "bravo-show",
                "alternative_names": ["RHOBH"],
                "imdb_id": "tt123",
                "tmdb_id": 456,
                "show_total_seasons": 5,
                "show_total_episodes": 90,
                "description": "A Bravo show",
                "networks": ["Bravo"],
                "genres": ["Reality"],
                "tmdb_status": "Returning Series",
                "tmdb_vote_average": 7.8,
                "imdb_rating_value": 7.2,
                "poster_url": "https://cdn.example.com/poster.jpg",
                "computed_slug": "bravo-show",
                "slug_collision_count": 0,
            }
        ],
    )

    rows, query_count = repo.search_shows("Bravo", limit=20, offset=0)

    assert query_count == 1
    assert rows == [
        {
            "id": "show-1",
            "name": "Bravo Show",
            "slug": "bravo-show",
            "canonical_slug": "bravo-show",
            "alternative_names": ["RHOBH"],
            "imdb_id": "tt123",
            "tmdb_id": 456,
            "show_total_seasons": 5,
            "show_total_episodes": 90,
            "description": "A Bravo show",
            "networks": ["Bravo"],
            "genres": ["Reality"],
            "tmdb_status": "Returning Series",
            "tmdb_vote_average": 7.8,
            "imdb_rating_value": 7.2,
            "poster_url": "https://cdn.example.com/poster.jpg",
            "streaming_providers": [],
            "watch_providers": [],
            "tags": [],
        }
    ]


def test_resolve_show_slug_prefers_alias_slug_and_counts_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "name": "The Real Housewives of Beverly Hills",
                "alternative_names": ["RHOBH"],
                "slug": "the-real-housewives-of-beverly-hills",
            }
        ],
    )

    resolved, query_count = repo.resolve_show_slug("real-housewives-of-beverly-hills")

    assert query_count == 1
    assert resolved == {
        "show_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "slug": "rhobh",
        "canonical_slug": "rhobh",
        "show_name": "The Real Housewives of Beverly Hills",
    }


def test_get_people_home_preserves_section_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo.pg, "db_read_connection", lambda label="read": nullcontext(_FakeConnection()))
    monkeypatch.setattr(
        repo,
        "_people_most_popular",
        lambda limit, cur=None: (
            [
                {
                    "person_id": "person-1",
                    "full_name": "Brandi Glanville",
                    "known_for": "RHOBH",
                    "photo_url": "https://cdn.example.com/photo.jpg",
                    "metric_value": 8,
                    "show_context": "rhobh",
                    "latest_at": "2024-01-01T00:00:00+00:00",
                }
            ],
            1,
        ),
    )
    monkeypatch.setattr(repo, "_people_most_shows", lambda limit, cur=None: ([], 1))
    monkeypatch.setattr(repo, "_people_top_episodes", lambda limit, cur=None: ([], 1))
    monkeypatch.setattr(repo, "_people_recently_added", lambda limit, cur=None: ([], 1))

    payload, query_count = repo.get_people_home(12)

    assert query_count == 4
    assert payload["pagination"] == {"limit": 12}
    assert payload["sections"]["recentlyViewed"] == {"items": [], "error": None}
    assert payload["sections"]["mostPopular"]["items"] == [
        {
            "person_id": "person-1",
            "person_slug": "brandi-glanville",
            "full_name": "Brandi Glanville",
            "known_for": "RHOBH",
            "photo_url": "https://cdn.example.com/photo.jpg",
            "show_context": "rhobh",
            "metric_label": "News Score",
            "metric_value": 8,
            "latest_at": "2024-01-01T00:00:00+00:00",
        }
    ]


def test_search_global_skips_episode_lookup_for_short_non_episode_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo.pg, "db_read_connection", lambda label="read": nullcontext(_FakeConnection()))
    calls = {"count": 0}

    def fake_fetch_all_with_cursor(_cur, _query, _params=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "id": "show-1",
                    "name": "Alan's Show",
                    "slug": "alans-show",
                    "alternative_names": [],
                    "canonical_slug": "alans-show",
                }
            ]
        if calls["count"] == 2:
            return [
                {
                    "id": "person-1",
                    "full_name": "Alan Cumming",
                    "known_for": "Host",
                    "show_context": "the-traitors-us",
                }
            ]
        raise AssertionError("episodes query should not run for short non-episode search terms")

    monkeypatch.setattr(repo.pg, "fetch_all_with_cursor", fake_fetch_all_with_cursor)

    payload, query_count = repo.search_global("ala", limit=8)

    assert query_count == 2
    assert payload["episodes"] == []
    assert payload["people"][0]["person_slug"] == "alan-cumming"


def test_get_show_detail_strips_raw_metadata_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {
            "id": "show-1",
            "name": "Bravo Show",
            "slug": "bravo-show",
            "canonical_slug": "bravo-show",
            "alternative_names": ["RHOBH"],
            "imdb_id": "tt123",
            "tmdb_id": 456,
            "tvdb_id": 789,
            "tvrage_id": "tv-1",
            "wikidata_id": "Q123",
            "external_ids": {"imdb_id": "tt123"},
            "show_total_seasons": 5,
            "show_total_episodes": 90,
            "description": "A Bravo show",
            "premiere_date": "2020-01-01",
            "genres": ["Reality"],
            "networks": ["Bravo"],
            "streaming_providers": ["Peacock"],
            "watch_providers": ["Peacock"],
            "tags": ["Housewives"],
            "primary_poster_image_id": "poster-1",
            "primary_backdrop_image_id": "backdrop-1",
            "primary_logo_image_id": "logo-1",
            "tmdb_status": "Returning Series",
            "tmdb_vote_average": 7.8,
            "imdb_rating_value": 7.2,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
            "poster_url": "https://cdn.example.com/poster.jpg",
            "backdrop_url": "https://cdn.example.com/backdrop.jpg",
            "logo_url": "https://cdn.example.com/logo.png",
            "tmdb_meta": {"ignored": True},
            "imdb_meta": {"ignored": True},
            "computed_slug": "bravo-show",
            "slug_collision_count": 0,
        },
    )

    row, query_count = repo.get_show_detail("show-1")

    assert query_count == 1
    assert row is not None
    assert row["canonical_slug"] == "bravo-show"
    assert "tmdb_meta" not in row
    assert "imdb_meta" not in row


def test_get_show_seasons_default_path_uses_explicit_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params=None):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0)

    assert query_count == 1
    assert rows == []
    assert "SELECT *" not in str(captured["query"])
    assert "s.id::text AS id" in str(captured["query"])
    assert "s.show_id::text AS show_id" in str(captured["query"])
    assert "s.show_name" in str(captured["query"])
    assert "s.title" in str(captured["query"])
    assert "s.premiere_date" in str(captured["query"])
    assert "s.url_original_poster" in str(captured["query"])
    assert "s.overview" in str(captured["query"])
    assert "s.synopsis" not in str(captured["query"])
    assert "s.episode_count" not in str(captured["query"])


def test_get_show_seasons_include_episode_signal_preserves_overview_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_fetch_all(query: str, params=None):
        captured["query"] = query
        captured["params"] = params
        return [
            {
                "id": "season-6",
                "show_id": "show-1",
                "show_name": "The Real Housewives of Salt Lake City",
                "season_number": 6,
                "name": "Season 6",
                "title": "S6",
                "overview": "Salt Lake City returns.",
                "premiere_date": "2024-09-18",
                "url_original_poster": "https://cdn.example.com/season-6.jpg",
                "episode_airdate_count": 18,
                "has_scheduled_or_aired_episode": True,
            }
        ]

    monkeypatch.setattr(repo.pg, "fetch_all", fake_fetch_all)

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0, include_episode_signal=True)

    assert query_count == 1
    assert rows == [
        {
            "id": "season-6",
            "show_id": "show-1",
            "show_name": "The Real Housewives of Salt Lake City",
            "season_number": 6,
            "name": "Season 6",
            "title": "S6",
            "overview": "Salt Lake City returns.",
            "premiere_date": "2024-09-18",
            "url_original_poster": "https://cdn.example.com/season-6.jpg",
            "episode_airdate_count": 18,
            "has_scheduled_or_aired_episode": True,
        }
    ]
    assert "s.overview" in str(captured["query"])
    assert "s.synopsis" not in str(captured["query"])
    assert "episode_airdate_count" in str(captured["query"])
    assert "s.episode_count" not in str(captured["query"])


def test_get_show_seasons_normalizes_json_unsafe_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo.pg,
        "fetch_all",
        lambda query, params=None: [
            {
                "id": "season-1",
                "show_id": "show-1",
                "tmdb_season_id": 101,
                "season_number": 3,
                "name": "Season 3",
                "overview": "Salt Lake City returns.",
                "air_date": date(2024, 2, 14),
                "poster_path": "/poster.jpg",
                "episode_count": 18,
                "tmdb_vote_average": float("nan"),
                "tmdb_vote_count": 42,
                "trr_score": Decimal("7.25"),
                "created_at": datetime(2024, 2, 14, 12, 30, tzinfo=UTC),
                "updated_at": datetime(2024, 2, 15, 6, 45, tzinfo=UTC),
                "episode_airdate_count": "18",
                "has_scheduled_or_aired_episode": True,
            }
        ],
    )

    rows, query_count = repo.get_show_seasons("show-1", limit=20, offset=0, include_episode_signal=True)

    assert query_count == 1
    assert rows == [
        {
            "id": "season-1",
            "show_id": "show-1",
            "tmdb_season_id": 101,
            "season_number": 3,
            "name": "Season 3",
            "overview": "Salt Lake City returns.",
            "air_date": "2024-02-14",
            "poster_path": "/poster.jpg",
            "episode_count": 18,
            "tmdb_vote_average": None,
            "tmdb_vote_count": 42,
            "trr_score": 7.25,
            "created_at": "2024-02-14T12:30:00+00:00",
            "updated_at": "2024-02-15T06:45:00+00:00",
            "episode_airdate_count": 18,
            "has_scheduled_or_aired_episode": True,
        }
    ]


def test_mirror_media_asset_updates_canonical_ingest_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, object]] = []

    monkeypatch.setattr(
        repo,
        "mirror_media_asset_row",
        lambda asset_row, force=False: {
            "hosted_url": "https://cdn.example.com/asset-1.jpg",
            "hosted_content_type": "image/jpeg",
            "metadata": {"thumb_url": "https://cdn.example.com/asset-1-thumb.jpg"},
        },
    )
    monkeypatch.setattr(repo.pg, "execute", lambda query, params=None: executed.append((query, params)))

    asset_id, failure = repo._mirror_media_asset(
        {"id": "asset-1", "source_url": "https://tmdb.example.com/asset-1.jpg"}
    )

    assert asset_id == "asset-1"
    assert failure is None
    assert any("ingest_completed_at" in query for query, _params in executed)
    assert any("ingest_last_error = null" in query for query, _params in executed)


def test_get_show_assets_preserves_logo_fields_and_dedupes_show_images(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {
                        "source_page_url": "https://www.bravotv.com/the-daily-dish",
                        "context_section": "hero",
                        "people_count": "2",
                        "people_count_source": "manual",
                        "thumbnail_crop": {"x": 0.2, "y": 0.3, "zoom": 1.4, "mode": "manual"},
                    },
                    "media_asset_id": "media-1",
                    "asset_id": "media-1",
                    "source": "web_scrape",
                    "source_url": None,
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": 1200,
                    "height": 800,
                    "caption": "Main logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-card.jpg",
                        "original_source_file_url": "https://origin.example.com/logo.jpg",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "hosted",
                    "fetched_at": "2024-01-02T00:00:00Z",
                    "created_at": "2024-01-03T00:00:00Z",
                }
            ]
        if "from core.show_images" in sql:
            return [
                {
                    "id": "show-image-duplicate",
                    "source": "tmdb",
                    "kind": "logo",
                    "image_type": None,
                    "url": "https://tmdb.example.com/logo-duplicate.jpg",
                    "url_original": None,
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "width": 1200,
                    "height": 800,
                    "created_at": "2024-01-04T00:00:00Z",
                    "metadata": None,
                },
                {
                    "id": "show-image-2",
                    "source": "tmdb",
                    "kind": "poster",
                    "image_type": "poster",
                    "url": "https://tmdb.example.com/poster.jpg",
                    "url_original": "https://origin.example.com/poster.jpg",
                    "hosted_url": "https://cdn.example.com/poster.jpg",
                    "width": 1000,
                    "height": 1500,
                    "created_at": "2024-01-05T00:00:00Z",
                    "metadata": {"thumb_url": "https://cdn.example.com/poster-thumb.jpg"},
                },
            ]
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo.get_show_assets("show-1", limit=10, offset=0)

    assert query_count == 2
    assert [asset["id"] for asset in assets] == ["media-1", "show-image-2"]
    assert assets[0]["source"] == "bravotv.com"
    assert assets[0]["source_url"] == "https://www.bravotv.com/the-daily-dish"
    assert assets[0]["display_url"] == "https://cdn.example.com/logo-card.jpg"
    assert assets[0]["original_url"] == "https://origin.example.com/logo.jpg"
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["logo_white_url"] == "https://cdn.example.com/logo-white.png"
    assert assets[0]["logo_link_is_primary"] is True
    assert assets[0]["people_count"] == 2
    assert assets[0]["people_count_source"] == "manual"
    assert assets[0]["thumbnail_crop_mode"] == "manual"
    assert assets[1]["source"] == "tmdb"
    assert assets[1]["thumb_url"] == "https://cdn.example.com/poster-thumb.jpg"


def test_get_show_assets_paginates_after_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "poster",
                    "link_is_primary": False,
                    "context": None,
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/1.jpg",
                    "hosted_url": "https://cdn.example.com/1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
                {
                    "link_id": "link-2",
                    "link_kind": "poster",
                    "link_is_primary": False,
                    "context": None,
                    "media_asset_id": "asset-2",
                    "asset_id": "asset-2",
                    "source": "web_scrape:tmdb.com",
                    "source_url": "https://www.tmdb.com/2.jpg",
                    "hosted_url": "https://cdn.example.com/2.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
            ]
        if "from core.show_images" in sql:
            captured_limits.append(int(params[1]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo.get_show_assets("show-1", limit=1, offset=1)

    assert query_count == 2
    assert captured_limits == [500, 500]
    assert [asset["id"] for asset in assets] == ["asset-2"]
    assert assets[0]["source"] == "tmdb.com"


def test_get_show_assets_full_path_uses_full_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.media_links as ml" in sql and "ml.entity_type = 'show'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {"thumbnail_crop": {"x": 0.5, "y": 0.4, "zoom": 1.2, "mode": "manual"}},
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "web_scrape",
                    "source_url": "https://www.bravotv.com/logo.jpg",
                    "hosted_url": "https://cdn.example.com/logo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": 1000,
                    "height": 500,
                    "caption": "Logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-card.jpg",
                        "detail_url": "https://cdn.example.com/logo-detail.jpg",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "hosted",
                    "fetched_at": "2024-01-01T00:00:00Z",
                    "created_at": "2024-01-02T00:00:00Z",
                }
            ]
        if "from core.show_images" in sql:
            captured_limits.append(int(params[1]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo.get_show_assets("show-1", limit=5001, offset=0, full=True)

    assert query_count == 2
    assert captured_limits == [5001, 5001]
    assert len(assets) == 1
    assert assets[0]["source"] == "bravotv.com"
    assert assets[0]["display_url"] == "https://cdn.example.com/logo-card.jpg"
    assert assets[0]["detail_url"] == "https://cdn.example.com/logo-detail.jpg"
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["logo_white_url"] == "https://cdn.example.com/logo-white.png"
    assert assets[0]["thumbnail_crop_mode"] == "manual"


def test_get_show_season_assets_default_path_paginates_after_fetch_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_limits: list[int] = []

    def fake_fetch_one(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.seasons" in sql:
            return {"id": "season-1", "premiere_date": "2024-01-01", "air_date": None}
        if "from core.shows" in sql:
            return {"name": "Bravo Show", "external_ids": {}}
        raise AssertionError(f"unexpected query: {sql}")

    def fake_fetch_all(query: str, params=None, cur=None):
        sql = str(query)
        if "from core.episodes" in sql and "air_date is not null" in sql:
            captured_limits.append(int(params[1]))
            return []
        if "from core.media_links as ml" in sql and "ml.entity_type = 'season'" in sql:
            captured_limits.append(int(params[1]))
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "poster",
                    "context": None,
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/1.jpg",
                    "hosted_url": "https://cdn.example.com/1.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
                {
                    "link_id": "link-2",
                    "link_kind": "poster",
                    "context": {"source_page_url": "https://www.tmdb.com/2.jpg"},
                    "media_asset_id": "asset-2",
                    "asset_id": "asset-2",
                    "source": "web_scrape",
                    "source_url": None,
                    "hosted_url": "https://cdn.example.com/2.jpg",
                    "hosted_content_type": "image/jpeg",
                    "width": None,
                    "height": None,
                    "caption": None,
                    "metadata": None,
                    "ingest_status": "hosted",
                    "fetched_at": None,
                    "created_at": None,
                },
            ]
        if "from core.season_images" in sql:
            captured_limits.append(int(params[2]))
            return []
        if "from core.episode_images" in sql:
            captured_limits.append(int(params[2]))
            return []
        if "from core.credits as c" in sql:
            captured_limits.append(int(params[2]))
            return []
        raise AssertionError(f"unexpected query: {sql}")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo.get_show_season_assets("show-1", 6, limit=1, offset=1)

    assert query_count == 7
    assert captured_limits == [500, 500, 500, 500, 500]
    assert [asset["id"] for asset in assets] == ["asset-2"]
    assert assets[0]["source"] == "tmdb.com"


def test_assign_season_backdrops_skips_when_everything_already_assigned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo,
        "_fetch_one_row",
        lambda query, params=None, cur=None: {"id": "season-1", "show_id": "show-1", "season_number": 6},
    )
    monkeypatch.setattr(
        repo,
        "_fetch_all_rows",
        lambda query, params=None, cur=None: [{"media_asset_id": "asset-1"}],
    )

    payload, query_count, show_id = repo.assign_season_backdrops("season-1", ["asset-1"])

    assert show_id == "show-1"
    assert query_count == 2
    assert payload == {
        "requested": 1,
        "assigned": 0,
        "skipped": 1,
        "mirrored_attempted": 0,
        "mirrored_failed": 0,
        "mirrored_failed_ids": [],
    }


def test_assign_season_backdrops_reports_mirror_failures_without_insert(monkeypatch: pytest.MonkeyPatch) -> None:
    call_index = {"value": 0}
    inserted_rows: list[object] = []

    def fake_fetch_one(_query, params=None, cur=None):
        return {"id": "season-1", "show_id": "show-1", "season_number": 6}

    def fake_fetch_all(_query, params=None, cur=None):
        call_index["value"] += 1
        if call_index["value"] == 1:
            return []
        if call_index["value"] == 2:
            return [
                {"id": "asset-2", "hosted_url": None, "source": "tmdb", "source_url": "https://tmdb.example.com/2.jpg"}
            ]
        if call_index["value"] == 3:
            return [{"id": "asset-2", "hosted_url": None}]
        raise AssertionError("unexpected fetch_all call")

    monkeypatch.setattr(repo, "_fetch_one_row", fake_fetch_one)
    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)
    monkeypatch.setattr(repo, "_mirror_media_asset", lambda asset_row: ("asset-2", "Mirror failed"))
    monkeypatch.setattr(
        repo.pg,
        "execute_values_no_return",
        lambda query, rows, conn=None: inserted_rows.extend(rows),
    )

    payload, _query_count, _show_id = repo.assign_season_backdrops("season-1", ["asset-2"])

    assert payload == {
        "requested": 1,
        "assigned": 0,
        "skipped": 0,
        "mirrored_attempted": 1,
        "mirrored_failed": 1,
        "mirrored_failed_ids": ["asset-2"],
    }
    assert inserted_rows == []


def test_get_show_assets_merges_default_rows_with_dedupe_and_logo_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch_all(query: str, params=None, cur=None):
        if "from core.media_links as ml" in query:
            return [
                {
                    "link_id": "link-1",
                    "link_kind": "logo",
                    "link_is_primary": True,
                    "context": {
                        "context_section": "branding",
                        "thumbnail_crop": {"x": 0.4, "y": 0.6, "zoom": 1.2, "mode": "manual"},
                    },
                    "media_asset_id": "asset-1",
                    "asset_id": "asset-1",
                    "source": "tmdb",
                    "source_url": "https://tmdb.example.com/logo.png",
                    "hosted_url": "https://cdn.example.com/logo.png",
                    "hosted_content_type": "image/png",
                    "width": 1000,
                    "height": 300,
                    "caption": "Primary logo",
                    "metadata": {
                        "display_url": "https://cdn.example.com/logo-display.png",
                        "logo_black_url": "https://cdn.example.com/logo-black.png",
                        "logo_white_url": "https://cdn.example.com/logo-white.png",
                    },
                    "ingest_status": "completed",
                    "fetched_at": "2026-03-01T00:00:00Z",
                    "created_at": "2026-03-01T00:00:00Z",
                }
            ]
        if "from core.show_images" in query:
            return [
                {
                    "id": "legacy-duplicate",
                    "source": "fanart",
                    "kind": "logo",
                    "image_type": "logo",
                    "url": "https://fanart.example.com/logo.png",
                    "url_original": "https://fanart.example.com/logo-original.png",
                    "hosted_url": "https://cdn.example.com/logo.png",
                    "width": 1000,
                    "height": 300,
                    "created_at": "2026-03-01T00:00:00Z",
                    "metadata": {},
                },
                {
                    "id": "legacy-poster",
                    "source": "fanart",
                    "kind": "poster",
                    "image_type": "poster",
                    "url": "https://fanart.example.com/poster.jpg",
                    "url_original": "https://fanart.example.com/poster-original.jpg",
                    "hosted_url": "https://cdn.example.com/poster.jpg",
                    "width": 1000,
                    "height": 1500,
                    "created_at": "2026-03-02T00:00:00Z",
                    "metadata": {
                        "thumb_url": "https://cdn.example.com/poster-thumb.jpg",
                        "display_url": "https://cdn.example.com/poster-display.jpg",
                    },
                },
            ]
        raise AssertionError(f"unexpected query: {query[:80]}")

    monkeypatch.setattr(repo, "_fetch_all_rows", fake_fetch_all)

    assets, query_count = repo.get_show_assets("show-1", limit=10, offset=0, sources=["tmdb", "fanart"])

    assert query_count == 2
    assert len(assets) == 2
    assert assets[0]["id"] == "asset-1"
    assert assets[0]["logo_link_is_primary"] is True
    assert assets[0]["logo_black_url"] == "https://cdn.example.com/logo-black.png"
    assert assets[0]["thumbnail_crop_mode"] == "manual"
    assert assets[1]["id"] == "legacy-poster"
    assert assets[1]["thumb_url"] == "https://cdn.example.com/poster-thumb.jpg"
    assert assets[1]["display_url"] == "https://cdn.example.com/poster-display.jpg"
    assert [asset["hosted_url"] for asset in assets] == [
        "https://cdn.example.com/logo.png",
        "https://cdn.example.com/poster.jpg",
    ]


def test_shape_show_cast_payload_defaults_to_episode_evidence_and_falls_back() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {
                "person_id": "person-1",
                "full_name": "Fallback Person",
                "photo_url": None,
                "total_episodes": 0,
                "archive_episode_count": 1,
            }
        ],
        limit=20,
        offset=0,
        min_episodes=None,
        has_explicit_min_episodes=False,
        exclude_zero_episode_members=False,
        require_image=False,
        roster_mode="episode_evidence",
    )

    assert payload["cast_source"] == "show_fallback"
    assert payload["eligibility_warning"] is not None
    assert payload["cast"][0]["person_id"] == "person-1"
    assert payload["archive_footage_cast"][0]["person_id"] == "person-1"


def test_shape_show_cast_payload_respects_membership_mode_and_filters() -> None:
    payload = repo._shape_show_cast_payload(
        [
            {"person_id": "person-1", "photo_url": None, "total_episodes": 0, "archive_episode_count": 0},
            {
                "person_id": "person-2",
                "photo_url": "https://cdn.example.com/photo.jpg",
                "total_episodes": 5,
                "archive_episode_count": 0,
            },
        ],
        limit=20,
        offset=0,
        min_episodes=0,
        has_explicit_min_episodes=True,
        exclude_zero_episode_members=True,
        require_image=True,
        roster_mode="imdb_show_membership",
    )

    assert payload["cast_source"] == "imdb_show_membership"
    assert payload["eligibility_warning"] is None
    assert payload["cast"] == [
        {
            "person_id": "person-2",
            "photo_url": "https://cdn.example.com/photo.jpg",
            "total_episodes": 5,
            "archive_episode_count": 0,
        }
    ]


def test_shape_season_cast_payload_suppresses_fallback_for_archive_only() -> None:
    payload = repo._shape_season_cast_payload(
        [
            {
                "person_id": "person-1",
                "person_name": "Archive Person",
                "episodes_in_season": 0,
                "total_episodes": 8,
                "photo_url": None,
                "archive_episodes_in_season": 2,
            }
        ],
        limit=20,
        offset=0,
        include_archive_only=True,
    )

    assert payload["cast_source"] == "season_evidence"
    assert payload["eligibility_warning"] is None
    assert payload["include_archive_only"] is True
    assert payload["cast"][0]["archive_episodes_in_season"] == 2
