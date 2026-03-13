from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from api.routers import admin_brands


def test_seed_logo_targets_from_entity_links_maps_and_filters_hosts() -> None:
    rows = [
        {
            "show_id": "show-1",
            "entity_type": "show",
            "entity_id": "show-1",
            "season_number": 0,
            "link_kind": "instagram",
            "label": "Instagram",
            "url": "https://www.instagram.com/thetraitors",
        },
        {
            "show_id": "show-1",
            "entity_type": "show",
            "entity_id": "show-1",
            "season_number": 0,
            "link_kind": "wikipedia",
            "label": "Wikipedia",
            "url": "https://deadline.com/the-traitors-news",
        },
        {
            "show_id": "show-1",
            "entity_type": "show",
            "entity_id": "show-1",
            "season_number": 0,
            "link_kind": "twitter",
            "label": "Twitter",
            "url": "http://localhost:3000/internal",
        },
    ]

    with patch("api.routers.admin_brands.pg.fetch_all", return_value=rows):
        targets = admin_brands._seed_logo_targets_from_entity_links(show_id="show-1")

    by_type_key = {(row["target_type"], row["target_key"]) for row in targets}
    assert ("social", "instagram.com") in by_type_key
    assert ("publication", "deadline.com") in by_type_key
    assert all(target["target_key"] != "localhost" for target in targets)


def test_brand_logo_assets_variant_select_sql_falls_back_when_columns_missing() -> None:
    admin_brands._brand_logo_assets_variant_columns.cache_clear()
    with patch("api.routers.admin_brands.pg.fetch_all", return_value=[]):
        sql = admin_brands._brand_logo_assets_variant_select_sql()
    assert "null::text as hosted_logo_black_url" in sql
    assert "null::text as hosted_logo_white_url" in sql


def test_brand_logo_assets_variant_select_sql_uses_columns_when_present() -> None:
    admin_brands._brand_logo_assets_variant_columns.cache_clear()
    with patch(
        "api.routers.admin_brands.pg.fetch_all",
        return_value=[
            {"column_name": "hosted_logo_black_url"},
            {"column_name": "hosted_logo_white_url"},
        ],
    ):
        sql = admin_brands._brand_logo_assets_variant_select_sql()
    assert "hosted_logo_black_url as hosted_logo_black_url" in sql
    assert "hosted_logo_white_url as hosted_logo_white_url" in sql


def test_network_streaming_variant_select_sql_falls_back_when_columns_missing() -> None:
    admin_brands._network_streaming_logo_assets_variant_columns.cache_clear()
    with patch("api.routers.admin_brands.pg.fetch_all", return_value=[]):
        sql = admin_brands._network_streaming_variant_select_sql()
    assert "null::text as hosted_logo_black_url" in sql
    assert "null::text as hosted_logo_white_url" in sql


def test_network_streaming_variant_select_sql_uses_columns_when_present() -> None:
    admin_brands._network_streaming_logo_assets_variant_columns.cache_clear()
    with patch(
        "api.routers.admin_brands.pg.fetch_all",
        return_value=[
            {"column_name": "hosted_logo_black_url"},
            {"column_name": "hosted_logo_white_url"},
        ],
    ):
        sql = admin_brands._network_streaming_variant_select_sql()
    assert "hosted_logo_black_url as hosted_logo_black_url" in sql
    assert "hosted_logo_white_url as hosted_logo_white_url" in sql


def test_find_related_network_assets_returns_empty_on_missing_variant_columns() -> None:
    admin_brands._network_streaming_logo_assets_variant_columns.cache_clear()
    missing_column_error = RuntimeError('column "hosted_logo_black_url" does not exist')
    with patch(
        "api.routers.admin_brands.pg.fetch_all",
        side_effect=[[], missing_column_error, []],
    ):
        rows = admin_brands._find_related_network_streaming_assets_by_host(
            target_type="publication",
            target_host="imdb.com",
            logo_role="wordmark",
            limit=20,
        )
    assert rows == []


def test_fetch_all_with_logo_variant_fallback_retries_with_null_variants() -> None:
    calls: list[str] = []

    def _fake_fetch_all(query: str, _params: list[object] | None = None) -> list[dict[str, object]]:
        calls.append(query)
        if len(calls) == 1:
            raise RuntimeError('column "hosted_logo_black_url" does not exist')
        return [{"ok": True}]

    with patch("api.routers.admin_brands.pg.fetch_all", side_effect=_fake_fetch_all):
        rows = admin_brands._fetch_all_with_logo_variant_fallback(
            query_builder=lambda variant: f"select {variant} 1 as ok",
            params=[],
            variant_sql=(
                "hosted_logo_black_url as hosted_logo_black_url, hosted_logo_white_url as hosted_logo_white_url,"
            ),
            cache_clear=lambda: None,
            fallback_key="test-fallback",
            fallback_message="test",
        )

    assert rows == [{"ok": True}]
    assert any("null::text as hosted_logo_black_url" in query for query in calls)


def test_list_brand_logos_include_missing_adds_synthetic_role_rows() -> None:
    existing_rows = [
        {
            "id": "logo-1",
            "target_type": "publication",
            "target_key": "deadline.com",
            "target_label": "deadline.com",
            "source_url": "https://commons.wikimedia.org/file/deadline.svg",
            "source_page_url": None,
            "source_domain": "commons.wikimedia.org",
            "hosted_logo_url": "https://cdn.example.com/deadline.svg",
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "is_primary": True,
            "mirror_status": "mirrored",
            "failure_reason": None,
            "metadata": {"logo_role": "wordmark"},
            "logo_role": "wordmark",
            "source_provider": "wikimedia_commons",
            "discovered_from": "https://deadline.com/",
            "created_at": None,
            "updated_at": None,
        }
    ]

    with (
        patch("api.routers.admin_brands.pg.fetch_all", return_value=existing_rows),
        patch("api.routers.admin_brands.pg.fetch_one", return_value={"count": 1}),
        patch(
            "api.routers.admin_brands._list_logo_targets",
            return_value={
                "rows": [
                    {
                        "target_type": "publication",
                        "target_key": "deadline.com",
                        "target_label": "deadline.com",
                        "discovered_from": "https://deadline.com/",
                    }
                ],
                "count": 1,
            },
        ),
    ):
        payload = admin_brands._list_brand_logos(
            target_type="publication",
            q="",
            limit=50,
            offset=0,
            include_missing=True,
            show_id=None,
        )

    rows = payload["rows"]
    assert any(row.get("logo_role") == "wordmark" for row in rows)
    assert any(row.get("logo_role") == "icon" and row.get("hosted_logo_url") is None for row in rows)


def test_list_brand_logos_include_related_ignores_missing_related_variant_columns() -> None:
    admin_brands._brand_logo_assets_variant_columns.cache_clear()
    base_rows = [
        {
            "id": "logo-1",
            "target_type": "publication",
            "target_key": "imdb.com",
            "target_label": "imdb.com",
            "source_url": "https://commons.wikimedia.org/wiki/Special:FilePath/IMDB_logo.svg",
            "source_page_url": None,
            "source_domain": "commons.wikimedia.org",
            "hosted_logo_url": "https://cdn.example.com/imdb.svg",
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "is_primary": True,
            "mirror_status": "mirrored",
            "failure_reason": None,
            "metadata": {"logo_role": "wordmark"},
            "logo_role": "wordmark",
            "source_provider": "wikimedia_commons",
            "discovered_from": "https://imdb.com/",
            "option_kind": "stored",
            "origin_target_type": "publication",
            "created_at": None,
            "updated_at": None,
        }
    ]
    with (
        patch("api.routers.admin_brands.pg.fetch_all", return_value=base_rows),
        patch(
            "api.routers.admin_brands._find_related_network_streaming_assets_by_host",
            side_effect=RuntimeError('column "hosted_logo_black_url" does not exist'),
        ),
    ):
        payload = admin_brands._list_brand_logos(
            target_type="publication",
            q="",
            limit=50,
            offset=0,
            include_missing=False,
            target_key="imdb.com",
            logo_role="wordmark",
            source_provider=None,
            include_related=True,
            show_id=None,
        )
    assert payload["count"] >= 1
    assert payload["rows"][0]["target_key"] == "imdb.com"


def test_resolve_sync_target_types_falls_back_to_page_defaults() -> None:
    payload = admin_brands.BrandLogosSyncRequest(scope="page", page="news")
    assert admin_brands._resolve_sync_target_types(payload) == ["publication", "social"]


def test_sync_brand_logos_validates_required_scope_fields() -> None:
    db = SimpleNamespace()
    with pytest.raises(ValueError, match="page is required"):
        admin_brands._sync_brand_logos(
            payload=admin_brands.BrandLogosSyncRequest(scope="page", page=None),
            db=db,
        )
    with pytest.raises(ValueError, match="show_id is required"):
        admin_brands._sync_brand_logos(
            payload=admin_brands.BrandLogosSyncRequest(scope="show", show_id=""),
            db=db,
        )


def test_list_logo_option_sources_groups_counts() -> None:
    rows = [
        {"source_provider": "wikimedia_commons"},
        {"source_provider": "wikimedia_commons"},
        {"source_provider": "logos_fandom"},
    ]
    with (
        patch("api.routers.admin_brands._list_logo_options", return_value={"rows": rows, "count": 3}),
        patch(
            "api.routers.admin_brands._load_logo_source_query_overrides",
            return_value={"logos1000": ["custom-brand-logo", "custom-brand-mark"]},
        ),
    ):
        payload = admin_brands._list_logo_option_sources(
            target_type="publication",
            target_key="instagram.com",
            target_label="Instagram",
            logo_role="wordmark",
            include_related=True,
        )
    source_map = {row["source_provider"]: row for row in payload["sources"]}
    assert payload["sources"][0]["source_provider"] == "related_network_streaming"
    assert source_map["wikimedia_commons"]["total_count"] == 2
    assert source_map["wikimedia_commons"]["query_kind"] == "search_term"
    assert source_map["logos1000"]["query_kind"] == "slug"
    assert source_map["logos1000"]["effective_query_value"] == "custom-brand-logo"
    assert source_map["logos1000"]["query_values"] == ["custom-brand-logo", "custom-brand-mark"]
    assert source_map["logos_fandom"]["total_count"] == 1
    assert source_map["worldvectorlogo"]["total_count"] == 0
    assert source_map["worldvectorlogo"]["has_more"] is True


def test_list_logo_option_sources_falls_back_without_related_on_variant_error() -> None:
    with patch(
        "api.routers.admin_brands._list_logo_options",
        side_effect=[
            RuntimeError('column "hosted_logo_black_url" does not exist'),
            {"rows": [{"source_provider": "wikimedia_commons"}], "count": 1},
        ],
    ) as options_mock, patch(
        "api.routers.admin_brands._load_logo_source_query_overrides",
        return_value={},
    ):
        payload = admin_brands._list_logo_option_sources(
            target_type="publication",
            target_key="imdb.com",
            logo_role="wordmark",
            include_related=True,
        )

    assert payload["sources"][0]["source_provider"] == "related_network_streaming"
    source_map = {row["source_provider"]: row for row in payload["sources"]}
    assert source_map["wikimedia_commons"]["total_count"] == 1
    assert options_mock.call_args_list[1].kwargs["include_related"] is False


def test_discover_logo_candidates_filters_existing_without_dropping_nonmatching_roles() -> None:
    candidates = [
        SimpleNamespace(
            url="https://cdn.example.com/instagram-wordmark.svg",
            source_provider="wikimedia_commons",
            discovered_from="https://commons.wikimedia.org/wiki/File:Instagram.svg",
        ),
        SimpleNamespace(
            url="https://cdn.example.com/instagram-icon.png",
            source_provider="official_site",
            discovered_from="https://instagram.com",
        ),
    ]
    existing_rows = [
        {"source_url": "https://cdn.example.com/already-there.svg", "discovered_from": "https://instagram.com"},
    ]
    with (
        patch("api.routers.admin_brands._list_brand_logos", return_value={"rows": existing_rows, "count": 1}),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=candidates,
        ),
    ):
        payload = admin_brands._discover_logo_candidates_by_source(
            admin_brands.BrandLogosOptionDiscoverRequest(
                target_type="publication",
                target_key="instagram.com",
                target_label="instagram.com",
                logo_role="icon",
                offset=0,
                limit=10,
            )
        )
    assert len(payload["candidates"]) == 2
    assert {row["source_url"] for row in payload["candidates"]} == {
        "https://cdn.example.com/instagram-wordmark.svg",
        "https://cdn.example.com/instagram-icon.png",
    }
    assert all(row["logo_role"] == "icon" for row in payload["candidates"])


def test_discover_logo_candidates_passes_source_provider_filter_to_collection() -> None:
    existing_rows = [{"source_url": "https://cdn.example.com/already-there.svg"}]
    with (
        patch("api.routers.admin_brands._list_brand_logos", return_value={"rows": existing_rows, "count": 1}),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=[],
        ) as collect_mock,
    ):
        admin_brands._discover_logo_candidates_by_source(
            admin_brands.BrandLogosOptionDiscoverRequest(
                target_type="publication",
                target_key="instagram.com",
                target_label="instagram.com",
                logo_role="wordmark",
                source_provider="wikimedia_commons",
                query_override="instagram svg",
                offset=0,
                limit=10,
            )
        )

    assert collect_mock.call_args.kwargs["source_provider"] == "wikimedia_commons"
    assert collect_mock.call_args.kwargs["query_override"] == "instagram svg"


def test_discover_logo_candidates_falls_back_without_related_on_variant_error() -> None:
    with (
        patch(
            "api.routers.admin_brands._list_brand_logos",
            side_effect=[
                RuntimeError('column "hosted_logo_black_url" does not exist'),
                {"rows": [], "count": 0},
            ],
        ) as logos_mock,
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=[],
        ),
    ):
        admin_brands._discover_logo_candidates_by_source(
            admin_brands.BrandLogosOptionDiscoverRequest(
                target_type="publication",
                target_key="imdb.com",
                target_label="imdb.com",
                logo_role="wordmark",
                offset=0,
                limit=10,
                include_related=True,
            )
        )

    assert logos_mock.call_count == 2
    assert logos_mock.call_args_list[1].kwargs["include_related"] is False


def test_discover_logo_candidates_returns_total_count() -> None:
    candidates = [
        SimpleNamespace(
            url=f"https://cdn.example.com/asset-{index}.svg",
            source_provider="wikimedia_commons",
            discovered_from=f"https://commons.wikimedia.org/wiki/File:Asset_{index}.svg",
        )
        for index in range(3)
    ]
    with (
        patch("api.routers.admin_brands._list_brand_logos", return_value={"rows": [], "count": 0}),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=candidates,
        ),
    ):
        payload = admin_brands._discover_logo_candidates_by_source(
            admin_brands.BrandLogosOptionDiscoverRequest(
                target_type="publication",
                target_key="instagram.com",
                target_label="Instagram",
                logo_role="wordmark",
                source_provider="wikimedia_commons",
                offset=0,
                limit=2,
            )
        )

    assert payload["total_count"] == 3
    assert len(payload["candidates"]) == 2


def test_save_logo_source_query_upserts_and_resets_override() -> None:
    with (
        patch("api.routers.admin_brands._upsert_logo_source_query_override") as upsert_mock,
        patch("api.routers.admin_brands._delete_logo_source_query_override") as delete_mock,
        patch(
            "api.routers.admin_brands._load_logo_source_query_overrides",
            side_effect=[{"logos1000": ["peacock-logo", "peacock-symbol"]}, {}],
        ),
    ):
        saved = admin_brands._save_logo_source_query(
            admin_brands.BrandLogosSourceQueryRequest(
                target_type="publication",
                target_key="peacocktv.com",
                target_label="peacocktv.com",
                logo_role="wordmark",
                source_provider="logos1000",
                query_values=["/peacock-logo/", "peacock-symbol"],
            )
        )
        reset = admin_brands._save_logo_source_query(
            admin_brands.BrandLogosSourceQueryRequest(
                target_type="publication",
                target_key="peacocktv.com",
                target_label="peacocktv.com",
                logo_role="wordmark",
                source_provider="logos1000",
                query_value="",
            )
        )

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args.kwargs["query_values"] == ["peacock-logo", "peacock-symbol"]
    delete_mock.assert_called_once()
    assert saved["source"]["effective_query_value"] == "peacock-logo"
    assert saved["source"]["query_values"] == ["peacock-logo", "peacock-symbol"]
    assert reset["source"]["effective_query_value"] == "peacocktv-logo"


def test_save_logo_source_query_raises_explicit_migration_error_for_multi_query_stale_schema() -> None:
    with patch(
        "api.routers.admin_brands._upsert_logo_source_query_override",
        side_effect=RuntimeError(admin_brands._QUERY_VALUES_MIGRATION_ERROR),
    ):
        with pytest.raises(RuntimeError, match="query_values"):
            admin_brands._save_logo_source_query(
                admin_brands.BrandLogosSourceQueryRequest(
                    target_type="publication",
                    target_key="peacocktv.com",
                    target_label="peacocktv.com",
                    logo_role="wordmark",
                    source_provider="logos1000",
                    query_values=["peacock-logo", "peacock-symbol"],
                )
            )


def test_upsert_logo_source_query_override_single_value_falls_back_without_query_values_column() -> None:
    calls: list[tuple[str, list[object] | None]] = []

    def _fake_fetch_one(query: str, params: list[object] | None = None) -> dict[str, object]:
        calls.append((query, params))
        if len(calls) == 1:
            raise RuntimeError('column "query_values" of relation "brand_logo_source_queries" does not exist')
        return {"target_type": "publication"}

    with patch("api.routers.admin_brands.pg.fetch_one", side_effect=_fake_fetch_one):
        admin_brands._upsert_logo_source_query_override(
            target_type="publication",
            target_key="peacocktv.com",
            logo_role="wordmark",
            source_provider="logos1000",
            query_values=["peacock-logo"],
        )

    assert len(calls) == 2
    assert "query_values" in calls[0][0]
    assert "query_values" not in calls[1][0]


def test_upsert_logo_source_query_override_multi_value_requires_query_values_column() -> None:
    with patch(
        "api.routers.admin_brands.pg.fetch_one",
        side_effect=RuntimeError('column "query_values" of relation "brand_logo_source_queries" does not exist'),
    ):
        with pytest.raises(RuntimeError, match="0178_brand_logo_source_query_values.sql"):
            admin_brands._upsert_logo_source_query_override(
                target_type="publication",
                target_key="peacocktv.com",
                logo_role="wordmark",
                source_provider="logos1000",
                query_values=["peacock-logo", "peacock-symbol"],
            )


def test_select_logo_option_skips_feature_selection_when_set_featured_false() -> None:
    selected_row = {
        "id": "asset-1",
        "source_url": "https://cdn.example.com/logo.svg",
        "source_provider": "logos1000",
    }

    with (
        patch("api.routers.admin_brands._fetch_logo_option_row", side_effect=[selected_row, selected_row, selected_row]),
        patch("api.routers.admin_brands._set_brand_role_selection") as set_brand_mock,
        patch("api.routers.admin_brands._set_network_role_selection") as set_network_mock,
        patch(
            "api.routers.admin_brands._selected_logo_role_summary",
            return_value={"wordmark": {"selected_asset_id": "existing-asset"}},
        ),
    ):
        result = admin_brands._select_logo_option(
            payload=admin_brands.BrandLogosOptionSelectRequest(
                target_type="publication",
                target_key="imdb.com",
                target_label="IMDb",
                logo_role="wordmark",
                asset_id="asset-1",
                set_featured=False,
            ),
            db=object(),
        )

    assert result["selected"]["id"] == "asset-1"
    set_brand_mock.assert_not_called()
    set_network_mock.assert_not_called()


def test_select_logo_option_features_candidate_when_set_featured_true() -> None:
    imported_row = {
        "id": "asset-1",
        "source_url": "https://cdn.example.com/logo.svg",
        "source_provider": "logos1000",
    }

    with (
        patch("api.routers.admin_brands._import_logo_option_candidate", return_value=imported_row) as import_mock,
        patch("api.routers.admin_brands._set_brand_role_selection") as set_brand_mock,
        patch("api.routers.admin_brands._fetch_logo_option_row", return_value=imported_row),
        patch(
            "api.routers.admin_brands._selected_logo_role_summary",
            return_value={"wordmark": {"selected_asset_id": "asset-1"}},
        ),
    ):
        result = admin_brands._select_logo_option(
            payload=admin_brands.BrandLogosOptionSelectRequest(
                target_type="publication",
                target_key="imdb.com",
                target_label="IMDb",
                logo_role="wordmark",
                candidate=admin_brands.BrandLogoDiscoverCandidateRequest(
                    source_url="https://cdn.example.com/logo.svg",
                    source_provider="logos1000",
                    discovered_from="https://1000logos.net/imdb-logo/",
                ),
                set_featured=True,
            ),
            db=object(),
        )

    assert result["selected"]["id"] == "asset-1"
    assert import_mock.call_args.kwargs["set_featured"] is True
    set_brand_mock.assert_called_once()


def test_sync_brand_logos_reports_related_pair_metrics() -> None:
    db = SimpleNamespace()
    with (
        patch(
            "api.routers.admin_brands._load_sync_targets",
            return_value=[
                {
                    "target_type": "publication",
                    "target_key": "peacocktv.com",
                    "target_label": "peacocktv.com",
                    "discovered_from": "https://peacocktv.com",
                    "discovered_from_urls": ["https://peacocktv.com"],
                }
            ],
        ),
        patch(
            "api.routers.admin_brands._load_existing_logo_role_flags",
            return_value={"wordmark": False, "icon": False, "wordmark_count": 0, "icon_count": 0},
        ),
        patch(
            "api.routers.admin_brands._load_related_pair_candidates_for_sync",
            return_value=[
                {
                    "logo_role": "wordmark",
                    "source_url": "https://assets.example.com/peacock-wordmark.svg",
                    "discovered_from": "https://peacocktv.com",
                },
                {
                    "logo_role": "icon",
                    "source_url": "https://assets.example.com/peacock-icon.svg",
                    "discovered_from": "https://peacocktv.com",
                },
            ],
        ),
        patch(
            "api.routers.admin_brands._sync_import_logo_source",
            return_value=(True, False),
        ),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            return_value=[],
        ),
        patch(
            "api.routers.admin_brands._load_logo_source_query_overrides",
            return_value={},
        ),
    ):
        result = admin_brands._sync_brand_logos(
            payload=admin_brands.BrandLogosSyncRequest(scope="page", page="news", only_missing=True, limit=10),
            db=db,
        )
    assert result["related_pairs_created"] == 2
    assert result["options_imported_wordmark"] >= 1
    assert result["options_imported_icon"] >= 1


def test_sync_brand_logos_uses_saved_source_query_overrides() -> None:
    override_url = "https://logos.fandom.com/wiki/Bravo_(United_States)/Other"
    override_candidate = SimpleNamespace(
        url="https://static.wikia.nocookie.net/logopedia/images/2/22/Bravo_2024.svg/revision/latest?cb=20240508041547",
        source_provider="logos_fandom",
        discovered_from=override_url,
        context="search",
    )
    collect_calls: list[dict[str, object]] = []

    def _fake_collect(**kwargs: object) -> list[SimpleNamespace]:
        collect_calls.append(dict(kwargs))
        if kwargs.get("source_provider") == "logos_fandom":
            return [override_candidate]
        return []

    db = SimpleNamespace()
    with (
        patch(
            "api.routers.admin_brands._load_sync_targets",
            return_value=[
                {
                    "target_type": "publication",
                    "target_key": "bravotv.com",
                    "target_label": "bravotv.com",
                    "discovered_from": "https://www.bravotv.com",
                    "discovered_from_urls": ["https://www.bravotv.com"],
                }
            ],
        ),
        patch(
            "api.routers.admin_brands._load_existing_logo_role_flags",
            return_value={"wordmark": True, "icon": True, "wordmark_count": 1, "icon_count": 1},
        ),
        patch(
            "api.routers.admin_brands._load_related_pair_candidates_for_sync",
            return_value=[],
        ),
        patch(
            "api.routers.admin_brands._load_logo_source_query_overrides",
            side_effect=[{"logos_fandom": [override_url]}, {}],
        ),
        patch(
            "api.routers.admin_brands.collect_free_logo_candidates",
            side_effect=_fake_collect,
        ),
        patch(
            "api.routers.admin_brands._detect_logo_role",
            return_value="wordmark",
        ),
        patch(
            "api.routers.admin_brands._sync_import_logo_source",
            return_value=(True, False),
        ) as import_mock,
    ):
        result = admin_brands._sync_brand_logos(
            payload=admin_brands.BrandLogosSyncRequest(scope="page", page="news", only_missing=True, limit=10),
            db=db,
        )

    assert any(call.get("source_provider") == "logos_fandom" for call in collect_calls)
    assert any(call.get("query_override") == [override_url] for call in collect_calls)
    import_mock.assert_called_once()
    assert import_mock.call_args.kwargs["discovered_from"] == override_url
    assert result["imports_created"] == 1
    assert result["options_imported_wordmark"] == 1
