from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import scripts.sync.sync_networks_streaming_links as mod


def _args(**overrides) -> argparse.Namespace:
    base = {
        "all": True,
        "force": False,
        "dry_run": False,
        "skip_s3": True,
        "unresolved_only": False,
        "limit": None,
        "verbose": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_resolve_entity_metadata_uses_alias_candidates() -> None:
    entity = {
        "sitelinks": {"enwiki": {"title": "A%26E"}},
        "claims": {
            "P154": [
                {
                    "mainsnak": {
                        "datavalue": {"value": "AandE logo.svg"},
                    }
                }
            ]
        },
    }

    with patch.object(mod, "_search_wikidata_item", side_effect=[None, "Q34794"]) as search_item:
        with patch.object(mod, "_fetch_wikidata_entity", return_value=entity) as fetch_entity:
            out = mod._resolve_entity_metadata("A&E", None, ["A and E"])

    assert out["wikidata_id"] == "Q34794"
    assert out["wikipedia_url"] == "https://en.wikipedia.org/wiki/A%2526E"
    assert out["wikimedia_logo_file"] == "AandE logo.svg"
    assert search_item.call_count == 2
    fetch_entity.assert_called_once_with("Q34794")


def test_to_pg_text_array_literal_escapes_values() -> None:
    out = mod._to_pg_text_array_literal(["override", 'tm"db', r"wikimedia\logos", ""])
    assert out == '{"override","tm\\"db","wikimedia\\\\logos"}'


def test_extract_enwiki_url_falls_back_to_non_en_wikipedia() -> None:
    entity = {
        "sitelinks": {
            "dewiki": {"title": "Peacock_(Streamingdienst)"},
        }
    }
    out = mod._extract_enwiki_url(entity)
    assert out == "https://de.wikipedia.org/wiki/Peacock_%28Streamingdienst%29"


def test_derive_metadata_aliases_strips_streaming_suffixes() -> None:
    aliases = mod._derive_metadata_aliases("streaming", "Paramount+ Originals Amazon Channel")
    keys = {item.casefold() for item in aliases}
    assert "paramount+ originals amazon channel" in keys
    assert "paramount+ originals" in keys
    assert "paramount+" in keys


def test_build_logo_candidates_includes_override_tmdb_and_wikimedia() -> None:
    override = mod.OverrideConfig(
        id="1",
        entity_type="network",
        entity_key="bravo",
        display_name_override=None,
        wikidata_id_override=None,
        wikipedia_url_override=None,
        aliases_override=[],
        source_priority_override=[],
        logo_source_urls_by_source={
            "official": ["https://cdn.example.com/bravo-official.png"],
            "catalog": ["https://catalog.example.com/bravo.svg"],
        },
    )
    core_row = {"tmdb_logo_path": "/bravo.png"}

    candidates = mod._build_logo_candidates(
        override=override,
        core_row=core_row,
        wikimedia_logo_file="Bravo logo.svg",
    )

    assert candidates["official"] == ["https://cdn.example.com/bravo-official.png"]
    assert candidates["catalog"] == ["https://catalog.example.com/bravo.svg"]
    assert candidates["tmdb"] == ["https://image.tmdb.org/t/p/original/bravo.png"]
    assert candidates["wikimedia"][0].startswith("https://commons.wikimedia.org/wiki/Special:FilePath/Bravo_logo.svg")


def test_build_resolution_status_resolved_requires_all_fields() -> None:
    status, reason = mod._build_resolution_status(
        wikidata_id="Q1",
        wikipedia_url="https://en.wikipedia.org/wiki/Test",
        hosted_logo_url="https://cdn.example.com/logo.png",
        hosted_logo_black_url="https://cdn.example.com/logo-black.png",
        hosted_logo_white_url="https://cdn.example.com/logo-white.png",
        base_logo_format="png",
        reason=None,
    )
    assert status == "resolved"
    assert reason is None


def test_build_resolution_status_failed_for_processing_errors() -> None:
    status, reason = mod._build_resolution_status(
        wikidata_id="Q1",
        wikipedia_url="https://en.wikipedia.org/wiki/Test",
        hosted_logo_url="https://cdn.example.com/logo.png",
        hosted_logo_black_url="",
        hosted_logo_white_url="",
        base_logo_format="unknown",
        reason="transparent_extraction_failed",
    )
    assert status == "failed"
    assert reason == "transparent_extraction_failed"


def test_process_entity_uses_override_metadata_when_wikidata_search_empty() -> None:
    entity = mod.InventoryEntity(
        entity_type="network",
        entity_key="bravo",
        display_name="Bravo",
        available_show_count=10,
        added_show_count=4,
    )
    core_row = {
        "id": 77,
        "name": "Bravo",
        "hosted_logo_url": "https://cdn.example.com/bravo.png",
        "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
        "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
        "wikidata_id": None,
        "wikipedia_url": None,
        "wikimedia_logo_file": None,
    }
    override = mod.OverrideConfig(
        id="1",
        entity_type="network",
        entity_key="bravo",
        display_name_override="Bravo",
        wikidata_id_override="Q1519874",
        wikipedia_url_override="https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
        aliases_override=[],
        source_priority_override=[],
        logo_source_urls_by_source={},
    )
    summary = mod.SyncSummary()

    with patch.object(
        mod,
        "_resolve_entity_metadata",
        return_value={
            "wikidata_id": None,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        },
    ):
        mod._process_entity(
            db=object(),
            entity=entity,
            core_row=core_row,
            override=override,
            run_id="test-run",
            args=_args(skip_s3=True, dry_run=True),
            summary=summary,
            s3_client=None,
        )

    assert summary.processed == 1
    assert summary.wikidata_linked == 1
    assert summary.wikipedia_linked == 1
    assert summary.links_enriched == 1
    assert summary.unresolved_logos == []


def test_process_entity_records_unresolved_when_variant_generation_fails() -> None:
    entity = mod.InventoryEntity(
        entity_type="streaming",
        entity_key="peacock",
        display_name="Peacock",
        available_show_count=8,
        added_show_count=3,
    )
    core_row = {
        "provider_id": 531,
        "provider_name": "Peacock",
        "hosted_logo_url": None,
        "hosted_logo_black_url": None,
        "hosted_logo_white_url": None,
        "wikidata_id": None,
        "wikipedia_url": None,
        "wikimedia_logo_file": None,
    }
    summary = mod.SyncSummary()

    with patch.object(
        mod,
        "_resolve_entity_metadata",
        return_value={
            "wikidata_id": "Q111",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Peacock_(streaming_service)",
            "wikimedia_logo_file": "Peacock_logo.svg",
        },
    ):
        with patch.object(
            mod,
            "mirror_external_logo_row",
            return_value={"hosted_logo_url": "https://cdn.example.com/peacock.png"},
        ):
            with patch.object(
                mod,
                "mirror_logo_monochrome_variants_row",
                side_effect=RuntimeError("transparent_extraction_failed"),
            ):
                with patch.object(mod, "_update_core_row"):
                    with patch.object(mod, "_upsert_completion", return_value={"id": "completion-1"}):
                        with patch.object(mod, "_insert_attempts"):
                            mod._process_entity(
                                db=object(),
                                entity=entity,
                                core_row=core_row,
                                override=None,
                                run_id="test-run",
                                args=_args(skip_s3=False, dry_run=False),
                                summary=summary,
                                s3_client=object(),
                            )

    assert summary.logos_mirrored == 1
    assert len(summary.unresolved_logos) == 1
    assert summary.unresolved_logos[0].reason == "transparent_extraction_failed"


def test_run_sync_filters_to_unresolved_only() -> None:
    inventory = {
        ("network", "bravo"): mod.InventoryEntity(
            entity_type="network",
            entity_key="bravo",
            display_name="Bravo",
            available_show_count=10,
            added_show_count=4,
        ),
        ("streaming", "peacock"): mod.InventoryEntity(
            entity_type="streaming",
            entity_key="peacock",
            display_name="Peacock",
            available_show_count=8,
            added_show_count=3,
        ),
    }
    seen: list[tuple[str, str]] = []

    def fake_process_entity(_db, **kwargs):  # noqa: ANN003
        entity = kwargs["entity"]
        seen.append((entity.entity_type, entity.entity_key))

    with patch.object(mod, "load_env"):
        with patch.object(mod, "create_supabase_admin_client", return_value=object()):
            with patch.object(mod, "_load_used_inventory", return_value=inventory):
                with patch.object(mod, "_load_dimension_lookup", return_value={}):
                    with patch.object(mod, "_load_overrides", return_value={}):
                            with patch.object(mod, "_load_unresolved_keys", return_value={("network", "bravo")}):
                                with patch.object(mod, "_process_entity", side_effect=fake_process_entity):
                                    with patch.object(mod, "_refresh_completion_snapshot"):
                                        with patch.object(
                                            mod,
                                            "_build_sync_context",
                                            return_value=mod.SyncRunContext(
                                                tmdb_api_key=None,
                                                tmdb_bearer_token=None,
                                            ),
                                        ):
                                            mod.run_sync(_args(unresolved_only=True))

    assert seen == [("network", "bravo")]


def test_load_unresolved_keys_includes_missing_completion_rows() -> None:
    used_keys = {("network", "bravo"), ("streaming", "peacock")}

    db = MagicMock()
    query = db.schema.return_value.table.return_value.select.return_value.order.return_value
    query.execute.return_value = MagicMock(data=[])

    rows = [
        {
            "entity_type": "network",
            "entity_key": "bravo",
            "resolution_status": "resolved",
        }
    ]

    with patch.object(mod, "_iter_rows_paged", return_value=iter(rows)):
        unresolved = mod._load_unresolved_keys(db, used_keys=used_keys)

    assert unresolved == {("streaming", "peacock")}


def test_upsert_completion_converts_source_priority_to_pg_array() -> None:
    db = MagicMock()
    upsert_query = db.schema.return_value.table.return_value.upsert.return_value
    upsert_query.execute.return_value = MagicMock(error=None, data=[{"id": "row-1"}])

    row = {
        "entity_type": "network",
        "entity_key": "bravo",
        "source_priority": ["override", "tmdb", "wikimedia"],
    }

    _ = mod._upsert_completion(db, row)

    payload = db.schema.return_value.table.return_value.upsert.call_args.args[0]
    assert payload["source_priority"] == '{"override","tmdb","wikimedia"}'
