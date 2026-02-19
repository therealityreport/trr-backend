from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import scripts.sync.sync_networks_streaming_links as mod
from trr_backend.media.s3_mirror import MonochromeLogoMirrorResult


def _args(**overrides) -> argparse.Namespace:
    base = {
        "all": True,
        "force": False,
        "dry_run": False,
        "skip_s3": True,
        "limit": None,
        "verbose": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_resolve_entity_metadata_extracts_wikipedia_and_logo_file() -> None:
    entity = {
        "sitelinks": {"enwiki": {"title": "Bravo (American TV network)"}},
        "claims": {
            "P154": [
                {
                    "mainsnak": {
                        "datavalue": {"value": "Bravo TV logo.svg"},
                    }
                }
            ]
        },
    }
    with patch.object(mod, "_fetch_wikidata_entity", return_value=entity) as fetch_entity:
        with patch.object(mod, "_search_wikidata_item") as search_item:
            out = mod._resolve_entity_metadata("Bravo", "Q1519874")

    assert out["wikidata_id"] == "Q1519874"
    assert out["wikipedia_url"] == "https://en.wikipedia.org/wiki/Bravo_%28American_TV_network%29"
    assert out["wikimedia_logo_file"] == "Bravo TV logo.svg"
    fetch_entity.assert_called_once_with("Q1519874")
    search_item.assert_not_called()


def test_list_rows_filters_to_used_name_scope() -> None:
    fake_db = MagicMock()
    order_query = fake_db.schema.return_value.table.return_value.select.return_value.order.return_value
    rows = [
        {"id": 1, "name": "Bravo", "hosted_logo_url": None},
        {"id": 2, "name": "Unused Network", "hosted_logo_url": None},
    ]
    with patch.object(mod, "_iter_rows_paged", return_value=iter(rows)):
        selected = mod._list_rows(
            fake_db,
            table="networks",
            id_field="id",
            name_field="name",
            used_name_keys={"bravo"},
            limit=None,
        )

    assert order_query is not None
    assert [row["id"] for row in selected] == [1]
    assert selected[0]["_name"] == "Bravo"


def test_sync_table_does_not_overwrite_existing_links_without_force() -> None:
    rows = [
        {
            "id": 10,
            "_name": "Bravo",
            "hosted_logo_url": "https://cdn.example.com/existing.png",
            "hosted_logo_key": "images/logos/networks/10/existing.png",
            "hosted_logo_sha256": "abc",
            "hosted_logo_black_url": "https://cdn.example.com/black.png",
            "hosted_logo_white_url": "https://cdn.example.com/white.png",
            "wikidata_id": "Q1519874",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
            "wikimedia_logo_file": "Bravo TV logo.svg",
        }
    ]
    summary = mod.SyncSummary()

    with patch.object(mod, "_list_rows", return_value=rows):
        with patch.object(
            mod,
            "_resolve_entity_metadata",
            return_value={
                "wikidata_id": "Q999999",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Updated",
                "wikimedia_logo_file": "Updated.svg",
            },
        ):
            with patch.object(mod, "_update_row") as update_row:
                mod._sync_table(
                    db=object(),
                    table="networks",
                    id_field="id",
                    name_field="name",
                    row_type="network",
                    logo_kind="networks",
                    used_name_keys={"bravo"},
                    args=_args(force=False, skip_s3=True),
                    summary=summary,
                    s3_client=None,
                )

    assert summary.processed == 1
    assert summary.links_enriched == 0
    assert summary.unresolved_logos == []
    update_row.assert_not_called()


def test_sync_table_mirrors_missing_logo_and_bw_variants() -> None:
    rows = [
        {
            "provider_id": 531,
            "_name": "Peacock",
            "hosted_logo_url": None,
            "hosted_logo_key": None,
            "hosted_logo_sha256": None,
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "wikidata_id": None,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        }
    ]
    summary = mod.SyncSummary()

    with patch.object(mod, "_list_rows", return_value=rows):
        with patch.object(
            mod,
            "_resolve_entity_metadata",
            return_value={
                "wikidata_id": "Q111",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Peacock_(streaming_service)",
                "wikimedia_logo_file": "Peacock logo.svg",
            },
        ):
            with patch.object(
                mod,
                "mirror_external_logo_row",
                return_value={"hosted_logo_url": "https://cdn.example.com/logo.png"},
            ) as mirror_logo:
                with patch.object(
                    mod,
                    "mirror_logo_monochrome_variants_row",
                    return_value=MonochromeLogoMirrorResult(
                        patch={
                            "hosted_logo_black_url": "https://cdn.example.com/logo-black.png",
                            "hosted_logo_white_url": "https://cdn.example.com/logo-white.png",
                        },
                        black_mirrored=1,
                        white_mirrored=1,
                    ),
                ) as mirror_variants:
                    with patch.object(mod, "_update_row") as update_row:
                        mod._sync_table(
                            db=object(),
                            table="watch_providers",
                            id_field="provider_id",
                            name_field="provider_name",
                            row_type="streaming",
                            logo_kind="watch-providers",
                            used_name_keys={"peacock"},
                            args=_args(skip_s3=False, dry_run=False),
                            summary=summary,
                            s3_client=object(),
                        )

    assert summary.processed == 1
    assert summary.links_enriched == 1
    assert summary.wikidata_linked == 1
    assert summary.wikipedia_linked == 1
    assert summary.logos_mirrored == 1
    assert summary.variants_black_mirrored == 1
    assert summary.variants_white_mirrored == 1
    assert summary.unresolved_logos == []
    mirror_logo.assert_called_once()
    mirror_variants.assert_called_once()
    update_row.assert_called_once()


def test_sync_table_records_unresolved_reason_for_variant_failure() -> None:
    rows = [
        {
            "id": 22,
            "_name": "Bravo",
            "hosted_logo_url": None,
            "hosted_logo_key": None,
            "hosted_logo_sha256": None,
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "wikidata_id": None,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        }
    ]
    summary = mod.SyncSummary()

    with patch.object(mod, "_list_rows", return_value=rows):
        with patch.object(
            mod,
            "_resolve_entity_metadata",
            return_value={
                "wikidata_id": "Q1519874",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
                "wikimedia_logo_file": "Bravo TV logo.svg",
            },
        ):
            with patch.object(
                mod,
                "mirror_external_logo_row",
                return_value={"hosted_logo_url": "https://cdn.example.com/bravo.png"},
            ):
                with patch.object(
                    mod,
                    "mirror_logo_monochrome_variants_row",
                    side_effect=RuntimeError("transparent_extraction_failed"),
                ):
                    with patch.object(mod, "_update_row"):
                        mod._sync_table(
                            db=object(),
                            table="networks",
                            id_field="id",
                            name_field="name",
                            row_type="network",
                            logo_kind="networks",
                            used_name_keys={"bravo"},
                            args=_args(skip_s3=False, dry_run=False),
                            summary=summary,
                            s3_client=object(),
                        )

    assert summary.logos_mirrored == 1
    assert len(summary.unresolved_logos) == 1
    assert summary.unresolved_logos[0].reason == "transparent_extraction_failed"


def test_run_sync_passes_used_scope_sets_to_tables() -> None:
    seen: list[tuple[str, str, set[str]]] = []

    def fake_sync_table(
        db,
        *,
        table,
        id_field,
        name_field,
        row_type,
        logo_kind,
        used_name_keys,
        args,
        summary,
        s3_client,
    ):
        seen.append((table, row_type, set(used_name_keys)))

    with patch.object(mod, "load_env"):
        with patch.object(mod, "create_supabase_admin_client", return_value=object()):
            with patch.object(mod, "_collect_used_network_keys", return_value={"bravo"}):
                with patch.object(mod, "_collect_used_provider_keys", return_value={"peacock"}):
                    with patch.object(mod, "_sync_table", side_effect=fake_sync_table):
                        mod.run_sync(_args(skip_s3=True))

    assert seen == [
        ("networks", "network", {"bravo"}),
        ("watch_providers", "streaming", {"peacock"}),
    ]
