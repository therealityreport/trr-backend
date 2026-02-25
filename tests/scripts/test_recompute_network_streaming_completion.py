from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import scripts.sync.recompute_network_streaming_completion as mod


def _args(**overrides) -> argparse.Namespace:
    base = {
        "entity_type": None,
        "limit": None,
        "dry_run": True,
        "verbose": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _mock_query_chain(mock_db: MagicMock) -> None:
    chain = mock_db.schema.return_value.table.return_value.select.return_value.order.return_value
    chain.eq.return_value = chain
    chain.limit.return_value = chain


def test_run_recompute_applies_production_logo_optional_policy() -> None:
    mock_db = MagicMock()
    _mock_query_chain(mock_db)
    rows = [
        {
            "entity_type": "production",
            "entity_key": "shed media",
            "entity_id": "13120",
            "display_name": "Shed Media",
            "wikidata_id": None,
            "wikipedia_url": None,
            "hosted_logo_url": None,
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "base_logo_format": "unknown",
            "resolution_status": "manual_required",
            "resolution_reason": "incomplete_metadata",
            "resolution_policy": "strict",
            "logo_required": True,
        },
        {
            "entity_type": "network",
            "entity_key": "bravo",
            "entity_id": "74",
            "display_name": "Bravo",
            "wikidata_id": "Q902771",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Bravo_(American_TV_network)",
            "hosted_logo_url": "https://cdn.example.com/bravo.png",
            "hosted_logo_black_url": "https://cdn.example.com/bravo-black.png",
            "hosted_logo_white_url": "https://cdn.example.com/bravo-white.png",
            "base_logo_format": "png",
            "resolution_status": "resolved",
            "resolution_reason": None,
            "resolution_policy": "strict",
            "logo_required": True,
        },
    ]

    with patch("scripts.sync.recompute_network_streaming_completion.load_env"):
        with patch(
            "scripts.sync.recompute_network_streaming_completion.create_supabase_admin_client",
            return_value=mock_db,
        ):
            with patch(
                "scripts.sync.recompute_network_streaming_completion.sync_links._iter_rows_paged",
                return_value=rows,
            ):
                summary = mod.run_recompute(_args(dry_run=True))

    assert summary.scanned == 2
    assert summary.updated == 1
    assert summary.resolved == 2
    assert summary.manual_required == 0
    assert summary.failed == 0
    mock_db.schema.return_value.table.return_value.update.assert_not_called()


def test_run_recompute_writes_updated_status_when_not_dry_run() -> None:
    mock_db = MagicMock()
    _mock_query_chain(mock_db)
    update_chain = (
        mock_db.schema.return_value.table.return_value.update.return_value.eq.return_value.eq.return_value
    )
    update_response = MagicMock()
    update_response.error = None
    update_chain.execute.return_value = update_response

    rows = [
        {
            "entity_type": "production",
            "entity_key": "shed media",
            "entity_id": "",
            "display_name": "Shed Media",
            "wikidata_id": "",
            "wikipedia_url": "",
            "hosted_logo_url": None,
            "hosted_logo_black_url": None,
            "hosted_logo_white_url": None,
            "base_logo_format": "unknown",
            "resolution_status": "resolved",
            "resolution_reason": None,
            "resolution_policy": "production_logo_optional",
            "logo_required": False,
        },
    ]

    with patch("scripts.sync.recompute_network_streaming_completion.load_env"):
        with patch(
            "scripts.sync.recompute_network_streaming_completion.create_supabase_admin_client",
            return_value=mock_db,
        ):
            with patch(
                "scripts.sync.recompute_network_streaming_completion.sync_links._iter_rows_paged",
                return_value=rows,
            ):
                summary = mod.run_recompute(_args(dry_run=False))

    assert summary.scanned == 1
    assert summary.updated == 1
    assert summary.manual_required == 1
    mock_db.schema.return_value.table.return_value.update.assert_called_once()
