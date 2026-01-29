"""Tests for _sync_common module, especially --skip-db functionality."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from scripts._sync_common import (
    fetch_show_rows,
    filter_show_rows_for_sync,
    load_env_and_db,
)


class TestLoadEnvAndDb:
    """Tests for load_env_and_db function."""

    def test_returns_none_when_skip_db(self) -> None:
        """Test load_env_and_db returns None when skip_db=True."""
        db = load_env_and_db(skip_db=True)
        assert db is None

    def test_returns_client_when_not_skip_db(self) -> None:
        """Test load_env_and_db returns Client when skip_db=False."""
        with (
            patch("scripts._sync_common.load_env"),
            patch("scripts._sync_common.assert_core_shows_table_exists") as assert_table,
            patch("scripts._sync_common.create_supabase_admin_client") as create_db_client,
        ):
            sentinel = MagicMock()
            create_db_client.return_value = sentinel
            db = load_env_and_db(skip_db=False)
            assert db is sentinel
            assert_table.assert_called_once_with(sentinel)


class TestFetchShowRows:
    """Tests for fetch_show_rows function."""

    def test_synthetic_rows_for_skip_db_with_imdb_id(self) -> None:
        """Test fetch_show_rows returns synthetic rows when db=None and imdb_id provided."""
        args = argparse.Namespace(
            all=False,
            show_id=[],
            tmdb_show_id=[],
            imdb_series_id=["tt1234567", "tt7654321"],
            limit=None,
        )

        rows = fetch_show_rows(None, args)

        assert len(rows) == 2
        assert rows[0]["imdb_id"] == "tt1234567"
        assert rows[1]["imdb_id"] == "tt7654321"
        assert "[Debug:" in rows[0]["name"]

    def test_error_for_skip_db_with_all(self) -> None:
        """Test fetch_show_rows errors when db=None and --all is used."""
        args = argparse.Namespace(
            all=True,
            show_id=[],
            tmdb_show_id=[],
            imdb_series_id=[],
            limit=None,
        )

        with pytest.raises(RuntimeError, match="--skip-db only supported with --imdb-id"):
            fetch_show_rows(None, args)

    def test_error_for_skip_db_without_specific_ids(self) -> None:
        """Test fetch_show_rows errors when db=None and no specific IDs provided."""
        args = argparse.Namespace(
            all=False,
            show_id=[],
            tmdb_show_id=[],
            imdb_series_id=[],
            limit=None,
        )

        with pytest.raises(RuntimeError, match="--skip-db only supported with --imdb-id"):
            fetch_show_rows(None, args)


class TestFilterShowRowsForSync:
    """Tests for filter_show_rows_for_sync function."""

    def test_skip_db_mode_returns_all_shows(self) -> None:
        """Test filter_show_rows_for_sync returns all shows when db=None."""
        show_rows = [
            {"imdb_id": "tt1234567", "name": "Show 1"},
            {"imdb_id": "tt7654321", "name": "Show 2"},
        ]

        result = filter_show_rows_for_sync(
            None,
            show_rows,
            table_name="test_table",
            incremental=True,
            resume=True,
            force=False,
            verbose=False,
        )

        assert len(result.selected) == 2
        assert len(result.skipped) == 0
        assert result.reasons == {"skip-db": 2}

    def test_skip_db_mode_with_verbose(self, capsys) -> None:
        """Test filter_show_rows_for_sync prints skip-db message when verbose."""
        show_rows = [{"imdb_id": "tt1234567", "name": "Show 1"}]

        filter_show_rows_for_sync(
            None,
            show_rows,
            table_name="test_table",
            incremental=True,
            resume=True,
            force=False,
            verbose=True,
        )

        captured = capsys.readouterr()
        assert "skip-db mode" in captured.out
        assert "selecting all 1 shows" in captured.out

    def test_normal_mode_requires_db(self) -> None:
        """Test filter_show_rows_for_sync requires db when not in skip-db mode."""
        show_rows = [
            {"id": "uuid-1", "imdb_id": "tt1234567", "name": "Show 1"},
        ]

        # Mock db client
        db = MagicMock()
        db.schema.return_value.table.return_value.select.return_value.in_.return_value.execute.return_value.data = []

        result = filter_show_rows_for_sync(
            db,
            show_rows,
            table_name="test_table",
            incremental=False,
            resume=False,
            force=True,
            verbose=False,
        )

        # Should process the show (force=True)
        assert len(result.selected) == 1
        assert result.reasons.get("force") == 1
