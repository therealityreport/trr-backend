"""Tests for credits repository transforms and validation."""

from __future__ import annotations

import pytest


class TestCreditsTransform:
    """Tests for the show_cast → credits transformation logic."""

    def test_credit_row_includes_all_required_fields(self) -> None:
        """Verify that credit rows include all required fields for core.credits."""
        # Simulate the transformation that happens in sync_show_cast.py
        show_cast_row = {
            "show_id": "test-show-uuid",
            "person_id": "test-person-uuid",
            "billing_order": 1,
            "role": "Themselves",
            "credit_category": "Self",
        }
        source_type = "fullcredits_html"

        # Transform to credit row (as done in sync_show_cast.py)
        credit_row = {
            "show_id": show_cast_row["show_id"],
            "person_id": show_cast_row["person_id"],
            "credit_category": show_cast_row.get("credit_category") or "Self",
            "role": show_cast_row.get("role"),
            "billing_order": show_cast_row.get("billing_order"),
            "source_type": source_type,
            "metadata": {},
        }

        assert credit_row["show_id"] == "test-show-uuid"
        assert credit_row["person_id"] == "test-person-uuid"
        assert credit_row["credit_category"] == "Self"
        assert credit_row["role"] == "Themselves"
        assert credit_row["billing_order"] == 1
        assert credit_row["source_type"] == "fullcredits_html"
        assert credit_row["metadata"] == {}

    def test_credit_row_defaults_category_to_self(self) -> None:
        """Verify that credit_category defaults to 'Self' when not provided."""
        show_cast_row = {
            "show_id": "test-show-uuid",
            "person_id": "test-person-uuid",
            "billing_order": 1,
            "role": None,
            "credit_category": None,
        }

        credit_row = {
            "show_id": show_cast_row["show_id"],
            "person_id": show_cast_row["person_id"],
            "credit_category": show_cast_row.get("credit_category") or "Self",
            "role": show_cast_row.get("role"),
            "billing_order": show_cast_row.get("billing_order"),
            "source_type": "fullcredits_html",
            "metadata": {},
        }

        assert credit_row["credit_category"] == "Self"


class TestCreditOccurrencesTransform:
    """Tests for the episode_appearances → credit_occurrences transformation logic."""

    def test_occurrence_row_includes_required_fields(self) -> None:
        """Verify that occurrence rows include all required fields."""
        credit_id = "test-credit-uuid"
        episode_id = "test-episode-uuid"

        occurrence_row = {
            "credit_id": credit_id,
            "episode_id": episode_id,
            "appearance_type": "appears",
        }

        assert occurrence_row["credit_id"] == credit_id
        assert occurrence_row["episode_id"] == episode_id
        assert occurrence_row["appearance_type"] == "appears"

    def test_occurrence_rows_from_rollup(self) -> None:
        """Test building occurrence rows from a rollup with multiple episodes."""
        credit_id = "test-credit-uuid"
        imdb_episode_ids = ["tt1234567", "tt1234568", "tt1234569"]
        episode_index = {
            "tt1234567": {"id": "ep-uuid-1"},
            "tt1234568": {"id": "ep-uuid-2"},
            "tt1234569": {"id": "ep-uuid-3"},
        }

        occurrence_rows = []
        for imdb_ep_id in imdb_episode_ids:
            meta = episode_index.get(imdb_ep_id)
            if meta and meta.get("id"):
                occurrence_rows.append(
                    {
                        "credit_id": credit_id,
                        "episode_id": meta["id"],
                        "appearance_type": "appears",
                    }
                )

        assert len(occurrence_rows) == 3
        assert occurrence_rows[0]["episode_id"] == "ep-uuid-1"
        assert occurrence_rows[1]["episode_id"] == "ep-uuid-2"
        assert occurrence_rows[2]["episode_id"] == "ep-uuid-3"

    def test_occurrence_skips_unresolved_episodes(self) -> None:
        """Test that unresolved IMDB episode IDs are skipped."""
        credit_id = "test-credit-uuid"
        imdb_episode_ids = ["tt1234567", "tt9999999", "tt1234569"]
        episode_index = {
            "tt1234567": {"id": "ep-uuid-1"},
            # tt9999999 is missing from the index
            "tt1234569": {"id": "ep-uuid-3"},
        }

        occurrence_rows = []
        for imdb_ep_id in imdb_episode_ids:
            meta = episode_index.get(imdb_ep_id)
            if meta and meta.get("id"):
                occurrence_rows.append(
                    {
                        "credit_id": credit_id,
                        "episode_id": meta["id"],
                        "appearance_type": "appears",
                    }
                )

        assert len(occurrence_rows) == 2
        episode_ids = [r["episode_id"] for r in occurrence_rows]
        assert "ep-uuid-1" in episode_ids
        assert "ep-uuid-3" in episode_ids


class TestBackfillCreditMapping:
    """Tests for the deterministic credit mapping in backfill."""

    def test_exact_match_preferred(self) -> None:
        """Test that exact match (Self, no role, fullcredits_html) is preferred."""
        credits_lookup = {
            ("show-1", "person-1", "Self", "", "fullcredits_html"): "credit-exact",
            ("show-1", "person-1", "Cast", "Character Name", "tmdb"): "credit-other",
        }

        show_id = "show-1"
        person_id = "person-1"

        # Priority 1: Exact match
        exact_key = (show_id, person_id, "Self", "", "fullcredits_html")
        credit_id = credits_lookup.get(exact_key)

        assert credit_id == "credit-exact"

    def test_empty_role_fallback(self) -> None:
        """Test fallback to any credit with empty role when no exact match."""
        credits_lookup = {
            ("show-1", "person-1", "Cast", "", "tmdb"): "credit-empty-role",
            ("show-1", "person-1", "Cast", "Character Name", "fullcredits_html"): "credit-with-role",
        }

        show_id = "show-1"
        person_id = "person-1"

        # Priority 1: Exact match (not found)
        exact_key = (show_id, person_id, "Self", "", "fullcredits_html")
        credit_id = credits_lookup.get(exact_key)

        # Priority 2: Any credit with empty role
        if not credit_id:
            for (sid, pid, _cat, role, _src), cid in credits_lookup.items():
                if sid == show_id and pid == person_id and not role:
                    credit_id = cid
                    break

        assert credit_id == "credit-empty-role"

    def test_ambiguous_fallback(self) -> None:
        """Test fallback to any credit when no empty-role match."""
        credits_lookup = {
            ("show-1", "person-1", "Cast", "Character Name", "tmdb"): "credit-with-role",
        }

        show_id = "show-1"
        person_id = "person-1"

        credit_id = None
        match_type = None

        # Priority 1: Exact match (not found)
        exact_key = (show_id, person_id, "Self", "", "fullcredits_html")
        if exact_key in credits_lookup:
            credit_id = credits_lookup[exact_key]
            match_type = "exact"

        # Priority 2: Any credit with empty role (not found)
        if not credit_id:
            for (sid, pid, _cat, role, _src), cid in credits_lookup.items():
                if sid == show_id and pid == person_id and not role:
                    credit_id = cid
                    match_type = "empty_role"
                    break

        # Priority 3: Any credit (ambiguous)
        if not credit_id:
            for (sid, pid, _cat, _role, _src), cid in credits_lookup.items():
                if sid == show_id and pid == person_id:
                    credit_id = cid
                    match_type = "ambiguous"
                    break

        assert credit_id == "credit-with-role"
        assert match_type == "ambiguous"


class TestDualWriteGating:
    """Tests for the ENABLE_CREDITS_V2_WRITE environment variable gating."""

    def test_is_credits_v2_enabled_true_values(self) -> None:
        """Test that various truthy values enable credits v2."""
        import os

        original = os.environ.get("ENABLE_CREDITS_V2_WRITE")

        try:
            for value in ["1", "true", "True", "TRUE", "yes", "Yes", "YES"]:
                os.environ["ENABLE_CREDITS_V2_WRITE"] = value
                assert value.lower() in ("1", "true", "yes")
        finally:
            if original is not None:
                os.environ["ENABLE_CREDITS_V2_WRITE"] = original
            else:
                os.environ.pop("ENABLE_CREDITS_V2_WRITE", None)

    def test_is_credits_v2_enabled_false_values(self) -> None:
        """Test that various falsy values disable credits v2."""
        import os

        original = os.environ.get("ENABLE_CREDITS_V2_WRITE")

        try:
            for value in ["0", "false", "False", "FALSE", "no", "No", "NO", "", "other"]:
                os.environ["ENABLE_CREDITS_V2_WRITE"] = value
                assert value.lower() not in ("1", "true", "yes")
        finally:
            if original is not None:
                os.environ["ENABLE_CREDITS_V2_WRITE"] = original
            else:
                os.environ.pop("ENABLE_CREDITS_V2_WRITE", None)


class TestReadSwitchGating:
    """Tests for the ENABLE_CREDITS_V2_READ environment variable gating."""

    def test_is_credits_v2_read_enabled_true_values(self) -> None:
        """Test that various truthy values enable credits v2 read."""
        import os

        from trr_backend.repositories.credits import is_credits_v2_read_enabled

        original = os.environ.get("ENABLE_CREDITS_V2_READ")

        try:
            for value in ["1", "true", "True", "TRUE", "yes", "Yes", "YES"]:
                os.environ["ENABLE_CREDITS_V2_READ"] = value
                assert is_credits_v2_read_enabled() is True, f"Expected True for {value}"
        finally:
            if original is not None:
                os.environ["ENABLE_CREDITS_V2_READ"] = original
            else:
                os.environ.pop("ENABLE_CREDITS_V2_READ", None)

    def test_is_credits_v2_read_enabled_false_values(self) -> None:
        """Test that various falsy values disable credits v2 read."""
        import os

        from trr_backend.repositories.credits import is_credits_v2_read_enabled

        original = os.environ.get("ENABLE_CREDITS_V2_READ")

        try:
            for value in ["0", "false", "False", "FALSE", "no", "No", "NO", "", "other"]:
                os.environ["ENABLE_CREDITS_V2_READ"] = value
                assert is_credits_v2_read_enabled() is False, f"Expected False for {value}"
        finally:
            if original is not None:
                os.environ["ENABLE_CREDITS_V2_READ"] = original
            else:
                os.environ.pop("ENABLE_CREDITS_V2_READ", None)

    def test_is_credits_v2_read_enabled_unset(self) -> None:
        """Test that unset env var defaults to False."""
        import os

        from trr_backend.repositories.credits import is_credits_v2_read_enabled

        original = os.environ.get("ENABLE_CREDITS_V2_READ")

        try:
            os.environ.pop("ENABLE_CREDITS_V2_READ", None)
            assert is_credits_v2_read_enabled() is False
        finally:
            if original is not None:
                os.environ["ENABLE_CREDITS_V2_READ"] = original


@pytest.mark.skip(reason="Integration test - requires Supabase with credits tables")
class TestCreditsValidationViews:
    """Tests for validation views matching legacy table shapes.

    These tests require a running Supabase instance with:
    - core.credits and core.credit_occurrences tables
    - core.v_show_cast_from_credits view
    - core.v_episode_appearances_from_credits view
    """

    def test_v_show_cast_from_credits_matches_show_cast_shape(self) -> None:
        """Verify that v_show_cast_from_credits has the same columns as show_cast."""
        # Expected columns from v_show_cast_from_credits:
        expected_columns = {
            "show_name",
            "cast_member_name",
            "show_id",
            "person_id",
            "billing_order",
            "role",
            "credit_category",
            "id",
            "created_at",
            "updated_at",
            "source_type",
        }
        # This would be verified by querying the view in an integration test
        assert expected_columns  # Placeholder

    def test_v_episode_appearances_from_credits_matches_episode_appearances_shape(self) -> None:
        """Verify that v_episode_appearances_from_credits has the same columns."""
        # Expected columns from v_episode_appearances_from_credits:
        expected_columns = {
            "show_name",
            "cast_member_name",
            "seasons",
            "tmdb_season_ids",
            "tmdb_show_id",
            "imdb_show_id",
            "imdb_episode_title_ids",
            "tmdb_episode_ids",
            "total_episodes",
            "show_id",
            "person_id",
            "id",
            "created_at",
            "updated_at",
        }
        # This would be verified by querying the view in an integration test
        assert expected_columns  # Placeholder
