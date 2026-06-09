"""Integration tests for credits views - requires local Supabase.

These tests verify that the validation views return correct data.
Run with: RUN_DB_TESTS=1 pytest tests/repositories/test_credits_integration.py -v

Prerequisites:
- Local Supabase running (`supabase start`)
- Migrations applied (`supabase db reset`)
- SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY set (in .env file)
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from dotenv import load_dotenv

# Skip entire module if DB tests not enabled
pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_DB_TESTS", "").lower() not in ("1", "true", "yes"),
    reason="RUN_DB_TESTS not enabled - set RUN_DB_TESTS=1 to run integration tests",
)


@pytest.fixture(scope="module")
def db_client():
    """Create Supabase client for tests."""
    load_dotenv()

    from trr_backend.db import create_supabase_admin_client

    return create_supabase_admin_client()


@pytest.fixture
def test_show(db_client) -> dict[str, Any]:
    """Create a test show and clean up after."""
    show_id = str(uuid.uuid4())
    show_data = {
        "id": show_id,
        "name": f"Test Show {show_id[:8]}",
        "tmdb_id": 99999,
        "imdb_id": "tt9999999",
    }

    # Insert show
    response = db_client.schema("core").table("shows").insert(show_data).execute()
    assert not (hasattr(response, "error") and response.error), f"Failed to create show: {response.error}"

    yield show_data

    # Cleanup: delete show (cascades to credits, seasons, episodes)
    db_client.schema("core").table("shows").delete().eq("id", show_id).execute()


@pytest.fixture
def test_season(db_client, test_show) -> dict[str, Any]:
    """Create a test season."""
    season_id = str(uuid.uuid4())
    season_data = {
        "id": season_id,
        "show_id": test_show["id"],
        "season_number": 1,
        "tmdb_season_id": 88888,
    }

    response = db_client.schema("core").table("seasons").insert(season_data).execute()
    assert not (hasattr(response, "error") and response.error), f"Failed to create season: {response.error}"

    yield season_data


@pytest.fixture
def test_episodes(db_client, test_show, test_season) -> list[dict[str, Any]]:
    """Create two test episodes."""
    episodes = []
    for i in range(1, 3):
        episode_id = str(uuid.uuid4())
        episode_data = {
            "id": episode_id,
            "show_id": test_show["id"],
            "season_id": test_season["id"],
            "season_number": test_season["season_number"],  # Required field
            "episode_number": i,
            "title": f"Episode {i}",
            "imdb_episode_id": f"tt888888{i}",
            "tmdb_episode_id": 77777 + i,
        }
        response = db_client.schema("core").table("episodes").insert(episode_data).execute()
        assert not (hasattr(response, "error") and response.error), f"Failed to create episode: {response.error}"
        episodes.append(episode_data)

    yield episodes


@pytest.fixture
def test_people(db_client) -> list[dict[str, Any]]:
    """Create two test people and clean up after."""
    people = []
    for i in range(1, 3):
        person_id = str(uuid.uuid4())
        person_data = {
            "id": person_id,
            "full_name": f"Test Person {i}",
            "external_ids": {"imdb": f"nm999999{i}"},
        }
        response = db_client.schema("core").table("people").insert(person_data).execute()
        assert not (hasattr(response, "error") and response.error), f"Failed to create person: {response.error}"
        people.append(person_data)

    yield people

    # Cleanup
    for person in people:
        db_client.schema("core").table("people").delete().eq("id", person["id"]).execute()


@pytest.fixture
def test_credits(db_client, test_show, test_people) -> list[dict[str, Any]]:
    """Create test credits for each person on the show."""
    credits = []
    for i, person in enumerate(test_people):
        credit_id = str(uuid.uuid4())
        credit_data = {
            "id": credit_id,
            "show_id": test_show["id"],
            "person_id": person["id"],
            "credit_category": "Self",
            "role": None,
            "billing_order": i + 1,
            "source_type": "fullcredits_html",
            "metadata": {},
        }
        response = db_client.schema("core").table("credits").insert(credit_data).execute()
        assert not (hasattr(response, "error") and response.error), f"Failed to create credit: {response.error}"
        credits.append(credit_data)

    yield credits


@pytest.fixture
def test_occurrences(db_client, test_credits, test_episodes) -> list[dict[str, Any]]:
    """Create credit occurrences: person 1 in both episodes, person 2 in episode 1 only."""
    occurrences = []

    # Person 1 (credits[0]) appears in both episodes
    for episode in test_episodes:
        occ = {
            "credit_id": test_credits[0]["id"],
            "episode_id": episode["id"],
            "appearance_type": "appears",
        }
        response = db_client.schema("core").table("credit_occurrences").insert(occ).execute()
        assert not (hasattr(response, "error") and response.error), f"Failed to create occurrence: {response.error}"
        occurrences.append(occ)

    # Person 2 (credits[1]) appears only in episode 1
    occ = {
        "credit_id": test_credits[1]["id"],
        "episode_id": test_episodes[0]["id"],
        "appearance_type": "appears",
    }
    response = db_client.schema("core").table("credit_occurrences").insert(occ).execute()
    assert not (hasattr(response, "error") and response.error), f"Failed to create occurrence: {response.error}"
    occurrences.append(occ)

    yield occurrences


class TestViewShowCastFromCredits:
    """Test v_show_cast_from_credits view."""

    def test_returns_all_credits_for_show(self, db_client, test_show, test_credits, test_people) -> None:
        """Verify view returns all credits for a show with correct columns."""
        response = (
            db_client.schema("core")
            .table("v_show_cast_from_credits")
            .select("*")
            .eq("show_id", test_show["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 2, f"Expected 2 credits, got {len(data)}"

        # Verify columns exist
        row = data[0]
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
        assert expected_columns.issubset(set(row.keys())), f"Missing columns: {expected_columns - set(row.keys())}"

        # Verify values
        person_names = {r["cast_member_name"] for r in data}
        assert "Test Person 1" in person_names
        assert "Test Person 2" in person_names


class TestViewEpisodeCredits:
    """Test v_episode_credits view (who is in episode X?)."""

    def test_returns_people_in_episode(
        self, db_client, test_episodes, test_credits, test_people, test_occurrences
    ) -> None:
        """Verify view returns correct people for a given episode."""
        # Query episode 1 - should have both people
        response = (
            db_client.schema("core")
            .table("v_episode_credits")
            .select("*")
            .eq("episode_id", test_episodes[0]["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 2, f"Expected 2 people in episode 1, got {len(data)}"

        person_names = {r["person_name"] for r in data}
        assert "Test Person 1" in person_names
        assert "Test Person 2" in person_names

    def test_episode_2_has_only_person_1(
        self, db_client, test_episodes, test_credits, test_people, test_occurrences
    ) -> None:
        """Verify episode 2 only has person 1."""
        response = (
            db_client.schema("core")
            .table("v_episode_credits")
            .select("*")
            .eq("episode_id", test_episodes[1]["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 1, f"Expected 1 person in episode 2, got {len(data)}"
        assert data[0]["person_name"] == "Test Person 1"


class TestViewPersonShowSeasons:
    """Test v_person_show_seasons view (seasons per person per show)."""

    def test_returns_seasons_for_person(
        self, db_client, test_show, test_credits, test_people, test_occurrences
    ) -> None:
        """Verify view returns correct seasons array and total episodes."""
        response = (
            db_client.schema("core")
            .table("v_person_show_seasons")
            .select("*")
            .eq("show_id", test_show["id"])
            .eq("person_id", test_people[0]["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 1, f"Expected 1 row, got {len(data)}"

        row = data[0]
        assert row["seasons_appeared"] == [1], f"Expected [1], got {row['seasons_appeared']}"
        assert row["total_episodes"] == 2, f"Expected 2 episodes, got {row['total_episodes']}"

    def test_person_2_has_fewer_episodes(
        self, db_client, test_show, test_credits, test_people, test_occurrences
    ) -> None:
        """Verify person 2 only has 1 episode."""
        response = (
            db_client.schema("core")
            .table("v_person_show_seasons")
            .select("*")
            .eq("show_id", test_show["id"])
            .eq("person_id", test_people[1]["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 1
        assert data[0]["total_episodes"] == 1


class TestViewEpisodeAppearancesFromCredits:
    """Test v_episode_appearances_from_credits validation view."""

    def test_aggregates_episodes_correctly(
        self, db_client, test_show, test_credits, test_people, test_episodes, test_occurrences
    ) -> None:
        """Verify view aggregates episode arrays correctly."""
        response = (
            db_client.schema("core")
            .table("v_episode_appearances_from_credits")
            .select("*")
            .eq("show_id", test_show["id"])
            .eq("person_id", test_people[0]["id"])
            .execute()
        )

        assert not (hasattr(response, "error") and response.error)
        data = response.data or []

        assert len(data) == 1, f"Expected 1 row, got {len(data)}"

        row = data[0]

        # Person 1 should have 2 IMDB episode IDs
        imdb_ids = row.get("imdb_episode_title_ids") or []
        assert len(imdb_ids) == 2, f"Expected 2 imdb_episode_title_ids, got {len(imdb_ids)}"
        assert "tt8888881" in imdb_ids
        assert "tt8888882" in imdb_ids

        # Person 1 should have total_episodes = 2
        assert row["total_episodes"] == 2

        # Seasons should be [1]
        assert row["seasons"] == [1]
