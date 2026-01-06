from __future__ import annotations

import json
from pathlib import Path

from trr_backend.ingestion.tmdb_show_backfill import (
    build_tmdb_show_patch,
    extract_tmdb_network_ids,
    extract_tmdb_production_company_ids,
    needs_tmdb_enrichment,
    resolve_tmdb_id_from_find_payload,
    select_tmdb_tv_result,
)


def test_resolve_tmdb_id_no_results() -> None:
    payload = {"tv_results": []}
    tmdb_id, reason = resolve_tmdb_id_from_find_payload(payload, show_name="Sample Show", premiere_date="2020-01-01")
    assert tmdb_id is None
    assert reason == "no_tv_results"


def test_resolve_tmdb_id_single_result() -> None:
    payload = {"tv_results": [{"id": 101, "name": "Sample Show"}]}
    tmdb_id, reason = resolve_tmdb_id_from_find_payload(payload, show_name="Sample Show", premiere_date="2020-01-01")
    assert tmdb_id == 101
    assert reason == "single_result"


def test_resolve_tmdb_id_prefers_name_match() -> None:
    results = [
        {"id": 101, "name": "Sample Show", "first_air_date": "2020-01-01"},
        {"id": 202, "name": "Different Show", "first_air_date": "2020-01-01"},
    ]
    tmdb_id, reason = select_tmdb_tv_result(results, show_name="Sample Show", premiere_date="2020-01-01")
    assert tmdb_id == 101
    assert reason == "name_match"


def test_resolve_tmdb_id_ambiguous_when_scores_tie() -> None:
    results = [
        {"id": 101, "name": "Same Name", "first_air_date": "2020-01-01", "popularity": 10.0},
        {"id": 202, "name": "Same Name", "first_air_date": "2020-01-01", "popularity": 10.0},
    ]
    tmdb_id, reason = select_tmdb_tv_result(results, show_name="Same Name", premiere_date="2020-01-01")
    assert tmdb_id is None
    assert reason == "ambiguous"


def test_build_tmdb_show_patch_and_arrays() -> None:
    details = {
        "id": 999,
        "name": "Test Show",
        "status": "Running",
        "type": "Scripted",
        "first_air_date": "2021-01-01",
        "last_air_date": "2024-01-01",
        "vote_average": 7.5,
        "vote_count": 42,
        "popularity": 12.3,
        "networks": [{"id": 1}, {"id": 2}],
        "production_companies": [{"id": 10}],
    }
    patch = build_tmdb_show_patch(details, fetched_at="2025-01-01T00:00:00Z")
    assert patch["tmdb_name"] == "Test Show"
    assert patch["tmdb_status"] == "Running"
    assert patch["tmdb_type"] == "Scripted"
    assert patch["tmdb_vote_average"] == 7.5
    assert patch["tmdb_vote_count"] == 42
    assert patch["tmdb_popularity"] == 12.3
    assert patch["tmdb_meta"].get("id") == 999

    assert extract_tmdb_network_ids(details) == [1, 2]
    assert extract_tmdb_production_company_ids(details) == [10]


def test_needs_tmdb_enrichment_checks_required_fields() -> None:
    base = {
        "tmdb_id": 123,
        "tmdb_meta": {},
        "tmdb_fetched_at": "2025-01-01T00:00:00Z",
        "tmdb_vote_average": 1.0,
        "tmdb_vote_count": 2,
        "tmdb_popularity": 3.0,
        "tmdb_first_air_date": "2020-01-01",
        "tmdb_last_air_date": "2021-01-01",
        "tmdb_status": "Running",
        "tmdb_type": "Scripted",
    }
    assert needs_tmdb_enrichment(base) is False

    missing = dict(base)
    missing["tmdb_meta"] = None
    assert needs_tmdb_enrichment(missing) is True


def test_resolve_then_enrich_flow_with_fixtures() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    find_payload = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "find_by_imdb_id_sample.json").read_text()
    )
    details_payload = json.loads(
        (repo_root / "tests" / "fixtures" / "tmdb" / "tv_details_sample.json").read_text()
    )

    tmdb_id, reason = resolve_tmdb_id_from_find_payload(
        find_payload,
        show_name="RuPaul's Drag Race",
        premiere_date="2009-02-02",
    )
    assert tmdb_id == 12345
    assert reason in {"single_result", "name_match"}

    patch = build_tmdb_show_patch(details_payload, fetched_at="2025-12-18T00:00:00Z")
    assert patch["tmdb_meta"].get("number_of_seasons") == 16
    assert patch["tmdb_meta"].get("number_of_episodes") == 250
    assert extract_tmdb_network_ids(details_payload) == [1]
