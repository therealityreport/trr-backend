from __future__ import annotations

from unittest.mock import patch

import scripts.shows.backfill_bravo_person_source_links as mod


def test_parse_args_supports_thresholds_and_diagnostics() -> None:
    args = mod._parse_args(
        [
            "--apply",
            "--show-id",
            "show-1",
            "--warn-fetch-errors",
            "10",
            "--fail-fetch-errors",
            "20",
            "--warn-pending-person-sources",
            "1",
            "--fail-pending-person-sources",
            "2",
            "--diagnose-missing-person-sources",
            "--diagnose-name",
            "Andy Cohen",
            "--diagnostics-json",
            "/tmp/diag.json",
        ]
    )
    assert args.apply is True
    assert args.show_id == ["show-1"]
    assert args.warn_fetch_errors == 10
    assert args.fail_fetch_errors == 20
    assert args.warn_pending_person_sources == 1
    assert args.fail_pending_person_sources == 2
    assert args.diagnose_missing_person_sources is True
    assert args.diagnose_name == ["Andy Cohen"]
    assert args.diagnostics_json == "/tmp/diag.json"


def test_count_pending_person_source_links_returns_zero_for_empty_show_set() -> None:
    assert mod._count_pending_person_source_links([]) == 0


def test_count_pending_person_source_links_queries_db() -> None:
    with patch.object(mod.pg, "fetch_all", return_value=[{"pending_count": 4}]) as fetch_all:
        out = mod._count_pending_person_source_links(["a", "b"])
    assert out == 4
    assert fetch_all.call_count == 1


def test_load_show_cast_people_for_diagnostics_orders_by_selected_aliases() -> None:
    with patch.object(mod.pg, "fetch_all", return_value=[]) as fetch_all:
        mod._load_show_cast_people_for_diagnostics("show-1")
    query = fetch_all.call_args.args[0]
    assert "ORDER BY person_name NULLS LAST, person_id" in query


def test_diagnose_missing_person_sources_captures_fetch_error_reasons() -> None:
    show_id = "show-1"
    person_id = "person-1"
    with patch.object(
        mod,
        "_load_show_cast_people_for_diagnostics",
        return_value=[
            {
                "person_id": person_id,
                "person_name": "Andy Cohen",
                "external_ids": {"imdb": "nm0169212", "tmdb": "54772"},
                "cast_tmdb_imdb_id": None,
                "cast_tmdb_tmdb_id": None,
                "cast_tmdb_wikidata_id": None,
            }
        ],
    ):
        with patch.object(mod, "_load_person_link_state_by_person_id", return_value={}):
            with patch.object(
                mod.admin_show_links,
                "_validate_person_knowledge_url",
                side_effect=[(None, "fetch_error"), (None, "fetch_error")],
            ):
                with patch.object(mod, "_owner_signal_for_candidate", return_value=False):
                    out = mod._diagnose_missing_person_sources(show_ids=[show_id], names=["Andy Cohen"])
    assert len(out) == 1
    row = out[0]
    assert row["person_name"] == "Andy Cohen"
    assert row["imdb_missing_reason"] == "unverifiable_fetch_error"
    assert row["tmdb_missing_reason"] == "unverifiable_fetch_error"
    assert row["imdb_owner_match_signal"] is False
    assert row["tmdb_owner_match_signal"] is False


def test_main_returns_exit_code_2_when_pending_threshold_exceeded() -> None:
    stats = {
        "cleanup_scanned": 1,
        "cleanup_invalid": 0,
        "cleanup_promoted": 0,
        "cleanup_deleted": 0,
        "cleanup_fetch_errors": 0,
        "discovered_upserted": 1,
        "discovery_skipped_non_http": 0,
        "discovery_skipped_person_source_non_approved": 0,
        "discovery_skipped_duplicate": 0,
        "invalid_reason_counts": {},
    }
    with patch.object(mod, "load_env"):
        with patch.object(mod, "create_supabase_admin_client", return_value=object()):
            with patch.object(mod, "_list_impacted_show_ids", return_value=["show-1"]):
                with patch.object(mod, "_run_show", return_value=stats):
                    with patch.object(mod, "_count_pending_person_source_links", return_value=1):
                        code = mod.main(["--fail-pending-person-sources", "0"])
    assert code == 2
