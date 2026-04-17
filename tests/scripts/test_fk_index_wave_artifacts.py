from __future__ import annotations

from pathlib import Path

import yaml

import scripts.db.build_fk_index_wave_artifacts as artifact_mod


def test_build_wave_artifacts_marks_missing_query_checks(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    inventory_path = docs_root / "wave-2-inventory.yml"
    docs_root.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text(
        yaml.safe_dump(
            {
                "metadata": {"connection_mode": "runtime", "resolved_db_host": "pooler.example.com"},
                "entries": [
                    {
                        "schema": "ml",
                        "table": "analysis_media_assets",
                        "constraint_name": "analysis_media_assets_show_id_fkey",
                        "proposed_index_name": "ml_analysis_media_assets_show_id_idx",
                        "proposed_index_columns": ["show_id"],
                        "proposed_partial_predicate": None,
                        "statement_timeout_tier": "30min",
                        "decision": "add",
                    },
                    {
                        "schema": "screenalytics",
                        "table": "video_assets",
                        "constraint_name": "video_assets_media_asset_id_fkey",
                        "proposed_index_name": "screenalytics_video_assets_media_asset_id_idx",
                        "proposed_index_columns": ["media_asset_id"],
                        "proposed_partial_predicate": "media_asset_id is not null",
                        "statement_timeout_tier": "30min",
                        "decision": "add",
                    },
                    {
                        "schema": "firebase_surveys",
                        "table": "response_events",
                        "constraint_name": "response_events_survey_id_fkey",
                        "fk_columns_in_order": ["survey_id"],
                        "nullable_columns": [],
                        "referenced_schema": "firebase_surveys",
                        "referenced_table": "surveys",
                        "estimated_row_count": 0,
                        "single_column_null_frac": None,
                        "hot_table": False,
                        "covered_by_existing_index": False,
                        "proposed_index_name": "firebase_surveys_response_events_survey_id_idx",
                        "proposed_index_columns": ["survey_id"],
                        "proposed_partial_predicate": None,
                        "statement_timeout_tier": "30min",
                        "decision": "add",
                    },
                    {
                        "schema": "legacy_logs",
                        "table": "audit_trail",
                        "constraint_name": "audit_trail_actor_id_fkey",
                        "proposed_index_name": "legacy_logs_audit_trail_actor_id_idx",
                        "proposed_index_columns": ["actor_id"],
                        "proposed_partial_predicate": None,
                        "statement_timeout_tier": "30min",
                        "decision": "add",
                    },
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-2")

    persisted = yaml.safe_load(inventory_path.read_text(encoding="utf-8"))
    decisions = {entry["schema"]: entry["decision"] for entry in persisted["entries"]}
    assert decisions["ml"] == "add"
    assert decisions["screenalytics"] == "add"
    assert decisions["firebase_surveys"] == "add"
    assert decisions["legacy_logs"] == "defer-missing-query-check"

    # Idempotent re-run: supported-schema entries stay `add`, unsupported stays deferred.
    artifact_mod.build_wave_artifacts("wave-2")
    persisted = yaml.safe_load(inventory_path.read_text(encoding="utf-8"))
    decisions = {entry["schema"]: entry["decision"] for entry in persisted["entries"]}
    assert decisions["ml"] == "add"
    assert decisions["screenalytics"] == "add"
    assert decisions["firebase_surveys"] == "add"
    assert decisions["legacy_logs"] == "defer-missing-query-check"

    assert persisted["metadata"]["query_check_supported_schemas"] == [
        "firebase_surveys",
        "ml",
        "pipeline",
        "screenalytics",
        "surveys",
    ]

    forward_sql = (docs_root / "wave-2-forward.sql").read_text(encoding="utf-8")
    rollback_sql = (docs_root / "wave-2-rollback.sql").read_text(encoding="utf-8")
    status_md = (docs_root / "wave-2-status.md").read_text(encoding="utf-8")

    assert "ml_analysis_media_assets_show_id_idx" in forward_sql
    assert "screenalytics_video_assets_media_asset_id_idx" in forward_sql
    assert "firebase_surveys_response_events_survey_id_idx" in forward_sql
    assert "legacy_logs_audit_trail_actor_id_idx" not in forward_sql
    assert "DROP INDEX CONCURRENTLY IF EXISTS" in rollback_sql
    assert "defer-missing-query-check" in status_md
    assert "Pending. Respect `24h` soak between Wave 1 apply completion and Wave 2 apply start." in status_md
    # Stale per-sub-wave wording must not reappear.
    assert "Wave 1A" not in status_md
    assert "Wave 1B" not in status_md
    assert "Wave 2A" not in status_md
    assert "Wave 2B" not in status_md
