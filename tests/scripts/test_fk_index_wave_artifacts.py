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


def _wave1_entry(
    *,
    schema: str = "social",
    table: str = "scrape_jobs",
    constraint_name: str | None = None,
    fk_columns: list[str] | None = None,
    nullable_columns: list[str] | None = None,
    referenced_schema: str = "social",
    referenced_table: str = "accounts",
    estimated_row_count: int = 0,
    single_column_null_frac: float | None = None,
    hot_table: bool = False,
    covered_by_existing_index: bool = False,
    proposed_index_name: str | None = None,
    proposed_index_columns: list[str] | None = None,
    proposed_partial_predicate: str | None = None,
    statement_timeout_tier: str = "30min",
    decision: str = "add",
) -> dict[str, object]:
    """Build a wave-1 entry with sensible defaults. `admin`, `core`, and `social`
    are the wave-1 query-check supported schemas, so `schema=social` keeps the
    generator on the rollout-ready path."""

    cols = fk_columns or ["account_id"]
    return {
        "schema": schema,
        "table": table,
        "constraint_name": constraint_name or f"{table}_{cols[0]}_fkey",
        "fk_columns_in_order": cols,
        "nullable_columns": nullable_columns or [],
        "referenced_schema": referenced_schema,
        "referenced_table": referenced_table,
        "estimated_row_count": estimated_row_count,
        "single_column_null_frac": single_column_null_frac,
        "hot_table": hot_table,
        "covered_by_existing_index": covered_by_existing_index,
        "proposed_index_name": proposed_index_name or f"{schema}_{table}_{cols[0]}_idx",
        "proposed_index_columns": proposed_index_columns or cols,
        "proposed_partial_predicate": proposed_partial_predicate,
        "statement_timeout_tier": statement_timeout_tier,
        "decision": decision,
    }


def _write_wave1_inventory(docs_root: Path, entries: list[dict[str, object]]) -> None:
    docs_root.mkdir(parents=True, exist_ok=True)
    inventory_path = docs_root / "wave-1-inventory.yml"
    inventory_path.write_text(
        yaml.safe_dump(
            {
                "metadata": {
                    "connection_mode": "runtime",
                    "resolved_db_host": "pooler.example.com",
                },
                "entries": entries,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_forward_sql_includes_pooler_guard_exactly_once(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    _write_wave1_inventory(docs_root, [_wave1_entry()])
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    forward_sql = (docs_root / "wave-1-forward.sql").read_text(encoding="utf-8")
    assert forward_sql.count("RAISE EXCEPTION 'Refusing apply: application_name") == 1
    assert forward_sql.count("DO $pre$") == 1


def test_forward_sql_emits_analyze_for_large_tables(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    big_entry = _wave1_entry(
        table="large_table",
        constraint_name="large_table_account_id_fkey",
        estimated_row_count=2_000_000,
    )
    small_entry = _wave1_entry(
        table="small_table",
        constraint_name="small_table_account_id_fkey",
        estimated_row_count=500_000,
    )
    _write_wave1_inventory(docs_root, [big_entry, small_entry])
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    forward_sql = (docs_root / "wave-1-forward.sql").read_text(encoding="utf-8")
    assert 'ANALYZE "social"."large_table";' in forward_sql
    assert 'ANALYZE "social"."small_table";' not in forward_sql


def test_forward_sql_skips_analyze_when_no_large_tables(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    _write_wave1_inventory(docs_root, [_wave1_entry(estimated_row_count=100)])
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    forward_sql = (docs_root / "wave-1-forward.sql").read_text(encoding="utf-8")
    for line in forward_sql.splitlines():
        assert not line.startswith("ANALYZE "), f"unexpected ANALYZE line: {line!r}"


def test_status_md_disk_targets_deduplicates_by_table(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    entries = [
        _wave1_entry(
            table="scrape_jobs",
            constraint_name="scrape_jobs_account_id_fkey",
            fk_columns=["account_id"],
            proposed_index_name="social_scrape_jobs_account_id_idx",
            estimated_row_count=50_000,
        ),
        _wave1_entry(
            table="scrape_jobs",
            constraint_name="scrape_jobs_season_id_fkey",
            fk_columns=["season_id"],
            proposed_index_name="social_scrape_jobs_season_id_idx",
            estimated_row_count=50_000,
        ),
        _wave1_entry(
            table="scrape_jobs",
            constraint_name="scrape_jobs_show_id_fkey",
            fk_columns=["show_id"],
            proposed_index_name="social_scrape_jobs_show_id_idx",
            estimated_row_count=50_000,
        ),
        _wave1_entry(
            table="instagram_comments",
            constraint_name="instagram_comments_account_id_fkey",
            fk_columns=["account_id"],
            proposed_index_name="social_instagram_comments_account_id_idx",
            estimated_row_count=100_000,
        ),
    ]
    _write_wave1_inventory(docs_root, entries)
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    status_md = (docs_root / "wave-1-status.md").read_text(encoding="utf-8")
    # Isolate the disk-targets section so we don't count table mentions elsewhere.
    header = "## Pre-Flight Disk Targets"
    assert header in status_md
    section = status_md.split(header, 1)[1].split("## ", 1)[0]

    assert section.count("`social.scrape_jobs`") == 1
    assert section.count("`social.instagram_comments`") == 1
    # instagram_comments (100k) sorts ahead of scrape_jobs (50k).
    assert section.index("`social.instagram_comments`") < section.index("`social.scrape_jobs`")


def test_status_md_disk_targets_caps_at_top_5(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    counts = [7000, 6000, 5000, 4000, 3000, 2000, 1000]
    entries = [
        _wave1_entry(
            table=f"table_{count}",
            constraint_name=f"table_{count}_account_id_fkey",
            proposed_index_name=f"social_table_{count}_account_id_idx",
            estimated_row_count=count,
        )
        for count in counts
    ]
    _write_wave1_inventory(docs_root, entries)
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    status_md = (docs_root / "wave-1-status.md").read_text(encoding="utf-8")
    header = "## Pre-Flight Disk Targets"
    section = status_md.split(header, 1)[1].split("## ", 1)[0]
    disk_lines = [line for line in section.splitlines() if line.startswith("- `")]

    assert len(disk_lines) == 5
    assert "`social.table_1000`" not in section
    assert "`social.table_2000`" not in section
    assert "`social.table_3000`" in section


def test_status_md_disk_targets_empty_fallback_when_no_positive_counts(tmp_path: Path, monkeypatch) -> None:
    docs_root = tmp_path / "docs"
    entries = [
        _wave1_entry(
            table="empty_a",
            constraint_name="empty_a_account_id_fkey",
            proposed_index_name="social_empty_a_account_id_idx",
            estimated_row_count=0,
        ),
        _wave1_entry(
            table="empty_b",
            constraint_name="empty_b_account_id_fkey",
            proposed_index_name="social_empty_b_account_id_idx",
            estimated_row_count=0,
        ),
    ]
    _write_wave1_inventory(docs_root, entries)
    monkeypatch.setattr(artifact_mod, "DOCS_ROOT", docs_root)

    artifact_mod.build_wave_artifacts("wave-1")

    status_md = (docs_root / "wave-1-status.md").read_text(encoding="utf-8")
    header = "## Pre-Flight Disk Targets"
    section = status_md.split(header, 1)[1].split("## ", 1)[0]
    assert "- (no large tables in this wave)" in section
    row_lines = [line for line in section.splitlines() if line.startswith("- `") and "estimated_row_count" in line]
    assert row_lines == []
