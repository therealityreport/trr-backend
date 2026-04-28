from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import yaml

import scripts.db.unused_index_evidence_report as report_mod


def _live_row(
    *,
    schema: str = "social",
    table: str = "posts",
    index: str = "posts_user_id_idx",
    idx_scan: int = 0,
    is_primary: bool = False,
    is_unique: bool = False,
    is_exclusion: bool = False,
    constraint_name: str | None = None,
) -> dict[str, object]:
    return {
        "schema_name": schema,
        "table_name": table,
        "index_name": index,
        "idx_scan": idx_scan,
        "idx_tup_read": 0,
        "idx_tup_fetch": 0,
        "index_bytes": 1024,
        "table_bytes": 4096,
        "index_size": "1024 bytes",
        "table_size": "4096 bytes",
        "is_primary": is_primary,
        "is_unique": is_unique,
        "is_exclusion": is_exclusion,
        "constraint_name": constraint_name,
        "constraint_type": "p" if constraint_name else None,
        "index_definition": f'CREATE INDEX {index} ON {schema}.{table} (created_at)',
    }


def test_parse_advisor_unused_indexes_reads_exact_bullets_only() -> None:
    snapshot = """
### `unused_index` (2)

#### `social`
- `posts_user_id_idx` on `social.posts`
- `quoted_table_idx` on `core."QuotedTable"`
- _...and 100 more in `social`._

### `unindexed_foreign_keys`
- `ignored_idx` on `social.posts`
"""

    parsed = report_mod.parse_advisor_unused_indexes(snapshot)

    assert report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx") in parsed
    assert report_mod.AdvisorIndex("core", "QuotedTable", "quoted_table_idx") in parsed
    assert report_mod.AdvisorIndex("social", "posts", "ignored_idx") not in parsed
    assert len(parsed) == 2


def test_parse_advisor_unused_indexes_reads_management_api_json() -> None:
    payload = {
        "lints": [
            {
                "name": "unused_index",
                "detail": "Index `posts_user_id_idx` on table `social.posts` has not been used",
            },
            {
                "name": "unindexed_foreign_keys",
                "detail": "Table `social.posts` has a foreign key without a covering index.",
            },
        ]
    }

    parsed = report_mod.parse_advisor_unused_indexes(json.dumps(payload))

    assert parsed == {report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx")}


def test_discover_index_migration_sources_marks_recent_indexes(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "20260428120000_recent.sql").write_text(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "social"."posts_user_id_idx" ON social.posts (user_id);\n',
        encoding="utf-8",
    )
    (migrations / "20260101000000_old.sql").write_text(
        "CREATE INDEX old_posts_idx ON social.posts (created_at);\n",
        encoding="utf-8",
    )

    sources = report_mod.discover_index_migration_sources(
        migrations,
        reference_date=date(2026, 4, 28),
    )

    recent_key = report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx")
    old_key = report_mod.AdvisorIndex("social", "posts", "old_posts_idx")
    assert sources[recent_key]["migration_recent"] is True
    assert sources[old_key]["migration_recent"] is False
    assert sources[recent_key]["migration_version"] == "20260428120000"


def test_load_fk_hardening_indexes_reads_add_decisions(tmp_path: Path) -> None:
    docs_root = tmp_path / "fk-index-hardening"
    docs_root.mkdir()
    (docs_root / "wave-1-inventory.yml").write_text(
        yaml.safe_dump(
            {
                "entries": [
                    {
                        "schema": "social",
                        "table": "posts",
                        "proposed_index_name": "social_posts_user_id_idx",
                        "decision": "add",
                    },
                    {
                        "schema": "social",
                        "table": "posts",
                        "proposed_index_name": "social_posts_skip_idx",
                        "decision": "skip-covered",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    indexes = report_mod.load_fk_hardening_indexes(docs_root)

    assert indexes == {report_mod.AdvisorIndex("social", "posts", "social_posts_user_id_idx")}


def test_build_report_rows_separates_advisor_reported_from_drop_approval() -> None:
    advisor_indexes = {report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx")}
    rows = report_mod.build_report_rows(
        [
            _live_row(index="posts_user_id_idx"),
            _live_row(index="posts_seen_idx", idx_scan=12),
            _live_row(index="posts_missing_advisor_idx"),
            _live_row(index="posts_constraint_idx", constraint_name="posts_pkey"),
            _live_row(index="posts_unique_idx", is_unique=True),
            _live_row(index="posts_fk_hardening_idx"),
            _live_row(index="posts_recent_idx"),
        ],
        advisor_indexes=advisor_indexes,
        fk_hardening_indexes={report_mod.AdvisorIndex("social", "posts", "posts_fk_hardening_idx")},
        migration_sources={
            report_mod.AdvisorIndex("social", "posts", "posts_recent_idx"): {
                "migration_version": "20260428120000",
                "migration_path": "supabase/migrations/20260428120000_recent.sql",
                "migration_recent": True,
            }
        },
    )

    by_index = {row["index"]: row for row in rows}

    assert by_index["posts_user_id_idx"]["advisor_reported"] is True
    assert by_index["posts_user_id_idx"]["review_status"] == "drop_review_required"
    assert by_index["posts_user_id_idx"]["approved_to_drop"] == "no"
    assert by_index["posts_seen_idx"]["review_status"] == "defer:idx_scan_nonzero"
    assert by_index["posts_missing_advisor_idx"]["review_status"] == "defer:missing_advisor_match"
    assert by_index["posts_constraint_idx"]["review_status"] == "excluded"
    assert by_index["posts_constraint_idx"]["exclude_reasons"] == "constraint-backed"
    assert by_index["posts_unique_idx"]["exclude_reasons"] == "unique-index"
    assert by_index["posts_fk_hardening_idx"]["exclude_reasons"] == "fk-hardening-index"
    assert by_index["posts_recent_idx"]["exclude_reasons"] == "recent-migration"


def test_build_report_rows_keys_migration_sources_by_schema_table_and_index() -> None:
    rows = report_mod.build_report_rows(
        [
            _live_row(schema="social", table="posts", index="shared_name_idx"),
            _live_row(schema="core", table="posts", index="shared_name_idx"),
        ],
        advisor_indexes={
            report_mod.AdvisorIndex("social", "posts", "shared_name_idx"),
            report_mod.AdvisorIndex("core", "posts", "shared_name_idx"),
        },
        fk_hardening_indexes=set(),
        migration_sources={
            report_mod.AdvisorIndex("social", "posts", "shared_name_idx"): {
                "migration_version": "20260428120000",
                "migration_path": "supabase/migrations/20260428120000_recent.sql",
                "migration_recent": True,
            }
        },
    )

    by_schema = {row["schema"]: row for row in rows}

    assert by_schema["social"]["exclude_reasons"] == "recent-migration"
    assert by_schema["social"]["review_status"] == "excluded"
    assert by_schema["core"]["exclude_reasons"] == ""
    assert by_schema["core"]["review_status"] == "drop_review_required"


def test_render_markdown_report_records_rules_and_groups() -> None:
    rows = report_mod.build_report_rows(
        [_live_row(index="posts_user_id_idx")],
        advisor_indexes={report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx")},
        fk_hardening_indexes=set(),
        migration_sources={},
    )

    markdown = report_mod.render_markdown_report(
        rows,
        metadata={
            "advisor_snapshot": "docs/workspace/supabase-advisor-snapshot-2026-04-27.md",
            "db_url_source": "TRR_DB_DIRECT_URL",
            "resolved_db_host": "db.example.supabase.co",
            "schemas": ["social"],
            "advisor_exact_match_count": 1,
            "fk_hardening_count": 0,
        },
    )

    assert "No index in this report is approved to drop by default." in markdown
    assert "| social write-heavy candidates | 1 | 1 | 1 | 0 |" in markdown
    assert "approved_to_drop" in markdown
    assert "| social | posts | posts_user_id_idx | 0 |" in markdown


def test_write_owner_review_packets_prefills_rollback_and_keeps_approvals_closed(tmp_path: Path) -> None:
    rows = report_mod.build_report_rows(
        [
            _live_row(schema="social", table="posts", index="posts_user_id_idx"),
            _live_row(schema="admin", table="jobs", index="jobs_status_idx"),
        ],
        advisor_indexes={
            report_mod.AdvisorIndex("social", "posts", "posts_user_id_idx"),
            report_mod.AdvisorIndex("admin", "jobs", "jobs_status_idx"),
        },
        fk_hardening_indexes=set(),
        migration_sources={},
    )

    packets = report_mod.write_owner_review_packets(
        rows,
        tmp_path,
        metadata={
            "advisor_snapshot": "snapshot.json",
            "resolved_db_host": "db.example.supabase.co",
        },
    )

    assert len(packets) == 2
    assert (tmp_path / "README.md").exists()
    with (tmp_path / "social-data-backfill-owner.csv").open(encoding="utf-8", newline="") as fh:
        social_rows = list(csv.DictReader(fh))
    assert social_rows[0]["approved_to_drop"] == "no"
    assert social_rows[0]["rollback_sql"] == "CREATE INDEX posts_user_id_idx ON social.posts (created_at);"
    assert social_rows[0]["drop_sql"] == 'DROP INDEX CONCURRENTLY IF EXISTS "social"."posts_user_id_idx";'


def test_render_approved_drop_sql_emits_no_drops_without_explicit_approval() -> None:
    sql = report_mod.render_approved_drop_sql(
        [
            {
                "owner": "social data/backfill owner",
                "schema": "social",
                "table": "posts",
                "index": "posts_user_id_idx",
                "idx_scan": "0",
                "advisor_reported": "True",
                "review_status": "drop_review_required",
                "approved_to_drop": "no",
            }
        ],
        source_label="packets",
    )

    assert "No indexes are explicitly approved to drop" in sql
    assert "DROP INDEX CONCURRENTLY IF EXISTS" not in sql


def test_render_approved_drop_sql_requires_approval_fields() -> None:
    rows = [
        {
            "owner": "social data/backfill owner",
            "schema": "social",
            "table": "posts",
            "index": "posts_user_id_idx",
            "idx_scan": "0",
            "advisor_reported": "True",
            "review_status": "drop_review_required",
            "approved_to_drop": "yes",
            "approval_reason": "",
            "approved_by": "owner",
            "reviewed_routes_or_jobs": "admin social page",
            "stats_window_checked_at": "2026-04-28",
            "rollback_sql": "CREATE INDEX posts_user_id_idx ON social.posts (created_at);",
        }
    ]

    try:
        report_mod.render_approved_drop_sql(rows, source_label="packets")
    except ValueError as exc:
        assert "missing required approval field approval_reason" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("approved rows without approval_reason must fail")


def test_render_approved_drop_sql_only_emits_approved_rows() -> None:
    rows = [
        {
            "owner": "admin tooling owner",
            "schema": "admin",
            "table": "jobs",
            "index": "jobs_status_idx",
            "idx_scan": "0",
            "advisor_reported": "True",
            "review_status": "drop_review_required",
            "approved_to_drop": "yes",
            "approval_reason": "route no longer filters by status",
            "approved_by": "admin owner",
            "reviewed_routes_or_jobs": "admin jobs route",
            "stats_window_checked_at": "2026-04-28T00:00:00Z",
            "rollback_sql": "CREATE INDEX jobs_status_idx ON admin.jobs (status);",
        },
        {
            "owner": "social data/backfill owner",
            "schema": "social",
            "table": "posts",
            "index": "posts_user_id_idx",
            "idx_scan": "0",
            "advisor_reported": "True",
            "review_status": "drop_review_required",
            "approved_to_drop": "no",
            "rollback_sql": "CREATE INDEX posts_user_id_idx ON social.posts (created_at);",
        },
    ]

    sql = report_mod.render_approved_drop_sql(rows, source_label="packets")

    assert 'DROP INDEX CONCURRENTLY IF EXISTS "admin"."jobs_status_idx";' in sql
    assert "jobs_status_idx ON admin.jobs" in sql
    assert "posts_user_id_idx" not in sql
