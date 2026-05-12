#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg2
import psycopg2.extras
import yaml

try:
    from scripts._db_url import ResolvedDbUrl, resolve_db_url, resolve_direct_db_url
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts._db_url import ResolvedDbUrl, resolve_db_url, resolve_direct_db_url


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parent
DEFAULT_ADVISOR_SNAPSHOT = WORKSPACE_ROOT / "docs/workspace/supabase-advisor-snapshot-2026-04-27.md"
DEFAULT_OUTPUT = WORKSPACE_ROOT / "docs/workspace/unused-index-advisor-review-2026-04-27.md"
FK_HARDENING_DOCS = REPO_ROOT / "docs/db/fk-index-hardening"
MIGRATIONS_DIR = REPO_ROOT / "supabase/migrations"
REVIEW_SCHEMAS = [
    "social",
    "core",
    "public",
    "admin",
    "ml",
    "screenalytics",
    "firebase_surveys",
    "surveys",
    "pipeline",
]

ADVISOR_UNUSED_RE = re.compile(r"^- `(?P<index>[^`]+)` on `(?P<object>[^`]+)`")
ADVISOR_UNUSED_DETAIL_RE = re.compile(
    r"Index\s+\\?`(?P<index>[^`\\]+)\\?`\s+on\s+table\s+\\?`(?P<object>[^`\\]+)\\?`\s+has\s+not\s+been\s+used",
    flags=re.IGNORECASE,
)
CREATE_INDEX_RE = re.compile(
    r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>(?:\"[^\"]+\"|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w$]*))?)\s+ON\b",
    flags=re.IGNORECASE,
)
CREATE_INDEX_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>(?:\"[^\"]+\"|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w$]*))?)\s+ON\s+"
    r"(?:ONLY\s+)?(?P<table>(?:\"[^\"]+\"|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w$]*))?)",
    flags=re.IGNORECASE,
)

INDEX_STATS_SQL = """
select
  n.nspname as schema_name,
  t.relname as table_name,
  i.relname as index_name,
  coalesce(s.idx_scan, 0)::bigint as idx_scan,
  coalesce(s.idx_tup_read, 0)::bigint as idx_tup_read,
  coalesce(s.idx_tup_fetch, 0)::bigint as idx_tup_fetch,
  pg_relation_size(i.oid)::bigint as index_bytes,
  pg_total_relation_size(t.oid)::bigint as table_bytes,
  pg_size_pretty(pg_relation_size(i.oid)) as index_size,
  pg_size_pretty(pg_total_relation_size(t.oid)) as table_size,
  ix.indisprimary as is_primary,
  ix.indisunique as is_unique,
  ix.indisexclusion as is_exclusion,
  con.conname as constraint_name,
  con.contype as constraint_type,
  pg_get_indexdef(i.oid) as index_definition
from pg_index ix
join pg_class i on i.oid = ix.indexrelid
join pg_class t on t.oid = ix.indrelid
join pg_namespace n on n.oid = t.relnamespace
left join pg_stat_user_indexes s on s.indexrelid = i.oid
left join pg_constraint con on con.conindid = i.oid
where n.nspname = any(%s::text[])
  and t.relkind in ('r', 'p')
order by coalesce(s.idx_scan, 0), n.nspname, t.relname, i.relname
"""

OWNER_PACKET_COLUMNS = [
    "workload",
    "owner",
    "schema",
    "table",
    "index",
    "idx_scan",
    "idx_tup_read",
    "idx_tup_fetch",
    "index_size",
    "table_size",
    "advisor_reported",
    "migration_version",
    "migration_path",
    "review_status",
    "approved_to_drop",
    "approval_reason",
    "approved_by",
    "reviewed_routes_or_jobs",
    "stats_window_checked_at",
    "rollback_sql",
    "drop_sql",
]

APPROVAL_REQUIRED_COLUMNS = [
    "approval_reason",
    "approved_by",
    "reviewed_routes_or_jobs",
    "stats_window_checked_at",
    "rollback_sql",
]


@dataclass(frozen=True)
class AdvisorIndex:
    schema: str
    table: str
    index: str


def _strip_identifier(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] == '"':
        return stripped[1:-1]
    return stripped


def _split_qualified_name(value: str) -> tuple[str, str] | None:
    normalized = re.sub(r"\s*\.\s*", ".", value.strip())
    if "." not in normalized:
        return None
    schema, table = normalized.split(".", 1)
    return _strip_identifier(schema), _strip_identifier(table)


def parse_advisor_unused_indexes(snapshot_text: str) -> set[AdvisorIndex]:
    """Parse exact unused-index bullets from the saved advisor snapshot.

    The 2026-04-27 snapshot intentionally summarizes large schemas, so this
    parser only treats explicit `index` on `schema.table` bullets as matches.
    """

    stripped = snapshot_text.lstrip()
    if stripped.startswith("{"):
        payload = json.loads(stripped)
        parsed: set[AdvisorIndex] = set()
        for lint in payload.get("lints") or []:
            if lint.get("name") != "unused_index":
                continue
            detail = str(lint.get("detail") or "")
            match = ADVISOR_UNUSED_DETAIL_RE.search(detail)
            if not match:
                continue
            qualified = _split_qualified_name(match.group("object"))
            if not qualified:
                continue
            schema, table = qualified
            parsed.add(AdvisorIndex(schema=schema, table=table, index=match.group("index").strip()))
        return parsed

    in_unused_section = False
    parsed: set[AdvisorIndex] = set()
    for raw_line in snapshot_text.splitlines():
        line = raw_line.strip()
        if line.startswith("### `unused_index`"):
            in_unused_section = True
            continue
        if in_unused_section and line.startswith("### "):
            break
        if not in_unused_section:
            continue
        match = ADVISOR_UNUSED_RE.match(line)
        if not match:
            continue
        qualified = _split_qualified_name(match.group("object"))
        if not qualified:
            continue
        schema, table = qualified
        parsed.add(AdvisorIndex(schema=schema, table=table, index=match.group("index").strip()))
    return parsed


def _index_name_from_create_target(raw_name: str) -> str:
    final_part = raw_name.split(".")[-1]
    return _strip_identifier(final_part)


def _migration_date_from_version(version: str) -> date | None:
    if len(version) < 8 or not version[:8].isdigit():
        return None
    try:
        return date(int(version[:4]), int(version[4:6]), int(version[6:8]))
    except ValueError:
        return None


def discover_index_migration_sources(
    migrations_dir: Path = MIGRATIONS_DIR,
    *,
    recent_days: int = 14,
    reference_date: date | None = None,
) -> dict[AdvisorIndex, dict[str, Any]]:
    reference = reference_date or date.today()
    recent_cutoff = reference - timedelta(days=recent_days)
    sources: dict[AdvisorIndex, dict[str, Any]] = {}
    if not migrations_dir.is_dir():
        return sources

    for path in sorted(migrations_dir.glob("*.sql")):
        version = path.name.split("_", 1)[0]
        migration_date = _migration_date_from_version(version)
        try:
            sql = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            sql = path.read_text(encoding="utf-8", errors="ignore")
        for match in CREATE_INDEX_TABLE_RE.finditer(sql):
            index_name = _index_name_from_create_target(match.group("name"))
            qualified_table = _split_qualified_name(match.group("table"))
            if not qualified_table:
                continue
            schema, table = qualified_table
            try:
                migration_path = str(path.relative_to(REPO_ROOT))
            except ValueError:
                migration_path = str(path)
            sources.setdefault(
                AdvisorIndex(schema=schema, table=table, index=index_name),
                {
                    "migration_version": version,
                    "migration_path": migration_path,
                    "migration_recent": bool(migration_date and migration_date >= recent_cutoff),
                },
            )
    return sources


def load_fk_hardening_indexes(docs_root: Path = FK_HARDENING_DOCS) -> set[AdvisorIndex]:
    indexes: set[AdvisorIndex] = set()
    if not docs_root.is_dir():
        return indexes
    for path in sorted(docs_root.glob("wave-*-inventory.yml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for entry in payload.get("entries") or []:
            name = entry.get("proposed_index_name")
            schema = entry.get("schema")
            table = entry.get("table")
            if name and schema and table and entry.get("decision") == "add":
                indexes.add(AdvisorIndex(schema=str(schema), table=str(table), index=str(name)))
    return indexes


def workload_group(schema: str) -> str:
    if schema == "social":
        return "social write-heavy candidates"
    if schema == "core":
        return "core catalog/media candidates"
    if schema in {"public", "surveys", "firebase_surveys"}:
        return "public/survey candidates"
    if schema == "admin":
        return "admin candidates"
    if schema in {"ml", "screenalytics"}:
        return "ml/screenalytics candidates"
    return "pipeline/other candidates"


def workload_owner(schema: str) -> str:
    owners = {
        "social": "social data/backfill owner",
        "core": "catalog/media owner",
        "public": "survey/public app owner",
        "surveys": "survey/public app owner",
        "firebase_surveys": "survey/public app owner",
        "admin": "admin tooling owner",
        "ml": "screenalytics/ml owner",
        "screenalytics": "screenalytics/ml owner",
        "pipeline": "pipeline owner",
    }
    return owners.get(schema, "route owner required")


def _exclusion_reasons(
    row: dict[str, Any],
    *,
    fk_hardening_indexes: set[AdvisorIndex],
    migration_sources: dict[AdvisorIndex, dict[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    index_key = AdvisorIndex(
        schema=str(row["schema_name"]),
        table=str(row["table_name"]),
        index=str(row["index_name"]),
    )
    if row.get("is_primary"):
        reasons.append("primary-key")
    if row.get("is_unique"):
        reasons.append("unique-index")
    if row.get("is_exclusion"):
        reasons.append("exclusion-index")
    if row.get("constraint_name"):
        reasons.append("constraint-backed")
    if index_key in fk_hardening_indexes:
        reasons.append("fk-hardening-index")
    if migration_sources.get(index_key, {}).get("migration_recent"):
        reasons.append("recent-migration")
    return reasons


def normalize_index_row(
    row: dict[str, Any],
    *,
    advisor_indexes: set[AdvisorIndex],
    fk_hardening_indexes: set[AdvisorIndex],
    migration_sources: dict[AdvisorIndex, dict[str, Any]],
) -> dict[str, Any]:
    schema = str(row["schema_name"])
    table = str(row["table_name"])
    index_name = str(row["index_name"])
    index_key = AdvisorIndex(schema=schema, table=table, index=index_name)
    advisor_reported = index_key in advisor_indexes
    source = migration_sources.get(index_key, {})
    reasons = _exclusion_reasons(row, fk_hardening_indexes=fk_hardening_indexes, migration_sources=migration_sources)
    idx_scan = int(row.get("idx_scan") or 0)

    if reasons:
        status = "excluded"
    elif idx_scan > 0:
        status = "defer:idx_scan_nonzero"
    elif not advisor_reported:
        status = "defer:missing_advisor_match"
    else:
        status = "drop_review_required"

    return {
        "workload": workload_group(schema),
        "owner": workload_owner(schema),
        "schema": schema,
        "table": table,
        "index": index_name,
        "idx_scan": idx_scan,
        "idx_tup_read": int(row.get("idx_tup_read") or 0),
        "idx_tup_fetch": int(row.get("idx_tup_fetch") or 0),
        "index_size": row.get("index_size") or "",
        "table_size": row.get("table_size") or "",
        "index_bytes": int(row.get("index_bytes") or 0),
        "table_bytes": int(row.get("table_bytes") or 0),
        "advisor_reported": advisor_reported,
        "constraint_backed": bool(row.get("constraint_name")),
        "constraint_name": row.get("constraint_name") or "",
        "constraint_type": row.get("constraint_type") or "",
        "migration_version": source.get("migration_version", ""),
        "migration_path": source.get("migration_path", ""),
        "migration_recent": bool(source.get("migration_recent")),
        "index_definition": row.get("index_definition") or "",
        "exclude_reasons": ",".join(reasons),
        "review_status": status,
        "approved_to_drop": "no",
        "rollback_sql_required": "yes" if status == "drop_review_required" else "n/a",
    }


def build_report_rows(
    live_rows: list[dict[str, Any]],
    *,
    advisor_indexes: set[AdvisorIndex],
    fk_hardening_indexes: set[AdvisorIndex],
    migration_sources: dict[AdvisorIndex, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        normalize_index_row(
            row,
            advisor_indexes=advisor_indexes,
            fk_hardening_indexes=fk_hardening_indexes,
            migration_sources=migration_sources,
        )
        for row in live_rows
    ]
    return sorted(rows, key=lambda row: (row["workload"], row["idx_scan"], row["schema"], row["table"], row["index"]))


def fetch_live_index_rows(resolved: ResolvedDbUrl, schemas: list[str]) -> list[dict[str, Any]]:
    with psycopg2.connect(resolved.value, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(INDEX_STATS_SQL, [schemas])
            return [dict(row) for row in cur.fetchall()]


def _markdown_cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_markdown_cell(row.get(column, "")) for column in columns) + " |")
    return lines


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "owner"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def drop_index_sql(row: dict[str, Any]) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_quote_ident(row['schema'])}.{_quote_ident(row['index'])};"


def rollback_sql(row: dict[str, Any]) -> str:
    definition = str(row.get("index_definition") or "").strip().rstrip(";")
    return f"{definition};" if definition else ""


def owner_packet_row(row: dict[str, Any]) -> dict[str, Any]:
    packet = {column: row.get(column, "") for column in OWNER_PACKET_COLUMNS}
    packet["approved_to_drop"] = "no"
    packet["approval_reason"] = ""
    packet["approved_by"] = ""
    packet["reviewed_routes_or_jobs"] = ""
    packet["stats_window_checked_at"] = ""
    packet["rollback_sql"] = rollback_sql(row)
    packet["drop_sql"] = drop_index_sql(row)
    return packet


def write_owner_review_packets(
    rows: list[dict[str, Any]],
    output_dir: Path,
    *,
    metadata: dict[str, Any],
    force: bool = False,
) -> list[dict[str, Any]]:
    candidates = [row for row in rows if row["review_status"] == "drop_review_required"]
    packets = [owner_packet_row(row) for row in candidates]
    output_dir.mkdir(parents=True, exist_ok=True)

    by_owner: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for packet in packets:
        by_owner[str(packet["owner"])].append(packet)

    index_rows = [
        {
            "owner": owner,
            "candidate_count": len(owner_rows),
            "packet_csv": f"{_slug(owner)}.csv",
            "packet_markdown": f"{_slug(owner)}.md",
        }
        for owner, owner_rows in sorted(by_owner.items())
    ]
    index_lines = [
        "# Unused Index Owner Review Packets",
        "",
        "Status: review-only. Every `approved_to_drop` value starts as `no`.",
        "",
        "Only rows that remain `approved_to_drop=yes` after owner review and have all required approval fields may be "
        "rendered into Phase 3 drop SQL.",
        "",
        "Required approval fields: "
        + ", ".join(f"`{column}`" for column in ["approved_to_drop", *APPROVAL_REQUIRED_COLUMNS])
        + ".",
        "",
        "## Inputs",
        "",
        f"- Advisor snapshot: `{metadata['advisor_snapshot']}`",
        f"- Resolved DB host: `{metadata['resolved_db_host']}`",
        f"- Candidate source rows: `{len(candidates)}`",
        "",
        "## Packets",
        "",
        *_table(index_rows, ["owner", "candidate_count", "packet_csv", "packet_markdown"]),
        "",
        "## Phase 3 Gate",
        "",
        "Render drop SQL with:",
        "",
        "```bash",
        "cd /Users/thomashulihan/Projects/TRR/TRR-Backend",
        ".venv/bin/python scripts/db/unused_index_evidence_report.py \\",
        f"  --approval-packet-dir {output_dir} \\",
        f"  --drop-sql-output {output_dir / 'phase3-approved-drops.sql'}",
        "```",
    ]
    index_path = output_dir / "README.md"
    if index_path.exists() and not force:
        raise FileExistsError(f"{index_path} already exists; pass --force-owner-packets to regenerate packets.")
    index_path.write_text("\n".join(index_lines).rstrip() + "\n", encoding="utf-8")

    markdown_columns = [
        "schema",
        "table",
        "index",
        "idx_scan",
        "index_size",
        "table_size",
        "migration_path",
        "approved_to_drop",
        "drop_sql",
    ]
    for owner, owner_rows in sorted(by_owner.items()):
        slug = _slug(owner)
        csv_path = output_dir / f"{slug}.csv"
        markdown_path = output_dir / f"{slug}.md"
        for path in (csv_path, markdown_path):
            if path.exists() and not force:
                raise FileExistsError(f"{path} already exists; pass --force-owner-packets to regenerate packets.")

        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=OWNER_PACKET_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(owner_rows)

        markdown_lines = [
            f"# Unused Index Owner Packet - {owner}",
            "",
            "Status: review-only. No index is approved by default.",
            "",
            "Approval requirements:",
            "",
            "- Set `approved_to_drop=yes` only after route/job review.",
            "- Fill `approval_reason`, `approved_by`, `reviewed_routes_or_jobs`, and `stats_window_checked_at`.",
            "- Keep the generated `rollback_sql`; it was captured from `pg_get_indexdef` for this live index.",
            "- Do not approve rows whose workload has not had a meaningful stats window, unless the owner records an "
            "urgent approval reason.",
            "",
            f"Candidate count: `{len(owner_rows)}`.",
            "",
            "Full rollback SQL is in the companion CSV.",
            "",
            *_table(owner_rows, markdown_columns),
            "",
        ]
        markdown_path.write_text("\n".join(markdown_lines).rstrip() + "\n", encoding="utf-8")

    return packets


def read_owner_packet_rows(packet_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(packet_dir.glob("*.csv")):
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                row["_packet_path"] = str(path)
                rows.append(row)
    return rows


def _is_yes(value: Any) -> bool:
    return str(value).strip().lower() == "yes"


def _line_comment(value: str) -> list[str]:
    return [f"-- {line}" if line else "--" for line in value.splitlines()]


def validate_approved_rows(rows: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for row in rows:
        label = f"{row.get('schema')}.{row.get('index')} ({row.get('_packet_path', 'packet')})"
        if str(row.get("review_status", "")).strip() != "drop_review_required":
            errors.append(f"{label}: approved row must have review_status=drop_review_required")
        if not _is_yes(row.get("advisor_reported")) and str(row.get("advisor_reported")).lower() != "true":
            errors.append(f"{label}: approved row must be advisor_reported")
        try:
            idx_scan = int(row.get("idx_scan") or 0)
        except ValueError:
            idx_scan = -1
        if idx_scan != 0:
            errors.append(f"{label}: approved row must still have idx_scan=0 in the packet")
        for column in APPROVAL_REQUIRED_COLUMNS:
            if not str(row.get(column, "")).strip():
                errors.append(f"{label}: missing required approval field {column}")
        rollback = str(row.get("rollback_sql") or "").strip()
        if rollback and not re.match(r"^create\s+(?:unique\s+)?index\b", rollback, flags=re.IGNORECASE):
            errors.append(f"{label}: rollback_sql must start with CREATE INDEX or CREATE UNIQUE INDEX")
    return errors


def render_approved_drop_sql(packet_rows: list[dict[str, Any]], *, source_label: str) -> str:
    approved = [row for row in packet_rows if _is_yes(row.get("approved_to_drop"))]
    errors = validate_approved_rows(approved)
    if errors:
        raise ValueError("\n".join(errors))

    lines = [
        "-- Phase 3 approved unused-index drops.",
        "-- Generated by TRR-Backend/scripts/db/unused_index_evidence_report.py.",
        f"-- Source: {source_label}",
        "-- Execute manually with owner approval. Do not wrap DROP INDEX CONCURRENTLY in an explicit transaction.",
        "",
    ]
    if not approved:
        lines.extend(
            [
                "-- No indexes are explicitly approved to drop.",
                "-- Add approved_to_drop=yes and the required approval fields in owner packet CSVs, then regenerate "
                "this file.",
                "",
            ]
        )
        return "\n".join(lines)

    for row in sorted(approved, key=lambda item: (item["owner"], item["schema"], item["table"], item["index"])):
        lines.extend(
            [
                f"-- owner: {row['owner']}",
                f"-- table: {_quote_ident(row['schema'])}.{_quote_ident(row['table'])}",
                f"-- index: {_quote_ident(row['schema'])}.{_quote_ident(row['index'])}",
                f"-- approval_reason: {row['approval_reason']}",
                f"-- approved_by: {row['approved_by']}",
                f"-- reviewed_routes_or_jobs: {row['reviewed_routes_or_jobs']}",
                f"-- stats_window_checked_at: {row['stats_window_checked_at']}",
                drop_index_sql(row),
                "",
            ]
        )

    lines.extend(["-- Rollback SQL captured in owner packets:", ""])
    for row in sorted(approved, key=lambda item: (item["owner"], item["schema"], item["table"], item["index"])):
        lines.extend([f"-- rollback for {_quote_ident(row['schema'])}.{_quote_ident(row['index'])}:"])
        lines.extend(_line_comment(str(row["rollback_sql"]).strip()))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_markdown_report(rows: list[dict[str, Any]], *, metadata: dict[str, Any]) -> str:
    status_counts = Counter(row["review_status"] for row in rows)
    workload_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        workload_counts[row["workload"]]["total"] += 1
        if row["advisor_reported"]:
            workload_counts[row["workload"]]["advisor_reported"] += 1
        if row["review_status"] == "drop_review_required":
            workload_counts[row["workload"]]["drop_review_required"] += 1
        if row["review_status"] == "excluded":
            workload_counts[row["workload"]]["excluded"] += 1

    summary_rows = [
        {
            "workload": workload,
            "total": counts["total"],
            "advisor_reported": counts["advisor_reported"],
            "drop_review_required": counts["drop_review_required"],
            "excluded": counts["excluded"],
        }
        for workload, counts in sorted(workload_counts.items())
    ]

    lines = [
        "# Unused Index Advisor Review",
        "",
        "Generated by `TRR-Backend/scripts/db/unused_index_evidence_report.py`. This report contains no database "
        "credentials.",
        "",
        "Status: evidence gate only. No index in this report is approved to drop by default.",
        "",
        "## Inputs",
        "",
        f"- Advisor snapshot: `{metadata['advisor_snapshot']}`",
        f"- Connection source: `{metadata['db_url_source']}`",
        f"- Resolved DB host: `{metadata['resolved_db_host']}`",
        f"- Review schemas: `{', '.join(metadata['schemas'])}`",
        f"- Exact advisor unused-index bullets parsed: `{metadata['advisor_exact_match_count']}`",
        f"- FK-hardening indexes excluded by default: `{metadata['fk_hardening_count']}`",
        "",
        "## Summary",
        "",
        *_table(summary_rows, ["workload", "total", "advisor_reported", "drop_review_required", "excluded"]),
        "",
        "## Status Counts",
        "",
        *_table(
            [{"status": status, "count": count} for status, count in sorted(status_counts.items())],
            ["status", "count"],
        ),
        "",
        "## Review Rules",
        "",
        "- `advisor_reported` means the saved snapshot explicitly named the index. It is not approval to drop.",
        "- `drop_review_required` means live stats show `idx_scan = 0`, the snapshot named the index, and default "
        "exclusions did not apply.",
        "- `approved_to_drop` is always `no` until a route owner records reason, rollback SQL, and a 7-day production "
        "recheck or explicit urgent approval.",
        "- Constraint-backed, primary, unique, exclusion, FK-hardening, and recent-migration indexes are excluded by "
        "default.",
        "",
    ]

    detail_columns = [
        "schema",
        "table",
        "index",
        "idx_scan",
        "idx_tup_read",
        "idx_tup_fetch",
        "index_size",
        "table_size",
        "advisor_reported",
        "review_status",
        "approved_to_drop",
        "owner",
        "exclude_reasons",
        "migration_path",
    ]
    for workload in sorted({row["workload"] for row in rows}):
        workload_rows = [row for row in rows if row["workload"] == workload]
        lines.extend([f"## {workload.title()}", "", *_table(workload_rows, detail_columns), ""])

    return "\n".join(lines).rstrip() + "\n"


def write_csv(rows: list[dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "workload",
        "owner",
        "schema",
        "table",
        "index",
        "idx_scan",
        "idx_tup_read",
        "idx_tup_fetch",
        "index_size",
        "table_size",
        "advisor_reported",
        "constraint_backed",
        "constraint_name",
        "constraint_type",
        "migration_version",
        "migration_path",
        "migration_recent",
        "exclude_reasons",
        "review_status",
        "approved_to_drop",
        "rollback_sql_required",
    ]
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _resolve_connection(connection_mode: str) -> ResolvedDbUrl:
    if connection_mode == "direct":
        return resolve_direct_db_url(
            allow_database_url=False,
            allow_deprecated_supabase_db_url=False,
            allow_local_supabase_status=False,
        )
    if connection_mode == "runtime":
        return resolve_db_url(
            allow_database_url=False,
            allow_deprecated_supabase_db_url=False,
            allow_local_supabase_status=False,
        )
    raise RuntimeError(f"Unsupported connection mode {connection_mode!r}.")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an unused-index evidence review from live pg_stat_user_indexes."
    )
    parser.add_argument("--advisor-snapshot", type=Path, default=DEFAULT_ADVISOR_SNAPSHOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--csv-output", type=Path, default=None)
    parser.add_argument("--schemas", default=",".join(REVIEW_SCHEMAS), help="Comma-separated schemas to review.")
    parser.add_argument("--connection-mode", choices=["direct", "runtime"], default="direct")
    parser.add_argument("--recent-days", type=int, default=14)
    parser.add_argument("--owner-packet-dir", type=Path, default=None)
    parser.add_argument("--force-owner-packets", action="store_true")
    parser.add_argument("--approval-packet-dir", type=Path, default=None)
    parser.add_argument("--drop-sql-output", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.approval_packet_dir:
        if not args.drop_sql_output:
            print(
                "[unused-index-evidence] ERROR: --drop-sql-output is required with --approval-packet-dir",
                file=sys.stderr,
            )
            return 2
        packet_rows = read_owner_packet_rows(args.approval_packet_dir)
        try:
            rendered_drop_sql = render_approved_drop_sql(packet_rows, source_label=str(args.approval_packet_dir))
        except ValueError as exc:
            print(f"[unused-index-evidence] ERROR: {exc}", file=sys.stderr)
            return 2
        args.drop_sql_output.parent.mkdir(parents=True, exist_ok=True)
        args.drop_sql_output.write_text(rendered_drop_sql, encoding="utf-8")
        return 0

    if args.drop_sql_output and not args.owner_packet_dir:
        print(
            "[unused-index-evidence] ERROR: --drop-sql-output requires --owner-packet-dir or --approval-packet-dir",
            file=sys.stderr,
        )
        return 2

    schemas = [schema.strip() for schema in args.schemas.split(",") if schema.strip()]
    advisor_text = args.advisor_snapshot.read_text(encoding="utf-8")
    advisor_indexes = parse_advisor_unused_indexes(advisor_text)
    migration_sources = discover_index_migration_sources(recent_days=args.recent_days)
    fk_hardening_indexes = load_fk_hardening_indexes()
    try:
        resolved = _resolve_connection(args.connection_mode)
    except RuntimeError as exc:
        print(f"[unused-index-evidence] ERROR: {exc}", file=sys.stderr)
        return 2

    live_rows = fetch_live_index_rows(resolved, schemas)
    rows = build_report_rows(
        live_rows,
        advisor_indexes=advisor_indexes,
        fk_hardening_indexes=fk_hardening_indexes,
        migration_sources=migration_sources,
    )
    metadata = {
        "advisor_snapshot": str(args.advisor_snapshot),
        "db_url_source": resolved.source,
        "resolved_db_host": urlsplit(resolved.value).hostname or "",
        "schemas": schemas,
        "advisor_exact_match_count": len(advisor_indexes),
        "fk_hardening_count": len(fk_hardening_indexes),
    }
    rendered = render_markdown_report(rows, metadata=metadata)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    write_csv(rows, args.csv_output or args.output.with_suffix(".csv"))
    packet_rows: list[dict[str, Any]] = []
    if args.owner_packet_dir:
        try:
            packet_rows = write_owner_review_packets(
                rows,
                args.owner_packet_dir,
                metadata=metadata,
                force=args.force_owner_packets,
            )
        except FileExistsError as exc:
            print(f"[unused-index-evidence] ERROR: {exc}", file=sys.stderr)
            return 2
    if args.drop_sql_output:
        try:
            rendered_drop_sql = render_approved_drop_sql(packet_rows, source_label=str(args.owner_packet_dir))
        except ValueError as exc:
            print(f"[unused-index-evidence] ERROR: {exc}", file=sys.stderr)
            return 2
        args.drop_sql_output.parent.mkdir(parents=True, exist_ok=True)
        args.drop_sql_output.write_text(rendered_drop_sql, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
