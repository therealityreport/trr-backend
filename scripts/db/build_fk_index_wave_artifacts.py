from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_ROOT = REPO_ROOT / "docs" / "db" / "fk-index-hardening"
SUPPORTED_QUERY_CHECK_SCHEMAS = {
    "wave-1": {"admin", "core", "social"},
    "wave-2": {"firebase_surveys", "ml", "pipeline", "screenalytics", "surveys"},
}
QUERY_CHECK_ARTIFACTS = {
    "wave-1": "scripts/db/fk_index_wave1_explain.sql",
    "wave-2": "scripts/db/fk_index_wave2_explain.sql",
}


def load_inventory(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a top-level mapping.")
    return data


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sort_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        entries,
        key=lambda entry: (
            str(entry.get("schema") or ""),
            str(entry.get("table") or ""),
            str(entry.get("constraint_name") or ""),
        ),
    )


def _render_index_columns(columns: list[str]) -> str:
    return ", ".join(quote_ident(column) for column in columns)


def _render_forward_sql(wave_name: str, entries: list[dict[str, Any]]) -> str:
    generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"-- {wave_name} FK index hardening forward apply",
        f"-- generated_at: {generated_at}",
        "-- apply with direct Postgres connectivity only",
        "",
        "-- Operator contract: set PGAPPNAME=fk-index-<wave>-apply before invoking psql.",
        "-- This guard refuses to apply if the session is not running with that exact",
        "-- application_name, which would indicate either an operator misconfiguration",
        "-- or a pooler rewriting the connection.",
        "DO $pre$",
        "DECLARE",
        "  app_name text;",
        "BEGIN",
        "  SELECT current_setting('application_name', true) INTO app_name;",
        "  IF app_name IS NULL OR app_name NOT LIKE 'fk-index-%-apply' THEN",
        (
            "    RAISE EXCEPTION 'Refusing apply: application_name is %, "
            "expected fk-index-<wave>-apply. Set PGAPPNAME before running psql.',"
        ),
        "      COALESCE(app_name, '<null>');",
        "  END IF;",
        "END",
        "$pre$;",
        "",
    ]
    for entry in entries:
        lines.extend(
            [
                (f"-- {entry['schema']}.{entry['table']} {entry['constraint_name']} -> {entry['proposed_index_name']}"),
                "SET lock_timeout = '5s';",
                f"SET statement_timeout = '{entry['statement_timeout_tier']}';",
                (
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                    f"{quote_ident(entry['proposed_index_name'])} "
                    f"ON {quote_ident(entry['schema'])}.{quote_ident(entry['table'])} "
                    f"USING btree ({_render_index_columns(entry['proposed_index_columns'])})"
                    + (
                        f" WHERE {entry['proposed_partial_predicate']}"
                        if entry.get("proposed_partial_predicate")
                        else ""
                    )
                    + ";"
                ),
            ]
        )
        if (entry.get("estimated_row_count") or 0) > 1_000_000:
            lines.append(f"ANALYZE {quote_ident(entry['schema'])}.{quote_ident(entry['table'])};")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_rollback_sql(wave_name: str, entries: list[dict[str, Any]]) -> str:
    generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"-- {wave_name} FK index hardening rollback",
        f"-- generated_at: {generated_at}",
        "-- apply with direct Postgres connectivity only",
        "",
    ]
    for entry in entries:
        lines.extend(
            [
                (f"-- rollback {entry['schema']}.{entry['table']} {entry['proposed_index_name']}"),
                "SET lock_timeout = '5s';",
                "SET statement_timeout = '3h';",
                (
                    f"DROP INDEX CONCURRENTLY IF EXISTS "
                    f"{quote_ident(entry['schema'])}.{quote_ident(entry['proposed_index_name'])};"
                ),
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _render_status_md(
    *,
    wave_name: str,
    inventory: dict[str, Any],
    rollout_ready: list[dict[str, Any]],
    deferred: list[dict[str, Any]],
) -> str:
    metadata = inventory.get("metadata") or {}
    generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    ready_count = len(rollout_ready)
    deferred_count = len(deferred)
    ready_by_schema: dict[str, int] = {}
    deferred_by_schema: dict[str, int] = {}
    for entry in rollout_ready:
        ready_by_schema[entry["schema"]] = ready_by_schema.get(entry["schema"], 0) + 1
    for entry in deferred:
        deferred_by_schema[entry["schema"]] = deferred_by_schema.get(entry["schema"], 0) + 1

    representative_query_path = f"../../{QUERY_CHECK_ARTIFACTS[wave_name]}"
    inventory_mode = metadata.get("connection_mode")
    inventory_host = metadata.get("resolved_db_host")

    lines = [
        f"# {wave_name.title()} FK Index Hardening Status",
        "",
        f"- generated_at: `{generated_at}`",
        f"- inventory_source: [`{wave_name}-inventory.yml`](./{wave_name}-inventory.yml)",
        f"- representative_query_checks: [`{QUERY_CHECK_ARTIFACTS[wave_name]}`]({representative_query_path})",
        f"- connection_mode_used_for_inventory: `{inventory_mode}`",
        f"- resolved_inventory_host: `{inventory_host}`",
        "",
        "## Pre-flight Checks",
        "",
        f"- Inventory regenerated from the live database on `{generated_at}`.",
        (
            f"- Inventory ran on `{inventory_mode}` because direct-host connectivity "
            "is currently blocked from this workstation."
        ),
        (
            "- Direct apply / observer lane remains blocked until "
            "`db.<project>.supabase.co:5432` is reachable from this machine."
        ),
        (
            "- Query-check gate applied before generating forward SQL; candidates "
            "without a committed representative query artifact are deferred."
        ),
        "",
        "## Candidate Summary",
        "",
        f"- Rollout-ready indexes: `{ready_count}`",
        f"- Deferred for missing query check: `{deferred_count}`",
        f"- Rollout-ready by schema: `{ready_by_schema}`",
        f"- Deferred by schema: `{deferred_by_schema}`",
        "",
        "## Pre-Flight Disk Targets",
        "",
    ]
    # Dedupe by (schema, table) — multiple FK indexes on the same target table
    # should only count once for operator disk-headroom planning.
    unique_by_table: dict[tuple[str, str], int] = {}
    for entry in rollout_ready:
        key = (str(entry.get("schema") or ""), str(entry.get("table") or ""))
        count = int(entry.get("estimated_row_count") or 0)
        if count > unique_by_table.get(key, 0):
            unique_by_table[key] = count

    disk_targets_sorted = sorted(
        ((schema, table, count) for (schema, table), count in unique_by_table.items() if count > 0),
        key=lambda item: (-item[2], item[0], item[1]),
    )[:5]
    if disk_targets_sorted:
        for schema, table, count in disk_targets_sorted:
            lines.append(f"- `{schema}.{table}` — estimated_row_count: {count}")
    else:
        lines.append("- (no large tables in this wave)")
    lines.extend(
        [
            "",
            "## Rollout Files",
            "",
            f"- Forward SQL: [`{wave_name}-forward.sql`](./{wave_name}-forward.sql)",
            f"- Rollback SQL: [`{wave_name}-rollback.sql`](./{wave_name}-rollback.sql)",
            "",
            "## Baseline Snapshot",
            "",
            "- Pending. Capture with `scripts/db/run_fk_index_observer.py baseline` once direct connectivity is fixed.",
            "",
            "## Per-Candidate Apply Outcome",
            "",
            "- Pending direct-lane rollout.",
            "",
            "## Invalid-Index Cleanup",
            "",
            "- Pending direct-lane rollout.",
            "",
            "## Aborts and Rollbacks",
            "",
            "- None yet.",
            "",
            "## Schema-Doc Diffs",
            "",
            (
                "- Pending. Run `make schema-docs-check` after direct apply on the "
                "validation target and commit any resulting `supabase/schema_docs/*` drift."
            ),
            "",
            "## Soak Results",
            "",
            "- Pending. Respect `24h` soak between Wave 1 apply completion and Wave 2 apply start.",
            "",
            "## Next Checkpoint",
            "",
            "- Do not start apply until direct-connectivity is repaired and baseline snapshots are captured.",
        ]
    )
    if deferred:
        lines.extend(
            [
                "",
                "## Deferred Candidates",
                "",
            ]
        )
        for entry in deferred:
            lines.append(
                f"- `{entry['schema']}.{entry['table']}` "
                f"`{entry['proposed_index_name']}` -> `defer-missing-query-check`"
            )
    return "\n".join(lines).rstrip() + "\n"


def build_wave_artifacts(wave_name: str) -> None:
    inventory_path = DOCS_ROOT / f"{wave_name}-inventory.yml"
    inventory = load_inventory(inventory_path)
    entries = _sort_entries(list(inventory.get("entries") or []))
    supported_schemas = SUPPORTED_QUERY_CHECK_SCHEMAS[wave_name]

    rollout_ready: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    for entry in entries:
        original_decision = entry.get("base_decision") or entry.get("decision")
        if original_decision != "add":
            continue
        entry["base_decision"] = "add"
        if entry["schema"] not in supported_schemas:
            entry["decision"] = "defer-missing-query-check"
            deferred.append(entry)
        else:
            entry["decision"] = "add"
            rollout_ready.append(entry)

    inventory.setdefault("metadata", {})
    inventory["metadata"]["query_check_supported_schemas"] = sorted(supported_schemas)
    inventory["metadata"]["representative_query_artifact"] = QUERY_CHECK_ARTIFACTS[wave_name]
    inventory["metadata"]["rollout_ready_count"] = len(rollout_ready)
    inventory["metadata"]["deferred_missing_query_check_count"] = len(deferred)
    inventory_path.write_text(yaml.safe_dump(inventory, sort_keys=False), encoding="utf-8")

    (DOCS_ROOT / f"{wave_name}-forward.sql").write_text(
        _render_forward_sql(wave_name, rollout_ready),
        encoding="utf-8",
    )
    (DOCS_ROOT / f"{wave_name}-rollback.sql").write_text(
        _render_rollback_sql(wave_name, rollout_ready),
        encoding="utf-8",
    )
    (DOCS_ROOT / f"{wave_name}-status.md").write_text(
        _render_status_md(
            wave_name=wave_name,
            inventory=inventory,
            rollout_ready=rollout_ready,
            deferred=deferred,
        ),
        encoding="utf-8",
    )
    evidence_dir = DOCS_ROOT / "evidence" / wave_name
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / ".gitkeep").write_text("", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build wave FK index rollout artifacts from frozen inventory.")
    parser.add_argument("--wave", choices=sorted(SUPPORTED_QUERY_CHECK_SCHEMAS), required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    build_wave_artifacts(args.wave)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
