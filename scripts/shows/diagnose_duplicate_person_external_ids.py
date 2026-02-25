#!/usr/bin/env python3
"""Diagnose duplicate people identities with conflicting external IDs in cast scope.

This script surfaces same-name person rows where:
- there are duplicate `core.people` records for a normalized name,
- at least one row is linked to cast in the selected show scope,
- and external IDs conflict across duplicate rows.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:
    REPO_ROOT = Path(__file__).resolve().parents[2]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from trr_backend.db import pg
    from trr_backend.utils.env import load_env


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="diagnose_duplicate_person_external_ids",
        description="Find duplicate people rows with conflicting external IDs in show cast scope.",
    )
    parser.add_argument(
        "--show-id",
        action="append",
        default=[],
        help="Optional show UUID(s). Defaults to Bravo-network shows when omitted.",
    )
    parser.add_argument(
        "--json-summary",
        default="",
        help="Optional JSON output path ('-' prints to stdout).",
    )
    return parser.parse_args(argv)


def _normalize_id(value: Any) -> str | None:
    candidate = str(value or "").strip()
    return candidate or None


def _list_scope_show_ids(explicit_show_ids: list[str]) -> list[str]:
    normalized = [str(show_id).strip() for show_id in explicit_show_ids if str(show_id).strip()]
    if normalized:
        return sorted(set(normalized))
    rows = pg.fetch_all(
        """
        SELECT DISTINCT s.id::text AS show_id
        FROM core.shows s
        WHERE EXISTS (
          SELECT 1
          FROM unnest(COALESCE(s.networks, '{}')) AS network_name
          WHERE lower(network_name) = 'bravo'
        )
        ORDER BY show_id
        """
    )
    return [str(row.get("show_id") or "").strip() for row in rows if row.get("show_id")]


def _fetch_duplicate_people_rows(scope_show_ids: list[str]) -> list[dict[str, Any]]:
    if not scope_show_ids:
        return []
    return pg.fetch_all(
        """
        WITH duplicate_names AS (
          SELECT lower(trim(full_name)) AS name_key
          FROM core.people
          WHERE NULLIF(trim(full_name), '') IS NOT NULL
          GROUP BY lower(trim(full_name))
          HAVING COUNT(*) > 1
        ),
        cast_scope AS (
          SELECT
            sc.person_id,
            COUNT(DISTINCT sc.show_id)::int AS cast_show_count,
            ARRAY_AGG(DISTINCT sc.show_id::text ORDER BY sc.show_id::text) AS cast_show_ids
          FROM core.show_cast sc
          WHERE sc.show_id = ANY(%s::uuid[])
          GROUP BY sc.person_id
        )
        SELECT
          p.id::text AS person_id,
          p.full_name,
          lower(trim(p.full_name)) AS name_key,
          COALESCE(NULLIF(trim(p.external_ids->>'imdb'), ''), NULLIF(trim(p.external_ids->>'imdb_id'), '')) AS imdb_id,
          COALESCE(NULLIF(trim(p.external_ids->>'tmdb'), ''), NULLIF(trim(p.external_ids->>'tmdb_id'), '')) AS tmdb_id,
          COALESCE(
            NULLIF(trim(p.external_ids->>'wikidata'), ''),
            NULLIF(trim(p.external_ids->>'wikidata_id'), '')
          ) AS wikidata_id,
          COALESCE(cs.cast_show_count, 0)::int AS cast_show_count,
          COALESCE(cs.cast_show_ids, ARRAY[]::text[]) AS cast_show_ids
        FROM core.people p
        JOIN duplicate_names dn ON dn.name_key = lower(trim(p.full_name))
        LEFT JOIN cast_scope cs ON cs.person_id = p.id
        ORDER BY lower(trim(p.full_name)), p.id
        """,
        [scope_show_ids],
    )


def _group_duplicate_conflicts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        name_key = str(row.get("name_key") or "").strip()
        if not name_key:
            continue
        grouped.setdefault(name_key, []).append(row)

    findings: list[dict[str, Any]] = []
    for name_key, name_rows in grouped.items():
        has_cast_linked = any(int(row.get("cast_show_count") or 0) > 0 for row in name_rows)
        if not has_cast_linked:
            continue

        conflict_values: dict[str, list[str]] = {}
        has_conflict = False
        for id_key in ("imdb_id", "tmdb_id", "wikidata_id"):
            values = sorted({_normalize_id(row.get(id_key)) for row in name_rows if _normalize_id(row.get(id_key))})
            if values:
                conflict_values[id_key] = values
            if len(values) > 1:
                has_conflict = True

        if not has_conflict:
            continue

        canonical_row = max(
            name_rows,
            key=lambda row: (
                int(row.get("cast_show_count") or 0),
                sum(1 for key in ("imdb_id", "tmdb_id", "wikidata_id") if _normalize_id(row.get(key))),
            ),
        )

        findings.append(
            {
                "name": str(canonical_row.get("full_name") or "").strip() or name_key,
                "name_key": name_key,
                "conflicts": conflict_values,
                "recommended_canonical_person_id": str(canonical_row.get("person_id") or "").strip(),
                "people": [
                    {
                        "person_id": str(row.get("person_id") or "").strip(),
                        "full_name": str(row.get("full_name") or "").strip(),
                        "cast_show_count": int(row.get("cast_show_count") or 0),
                        "cast_show_ids": [
                            str(value).strip()
                            for value in (row.get("cast_show_ids") or [])
                            if str(value).strip()
                        ],
                        "imdb_id": _normalize_id(row.get("imdb_id")),
                        "tmdb_id": _normalize_id(row.get("tmdb_id")),
                        "wikidata_id": _normalize_id(row.get("wikidata_id")),
                    }
                    for row in name_rows
                ],
            }
        )

    findings.sort(key=lambda row: (str(row.get("name") or ""), str(row.get("recommended_canonical_person_id") or "")))
    return findings


def _write_json(path: str, payload: Any) -> None:
    summary_json = json.dumps(payload, indent=2, sort_keys=True)
    if path.strip() == "-":
        print(summary_json)
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(summary_json)
    print(f"json_written={path}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()

    scope_show_ids = _list_scope_show_ids(args.show_id)
    rows = _fetch_duplicate_people_rows(scope_show_ids)
    findings = _group_duplicate_conflicts(rows)

    print(f"scope_shows={len(scope_show_ids)}")
    print(f"duplicate_rows_scanned={len(rows)}")
    print(f"conflicting_duplicates={len(findings)}")
    for finding in findings[:25]:
        print(
            f"duplicate_conflict name={finding['name']} "
            f"canonical={finding['recommended_canonical_person_id']} conflicts={finding['conflicts']}"
        )
    if len(findings) > 25:
        print(f"duplicate_conflicts_truncated={len(findings) - 25}")

    payload = {
        "scope_show_ids": scope_show_ids,
        "duplicate_rows_scanned": len(rows),
        "conflicting_duplicates": findings,
        "conflicting_duplicates_count": len(findings),
    }

    if args.json_summary:
        _write_json(args.json_summary, payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
