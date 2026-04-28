#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

try:
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.utils.env import load_env


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parent
DEFAULT_OUTPUT_DIR = WORKSPACE_ROOT / "docs/workspace"
DB_URL_SOURCES = ("TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL")
SECRET_RE = re.compile(
    r"(postgres(?:ql)?://[^\s'\"<>]+|service_role[^\s'\"<>]*|eyJ[a-zA-Z0-9_.=-]+)",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class ResolvedDbUrl:
    value: str
    source: str


@dataclass(frozen=True)
class QuerySpec:
    label: str
    route: str
    sql: str
    parameters: dict[str, str]


QUERY_SPECS: tuple[QuerySpec, ...] = (
    QuerySpec(
        label="profile_dashboard/shared_account_source",
        route="/api/v1/admin/socials/profiles/:platform/:handle/dashboard",
        parameters={"platform": "instagram", "handle": "thetraitorsus", "safe_limit": "25"},
        sql="""
            select
              id::text,
              platform,
              source_scope,
              account_handle,
              is_active,
              scrape_priority,
              last_scrape_status,
              last_scrape_at,
              updated_at
            from social.shared_account_sources
            where platform = 'instagram'
              and lower(account_handle) = lower('thetraitorsus')
            order by is_active desc, scrape_priority asc, updated_at desc nulls last
            limit 25
        """,
    ),
    QuerySpec(
        label="profile_dashboard/recent_catalog_jobs",
        route="/api/v1/admin/socials/profiles/:platform/:handle/dashboard",
        parameters={"platform": "instagram", "handle": "thetraitorsus", "safe_limit": "25", "safe_offset": "0"},
        sql="""
            select
              j.id::text as job_id,
              j.run_id::text as run_id,
              j.platform,
              j.job_type,
              j.status,
              j.items_found,
              j.created_at,
              r.status as run_status,
              r.created_at as run_created_at
            from social.scrape_jobs as j
            left join social.scrape_runs as r
              on r.id = j.run_id
            where j.platform = 'instagram'
              and (
                lower(
                  coalesce(
                    j.config->>'account_handle',
                    j.config->>'handle',
                    j.config->>'username',
                    j.metadata->>'account_handle',
                    ''
                  )
                ) =
                  lower('thetraitorsus')
                or lower(coalesce(j.config->>'source_account', '')) = lower('thetraitorsus')
              )
            order by j.created_at desc nulls last
            limit 25 offset 0
        """,
    ),
    QuerySpec(
        label="shared_ingest/recent_runs",
        route="/api/v1/admin/socials/runs and /shared/runs",
        parameters={
            "source_scope": "bravo",
            "season_id": "00000000-0000-0000-0000-000000000000",
            "safe_limit": "25",
            "safe_offset": "0",
        },
        sql="""
            select
              r.id::text,
              r.season_id::text,
              r.show_id::text,
              r.source_scope,
              r.status,
              r.config->>'pipeline_ingest_mode' as pipeline_ingest_mode,
              r.created_at,
              r.started_at,
              r.completed_at
            from social.scrape_runs as r
            where r.source_scope = 'bravo'
              and (
                r.season_id = '00000000-0000-0000-0000-000000000000'::uuid
                or coalesce(r.config->>'pipeline_ingest_mode', '') in (
                  'shared_account_async',
                  'shared_account_catalog_backfill'
                )
              )
            order by r.created_at desc
            limit 25 offset 0
        """,
    ),
    QuerySpec(
        label="shared_review_queue/open_items",
        route="/api/v1/admin/socials/shared/review-queue",
        parameters={"source_scope": "bravo", "review_status": "open", "safe_limit": "25", "safe_offset": "0"},
        sql="""
            select
              q.id::text,
              q.platform,
              q.source_scope,
              q.source_id,
              q.source_account,
              q.review_status,
              q.review_reason,
              q.resolved_show_id::text,
              q.resolved_season_id::text,
              q.created_at,
              q.updated_at
            from social.shared_post_review_queue as q
            where q.source_scope = 'bravo'
              and q.review_status = 'open'
            order by q.created_at desc
            limit 25 offset 0
        """,
    ),
    QuerySpec(
        label="social_landing/socialblade_rows",
        route="/api/v1/admin/socials/landing-socialblade-rows",
        parameters={"platforms": "instagram,youtube,facebook", "handle": "thetraitorsus", "safe_limit": "25"},
        sql="""
            select
              id::text as id,
              person_id::text as person_id,
              platform,
              account_handle,
              scraped_at,
              updated_at,
              created_at,
              stats_refreshed,
              raw_response->>'socialblade_url' as socialblade_url
            from pipeline.socialblade_growth_data
            where platform = any(array['instagram', 'youtube', 'facebook']::text[])
              and (
                person_id = '00000000-0000-0000-0000-000000000000'::uuid
                or account_handle = 'thetraitorsus'
              )
            order by
              platform asc,
              account_handle asc,
              person_id asc nulls last,
              updated_at desc nulls last,
              scraped_at desc nulls last,
              created_at desc nulls last,
              id asc
            limit 25 offset 0
        """,
    ),
    QuerySpec(
        label="season_analytics/season_targets",
        route="/api/v1/admin/socials/shows/:show_id/seasons/:season_number/social/analytics",
        parameters={"season_id": "00000000-0000-0000-0000-000000000000", "source_scope": "bravo"},
        sql="""
            select
              season_id::text,
              show_id::text,
              platform,
              source_scope,
              timezone,
              is_active,
              updated_at
            from social.season_targets
            where season_id = '00000000-0000-0000-0000-000000000000'::uuid
              and source_scope = 'bravo'
            order by platform asc
        """,
    ),
    QuerySpec(
        label="week_live_health/instagram_week_bucket",
        route="/api/v1/admin/socials/shows/:show_id/seasons/:season_number/social/analytics/week/:week_index/live-health",
        parameters={
            "season_id": "00000000-0000-0000-0000-000000000000",
            "week_start": "2026-01-01T00:00:00+00:00",
            "week_end": "2026-01-08T00:00:00+00:00",
            "handle": "thetraitorsus",
        },
        sql="""
            select
              date_trunc('day', p.posted_at) as day_utc,
              ltrim(lower(coalesce(p.source_account, p.username)), '@') as account_handle,
              count(*)::int as posts,
              coalesce(sum(p.comments_count), 0)::bigint as comments,
              coalesce(sum(p.likes), 0)::bigint as likes
            from social.instagram_posts as p
            where p.season_id = '00000000-0000-0000-0000-000000000000'::uuid
              and p.posted_at >= '2026-01-01T00:00:00+00:00'::timestamptz
              and p.posted_at < '2026-01-08T00:00:00+00:00'::timestamptz
              and ltrim(lower(coalesce(p.source_account, p.username)), '@') = ltrim(lower('thetraitorsus'), '@')
            group by day_utc, account_handle
            order by day_utc asc, account_handle asc
        """,
    ),
)


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def resolve_db_url() -> ResolvedDbUrl:
    for source in DB_URL_SOURCES:
        value = (os.getenv(source) or "").strip()
        if value:
            return ResolvedDbUrl(value=value, source=source)
    raise RuntimeError("No database URL configured. Set TRR_DB_SESSION_URL, TRR_DB_URL, or TRR_DB_FALLBACK_URL.")


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if value is None or isinstance(value, str | int | float | bool):
        return _redact_text(value) if isinstance(value, str) else value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return _redact_text(str(value))


def _redact_text(value: str) -> str:
    return SECRET_RE.sub("[redacted]", value)


def _filter_specs(labels: str | None) -> tuple[QuerySpec, ...]:
    if not labels:
        return QUERY_SPECS
    requested = {label.strip() for label in labels.split(",") if label.strip()}
    known = {spec.label for spec in QUERY_SPECS}
    unknown = sorted(requested - known)
    if unknown:
        raise SystemExit(f"Unknown label(s): {', '.join(unknown)}")
    return tuple(spec for spec in QUERY_SPECS if spec.label in requested)


def _verify_extension(cur: psycopg2.extensions.cursor) -> dict[str, str]:
    cur.execute(
        """
        select e.extname, n.nspname, e.extversion
        from pg_extension e
        join pg_namespace n on n.oid = e.extnamespace
        where e.extname = 'index_advisor'
        """
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("index_advisor extension is not installed; no advisor report was written.")
    if row["nspname"] != "extensions":
        raise RuntimeError(
            f"index_advisor is installed in schema {row['nspname']!r}; expected 'extensions'. No report was written."
        )
    return {
        "name": str(row["extname"]),
        "schema": str(row["nspname"]),
        "version": str(row["extversion"]),
    }


def _run_query_advisor(cur: psycopg2.extensions.cursor, spec: QuerySpec) -> dict[str, Any]:
    try:
        cur.execute("select * from extensions.index_advisor(%s)", [_normalize_sql(spec.sql)])
        rows = [_json_safe(dict(row)) for row in cur.fetchall()]
        return {
            "label": spec.label,
            "route": spec.route,
            "parameters": spec.parameters,
            "status": "ok",
            "recommendations": rows,
            "errors": [],
            "review_required": True,
        }
    except Exception as exc:  # noqa: BLE001 - report per-query advisor failures as evidence
        cur.connection.rollback()
        cur.execute("begin read only")
        cur.execute("set local statement_timeout = '8s'")
        cur.execute("set local lock_timeout = '1s'")
        return {
            "label": spec.label,
            "route": spec.route,
            "parameters": spec.parameters,
            "status": "advisor_error",
            "recommendations": [],
            "errors": [_redact_text(str(exc))],
            "review_required": True,
        }


def build_report(specs: tuple[QuerySpec, ...], *, output_date: str, resolved: ResolvedDbUrl) -> dict[str, Any]:
    with psycopg2.connect(
        resolved.value,
        connect_timeout=10,
        application_name="trr-index-advisor-social-hot-paths",
        cursor_factory=psycopg2.extras.RealDictCursor,
    ) as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("set local statement_timeout = '8s'")
            cur.execute("set local lock_timeout = '1s'")
            extension = _verify_extension(cur)
            queries = [_run_query_advisor(cur, spec) for spec in specs]
            return {
                "metadata": {
                    "generated_at": datetime.now(UTC).isoformat(),
                    "output_date": output_date,
                    "database": {"source": resolved.source, "value": "redacted"},
                    "extension_schema": extension["schema"],
                    "extension_version": extension["version"],
                    "read_only": True,
                },
                "queries": queries,
            }


def write_reports(report: dict[str, Any], output_dir: Path, output_date: str) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"index-advisor-social-hot-paths-{output_date}.json"
    md_path = output_dir / f"index-advisor-social-hot-paths-{output_date}.md"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        f"# Index Advisor Social Hot Paths - {output_date}",
        "",
        "This report is evidence-only. Do not execute returned DDL without a separate owner-approved migration review.",
        "",
        "## Metadata",
        "",
        f"- Generated at: `{report['metadata']['generated_at']}`",
        f"- DB URL source: `{report['metadata']['database']['source']}`",
        f"- Extension schema: `{report['metadata']['extension_schema']}`",
        f"- Extension version: `{report['metadata']['extension_version']}`",
        f"- Read-only: `{report['metadata']['read_only']}`",
        "",
        "## Query Results",
        "",
        "| Label | Status | Recommendations | Errors |",
        "| --- | --- | ---: | ---: |",
    ]
    for query in report["queries"]:
        lines.append(
            "| {label} | {status} | {recommendations} | {errors} |".format(
                label=query["label"],
                status=query["status"],
                recommendations=len(query["recommendations"]),
                errors=len(query["errors"]),
            )
        )
    lines.extend(["", "## Details", ""])
    for query in report["queries"]:
        lines.extend(
            [
                f"### {query['label']}",
                "",
                f"- Route: `{query['route']}`",
                f"- Status: `{query['status']}`",
                f"- Review required: `{query['review_required']}`",
                f"- Parameters: `{json.dumps(query['parameters'], sort_keys=True)}`",
                "",
            ]
        )
        if query["errors"]:
            lines.extend(["Errors:", ""])
            for error in query["errors"]:
                lines.append(f"- `{error}`")
            lines.append("")
        if query["recommendations"]:
            lines.extend(["Recommendations:", ""])
            lines.append("```json")
            lines.append(json.dumps(query["recommendations"], indent=2, sort_keys=True))
            lines.append("```")
            lines.append("")
        else:
            lines.extend(["No advisor recommendations were returned for this query.", ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def _print_dry_run(specs: tuple[QuerySpec, ...]) -> None:
    for spec in specs:
        print(f"{spec.label}\t{spec.route}\t{json.dumps(spec.parameters, sort_keys=True)}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run index_advisor against TRR social hot-path query strings.")
    parser.add_argument("--dry-run", action="store_true", help="Print query labels without connecting to the database.")
    parser.add_argument("--output-date", default=date.today().isoformat(), help="Report date in YYYY-MM-DD format.")
    parser.add_argument("--labels", help="Comma-separated query labels to run.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for JSON/Markdown reports.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    specs = _filter_specs(args.labels)
    if args.dry_run:
        _print_dry_run(specs)
        return 0

    load_env(override=False)
    resolved = resolve_db_url()
    report = build_report(specs, output_date=args.output_date, resolved=resolved)
    json_path, md_path = write_reports(report, args.output_dir, args.output_date)
    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
