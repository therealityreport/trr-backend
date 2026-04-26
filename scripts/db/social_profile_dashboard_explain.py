#!/usr/bin/env python3
"""Capture query plans for social profile dashboard read paths.

Index migrations are intentionally not part of this phase. This helper prepares
the next query-plan-backed indexing phase by rendering and, when not in dry-run
mode, running EXPLAIN ANALYZE for the dashboard summary, detail, hashtag,
comments, and catalog-progress reads that make the page expensive.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NamedTuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts._db_url import resolve_direct_db_url  # noqa: E402

POST_TABLES = {
    "instagram": "instagram_posts",
    "tiktok": "tiktok_posts",
    "youtube": "youtube_videos",
    "twitter": "twitter_tweets",
    "facebook": "facebook_posts",
    "threads": "meta_threads_posts",
}
CATALOG_TABLES = {
    "instagram": "instagram_account_catalog_posts",
    "tiktok": "tiktok_account_catalog_posts",
    "youtube": "youtube_account_catalog_posts",
    "twitter": "twitter_account_catalog_posts",
    "facebook": "facebook_account_catalog_posts",
    "threads": "threads_account_catalog_posts",
}
COMMENT_TABLES = {
    "instagram": ("instagram_comments", "post_id"),
    "tiktok": ("tiktok_comments", "post_id"),
    "youtube": ("youtube_comments", "video_id"),
    "facebook": ("facebook_comments", "post_id"),
    "threads": ("meta_threads_comments", "post_id"),
}
SOURCE_ID_COLUMNS = {
    "instagram": "shortcode",
    "tiktok": "video_id",
    "youtube": "video_id",
    "twitter": "tweet_id",
    "facebook": "post_id",
    "threads": "post_id",
}
POSTED_AT_COLUMNS = {
    "instagram": "posted_at",
    "tiktok": "posted_at",
    "youtube": "published_at",
    "twitter": "created_at",
    "facebook": "posted_at",
    "threads": "posted_at",
}
INGEST_MODE = "shared_account_catalog_backfill"
OUTPUT_ROOT = REPO_ROOT / "tmp" / "social-profile-dashboard-explain"
SEQUENTIAL_SCAN_ROW_THRESHOLD = 10_000


class QuerySpec(NamedTuple):
    name: str
    sql: str
    params: list[Any]
    tables: tuple[str, ...]


def _normalize_platform(value: str) -> str:
    platform = value.strip().lower()
    if platform not in POST_TABLES:
        raise ValueError(f"Unsupported platform {value!r}; expected one of {sorted(POST_TABLES)}.")
    return platform


def _normalize_handle(value: str) -> str:
    handle = value.strip().lstrip("@").lower()
    if not re.fullmatch(r"[a-z0-9._-]{1,64}", handle):
        raise ValueError("Handle must be a normalized social account handle.")
    return handle


def _owner_match_sql(platform: str, alias: str = "p") -> str:
    if platform == "twitter":
        return (
            "nullif(ltrim(lower(coalesce("
            f"nullif({alias}.username, ''), nullif({alias}.source_account, '')"
            ")), '@'), '') = %s"
        )
    return f"lower({alias}.source_account) = %s"


def _summary_metric_exprs(platform: str, alias: str = "p") -> tuple[str, str]:
    if platform == "instagram":
        return f"(coalesce({alias}.likes, 0) + coalesce({alias}.comments_count, 0))", f"coalesce({alias}.views, 0)"
    if platform == "tiktok":
        return (
            f"(coalesce({alias}.likes, 0) + coalesce({alias}.comments_count, 0) + coalesce({alias}.shares, 0))",
            f"coalesce({alias}.views, 0)",
        )
    if platform == "twitter":
        return (
            f"(coalesce({alias}.likes, 0) + coalesce({alias}.retweets, 0) + "
            f"coalesce({alias}.replies_count, 0) + coalesce({alias}.quotes, 0))",
            f"coalesce({alias}.views, 0)",
        )
    if platform == "youtube":
        return f"(coalesce({alias}.likes, 0) + coalesce({alias}.comments_count, 0))", f"coalesce({alias}.views, 0)"
    if platform == "facebook":
        return (
            f"(coalesce({alias}.likes, 0) + coalesce({alias}.comments_count, 0) + coalesce({alias}.shares, 0))",
            "0",
        )
    return f"(coalesce({alias}.likes, 0) + coalesce({alias}.comments_count, 0))", "0"


def _build_query_specs(*, platform: str, handle: str, run_id: str | None) -> list[QuerySpec]:
    post_table = POST_TABLES[platform]
    catalog_table = CATALOG_TABLES[platform]
    source_id_column = SOURCE_ID_COLUMNS[platform]
    posted_at_column = POSTED_AT_COLUMNS[platform]
    owner_match = _owner_match_sql(platform)
    engagement_expr, views_expr = _summary_metric_exprs(platform)
    specs = [
        QuerySpec(
            name="profile_source_rows",
            sql="""
                select id::text, source_scope, platform, account_handle, is_active, scrape_priority, updated_at
                from social.shared_account_sources
                where platform = %s
                  and account_handle = %s
                order by is_active desc, scrape_priority asc, updated_at desc
                limit 25
            """,
            params=[platform, handle],
            tables=("social.shared_account_sources",),
        ),
        QuerySpec(
            name="summary_totals",
            sql=f"""
                select
                  count(*)::int as total_posts,
                  coalesce(sum({engagement_expr}), 0)::bigint as total_engagement,
                  coalesce(sum({views_expr}), 0)::bigint as total_views,
                  min(p.{posted_at_column}) as first_post_at,
                  max(p.{posted_at_column}) as last_post_at
                from social.{post_table} p
                where {owner_match}
            """,
            params=[handle],
            tables=(f"social.{post_table}",),
        ),
        QuerySpec(
            name="posts_page",
            sql=f"""
                select
                  p.id::text,
                  p.{source_id_column}::text as source_id,
                  p.{posted_at_column} as posted_at,
                  p.show_id::text,
                  p.season_id::text
                from social.{post_table} p
                where {owner_match}
                order by p.{posted_at_column} desc nulls last, p.id desc
                limit 50
            """,
            params=[handle],
            tables=(f"social.{post_table}",),
        ),
        QuerySpec(
            name="hashtag_summary",
            sql=f"""
                with hashtag_rows as (
                  select
                    lower(trim(both '#' from trim(tag.value))) as hashtag,
                    p.{source_id_column}::text as source_id,
                    p.{posted_at_column} as posted_at
                  from social.{post_table} p
                  cross join lateral jsonb_array_elements_text(
                    coalesce(to_jsonb(p) -> 'hashtags', '[]'::jsonb)
                  ) as tag(value)
                  where {owner_match}
                    and nullif(trim(tag.value), '') is not null
                )
                select hashtag, count(*)::int as usage_count, max(posted_at) as latest_seen_at
                from hashtag_rows
                group by hashtag
                order by count(*) desc, hashtag asc
                limit 100
            """,
            params=[handle],
            tables=(f"social.{post_table}",),
        ),
        QuerySpec(
            name="catalog_summary",
            sql=f"""
                select
                  count(*)::int as catalog_total_posts,
                  count(*) filter (where assignment_status = 'assigned')::int as assigned_posts,
                  count(*) filter (where assignment_status = 'needs_review')::int as pending_review_posts,
                  count(*) filter (where assignment_status = 'unassigned')::int as unassigned_posts,
                  min(posted_at) as catalog_first_post_at,
                  max(posted_at) as catalog_last_post_at
                from social.{catalog_table} p
                where lower(p.source_account) = %s
            """,
            params=[handle],
            tables=(f"social.{catalog_table}",),
        ),
        QuerySpec(
            name="recent_catalog_runs",
            sql="""
                select r.id::text as run_id, r.status, r.created_at, r.started_at, r.completed_at
                from social.scrape_runs r
                where coalesce(r.config->>'pipeline_ingest_mode', '') = %s
                  and (
                    exists (
                      select 1
                      from social.scrape_jobs j
                      where j.run_id = r.id
                        and j.platform = %s
                        and lower(
                          coalesce(nullif(j.config->>'account', ''), nullif(j.metadata->>'account', ''), '')
                        ) = %s
                    )
                    or (
                      lower(coalesce(r.config->>'platform', r.config->'platforms'->>0, '')) = %s
                      and ltrim(
                        lower(
                          coalesce(
                            r.config->>'account_handle',
                            r.config->>'account',
                            r.config->'accounts_override'->>0,
                            ''
                          )
                        ),
                        '@'
                      ) = %s
                    )
                  )
                order by r.created_at desc
                limit 3
            """,
            params=[INGEST_MODE, platform, handle, platform, handle],
            tables=("social.scrape_runs", "social.scrape_jobs"),
        ),
    ]
    if platform == "twitter":
        specs.append(
            QuerySpec(
                name="comments_coverage",
                sql=f"""
                    with scoped_posts as (
                      select p.{source_id_column}::text as source_id
                      from social.{post_table} p
                      where {owner_match}
                      order by p.{posted_at_column} desc nulls last, p.id desc
                      limit 250
                    )
                    select count(*)::int as stored_comments
                    from social.twitter_tweets t
                    join scoped_posts p
                      on t.reply_to_tweet_id = p.source_id
                      or t.quoted_tweet_id = p.source_id
                    where t.is_reply = true or t.is_quote = true
                """,
                params=[handle],
                tables=(f"social.{post_table}",),
            )
        )
    elif platform in COMMENT_TABLES:
        comment_table, fk_column = COMMENT_TABLES[platform]
        specs.append(
            QuerySpec(
                name="comments_coverage",
                sql=f"""
                    with scoped_posts as (
                      select p.id::text as id
                      from social.{post_table} p
                      where {owner_match}
                      order by p.{posted_at_column} desc nulls last, p.id desc
                      limit 250
                    )
                    select count(*)::int as stored_comments
                    from social.{comment_table} c
                    join scoped_posts p on p.id = c.{fk_column}::text
                """,
                params=[handle],
                tables=(f"social.{post_table}", f"social.{comment_table}"),
            )
        )
    if run_id:
        specs.extend(
            [
                QuerySpec(
                    name="catalog_progress_run",
                    sql="""
                        select id::text as run_id,
                               status,
                               source_scope,
                               config,
                               summary,
                               created_at,
                               started_at,
                               completed_at
                        from social.scrape_runs
                        where id = %s::uuid
                          and coalesce(config->>'pipeline_ingest_mode', '') = %s
                        limit 1
                    """,
                    params=[run_id, INGEST_MODE],
                    tables=("social.scrape_runs",),
                ),
                QuerySpec(
                    name="catalog_progress_jobs",
                    sql="""
                        select id::text, platform, job_type, status, items_found, created_at, started_at, completed_at
                        from social.scrape_jobs
                        where run_id = %s::uuid
                          and platform = %s
                          and lower(coalesce(config->>'account', '')) = %s
                        order by coalesce(completed_at, started_at, created_at) desc, created_at desc
                        limit 250
                    """,
                    params=[run_id, platform, handle],
                    tables=("social.scrape_jobs",),
                ),
            ]
        )
    return specs


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _collect_plan_nodes(plan: Any) -> list[dict[str, Any]]:
    if isinstance(plan, list) and plan:
        return _collect_plan_nodes(plan[0])
    if not isinstance(plan, dict):
        return []
    root = plan.get("Plan") if "Plan" in plan and isinstance(plan.get("Plan"), dict) else plan
    nodes = [root]
    for child in root.get("Plans") or []:
        nodes.extend(_collect_plan_nodes(child))
    return nodes


def _table_row_estimates(conn: Any, table_names: set[str]) -> dict[str, int]:
    if not table_names:
        return {}
    names_by_schema: dict[str, list[str]] = {}
    for table_name in table_names:
        schema, _, relation = table_name.partition(".")
        names_by_schema.setdefault(schema, []).append(relation)
    estimates: dict[str, int] = {}
    with conn.cursor() as cur:
        for schema, relations in names_by_schema.items():
            cur.execute(
                """
                select n.nspname || '.' || c.relname as table_name, greatest(c.reltuples::bigint, 0) as estimated_rows
                from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = %s
                  and c.relname = any(%s)
                """,
                [schema, relations],
            )
            for row in cur.fetchall():
                estimates[str(row["table_name"])] = int(row["estimated_rows"] or 0)
    return estimates


def _sequential_scan_findings(
    *,
    query_name: str,
    plan: Any,
    row_estimates: dict[str, int],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for node in _collect_plan_nodes(plan):
        if node.get("Node Type") != "Seq Scan":
            continue
        schema = str(node.get("Schema") or "public")
        relation = str(node.get("Relation Name") or "").strip()
        table_name = f"{schema}.{relation}" if relation else ""
        estimated_table_rows = row_estimates.get(table_name, 0)
        total_cost = float(node.get("Total Cost") or 0)
        if estimated_table_rows < SEQUENTIAL_SCAN_ROW_THRESHOLD and total_cost < 10_000:
            continue
        findings.append(
            {
                "query_name": query_name,
                "table": table_name,
                "estimated_table_rows": estimated_table_rows,
                "plan_rows": int(node.get("Plan Rows") or 0),
                "actual_rows": int(node.get("Actual Rows") or 0),
                "total_cost": total_cost,
            }
        )
    return findings


def _render_dry_run(specs: list[QuerySpec]) -> dict[str, Any]:
    return {
        "mode": "dry-run",
        "queries": [
            {
                "name": spec.name,
                "sql": " ".join(spec.sql.split()),
                "params": spec.params,
                "tables": list(spec.tables),
            }
            for spec in specs
        ],
    }


def _run_explains(*, specs: list[QuerySpec], output_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    import psycopg2
    import psycopg2.extras

    resolved = resolve_direct_db_url(allow_database_url=True)
    inspected_tables = {table for spec in specs for table in spec.tables}
    payload: dict[str, Any] = {
        "captured_at": datetime.now(UTC).isoformat(),
        "db_source": resolved.source,
        "plans": [],
        "sequential_scan_findings": [],
    }
    with psycopg2.connect(resolved.value, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        row_estimates = _table_row_estimates(conn, inspected_tables)
        payload["table_row_estimates"] = row_estimates
        with conn.cursor() as cur:
            cur.execute("set statement_timeout to '20s'")
            for spec in specs:
                cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {spec.sql}", spec.params)
                row = cur.fetchone() or {}
                plan = row.get("QUERY PLAN")
                findings = _sequential_scan_findings(
                    query_name=spec.name,
                    plan=plan,
                    row_estimates=row_estimates,
                )
                payload["plans"].append(
                    {
                        "name": spec.name,
                        "sql": " ".join(spec.sql.split()),
                        "params": _json_safe(spec.params),
                        "tables": list(spec.tables),
                        "plan": _json_safe(plan),
                        "sequential_scan_findings": findings,
                    }
                )
                payload["sequential_scan_findings"].extend(findings)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload, list(payload["sequential_scan_findings"])


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, help="Social platform, e.g. instagram")
    parser.add_argument("--handle", required=True, help="Account handle, with or without @")
    parser.add_argument("--run-id", default=None, help="Optional catalog run id to include progress reads")
    parser.add_argument("--dry-run", action="store_true", help="Render SQL and params without connecting to Postgres")
    parser.add_argument("--output-dir", default=str(OUTPUT_ROOT), help="Directory for live EXPLAIN JSON artifacts")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    platform = _normalize_platform(args.platform)
    handle = _normalize_handle(args.handle)
    run_id = str(args.run_id).strip() if args.run_id else None
    specs = _build_query_specs(platform=platform, handle=handle, run_id=run_id)
    if args.dry_run:
        print(json.dumps(_json_safe(_render_dry_run(specs)), indent=2, sort_keys=True))
        return 0

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = Path(args.output_dir) / f"{platform}-{handle}-{timestamp}.json"
    payload, findings = _run_explains(specs=specs, output_path=output_path)
    print(json.dumps({"output": str(output_path), "sequential_scan_findings": findings}, indent=2, sort_keys=True))
    return 2 if payload["sequential_scan_findings"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
