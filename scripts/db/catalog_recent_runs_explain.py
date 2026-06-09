#!/usr/bin/env python3
"""Generate or run the `_catalog_recent_runs` EXPLAIN statement.

This helper is evidence-only. It reuses the backend query builder so operators
can inspect the exact current query shape before proposing an index or rewrite.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg2

try:
    from trr_backend.socials import social_season_analytics_impl as social_repo
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.socials import social_season_analytics_impl as social_repo


DB_URL_SOURCES = ("TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL")
DEFAULT_ROUTE = "/api/v1/admin/socials/profiles/:platform/:handle/dashboard"


def _resolve_db_url() -> tuple[str, str]:
    for source in DB_URL_SOURCES:
        value = (os.getenv(source) or "").strip()
        if value:
            return value, source
    raise SystemExit("No database URL configured. Set TRR_DB_SESSION_URL, TRR_DB_URL, or TRR_DB_FALLBACK_URL.")


def _json_safe(value: Any) -> Any:
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _build_explain(args: argparse.Namespace) -> tuple[str, list[Any]]:
    return social_repo._catalog_recent_runs_explain_statement(
        args.platform,
        args.account_handle,
        limit=args.limit,
    )


def _render_statement(sql: str, params: list[Any], args: argparse.Namespace) -> str:
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "hot_path": "profile_dashboard",
        "label": "_catalog_recent_runs",
        "route": DEFAULT_ROUTE,
        "platform": args.platform,
        "account_handle": args.account_handle,
        "limit": args.limit,
        "execute": False,
        "analyze": False,
        "note": "Parameterized EXPLAIN only; run with --execute for current DB planner proof.",
        "sql": sql.strip(),
        "params": _json_safe(params),
    }
    if args.format == "json":
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    lines = [
        "# _catalog_recent_runs EXPLAIN",
        f"generated_at: {payload['generated_at']}",
        f"hot_path: {payload['hot_path']}",
        f"label: {payload['label']}",
        f"route: {payload['route']}",
        f"platform: {payload['platform']}",
        f"account_handle: {payload['account_handle']}",
        f"limit: {payload['limit']}",
        "analyze: false",
        "",
        "SQL:",
        sql.strip(),
        "",
        "Params:",
    ]
    lines.extend(f"{index}: {json.dumps(_json_safe(param), sort_keys=True)}" for index, param in enumerate(params, 1))
    lines.append("")
    return "\n".join(lines)


def _run_explain(sql: str, params: list[Any], args: argparse.Namespace) -> dict[str, Any]:
    db_url, db_url_source = _resolve_db_url()
    with psycopg2.connect(
        db_url,
        connect_timeout=10,
        application_name="trr-catalog-recent-runs-explain",
    ) as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("set local statement_timeout = %s", [args.statement_timeout])
            cur.execute("set local lock_timeout = %s", [args.lock_timeout])
            cur.execute("set local idle_in_transaction_session_timeout = '15s'")
            cur.execute("set local row_security = on")
            cur.execute(sql, params)
            plan_rows = [str(row[0]) for row in cur.fetchall()]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "hot_path": "profile_dashboard",
        "label": "_catalog_recent_runs",
        "route": DEFAULT_ROUTE,
        "platform": args.platform,
        "account_handle": args.account_handle,
        "limit": args.limit,
        "execute": True,
        "analyze": False,
        "database": {"source": db_url_source, "value": "redacted"},
        "statement_timeout": args.statement_timeout,
        "lock_timeout": args.lock_timeout,
        "sql": sql.strip(),
        "params": _json_safe(params),
        "plan": plan_rows,
    }


def _render_execution(report: dict[str, Any], args: argparse.Namespace) -> str:
    if args.format == "json":
        return json.dumps(report, indent=2, sort_keys=True) + "\n"
    lines = [
        "# _catalog_recent_runs EXPLAIN result",
        f"generated_at: {report['generated_at']}",
        f"hot_path: {report['hot_path']}",
        f"label: {report['label']}",
        f"route: {report['route']}",
        f"platform: {report['platform']}",
        f"account_handle: {report['account_handle']}",
        f"limit: {report['limit']}",
        f"database_source: {report['database']['source']}",
        f"statement_timeout: {report['statement_timeout']}",
        f"lock_timeout: {report['lock_timeout']}",
        "analyze: false",
        "",
        "Plan:",
    ]
    lines.extend(str(line) for line in report["plan"])
    lines.append("")
    return "\n".join(lines)


def _write_or_print(content: str, output: str | None) -> None:
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(f"Wrote {output_path}")
        return
    print(content, end="")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate or run the exact `_catalog_recent_runs` EXPLAIN statement.",
    )
    parser.add_argument("--platform", default="instagram", help="Social platform to inspect.")
    parser.add_argument("--account-handle", default="bravotv", help="Account handle to inspect; @ prefix is optional.")
    parser.add_argument("--limit", type=int, default=10, help="Recent-runs limit; backend clamps this to 1..25.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run the read-only EXPLAIN against TRR_DB_SESSION_URL, TRR_DB_URL, or TRR_DB_FALLBACK_URL.",
    )
    parser.add_argument("--statement-timeout", default="8s", help="Transaction-local statement timeout for --execute.")
    parser.add_argument("--lock-timeout", default="1s", help="Transaction-local lock timeout for --execute.")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")
    parser.add_argument("--output", help="Optional file path for generated statement or plan output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sql, params = _build_explain(args)
    if args.execute:
        report = _run_explain(sql, params, args)
        content = _render_execution(report, args)
    else:
        content = _render_statement(sql, params, args)
    _write_or_print(content, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
