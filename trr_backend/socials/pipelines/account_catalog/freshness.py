"""Canonical account-catalog freshness and gap-status read models.

The module owns a small default persistence composition so its public reads are
usable in a clean process. The legacy composition root may still replace those
ports after import to preserve compatibility-path monkeypatch behavior while
the wider social analytics migration continues.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from psycopg2 import errors as psycopg_errors

from trr_backend.db import pg
from trr_backend.socials.pipelines.account_catalog.live_profile_total import (
    cached_instagram_live_profile_total_posts,
    cached_instagram_live_profile_total_posts_cached_only,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

logger = logging.getLogger(__name__)

SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE = "social_catalog_gap_analysis"

_CATALOG_RECENT_RUN_STAGES = (
    "shared_account_discovery",
    "shared_account_posts",
    "tiktok_posts_scrapling",
    "threads_posts_scrapling",
    "post_classify",
    "season_materialize",
    "analytics_refresh",
)
_CATALOG_POST_TABLES = {
    "instagram": "instagram_account_catalog_posts",
    "tiktok": "tiktok_account_catalog_posts",
    "twitter": "twitter_account_catalog_posts",
    "youtube": "youtube_account_catalog_posts",
    "facebook": "facebook_account_catalog_posts",
    "threads": "threads_account_catalog_posts",
}
_MATERIALIZED_POST_TABLES = {
    "instagram": "instagram_posts",
    "tiktok": "tiktok_posts",
    "twitter": "twitter_tweets",
    "youtube": "youtube_videos",
    "facebook": "facebook_posts",
    "threads": "meta_threads_posts",
}
_SHOW_EXTERNAL_ID_KEYS = {
    "facebook": ("facebook_handle", "facebook", "facebook_id"),
    "instagram": ("instagram_handle", "instagram", "instagram_id"),
    "threads": ("threads_handle", "threads", "threads_id"),
    "tiktok": ("tiktok_handle", "tiktok", "tiktok_id"),
    "twitter": ("twitter_handle", "twitter", "x_handle", "twitter_id", "x_id"),
    "youtube": ("youtube_handle", "youtube", "youtube_id"),
}
_DEFAULT_PLATFORM_ACCOUNTS = {
    "facebook": frozenset({"bravo"}),
    "instagram": frozenset({"bravotv", "bravodailydish", "bravowwhl"}),
    "threads": frozenset({"bravotv", "bravodailydish", "bravowwhl"}),
    "tiktok": frozenset({"bravotv", "bravowwhl"}),
    "twitter": frozenset({"bravotv", "bravowwhl"}),
    "youtube": frozenset({"bravo"}),
}
_HANDLE_ALIASES = {"wwhlbravo": "bravowwhl"}
_HANDLE_RE = re.compile(r"^[a-z0-9._-]{1,64}$")
_ACTIVE_RUN_STATUSES = {"queued", "pending", "retrying", "running", "cancelling"}


@dataclass(frozen=True, slots=True)
class AccountCatalogFreshnessDependencies:
    """Ports for persistence helpers still composed by the legacy core."""

    normalize_platform: Callable[[Any], str]
    normalize_handle: Callable[[Any], str]
    assert_profile_exists: Callable[[str, str], Any]
    catalog_recent_runs: Callable[..., list[dict[str, Any]]]
    get_active_run: Callable[..., dict[str, Any] | None]
    shared_catalog_total_posts: Callable[..., int]
    catalog_newest_stored_post_at: Callable[..., datetime | None]
    catalog_oldest_stored_post_at: Callable[..., datetime | None]
    latest_account_frontier: Callable[..., dict[str, Any]]
    cached_live_profile_total_posts: Callable[[str, str], int | None]
    cached_live_profile_total_posts_cached_only: Callable[[str, str], int | None]
    now_utc: Callable[[], datetime]
    iso: Callable[[datetime | None], str | None]
    gap_analysis_operation_type: Callable[[], str]
    gap_analysis_request_payload: Callable[[str, str], dict[str, str]]
    normalize_gap_analysis_operation_status: Callable[[Any], str]
    extract_gap_analysis_operation_result: Callable[[dict[str, Any] | None], dict[str, Any] | None]
    extract_gap_analysis_operation_error: Callable[[dict[str, Any] | None], dict[str, Any] | None]
    freshness_degradable_error: Callable[[Exception], bool]
    freshness_degraded_error_payload: Callable[[Exception], dict[str, Any]]


def _default_normalize_platform(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        canonical = ""
    elif tokens[0] == "meta" and len(tokens) > 1 and tokens[1] == "threads":
        canonical = "threads"
    else:
        canonical = {
            "x": "twitter",
            "ig": "instagram",
            "insta": "instagram",
            "fb": "facebook",
            "meta": "facebook",
        }.get(tokens[0], tokens[0])
    if canonical not in SOCIAL_SUPPORTED_PLATFORMS:
        supported = ", ".join(SOCIAL_SUPPORTED_PLATFORMS)
        raise ValueError(f"INVALID_PLATFORM_FILTER: Unsupported platform '{value}'. Supported platforms: {supported}")
    return canonical


def _default_normalize_handle(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Invalid account handle.")
    candidate = raw
    if "://" in raw or raw.lower().startswith("www."):
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        candidate = path_parts[0] if path_parts else str(parsed.netloc or "")
    candidate = candidate.strip().lstrip("@").split("?")[0].split("#")[0].split("/")[0].strip().lower()
    if not candidate or not _HANDLE_RE.fullmatch(candidate):
        raise ValueError("Invalid account handle.")
    return _HANDLE_ALIASES.get(candidate, candidate)


def _fetch_one(
    sql: str,
    params: list[Any],
    *,
    conn: Any | None = None,
    label: str,
) -> dict[str, Any] | None:
    if conn is None:
        return pg.fetch_one(sql, params)
    with pg.db_cursor(conn=conn, label=label) as cur:
        return pg.fetch_one_with_cursor(cur, sql, params)


def _fetch_all(
    sql: str,
    params: list[Any],
    *,
    conn: Any | None = None,
    label: str,
) -> list[dict[str, Any]]:
    if conn is None:
        return [dict(row) for row in pg.fetch_all(sql, params)]
    with pg.db_cursor(conn=conn, label=label) as cur:
        return [dict(row) for row in pg.fetch_all_with_cursor(cur, sql, params)]


def _instagram_catalog_account_match_sql(alias: str = "p") -> str:
    """Match ownership and coauthor fields without relying on legacy helpers."""

    row_json = f"to_jsonb({alias})"
    direct_candidates = (
        f"{row_json} ->> 'source_account'",
        f"{row_json} ->> 'owner_username'",
        f"{row_json} ->> 'username'",
        f"{row_json} -> 'raw_data' ->> 'source_account'",
        f"{row_json} -> 'raw_data' ->> 'owner_username'",
        f"{row_json} -> 'raw_data' ->> 'username'",
    )
    direct_array = ",\n".join(
        f"nullif(ltrim(lower(trim(coalesce({candidate}, ''))), '@'), '')" for candidate in direct_candidates
    )
    collaborator_paths = (
        f"{row_json} -> 'collaborators'",
        f"{row_json} -> 'collaborators_detail'",
        f"{row_json} -> 'raw_data' -> 'collaborators'",
        f"{row_json} -> 'raw_data' -> 'collaborators_detail'",
    )
    collaborator_checks: list[str] = []
    for index, path_sql in enumerate(collaborator_paths, start=1):
        collaborator_checks.append(
            f"""
            exists (
              select 1
              from jsonb_array_elements(
                case
                  when jsonb_typeof(coalesce({path_sql}, 'null'::jsonb)) = 'array' then {path_sql}
                  when jsonb_typeof(coalesce({path_sql}, 'null'::jsonb)) in ('object', 'string')
                    then jsonb_build_array({path_sql})
                  else '[]'::jsonb
                end
              ) as collaborator_{index}(value)
              where account_match.account_handle = nullif(
                ltrim(lower(trim(coalesce(
                  case
                    when jsonb_typeof(collaborator_{index}.value) = 'object' then coalesce(
                      collaborator_{index}.value ->> 'username',
                      collaborator_{index}.value ->> 'handle',
                      collaborator_{index}.value ->> 'user_name',
                      collaborator_{index}.value ->> 'name'
                    )
                    when jsonb_typeof(collaborator_{index}.value) = 'string'
                      then collaborator_{index}.value #>> '{{}}'
                    else trim(both '"' from collaborator_{index}.value::text)
                  end,
                  ''
                ))), '@'), '')
            )
            """
        )
    return f"""
        exists (
          select 1
          from (select %s::text as account_handle) account_match
          where account_match.account_handle = any(array_remove(array[{direct_array}], null))
             or {" or ".join(collaborator_checks)}
        )
    """


def _catalog_account_match_sql(platform: str, alias: str = "p") -> str:
    if platform == "instagram":
        return _instagram_catalog_account_match_sql(alias)
    return f"lower({alias}.source_account) = %s"


def _default_shared_catalog_total_posts(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> int:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    table = _CATALOG_POST_TABLES.get(normalized_platform)
    if table is None:
        return 0
    try:
        row = (
            _fetch_one(
                f"""
            select count(*)::int as total
            from social.{table} p
            where {_catalog_account_match_sql(normalized_platform)}
            """,
                [normalized_account],
                conn=conn,
                label="catalog-freshness-total",
            )
            or {}
        )
    except psycopg_errors.UndefinedTable:
        return 0
    return max(0, int(row.get("total") or 0))


def _default_catalog_stored_post_at(
    platform: str,
    account_handle: str,
    *,
    aggregate: str,
    conn: Any | None = None,
) -> datetime | None:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    table = _CATALOG_POST_TABLES.get(normalized_platform)
    if table is None:
        return None
    try:
        row = (
            _fetch_one(
                f"""
                select {aggregate}(posted_at) as stored_at
                from social.{table} p
                where {_catalog_account_match_sql(normalized_platform)}
                """,
                [normalized_account],
                conn=conn,
                label=f"catalog-freshness-{aggregate}",
            )
            or {}
        )
    except psycopg_errors.UndefinedTable:
        return None
    except RuntimeError as exc:
        if aggregate == "min" and pg.is_database_service_unavailable_error(exc):
            return None
        raise
    value = row.get("stored_at")
    return value if isinstance(value, datetime) else None


def _default_catalog_newest_stored_post_at(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> datetime | None:
    return _default_catalog_stored_post_at(platform, account_handle, aggregate="max", conn=conn)


def _default_catalog_oldest_stored_post_at(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> datetime | None:
    return _default_catalog_stored_post_at(platform, account_handle, aggregate="min", conn=conn)


def _default_catalog_recent_runs(
    platform: str,
    account_handle: str,
    *,
    limit: int = 10,
    conn: Any | None = None,
    auto_recover_pending: bool = True,
) -> list[dict[str, Any]]:
    del auto_recover_pending
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    safe_limit = max(1, min(int(limit), 25))
    sql = """
        with scoped_runs as (
          select
            r.id as run_uuid,
            r.id::text as run_id,
            coalesce(r.config, '{}'::jsonb) as run_config,
            coalesce(r.summary, '{}'::jsonb) as run_summary,
            r.status as run_status,
            r.created_at,
            r.started_at,
            r.completed_at
          from social.scrape_runs r
          where coalesce(r.config->>'pipeline_ingest_mode', '') = 'shared_account_catalog_backfill'
            and nullif(coalesce(r.config->>'failure_dismissed_at', ''), '') is null
            and (
              exists (
                select 1
                from social.scrape_jobs j
                where j.run_id = r.id
                  and j.platform = %s
                  and lower(coalesce(nullif(j.config->>'account', ''), nullif(j.metadata->>'account', ''), '')) = %s
                  and lower(coalesce(
                    nullif(j.config->>'stage', ''),
                    nullif(j.metadata->>'stage', ''),
                    nullif(j.job_type, ''),
                    'unknown'
                  )) = any(%s::text[])
              )
              or (
                (
                  lower(coalesce(r.config->>'launch_state', '')) = 'pending'
                  or lower(coalesce(r.config->>'launch_task_resolution_pending', 'false')) = 'true'
                )
                and lower(coalesce(nullif(r.config->>'platform', ''), nullif(r.config->'platforms'->>0, ''), '')) = %s
                and ltrim(lower(coalesce(
                  nullif(r.config->>'account_handle', ''),
                  nullif(r.config->>'account', ''),
                  nullif(r.config->'accounts_override'->>0, ''),
                  ''
                )), '@') = %s
              )
            )
        )
        select
          latest_job.job_id,
          scoped_runs.run_id,
          coalesce(nullif(lower(coalesce(scoped_runs.run_status, '')), ''), latest_job.job_status) as status,
          scoped_runs.created_at,
          scoped_runs.started_at,
          scoped_runs.completed_at,
          coalesce(latest_job.metadata, '{}'::jsonb) as metadata,
          scoped_runs.run_config,
          scoped_runs.run_summary
        from scoped_runs
        left join lateral (
          select
            j.id::text as job_id,
            lower(coalesce(nullif(j.status, ''), '')) as job_status,
            coalesce(j.metadata, '{}'::jsonb) as metadata
          from social.scrape_jobs j
          where j.run_id = scoped_runs.run_uuid
            and j.platform = %s
            and lower(coalesce(nullif(j.config->>'account', ''), nullif(j.metadata->>'account', ''), '')) = %s
            and lower(coalesce(
              nullif(j.config->>'stage', ''),
              nullif(j.metadata->>'stage', ''),
              nullif(j.job_type, ''),
              'unknown'
            )) = any(%s::text[])
          order by coalesce(j.completed_at, j.started_at, j.created_at) desc, j.id desc
          limit 1
        ) latest_job on true
        order by scoped_runs.created_at desc, scoped_runs.run_id desc
        limit %s
    """
    params: list[Any] = [
        normalized_platform,
        normalized_account,
        list(_CATALOG_RECENT_RUN_STAGES),
        normalized_platform,
        normalized_account,
        normalized_platform,
        normalized_account,
        list(_CATALOG_RECENT_RUN_STAGES),
        safe_limit,
    ]
    try:
        return _fetch_all(sql, params, conn=conn, label="catalog-freshness-recent-runs")
    except psycopg_errors.UndefinedTable:
        return []


def _default_get_active_run(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    for row in _default_catalog_recent_runs(
        platform,
        account_handle,
        limit=10,
        conn=conn,
        auto_recover_pending=False,
    ):
        status = str(row.get("status") or "").strip().lower()
        if status not in _ACTIVE_RUN_STATUSES:
            continue
        return {
            "run_id": str(row.get("run_id") or "").strip(),
            "status": status,
            "created_at": row.get("created_at"),
            "started_at": row.get("started_at"),
            "config": dict(row.get("run_config") or {}),
            "summary": dict(row.get("run_summary") or {}),
        }
    return None


def _default_latest_account_frontier(
    platform: str,
    account_handle: str,
    *,
    conn: Any | None = None,
) -> dict[str, Any]:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    try:
        relation_row = _fetch_one(
            "select to_regclass(%s) is not null as exists",
            ["social.shared_account_run_frontiers"],
            conn=conn,
            label="catalog-freshness-frontier-ready",
        )
    except Exception:  # noqa: BLE001
        return {}
    if not bool((relation_row or {}).get("exists")):
        return {}
    try:
        row = _fetch_one(
            """
            select
              id::text as id,
              run_id::text as run_id,
              platform,
              account_handle,
              status,
              next_cursor,
              posts_checked,
              posts_saved,
              pages_scanned,
              exhausted,
              metadata,
              created_at,
              updated_at
            from social.shared_account_run_frontiers
            where platform = %s
              and account_handle = %s
              and coalesce(exhausted, false) = false
              and nullif(trim(coalesce(next_cursor, '')), '') is not null
            order by updated_at desc
            limit 1
            """,
            [normalized_platform, normalized_account],
            conn=conn,
            label="catalog-freshness-frontier",
        )
    except psycopg_errors.UndefinedTable:
        return {}
    return dict(row or {})


def _optional_exists(sql: str, params: list[Any]) -> bool:
    try:
        return bool(pg.fetch_one(sql, params))
    except psycopg_errors.UndefinedTable:
        return False


def _default_assert_profile_exists(platform: str, account_handle: str) -> None:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    if _optional_exists(
        "select 1 from social.shared_account_sources where platform = %s and account_handle = %s limit 1",
        [normalized_platform, normalized_account],
    ):
        return

    external_id_keys = _SHOW_EXTERNAL_ID_KEYS.get(normalized_platform, ())
    if external_id_keys:
        clauses = " or ".join(["lower(coalesce(external_ids ->> %s, '')) = %s"] * len(external_id_keys))
        params: list[Any] = []
        for key in external_id_keys:
            params.extend([key, normalized_account])
        if _optional_exists(f"select 1 from core.shows where {clauses} limit 1", params):
            return

    if normalized_account in _DEFAULT_PLATFORM_ACCOUNTS.get(normalized_platform, frozenset()):
        return

    if _default_shared_catalog_total_posts(normalized_platform, normalized_account) > 0:
        return

    table = _MATERIALIZED_POST_TABLES.get(normalized_platform)
    if table and _optional_exists(
        f"""
        select 1
        from social.{table} p
        where %s = any(array_remove(array[
          nullif(ltrim(lower(coalesce(to_jsonb(p)->>'source_account', '')), '@'), ''),
          nullif(ltrim(lower(coalesce(to_jsonb(p)->>'owner_username', '')), '@'), ''),
          nullif(ltrim(lower(coalesce(to_jsonb(p)->>'username', '')), '@'), ''),
          nullif(ltrim(lower(coalesce(to_jsonb(p)->'raw_data'->>'source_account', '')), '@'), ''),
          nullif(ltrim(lower(coalesce(to_jsonb(p)->'raw_data'->>'owner_username', '')), '@'), ''),
          nullif(ltrim(lower(coalesce(to_jsonb(p)->'raw_data'->>'username', '')), '@'), '')
        ], null))
        limit 1
        """,
        [normalized_account],
    ):
        return

    if normalized_platform == "instagram" and _optional_exists(
        """
        select 1
        from social.instagram_account_catalog_post_collaborators
        where collaborator_handle = %s
        limit 1
        """,
        [normalized_account],
    ):
        return
    raise LookupError("Social account profile not found.")


def _default_now_utc() -> datetime:
    return datetime.now(UTC)


def _default_iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _default_cached_live_profile_total_posts(platform: str, account_handle: str) -> int | None:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    if normalized_platform != "instagram":
        return None
    return cached_instagram_live_profile_total_posts(normalized_account)


def _default_cached_live_profile_total_posts_cached_only(platform: str, account_handle: str) -> int | None:
    normalized_platform = _default_normalize_platform(platform)
    normalized_account = _default_normalize_handle(account_handle)
    if normalized_platform != "instagram":
        return None
    return cached_instagram_live_profile_total_posts_cached_only(normalized_account)


_dependencies: AccountCatalogFreshnessDependencies | None = None


def configure_account_catalog_freshness_dependencies(
    dependencies: AccountCatalogFreshnessDependencies,
) -> None:
    """Replace the default ports for compatibility-path composition."""

    global _dependencies
    _dependencies = dependencies


def _configured_dependencies() -> AccountCatalogFreshnessDependencies:
    global _dependencies
    dependencies = _dependencies
    if dependencies is None:
        dependencies = _build_default_dependencies()
        _dependencies = dependencies
    return dependencies


def _social_catalog_gap_analysis_request_payload(platform: str, account_handle: str) -> dict[str, str]:
    dependencies = _configured_dependencies()
    return {
        "platform": dependencies.normalize_platform(platform),
        "account_handle": dependencies.normalize_handle(account_handle),
    }


def _normalize_gap_analysis_operation_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "pending":
        return "queued"
    if normalized in {"running", "cancelling"}:
        return "running"
    if normalized == "completed":
        return "completed"
    if normalized in {"failed", "cancelled"}:
        return "failed"
    return "idle"


def _extract_gap_analysis_operation_result(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    result_payload = row.get("result_payload")
    if not isinstance(result_payload, dict):
        return None
    nested = result_payload.get("result")
    if isinstance(nested, dict):
        return nested
    if isinstance(result_payload.get("gap_type"), str):
        return result_payload
    return None


def _extract_gap_analysis_operation_error(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    error_payload = row.get("error_payload")
    if isinstance(error_payload, dict):
        return error_payload
    progress_payload = row.get("progress_payload")
    if isinstance(progress_payload, dict) and str(progress_payload.get("status") or "").strip().lower() == "failed":
        return progress_payload
    return None


def get_social_account_catalog_gap_analysis_status(platform: str, account_handle: str) -> dict[str, Any]:
    """Return the latest gap-analysis operation, including a stale completed result."""

    from trr_backend.repositories import admin_operations as admin_operations_repo

    dependencies = _configured_dependencies()
    request_payload = dependencies.gap_analysis_request_payload(platform, account_handle)
    normalized_platform = request_payload["platform"]
    normalized_account = request_payload["account_handle"]
    dependencies.assert_profile_exists(normalized_platform, normalized_account)

    operation_type = dependencies.gap_analysis_operation_type()
    latest_operation = admin_operations_repo.get_latest_operation_for_request_payload(
        operation_type=operation_type,
        request_payload=request_payload,
    )
    latest_completed_operation = admin_operations_repo.get_latest_operation_for_request_payload(
        operation_type=operation_type,
        request_payload=request_payload,
        statuses=["completed"],
    )

    if not latest_operation:
        return {
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "status": "idle",
            "operation_id": None,
            "result": None,
            "stale": False,
            "duration_ms": None,
            "stage_timings": None,
            "last_requested_at": None,
            "last_completed_at": None,
            "last_error": None,
        }

    normalized_status = dependencies.normalize_gap_analysis_operation_status(latest_operation.get("status"))
    latest_result = dependencies.extract_gap_analysis_operation_result(latest_operation)
    completed_result = dependencies.extract_gap_analysis_operation_result(latest_completed_operation)
    current_result = latest_result
    stale = False

    if normalized_status in {"queued", "running"} and completed_result:
        current_result = completed_result
        stale = True

    result_payload: dict[str, Any] = {}
    candidate_result_payload = latest_operation.get("result_payload")
    if isinstance(candidate_result_payload, dict):
        result_payload = candidate_result_payload
    completed_payload: dict[str, Any] = {}
    if isinstance(latest_completed_operation, dict):
        candidate_completed_payload = latest_completed_operation.get("result_payload")
        if isinstance(candidate_completed_payload, dict):
            completed_payload = candidate_completed_payload

    return {
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "status": normalized_status,
        "operation_id": str(latest_operation.get("id") or "").strip() or None,
        "result": current_result,
        "stale": stale,
        "duration_ms": (
            result_payload.get("duration_ms")
            if normalized_status == "completed"
            else completed_payload.get("duration_ms")
            if stale
            else None
        ),
        "stage_timings": (
            result_payload.get("stage_timings")
            if normalized_status == "completed"
            else completed_payload.get("stage_timings")
            if stale
            else None
        ),
        "last_requested_at": latest_operation.get("created_at"),
        "last_completed_at": (
            latest_operation.get("completed_at")
            if normalized_status == "completed"
            else (latest_completed_operation or {}).get("completed_at")
        ),
        "last_error": dependencies.extract_gap_analysis_operation_error(latest_operation),
    }


def get_social_account_catalog_freshness(
    platform: str,
    account_handle: str,
    *,
    use_cached_live_total_only: bool = False,
    statement_timeout_ms: int = 3000,
) -> dict[str, Any]:
    """Report whether an Instagram account catalog needs a recent sync."""

    dependencies = _configured_dependencies()
    normalized_platform = dependencies.normalize_platform(platform)
    normalized_account = dependencies.normalize_handle(account_handle)
    if normalized_platform != "instagram":
        raise ValueError("Catalog freshness checks are currently only supported for Instagram.")
    dependencies.assert_profile_exists(normalized_platform, normalized_account)

    safe_statement_timeout_ms = max(1000, min(int(statement_timeout_ms), 30000))
    freshness_error: dict[str, Any] | None = None
    with pg.db_connection(label="catalog-freshness", pool_name="social_profile") as conn:
        with pg.db_cursor(conn=conn, label="catalog-freshness-timeout") as cur:
            cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
        try:
            latest_run = (
                dependencies.catalog_recent_runs(normalized_platform, normalized_account, limit=1, conn=conn) or [{}]
            )[0]
        except Exception as exc:  # noqa: BLE001
            if not dependencies.freshness_degradable_error(exc):
                raise
            logger.warning(
                "[catalog-freshness] recent_runs_unavailable platform=%s account=%s error=%s",
                normalized_platform,
                normalized_account,
                exc,
            )
            latest_run = {}
            freshness_error = dependencies.freshness_degraded_error_payload(exc)
        latest_run_status = str(latest_run.get("status") or "").strip().lower() or None
        active_run = None
        if freshness_error is None:
            try:
                active_run = dependencies.get_active_run(normalized_platform, normalized_account, conn=conn)
            except Exception as exc:  # noqa: BLE001
                if not dependencies.freshness_degradable_error(exc):
                    raise
                logger.warning(
                    "[catalog-freshness] active_run_unavailable platform=%s account=%s error=%s",
                    normalized_platform,
                    normalized_account,
                    exc,
                )
                freshness_error = dependencies.freshness_degraded_error_payload(exc)
        active_run_status = str((active_run or {}).get("status") or "").strip().lower() or None
        stored_total_posts = dependencies.shared_catalog_total_posts(
            normalized_platform,
            normalized_account,
            conn=conn,
        )
        catalog_newest_at = dependencies.catalog_newest_stored_post_at(
            normalized_platform,
            normalized_account,
            conn=conn,
        )
        catalog_oldest_at = dependencies.catalog_oldest_stored_post_at(
            normalized_platform,
            normalized_account,
            conn=conn,
        )
        frontier = dependencies.latest_account_frontier(normalized_platform, normalized_account, conn=conn)
    has_resumable_frontier = bool(frontier.get("next_cursor") and not frontier.get("exhausted"))
    checked_at = dependencies.now_utc().isoformat()

    base_payload = {
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "checked_at": checked_at,
        "stored_total_posts": stored_total_posts,
        "latest_catalog_run_status": latest_run_status,
        "active_run_status": active_run_status,
        "catalog_newest_post_at": dependencies.iso(catalog_newest_at),
        "catalog_oldest_post_at": dependencies.iso(catalog_oldest_at),
        "has_resumable_frontier": has_resumable_frontier,
        "frontier_pages_scanned": frontier.get("pages_scanned") if has_resumable_frontier else None,
        "frontier_posts_checked": frontier.get("posts_checked") if has_resumable_frontier else None,
    }

    if freshness_error is not None:
        return {
            **base_payload,
            "eligible": False,
            "reason": "catalog_recent_runs_unavailable",
            "live_total_posts_current": None,
            "delta_posts": 0,
            "needs_recent_sync": False,
            "degraded": True,
            "recent_runs_available": False,
            "freshness_error": freshness_error,
        }

    if active_run:
        return {
            **base_payload,
            "eligible": False,
            "reason": "active_run",
            "live_total_posts_current": None,
            "delta_posts": 0,
            "needs_recent_sync": False,
        }
    if latest_run_status != "completed":
        return {
            **base_payload,
            "eligible": False,
            "reason": "latest_run_not_completed",
            "live_total_posts_current": None,
            "delta_posts": 0,
            "needs_recent_sync": False,
        }

    live_total_posts = (
        dependencies.cached_live_profile_total_posts_cached_only(normalized_platform, normalized_account)
        if use_cached_live_total_only
        else dependencies.cached_live_profile_total_posts(normalized_platform, normalized_account)
    )
    delta_posts = 0
    needs_recent_sync = False
    if live_total_posts is not None and live_total_posts > stored_total_posts:
        delta_posts = live_total_posts - stored_total_posts
        needs_recent_sync = True
    return {
        **base_payload,
        "eligible": True,
        "reason": None,
        "live_total_posts_current": live_total_posts,
        "delta_posts": delta_posts,
        "needs_recent_sync": needs_recent_sync,
    }


def _catalog_freshness_degradable_error(error: Exception) -> bool:
    if pg._is_statement_timeout_error(error):
        return True
    if isinstance(error, pg.DatabaseServiceUnavailableError):
        return error.reason in {"statement_timeout", "pool_capacity", "session_pool_capacity", "pool_initialization"}
    return False


def _catalog_freshness_degraded_error_payload(error: Exception) -> dict[str, Any]:
    detail = pg.database_service_unavailable_detail(error)
    reason = str(detail.get("reason") or "database_unavailable")
    return {
        "code": "CATALOG_RECENT_RUNS_UNAVAILABLE",
        "reason": reason,
        "message": "Recent catalog-run state is temporarily unavailable; stored catalog totals are still shown.",
        "retryable": True,
        "retry_after_ms": detail.get("retry_after_ms", 1000),
    }


def _build_default_dependencies() -> AccountCatalogFreshnessDependencies:
    """Compose the canonical standalone adapters with late-bound callables."""

    return AccountCatalogFreshnessDependencies(
        normalize_platform=lambda value: _default_normalize_platform(value),
        normalize_handle=lambda value: _default_normalize_handle(value),
        assert_profile_exists=lambda platform, account: _default_assert_profile_exists(platform, account),
        catalog_recent_runs=lambda platform, account, **kwargs: _default_catalog_recent_runs(
            platform, account, **kwargs
        ),
        get_active_run=lambda platform, account, **kwargs: _default_get_active_run(platform, account, **kwargs),
        shared_catalog_total_posts=lambda platform, account, **kwargs: _default_shared_catalog_total_posts(
            platform, account, **kwargs
        ),
        catalog_newest_stored_post_at=lambda platform, account, **kwargs: _default_catalog_newest_stored_post_at(
            platform, account, **kwargs
        ),
        catalog_oldest_stored_post_at=lambda platform, account, **kwargs: _default_catalog_oldest_stored_post_at(
            platform, account, **kwargs
        ),
        latest_account_frontier=lambda platform, account, **kwargs: _default_latest_account_frontier(
            platform, account, **kwargs
        ),
        cached_live_profile_total_posts=lambda platform, account: _default_cached_live_profile_total_posts(
            platform, account
        ),
        cached_live_profile_total_posts_cached_only=lambda platform,
        account: _default_cached_live_profile_total_posts_cached_only(platform, account),
        now_utc=lambda: _default_now_utc(),
        iso=lambda value: _default_iso(value),
        gap_analysis_operation_type=lambda: SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
        gap_analysis_request_payload=lambda platform, account: {
            "platform": _default_normalize_platform(platform),
            "account_handle": _default_normalize_handle(account),
        },
        normalize_gap_analysis_operation_status=lambda value: _normalize_gap_analysis_operation_status(value),
        extract_gap_analysis_operation_result=lambda row: _extract_gap_analysis_operation_result(row),
        extract_gap_analysis_operation_error=lambda row: _extract_gap_analysis_operation_error(row),
        freshness_degradable_error=lambda error: _catalog_freshness_degradable_error(error),
        freshness_degraded_error_payload=lambda error: _catalog_freshness_degraded_error_payload(error),
    )


__all__ = [
    "AccountCatalogFreshnessDependencies",
    "SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE",
    "configure_account_catalog_freshness_dependencies",
    "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status",
]
