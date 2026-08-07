#!/usr/bin/env python3
"""Classify low-volume Instagram comments gaps blocked by public-mode approval."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.db import pg  # noqa: E402
from trr_backend.socials import social_season_analytics_impl as social_repo  # noqa: E402
from trr_backend.socials.instagram.comments_scrapling.fetcher import (  # noqa: E402
    InstagramCommentsFetchResult,
)
from trr_backend.socials.instagram.comments_scrapling.job_runner import (  # noqa: E402
    APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON,
    _classify_unavailable_instagram_comment_gap,
)
from trr_backend.utils.env import load_env  # noqa: E402

APPROVAL_ERROR_CODE = "instagram_comments_public_requires_approval"
CONFIRM_APPLY = "CLASSIFY APPROVAL BLOCKED COMMENTS"
DEFAULT_MAX_REPORTED_COMMENTS = 99


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="classify_approval_blocked_comments",
        description=(
            "Preview or insert classified-missing Instagram comment rows for "
            "low-volume targets blocked by public-mode approval."
        ),
    )
    parser.add_argument("--run-id", required=True, help="Comments child social.scrape_runs id.")
    parser.add_argument("--account", default="bravotv", help="Instagram account handle.")
    parser.add_argument("--target", action="append", help="Optional shortcode filter. Repeatable or comma-separated.")
    parser.add_argument("--job-limit", type=int, help="Maximum approval-failed jobs to inspect.")
    parser.add_argument(
        "--max-reported-comments",
        type=int,
        default=DEFAULT_MAX_REPORTED_COMMENTS,
        help="Only classify posts with reported comments at or below this value. Use 0 to disable.",
    )
    parser.add_argument("--max-comments-per-post", type=int, default=0, help="0 means uncapped.")
    parser.add_argument(
        "--target-detail-limit",
        type=int,
        default=100,
        help="Maximum target detail rows to emit. Use 0 for all rows.",
    )
    parser.add_argument(
        "--include-non-failed-jobs",
        action="store_true",
        help="Also inspect non-failed jobs with the approval error code.",
    )
    parser.add_argument("--apply", action="store_true", help="Insert classified-missing rows.")
    parser.add_argument(
        "--confirm-apply",
        help=f"Required with --apply. Exact value: {CONFIRM_APPLY!r}.",
    )
    parser.add_argument(
        "--confirm-run-id",
        help="Required with --apply; must exactly match --run-id.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    return parser.parse_args(argv)


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _normalize_terms(values: Sequence[Any] | None) -> list[str]:
    terms: list[str] = []
    for value in values or []:
        for part in str(value or "").split(","):
            normalized = part.strip()
            if normalized:
                terms.append(normalized)
    return list(dict.fromkeys(terms))


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _nested(mapping: Mapping[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _metadata_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _normalize_terms([value])
    if isinstance(value, Sequence):
        return _normalize_terms(value)
    return []


def _extract_approval_target_source_ids(metadata: Mapping[str, Any], _config: Mapping[str, Any]) -> list[str]:
    candidates = [
        _nested(metadata, "runtime_metadata", "incomplete_target_source_ids"),
        _nested(metadata, "runtime_metadata", "zero_comment_incomplete_target_source_ids"),
        _nested(metadata, "runtime_metadata", "auth_failed_target_source_ids"),
        _nested(metadata, "activity", "incomplete_target_source_ids"),
        _nested(metadata, "activity", "auth_failed_target_source_ids"),
        _nested(metadata, "post_fetch_failures", "target_source_ids"),
        _nested(metadata, "post_auth_failures", "target_source_ids"),
        _nested(metadata, "retry_rebalance", "remaining_target_source_ids"),
        metadata.get("incomplete_target_source_ids"),
        metadata.get("zero_comment_incomplete_target_source_ids"),
        metadata.get("public_blocked_target_source_ids"),
        metadata.get("auth_failed_target_source_ids"),
    ]
    targets: list[str] = []
    for candidate in candidates:
        targets.extend(_metadata_string_list(candidate))
    return list(dict.fromkeys(targets))


def _reason_for_target(metadata: Mapping[str, Any], shortcode: str) -> str | None:
    reason_maps = [
        _nested(metadata, "runtime_metadata", "incomplete_fetch_reasons"),
        _nested(metadata, "runtime_metadata", "auth_failed_fetch_reasons"),
        _nested(metadata, "post_fetch_failures", "fetch_reasons"),
        _nested(metadata, "post_auth_failures", "fetch_reasons"),
        metadata.get("incomplete_fetch_reasons"),
        metadata.get("auth_failed_fetch_reasons"),
    ]
    for reason_map in reason_maps:
        if not isinstance(reason_map, Mapping):
            continue
        reason = str(reason_map.get(shortcode) or "").strip()
        if reason:
            return reason
    return None


def _fetch_approval_jobs(
    *,
    run_id: str,
    include_non_failed_jobs: bool,
    job_limit: int | None,
    conn: Any | None = None,
) -> list[dict[str, Any]]:
    status_sql = "" if include_non_failed_jobs else "and lower(j.status) = 'failed'"
    limit_sql = ""
    params: list[Any] = [run_id, APPROVAL_ERROR_CODE, social_repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE]
    if job_limit is not None:
        limit_sql = "limit %s"
        params.append(max(1, int(job_limit)))
    return pg.fetch_all(
        f"""
        select
          j.id::text as job_id,
          j.run_id::text as run_id,
          j.status,
          j.last_error_code,
          j.error_message,
          coalesce(j.config, '{{}}'::jsonb) as config,
          coalesce(j.metadata, '{{}}'::jsonb) as metadata
        from social.scrape_jobs j
        where j.run_id = %s::uuid
          and lower(coalesce(j.last_error_code, j.metadata->>'error_code', '')) = %s
          and coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s
          {status_sql}
        order by j.completed_at desc nulls last, j.created_at desc nulls last, j.id desc
        {limit_sql}
        """,
        params,
        conn=conn,
    )


def _fetch_target_post_rows(
    *,
    account_handle: str,
    shortcodes: Sequence[str],
    conn: Any | None = None,
) -> dict[str, dict[str, Any]]:
    requested = _normalize_terms(shortcodes)
    if not requested:
        return {}
    normalized_account = _normalize_account(account_handle)
    reported_comments_sql = social_repo._instagram_reported_comments_sql("p")  # noqa: SLF001
    facebook_comments_sql = social_repo._instagram_external_facebook_comments_sql("p")  # noqa: SLF001
    account_sql = ""
    params: list[Any] = [requested]
    if normalized_account:
        account_sql = "and ltrim(lower(coalesce(p.source_account, '')), '@') = %s"
        params.append(normalized_account)
    rows = pg.fetch_all(
        f"""
        with requested as (
          select nullif(shortcode, '')::text as shortcode, ordinality::int as sort_order
          from unnest(%s::text[]) with ordinality as request(shortcode, ordinality)
          where nullif(shortcode, '') is not null
        ),
        candidate_posts as (
          select
            p.id::text as post_id,
            p.shortcode::text as shortcode,
            ({reported_comments_sql})::int as reported_comments,
            ({facebook_comments_sql})::int as facebook_comment_count,
            row_number() over (
              partition by p.shortcode
              order by p.posted_at desc nulls last, p.id desc
            ) as row_number
          from social.instagram_posts p
          join requested r on r.shortcode = p.shortcode
          where nullif(p.shortcode, '') is not null
            {account_sql}
        ),
        selected_posts as (
          select post_id, shortcode, reported_comments, facebook_comment_count
          from candidate_posts
          where row_number = 1
        ),
        saved_comment_counts as (
          select
            sp.post_id,
            count(c.id) filter (where coalesce(c.is_missing, false) = false)::int as stored_total_comments,
            count(c.id) filter (
              where coalesce(c.is_missing, false) = true
                and coalesce(c.source_snapshot_type, '') = 'classified_missing_comments'
            )::int as existing_missing_comments
          from selected_posts sp
          left join social.instagram_comments c on c.post_id = sp.post_id::uuid
          group by sp.post_id
        )
        select
          r.shortcode,
          sp.post_id,
          coalesce(sp.reported_comments, 0)::int as reported_comments,
          coalesce(sp.facebook_comment_count, 0)::int as facebook_comment_count,
          coalesce(scc.stored_total_comments, 0)::int as stored_total_comments,
          coalesce(scc.existing_missing_comments, 0)::int as existing_missing_comments
        from requested r
        left join selected_posts sp on sp.shortcode = r.shortcode
        left join saved_comment_counts scc on scc.post_id = sp.post_id
        order by r.sort_order
        """,
        params,
        conn=conn,
    )
    return {str(row.get("shortcode") or "").strip(): dict(row) for row in rows}


def _target_plan_row(
    *,
    shortcode: str,
    job_ids: Sequence[str],
    source_reasons: Sequence[str],
    post_row: Mapping[str, Any] | None,
    max_reported_comments: int,
    max_comments_per_post: int,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "shortcode": shortcode,
        "job_ids": list(dict.fromkeys(str(job_id) for job_id in job_ids if str(job_id).strip())),
        "source_reasons": list(dict.fromkeys(str(reason) for reason in source_reasons if str(reason).strip())),
        "classification_reason": APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON,
        "eligible": False,
        "would_insert_missing_comments": 0,
        "inserted_missing_comments": 0,
    }
    if not post_row or not str(post_row.get("post_id") or "").strip():
        row["skip_reason"] = "post_not_found"
        return row

    reported_comments = _safe_int(post_row.get("reported_comments"))
    stored_total_comments = _safe_int(post_row.get("stored_total_comments"))
    existing_missing_comments = _safe_int(post_row.get("existing_missing_comments"))
    facebook_comment_count = _safe_int(post_row.get("facebook_comment_count"))
    target_count = reported_comments
    if max_comments_per_post > 0:
        target_count = min(target_count, max_comments_per_post)
    residual = max(0, target_count - stored_total_comments - existing_missing_comments - facebook_comment_count)
    row.update(
        {
            "post_id": str(post_row.get("post_id") or ""),
            "reported_comments": reported_comments,
            "target_comment_count": target_count,
            "stored_total_comments": stored_total_comments,
            "existing_missing_comments": existing_missing_comments,
            "facebook_comment_count": facebook_comment_count,
            "would_insert_missing_comments": residual,
        }
    )
    if reported_comments <= 0:
        row["skip_reason"] = "no_reported_comments"
        return row
    if max_reported_comments > 0 and reported_comments > max_reported_comments:
        row["skip_reason"] = "reported_comments_above_terminal_threshold"
        return row
    if residual <= 0:
        row["skip_reason"] = "no_unaccounted_gap"
        return row
    row["eligible"] = True
    return row


def _build_payload(
    *,
    run_id: str,
    account_handle: str,
    target_source_ids: Sequence[str],
    include_non_failed_jobs: bool,
    job_limit: int | None,
    max_reported_comments: int,
    max_comments_per_post: int,
    apply: bool,
    target_detail_limit: int = 0,
    conn: Any | None = None,
) -> dict[str, Any]:
    normalized_targets = set(_normalize_terms(target_source_ids))
    jobs = _fetch_approval_jobs(
        run_id=run_id,
        include_non_failed_jobs=include_non_failed_jobs,
        job_limit=job_limit,
        conn=conn,
    )
    candidates: dict[str, dict[str, Any]] = {}
    for job in jobs:
        metadata = _metadata_dict(job.get("metadata"))
        config = _metadata_dict(job.get("config"))
        job_id = str(job.get("job_id") or "").strip()
        for shortcode in _extract_approval_target_source_ids(metadata, config):
            if normalized_targets and shortcode not in normalized_targets:
                continue
            candidate = candidates.setdefault(shortcode, {"job_ids": [], "source_reasons": []})
            if job_id:
                candidate["job_ids"].append(job_id)
            reason = _reason_for_target(metadata, shortcode)
            if reason:
                candidate["source_reasons"].append(reason)

    post_rows = _fetch_target_post_rows(
        account_handle=account_handle,
        shortcodes=list(candidates.keys()),
        conn=conn,
    )
    target_rows = [
        _target_plan_row(
            shortcode=shortcode,
            job_ids=candidate.get("job_ids") or [],
            source_reasons=candidate.get("source_reasons") or [],
            post_row=post_rows.get(shortcode),
            max_reported_comments=max(0, int(max_reported_comments or 0)),
            max_comments_per_post=max(0, int(max_comments_per_post or 0)),
        )
        for shortcode, candidate in candidates.items()
    ]

    inserted_missing_comments = 0
    if apply:
        for target in target_rows:
            if not target.get("eligible"):
                continue
            inserted = _classify_unavailable_instagram_comment_gap(
                conn=conn,
                post_id=str(target.get("post_id") or ""),
                result=InstagramCommentsFetchResult(reported_comment_count=_safe_int(target.get("reported_comments"))),
                stored_total_comments=_safe_int(target.get("stored_total_comments")),
                max_comments_per_post=max(0, int(max_comments_per_post or 0)),
                run_id=run_id,
                job_id=(target.get("job_ids") or [None])[0],
                reason=APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON,
            )
            target["inserted_missing_comments"] = inserted
            inserted_missing_comments += inserted

    eligible_targets = [target for target in target_rows if target.get("eligible")]
    safe_target_detail_limit = max(0, int(target_detail_limit or 0))
    emitted_target_rows = target_rows
    if safe_target_detail_limit > 0:
        emitted_target_rows = target_rows[:safe_target_detail_limit]
    payload = {
        "ok": True,
        "mode": "apply" if apply else "dry_run",
        "run_id": run_id,
        "account": _normalize_account(account_handle),
        "approval_error_code": APPROVAL_ERROR_CODE,
        "classification_reason": APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON,
        "max_reported_comments": max(0, int(max_reported_comments or 0)),
        "max_comments_per_post": max(0, int(max_comments_per_post or 0)),
        "jobs": {
            "approval_jobs": len(jobs),
            "job_ids": [str(job.get("job_id") or "") for job in jobs if str(job.get("job_id") or "").strip()],
        },
        "totals": {
            "candidate_targets": len(target_rows),
            "eligible_targets": len(eligible_targets),
            "would_insert_missing_comments": sum(
                _safe_int(target.get("would_insert_missing_comments")) for target in eligible_targets
            ),
            "inserted_missing_comments": inserted_missing_comments,
            "skipped_targets": len(target_rows) - len(eligible_targets),
        },
        "target_detail_limit": safe_target_detail_limit,
        "targets_truncated": len(emitted_target_rows) < len(target_rows),
        "targets": emitted_target_rows,
    }
    return payload


def _refusal_payload(args: argparse.Namespace, reasons: Sequence[str]) -> dict[str, Any]:
    return {
        "ok": False,
        "mode": "dry_run",
        "status": "refused",
        "run_id": args.run_id,
        "account": _normalize_account(args.account),
        "refusal_reasons": list(reasons),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    if args.apply:
        refusal_reasons: list[str] = []
        if args.confirm_apply != CONFIRM_APPLY:
            refusal_reasons.append("missing --confirm-apply")
        if str(args.confirm_run_id or "").strip() != str(args.run_id or "").strip():
            refusal_reasons.append("missing --confirm-run-id")
        if refusal_reasons:
            payload = _refusal_payload(args, refusal_reasons)
            print(json.dumps(payload, indent=2, sort_keys=True, default=str))
            return 2

    load_env()
    os.environ.setdefault("TRR_DB_POOL_CLOSE_AFTER_RETURN", "true")
    try:
        if args.apply:
            with pg.db_connection(label="classify_approval_blocked_comments") as conn:
                payload = _build_payload(
                    run_id=args.run_id,
                    account_handle=args.account,
                    target_source_ids=_normalize_terms(args.target),
                    include_non_failed_jobs=bool(args.include_non_failed_jobs),
                    job_limit=args.job_limit,
                    max_reported_comments=args.max_reported_comments,
                    max_comments_per_post=args.max_comments_per_post,
                    target_detail_limit=args.target_detail_limit,
                    apply=True,
                    conn=conn,
                )
        else:
            payload = _build_payload(
                run_id=args.run_id,
                account_handle=args.account,
                target_source_ids=_normalize_terms(args.target),
                include_non_failed_jobs=bool(args.include_non_failed_jobs),
                job_limit=args.job_limit,
                max_reported_comments=args.max_reported_comments,
                max_comments_per_post=args.max_comments_per_post,
                target_detail_limit=args.target_detail_limit,
                apply=False,
            )
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        pg.close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
