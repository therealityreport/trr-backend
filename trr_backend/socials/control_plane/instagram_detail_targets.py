"""Durable Post Details ownership and checkpoints (contract version 1).

All functions accepting a connection participate in the caller's transaction.
No provider request belongs inside any of these transactions.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any

from trr_backend.db import pg

VERSION = 1
SATISFIED = frozenset({"committed", "cached_satisfied", "source_unavailable"})
current_target: ContextVar[Mapping[str, Any] | None] = ContextVar("instagram_detail_target", default=None)


@contextmanager
def writing_target(target: Mapping[str, Any]):
    token = current_target.set(target)
    try:
        yield
    finally:
        current_target.reset(token)


class LostDetailOwnershipError(RuntimeError):
    """A newer dispatch or target lease owns this work."""


def _one(sql: str, params: list[Any], *, conn: Any = None) -> dict[str, Any] | None:
    with pg.db_cursor(conn=conn, label="instagram_detail_targets") as cur:
        return pg.fetch_one_with_cursor(cur, sql, params)


def reserve_dispatch(job_id: str, *, seconds: int = 720) -> dict[str, Any] | None:
    if pg._transaction_connection.get() is not None:
        raise RuntimeError("Dispatch requires a committed launch transaction")
    return _one(
        """update social.scrape_jobs set
          detail_dispatch_generation = detail_dispatch_generation + 1,
          detail_dispatch_token = %s,
          detail_dispatch_expires_at = now() + %s * interval '1 second',
          detail_completion_receipt = null
        where id = %s::uuid and status in ('queued', 'pending', 'retrying')
          and available_at <= now()
          and (detail_dispatch_expires_at is null or detail_dispatch_expires_at < now())
        returning detail_dispatch_token as token, detail_dispatch_generation as generation""",
        [uuid.uuid4().hex, max(120, seconds), job_id],
    )


def bind_dispatch(job_id: str, token: str, result: Mapping[str, Any]) -> bool:
    # A missing/uncertain provider result retains the reservation until expiry.
    dispatch = {
        "dispatch_backend": "modal",
        "remote_invocation_id": result.get("call_id"),
        "remote_function_name": result.get("function_name"),
        "remote_invocation_status": "unknown",
        "spawn_outcome": "bound" if result.get("dispatched") else "uncertain",
    }
    return bool(
        _one(
            """update social.scrape_jobs set metadata = jsonb_set(
          coalesce(metadata, '{}'::jsonb), '{dispatch}',
          coalesce(metadata->'dispatch', '{}'::jsonb) || %s::jsonb)
        where id = %s::uuid and detail_dispatch_token = %s
        returning id""",
            [json.dumps(dispatch), job_id, token],
        )
    )


def assert_owner(job_id: str, token: str, worker_id: str, *, conn: Any) -> dict[str, Any]:
    row = _one(
        """select id, run_id, detail_dispatch_generation from social.scrape_jobs
        where id = %s::uuid and detail_dispatch_token = %s and worker_id = %s
          and status = 'running' and detail_dispatch_expires_at > now()
        for update""",
        [job_id, token, worker_id],
        conn=conn,
    )
    if not row:
        raise LostDetailOwnershipError(job_id)
    return row


def freeze_manifest(run_id: str, account: str, config: Mapping[str, Any], *, conn: Any) -> dict[str, Any]:
    account = account.strip().lstrip("@").lower()
    row = _one("select config from social.scrape_runs where id = %s::uuid for update", [run_id], conn=conn)
    if row is None:
        raise ValueError("Post Details run does not exist")
    manifests = dict((row.get("config") or {}).get("detail_manifests") or {})
    if account in manifests:
        return manifests[account]
    try:
        metadata_days = int(
            config.get("details_refresh_stale_metadata_days")
            or os.getenv("SOCIAL_INSTAGRAM_DETAILS_REFRESH_STALE_METADATA_DAYS")
            or 30
        )
    except (TypeError, ValueError):
        metadata_days = 30
    policy = {
        "version": VERSION,
        "identity": str(uuid.uuid4()),
        "account": account,
        "platform": "instagram",
        "run_id": run_id,
        "source_scope": config.get("source_scope") or "network",
        "date_start": str(config.get("date_start") or "") or None,
        "date_end": str(config.get("date_end") or "") or None,
        "selected_tasks": config.get("selected_tasks") or config.get("effective_selected_tasks") or [],
        "metadata_ttl_days": max(1, min(365, metadata_days)),
        "field_policy_version": 1,
        "metrics_ttl_hours": 24,
        "force": bool(config.get("details_refresh_force_detail_fetch")),
    }
    with pg.db_cursor(conn=conn, label="freeze_instagram_detail_manifest") as cur:
        cur.execute(
            """insert into social.instagram_detail_run_targets
              (run_id, source_id, target_key, account_handle, required_mask, policy_version, state, error_code)
            select %s::uuid, normalized_id, normalized_id, %s, %s::jsonb, 1,
              case when normalized_id ~ '^[A-Za-z0-9_-]+$' then 'pending' else 'failed' end,
              case when normalized_id ~ '^[A-Za-z0-9_-]+$' then null else 'invalid_catalog_identifier' end
            from (
              select distinct case when trim(source_id) = '' then 'invalid:' || id::text
                when trim(source_id) ~* ('^https?://(www[.])?instagram[.]com/'
                  || '(p|reel|tv)/[A-Za-z0-9_-]+/?([?#].*)?$')
                  then substring(trim(source_id) from '/(?:p|reel|tv)/([A-Za-z0-9_-]+)')
                else trim(source_id) end as normalized_id
              from social.instagram_account_catalog_posts
              where lower(ltrim(trim(source_account), '@')) = %s
                and (%s::timestamptz is null or posted_at >= %s::timestamptz)
                and (%s::timestamptz is null or posted_at <= %s::timestamptz)
            ) scope on conflict (run_id, source_id) do nothing""",
            [
                run_id,
                account,
                json.dumps(policy),
                account,
                policy["date_start"],
                policy["date_start"],
                policy["date_end"],
                policy["date_end"],
            ],
        )
    manifests[account] = policy
    _one(
        """update social.scrape_runs set config = coalesce(config, '{}'::jsonb)
        || jsonb_build_object('detail_manifests', %s::jsonb) where id = %s::uuid returning id""",
        [json.dumps(manifests), run_id],
        conn=conn,
    )
    return policy


def snapshot(run_id: str, *, account: str | None = None, conn: Any = None) -> dict[str, Any]:
    return (
        _one(
            """select count(*)::int as total,
          count(*) filter (where state = 'committed')::int as committed,
          count(*) filter (where state = 'cached_satisfied')::int as cached_satisfied,
          count(*) filter (where state = 'source_unavailable')::int as source_unavailable,
          count(*) filter (where state not in ('committed','cached_satisfied','source_unavailable'))::int as unresolved,
          count(*) filter (where state = 'failed')::int as failed,
          count(*) filter (where state = 'retry_wait')::int as retry_wait,
          count(*) filter (where state not in ('committed','cached_satisfied','source_unavailable')
            and attempt_count < 3 and coalesce(error_code, '') <> 'invalid_catalog_identifier')::int as resumable,
          coalesce(sum(attempt_count), 0)::int as attempted,
          coalesce(sum(request_count), 0)::int as requests,
          min(next_attempt_at) filter (where state = 'retry_wait') as next_attempt_at,
          min(case when state = 'leased' then lease_expires_at
                   when state = 'retry_wait' then next_attempt_at
                   when state = 'pending' then now() end) as next_eligible_at,
          max(committed_at) as last_progress_at
        from social.instagram_detail_run_targets where run_id = %s::uuid
          and (%s::text is null or account_handle = %s)""",
            [run_id, account, account],
            conn=conn,
        )
        or {}
    )


def claim_target(run_id: str, account: str, job_id: str, token: str, worker_id: str) -> dict[str, Any] | None:
    with pg.db_connection(label="claim_instagram_detail_target") as conn:
        assert_owner(job_id, token, worker_id, conn=conn)
        return _one(
            """with candidate as (
              select run_id, source_id from social.instagram_detail_run_targets
              where run_id = %s::uuid and account_handle = %s and (
                state = 'pending' or (state = 'retry_wait' and next_attempt_at <= now())
                or (state = 'leased' and lease_expires_at <= now()))
              order by target_key for update skip locked limit 1
            ) update social.instagram_detail_run_targets t set state = 'leased',
              lease_owner = %s, lease_job_id = %s::uuid, lease_generation = lease_generation + 1,
              lease_expires_at = now() + interval '90 seconds'
            from candidate c where t.run_id = c.run_id and t.source_id = c.source_id
            returning t.*""",
            [run_id, account, token, job_id],
            conn=conn,
        )


def checkpoint(
    target: Mapping[str, Any],
    *,
    state: str,
    conn: Any,
    fetched_at: Any = None,
    error_code: str | None = None,
    next_attempt_at: Any = None,
    unavailable_evidence: Mapping[str, Any] | None = None,
) -> None:
    if state not in SATISFIED | {"pending", "retry_wait", "failed"}:
        raise ValueError("Invalid target outcome")
    if state == "cached_satisfied" and fetched_at is not None:
        raise ValueError("Cached satisfaction cannot manufacture a fetch timestamp")
    if state == "source_unavailable":
        evidence = unavailable_evidence or {}
        if not (
            evidence.get("authenticated") is True
            and evidence.get("transport_healthy") is True
            and evidence.get("source_id") == target["source_id"]
            and evidence.get("reason") in {"deleted", "private", "inaccessible"}
            and evidence.get("verified_at")
        ):
            raise ValueError("Source unavailability requires explicit healthy authenticated transport evidence")
        error_code = "source_" + str(evidence["reason"])
    row = _one(
        """with owner as materialized (
          select id from social.scrape_jobs where id = %s::uuid and detail_dispatch_token = %s
            and status = 'running' and detail_dispatch_expires_at > now() for update
        ) update social.instagram_detail_run_targets set state = %s, fetched_at = %s,
          committed_at = case when %s in ('committed','cached_satisfied','source_unavailable') then now() else null end,
          error_code = %s, error_summary = %s, next_attempt_at = %s, lease_expires_at = null
        where run_id = %s::uuid and source_id = %s and state = 'leased'
          and lease_owner = %s and lease_generation = %s and lease_expires_at > now()
          and exists (select 1 from owner)
        returning source_id""",
        [
            target["lease_job_id"],
            target["lease_owner"],
            state,
            fetched_at,
            state,
            error_code,
            json.dumps(
                {
                    key: unavailable_evidence.get(key)
                    for key in ("source_id", "reason", "verified_at", "authenticated", "transport_healthy")
                },
                default=str,
            )[:512]
            if unavailable_evidence
            else None,
            next_attempt_at,
            target["run_id"],
            target["source_id"],
            target["lease_owner"],
            target["lease_generation"],
        ],
        conn=conn,
    )
    if row is None:
        raise LostDetailOwnershipError(str(target["source_id"]))


def record_attempt(target: Mapping[str, Any], *, conn: Any, first_request: bool = True) -> bool:
    return bool(
        _one(
            """update social.instagram_detail_run_targets
        set attempt_count = attempt_count + %s, request_count = request_count + 1
        where run_id = %s::uuid and source_id = %s and state = 'leased'
          and lease_owner = %s and lease_generation = %s and lease_expires_at > now()
          and (not %s or attempt_count < 3) returning source_id""",
            [
                int(first_request),
                target["run_id"],
                target["source_id"],
                target["lease_owner"],
                target["lease_generation"],
                first_request,
            ],
            conn=conn,
        )
    )


def checkpoint_many(results: Sequence[Mapping[str, Any]], *, conn: Any) -> None:
    """Fence every buffered success before data writes in the same transaction.

    The caller holds assert_owner's dispatch lock. Any missing or stale target
    raises, so even the other rows updated by this statement must roll back.
    """
    if not results or len(results) > 50:
        raise ValueError("A detail checkpoint batch must contain 1 to 50 targets")
    values = []
    expected = set()
    owners = set()
    for result in results:
        target = result["target"]
        identity = (str(target["run_id"]), str(target["source_id"]))
        if identity in expected:
            raise ValueError("Duplicate durable detail target in checkpoint batch")
        expected.add(identity)
        owners.add((str(target["lease_job_id"]), str(target["lease_owner"])))
        state = result["state"]
        fetched_at = result.get("fetched_at")
        if state not in {"committed", "cached_satisfied"} or (state == "cached_satisfied" and fetched_at is not None):
            raise ValueError("Invalid buffered detail outcome")
        values.append(
            (
                *identity,
                str(target["lease_job_id"]),
                str(target["lease_owner"]),
                int(target["lease_generation"]),
                state,
                fetched_at,
            )
        )
    if len(owners) != 1:
        raise ValueError("A detail batch must belong to one dispatch")
    rows = pg.execute_values_returning(
        """update social.instagram_detail_run_targets t set
          state = v.state, fetched_at = v.fetched_at::timestamptz, committed_at = clock_timestamp(),
          error_code = null, error_summary = null, next_attempt_at = null, lease_expires_at = null
        from (values %s) v(run_id, source_id, job_id, owner, generation, state, fetched_at)
        where t.run_id = v.run_id::uuid and t.source_id = v.source_id
          and t.state = 'leased' and t.lease_job_id = v.job_id::uuid
          and t.lease_owner = v.owner and t.lease_generation = v.generation
          and t.lease_expires_at > clock_timestamp()
          and exists (select 1 from social.scrape_jobs j where j.id = v.job_id::uuid
            and j.detail_dispatch_token = v.owner and j.status = 'running'
            and j.detail_dispatch_expires_at > clock_timestamp())
        returning t.run_id::text, t.source_id""",
        values,
        conn=conn,
    )
    if len(rows) != len(expected) or {(row["run_id"], row["source_id"]) for row in rows} != expected:
        raise LostDetailOwnershipError("Buffered detail target lease changed")


def committed_outcomes(targets: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """Resolve an entire uncertain write batch in one generation-scoped query."""
    if not targets:
        return {}
    rows = pg.execute_values_returning(
        """select t.source_id, t.state from social.instagram_detail_run_targets t
        join (values %s) v(run_id, source_id, owner, generation, job_id)
          on t.run_id = v.run_id::uuid and t.source_id = v.source_id
          and t.lease_owner = v.owner and t.lease_generation = v.generation
          and t.lease_job_id = v.job_id::uuid""",
        [
            (
                str(t["run_id"]),
                str(t["source_id"]),
                str(t["lease_owner"]),
                int(t["lease_generation"]),
                str(t["lease_job_id"]),
            )
            for t in targets
        ],
    )
    return {row["source_id"]: row["state"] for row in rows if row["state"] in SATISFIED}


def committed_outcome(target: Mapping[str, Any]) -> str | None:
    """Resolve an ambiguous commit before replaying any persistence work."""
    row = _one(
        """select state from social.instagram_detail_run_targets
        where run_id = %s::uuid and source_id = %s and lease_generation = %s""",
        [target["run_id"], target["source_id"], target["lease_generation"]],
    )
    return str(row["state"]) if row and row["state"] in SATISFIED else None


def finish_chunk(
    legacy: Any,
    job: Mapping[str, Any],
    worker_id: str,
    *,
    error_code: str | None = None,
    cancelled: bool = False,
    stopped: bool = False,
) -> dict[str, Any]:
    job_id = str(job["id"])
    run_id = str(job["run_id"])
    token = str(job["detail_dispatch_token"])
    with pg.transaction() as conn:
        owner = assert_owner(job_id, token, worker_id, conn=conn)
        # Release only this generation's uncommitted work. Cancellation and a
        # short chunk do not manufacture target satisfaction.
        _one(
            """with released as (update social.instagram_detail_run_targets
              set state = 'pending', lease_expires_at = null
              where run_id = %s::uuid and state = 'leased' and lease_owner = %s returning 1)
            select count(*) from released""",
            [run_id, token],
            conn=conn,
        )
        totals = snapshot(
            run_id,
            account=(job.get("config") or {}).get("detail_manifest", {}).get("account"),
            conn=conn,
        )
        job_totals = _one(
            """select count(*)::int as committed from social.instagram_detail_run_targets
            where run_id = %s::uuid and lease_job_id = %s::uuid and state = 'committed'""",
            [run_id, job_id],
            conn=conn,
        ) or {"committed": 0}
        retryable_count = int(totals["unresolved"]) - int(totals["failed"])
        status = (
            "cancelled"
            if cancelled
            else "failed"
            if stopped
            else ("completed" if not totals["unresolved"] else "retrying" if retryable_count else "failed")
        )
        receipt = {
            "version": VERSION,
            "run_id": run_id,
            "job_id": job_id,
            "dispatch_generation": owner["detail_dispatch_generation"],
            "worker_id": worker_id,
            "claimed": True,
            "manifest": (job.get("config") or {}).get("detail_manifest"),
            "outcomes": totals,
            "status": status,
            "completion_kind": (
                "completed_with_unavailable"
                if status == "completed" and totals["source_unavailable"]
                else "no_work"
                if status == "completed" and not totals["total"]
                else status
            ),
        }
        encoded = json.dumps(receipt, default=str)
        _one(
            """update social.scrape_jobs set detail_completion_receipt = %s::jsonb,
              detail_dispatch_expires_at = null
            where id = %s::uuid and detail_dispatch_token = %s returning id""",
            [encoded, job_id, token],
            conn=conn,
        )
        # Keep the existing stage accounting/rollup transition in this same
        # transaction. items_found is unique committed targets, never attempts.
        legacy._finish_job(
            job_id,
            status=status,
            items_found=int(job_totals["committed"]),
            metadata={
                "detail_outcomes": json.loads(json.dumps(totals, default=str)),
                "detail_completion_receipt": json.loads(encoded),
                "stage": (job.get("config") or {}).get("stage"),
                "error_code": error_code,
                "resume_eligible": bool(totals["unresolved"]),
            },
            expected_worker_id=worker_id,
            expected_dispatch_token=token,
            defer_followups=True,
            next_available_at=max(
                datetime.now(UTC) + timedelta(seconds=5),
                totals.get("next_eligible_at") or datetime.now(UTC),
            )
            if status == "retrying"
            else None,
            last_error_code=error_code,
        )
    legacy._clear_worker_heartbeat_for_job(
        job_id=job_id,
        status="idle",
        metadata={"source": "detail_chunk_finish", "job_status": status},
    )
    legacy._invalidate_queue_status_cache()
    return receipt


def execute_job(legacy: Any, job: Mapping[str, Any], *, worker_id: str) -> dict[str, Any]:
    from trr_backend.socials.instagram.catalog_ingest import run_durable_instagram_details

    error_code = None
    cancelled = False
    stopped = False
    try:
        run_durable_instagram_details(job, worker_id=worker_id)
    except LostDetailOwnershipError:
        # No write, receipt or completion by a superseded worker.
        return legacy._load_current_job_row(str(job["id"]))
    except Exception as exc:
        stopped = True
        error_code = str(getattr(exc, "error_code", "") or "detail_worker_failed")
        cancelled = error_code in {
            legacy.SHARED_ACCOUNT_STAGE_CANCELLED_ERROR_CODE,
            legacy.SHARED_ACCOUNT_POSTS_CANCELLED_ERROR_CODE,
        }
    try:
        finish_chunk(legacy, job, worker_id, error_code=error_code, cancelled=cancelled, stopped=stopped)
    except LostDetailOwnershipError:
        return legacy._load_current_job_row(str(job["id"]))
    legacy._finalize_run_status(str(job["run_id"]))
    return legacy._load_current_job_row(str(job["id"]))


def recover_expired(
    legacy: Any, *, run_id: str | None = None, account: str | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    recovered = []
    with pg.transaction() as conn:
        with pg.db_cursor(conn=conn, label="recover_expired_detail_dispatch") as cur:
            cur.execute(
                """select id::text, run_id::text, worker_id, detail_dispatch_token, items_found
                from social.scrape_jobs
                where status = 'running' and config->>'detail_contract_version' = '1'
                  and detail_dispatch_expires_at < now()
                  and coalesce(heartbeat_at, claimed_at, started_at) < now() - interval '60 seconds'
                  and (%s::uuid is null or run_id = %s::uuid)
                  and (%s::text is null or config->'detail_manifest'->>'account' = %s)
                order by detail_dispatch_expires_at for update skip locked limit %s""",
                [run_id, run_id, account, account, min(250, max(1, limit))],
            )
            rows = list(cur.fetchall())
        for row in rows:
            totals = snapshot(row["run_id"], conn=conn)
            legacy._finish_job(
                row["id"],
                status="retrying",
                items_found=int(row.get("items_found") or 0),
                expected_worker_id=row["worker_id"],
                expected_dispatch_token=row["detail_dispatch_token"],
                defer_followups=True,
                metadata={
                    "detail_outcomes": json.loads(json.dumps(totals, default=str)),
                    "resume_eligible": True,
                    "error_code": "detail_dispatch_expired",
                },
                next_available_at=datetime.now(UTC) + timedelta(seconds=5),
                last_error_code="detail_dispatch_expired",
            )
            _one(
                """update social.scrape_jobs set detail_dispatch_token = null,
                  detail_dispatch_generation = detail_dispatch_generation + 1,
                  detail_dispatch_expires_at = null
                where id = %s::uuid and detail_dispatch_token = %s returning id""",
                [row["id"], row["detail_dispatch_token"]],
                conn=conn,
            )
            recovered.append({"id": row["id"], "run_id": row["run_id"], "status": "retrying"})
    for recovered_run in {row["run_id"] for row in recovered}:
        legacy._finalize_run_status(recovered_run, force_recompute=True)
    return recovered


def reconcile_provider_failure(legacy: Any, row: Mapping[str, Any], inspection: Mapping[str, Any]) -> bool:
    status = str(inspection.get("status") or "").lower()
    if status not in {"failed", "timeout", "timed_out", "cancelled", "terminated"}:
        return False
    token = row.get("detail_dispatch_token")
    if not token:
        return False
    with pg.transaction() as conn:
        current = _one(
            """select id, worker_id, items_found from social.scrape_jobs
            where id = %s::uuid and status = 'running' and detail_dispatch_token = %s
              and metadata->'dispatch'->>'remote_invocation_id' = %s for update""",
            [row["id"], token, (row.get("metadata") or {}).get("dispatch", {}).get("remote_invocation_id")],
            conn=conn,
        )
        if not current:
            return False
        legacy._finish_job(
            str(row["id"]),
            status="failed",
            items_found=int(current.get("items_found") or 0),
            expected_worker_id=current["worker_id"],
            expected_dispatch_token=token,
            defer_followups=True,
            last_error_code="detail_provider_" + status,
            metadata={"resume_eligible": True, "detail_provider_terminal_status": status},
        )
    legacy._finalize_run_status(str(row["run_id"]), force_recompute=True)
    return True


def resume_manifest(
    *, run_id: str, account_handle: str, manifest_identity: str, source_scope: str = "network"
) -> dict[str, Any]:
    """Resume only eligible unresolved targets in the caller's frozen manifest.

    The API adapter owns admin authentication. This primitive verifies the exact
    run/account/scope/manifest binding and never resets the three-attempt cap.
    """
    account = account_handle.strip().lstrip("@").lower()
    with pg.transaction() as conn:
        run = _one("select config from social.scrape_runs where id = %s::uuid", [run_id], conn=conn)
        manifest = ((run or {}).get("config") or {}).get("detail_manifests", {}).get(account)
        if not manifest or manifest.get("identity") != manifest_identity:
            raise ValueError("Post Details manifest identity does not match this account and run")
        with pg.db_cursor(conn=conn, label="resume_detail_jobs") as cur:
            cur.execute(
                """select id::text, status from social.scrape_jobs
                where run_id = %s::uuid and config->>'detail_contract_version' = '1'
                  and config->'detail_manifest'->>'identity' = %s
                  and coalesce(config->>'source_scope', 'network') = %s
                for update""",
                [run_id, manifest_identity, source_scope],
            )
            jobs = list(cur.fetchall())
        if not jobs:
            raise ValueError("No matching Post Details jobs exist for this scope")
        if any(job["status"] in {"running", "queued", "pending", "retrying", "cancelling"} for job in jobs):
            raise ValueError("Post Details already has active work; its existing continuation retains ownership")
        _one("select id from social.scrape_runs where id = %s::uuid for update", [run_id], conn=conn)
        job_ids = [job["id"] for job in jobs]
        # Fence old calls before making any target eligible again.
        _one(
            """with fenced as (update social.scrape_jobs set detail_dispatch_token = null,
              detail_dispatch_expires_at = null, detail_dispatch_generation = detail_dispatch_generation + 1
              where id = any(%s::uuid[]) returning id) select count(*) from fenced""",
            [job_ids],
            conn=conn,
        )
        _one(
            """with released as (update social.instagram_detail_run_targets
              set state = 'pending', lease_expires_at = null
              where run_id = %s::uuid and account_handle = %s and attempt_count < 3
                and state in ('failed','leased') and coalesce(error_code, '') <> 'invalid_catalog_identifier'
              returning 1) select count(*) from released""",
            [run_id, account],
            conn=conn,
        )
        totals = snapshot(run_id, account=account, conn=conn)
        eligible = int(totals["unresolved"]) - int(totals["failed"])
        if eligible:
            _one(
                """with resumed as (update social.scrape_jobs set status = 'retrying', worker_id = null,
                  claimed_at = null, completed_at = null, detail_completion_receipt = null,
                  available_at = %s, metadata = coalesce(metadata, '{}'::jsonb) - 'dispatch'
                  where id = any(%s::uuid[]) returning id) select count(*) from resumed""",
                [totals.get("next_eligible_at") or datetime.now(UTC), job_ids],
                conn=conn,
            )
            _one(
                """update social.scrape_runs set status = 'queued', completed_at = null,
                  cancelled_at = null where id = %s::uuid returning id""",
                [run_id],
                conn=conn,
            )
    return {
        "run_id": run_id,
        "manifest_identity": manifest_identity,
        "resumed": bool(eligible),
        "detail_contract_version": VERSION,
        "detail_outcomes": totals,
        "reason": None if eligible else "no_eligible_unresolved_targets",
    }
