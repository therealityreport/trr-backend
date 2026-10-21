"""Durable Reddit capture contract. No legacy data is enrolled by reads or migrations."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from datetime import UTC, datetime, timedelta
from typing import Any

from trr_backend.db import pg

logger = logging.getLogger(__name__)
SATISFIED = frozenset({"committed", "source_unavailable"})
STATES = frozenset({"pending", "running", "retry_wait", "committed", "source_unavailable", "failed", "cancelled"})
CURRENT: ContextVar[Execution | None] = ContextVar("reddit_capture_execution", default=None)


class LostClaim(RuntimeError):  # noqa: N818 - public control-flow exception
    pass


class RunPaused(LostClaim):
    pass


class RunCancelled(LostClaim):
    pass


class IdentityBlockedError(RuntimeError):
    pass


def enabled() -> bool:
    return os.getenv("REDDIT_PIPELINE_V2_ENABLED", "").lower() in {"1", "true", "yes"}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def _one(sql: str, params: Any = None, *, conn: Any = None) -> dict | None:
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def manifest_for_run(run_id: str) -> dict | None:
    return pg.fetch_one("select * from social.reddit_run_manifests where run_id=%s::uuid", [run_id])


def normalize_policy(payload: dict) -> dict:
    result = dict(payload)
    supplied = result.get("refresh_policy")
    force = result.get("force_rescrape") is True
    if force and supplied not in (None, "force"):
        raise ValueError("force_rescrape conflicts with refresh_policy")
    policy = "force" if force else str(supplied or "missing")
    if policy not in {"missing", "stale", "force"}:
        raise ValueError("Invalid refresh_policy")
    result["refresh_policy"] = policy
    result["force_rescrape"] = policy == "force"
    phases = result.get("requested_phases")
    if phases is None:
        mode = result.get("mode", "sync_posts")
        phases = ["comments", "media"] if mode == "sync_details" else ["discovery"]
        if mode == "sync_full":
            phases += ["comments", "media"]
        elif result.get("fetch_comments") and "comments" not in phases:
            phases += ["comments"]
    if not phases or any(x not in {"discovery", "comments", "media"} for x in phases):
        raise ValueError("requested_phases must select discovery, comments or media")
    result["requested_phases"] = sorted(set(phases))
    selected = result.get("selected_post_ids") or []
    if any(not isinstance(x, str) or not x.isalnum() or len(x) > 16 for x in selected):
        raise ValueError("Invalid selected_post_ids")
    result["selected_post_ids"] = sorted(set(selected))
    result["stale_after_seconds"] = max(60, min(86400, int(result.get("stale_after_seconds") or 86400)))
    result["matching_version"] = 1
    return result


def configuration_hash(payload: dict) -> str:
    # Exclude request telemetry and identity of the attempt, never behavior inputs.
    excluded = {"run_config_hash", "client_session_id", "client_workflow_id", "parent_run_id", "priority"}
    return hashlib.sha256(
        _json({k: v for k, v in normalize_policy(payload).items() if k not in excluded}).encode()
    ).hexdigest()


def create_run(payload: dict) -> dict:
    payload = normalize_policy(payload)
    payload["capture_contract_version"] = 2
    digest = configuration_hash(payload)
    payload["run_config_hash"] = digest
    scope = ":".join(str(payload[k]) for k in ("community_id", "season_id", "period_key"))
    with pg.transaction() as conn:
        _one("select pg_advisory_xact_lock(hashtextextended(%s,0))", [f"reddit-create:{scope}"], conn=conn)
        existing = _one(
            """select * from social.reddit_refresh_runs
            where community_id=%s and season_id=%s and period_key=%s and config_hash=%s
              and contract_version=2 and status in ('queued','running') order by created_at limit 1""",
            [payload["community_id"], payload["season_id"], payload["period_key"], digest],
            conn=conn,
        )
        if existing:
            return {**existing, "reused": True}
        row = _one(
            """insert into social.reddit_refresh_runs
(community_id,season_id,period_key,subreddit,status,request_payload,contract_version,config_hash,parent_run_id,priority)
            values (%s,%s,%s,%s,'queued',%s::jsonb,2,%s,%s::uuid,%s) returning *""",
            [
                payload["community_id"],
                payload["season_id"],
                payload["period_key"],
                payload["subreddit"],
                _json(payload),
                digest,
                payload.get("parent_run_id"),
                int(payload.get("priority", 10)),
            ],
            conn=conn,
        )
        if not row:
            raise RuntimeError("Run insert returned no row")
        _one(
            """insert into social.reddit_run_manifests(run_id,configuration,requested_phases)
            values (%s,%s::jsonb,%s::jsonb) returning run_id""",
            [row["id"], _json(payload), _json(payload["requested_phases"])],
            conn=conn,
        )
        _one("insert into social.reddit_dispatch_outbox(run_id) values (%s) returning run_id", [row["id"]], conn=conn)
        return {**row, "reused": False}


def claim_run(run_id: str, worker_id: str) -> dict | None:
    with pg.transaction() as conn:
        scope = _one(
            "select community_id,season_id,period_key from social.reddit_refresh_runs where id=%s", [run_id], conn=conn
        )
        if not scope:
            raise ValueError("Refresh run not found")
        _one(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))",
            ["reddit-claim:" + ":".join(str(scope[k]) for k in ("community_id", "season_id", "period_key"))],
            conn=conn,
        )
        return _one(
            """update social.reddit_refresh_runs r set status='running',claim_token=gen_random_uuid()::text,
            claimed_by_worker_id=%s,lease_expires_at=now()+interval '5 minutes',heartbeat_at=now(),
            started_at=coalesce(started_at,now()),attempt_count=coalesce(attempt_count,0)+1,updated_at=now()
            where r.id=%s and r.contract_version=2 and r.control_state='active'
            and coalesce(r.next_retry_at,now())<=now()
            and (r.status='queued' or (r.status='running' and r.lease_expires_at<now()))
            and not exists(select 1 from social.reddit_refresh_runs other where other.id<>r.id
                and other.community_id=r.community_id and other.season_id=r.season_id
                and other.period_key=r.period_key and other.status='running' and other.lease_expires_at>now())
            returning r.*""",
            [worker_id, run_id],
            conn=conn,
        )


def dispatch_due(dispatcher: Any, limit: int = 25) -> list[str]:
    """Claim outbox deliveries; expired dispatch leases are safe to deliver again."""
    rows = pg.fetch_all(
        """select o.run_id from social.reddit_dispatch_outbox o
        join social.reddit_refresh_runs r on r.id=o.run_id
        where r.control_state='active' and r.status in ('queued','running')
        and coalesce(r.next_retry_at,now())<=now() and o.next_attempt_at<=now()
        and (o.state='pending' or o.updated_at<now()-interval '5 minutes')
        order by r.priority,r.created_at limit %s""",
        [limit],
    )
    delivered = []
    for row in rows:
        run_id = str(row["run_id"])
        token = str(uuid.uuid4())
        claimed = pg.execute_returning(
            """update social.reddit_dispatch_outbox set state='dispatching',
            dispatch_token=%s,attempt_count=attempt_count+1,updated_at=now()
            where run_id=%s and (state='pending' or updated_at<now()-interval '5 minutes') returning run_id""",
            [token, run_id],
        )
        if not claimed:
            continue
        try:
            if not dispatcher(run_id):
                raise RuntimeError("Dispatch did not acknowledge delivery")
            pg.execute_returning(
                """update social.reddit_dispatch_outbox set
state='dispatched',dispatched_at=now(),updated_at=now() where run_id=%s and dispatch_token=%s
returning run_id""",
                [run_id, token],
            )
            delivered.append(run_id)
        except Exception as exc:
            pg.execute_returning(
                """update social.reddit_dispatch_outbox set
state='pending',error=%s,next_attempt_at=now()+interval '1 minute',updated_at=now() where
run_id=%s and dispatch_token=%s returning run_id""",
                [str(exc), run_id, token],
            )
    return delivered


def assert_owner(run_id: str, claim_token: str, conn: Any = None) -> dict:
    row = _one(
        """select status,control_state,claim_token,lease_expires_at>now() as live
        from social.reddit_refresh_runs where id=%s::uuid for update""",
        [run_id],
        conn=conn,
    )
    if not row or row.get("claim_token") != claim_token or row.get("status") != "running" or not row.get("live"):
        raise LostClaim("Reddit execution ownership expired or changed")
    if row["control_state"] == "paused":
        raise RunPaused("Run paused")
    if row["control_state"] in {"cancel_requested", "cancelled"}:
        raise RunCancelled("Run cancelled")
    return row


def ensure_target(
    run_id: str,
    kind: str,
    target_key: str,
    post_id: str | None = None,
    checkpoint: dict | None = None,
    conn: Any = None,
    required: bool = True,
) -> dict:
    execution = CURRENT.get()
    if conn is None and execution:
        with execution.transaction() as active:
            return ensure_target(run_id, kind, target_key, post_id, checkpoint, active, required=required)
    row = _one(
        """insert into social.reddit_work_targets(run_id,kind,target_key,reddit_post_id,checkpoint,required)
        values (%s::uuid,%s,%s,%s,%s::jsonb,%s) on conflict(run_id,kind,target_key)
        do update set target_key=excluded.target_key returning *""",
        [run_id, kind, target_key, post_id, _json(checkpoint or {}), required],
        conn=conn,
    )
    if not row:
        raise RuntimeError("Target insert returned no row")
    return row


def start_target(run_id: str, kind: str, target_key: str, claim_token: str) -> dict | None:
    with pg.transaction() as conn:
        assert_owner(run_id, claim_token, conn=conn)
        exhausted = _one(
            """update social.reddit_work_targets set state='failed',next_retry_at=null,
            error=coalesce(error,'Attempt budget exhausted after interrupted worker'),updated_at=now()
            where run_id=%s and kind=%s and target_key=%s and attempt_count>=5
            and state in ('pending','retry_wait','running') returning *""",
            [run_id, kind, target_key],
            conn=conn,
        )
        if exhausted and kind in {"comments", "media"} and exhausted.get("reddit_post_id") == target_key:
            _one(
                """update social.reddit_capture_state set state='failed',next_retry_at=null,
                evidence=evidence||%s::jsonb,updated_at=now()
                where reddit_post_id=%s and phase=%s and last_run_id=%s returning reddit_post_id""",
                [_json({"error": exhausted["error"]}), target_key, kind, run_id],
                conn=conn,
            )
        return _one(
            """update social.reddit_work_targets set state='running',attempt_count=attempt_count+1,
            attempted_at=now(),updated_at=now(),error=null where run_id=%s::uuid and kind=%s and target_key=%s
            and state in ('pending','retry_wait','running') and attempt_count<5
            and (next_retry_at is null or next_retry_at<=now()) returning *""",
            [run_id, kind, target_key],
            conn=conn,
        )


def finish_target(
    run_id: str,
    kind: str,
    target_key: str,
    claim_token: str,
    state: str,
    result: dict | None = None,
    checkpoint: dict | None = None,
    error: str | None = None,
    retry_after: float | None = None,
    conn: Any = None,
) -> None:
    if state not in STATES:
        raise ValueError("Invalid target outcome")
    with pg.transaction() if conn is None else _borrow(conn) as active:
        assert_owner(run_id, claim_token, conn=active)
        row = _one(
            "select * from social.reddit_work_targets where run_id=%s and kind=%s and target_key=%s for update",
            [run_id, kind, target_key],
            conn=active,
        )
        if row is None:
            raise ValueError("Target not found")
        if state == "source_unavailable" and not (result or {}).get("evidence"):
            raise ValueError("Source unavailable requires evidence")
        delay = None
        if state == "retry_wait":
            if int(row["attempt_count"]) >= 5:
                state = "failed"
            else:
                delay = max(
                    float(retry_after or 0),
                    min(3600, 30 * 2 ** max(0, int(row["attempt_count"]) - 1)) + random.random() * 5,
                )
        _one(
            """update social.reddit_work_targets set state=%s,result=%s::jsonb,
            checkpoint=coalesce(%s::jsonb,checkpoint),error=%s,
            next_retry_at=case when %s::float is null then null else now()+(%s * interval '1 second') end,
            committed_at=case when %s='committed' then now() else committed_at end,updated_at=now()
            where id=%s returning id""",
            [
                state,
                _json(result or {}),
                _json(checkpoint) if checkpoint is not None else None,
                error,
                delay,
                delay,
                state,
                row["id"],
            ],
            conn=active,
        )

        if kind in {"comments", "media"} and row.get("reddit_post_id") == target_key:
            _one(
                """update social.reddit_capture_state set state=%s,
                next_retry_at=case when %s::float is null then null else now()+(%s*interval '1 second') end,
                evidence=evidence||%s::jsonb,updated_at=now()
                where reddit_post_id=%s and phase=%s and last_run_id=%s returning reddit_post_id""",
                [state, delay, delay, _json({"error": error} if error else {}), target_key, kind, run_id],
                conn=active,
            )


@contextmanager
def _borrow(conn: Any):
    yield conn


def capture_due(post_ids: list[str], phase: str, policy: str = "missing", stale_after_seconds: int = 86400) -> set[str]:
    if not post_ids:
        return set()
    if policy == "force":
        return set(post_ids)
    rows = pg.fetch_all(
        """select reddit_post_id,state,successful_at,
        successful_at >= now()-(%s*interval '1 second') as fresh from social.reddit_capture_state
        where reddit_post_id=any(%s::text[]) and phase=%s""",
        [stale_after_seconds, post_ids, phase],
    )
    satisfied = {
        r["reddit_post_id"] for r in rows if r["state"] in SATISFIED and (policy == "missing" or r.get("fresh"))
    }
    return set(post_ids) - satisfied


def record_capture(
    post_id: str,
    phase: str,
    state: str,
    run_id: str,
    source_revision: str | None = None,
    evidence: dict | None = None,
    conn: Any = None,
) -> None:
    if state not in STATES:
        raise ValueError("Invalid capture state")
    _one(
        """insert into social.reddit_capture_state(reddit_post_id,phase,state,last_run_id,source_revision,
        evidence,attempted_at,successful_at) values (%s,%s,%s,%s,%s,%s::jsonb,now(),
        case when %s in ('committed','source_unavailable') then now() end)
        on conflict(reddit_post_id,phase) do update set
state=excluded.state,last_run_id=excluded.last_run_id,
source_revision=coalesce(excluded.source_revision,reddit_capture_state.source_revision),
evidence=excluded.evidence,attempted_at=now(),successful_at=coalesce(excluded.successful_at,reddit_capture_state.successful_at),
        updated_at=now() returning reddit_post_id""",
        [post_id, phase, state, run_id, source_revision, _json(evidence or {}), state],
        conn=conn,
    )


def list_targets(run_id: str, limit: int = 100, offset: int = 0) -> list[dict]:
    return pg.fetch_all(
        """select * from social.reddit_work_targets where run_id=%s::uuid
        order by kind,target_key limit %s offset %s""",
        [run_id, max(1, min(limit, 500)), max(0, offset)],
    )


def summarize_targets(rows: list[dict]) -> dict:
    phases: dict[str, dict] = {}
    total = Counter()
    for row in rows:
        phase = phases.setdefault(row["kind"], {"requested": 0, "satisfied": 0, "unresolved": 0, "states": {}})
        count = int(row.get("count", 1))
        state = row["state"]
        phase["states"][state] = phase["states"].get(state, 0) + count
        phase["requested"] += count
        key = "satisfied" if state in SATISFIED else "unresolved"
        phase[key] += count
        total["requested"] += count
        total[key] += count
    return {
        "requested": total["requested"],
        "satisfied": total["satisfied"],
        "unresolved": total["unresolved"],
        "outcomes": phases,
    }


def run_contract(run_id: str) -> dict:
    manifest = manifest_for_run(run_id)
    if not manifest:
        return {"contract_version": 1, "capture_status": "unknown", "coverage": {"state": "unknown"}}
    run = pg.fetch_one("select control_state from social.reddit_refresh_runs where id=%s", [run_id]) or {}
    rows = pg.fetch_all(
        """select kind,state,count(*)::int as count from social.reddit_work_targets
        where run_id=%s and required group by kind,state""",
        [run_id],
    )
    retry = (
        pg.fetch_one("select min(next_retry_at) as retry_at from social.reddit_work_targets where run_id=%s", [run_id])
        or {}
    )
    from trr_backend.repositories.reddit_contract import RedditRunCaptureContract

    return RedditRunCaptureContract.model_validate(
        {
            "contract_version": 2,
            "control_state": run.get("control_state", "active"),
            **summarize_targets(rows),
            "coverage": manifest.get("coverage") or {"state": "unknown"},
            **retry,
            "requested_phases": manifest["requested_phases"],
        }
    ).model_dump(mode="json")


def terminal_status(contract: dict, useful: bool) -> str:
    if contract.get("control_state") in {"cancelled", "cancel_requested"}:
        return "cancelled"
    if contract.get("unresolved", 0) or contract.get("coverage", {}).get("state") in {"partial", "unknown"}:
        return "partial" if useful else "failed"
    return "completed"


def control_run(run_id: str, action: str) -> dict:
    if action not in {"pause", "resume", "cancel", "retry"}:
        raise ValueError("Unknown run control")
    with pg.transaction() as conn:
        run = _one("select * from social.reddit_refresh_runs where id=%s::uuid for update", [run_id], conn=conn)
        if not run:
            raise ValueError("Run not found")
        if run.get("contract_version") != 2:
            raise ValueError("Legacy runs cannot be controlled; start an explicit new refresh")
        if action == "retry":
            if run["status"] in {"queued", "running"}:
                raise ValueError("Active run must finish or be cancelled before retry")
            payload = {
                **run["request_payload"],
                "parent_run_id": run_id,
                "refresh_policy": "missing",
                "force_rescrape": False,
            }
            # Retain only unresolved target selection; completed phases remain cache-satisfied.
            ids = pg.fetch_all(
                """select distinct reddit_post_id from social.reddit_work_targets where run_id=%s and state not in
('committed','source_unavailable') and reddit_post_id is not null""",
                [run_id],
            )
            if ids:
                payload["selected_post_ids"] = [r["reddit_post_id"] for r in ids]
            child = create_run(payload)
            if not child.get("reused"):
                # Transfer evidence, not the parent's outcome. Only unresolved targets get fresh attempts.
                _one(
                    """insert into social.reddit_work_targets
                    (run_id,kind,target_key,reddit_post_id,state,required,checkpoint,result,committed_at)
                    select %s,kind,target_key,reddit_post_id,
                      case when state in ('committed','source_unavailable') then state else 'pending' end,
                      required,checkpoint,result,committed_at from social.reddit_work_targets where run_id=%s
                    on conflict do nothing returning id""",
                    [child["id"], run_id],
                    conn=conn,
                )
                _one(
                    """update social.reddit_run_manifests child set coverage=parent.coverage
                    from social.reddit_run_manifests parent where child.run_id=%s and parent.run_id=%s
                    returning child.run_id""",
                    [child["id"], run_id],
                    conn=conn,
                )
            return child
        if action == "resume" and run["control_state"] == "paused":
            if run.get("claim_token") and run.get("lease_expires_at") and run["lease_expires_at"] > datetime.now(UTC):
                raise ValueError("Pause is checkpointing; retry resume after ownership releases")
            _one(
                """update social.reddit_refresh_runs set control_state='active',status='queued',completed_at=null
where id=%s returning id""",
                [run_id],
                conn=conn,
            )
            _one(
                """insert into social.reddit_dispatch_outbox(run_id) values(%s) on conflict(run_id) do update set
state='pending',next_attempt_at=now() returning run_id""",
                [run_id],
                conn=conn,
            )
        elif action == "pause" and run["status"] in {"queued", "running"}:
            _one(
                "update social.reddit_refresh_runs set control_state='paused' where id=%s returning id",
                [run_id],
                conn=conn,
            )
        elif action == "cancel" and run["status"] in {"queued", "running"}:
            _one(
                """update social.reddit_refresh_runs set
control_state='cancelled',status='cancelled',completed_at=now(),claim_token=null,lease_expires_at=null
where id=%s returning id""",
                [run_id],
                conn=conn,
            )
            _one(
                """update social.reddit_work_targets set state='cancelled',updated_at=now() where run_id=%s and
state not in ('committed','source_unavailable') returning id""",
                [run_id],
                conn=conn,
            )
            _one(
                "update social.reddit_dispatch_outbox set state='done' where run_id=%s returning run_id",
                [run_id],
                conn=conn,
            )
        elif action == "resume" and run["control_state"] == "active" and run["status"] in {"queued", "running"}:
            pass  # Idempotent resume never dispatches a duplicate.
        else:
            raise ValueError("Control action is not valid in the current run state")
    return run_contract(run_id)


class Execution:
    def __init__(self, run: dict):
        self.run_id = str(run["id"])
        self.claim_token = str(run["claim_token"])
        self.stop = threading.Event()
        self.failure: BaseException | None = None
        self.thread = threading.Thread(target=self._heartbeat, name="reddit-lease", daemon=True)

    def check(self) -> None:
        if self.failure:
            raise self.failure
        assert_owner(self.run_id, self.claim_token)

    def _heartbeat(self) -> None:
        while not self.stop.wait(30):
            try:
                with pg.transaction() as conn:
                    assert_owner(self.run_id, self.claim_token, conn=conn)
                    row = _one(
                        """update social.reddit_refresh_runs set heartbeat_at=now(),
                        lease_expires_at=now()+interval '5 minutes',updated_at=now()
                        where id=%s and claim_token=%s returning id""",
                        [self.run_id, self.claim_token],
                        conn=conn,
                    )
                    if not row:
                        raise LostClaim("Heartbeat lost ownership")
            except BaseException as exc:
                self.failure = exc
                return

    @contextmanager
    def transaction(self):
        if self.failure:
            raise self.failure
        with pg.transaction() as conn:
            assert_owner(self.run_id, self.claim_token, conn=conn)
            yield conn

    def __enter__(self):
        self.check()
        self.token = CURRENT.set(self)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=35)
        CURRENT.reset(self.token)


def submit(pool: Any, fn: Any, *args: Any, **kwargs: Any):
    return pool.submit(copy_context().run, fn, *args, **kwargs)


@contextmanager
def persistence():
    execution = CURRENT.get()
    with execution.transaction() if execution else pg.db_connection() as conn:
        yield conn


@contextmanager
def request_guard(identity: str, lane: str = "request", *, allow_blocked: bool = False):
    execution = CURRENT.get()
    lease = str(uuid.uuid4())
    while True:
        if execution:
            execution.check()
        wait = 1.0
        with pg.transaction() as conn:
            _one(
                "insert into social.reddit_request_budgets(identity_key) values(%s) "
                "on conflict do nothing returning identity_key",
                [identity],
                conn=conn,
            )
            budget = _one(
                "select *,now() as clock from social.reddit_request_budgets where identity_key=%s for update",
                [identity],
                conn=conn,
            )
            if not budget:
                raise RuntimeError("Request budget not found")
            if budget["blocked"] and not allow_blocked:
                raise IdentityBlockedError("Reddit OAuth identity blocked; verify authentication before resuming")
            now = budget["clock"]
            _one(
                "delete from social.reddit_request_leases where identity_key=%s and expires_at<=now() returning id",
                [identity],
                conn=conn,
            )
            active = (
                _one(
                    """select count(*) as total,count(*) filter(where lane='morechildren') as more from
social.reddit_request_leases where identity_key=%s""",
                    [identity],
                    conn=conn,
                )
                or {}
            )
            if now - budget["window_started_at"] >= timedelta(minutes=1):
                budget["used"] = 0
                _one(
                    """update social.reddit_request_budgets set used=0,window_started_at=now() where identity_key=%s
returning identity_key""",
                    [identity],
                    conn=conn,
                )
            provider_exhausted = (
                budget.get("provider_remaining") is not None
                and budget["provider_remaining"] <= 0
                and budget.get("provider_reset_at")
                and budget["provider_reset_at"] > now
            )
            cooling = budget.get("not_before") and budget["not_before"] > now
            if (
                not cooling
                and not provider_exhausted
                and budget["used"] < 60
                and active.get("total", 0) < 4
                and (lane != "morechildren" or not active.get("more"))
            ):
                # HTTP timeout is bounded below this lease. No DB connection is held across the request.
                _one(
                    """insert into social.reddit_request_leases(id,identity_key,lane,expires_at)
values(%s,%s,%s,now()+interval '5 minutes') returning id""",
                    [lease, identity, lane],
                    conn=conn,
                )
                _one(
                    """update social.reddit_request_budgets set
used=used+1,provider_remaining=greatest(0,provider_remaining-1),updated_at=now() where
identity_key=%s returning identity_key""",
                    [identity],
                    conn=conn,
                )
                break
            if cooling:
                wait = min(5, max(0.1, (budget["not_before"] - now).total_seconds()))
        time.sleep(wait)
    try:
        if execution:
            execution.check()
        yield
    finally:
        pg.execute_returning("delete from social.reddit_request_leases where id=%s returning id", [lease])


def observe_response(identity: str, status: int, headers: Any) -> None:
    lower = {str(k).lower(): v for k, v in headers.items()}

    def number(key: str) -> float | None:
        try:
            return max(0, float(lower[key]))
        except (KeyError, ValueError, TypeError):
            return None

    cooldown = number("retry-after") if status == 429 else None
    if status == 429 and cooldown is None:
        cooldown = number("x-ratelimit-reset") or 60
    pg.execute_returning(
        """update social.reddit_request_budgets set
        provider_remaining=coalesce(%s,provider_remaining),
        provider_reset_at=case when %s::float is null then provider_reset_at else
now()+(%s*interval '1 second') end,
        not_before=case when %s::float is null then not_before else
greatest(not_before,now()+(%s*interval '1 second')) end,
        updated_at=now() where identity_key=%s returning identity_key""",
        [
            number("x-ratelimit-remaining"),
            number("x-ratelimit-reset"),
            number("x-ratelimit-reset"),
            cooldown,
            cooldown,
            identity,
        ],
    )
