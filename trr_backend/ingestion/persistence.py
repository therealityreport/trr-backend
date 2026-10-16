"""Transactional business state; no engine, HTTP, Modal, or legacy namespace imports.

The transaction factory is injectable for isolated PostgreSQL tests. Production
uses the existing bounded connection pool. Never hold these transactions during
provider requests or remote dispatch. Canonical writers passed to commit_target
must use the supplied connection and perform only database work.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import nullcontext
from datetime import datetime
from typing import Any
from uuid import uuid4

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import (
    Conflict,
    Failure,
    LostOwnership,
    Outcome,
    RunIntent,
    Stage,
    fingerprint,
    stable_json,
    utc_timestamp,
)
from trr_backend.ingestion.evidence import Evidence


def _transaction():
    from trr_backend.db import pg

    return pg.db_connection(label="instagram-ingestion", pool_name="social_control")


def _row(cursor, query: str, params: tuple = ()) -> dict[str, Any] | None:
    cursor.execute(query, params)
    row = cursor.fetchone()
    return dict(row) if row else None


class Store:
    def __init__(self, transaction: Callable = _transaction, *, execution_placement: str = "modal"):
        if execution_placement not in {"modal", "local"}:
            raise ValueError("Unsupported ingestion execution placement")
        self.transaction = transaction
        self.execution_placement = execution_placement

    def check_placement(self, run: dict[str, Any]) -> None:
        # Missing is the historical Modal placement. Local execution requires the
        # additive migration and an explicit operator transition, never an env.
        if run.get("execution_placement", "modal") != self.execution_placement:
            raise LostOwnership("Run belongs to a different execution placement")

    def _execution_run(self, cur, run_id: str, account: str) -> dict[str, Any]:
        run = self._run(cur, run_id, account)
        self.check_placement(run)
        return run

    def command_result(self, account: str, key: str, command_hash: str) -> dict[str, Any] | None:
        """Resolve an accepted command before mutable launch prerequisites.

        Share Store.start's advisory lock so an in-flight acceptance finishes
        before lookup. A miss is provisional; start still checks under its own
        transaction lock before creating any new state.
        """
        if not key or len(key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"instagram-command:{account}:{key}",),
            )
            previous = _row(
                cur,
                """select command_hash,result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, key),
            )
            if previous is None:
                return None
            if previous["command_hash"] != command_hash:
                raise Conflict("Idempotency key already belongs to a different command")
            return previous["result"]

    def start(
        self,
        intent: RunIntent,
        *,
        idempotency_key: str,
        engine: str,
        workflow_version: str,
        initiated_by: str,
        request_payload: dict[str, Any] | None = None,
        initialize: Callable[[Any, str], None] | None = None,
    ) -> dict[str, Any]:
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        if engine not in {"dbos", "temporal"} or not workflow_version:
            raise ValueError("Engine and workflow version must be selected before launch")
        command_hash = fingerprint({"command": "start", "intent": request_payload or intent.payload()})
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"instagram-command:{intent.account}:{idempotency_key}",),
            )
            previous = _row(
                cur,
                """select command_hash, result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (intent.account, idempotency_key),
            )
            if previous:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return previous["result"]
            run_id = str(uuid4())
            payload = stable_json(intent.payload())
            cur.execute(
                """insert into social.scrape_runs(id, source_scope, status, initiated_by, config)
                values (%s,%s,'queued',%s,%s::jsonb)""",
                (run_id, intent.source_scope, initiated_by, payload),
            )
            cur.execute(
                """insert into social.instagram_ingestion_runs
                (run_id, account, intent, intent_hash, engine, workflow_version)
                values (%s,%s,%s::jsonb,%s,%s,%s)""",
                (run_id, intent.account, payload, intent.digest, engine, workflow_version),
            )
            for stage in (Stage.LISTING, *intent.selected_tasks):
                cur.execute(
                    "insert into social.instagram_ingestion_manifests(run_id,stage) values(%s,%s)",
                    (run_id, stage.value),
                )
            if initialize is not None:
                initialize(conn, run_id)
            execution_id = self._outbox(cur, run_id, 1, workflow_version)
            result = {
                "run_id": run_id,
                "engine": engine,
                "workflow_version": workflow_version,
                "execution_id": execution_id,
                "state": "queued",
                "generation": 1,
            }
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (intent.account, idempotency_key, command_hash, run_id, stable_json(result)),
            )
            return result

    @staticmethod
    def _outbox(cur, run_id: str, generation: int, workflow_version: str) -> str:
        execution_id = f"instagram:{run_id}:{generation}:{workflow_version}"
        cur.execute(
            """insert into social.instagram_ingestion_outbox(execution_id,run_id,generation)
            values(%s,%s,%s) on conflict do nothing""",
            (execution_id, run_id, generation),
        )
        return execution_id

    @staticmethod
    def _run(cur, run_id: str, account: str) -> dict[str, Any]:
        run = _row(
            cur,
            """select * from social.instagram_ingestion_runs
            where run_id=%s and account=%s for update""",
            (run_id, account),
        )
        if not run:
            raise LookupError("Instagram run not found for this account")
        return run

    def control(
        self,
        run_id: str,
        account: str,
        *,
        action: str,
        idempotency_key: str,
        selection: dict[str, Any] | None = None,
        expected_generation: int | None = None,
        expected_placement: str | None = None,
    ) -> dict[str, Any]:
        if action not in {"cancel", "resume", "retry_targets"}:
            raise ValueError("Unsupported run control")
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        if action == "retry_targets":
            if not selection or set(selection) != {"stage", "target_keys"}:
                raise ValueError("Target retry requires a stage and target identities")
            stage = Stage(selection["stage"])
            keys = sorted(set(selection["target_keys"]))
            if stage == Stage.LISTING or not 1 <= len(keys) <= 100 or any(not key for key in keys):
                raise ValueError("Invalid retry target selection")
            selection = {"stage": stage.value, "target_keys": keys}
        elif selection is not None:
            raise ValueError("Selection is only valid for target retry")
        command = {"command": action, "run_id": run_id, "selection": selection}
        if expected_generation is not None:
            if type(expected_generation) is not int or expected_generation < 1:
                raise ValueError("Control requires a positive expected generation")
            command["expected_generation"] = expected_generation
        if expected_placement is not None:
            if expected_placement not in {"local", "modal"}:
                raise ValueError("Unsupported expected execution placement")
            command["expected_placement"] = expected_placement
        command_hash = fingerprint(command)
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"instagram-command:{account}:{idempotency_key}",),
            )
            previous = _row(
                cur,
                """select command_hash,result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, idempotency_key),
            )
            if previous:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return previous["result"]
            run = self._run(cur, run_id, account)
            if expected_generation is not None and run["generation"] != expected_generation:
                raise LostOwnership("Control generation is no longer current")
            if expected_placement is not None and run.get("execution_placement", "modal") != expected_placement:
                raise LostOwnership("Control placement is no longer current")
            if action in {"resume", "retry_targets"}:
                self.check_placement(run)
            if action in {"resume", "retry_targets"} and run.get("execution_hold_reason"):
                raise Conflict("Run execution is held; its original work remains preserved")
            if run["state"] == "completed":
                raise Conflict("Completed run cannot be cancelled or resumed")
            if action in {"resume", "retry_targets"} and run["state"] not in {"cancelled", "failed", "blocked"}:
                raise Conflict("Run already has an active execution")
            if selection is not None:
                cur.execute(
                    """select target_key,outcome,attempts,max_attempts,blocked_reason
                    from social.instagram_ingestion_targets where run_id=%s and stage=%s
                    and target_key=any(%s) for update""",
                    (run_id, selection["stage"], selection["target_keys"]),
                )
                selected = cur.fetchall()
                if len(selected) != len(selection["target_keys"]):
                    raise LookupError("One or more retry targets do not belong to this run")
                if any(
                    row["outcome"] != "unresolved" or row["attempts"] >= row["max_attempts"] or row["blocked_reason"]
                    for row in selected
                ):
                    raise Conflict("Selected targets are settled, exhausted, or identity-blocked")
            generation = run["generation"] + 1
            state = "cancelled" if action == "cancel" else "queued"
            cur.execute(
                """update social.instagram_ingestion_runs
                set state=%s,generation=%s,execution_selection=%s::jsonb,
                    updated_at=clock_timestamp() where run_id=%s""",
                (state, generation, stable_json(selection) if selection is not None else None, run_id),
            )
            # Fence pending dispatch and in-flight writes; preserve outcomes,
            # counters, checkpoints, cooldowns and identity blocks on resume.
            cur.execute(
                """update social.instagram_ingestion_targets
                set owner_token=null,owner_expires_at=null,owner_started_at=null where run_id=%s""",
                (run_id,),
            )
            cur.execute(
                """update social.scrape_runs set status=%s,
                cancelled_at=case when %s='cancelled' then clock_timestamp() else null end where id=%s""",
                (state, state, run_id),
            )
            result = {
                "run_id": run_id,
                "state": state,
                "generation": generation,
                "engine": run["engine"],
                "workflow_version": run["workflow_version"],
            }
            if action in {"resume", "retry_targets"}:
                result["execution_id"] = self._outbox(cur, run_id, generation, run["workflow_version"])
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, idempotency_key, command_hash, run_id, stable_json(result)),
            )
            return result

    def hold_cancelled_run(
        self,
        run_id: str,
        account: str,
        expected_generation: int,
        reason: str,
        idempotency_key: str,
        initiated_by: str,
    ) -> dict[str, Any]:
        """Internal operator hold; no release, resume or history conversion.

        Cancellation must precede this command. Its generation fence remains
        unchanged, and the additive database constraint also fences older APIs.
        """
        if type(expected_generation) is not int or expected_generation < 1:
            raise ValueError("Hold requires an exact positive generation")
        if not isinstance(reason, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", reason):
            raise ValueError("Hold requires a bounded fixed reason code")
        if not isinstance(initiated_by, str) or not initiated_by.strip() or len(initiated_by) > 200:
            raise ValueError("Hold requires a bounded operator identity")
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        command_hash = fingerprint(
            {
                "command": "hold_cancelled_run",
                "run_id": run_id,
                "expected_generation": expected_generation,
                "reason": reason,
                "initiated_by": initiated_by,
            }
        )
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"instagram-command:{account}:{idempotency_key}",),
            )
            previous = _row(
                cur,
                """select command_hash,result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, idempotency_key),
            )
            if previous:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return previous["result"]
            run = self._run(cur, run_id, account)
            if run["state"] != "cancelled" or run["generation"] != expected_generation:
                raise Conflict("Hold requires the exact cancelled run generation")
            if run.get("execution_hold_reason") not in {None, reason}:
                raise Conflict("Run already has a different execution hold")
            active = _row(
                cur,
                """select target_key from social.instagram_ingestion_targets where run_id=%s
                and (owner_expires_at>clock_timestamp() or (outcome='unresolved'
                    and owner_token is not null and owner_expires_at is null)) limit 1""",
                (run_id,),
            )
            if active:
                raise Conflict("Run still has an active or uncertain target owner")
            cur.execute(
                "update social.instagram_ingestion_runs set execution_hold_reason=%s where run_id=%s",
                (reason, run_id),
            )
            result = {
                "run_id": run_id,
                "state": run["state"],
                "generation": run["generation"],
                "engine": run["engine"],
                "workflow_version": run["workflow_version"],
                "execution_hold_reason": reason,
                "initiated_by": initiated_by,
            }
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, idempotency_key, command_hash, run_id, stable_json(result)),
            )
            return result

    def set_execution_placement(
        self,
        run_id: str,
        account: str,
        *,
        expected_generation: int,
        expected_intent_hash: str,
        expected_placement: str,
        placement: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Explicit cancelled-public-run transition, never automatic fallback.

        The coordinator session lease must be held by the operator driver before
        this command. SQL still validates the exact generation, intent and all
        retained ownership evidence. A crash leaves the placement durable.
        """
        if type(expected_generation) is not int or expected_generation < 1:
            raise ValueError("Placement requires an exact positive generation")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_intent_hash):
            raise ValueError("Placement requires the exact frozen intent hash")
        if placement not in {"modal", "local"} or expected_placement not in {"modal", "local"}:
            raise ValueError("Unsupported execution placement")
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        command_hash = fingerprint(
            {
                "command": "set_execution_placement",
                "run_id": run_id,
                "expected_generation": expected_generation,
                "expected_intent_hash": expected_intent_hash,
                "expected_placement": expected_placement,
                "placement": placement,
            }
        )
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"instagram-command:{account}:{idempotency_key}",),
            )
            previous = _row(
                cur,
                """select command_hash,result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, idempotency_key),
            )
            if previous:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return previous["result"]
            run = self._run(cur, run_id, account)
            intent = RunIntent.from_payload(run["intent"])
            if (
                run["state"] != "cancelled"
                or run["generation"] != expected_generation
                or run["intent_hash"] != expected_intent_hash
                or intent.digest != expected_intent_hash
                or run.get("execution_placement", "modal") != expected_placement
            ):
                raise Conflict("Placement requires the exact cancelled run, intent and prior placement")
            if intent.effective_source_profile.access_mode != "public" or run.get("execution_hold_reason"):
                raise Conflict("Placement requires an unheld explicitly public run")
            active = _row(
                cur,
                """select target_key from social.instagram_ingestion_targets
                where run_id=%s and (owner_expires_at>clock_timestamp() or
                (outcome='unresolved' and owner_token is not null and owner_expires_at is null)) limit 1""",
                (run_id,),
            )
            uncertain = _row(
                cur,
                """select d.owner_token from social.instagram_ingestion_dispatches d
                join social.instagram_ingestion_targets t using(run_id,stage,target_key)
                where d.run_id=%s and t.outcome='unresolved'
                and not exists(select 1 from social.instagram_ingestion_receipts x
                    where x.run_id=d.run_id and x.owner_token=d.owner_token)
                and d.state in ('sending','accepted')
                and (d.generation>=%s or greatest(d.updated_at,%s)>
                    clock_timestamp()-interval '600 seconds') limit 1""",
                (run_id, expected_generation, run["updated_at"]),
            )
            if active or uncertain:
                raise Conflict("Run still has a live or uncertain execution owner")
            # The same cancelled-generation/drain proof that permits placement
            # also resolves old send ambiguity for Admission. Keep the journal
            # identities and counters; never manufacture a receipt or completion.
            cur.execute(
                """update social.instagram_ingestion_dispatches d set state='expired',
                    updated_at=clock_timestamp()
                from social.instagram_ingestion_targets t
                where d.run_id=%s and d.generation<%s and d.state in ('sending','accepted')
                    and t.run_id=d.run_id and t.stage=d.stage and t.target_key=d.target_key
                    and t.outcome='unresolved'
                    and greatest(d.updated_at,%s)<=clock_timestamp()-interval '600 seconds'
                    and not exists(select 1 from social.instagram_ingestion_receipts x
                        where x.run_id=d.run_id and x.owner_token=d.owner_token)""",
                (run_id, expected_generation, run["updated_at"]),
            )
            cur.execute(
                """update social.instagram_ingestion_runs set execution_placement=%s,
                updated_at=clock_timestamp() where run_id=%s""",
                (placement, run_id),
            )
            result = {
                "run_id": run_id,
                "state": "cancelled",
                "generation": expected_generation,
                "execution_placement": placement,
                "intent_hash": expected_intent_hash,
            }
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, idempotency_key, command_hash, run_id, stable_json(result)),
            )
            return result

    def discover(
        self,
        run_id: str,
        account: str,
        *,
        generation: int,
        stage: Stage,
        target_keys: list[str],
        max_attempts: int,
        cursor_state: dict[str, Any],
        page_id: str,
        expected_cursor_state: dict[str, Any],
        seal: bool = False,
    ) -> None:
        if max_attempts < 1 or (stage == Stage.DETAILS and max_attempts != 3):
            raise ValueError("Invalid lane attempt budget")
        if any(not key for key in target_keys):
            raise ValueError("Empty target identity")
        if not page_id:
            raise ValueError("A durable discovery page identity is required")
        request_hash = fingerprint(
            {
                "targets": sorted(set(target_keys)),
                "max_attempts": max_attempts,
                "cursor_state": cursor_state,
                "expected_cursor_state": expected_cursor_state,
                "seal": seal,
            }
        )
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Discovery execution is no longer current")
            manifest = _row(
                cur,
                """select discovery_complete,cursor_state from social.instagram_ingestion_manifests
                where run_id=%s and stage=%s for update""",
                (run_id, stage.value),
            )
            previous = _row(
                cur,
                """select request_hash from social.instagram_ingestion_discovery_pages
                where run_id=%s and stage=%s and page_id=%s""",
                (run_id, stage.value, page_id),
            )
            if previous:
                if previous["request_hash"] != request_hash:
                    raise Conflict("Discovery page identity has different content")
                return
            if not manifest or manifest["discovery_complete"]:
                raise Conflict("Manifest is missing or already sealed")
            if manifest["cursor_state"] != expected_cursor_state:
                raise Conflict("Discovery cursor changed; stale page cannot advance it")
            for key in sorted(set(target_keys)):
                cur.execute(
                    """insert into social.instagram_ingestion_targets
                    (run_id,stage,target_key,max_attempts) values(%s,%s,%s,%s) on conflict do nothing""",
                    (run_id, stage.value, key, max_attempts),
                )
            cur.execute(
                """update social.instagram_ingestion_manifests set cursor_state=%s::jsonb,
                discovery_complete=%s,sealed_at=case when %s then clock_timestamp() else null end
                where run_id=%s and stage=%s""",
                (stable_json(cursor_state), seal, seal, run_id, stage.value),
            )
            cur.execute(
                """insert into social.instagram_ingestion_discovery_pages(run_id,stage,page_id,request_hash)
                values(%s,%s,%s,%s)""",
                (run_id, stage.value, page_id, request_hash),
            )

    def reserve(
        self, run_id: str, account: str, *, generation: int, stage: Stage, target_key: str, lease_seconds: int = 600
    ) -> dict[str, Any] | None:
        if not 1 <= lease_seconds <= 600:
            raise ValueError("Reservation must be bounded to at most ten minutes")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            return _row(
                cur,
                """update social.instagram_ingestion_targets set owner_token=%s,
                owner_generation=owner_generation+1,owner_expires_at=clock_timestamp()+make_interval(secs => %s),
                owner_started_at=null where run_id=%s and stage=%s and target_key=%s
                and outcome='unresolved' and blocked_reason is null and attempts<max_attempts
                and (next_attempt_at is null or next_attempt_at<=clock_timestamp())
                and (owner_expires_at is null or owner_expires_at<=clock_timestamp()) returning *""",
                (str(uuid4()), lease_seconds, run_id, stage.value, target_key),
            )

    def acquire(
        self, run_id: str, account: str, *, generation: int, stage: Stage, target_key: str, owner_token: str
    ) -> dict[str, Any]:
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            target = _row(
                cur,
                """update social.instagram_ingestion_targets
                set owner_started_at=clock_timestamp(),attempts=attempts+1
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                and owner_expires_at>clock_timestamp() and owner_started_at is null
                and outcome='unresolved' and attempts<max_attempts returning *""",
                (run_id, stage.value, target_key, owner_token),
            )
            if not target:
                raise LostOwnership("Reservation expired, already acquired, or superseded")
            cur.execute(
                """update social.instagram_ingestion_runs set state='running',
                updated_at=clock_timestamp() where run_id=%s""",
                (run_id,),
            )
            return target

    def check_owner(
        self,
        run_id: str,
        account: str,
        *,
        generation: int,
        stage: Stage,
        target_key: str,
        owner_token: str,
        record_request: bool = False,
        connection: Any = None,
    ) -> None:
        """Check cancellation at request boundaries; count before source dispatch.

        An explicit connection reuses the caller's transaction and never commits
        it. Identity gates lock lane -> run -> target; no Store operation holds
        run/target while acquiring an identity lane.
        """
        if connection is not None and connection.autocommit:
            raise ValueError("Ownership checks require an explicit transaction")
        with (
            nullcontext(connection) if connection is not None else self.transaction() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            target = _row(
                cur,
                """select target_key from social.instagram_ingestion_targets
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                and owner_started_at is not null and owner_expires_at>clock_timestamp()
                and outcome='unresolved' for update""",
                (run_id, stage.value, target_key, owner_token),
            )
            if not target:
                raise LostOwnership("Worker no longer owns this target")
            if record_request:
                cur.execute(
                    """update social.instagram_ingestion_targets set requests=requests+1
                    where run_id=%s and stage=%s and target_key=%s""",
                    (run_id, stage.value, target_key),
                )

    def retain_evidence(
        self,
        run_id: str,
        account: str,
        *,
        generation: int,
        stage: Stage,
        target_key: str,
        owner_token: str,
        evidence: Evidence,
    ) -> str:
        """Save already-sanitized bytes before parsing; reject late worker writes."""
        # Re-validate the sealed envelope rather than trusting a directly
        # constructed dataclass. Expiry is irrelevant while an active run pins it.
        evidence.read(pinned=True)
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            target = _row(
                cur,
                """select target_key from social.instagram_ingestion_targets
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                and owner_started_at is not null and owner_expires_at>clock_timestamp()
                and outcome='unresolved' for update""",
                (run_id, stage.value, target_key, owner_token),
            )
            if not target:
                raise LostOwnership("Evidence writer lost its target reservation")
            evidence_id = str(uuid4())
            cur.execute(
                """insert into social.instagram_ingestion_evidence
                (evidence_id,run_id,stage,target_key,source_identity,captured_at,expires_at,
                 content_hash,parser_version,schema_version,payload)
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    evidence_id,
                    run_id,
                    stage.value,
                    target_key,
                    evidence.source_identity,
                    evidence.captured_at,
                    evidence.expires_at,
                    evidence.content_hash,
                    evidence.parser_version,
                    evidence.schema_version,
                    evidence.payload_json,
                ),
            )
            return evidence_id

    def expire_evidence(self, *, limit: int = 100) -> int:
        """Drop expired payloads, retain provenance, and pin every unresolved run.

        A cancelled/failed run can still be resumed, so its evidence stays pinned.
        This maintenance command is never called from progress or replay GETs.
        """
        if not 1 <= limit <= 1000:
            raise ValueError("Expiry batch must contain 1 to 1000 rows")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """with expired as (
                select e.evidence_id from social.instagram_ingestion_evidence e
                join social.instagram_ingestion_runs r using(run_id)
                where e.expires_at<=clock_timestamp() and e.payload is not null and r.state='completed'
                order by e.expires_at limit %s for update of e skip locked
            ) update social.instagram_ingestion_evidence e set payload=null
                from expired where e.evidence_id=expired.evidence_id""",
                (limit,),
            )
            return cur.rowcount

    def commit_target(
        self,
        run_id: str,
        account: str,
        *,
        generation: int,
        stage: Stage,
        target_key: str,
        owner_token: str,
        observed_at: datetime,
        write_canonical: Callable[[Any], dict[str, Any]],
        evidence_id: str | None = None,
        outcome: Outcome = Outcome.COMMITTED,
    ) -> dict[str, Any]:
        """Commit canonical writes and their receipt atomically under the same fence.

        The canonical writer must enforce observation-time ordering at its row
        lock/upsert boundary. A later service layer supplies the lane-specific
        implementation; arbitrary transport work is forbidden in this callback.
        """
        if outcome == Outcome.UNRESOLVED:
            raise ValueError("Unresolved work cannot receive a settlement receipt")
        if outcome == Outcome.UNAVAILABLE and evidence_id is None:
            raise ValueError("Source unavailability requires retained source evidence")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            previous = _row(
                cur,
                """select * from social.instagram_ingestion_receipts
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s""",
                (run_id, stage.value, target_key, owner_token),
            )
            if previous:
                return previous
            target = _row(
                cur,
                """select * from social.instagram_ingestion_targets
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                and owner_expires_at>clock_timestamp() and owner_started_at is not null
                and outcome='unresolved' for update""",
                (run_id, stage.value, target_key, owner_token),
            )
            if not target:
                raise LostOwnership("Worker cannot commit after reservation loss")
            if evidence_id:
                evidence_row = _row(
                    cur,
                    """select captured_at from social.instagram_ingestion_evidence
                    where evidence_id=%s and run_id=%s and stage=%s and target_key=%s""",
                    (evidence_id, run_id, stage.value, target_key),
                )
                if not evidence_row:
                    raise Conflict("Evidence does not belong to this run and target")
                if utc_timestamp(evidence_row["captured_at"]) != utc_timestamp(observed_at):
                    raise Conflict("Observation time must match the retained source capture")
            canonical_identity = write_canonical(conn)
            receipt = _row(
                cur,
                """insert into social.instagram_ingestion_receipts
                (receipt_id,run_id,stage,target_key,owner_token,owner_generation,evidence_id,
                 observed_at,outcome,canonical_identity) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) returning *""",
                (
                    str(uuid4()),
                    run_id,
                    stage.value,
                    target_key,
                    owner_token,
                    target["owner_generation"],
                    evidence_id,
                    observed_at,
                    outcome.value,
                    stable_json(canonical_identity),
                ),
            )
            cur.execute(
                """update social.instagram_ingestion_targets set outcome=%s,observed_at=%s,
                committed_at=clock_timestamp(),owner_expires_at=null where run_id=%s and stage=%s and target_key=%s""",
                (outcome.value, observed_at, run_id, stage.value, target_key),
            )
            cur.execute(
                """update social.instagram_ingestion_runs set last_committed_at=clock_timestamp(),
                updated_at=clock_timestamp() where run_id=%s""",
                (run_id,),
            )
            assert receipt is not None
            return receipt

    def checkpoint_failure(
        self,
        run_id: str,
        account: str,
        *,
        generation: int,
        stage: Stage,
        target_key: str,
        owner_token: str,
        failure: Failure,
        checkpoint: dict[str, Any],
        requests: int,
    ) -> None:
        """Release owned work with its durable retry state, without inventing success.

        Checkpoints contain lane cursors and identity fingerprints, never headers,
        credentials or raw exception text. Requests is the attempt's cumulative
        request count; redelivery cannot count an attempt twice after release.
        """
        if requests < 0 or failure.source_unavailable:
            raise ValueError("Use an evidence-backed settlement for source unavailability")
        # Match the runtime's fixed-code projection. Never persist exception
        # messages or arbitrary payloads in operator-visible diagnostics.
        code = failure.code
        safe_code = code if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", code) else "activity_failed"
        checkpoint = dict(checkpoint, last_error_code=safe_code)
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running"}:
                raise LostOwnership("Run execution is no longer current")
            cur.execute(
                """update social.instagram_ingestion_targets
                set checkpoint=%s::jsonb,next_attempt_at=%s,blocked_reason=%s,
                    requests=requests+%s,owner_expires_at=null,owner_token=null,owner_started_at=null
                where run_id=%s and stage=%s and target_key=%s and owner_token=%s
                    and owner_started_at is not null and owner_expires_at>clock_timestamp()
                    and outcome='unresolved'""",
                (
                    stable_json(checkpoint),
                    failure.retry_at,
                    safe_code if failure.identity_blocked else None,
                    requests,
                    run_id,
                    stage.value,
                    target_key,
                    owner_token,
                ),
            )
            if cur.rowcount != 1:
                raise LostOwnership("Worker cannot checkpoint after reservation loss")

    def reconcile_completion(self, run_id: str, account: str, *, generation: int) -> str:
        """Explicit workflow command; progress reads never reconcile state."""
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._execution_run(cur, run_id, account)
            if run["generation"] != generation:
                raise LostOwnership("Run execution is no longer current")
            if run["state"] not in {"queued", "running"}:
                return run["state"]
            intent = RunIntent.from_payload(run["intent"])
            required = [Stage.LISTING.value, *(stage.value for stage in intent.selected_tasks)]
            summary = _row(
                cur,
                """select
                (select count(*) from social.instagram_ingestion_manifests
                    where run_id=%s and stage=any(%s) and discovery_complete) as sealed,
                count(*) filter(where outcome='unresolved') as unresolved,
                count(*) filter(where outcome='unresolved' and
                    (blocked_reason is not null or attempts>=max_attempts) and
                    (owner_expires_at is null or owner_expires_at<=clock_timestamp())) as blocked
                from social.instagram_ingestion_targets where run_id=%s""",
                (run_id, required, run_id),
            )
            state = run["state"]
            if summary["sealed"] == len(required) and summary["unresolved"] == 0:
                state = "completed"
            elif summary["unresolved"] and summary["blocked"] == summary["unresolved"]:
                state = "blocked"
            if state != run["state"]:
                cur.execute(
                    """update social.instagram_ingestion_runs set state=%s,
                    updated_at=clock_timestamp() where run_id=%s""",
                    (state, run_id),
                )
                # Legacy table has a narrower status vocabulary. The v2 run is
                # authoritative; blocked remains a visible failed legacy run.
                cur.execute(
                    "update social.scrape_runs set status=%s where id=%s",
                    ("failed" if state == "blocked" else state, run_id),
                )
            return state

    def dispatch_pending(
        self,
        submit: Callable[[dict[str, Any]], None],
        *,
        limit: int = 20,
        engine: str | None = None,
        workflow_version: str | None = None,
    ) -> int:
        """Deliver deterministic workflow IDs outside transactions, then acknowledge.

        The engine adapter must implement idempotent submission by execution_id.
        Competing dispatchers and crashes after submit can therefore redeliver.
        """
        if not 1 <= limit <= 100:
            raise ValueError("Outbox batch must contain 1 to 100 rows")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select o.*,r.account,r.engine,r.workflow_version from
                social.instagram_ingestion_outbox o join social.instagram_ingestion_runs r using(run_id)
                where o.acknowledged_at is null and o.available_at<=clock_timestamp()
                    and o.generation=r.generation and r.state in ('queued','running')
                    and coalesce(to_jsonb(r)->>'execution_placement','modal')=%s
                    and (%s is null or r.engine=%s) and (%s is null or r.workflow_version=%s)
                order by o.available_at,o.execution_id limit %s""",
                (self.execution_placement, engine, engine, workflow_version, workflow_version, limit),
            )
            pending = [dict(row) for row in cur.fetchall()]
        acknowledged = 0
        for item in pending:
            submit(item)
            with self.transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    """update social.instagram_ingestion_outbox
                    set acknowledged_at=clock_timestamp(),attempts=attempts+1
                    where execution_id=%s and acknowledged_at is null""",
                    (item["execution_id"],),
                )
                acknowledged += cur.rowcount
        return acknowledged
