"""Explicit SQL state for bounded scheduling, remote reconciliation and continuation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, LostOwnership, Stage, stable_json
from trr_backend.ingestion.persistence import Store, _row

PARTITION_SIZE = 50
PHASE_ORDER = "case stage when 'listing' then 0 when 'post_details' then 1 when 'comments' then 2 else 3 end"


class WorkflowStore(Store):
    def _current(self, cur, item):
        run = self._execution_run(cur, str(item["run_id"]), item["account"])
        if run["generation"] != item["generation"] or run["state"] not in {"queued", "running"}:
            raise LostOwnership("Workflow generation is no longer active")
        if run["engine"] != item["engine"] or run["workflow_version"] != item["workflow_version"]:
            raise Conflict("Workflow engine/version cannot change during a run")
        return run

    @staticmethod
    def _insert(cur, item, number):
        identity = f"instagram:{item['run_id']}:{item['generation']}:{item['workflow_version']}:partition:{number}"
        cur.execute(
            """insert into social.instagram_ingestion_partitions
            (execution_id,run_id,generation,partition_number,engine,workflow_version)
            values(%s,%s,%s,%s,%s,%s) on conflict do nothing""",
            (identity, item["run_id"], item["generation"], number, item["engine"], item["workflow_version"]),
        )
        return identity

    def seed(self, item):
        if item["engine"] != "dbos":
            raise Conflict("Only DBOS runs may enter this adapter")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            identity = self._insert(cur, item, 0)
            return dict(
                _row(
                    cur,
                    """select p.*,r.account from social.instagram_ingestion_partitions p
                join social.instagram_ingestion_runs r using(run_id) where execution_id=%s""",
                    (identity,),
                )
            )

    def dispatch_partitions(self, submit, *, limit=20, workflow_version=None):
        if not 1 <= limit <= 100:
            raise ValueError("Partition batch must contain 1 to 100 rows")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select p.*,r.account from social.instagram_ingestion_partitions p
                join social.instagram_ingestion_runs r using(run_id)
                where p.acknowledged_at is null and p.completed_at is null and p.available_at<=clock_timestamp()
                and p.generation=r.generation and r.state in ('queued','running')
                    and coalesce(to_jsonb(r)->>'execution_placement','modal')=%s
                and (%s is null or p.workflow_version=%s)
                order by p.available_at,p.execution_id limit %s""",
                (self.execution_placement, workflow_version, workflow_version, limit),
            )
            pending = [dict(row) for row in cur.fetchall()]
        count = 0
        for item in pending:
            submit(item)
            with self.transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    """update social.instagram_ingestion_partitions set acknowledged_at=clock_timestamp()
                    where execution_id=%s and acknowledged_at is null""",
                    (item["execution_id"],),
                )
                count += cur.rowcount
        return count

    def decision(self, item, ordinal: int, now: datetime):
        """Persist one scheduling choice; downstream phases require sealed manifests."""
        if not 0 <= ordinal < PARTITION_SIZE:
            raise ValueError("Partition decision bound exceeded")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._current(cur, item)
            partition = _row(
                cur,
                """select * from social.instagram_ingestion_partitions
                where execution_id=%s for update""",
                (item["execution_id"],),
            )
            if partition and (
                str(partition["run_id"]) != str(item["run_id"])
                or partition["generation"] != item["generation"]
                or partition["workflow_version"] != item["workflow_version"]
            ):
                raise LostOwnership("Partition belongs to a different execution")
            if not partition or partition["completed_at"]:
                raise LostOwnership("Partition is missing or already complete")
            prior = _row(
                cur,
                """select decision from social.instagram_ingestion_decisions
                where execution_id=%s and ordinal=%s""",
                (item["execution_id"], ordinal),
            )
            if prior:
                return prior["decision"]
            # Strict phase barrier, including selected empty-but-unsealed stages.
            phase = _row(
                cur,
                f"""select m.* from social.instagram_ingestion_manifests m
                where run_id=%s and (not discovery_complete or exists (
                    select 1 from social.instagram_ingestion_targets t where t.run_id=m.run_id
                    and t.stage=m.stage and outcome='unresolved')) order by {PHASE_ORDER} limit 1""",
                (item["run_id"],),
            )
            selection = run.get("execution_selection")
            selection_keys = None
            selection_settled = False
            full_phase = phase
            if selection is not None:
                try:
                    selected_stage = Stage(selection["stage"]).value
                    selection_keys = selection["target_keys"]
                    if (
                        not isinstance(selection_keys, list)
                        or not selection_keys
                        or any(not isinstance(key, str) or not key for key in selection_keys)
                    ):
                        raise ValueError("Invalid selected target keys")
                except (KeyError, TypeError, ValueError) as exc:
                    raise Conflict("Invalid execution selection") from exc
                selected = _row(
                    cur,
                    """select count(*) as total,
                    count(*) filter(where outcome='unresolved') as unresolved
                    from social.instagram_ingestion_targets where run_id=%s and stage=%s
                    and target_key=any(%s)""",
                    (item["run_id"], selected_stage, selection_keys),
                )
                if selected["total"] != len(set(selection_keys)):
                    raise Conflict("Selected targets must already exist in this run")
                phase = _row(
                    cur,
                    """select * from social.instagram_ingestion_manifests
                    where run_id=%s and stage=%s""",
                    (item["run_id"], selected_stage),
                )
                selection_settled = selected["unresolved"] == 0
            result: dict[str, Any] = {"kind": "complete"}
            if selection_settled:
                # Explicit retry never falls through into unrelated original work.
                result = {"kind": "blocked" if full_phase else "complete"}
            elif phase:
                result = {"kind": "wait", "seconds": 5.0}
                if phase["stage"] == "listing" or phase["discovery_complete"]:
                    # Return one small candidate, never all unresolved payloads.
                    candidate = _row(
                        cur,
                        """select t.stage,t.target_key,d.owner_token as dispatch_token,d.state
                        from social.instagram_ingestion_targets t
                        left join social.instagram_ingestion_dispatches d on d.owner_token=t.owner_token
                            and d.run_id=t.run_id and d.generation=%s
                        where t.run_id=%s and t.stage=%s and t.outcome='unresolved'
                            and (%s::text[] is null or t.target_key=any(%s))
                            and ((d.state='sending' and (t.owner_expires_at is null or t.owner_expires_at<=%s)) or (
                                (t.owner_expires_at is null or t.owner_expires_at<=%s)
                                and t.blocked_reason is null and t.attempts<t.max_attempts
                                and (t.next_attempt_at is null or t.next_attempt_at<=%s)))
                        order by t.target_key limit 1""",
                        (
                            item["generation"],
                            item["run_id"],
                            phase["stage"],
                            selection_keys,
                            selection_keys,
                            now,
                            now,
                            now,
                        ),
                    )
                    if candidate is None:
                        # Fill available lane capacity before revisiting an
                        # ambiguous live send; active owners do not serialize
                        # unrelated eligible targets in this sealed phase.
                        candidate = _row(
                            cur,
                            """select t.stage,t.target_key,d.owner_token as dispatch_token,d.state
                            from social.instagram_ingestion_targets t
                            join social.instagram_ingestion_dispatches d on d.owner_token=t.owner_token
                                and d.run_id=t.run_id and d.generation=%s
                            where t.run_id=%s and t.stage=%s and t.outcome='unresolved' and d.state='sending'
                                and (%s::text[] is null or t.target_key=any(%s))
                            order by t.target_key limit 1""",
                            (item["generation"], item["run_id"], phase["stage"], selection_keys, selection_keys),
                        )
                    if candidate:
                        result = {
                            "kind": "reconcile" if candidate["state"] == "sending" else "target",
                            "stage": candidate["stage"],
                            "target_key": candidate["target_key"],
                        }
                        if candidate["state"] == "sending":
                            result["owner_token"] = str(candidate["dispatch_token"])
                    else:
                        # Aggregates return one row, only when no candidate is
                        # available. Checkpoint JSON never crosses this boundary.
                        summary = _row(
                            cur,
                            """select count(*) as total,
                            count(*) filter(where (owner_expires_at is null or owner_expires_at<=%s)
                                and (blocked_reason is not null or attempts>=max_attempts)) as blocked,
                            count(*) filter(where owner_expires_at>%s) as active,
                            min(case when owner_expires_at>%s then owner_expires_at
                                when blocked_reason is null and attempts<max_attempts and next_attempt_at>%s
                                then next_attempt_at end) as wake_at
                            from social.instagram_ingestion_targets where run_id=%s and stage=%s
                                and outcome='unresolved' and (%s::text[] is null or target_key=any(%s))""",
                            (now, now, now, now, item["run_id"], phase["stage"], selection_keys, selection_keys),
                        )
                        if summary["total"] and summary["blocked"] == summary["total"]:
                            result = {"kind": "blocked"}
                        elif summary["wake_at"]:
                            seconds = max(0.01, (summary["wake_at"] - now).total_seconds())
                            # Receipts may arrive before lease expiry. Poll active
                            # workers while preserving exact persisted cooldowns.
                            result = {"kind": "wait", "seconds": min(1.0, seconds) if summary["active"] else seconds}
            cur.execute(
                """insert into social.instagram_ingestion_decisions(execution_id,ordinal,decision)
                values(%s,%s,%s::jsonb)""",
                (item["execution_id"], ordinal, stable_json(result)),
            )
            return result

    def reserve_decision(self, item, decision, *, ordinal=None):
        """Use Store.reserve in this same transaction, journal before leaving it."""
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            if ordinal is not None:
                recorded = _row(
                    cur,
                    """select decision from social.instagram_ingestion_decisions
                    where execution_id=%s and ordinal=%s for update""",
                    (item["execution_id"], ordinal),
                )
                if recorded and recorded["decision"].get("owner_token"):
                    dispatch = _row(
                        cur,
                        """select * from social.instagram_ingestion_dispatches
                        where owner_token=%s and run_id=%s and generation=%s""",
                        (recorded["decision"]["owner_token"], item["run_id"], item["generation"]),
                    )
                    if not dispatch:
                        raise Conflict("Decision lost its durable reservation")
                    return self._reservation(item, dispatch)

            @contextmanager
            def same_transaction():
                yield conn

            args = {k: item[k] for k in ("run_id", "account", "generation")}
            args.update(stage=Stage(decision["stage"]), target_key=decision["target_key"])
            # Idempotent recovery after reservation, before DBOS step ack.
            existing = _row(
                cur,
                """select d.*,t.owner_expires_at,t.owner_started_at,t.outcome from
                social.instagram_ingestion_dispatches d join social.instagram_ingestion_targets t
                using(run_id,stage,target_key) where d.run_id=%s and d.generation=%s and d.stage=%s
                and d.target_key=%s and d.owner_token=t.owner_token and t.outcome='unresolved'
                and (t.owner_expires_at>clock_timestamp() or d.state='sending')""",
                (item["run_id"], item["generation"], decision["stage"], decision["target_key"]),
            )
            if existing:
                if ordinal is not None:
                    self._bind_decision(cur, item, ordinal, str(existing["owner_token"]))
                return self._reservation(item, existing)
            target = Store(same_transaction, execution_placement=self.execution_placement).reserve(**args)
            if not target:
                return None
            dispatch = _row(
                cur,
                """insert into social.instagram_ingestion_dispatches
                (owner_token,run_id,generation,stage,target_key) values(%s,%s,%s,%s,%s) returning *""",
                (
                    str(target["owner_token"]),
                    item["run_id"],
                    item["generation"],
                    decision["stage"],
                    decision["target_key"],
                ),
            )
            if ordinal is not None:
                self._bind_decision(cur, item, ordinal, str(dispatch["owner_token"]))
            return self._reservation(item, dispatch)

    @staticmethod
    def _bind_decision(cur, item, ordinal, token):
        cur.execute(
            """update social.instagram_ingestion_decisions
            set decision=decision || %s::jsonb where execution_id=%s and ordinal=%s""",
            (stable_json({"owner_token": token}), item["execution_id"], ordinal),
        )
        if cur.rowcount != 1:
            raise Conflict("Reservation requires its durable scheduling decision")

    def note_control_wait(self, item, ordinal, code):
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            cur.execute(
                """update social.instagram_ingestion_decisions
                set decision=decision || %s::jsonb where execution_id=%s and ordinal=%s""",
                (stable_json({"control_failure": code}), item["execution_id"], ordinal),
            )

    def engine_partitions(self, *, workflow_version, limit=20, after_execution_id=None):
        if not 1 <= limit <= 100:
            raise ValueError("Engine reconciliation batch must contain 1 to 100 rows")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select p.*,r.account from social.instagram_ingestion_partitions p
                join social.instagram_ingestion_runs r using(run_id)
                where p.acknowledged_at is not null and p.completed_at is null
                    and p.generation=r.generation and r.state in ('queued','running')
                    and coalesce(to_jsonb(r)->>'execution_placement','modal')=%s
                    and p.workflow_version=%s and (%s is null or p.execution_id>%s)
                order by p.execution_id limit %s""",
                (self.execution_placement, workflow_version, after_execution_id, after_execution_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    def pause_failed_partition(self, item, engine_status):
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            partition = _row(
                cur,
                """select * from social.instagram_ingestion_partitions
                where execution_id=%s and run_id=%s and generation=%s for update""",
                (item["execution_id"], item["run_id"], item["generation"]),
            )
            if not partition or partition["completed_at"]:
                return False
            diagnostic = {"control_failure": "ENGINE_TERMINAL", "engine_status": engine_status}
            last = _row(
                cur,
                """select ordinal from social.instagram_ingestion_decisions
                where execution_id=%s order by ordinal desc limit 1""",
                (item["execution_id"],),
            )
            if last:
                cur.execute(
                    """update social.instagram_ingestion_decisions set decision=decision || %s::jsonb
                    where execution_id=%s and ordinal=%s""",
                    (stable_json(diagnostic), item["execution_id"], last["ordinal"]),
                )
            else:
                cur.execute(
                    """insert into social.instagram_ingestion_decisions(execution_id,ordinal,decision)
                    values(%s,0,%s::jsonb)""",
                    (item["execution_id"], stable_json(dict(diagnostic, kind="engine_error"))),
                )
            cur.execute(
                """update social.instagram_ingestion_runs set state='blocked',updated_at=clock_timestamp()
                where run_id=%s""",
                (item["run_id"],),
            )
            cur.execute("update social.scrape_runs set status='failed' where id=%s", (item["run_id"],))
            return True

    @staticmethod
    def _reservation(item, row):
        return {
            "run_id": str(item["run_id"]),
            "account": item["account"],
            "generation": item["generation"],
            "stage": row["stage"],
            "target_key": row["target_key"],
            "owner_token": str(row["owner_token"]),
            "dispatch_state": row["state"],
            "remote_identity": row.get("remote_identity"),
        }

    def reservation(self, item, token):
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            row = _row(
                cur,
                """select * from social.instagram_ingestion_dispatches
                where owner_token=%s and run_id=%s and generation=%s""",
                (token, item["run_id"], item["generation"]),
            )
            if not row:
                raise LostOwnership("Dispatch reservation is missing")
            return self._reservation(item, row)

    def local_dispatch_status(self, item, token, now):
        """Receipt and exact-token acquisition precede remote reconciliation."""
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            row = _row(
                cur,
                """select d.state,t.outcome,t.owner_started_at,t.owner_expires_at,
                t.owner_token as current_token, exists(select 1 from social.instagram_ingestion_receipts r
                    where r.owner_token=d.owner_token and r.run_id=d.run_id and r.stage=d.stage
                    and r.target_key=d.target_key) as receipt
                from social.instagram_ingestion_dispatches d join social.instagram_ingestion_targets t
                using(run_id,stage,target_key) where d.owner_token=%s and d.run_id=%s and d.generation=%s""",
                (token, item["run_id"], item["generation"]),
            )
            if not row:
                raise LostOwnership("Dispatch reservation is missing")
            if row["receipt"] or row["outcome"] != "unresolved":
                return "settled"
            if str(row["current_token"]) != token or not row["owner_expires_at"] or row["owner_expires_at"] <= now:
                return "expired"
            if row["owner_started_at"]:
                return "accepted"
            return row["state"]

    def transition(self, item, token, state, remote_identity=None):
        if state not in {"reserved", "sending", "accepted", "expired"}:
            raise ValueError("Invalid dispatch state")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            self._current(cur, item)
            row = _row(
                cur,
                """select d.*,t.outcome,t.owner_token as current_token,t.owner_expires_at,t.owner_started_at
                from social.instagram_ingestion_dispatches d join social.instagram_ingestion_targets t
                using(run_id,stage,target_key) where d.owner_token=%s for update of d,t""",
                (token,),
            )
            if not row or str(row["run_id"]) != str(item["run_id"]) or row["generation"] != item["generation"]:
                raise LostOwnership("Dispatch reservation is no longer current")
            if state == "reserved" and row["state"] != "sending":
                return False
            if state == "sending" and row["state"] != "reserved":
                return False
            if state == "sending" and (
                str(row["current_token"]) != token
                or row["outcome"] != "unresolved"
                or row["owner_started_at"] is not None
                or row["owner_expires_at"] is None
                or row["owner_expires_at"] <= datetime.now(row["owner_expires_at"].tzinfo)
            ):
                raise LostOwnership("Cannot dispatch a stale reservation")
            cur.execute(
                """update social.instagram_ingestion_dispatches set state=%s,remote_identity=%s,
                updated_at=clock_timestamp() where owner_token=%s""",
                (state, remote_identity, token),
            )
            return True

    def complete_partition(self, item, *, continue_run=True, stop_state=None):
        """Fence cancellation and atomically complete and enqueue deterministic successor."""
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = self._current(cur, item)
            partition = _row(
                cur,
                """select * from social.instagram_ingestion_partitions
                where execution_id=%s for update""",
                (item["execution_id"],),
            )
            if (
                not partition
                or str(partition["run_id"]) != str(item["run_id"])
                or partition["generation"] != item["generation"]
            ):
                raise LostOwnership("Partition is missing or belongs to another execution")
            if partition["completed_at"]:
                return partition["next_execution_id"]
            if stop_state not in {None, "completed", "blocked"}:
                raise ValueError("Invalid workflow stop state")
            if continue_run:
                count = _row(
                    cur,
                    "select count(*) as count from social.instagram_ingestion_decisions where execution_id=%s",
                    (item["execution_id"],),
                )["count"]
                if count != PARTITION_SIZE:
                    raise Conflict("Cannot continue an unfinished partition")
            successor = None
            if continue_run:
                successor = self._insert(cur, item, partition["partition_number"] + 1)
            cur.execute(
                """update social.instagram_ingestion_partitions set completed_at=coalesce(
                completed_at,clock_timestamp()),next_execution_id=%s where execution_id=%s""",
                (successor, item["execution_id"]),
            )
            if stop_state and run.get("execution_selection") is not None:

                @contextmanager
                def same_transaction():
                    yield conn

                # Original manifests and all original targets remain the only
                # authority for full completion, including a narrowed execution.
                reconciled = Store(same_transaction, execution_placement=self.execution_placement).reconcile_completion(
                    item["run_id"], item["account"], generation=item["generation"]
                )
                stop_state = "completed" if reconciled == "completed" else "blocked"
            if stop_state:
                cur.execute(
                    "update social.instagram_ingestion_runs set state=%s,updated_at=clock_timestamp() where run_id=%s",
                    (stop_state, item["run_id"]),
                )
                cur.execute(
                    "update social.scrape_runs set status=%s where id=%s",
                    ("failed" if stop_state == "blocked" else stop_state, item["run_id"]),
                )
            return successor
