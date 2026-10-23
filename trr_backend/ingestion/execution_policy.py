"""Durable local execution limits outside immutable ingestion intent.

Only policy state and the existing command audit are mutable here. Admission may
use the pure helper with its own snapshot; no provider or lease work is done.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, LostOwnership, RunIntent, fingerprint, stable_json, utc_timestamp
from trr_backend.ingestion.persistence import Store, _row, _transaction

_LIMITS = frozenset({1, 2, 4, 6, 8})
_OPERATION = "set_execution_limits"


def _uuid(value: Any) -> str:
    return str(UUID(str(value)))


def _positive(value: Any) -> bool:
    return type(value) is int and value >= 1


def _hash(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _current_intent(run: Mapping[str, Any]) -> RunIntent:
    if run["state"] not in {"queued", "running"} or not _positive(run["generation"]):
        raise LostOwnership("Execution policy requires an active run generation")
    if run.get("execution_placement", "modal") not in {"local", "modal"}:
        raise Conflict("Invalid run execution placement")
    intent = RunIntent.from_payload(run["intent"])
    if intent.account != run["account"] or intent.digest != run["intent_hash"]:
        raise Conflict("Original run intent binding is invalid")
    from trr_backend.ingestion.validation_gate import require_allowed_intent

    require_allowed_intent(intent)
    return intent


def _current_policy_limit(run: Mapping[str, Any], policy: Mapping[str, Any] | None) -> int | None:
    """Return the current policy limit, or None when no policy applies."""
    if policy is None:
        return None
    if not isinstance(policy, Mapping):
        raise Conflict("Execution policy is malformed")
    try:
        policy_run = _uuid(policy["run_id"])
        if (
            not isinstance(policy["account"], str)
            or not _positive(policy["generation"])
            or not _hash(policy["intent_hash"])
            or policy["execution_placement"] not in {"local", "modal"}
        ):
            raise ValueError("Invalid policy fence")
        current = (
            policy_run == _uuid(run["run_id"])
            and policy["account"] == run["account"]
            and policy["generation"] == run["generation"]
            and policy["intent_hash"] == run["intent_hash"]
            and policy["execution_placement"] == run.get("execution_placement", "modal") == "local"
        )
        if not current:
            return None
        if not _positive(policy["revision"]) or type(policy["comments_parallelism"]) is not int:
            raise ValueError("Invalid policy value")
        if policy["comments_parallelism"] not in _LIMITS:
            raise ValueError("Unsupported policy limit")
        return policy["comments_parallelism"]
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise Conflict("Execution policy is malformed") from exc


def effective_comments_limit_from_run(run: Mapping[str, Any], policy: Mapping[str, Any] | None) -> int | None:
    """Apply a current policy to Admission's single run/policy snapshot.

    Admission preserves the original intent count when no policy applies.
    Matching policy with malformed limits fails closed.
    """
    intent = _current_intent(run)
    limit = _current_policy_limit(run, policy)
    return intent.comments_worker_count if limit is None else limit


class ExecutionPolicy:
    def __init__(self, transaction: Callable = _transaction):
        self.transaction = transaction

    def effective_comments_limit(self, item: Mapping[str, Any]) -> int:
        """Keep workers serial unless an exact current local policy opts in."""
        run_id = _uuid(item["run_id"])
        account = RunIntent(account=item["account"]).account
        if not _positive(item["generation"]):
            raise ValueError("Expected generation must be a positive integer")
        with self.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = _row(
                cur,
                """select r.*,to_jsonb(p) as execution_policy
                from social.instagram_ingestion_runs r
                left join social.instagram_ingestion_execution_policies p on p.run_id=r.run_id
                where r.run_id=%s and r.account=%s for share of r""",
                (run_id, account),
            )
            if run is None:
                raise LookupError("Instagram run not found for this account")
            if run["generation"] != item["generation"] or any(
                key in item and item[key] != run.get(key, "modal" if key == "execution_placement" else None)
                for key in ("intent_hash", "execution_placement")
            ):
                raise LostOwnership("Execution policy callback fence is no longer current")
            if any(key in item and item[key] != run[key] for key in ("engine", "workflow_version")):
                raise Conflict("Workflow engine/version cannot change during a run")
            _current_intent(run)
            limit = _current_policy_limit(run, run["execution_policy"])
            return 1 if limit is None else limit

    def set_limits(
        self,
        run_id: str,
        account: str,
        *,
        expected_generation: int,
        expected_intent_hash: str,
        expected_revision: int,
        comments_parallelism: int,
        idempotency_key: str,
        initiated_by: str,
        expected_placement: str = "local",
    ) -> dict[str, Any]:
        run_id = _uuid(run_id)
        account = RunIntent(account=account).account
        if not _positive(expected_generation) or not _hash(expected_intent_hash):
            raise ValueError("Exact expected generation and intent hash are required")
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("Expected policy revision must be a nonnegative integer")
        if type(comments_parallelism) is not int or comments_parallelism not in _LIMITS:
            raise ValueError("Comments parallelism must be 1, 2, 4, 6, or 8")
        if expected_placement != "local":
            raise ValueError("Execution limits may be changed only for local runs")
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 200 or not idempotency_key.strip():
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        if not isinstance(initiated_by, str) or not 1 <= len(initiated_by.strip()) <= 200:
            raise ValueError("An explicit policy operator is required")
        initiated_by = initiated_by.strip()
        command = {
            "command": _OPERATION,
            "run_id": run_id,
            "account": account,
            "expected_generation": expected_generation,
            "expected_intent_hash": expected_intent_hash,
            "expected_placement": expected_placement,
            "expected_revision": expected_revision,
            "comments_parallelism": comments_parallelism,
            "initiated_by": initiated_by,
        }
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
            # An exact repeat reports its accepted command result; it does not
            # reapply a limit after a later revision or generation transition.
            if previous is not None:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return previous["result"]
            run = Store._run(cur, run_id, account)
            intent = _current_intent(run)
            if (
                run["generation"] != expected_generation
                or run["intent_hash"] != expected_intent_hash
                or run.get("execution_placement", "modal") != expected_placement
            ):
                raise LostOwnership("Expected execution policy run fence is no longer current")
            policy = _row(
                cur,
                "select * from social.instagram_ingestion_execution_policies where run_id=%s for update",
                (run_id,),
            )
            revision = policy["revision"] if policy else 0
            if (policy is not None and not _positive(revision)) or revision != expected_revision:
                raise Conflict("Execution policy revision changed")
            old_limit = effective_comments_limit_from_run(run, policy) if policy else intent.comments_worker_count
            params = (account, expected_generation, expected_intent_hash, comments_parallelism, initiated_by)
            if policy is None:
                changed = _row(
                    cur,
                    """insert into social.instagram_ingestion_execution_policies
                    (run_id,account,generation,intent_hash,execution_placement,revision,comments_parallelism,updated_by)
                    values(%s,%s,%s,%s,'local',1,%s,%s) on conflict(run_id) do nothing returning *""",
                    (run_id, *params),
                )
            else:
                changed = _row(
                    cur,
                    """update social.instagram_ingestion_execution_policies
                    set account=%s,generation=%s,intent_hash=%s,comments_parallelism=%s,updated_by=%s,
                        execution_placement='local',revision=revision+1,updated_at=clock_timestamp()
                    where run_id=%s and revision=%s returning *""",
                    (*params, run_id, revision),
                )
            if changed is None:
                raise Conflict("Execution policy revision changed")
            result = {
                **{
                    key: changed[key]
                    for key in (
                        "account",
                        "generation",
                        "intent_hash",
                        "execution_placement",
                        "revision",
                        "comments_parallelism",
                        "updated_by",
                    )
                },
                "operation": _OPERATION,
                "run_id": str(changed["run_id"]),
                "updated_at": utc_timestamp(changed["updated_at"]),
                "previous_revision": revision,
                "previous_comments_parallelism": old_limit,
            }
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, idempotency_key, command_hash, run_id, stable_json(result)),
            )
            return result
