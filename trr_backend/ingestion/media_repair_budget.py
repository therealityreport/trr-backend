"""Explicit one-shot January media repair grant; no scheduling or source calls."""

from __future__ import annotations

import hashlib
import json
import plistlib
import re
import shlex
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, Stage, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.validation_gate import require_allowed_intent

WORKER_PLIST = Path.home() / "Library/LaunchAgents/com.trr.instagram-local-worker.plist"
_SHA = re.compile(r"[a-f0-9]{64}")
_CODE = re.compile(r"[a-z][a-z0-9_]{0,79}")
_ALLOWED_FAILURES = frozenset(
    {
        "avatar_refresh_asset_unresolved",
        "profile_provider_flags_ambiguous",
        "existing_object_content_conflict",
        "cached_avatar_alias_inventory_limit",
    }
)


@dataclass(frozen=True)
class RepairQualification:
    """Explicit parent-pinned review inputs, verified against installed bytes."""

    repair_revision: str
    manifest_path: str
    manifest_sha256: str
    review_path: str
    review_sha256: str
    freeze_path: str
    diagnostic_path: str
    diagnostic_sha256: str


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _runtime_path(value):
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not value.endswith(".py"):
        return False
    return value.startswith(("trr_backend/", "api/", "scripts/ingestion/"))


def qualify_installed_repair(qualification: RepairQualification) -> dict:
    """Validate a parent-pinned accepted review and installed runtime manifest.

    This verifies disk/configuration, not the running coordinator. The parent
    independently qualifies the running process before authorizing live apply.
    Only the named January binding assignment is extracted from configured
    program text; no shell text is executed or included in the receipt.
    """
    if not isinstance(qualification, RepairQualification):
        raise ValueError("Explicit installed repair qualification required")
    q = qualification
    if not all(_SHA.fullmatch(value or "") for value in (q.manifest_sha256, q.review_sha256, q.diagnostic_sha256)):
        raise ValueError("Parent-pinned review and manifest hashes required")
    if _sha(q.manifest_path) != q.manifest_sha256 or _sha(q.review_path) != q.review_sha256:
        raise Conflict("Parent-pinned repair evidence changed")
    manifest = json.loads(Path(q.manifest_path).read_text())
    review = json.loads(Path(q.review_path).read_text())
    freeze_hash = _sha(q.freeze_path)
    freeze = json.loads(Path(q.freeze_path).read_text())
    plist = plistlib.loads(WORKER_PLIST.read_bytes())
    configured = Path(plist["WorkingDirectory"]).resolve()
    configured_bindings = []
    environment_binding = (plist.get("EnvironmentVariables") or {}).get("TRR_INSTAGRAM_JANUARY_BINDING")
    if environment_binding:
        configured_bindings.append(environment_binding)
    for argument in plist.get("ProgramArguments", []):
        if not isinstance(argument, str):
            continue
        for token in shlex.split(argument):
            if token.startswith("TRR_INSTAGRAM_JANUARY_BINDING="):
                configured_bindings.append(token.split("=", 1)[1])
    configured_bindings = list(dict.fromkeys(configured_bindings))
    if len(configured_bindings) != 1:
        raise Conflict("One configured January run binding required")
    binding_path = Path(configured_bindings[0])
    run_binding = json.loads(binding_path.read_text())
    binding_release = run_binding.get("release_path")
    if not isinstance(binding_release, str) or not binding_release.strip():
        raise Conflict("Configured January binding requires a nonempty release path string")
    if Path(binding_release).resolve() != configured or run_binding.get("source_identity") != q.repair_revision:
        raise Conflict("Configured January binding and installed repair differ")
    if (
        configured.name != q.repair_revision
        or Path(manifest["release_path"]).resolve() != configured
        or manifest.get("source_identity") != q.repair_revision
        or review.get("accepted") is not True
        or review.get("source_identity") != q.repair_revision
        or review.get("freeze_sha256") != freeze_hash
    ):
        raise Conflict("Reviewed repair is not the configured installed release")
    files = {name: value for name, value in manifest.get("files", {}).items() if _runtime_path(name)}
    frozen = [item for item in freeze.get("files", []) if _runtime_path(item.get("path", ""))]
    if not files or not frozen:
        raise Conflict("Installed runtime and reviewed repair files required")
    for name, expected in files.items():
        path = (configured / name).resolve()
        if not path.is_relative_to(configured) or not _SHA.fullmatch(expected or "") or _sha(path) != expected:
            raise Conflict("Installed runtime differs from reviewed manifest")
    for item in frozen:
        if files.get(item["path"]) != item.get("sha256"):
            raise Conflict("Installed repair differs from accepted frozen files")
    qualified = {
        "repair_revision": q.repair_revision,
        "installed_release": str(configured),
        "manifest_sha256": q.manifest_sha256,
        "review_sha256": q.review_sha256,
        "freeze_sha256": freeze_hash,
        "verified_runtime_files": len(files),
        "installed_binding_sha256": _sha(binding_path),
        "installed_run_id": run_binding.get("successor_run_id"),
        "installed_intent_hash": run_binding.get("successor_intent_hash"),
        "installed_generation": run_binding.get("expected_generation"),
        "running_process_verification": "separate parent qualification required",
    }
    qualification_hash = fingerprint(qualified)
    if _sha(q.diagnostic_path) != q.diagnostic_sha256:
        raise Conflict("Parent-pinned diagnostic approval changed")
    approval = json.loads(Path(q.diagnostic_path).read_text())
    proof = approval.get("eligibility_proof") or {}
    if (
        approval.get("version") != 1
        or approval.get("approved") is not True
        or approval.get("qualification_sha256") != qualification_hash
        or proof.get("parent_reviewed_eligible") is not True
        or not isinstance(proof.get("schema_identity"), str)
        or not 0 < len(proof["schema_identity"]) <= 200
        or not _SHA.fullmatch(proof.get("sha256", ""))
        or _sha(proof["path"]) != proof["sha256"]
    ):
        raise Conflict("Parent diagnostic and eligibility proof qualification required")
    fields = (
        "run_id",
        "account",
        "generation",
        "intent_hash",
        "target_key",
        "checkpoint_sha256",
        "owner_generation",
        "attempts",
        "max_attempts",
        "failure_code",
        "failed_revision",
        "repair_revision",
    )
    return {
        **qualified,
        "qualification_sha256": qualification_hash,
        "diagnostic_sha256": q.diagnostic_sha256,
        "eligibility_proof_sha256": proof["sha256"],
        "eligibility_proof_schema_identity": proof["schema_identity"],
        "diagnostic_binding": {field: approval.get(field) for field in fields},
    }


def _request(
    *,
    run_id,
    account,
    generation,
    target_key,
    expected_checkpoint_hash,
    expected_owner_generation,
    expected_attempts,
    expected_max_attempts,
    expected_failure_code,
    qualification,
    failed_revision,
    reason,
    initiated_by,
):
    from uuid import UUID

    UUID(run_id)
    if account != "bravotv" or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", target_key or ""):
        raise ValueError("Exact January account and target required")
    if not all(type(value) is int and value >= 1 for value in (generation, expected_owner_generation)):
        raise ValueError("Exact positive generations required")
    if type(expected_attempts) is not int or type(expected_max_attempts) is not int:
        raise ValueError("Exact integer attempt state required")
    if (expected_attempts, expected_max_attempts) != (3, 3):
        raise Conflict("Only the fixed 3/3 to 3/4 repair grant is supported")
    if not _SHA.fullmatch(expected_checkpoint_hash or "") or not _CODE.fullmatch(expected_failure_code or ""):
        raise ValueError("Exact checkpoint and fixed diagnostic required")
    if not isinstance(qualification, RepairQualification):
        raise ValueError("Reviewed installed repair qualification required")
    if not all(
        isinstance(value, str) and 0 < len(value.strip()) <= 200
        for value in (qualification.repair_revision, failed_revision, reason, initiated_by)
    ):
        raise ValueError("Bounded repair reason, revisions and operator required")
    if expected_failure_code not in _ALLOWED_FAILURES:
        raise Conflict("Diagnostic is not an approved technical repair failure")
    if failed_revision == qualification.repair_revision:
        raise Conflict("Repair revision must differ from failed revision")
    return {
        "command": "extend_january_media_repair_budget_v1",
        "run_id": run_id,
        "account": account,
        "generation": generation,
        "target_key": target_key,
        "expected_checkpoint_hash": expected_checkpoint_hash,
        "expected_owner_generation": expected_owner_generation,
        "expected_attempts": expected_attempts,
        "expected_max_attempts": expected_max_attempts,
        "expected_failure_code": expected_failure_code,
        "qualification": asdict(qualification),
        "failed_revision": failed_revision,
        "reason": reason,
        "initiated_by": initiated_by,
    }


def _target_guard(cur, store, request, intent_hash, *, lock):
    if lock:
        run = store._execution_run(cur, request["run_id"], request["account"], shared_fence=True)
    else:
        run = _row(
            cur,
            "select * from social.instagram_ingestion_runs where run_id=%s and account=%s",
            (request["run_id"], request["account"]),
        )
        if not run:
            raise LookupError("Reviewed January run not found")
        store.check_placement(run)
    intent = RunIntent.from_payload(run["intent"])
    require_allowed_intent(intent)
    if (
        run["generation"] != request["generation"]
        or run["intent_hash"] != intent_hash
        or intent.digest != intent_hash
        or run["state"] not in {"queued", "running"}
        or run.get("execution_hold_reason")
        or run.get("execution_placement", "modal") != "local"
        or intent.account != "bravotv"
        or intent.scope != "bounded_window"
        or intent.date_start != datetime(2026, 1, 1, tzinfo=UTC)
        or intent.date_end != datetime(2026, 2, 1, tzinfo=UTC)
        or intent.effective_source_profile.access_mode != "public"
        or Stage.MEDIA not in intent.selected_tasks
    ):
        raise Conflict("Exact unheld active local public January run required")
    target = _row(
        cur,
        "select * from social.instagram_ingestion_targets where run_id=%s and stage='media' and target_key=%s"
        + (" for update" if lock else ""),
        (request["run_id"], request["target_key"]),
    )
    if not target:
        raise LookupError("Reviewed media target not found")
    if (
        target["outcome"] != "unresolved"
        or target["blocked_reason"] is not None
        or any(target[name] is not None for name in ("owner_token", "owner_started_at", "owner_expires_at"))
        or target["attempts"] != request["expected_attempts"]
        or target["max_attempts"] != request["expected_max_attempts"]
        or target["owner_generation"] != request["expected_owner_generation"]
        or fingerprint(target["checkpoint"]) != request["expected_checkpoint_hash"]
        or target["checkpoint"].get("last_error_code") != request["expected_failure_code"]
    ):
        raise Conflict("Reviewed exhausted released media state changed")
    busy = _row(
        cur,
        """select owner_token from social.instagram_ingestion_dispatches
        where run_id=%s and stage='media' and target_key=%s and state in ('reserved','sending','accepted') limit 1""",
        (request["run_id"], request["target_key"]),
    )
    if busy:
        raise Conflict("Media dispatch must be reaped and reconciled before repair grant")
    return target


def _diagnostic(target):
    checkpoint = target["checkpoint"]
    diagnostic = checkpoint.get("worker_diagnostics") or {}
    last = diagnostic.get("last_failure") or {}
    return {
        "attempts": target["attempts"],
        "max_attempts": target["max_attempts"],
        "requests": target["requests"],
        "owner_generation": target["owner_generation"],
        "checkpoint_sha256": fingerprint(checkpoint),
        "next_attempt_at": target["next_attempt_at"].isoformat() if target["next_attempt_at"] else None,
        "blocked_reason": target["blocked_reason"],
        "last_error_code": checkpoint.get("last_error_code"),
        "failure_code": last.get("code") if _CODE.fullmatch(str(last.get("code", ""))) else None,
        "failure_category": last.get("category") if _CODE.fullmatch(str(last.get("category", ""))) else None,
    }


def extend_media_repair_budget(store: Store, *, intent_hash: str, apply: bool = False, **kwargs) -> dict:
    """Plan by default; atomically journal the one approved 3/3→3/4 grant.

    Only target max_attempts and the command journal may change. The operator
    neither changes run state nor bypasses source holds or comment recovery caps.
    """
    if type(apply) is not bool or not _SHA.fullmatch(intent_hash or ""):
        raise ValueError("Explicit apply flag and intent fingerprint required")
    request = {**_request(**kwargs), "intent_hash": intent_hash}
    qualified = qualify_installed_repair(kwargs["qualification"])
    binding = {
        name: request[name]
        for name in ("run_id", "account", "generation", "intent_hash", "target_key", "failed_revision")
    }
    binding.update(
        checkpoint_sha256=request["expected_checkpoint_hash"],
        owner_generation=request["expected_owner_generation"],
        attempts=request["expected_attempts"],
        max_attempts=request["expected_max_attempts"],
        failure_code=request["expected_failure_code"],
        repair_revision=kwargs["qualification"].repair_revision,
    )
    if qualified["diagnostic_binding"] != binding:
        raise Conflict("Parent-pinned diagnostic does not match exact repair request")
    if (
        qualified["installed_run_id"] != request["run_id"]
        or qualified["installed_intent_hash"] != intent_hash
        or qualified["installed_generation"] != request["generation"]
    ):
        raise Conflict("Installed January binding differs from exact repair request")
    key = "media-repair:" + fingerprint(
        {name: request[name] for name in ("run_id", "generation", "target_key")}
        | {"repair_revision": kwargs["qualification"].repair_revision}
    )
    command_hash = fingerprint(request)
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        if not apply:
            cur.execute("set transaction isolation level repeatable read read only")
        if apply:
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"instagram-command:{kwargs['account']}:{key}",),
            )
        previous = _row(
            cur,
            """select command_hash,result from social.instagram_ingestion_commands
            where account=%s and idempotency_key=%s""",
            (kwargs["account"], key),
        )
        if previous:
            if previous["command_hash"] != command_hash:
                raise Conflict("Repair command key already belongs to different review inputs")
            return previous["result"]
        target = _target_guard(cur, store, request, intent_hash, lock=apply)
        before = _diagnostic(target)
        result = {
            "version": 1,
            "command": request["command"],
            "run_id": kwargs["run_id"],
            "account": kwargs["account"],
            "generation": kwargs["generation"],
            "target_key": kwargs["target_key"],
            "idempotency_key": key,
            "before": before,
            "after": {**before, "max_attempts": 4},
            "qualification": qualified,
            "failed_revision": kwargs["failed_revision"],
            "reason": kwargs["reason"],
            "initiated_by": kwargs["initiated_by"],
            "applied": apply,
            "final_acceptance": False,
        }
        if not apply:
            return result
        # Verify installed files again at the acceptance boundary. The parent
        # owns process handover; this cannot attest a process from a JSON claim.
        qualify_installed_repair(kwargs["qualification"])
        cur.execute(
            """update social.instagram_ingestion_targets set max_attempts=4
            where run_id=%s and stage='media' and target_key=%s and outcome='unresolved'
            and attempts=3 and max_attempts=3 and owner_generation=%s and owner_token is null
            and owner_started_at is null and owner_expires_at is null and blocked_reason is null returning *""",
            (kwargs["run_id"], kwargs["target_key"], kwargs["expected_owner_generation"]),
        )
        after = dict(cur.fetchone())
        if {**after, "max_attempts": 3} != target:
            raise Conflict("Repair budget changed unrelated target state")
        cur.execute("select clock_timestamp() as now")
        result["accepted_at"] = cur.fetchone()["now"].isoformat()
        cur.execute(
            """insert into social.instagram_ingestion_commands
            (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
            (kwargs["account"], key, command_hash, kwargs["run_id"], stable_json(result)),
        )
        return result
