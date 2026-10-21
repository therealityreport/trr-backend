#!/usr/bin/env python3
"""One-shot Instagram reply comparison, anonymous arms before pinned login.

Default invocation prints the plan without database, provider or Instagram IO.
Use the same private ledger and output directory for every arm. No run is resumed
and no canonical data is written. An uncertain dispatch must be reconciled rather
than repeated. Source request counts are accounting, not an artificial quota.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

FROZEN_RUN = "dbc80df9-0df7-43ca-9abc-2f1d1e393056"
SUBJECTS = (
    ("DY1vXi-kRBb", "17966467277918174"),
    ("DUO2bm4EklP", "17843046855688281"),
    ("DTrXsaGEt7e", "18546860833063860"),
)


def write_once(path, payload):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(payload, stream, sort_keys=True, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def require_anonymous_failures(output):
    """Missing, uncertain, or successful anonymous evidence never enables login."""
    from trr_backend.socials.instagram.child_reply_probe import conclusive_anonymous_failure

    for arm in ("mac", "decodo", "modal"):
        path = output / f"{arm}.result.json"
        if not path.is_file():
            raise ValueError("Anonymous comparison is incomplete")
        result = json.loads(path.read_text())
        if (
            result.get("arm") != arm
            or result.get("status") != "completed"
            or result.get("original_run_unchanged") is not True
            or not result.get("subjects")
            or any(
                subject.get("authenticated") is not False
                or subject.get("requests", 0) < 1
                or subject.get("positive_reply_proof") is not False
                or not conclusive_anonymous_failure(subject, require_rest=arm == "mac")
                for subject in result["subjects"]
            )
        ):
            raise ValueError("Anonymous comparison has not established fallback eligibility")


def _proxy(comparison_id):
    import httpx

    from trr_backend.socials.instagram._proxy_sessions import apply_decodo_session_affinity

    settings = {name: os.environ.get(name) for name in ("DECODO_USERNAME", "DECODO_PASSWORD", "DECODO_GATEWAY")}
    if not all(settings.values()):
        raise ValueError("Proxy configuration missing")
    gateway = settings["DECODO_GATEWAY"]
    parsed = urlsplit(gateway if "://" in gateway else "http://" + gateway)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or not parsed.port
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Proxy gateway invalid")
    username, mode = apply_decodo_session_affinity(
        settings["DECODO_USERNAME"], use_sticky_proxy=True, session_ttl_seconds=600, session_id=comparison_id
    )
    if mode == "rotating":
        raise ValueError("Sticky proxy session required")
    return httpx.Proxy(
        f"{parsed.scheme}://{parsed.hostname}:{parsed.port}", auth=(username, settings["DECODO_PASSWORD"])
    )


def _store_and_guard(comparison_id):
    from psycopg2.extras import RealDictCursor

    from scripts.ingestion.local_driver import _project_dsn
    from trr_backend.db.connection import (
        resolve_database_url_candidate_details,
        resolve_session_database_url_candidate_details,
    )
    from trr_backend.ingestion.fixed_validation import _require_exclusive_cursor
    from trr_backend.ingestion.workflow_store import WorkflowStore

    ordinary, sessions = resolve_database_url_candidate_details(), resolve_session_database_url_candidate_details()
    if (
        not ordinary
        or not sessions
        or any(not _project_dsn(item["url"]) for item in ordinary)
        or any(not _project_dsn(item["url"], session_only=True) for item in sessions)
    ):
        raise ValueError("Database project mismatch")
    store = WorkflowStore(execution_placement="local")

    def guard():
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            _require_exclusive_cursor(cur, comparison_id)
            cur.execute(
                "select row_to_json(r) as record from social.instagram_ingestion_runs r where run_id=%s", (FROZEN_RUN,)
            )
            row = cur.fetchone()
            if not row or row["record"]["state"] != "cancelled":
                raise ValueError("Frozen run must remain cancelled")
            records = {"run": row["record"]}
            cur.execute(
                """select row_to_json(t) as record from social.instagram_ingestion_targets t
                           where run_id=%s order by stage,target_key""",
                (FROZEN_RUN,),
            )
            records["targets"] = [item["record"] for item in cur.fetchall()]
        return hashlib.sha256(json.dumps(records, sort_keys=True, default=str).encode()).hexdigest()

    return guard


def execute(options):
    if options.arm == "modal" and (
        os.environ.get("MODAL_PROFILE") != "admin-56995" or os.environ.get("MODAL_ENVIRONMENT") != "main"
    ):
        raise ValueError("Explicit admin-56995 profile and main environment required")
    import httpx

    from trr_backend.ingestion.credential_snapshot import CredentialSnapshot
    from trr_backend.ingestion.fixed_validation import AccountBudget
    from trr_backend.socials.instagram.child_reply_probe import conclusive_anonymous_failure, probe_child_reply_access

    output = options.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    if options.arm == "login":
        require_anonymous_failures(output)
    if (output / f"{options.arm}.started.json").exists():
        raise ValueError("One-shot already started; reconcile existing evidence")
    comparison_id = str(uuid4())
    guard = _store_and_guard(comparison_id)
    original_hash = guard()
    proxy = _proxy(comparison_id) if options.arm == "decodo" else None
    snapshot = CredentialSnapshot.from_environ() if options.arm == "login" else None
    budget = AccountBudget(options.budget_path, "bravotv")
    subjects = SUBJECTS[:2] if options.arm == "mac" else SUBJECTS[:1]
    report = {
        "schema": "instagram-child-reply-comparison-v1",
        "arm": options.arm,
        "comparison_id": comparison_id,
        "started_at": datetime.now(UTC).isoformat(),
        "original_run": FROZEN_RUN,
        "original_run_hash": original_hash,
        "original_run_unchanged": False,
        "status": "started",
        "subjects": [],
    }

    def before_request(kind):
        if guard() != original_hash:
            raise ValueError("Frozen run changed")
        budget.consume("reply_comparison_" + options.arm + "_" + kind)

    with budget.stream():
        write_once(output / f"{options.arm}.started.json", report)
        try:
            if options.arm == "modal":
                import modal

                from scripts.modal.deploy_backend import verify_required_workspace

                verify_required_workspace()
                function = modal.Function.from_name(
                    "trr-backend-jobs", "probe_instagram_child_reply_access", environment_name="main"
                )
                # Conservative reservations survive a caller/worker crash; never refund uncertain sends.
                for kind in ("bootstrap_reserved", "348_reserved", "362_reserved"):
                    before_request(kind)
                call = function.spawn(*subjects[0])
                write_once(output / "modal.dispatch.json", {"call_id": call.object_id, "environment": "main"})
                report["subjects"].append(call.get(timeout=150))
            else:
                for shortcode, parent in subjects:
                    with httpx.Client(
                        transport=httpx.HTTPTransport(proxy=proxy, retries=0, trust_env=False),
                        trust_env=False,
                        follow_redirects=False,
                        timeout=20,
                        headers={"accept-encoding": "gzip, deflate"},
                    ) as client:
                        result = probe_child_reply_access(
                            shortcode,
                            parent,
                            client=client,
                            before_request=before_request,
                            include_rest=options.arm in {"mac", "login"},
                            include_embed=options.arm == "mac",
                            credential_snapshot=snapshot,
                        )
                    report["subjects"].append(result)
                    if result.get("stop_reason") in {"server_retry_after", "http_rate_limited", "source_auth_failed"}:
                        break
            report["status"] = (
                "completed"
                if options.arm == "login"
                or any(subject.get("positive_reply_proof") for subject in report["subjects"])
                or all(
                    conclusive_anonymous_failure(subject, require_rest=options.arm == "mac")
                    for subject in report["subjects"]
                )
                else "indeterminate"
            )
        except (Exception, KeyboardInterrupt) as exc:
            # Exception messages may include secrets; retain the stable type only.
            report["status"] = "uncertain" if options.arm == "modal" else "failed"
            report["error_type"] = type(exc).__name__
        finally:
            try:
                report["original_run_unchanged"] = guard() == original_hash
            except Exception:
                report["original_run_unchanged"] = False
            if not report["original_run_unchanged"]:
                report["status"] = "failed"
            write_once(output / f"{options.arm}.result.json", report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "arm": options.arm,
                "positive_reply_proof": any(row.get("positive_reply_proof") for row in report["subjects"]),
                "result": str(output / f"{options.arm}.result.json"),
            }
        )
    )
    return 0 if report["status"] == "completed" else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=("mac", "decodo", "modal", "login"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budget-path", type=Path, required=True)
    parser.add_argument("--apply-once", action="store_true")
    options = parser.parse_args(argv)
    if not options.apply_once:
        print(
            json.dumps(
                {
                    "status": "plan",
                    "arm": options.arm,
                    "network_sent": False,
                    "subjects": SUBJECTS[:2] if options.arm == "mac" else SUBJECTS[:1],
                    "login_requires_all_anonymous_failures": True,
                }
            )
        )
        return 0
    try:
        return execute(options)
    except Exception as exc:
        print(json.dumps({"status": "held", "error_type": type(exc).__name__, "automatic_retry": False}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
