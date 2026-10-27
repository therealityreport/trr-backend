#!/usr/bin/env python3
"""One explicit January recovery window, with durable receipts and native restoration.

Default is read-only preparation. --execute is the operator's explicit start.
Credentials stay in the existing environment file and native launch command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import re
import shlex
import signal
import subprocess
import sys
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
RUN = "2aef72ed-110c-4af9-b2b3-4a45da170a32"
INTENT = "10746e1197ea8b5f7272a90c662a2d1ebfd6e36a8a01cda4be1c73be428b9fc4"
ENV = Path.home() / "Library/Application Support/TRR/instagram-worker/config/fixed-post-98c6604b.env"
PLIST = Path.home() / "Library/LaunchAgents/com.trr.instagram-local-worker.plist"


def save(directory, name, value):
    path = directory / name
    with path.open("x") as stream:
        json.dump(value, stream, default=str, sort_keys=True)
    path.chmod(0o600)
    return path


def connect(*, readonly=True):
    from urllib.parse import urlsplit

    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(ENV, override=True)
    url = os.environ.get("TRR_DB_URL") or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    parts = urlsplit(url)
    if "vwxfvzutyufrkhfgoeaa" not in (parts.hostname or "") + (parts.username or ""):
        raise RuntimeError("Wrong database project")
    return psycopg2.connect(
        url,
        connect_timeout=5,
        options=(
            "-c statement_timeout=30000 -c lock_timeout=2000"
            + (" -c default_transaction_read_only=on" if readonly else "")
        ),
    )


def transaction():
    from contextlib import contextmanager

    @contextmanager
    def scope():
        connection = connect(readonly=False)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    return scope()


def grants():
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("""select pid from pg_locks where locktype='advisory' and granted
            and classid=48112 and objid=20261017 and objsubid=2""")
        return [row[0] for row in cur.fetchall()]


def processes(pid):
    rows = [
        tuple(map(int, line.split()))
        for line in subprocess.check_output(["ps", "-axo", "pid=,ppid="], text=True).splitlines()
        if line.strip()
    ]
    found = {pid}
    while True:
        expanded = found | {child for child, parent in rows if parent in found}
        if expanded == found:
            return found
        found = expanded


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def lease_record(path, pid):
    if not path.is_file():
        return None
    for line in reversed(path.read_text().splitlines()[-100:]):
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("status") == "local_worker_lease_acquired" and row.get("pid") == pid:
            return row
    return None


def stop_native(label):
    subprocess.run(["launchctl", "bootout", label], capture_output=True)
    end = time.monotonic() + 5
    while True:
        state = subprocess.run(["launchctl", "print", label], capture_output=True)
        if state.returncode != 0 or time.monotonic() >= end:
            break
        time.sleep(0.1)
    if state.returncode == 0:
        raise RuntimeError("Native bootout is not confirmed")
    # A failed acknowledgment after unregistering is a proved stop, not a
    # reason to abandon the finalizer. Process/grant drain is checked separately.
    return True


def shutdown_native_attempt(label, record_attempt):
    """Engage reconciliation even if launchd's acknowledgment is delayed/lost."""
    record_attempt()
    return stop_native(label)


def wait_native_drain(label, pid, *, timeout=45):
    end = time.monotonic() + timeout
    stable = 0
    owned = processes(pid)
    while time.monotonic() < end:
        state = subprocess.run(["launchctl", "print", label], capture_output=True)
        absent = state.returncode != 0
        processes_absent = not any(alive(value) for value in owned)
        stable = stable + 1 if absent and processes_absent and not grants() else 0
        if stable == 2:
            return
        time.sleep(0.5)
    raise RuntimeError("Prior native registration/process/grant remains; restoration held")


def reconcile_window(source_revision, workflow_version, *, expected_generation=2, stop_policy=None):
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.bounded_recovery import window_key

    with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select result from social.instagram_ingestion_commands
            where account='bravotv' and idempotency_key=%s""",
            (window_key(RUN, expected_generation + 1),),
        )
        row = cur.fetchone()
    if row is None:
        return None
    window = row["result"]
    expected = {
        "run_id": RUN,
        "account": "bravotv",
        "expected_generation": expected_generation,
        "generation": expected_generation + 1,
        "expected_intent_hash": INTENT,
        "expected_workflow_version": workflow_version,
        "source_revision": source_revision,
        "initiated_by": "january-useful-progress-operator" if stop_policy else "january-one-hour-operator",
        "priority": "comments_first",
        "duration_seconds": stop_policy["max_duration_seconds"] if stop_policy else 3600,
    }
    if stop_policy:
        expected["stop_policy"] = stop_policy
    if any(window.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Uncertain authorization belongs to a different window")
    return window


def stop_owned_child(child):
    """Signal errors do not skip reaping; live uncertainty still blocks restore."""
    owned = processes(child.pid)
    errors = []

    def send(sig):
        try:
            os.killpg(child.pid, sig)
        except OSError as error:
            errors.append({"signal": int(sig), "error_class": type(error).__name__})

    send(signal.SIGTERM)
    try:
        child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        send(signal.SIGKILL)
        try:
            child.kill()
        except OSError as error:
            errors.append({"signal": int(signal.SIGKILL), "error_class": type(error).__name__})
        child.wait(timeout=3)
    # Activity/audit children have independent parent-loss guards. Capture their
    # identities before signaling so reparenting cannot hide a surviving child.
    end = time.monotonic() + 3
    while any(alive(pid) for pid in owned) and time.monotonic() < end:
        time.sleep(0.05)
    if any(alive(pid) for pid in owned):
        raise RuntimeError("Owned child process remains; restoration held")
    return {"returncode": child.returncode, "signal_errors": errors, "owned_processes_absent": sorted(owned)}


def snapshot():
    from psycopg2.extras import RealDictCursor

    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("set transaction isolation level repeatable read read only")
            cur.execute("select * from social.instagram_ingestion_runs where run_id=%s", (RUN,))
            run = dict(cur.fetchone())
            if run["account"] != "bravotv" or run["intent_hash"] != INTENT or run["execution_placement"] != "local":
                raise RuntimeError("January binding changed")
            cur.execute(
                """select stage,target_key,outcome,attempts,max_attempts,requests,blocked_reason,
                next_attempt_at,owner_token,owner_expires_at,
                encode(sha256(convert_to((to_jsonb(t)-'owner_token'-'owner_expires_at'-'owner_started_at')::text,
                    'UTF8')),'hex') as preservation_sha256,
                jsonb_build_object(
                    'last_error_code',checkpoint->'last_error_code',
                    'worker_diagnostics',jsonb_build_object(
                        'last_failure',checkpoint#>'{worker_diagnostics,last_failure}',
                        'committed_pages_since_failure',checkpoint#>'{worker_diagnostics,committed_pages_since_failure}'),
                    'comment_pages',jsonb_build_object(
                        'lanes',checkpoint#>'{comment_pages,lanes}',
                        'reply_access',checkpoint#>'{comment_pages,reply_access}'),
                    'public_reply_failures',checkpoint->'public_reply_failures') as reporting_checkpoint,
                checkpoint#>'{comment_pages,settlement}' as settlement
                ,checkpoint#>'{comment_pages,expected}' as expected,
                array(select jsonb_object_keys(coalesce(
                    checkpoint#>'{comment_pages,observed}','{}'::jsonb))) as observed_ids
                from social.instagram_ingestion_targets t where run_id=%s order by stage,target_key""",
                (RUN,),
            )
            targets = [dict(row) for row in cur.fetchall()]
            for target in targets:
                # Never save raw cursor values, request envelopes or source URLs.
                target["diagnostics"] = target_diagnostics(target.pop("reporting_checkpoint"))
            cur.execute(
                """select p.shortcode,c.comment_id,c.parent_comment_external_id,
                parent.comment_id as linked_parent,c.is_missing,
                to_jsonb(c)->'hosted_media_urls' as hosted_media_urls,
                to_jsonb(c)->>'hosted_author_profile_pic_url' as hosted_avatar,
                encode(sha256(convert_to(coalesce(to_jsonb(c.media_urls),'[]'::jsonb)::text,
                    'UTF8')),'hex') as media_sha256
                from social.instagram_comments c join social.instagram_posts p on p.id=c.post_id
                left join social.instagram_comments parent on parent.id=c.parent_comment_id
                where p.shortcode in (select target_key from social.instagram_ingestion_targets
                    where run_id=%s and stage='comments') order by p.shortcode,c.comment_id,c.id""",
                (RUN,),
            )
            comments = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """select page_id,evidence_id from social.instagram_ingestion_comment_pages
                where run_id=%s order by page_id""",
                (RUN,),
            )
            pages = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """select receipt_id,stage,target_key,canonical_identity
                from social.instagram_ingestion_receipts where run_id=%s""",
                (RUN,),
            )
            receipts = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """select target_key,receipt_id,evidence_id,canonical_identity
                from social.instagram_ingestion_replay_receipts where run_id=%s""",
                (RUN,),
            )
            replays = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """select t.target_key,p.id as legacy_post_id,sp.id as canonical_post_id,
                p.asset_manifest,b.asset_manifest as sidecar_manifest,a.position,a.hosted_url,a.hosted_thumbnail_url,
                encode(sha256(convert_to(to_jsonb(p)::text,'UTF8')),'hex') as post_sha256,
                encode(sha256(convert_to(coalesce(to_jsonb(a),'null'::jsonb)::text,'UTF8')),'hex') as asset_sha256
                from social.instagram_ingestion_targets t join social.instagram_posts p on p.shortcode=t.target_key
                join social.social_posts sp on sp.platform='instagram' and sp.source_id=p.shortcode
                left join social.instagram_post_payloads b on b.post_id=p.id
                left join social.social_post_media_assets a on a.platform='instagram' and a.post_id=sp.id
                where t.run_id=%s and t.stage='media' order by t.target_key,a.position,a.id""",
                (RUN,),
            )
            media = [dict(row) for row in cur.fetchall()]
            return {
                "at": datetime.now(UTC).isoformat(),
                "run": run,
                "targets": targets,
                "comments": comments,
                "pages": pages,
                "receipts": receipts,
                "replays": replays,
                "media": media,
            }
    finally:
        conn.close()


def target_diagnostics(checkpoint):
    """Bounded factual projection; missing old snapshot fields remain unknown.

    Failure facts are historical, not evidence that a target is currently held.
    Lane coverage is reported independently from branch holds and target outcome.
    """

    def token(value):
        return value if isinstance(value, str) and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]{0,79}", value) else None

    def number(value):
        return value if type(value) is int and value >= 0 else None

    def timestamp(value):
        if not isinstance(value, str) or len(value) > 40:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
        except ValueError:
            return None

    def presence(value):
        if value is None or value == {}:
            return "absent"
        return "present" if isinstance(value, dict) else "unknown"

    if not isinstance(checkpoint, dict):
        return {"availability": "unknown"}
    pages = checkpoint.get("comment_pages")
    pages = pages if isinstance(pages, dict) else {}
    worker = checkpoint.get("worker_diagnostics")
    worker = worker if isinstance(worker, dict) else {}
    failure = worker.get("last_failure")
    access = pages.get("reply_access")
    technical = checkpoint.get("public_reply_failures")
    lanes = pages.get("lanes")
    proof = {"availability": presence(lanes)}
    if isinstance(lanes, dict):
        proof["total"] = len(lanes)
        for field in ("exhausted", "repeated", "started_from_root"):
            proof[field] = {
                "true": sum(isinstance(lane, dict) and lane.get(field) is True for lane in lanes.values()),
                "false": sum(isinstance(lane, dict) and lane.get(field) is False for lane in lanes.values()),
                "unknown": sum(
                    not isinstance(lane, dict) or type(lane.get(field)) is not bool for lane in lanes.values()
                ),
            }
        # The maintained lane schema stores cursor history, not a status/outcome
        # or a single current cursor. Do not infer exhaustion from empty history.
        proof["cursor_history"] = {
            "known_lanes": sum(
                isinstance(lane, dict) and isinstance(lane.get("cursors"), list) for lane in lanes.values()
            ),
            "entries": sum(
                len(lane["cursors"])
                for lane in lanes.values()
                if isinstance(lane, dict) and isinstance(lane.get("cursors"), list)
            ),
        }
    holds = {"availability": presence(technical)}
    if isinstance(technical, dict):
        holds.update(
            total=len(technical),
            technical_hold=sum(
                isinstance(value, dict) and value.get("technical_hold") is True for value in technical.values()
            ),
            branches=[
                {
                    "parent_comment_id": parent
                    if isinstance(parent, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,80}", parent)
                    else None,
                    "code": token(value.get("code")),
                    "technical_hold": value.get("technical_hold")
                    if type(value.get("technical_hold")) is bool
                    else None,
                    "failure_count": number(value.get("failure_count")),
                    "next_attempt_at": timestamp(value.get("next_attempt_at")),
                    "retry_source": token(value.get("retry_source")),
                }
                for parent, value in list(technical.items())[:100]
                if isinstance(value, dict)
            ],
            branches_truncated=len(technical) > 100,
        )
    return {
        "availability": "known",
        "last_error_code": token(checkpoint.get("last_error_code")),
        "last_failure": {
            "code": token(failure.get("code")),
            "category": token(failure.get("category")),
            "exception_class": token(failure.get("exception_class")),
            "observed_at": timestamp(failure.get("observed_at")),
            "owner_generation": number(failure.get("owner_generation")),
        }
        if isinstance(failure, dict)
        else None,
        "committed_pages_since_failure": number(worker.get("committed_pages_since_failure")),
        "reply_access": {
            "availability": "present",
            "code": token(access.get("code")),
            "attempts": number(access.get("attempts")),
            "retry_source": token(access.get("retry_source")),
            "next_attempt_at": timestamp(access.get("next_attempt_at")),
            "lane_count": len(access["lanes"]) if isinstance(access.get("lanes"), list) else None,
        }
        if isinstance(access, dict) and access
        else {"availability": presence(access)},
        "public_reply_failures": holds,
        "lane_proof": proof,
    }


def report(before, after, window):
    previous = {(row["shortcode"], row["comment_id"]) for row in before["comments"]}
    previous_missing = {(row["shortcode"], row["comment_id"]): row["is_missing"] for row in before["comments"]}
    new = [row for row in after["comments"] if (row["shortcode"], row["comment_id"]) not in previous]
    old_pages = {str(row["page_id"]) for row in before["pages"]}
    old_evidence = {str(row["evidence_id"]) for row in before["pages"]}
    old_replays = {str(row["receipt_id"]) for row in before["replays"]}
    from psycopg2.extras import RealDictCursor

    fresh = set()
    with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select p.page_id,p.evidence_id,p.target_key,p.canonical_identity,e.captured_at from
            social.instagram_ingestion_comment_pages p join social.instagram_ingestion_evidence e using(evidence_id)
            where p.run_id=%s and e.captured_at>=%s and e.captured_at=p.observed_at
            and e.content_hash=p.content_hash""",
            (RUN, window["started_at"]),
        )
        for row in cur.fetchall():
            if str(row["page_id"]) not in old_pages and str(row["evidence_id"]) not in old_evidence:
                fresh.update(
                    (row["target_key"], str(value))
                    for value in row["canonical_identity"].get("observed_comment_ids", [])
                )
    replayed = set()
    for row in after["replays"]:
        if str(row["receipt_id"]) not in old_replays and str(row["evidence_id"]) in old_evidence:
            # Receipt identities include the post shortcode in their canonical identity.
            identity = row["canonical_identity"]
            target = row["target_key"]
            replayed.update((target, str(value)) for value in identity.get("observed_comment_ids", []))
    per_post = {}
    saved_native = {
        (row["target_key"], str(value))
        for row in before["targets"]
        if row["stage"] == "comments"
        for value in row.get("observed_ids", [])
    }
    for row in new:
        key = (row["shortcode"], row["comment_id"])
        classification = (
            "both"
            if key in fresh and key in replayed
            else "fresh_source"
            if key in fresh
            else "retained_replay"
            if key in replayed
            else "unattributed"
        )
        counts = per_post.setdefault(row["shortcode"], {})
        kind = "replies" if row["parent_comment_external_id"] or row["linked_parent"] else "top_level_comments"
        counts[classification + "_" + kind] = counts.get(classification + "_" + kind, 0) + 1
        if key in saved_native:
            counts["previously_saved_native_ids_restored"] = counts.get("previously_saved_native_ids_restored", 0) + 1
    old_targets = {row["target_key"]: row for row in before["targets"] if row["stage"] == "comments"}
    count_changes = []
    for row in after["targets"]:
        if row["stage"] != "comments":
            continue
        old = old_targets[row["target_key"]]
        old_expected = (old["settlement"] or {}).get("expected_total", old.get("expected"))
        new_expected = (row["settlement"] or {}).get("expected_total", row.get("expected"))
        observed_delta = len(row.get("observed_ids", [])) - len(old.get("observed_ids", []))
        if observed_delta or old_expected != new_expected:
            count_changes.append(
                {
                    "shortcode": row["target_key"],
                    "observed_id_change": observed_delta,
                    "expected_before": old_expected,
                    "expected_after": new_expected,
                }
            )

    def totals(snapshot):
        gaps = [
            row["settlement"]["count_gap"]
            for row in snapshot["targets"]
            if row["stage"] == "comments" and row["settlement"] and row["settlement"].get("count_gap", 0) > 0
        ]
        return {
            "canonical_comments_and_replies": len(snapshot["comments"]),
            "known_gap_posts": len(gaps),
            "known_gap_total": sum(gaps),
            "accounted_requests": sum(row["requests"] for row in snapshot["targets"]),
            "comment_pages": len(snapshot["pages"]),
            "media_committed": sum(
                row["stage"] == "media" and row["outcome"] == "committed" for row in snapshot["targets"]
            ),
        }

    restored = sum(
        row["is_missing"] is False
        for row in after["comments"]
        if previous_missing.get((row["shortcode"], row["comment_id"])) is True
    )
    return {
        "window": window,
        "before": totals(before),
        "after": totals(after),
        "new_canonical_ids": len(new),
        "previously_missing_ids_restored": restored,
        "per_post": per_post,
        "count_observation_changes": count_changes,
        "target_diagnostics": [
            {
                "stage": row["stage"],
                "target_key": row["target_key"],
                "outcome": row.get("outcome"),
                "blocked_reason": row.get("blocked_reason")
                if isinstance(row.get("blocked_reason"), str)
                and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", row["blocked_reason"])
                else None,
                "next_attempt_at": row.get("next_attempt_at"),
                "diagnostics": row.get("diagnostics", {"availability": "unknown"}),
            }
            for row in after["targets"]
        ],
        "new_immutable_receipts": len(after["receipts"]) - len(before["receipts"]),
        "final_acceptance": False,
    }


def transition_snapshot():
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """select encode(sha256(convert_to(coalesce(string_agg(
            encode(sha256(convert_to((to_jsonb(t)-'owner_token'-'owner_expires_at'-'owner_started_at')::text,
                'UTF8')),'hex'),'' order by stage,target_key),''),'UTF8')),'hex'),count(*),sum(requests)
            from social.instagram_ingestion_targets t where run_id=%s""",
            (RUN,),
        )
        targets = cur.fetchone()
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (RUN,))
        return {"targets": targets, "receipts": cur.fetchone()[0]}


def merge_frontier(frontiers, target, observed, supported, proofs, best):
    """Window-local scheduling counters; never write these unions as scan proof."""
    saved = frontiers.setdefault(target, {"observed": set(), "supported": set(), "proofs": set(), "best": 0})
    saved["observed"].update(observed)
    saved["supported"].update(supported)
    saved["proofs"].update(proofs)
    saved["best"] = max(saved["best"], best)
    return {
        "observed_ids": len(saved["observed"]),
        "supported_ids": len(saved["supported"]),
        "proof_count": len(saved["proofs"]),
        "best_observed_total": saved["best"],
    }


def progress(window, frontiers=None):
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.bounded_recovery_phase import phase

    frontiers = {} if frontiers is None else frontiers
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """select sum(requests),count(*) filter(where stage='media' and outcome='committed')
            from social.instagram_ingestion_targets where run_id=%s""",
            (RUN,),
        )
        requests, media_count = cur.fetchone()
        cur.execute(
            """select count(*) from social.instagram_ingestion_comment_pages
            where run_id=%s and committed_at>=%s""",
            (RUN, window["started_at"]),
        )
        pages = cur.fetchone()[0]
        cur.execute(
            """select count(*) from social.instagram_comments c
            join social.instagram_posts p on p.id=c.post_id where exists(
                select 1 from social.instagram_ingestion_targets t
                where t.run_id=%s and t.stage='comments' and t.target_key=p.shortcode)""",
            (RUN,),
        )
        canonical_ids = cur.fetchone()[0]
    with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        current_phase = phase(cur, window)
        cur.execute(
            """select target_key,
            array(select jsonb_object_keys(coalesce(checkpoint#>'{comment_pages,observed}','{}'::jsonb))) as observed,
            array(select key from jsonb_each(coalesce(checkpoint#>'{comment_pages,observed}','{}'::jsonb))
                where not coalesce((value->>'unsupported_reply_topology')::boolean,false)) as supported,
            array(select key from jsonb_each(coalesce(checkpoint#>'{comment_pages,lanes}','{}'::jsonb))
                where value->>'exhausted'='true') as proofs,
            checkpoint->'public_recovery' as recovery
            from social.instagram_ingestion_targets where run_id=%s and stage='comments'""",
            (RUN,),
        )
        targets = {}
        from trr_backend.ingestion.comment_recovery import _progress_frontier

        for row in cur.fetchall():
            retained = _progress_frontier(row["recovery"] or {})
            observed = set(row["observed"]) | set(retained["observed_ids"])
            supported = set(row["supported"]) | set(retained["supported_ids"])
            targets[row["target_key"]] = merge_frontier(
                frontiers,
                row["target_key"],
                observed,
                supported,
                row["proofs"],
                max(len(row["observed"]), retained["best_observed_total"]),
            )
        return {
            "at": datetime.now(UTC).isoformat(),
            "accounted_requests": requests,
            "new_comment_pages": pages,
            "media_committed": media_count,
            "phase": current_phase,
            "comments": {"canonical_ids": canonical_ids, "targets": targets},
            "media": {"committed_targets": media_count},
        }


def media_idle(path, window_id):
    for line in reversed(path.read_text().splitlines()[-200:]):
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("status") == "bounded_media_idle" and row.get("window_id") == window_id:
            return True
    return False


def continue_comment_progress(
    tracker, directory, *, expected_generation, metrics, now, wall_now, media_repair_source=None
):
    """Carry a failed predecessor's useful clock into its exact next generation.

    This is measurement continuity only. The new generation still needs its own
    authorization, drained ownership, source binding and immutable phase journal.
    """
    from trr_backend.ingestion.bounded_recovery import USEFUL_PROGRESS_POLICY

    window = json.loads((directory / "window.json").read_text())
    held = json.loads((directory / "held.json").read_text())
    ended = json.loads((directory / "worker-end.json").read_text())
    if (
        window.get("run_id") != RUN
        or window.get("account") != "bravotv"
        or window.get("expected_intent_hash") != INTENT
        or window.get("generation") != expected_generation - 1
        or window.get("stop_policy") != USEFUL_PROGRESS_POLICY
        or held.get("run_id") != RUN
        or held.get("generation") != expected_generation
        or held.get("state") != "cancelled"
        or held.get("execution_hold_reason") != "bounded_recovery_window_expired"
        or ended.get("stop_reason") != ("media_useful_progress_stalled" if media_repair_source else "worker_exited")
    ):
        raise RuntimeError("Progress continuation is not the exact failed predecessor")
    baseline = json.loads((directory / "progress-baseline.json").read_text())
    if baseline.get("phase") != "comments":
        raise RuntimeError("Cannot resume a non-comment baseline")
    seed_path = directory / "comment-progress-seed.json"
    seed = json.loads(seed_path.read_text()) if seed_path.exists() else None
    if seed is None and any((directory / name).exists() for name in ("continued-progress.json", "comments-stall.json")):
        raise RuntimeError("Predecessor continuation seed is missing")
    if seed is not None and (
        seed.get("version") != 1
        or seed.get("window_id") != window["window_id"]
        or type(seed.get("comment_phase_ended")) is not bool
        or seed.get("stop_reason") not in {"running", "complete", "useful_progress_stalled"}
        or seed["comment_phase_ended"] != (seed["stop_reason"] != "running")
    ):
        raise RuntimeError("Invalid predecessor continuation seed")
    if media_repair_source and (
        not seed
        or not seed["comment_phase_ended"]
        or not isinstance(window.get("source_revision"), str)
        or media_repair_source == window["source_revision"]
    ):
        raise RuntimeError("Media repair needs ended comments and a different verified source")
    frames = [{"at": seed["last_useful_at"], "comments": seed["metrics"]}] if seed else [baseline]
    for path in directory.glob("progress-*.json"):
        if path.name == "progress-baseline.json":
            continue
        frame = json.loads(path.read_text())
        if frame.get("phase") != "comments":
            if media_repair_source and frame.get("phase") == "media":
                continue
            raise RuntimeError("Cannot resume comment clock after media phase")
        if frame.get("stop_decision", {}).get("useful_progress") is True:
            frames.append(frame)
    frame = max(frames, key=lambda value: datetime.fromisoformat(value["at"]))
    last_useful = datetime.fromisoformat(frame["at"])
    started = datetime.fromisoformat(window["started_at"])
    if last_useful.tzinfo is None or last_useful > wall_now or (seed is None and last_useful < started):
        raise RuntimeError("Invalid predecessor progress clock")
    elapsed = (wall_now - last_useful).total_seconds()
    best = merge_comment_metrics(*(value["comments"] for value in frames))
    already_ended = bool(seed and seed["comment_phase_ended"])
    phase = recorded_recovery_phase(window)
    if media_repair_source and phase != "media":
        raise RuntimeError("Predecessor never entered the durable media phase")
    already_ended |= phase != "comments"
    previous_reason = seed["stop_reason"] if seed and seed["comment_phase_ended"] else "complete"
    tracker.observe("comments", best, now=now - elapsed, complete=already_ended and previous_reason == "complete")
    if already_ended and previous_reason == "useful_progress_stalled":
        if elapsed < tracker.stall_seconds:
            raise RuntimeError("Ended predecessor stall clock is inconsistent")
        tracker.observe("comments", best, now=now)
    decision = tracker.observe("comments", metrics, now=now)
    carried = {
        "version": 1,
        "window_id": window["window_id"],
        "last_useful_at": last_useful.isoformat(),
        "metrics": best,
        "comment_phase_ended": already_ended,
        "stop_reason": previous_reason if already_ended else "running",
    }
    carried = updated_comment_seed(carried, metrics, decision, wall_now)
    return {
        "previous_window_id": window["window_id"],
        "last_useful_at": carried["last_useful_at"],
        "decision": decision,
        "seed": carried,
        "media_repair_source": media_repair_source,
    }


def recorded_recovery_phase(window):
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.bounded_recovery_phase import phase

    with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        return phase(cur, window)


def merge_comment_metrics(*frames):
    best = {"canonical_ids": 0, "targets": {}}
    for frame in frames:
        best["canonical_ids"] = max(best["canonical_ids"], frame.get("canonical_ids", 0))
        for key, counters in frame.get("targets", {}).items():
            target = best["targets"].setdefault(key, {})
            for metric, value in counters.items():
                target[metric] = max(target.get(metric, 0), value)
    return best


def updated_comment_seed(seed, metrics, decision, wall_now):
    value = dict(seed, metrics=merge_comment_metrics(seed["metrics"], metrics))
    if decision["useful_progress"]:
        value["last_useful_at"] = wall_now.isoformat()
    if decision["reason"] != "running":
        value.update(comment_phase_ended=True, stop_reason=decision["reason"])
    return value


def replace_comment_seed(directory, seed):
    temporary = save(directory, f".comment-seed-{time.time_ns()}.json", seed)
    os.replace(temporary, directory / "comment-progress-seed.json")


def public_base_urls(public_bases):
    """Accept verified storage-binding entries and older URL-only bindings."""
    urls = tuple(value.get("url") if isinstance(value, dict) else value for value in public_bases)
    if any(not isinstance(value, str) or not value for value in urls):
        raise ValueError("Configured public storage base has no URL")
    return urls


def media_slots(snapshot, public_bases):
    public_bases = public_base_urls(public_bases)
    slots = {}

    def visit(value, prefix):
        if isinstance(value, dict):
            for key, child in value.items():
                visit(child, prefix + (str(key),))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, prefix + (str(index),))
        elif isinstance(value, str) and any(value.startswith(base.rstrip("/") + "/") for base in public_bases):
            slots[prefix] = value

    seen = set()
    for row in snapshot["media"]:
        if row["target_key"] not in seen:
            visit(row.get("asset_manifest") or row.get("sidecar_manifest"), (row["target_key"], "post"))
            seen.add(row["target_key"])
    for row in snapshot["comments"]:
        for key in ("hosted_media_urls", "hosted_avatar"):
            visit(row.get(key), (row["shortcode"], "comment", row["comment_id"], key))
    return slots


def verify_new_media(before, after, public_bases):
    """Read only new configured hosted objects; never follow a source URL."""
    from urllib.parse import urlsplit
    from urllib.request import Request, urlopen

    from scripts.ingestion.january_audit.hosted import object_key

    public_bases = public_base_urls(public_bases)
    old, current = media_slots(before, public_bases), media_slots(after, public_bases)
    new_slots = {key: url for key, url in current.items() if old.get(key) != url}
    previous_urls = set(old.values())
    urls = sorted(set(new_slots.values()) - previous_urls)
    verified, unverified = [], []
    deadline = time.monotonic() + 120
    for url in urls:
        try:
            key = object_key(url, None, public_bases)
            if time.monotonic() >= deadline:
                raise TimeoutError
            hashed, count = hashlib.sha256(), 0
            request = Request(url, headers={"User-Agent": "TRR-January-Recovery-Verification/1.0"})
            with urlopen(request, timeout=min(10, max(0.1, deadline - time.monotonic()))) as response:
                # Redirects must remain in the approved storage identity.
                object_key(response.url, key, public_bases)
                while block := response.read(65536):
                    if time.monotonic() >= deadline:
                        raise TimeoutError
                    count += len(block)
                    hashed.update(block)
                if count == 0 or (
                    response.headers.get("Content-Length") and count != int(response.headers["Content-Length"])
                ):
                    raise ValueError("Incomplete stored object")
            verified.append({"storage_key": key, "bytes": count, "sha256": hashed.hexdigest()})
        except Exception as error:
            unverified.append(
                {
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "host": urlsplit(url).hostname,
                    "failure_class": type(error).__name__,
                }
            )
    return {
        "new_or_changed_logical_slots": len(new_slots),
        "new_distinct_hosted_urls": len(urls),
        "full_objects_read": verified,
        "unverified": unverified,
        "verification_budget_seconds": 120,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--artifact-directory", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-generation", type=int, required=True)
    parser.add_argument("--continue-progress-from", type=Path)
    parser.add_argument("--after-media-admission-repair", action="store_true")
    args = parser.parse_args()
    from scripts.ingestion.january_progress import UsefulProgressTracker
    from trr_backend.ingestion.bounded_recovery import USEFUL_PROGRESS_POLICY

    stop_policy = dict(USEFUL_PROGRESS_POLICY)
    operator_identity = "january-useful-progress-operator"
    manifest = json.loads(args.manifest.read_text())
    if args.after_media_admission_repair and not args.continue_progress_from:
        raise RuntimeError("Media admission repair requires its exact predecessor")
    media_repair_source = manifest["source_identity"] if args.after_media_admission_repair else None
    release = Path(manifest["release_path"])
    for name, digest in manifest["python_hashes"].items():
        if hashlib.sha256((release / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("Candidate package changed")
    before = snapshot()
    if (before["run"]["state"], before["run"]["generation"], before["run"]["execution_hold_reason"]) != (
        "cancelled",
        args.expected_generation,
        "bounded_recovery_window_expired",
    ):
        raise RuntimeError("January is no longer the expected held generation")
    if args.continue_progress_from:
        # Validate predecessor files before disturbing the ordinary service or
        # granting a generation. Real metrics are sampled again after the grant.
        continue_comment_progress(
            UsefulProgressTracker(stop_policy["stall_seconds"]),
            args.continue_progress_from,
            expected_generation=args.expected_generation,
            metrics={"canonical_ids": len(before["comments"])},
            now=time.monotonic(),
            wall_now=datetime.now(UTC),
            media_repair_source=media_repair_source,
        )
    original = PLIST.read_bytes()
    native = plistlib.loads(original)
    launch_values = {
        word.split("=", 1)[0]: word.split("=", 1)[1]
        for word in shlex.split(native["ProgramArguments"][2])
        if "=" in word
    }
    storage_path = Path(launch_values["TRR_INSTAGRAM_JANUARY_STORAGE_BINDING"])
    if (
        hashlib.sha256(storage_path.read_bytes()).hexdigest()
        != launch_values["TRR_INSTAGRAM_JANUARY_STORAGE_BINDING_SHA256"]
    ):
        raise RuntimeError("Storage binding changed")
    storage = json.loads(storage_path.read_text())
    label = f"gui/{os.getuid()}/{native['Label']}"
    state = subprocess.check_output(["launchctl", "print", label], text=True)
    pid = int(re.search(r"\bpid = (\d+)\b", state)[1])
    if processes(pid) != {pid} or any(
        row["owner_expires_at"] and row["owner_expires_at"] > datetime.now(UTC) for row in before["targets"]
    ):
        raise RuntimeError("Existing activities have not drained")
    if not args.execute:
        print(
            json.dumps(
                {
                    "prepared": True,
                    "source": manifest["source_identity"],
                    "generation": args.expected_generation,
                    "stop_policy": stop_policy,
                }
            )
        )
        return 0
    directory = args.artifact_directory
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    save(directory, "before.json", before)
    (directory / "native-before.plist").write_bytes(original)
    (directory / "native-before.plist").chmod(0o600)
    child = None
    window = None
    stopped_native = False
    stop_reason = "operator_interrupted"
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.execution_policy import ExecutionPolicy
    from trr_backend.ingestion.persistence import Store
    from trr_backend.ingestion.workflows import control_call

    store = Store(transaction, execution_placement="local")

    def mark_native_shutdown_attempt():
        nonlocal stopped_native
        stopped_native = True

    try:
        shutdown_native_attempt(label, mark_native_shutdown_attempt)
        wait_native_drain(label, pid)
        window = control_call(
            store.resume_bounded_recovery_window,
            RUN,
            "bravotv",
            expected_generation=args.expected_generation,
            expected_intent_hash=INTENT,
            expected_workflow_version=before["run"]["workflow_version"],
            source_revision=manifest["source_identity"],
            initiated_by=operator_identity,
            stop_policy=stop_policy,
        )
        save(directory, "window.json", window)
        # The parent may only issue phase commands for this exact authorization.
        os.environ.update(
            {
                "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": window["window_id"],
                "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": RUN,
                "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": str(window["generation"]),
                "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": manifest["source_identity"],
            }
        )
        frontiers = {}
        tracker = UsefulProgressTracker(stop_policy["stall_seconds"])
        initial_progress = progress(window, frontiers)
        initial_decision = tracker.observe("comments", initial_progress["comments"], now=time.monotonic())
        comment_seed = {
            "version": 1,
            "window_id": window["window_id"],
            "last_useful_at": initial_progress["at"],
            "metrics": initial_progress["comments"],
            "comment_phase_ended": False,
            "stop_reason": "running",
        }
        if args.continue_progress_from:
            tracker = UsefulProgressTracker(stop_policy["stall_seconds"])
            continued = continue_comment_progress(
                tracker,
                args.continue_progress_from,
                expected_generation=args.expected_generation,
                metrics=initial_progress["comments"],
                now=time.monotonic(),
                wall_now=datetime.now(UTC),
                media_repair_source=media_repair_source,
            )
            initial_decision = continued["decision"]
            comment_seed = dict(continued["seed"], window_id=window["window_id"])
            save(directory, "continued-progress.json", continued)
        save(directory, "progress-baseline.json", initial_progress)
        replace_comment_seed(directory, comment_seed)
        if initial_decision["reason"] != "running":
            from trr_backend.ingestion.bounded_recovery_phase import transition

            changed = control_call(
                transition,
                store,
                run_id=RUN,
                account="bravotv",
                generation=window["generation"],
                intent_hash=INTENT,
                window_id=window["window_id"],
                source_revision=manifest["source_identity"],
                to_phase="draining_comments",
                reason="comments_stopped",
            )
            save(directory, "comments-stall.json", {"decision": initial_decision, "transition": changed})
        binding = json.loads(Path(launch_values["TRR_INSTAGRAM_JANUARY_BINDING"]).read_text())
        binding.update(
            source_identity=manifest["source_identity"],
            cloud_source_identity=manifest["source_identity"],
            expected_generation=window["generation"],
            release_path=str(release),
            deployment_version=manifest.get("deployment_version"),
            window=window,
            source_map_sha256=hashlib.sha256(
                json.dumps(manifest["python_hashes"], sort_keys=True).encode()
            ).hexdigest(),
        )
        binding_path = save(directory, "runtime-binding.json", binding)
        with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("select revision from social.instagram_ingestion_execution_policies where run_id=%s", (RUN,))
            policy = cur.fetchone()
        limits = ExecutionPolicy(transaction).set_limits(
            RUN,
            "bravotv",
            expected_generation=window["generation"],
            expected_intent_hash=INTENT,
            expected_revision=policy["revision"] if policy else 0,
            comments_parallelism=6,
            idempotency_key="window-policy:" + window["window_id"],
            initiated_by=operator_identity,
        )
        save(directory, "policy.json", limits)
        prefix, separator, suffix = native["ProgramArguments"][2].rpartition(" && exec ")
        if not separator or "scripts/ingestion/local_worker.py" not in suffix:
            raise RuntimeError("Native launch command changed")
        prefix = prefix.replace(native["WorkingDirectory"], str(release))
        exports = {
            "TRR_INSTAGRAM_LOCAL_WORKER_RUN_ID": RUN,
            "TRR_INSTAGRAM_LOCAL_WORKER_GENERATION": str(window["generation"]),
            "TRR_INSTAGRAM_RECOVERY_WINDOW_ID": window["window_id"],
            "TRR_INSTAGRAM_LOCAL_WORKER_MAX_ACTIVITIES": "6",
            "TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT": str(datetime.fromisoformat(window["deadline_at"]).timestamp()),
            "TRR_INSTAGRAM_LOCAL_WORKER_PARENT_PID": str(os.getpid()),
            "TRR_INSTAGRAM_JANUARY_RECOVERY_RUN_ID": RUN,
            "TRR_INSTAGRAM_JANUARY_RECOVERY_INTENT_HASH": INTENT,
            "TRR_INSTAGRAM_JANUARY_RECOVERY_SOURCE_REVISION": manifest["source_identity"],
            "SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY": "2",
            "TRR_INSTAGRAM_JANUARY_CERTIFICATION_OUTPUT": "",
            "TRR_INSTAGRAM_JANUARY_BINDING": str(binding_path),
        }
        command = prefix + " && export " + " ".join(k + "=" + shlex.quote(v) for k, v in exports.items())
        command += " && exec " + shlex.quote(str(release / ".venv/bin/python")) + " scripts/ingestion/local_worker.py"
        with (directory / "worker.log").open("x") as log:
            (directory / "worker.log").chmod(0o600)
            child = subprocess.Popen(
                ["/bin/zsh", "-c", command], cwd=release, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
            )
            save(
                directory,
                "worker-start.json",
                {
                    "pid": child.pid,
                    "source": manifest["source_identity"],
                    "window_id": window["window_id"],
                    "deadline_at": window["deadline_at"],
                },
            )
            startup_end = time.monotonic() + 12
            acquired = None
            while child.poll() is None and time.monotonic() < startup_end:
                acquired = lease_record(directory / "worker.log", child.pid)
                if acquired and grants() == [acquired["lease_backend_pid"]]:
                    break
                time.sleep(0.25)
            if not acquired or grants() != [acquired["lease_backend_pid"]]:
                raise RuntimeError("Bounded worker did not acquire the unique coordinator grant")
            save(directory, "worker-qualified.json", acquired)
            print(
                json.dumps({"status": "recovery_started", "pid": child.pid, "deadline_at": window["deadline_at"]}),
                flush=True,
            )
            last_phase = "comments"
            while child.poll() is None:
                remaining = datetime.fromisoformat(window["deadline_at"]).timestamp() - time.time()
                if remaining <= 0:
                    stop_reason = "safety_deadline"
                    break
                try:
                    child.wait(timeout=min(60, remaining))
                except subprocess.TimeoutExpired:
                    if datetime.fromisoformat(window["deadline_at"]).timestamp() <= time.time():
                        stop_reason = "safety_deadline"
                        break
                    observed = progress(window, frontiers)
                    current_phase = observed["phase"]
                    if current_phase == "draining_comments":
                        if last_phase != current_phase:
                            print(json.dumps({"status": "comments_draining"}), flush=True)
                        last_phase = current_phase
                        continue
                    decision = tracker.observe(
                        current_phase,
                        observed[current_phase],
                        now=time.monotonic(),
                        complete=current_phase == "media" and media_idle(directory / "worker.log", window["window_id"]),
                    )
                    if current_phase == "comments" and (decision["useful_progress"] or decision["reason"] != "running"):
                        comment_seed = updated_comment_seed(
                            comment_seed, observed["comments"], decision, datetime.now(UTC)
                        )
                        replace_comment_seed(directory, comment_seed)
                    if decision["useful_progress"] or last_phase != current_phase or decision["reason"] != "running":
                        observed["stop_decision"] = decision
                        save(directory, f"progress-{time.time_ns()}.json", observed)
                        print(
                            json.dumps(
                                {
                                    "status": "recovery_progress",
                                    "phase": current_phase,
                                    "canonical_ids": observed["comments"]["canonical_ids"],
                                    "media_committed": observed["media_committed"],
                                    "stop_decision": decision,
                                }
                            ),
                            flush=True,
                        )
                    last_phase = current_phase
                    if decision["reason"] != "running":
                        if current_phase == "comments":
                            from trr_backend.ingestion.bounded_recovery_phase import transition

                            changed = control_call(
                                transition,
                                store,
                                run_id=RUN,
                                account="bravotv",
                                generation=window["generation"],
                                intent_hash=INTENT,
                                window_id=window["window_id"],
                                source_revision=manifest["source_identity"],
                                to_phase="draining_comments",
                                reason="comments_stopped",
                            )
                            save(directory, "comments-stall.json", {"decision": decision, "transition": changed})
                        else:
                            stop_reason = "media_" + decision["reason"]
                            break
            else:
                stop_reason = "worker_exited"
            save(
                directory,
                "worker-end.json",
                {
                    "returncode_before_stop": child.poll(),
                    "work_elapsed_seconds": time.time() - datetime.fromisoformat(window["started_at"]).timestamp(),
                    "stop_reason": stop_reason,
                },
            )
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if child is not None:
            save(directory, "child-drained.json", stop_owned_child(child))
        if stopped_native:
            wait_native_drain(label, pid)
            if window is None:
                window = reconcile_window(
                    manifest["source_identity"],
                    before["run"]["workflow_version"],
                    expected_generation=args.expected_generation,
                    stop_policy=stop_policy,
                )
            stable = 0
            end = time.monotonic() + 30
            while time.monotonic() < end:
                stable = stable + 1 if not grants() else 0
                if stable == 2:
                    break
                time.sleep(0.5)
            if stable != 2:
                raise RuntimeError("Recovery grant remains; native restoration held")
            transition_before = transition_snapshot()
            if window:
                cancelled = control_call(
                    store.control,
                    RUN,
                    "bravotv",
                    action="cancel",
                    expected_generation=window["generation"],
                    expected_placement="local",
                    idempotency_key="window-stop:" + window["window_id"],
                )
                held = control_call(
                    store.hold_cancelled_run,
                    RUN,
                    "bravotv",
                    cancelled["generation"],
                    "bounded_recovery_window_expired",
                    "window-hold:" + window["window_id"],
                    operator_identity,
                )
                save(directory, "held.json", held)
            transition_after = transition_snapshot()
            if transition_before != transition_after:
                raise RuntimeError("Stop/hold changed preserved business records")
            save(directory, "hold-preservation.json", transition_after)
            after = snapshot()
            if (
                after["run"]["state"] != "cancelled"
                or after["run"]["execution_hold_reason"] != "bounded_recovery_window_expired"
            ):
                raise RuntimeError("Durable January hold absent; native restoration held")
            save(directory, "after.json", after)
            # Restore the exact ordinary service; never enable automatic recovery.
            if PLIST.read_bytes() != original:
                raise RuntimeError("Native launch configuration changed concurrently")
            restored_plist = dict(native)
            restored_plist["WorkingDirectory"] = str(release)
            restored_plist["ProgramArguments"] = [
                arg.replace(native["WorkingDirectory"], str(release)) for arg in native["ProgramArguments"]
            ]
            normal_prefix, normal_separator, normal_exec = restored_plist["ProgramArguments"][2].rpartition(" && exec ")
            if not normal_separator:
                raise RuntimeError("Ordinary native command lost its execution boundary")
            restored_plist["ProgramArguments"][2] = (
                normal_prefix
                + " && unset TRR_INSTAGRAM_RECOVERY_WINDOW_ID TRR_INSTAGRAM_LOCAL_WORKER_GENERATION && exec "
                + normal_exec
            )
            temporary = directory / "native-current.plist"
            temporary.write_bytes(plistlib.dumps(restored_plist))
            temporary.chmod(0o600)
            os.replace(temporary, PLIST)
            subprocess.run(
                ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(PLIST)], check=True, capture_output=True
            )
            restored = None
            end = time.monotonic() + 30
            while time.monotonic() < end:
                state = subprocess.check_output(["launchctl", "print", label], text=True)
                match = re.search(r"\bpid = (\d+)\b", state)
                if match:
                    candidate_pid = int(match[1])
                    native_lease = lease_record(Path(restored_plist["StandardOutPath"]), candidate_pid)
                    if native_lease and grants() == [native_lease["lease_backend_pid"]]:
                        restored = candidate_pid
                        break
                time.sleep(0.5)
            if restored is None:
                raise RuntimeError("Native worker restoration is not qualified")
            time.sleep(1)
            if not alive(restored) or grants() != [native_lease["lease_backend_pid"]]:
                raise RuntimeError("Restored native grant changed")
            save(directory, "restored.json", {"pid": restored, "grants": grants(), "package": str(release)})
            if window:
                result = report(before, after, window)
                result["stop_reason"] = stop_reason
                old_ids = {(row["shortcode"], row["comment_id"]) for row in before["comments"]}
                current_ids = {(row["shortcode"], row["comment_id"]) for row in after["comments"]}
                result["saved_identity_preservation"] = {
                    "before": len(old_ids),
                    "after": len(current_ids),
                    "missing_identities": len(old_ids - current_ids),
                }
                result["media"] = verify_new_media(before, after, storage["public_bases"])
                save(directory, "recovery-report.json", result)
                print(
                    json.dumps(
                        {
                            "status": "recovery_finished",
                            "report": str(directory / "recovery-report.json"),
                            "new_canonical_ids": result["new_canonical_ids"],
                        }
                    ),
                    flush=True,
                )
    return 0


if __name__ == "__main__":

    def interrupted(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    raise SystemExit(main())
