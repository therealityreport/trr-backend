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


def reconcile_window(source_revision, workflow_version):
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.bounded_recovery import window_key

    with closing(connect()) as conn, conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select result from social.instagram_ingestion_commands
            where account='bravotv' and idempotency_key=%s""",
            (window_key(RUN, 3),),
        )
        row = cur.fetchone()
    if row is None:
        return None
    window = row["result"]
    expected = {
        "run_id": RUN,
        "account": "bravotv",
        "expected_generation": 2,
        "generation": 3,
        "expected_intent_hash": INTENT,
        "expected_workflow_version": workflow_version,
        "source_revision": source_revision,
        "initiated_by": "january-one-hour-operator",
        "priority": "comments_first",
        "duration_seconds": 3600,
    }
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
                checkpoint#>'{comment_pages,settlement}' as settlement
                ,checkpoint#>'{comment_pages,expected}' as expected,
                array(select jsonb_object_keys(coalesce(
                    checkpoint#>'{comment_pages,observed}','{}'::jsonb))) as observed_ids
                from social.instagram_ingestion_targets t where run_id=%s order by stage,target_key""",
                (RUN,),
            )
            targets = [dict(row) for row in cur.fetchall()]
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


def progress(window):
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
        return {
            "at": datetime.now(UTC).isoformat(),
            "accounted_requests": requests,
            "new_comment_pages": pages,
            "media_committed": media_count,
        }


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
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    release = Path(manifest["release_path"])
    for name, digest in manifest["python_hashes"].items():
        if hashlib.sha256((release / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("Candidate package changed")
    before = snapshot()
    if (before["run"]["state"], before["run"]["generation"], before["run"]["execution_hold_reason"]) != (
        "cancelled",
        2,
        "bounded_recovery_window_expired",
    ):
        raise RuntimeError("January is no longer the expected held generation")
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
        print(json.dumps({"prepared": True, "source": manifest["source_identity"], "generation": 2, "seconds": 3600}))
        return 0
    directory = args.artifact_directory
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    save(directory, "before.json", before)
    (directory / "native-before.plist").write_bytes(original)
    (directory / "native-before.plist").chmod(0o600)
    child = None
    window = None
    stopped_native = False
    from psycopg2.extras import RealDictCursor

    from trr_backend.ingestion.execution_policy import ExecutionPolicy
    from trr_backend.ingestion.persistence import Store
    from trr_backend.ingestion.workflows import control_call

    store = Store(transaction, execution_placement="local")
    try:
        stopped_native = stop_native(label)
        deadline = time.monotonic() + 45
        stable = 0
        while time.monotonic() < deadline:
            stable = stable + 1 if not alive(pid) and not grants() else 0
            if stable == 2:
                break
            time.sleep(0.5)
        if stable != 2:
            raise RuntimeError("Prior worker/grant did not drain")
        window = control_call(
            store.resume_bounded_recovery_window,
            RUN,
            "bravotv",
            expected_generation=2,
            expected_intent_hash=INTENT,
            expected_workflow_version=before["run"]["workflow_version"],
            source_revision=manifest["source_identity"],
            initiated_by="january-one-hour-operator",
        )
        save(directory, "window.json", window)
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
            initiated_by="january-one-hour-operator",
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
            previous_progress, unchanged = None, 0
            while child.poll() is None:
                remaining = datetime.fromisoformat(window["deadline_at"]).timestamp() - time.time()
                if remaining <= 0:
                    break
                try:
                    child.wait(timeout=min(600 if unchanged >= 3 else 120, remaining))
                except subprocess.TimeoutExpired:
                    if datetime.fromisoformat(window["deadline_at"]).timestamp() <= time.time():
                        break
                    observed = progress(window)
                    signature = {key: value for key, value in observed.items() if key != "at"}
                    unchanged = unchanged + 1 if signature == previous_progress else 0
                    if signature != previous_progress:
                        save(directory, f"progress-{time.time_ns()}.json", observed)
                        print(json.dumps(dict(status="recovery_progress", **observed)), flush=True)
                    previous_progress = signature
            save(
                directory,
                "worker-end.json",
                {
                    "returncode_before_stop": child.poll(),
                    "work_elapsed_seconds": time.time() - datetime.fromisoformat(window["started_at"]).timestamp(),
                },
            )
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if child is not None:
            save(directory, "child-drained.json", stop_owned_child(child))
        if stopped_native:
            if window is None:
                window = reconcile_window(manifest["source_identity"], before["run"]["workflow_version"])
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
                    "january-one-hour-operator",
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
