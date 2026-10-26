#!/usr/bin/env python3
"""Preview or capture at most five auxiliary public January count witnesses."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from trr_backend.ingestion.count_witness import WitnessRequest, refresh_witnesses


def source_revision():
    root = Path(__file__).resolve().parents[2]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    paths = [
        "trr_backend/ingestion/count_witness.py",
        "scripts/ingestion/refresh_count_witnesses.py",
        "trr_backend/ingestion/public_transport.py",
        "trr_backend/ingestion/identity_policy.py",
        "trr_backend/ingestion/public_http.py",
        "trr_backend/ingestion/evidence.py",
        "trr_backend/ingestion/canonical.py",
        "trr_backend/socials/instagram/post_normalizer.py",
        "trr_backend/socials/instagram/permalink_metadata.py",
        "trr_backend/socials/instagram/public_post_extractor.py",
    ]
    return {
        "git_head": head,
        "files_sha256": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in paths},
    }


def validate_database_target():
    from scripts.ingestion.local_driver import _project_dsn
    from trr_backend.db.connection import resolve_database_url_candidate_details

    candidates = resolve_database_url_candidate_details()
    if not candidates or any(not _project_dsn(item["url"]) for item in candidates):
        raise ValueError("Every database candidate must target TRR vwxfvzutyufrkhfgoeaa")


def _parse(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortcode", action="append", required=True)
    parser.add_argument("--max-sends", type=int, default=3)
    parser.add_argument("--max-seconds", type=float, default=90)
    parser.add_argument("--artifact-directory", type=Path)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def child_main(argv):
    args = _parse(argv)
    try:
        request = WitnessRequest(tuple(args.shortcode), args.max_sends, args.max_seconds)
        validate_database_target()
        from trr_backend.ingestion.persistence import _transaction

        result = refresh_witnesses(
            request,
            transaction=_transaction,
            source_revision=source_revision(),
            apply=args.apply,
            artifact_directory=args.artifact_directory,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        # Do not expose DSNs, payloads, owner tokens or transport exceptions.
        print(json.dumps({"status": "rejected", "failure_class": type(error).__name__}))
        return 1


# Start this guard before importing backend code: blocked imports, pool checkout,
# sockets and commits cannot prevent independent expiry or parent-loss exit.
_GUARDED_CHILD_CODE = """
import os, sys, threading, time
expected_parent = int(sys.argv[1])
deadline = float(sys.argv[2])
def watchdog():
    while True:
        remaining = deadline - time.monotonic()
        if os.getppid() != expected_parent or remaining <= 0:
            os._exit(124)
        time.sleep(min(0.05, remaining))
if os.getppid() != expected_parent or time.monotonic() >= deadline:
    os._exit(124)
threading.Thread(target=watchdog, daemon=True).start()
from scripts.ingestion.refresh_count_witnesses import child_main
raise SystemExit(child_main(sys.argv[3:]))
"""


class _SupervisorSignal(BaseException):
    def __init__(self, signum):
        self.signum = signum


def _stop_owned_child(child, *, clock):
    cleanup_deadline = clock() + 2

    def signal_group(sig):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            pass

    reaped = False
    try:
        signal_group(signal.SIGTERM)
        try:
            child.wait(timeout=min(1, max(0, cleanup_deadline - clock())))
        except BaseException:
            # A broken wait must still reach KILL and the final reap attempt.
            pass
    finally:
        signal_group(signal.SIGKILL)
        try:
            child.wait(timeout=max(0, cleanup_deadline - clock()))
            reaped = True
        except BaseException:
            pass
    return reaped


def supervise(argv, *, seconds, started, clock=time.monotonic):
    """Own one fresh process group; bound work plus at most two seconds cleanup."""
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-c", _GUARDED_CHILD_CODE, str(os.getpid()), repr(started + seconds), *argv]
    child, previous, status, reaped = None, {}, None, False

    def interrupted(signum, _frame):
        raise _SupervisorSignal(signum)

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, interrupted)
        try:
            child = subprocess.Popen(command, cwd=root, start_new_session=True, stdin=subprocess.DEVNULL)
            return child.wait(timeout=max(0, started + seconds - clock()))
        except subprocess.TimeoutExpired:
            status = "deadline"
            return 124
        except _SupervisorSignal as error:
            status = "interrupted"
            return 128 + error.signum
        finally:
            # Repeated signals cannot interrupt bounded cleanup. KILL the exact
            # owned group even if the leader has exited; never retry the work.
            for sig in previous:
                signal.signal(sig, signal.SIG_IGN)
            if child is not None:
                reaped = _stop_owned_child(child, clock=clock)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if status is not None:
            print(
                json.dumps({"status": status, "child_reaped": reaped, "successful_private_artifacts_preserved": True})
            )


def main(argv=None):
    started = time.monotonic()
    argv = list(sys.argv[1:] if argv is None else argv)
    args = _parse(argv)
    try:
        WitnessRequest(tuple(args.shortcode), args.max_sends, args.max_seconds)
        # Supervision begins before child imports, environment/DSN validation,
        # pool checkout, source hashing, network I/O or transaction completion.
        return supervise(argv, seconds=args.max_seconds, started=started)
    except Exception as error:
        print(json.dumps({"status": "rejected", "failure_class": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
