"""Disabled-by-default, finite coordinator process owned by the existing sweep.

The child alone imports DBOS and opens its session lease. Its inner deadline
retains the lease through process exit; this parent owns process-group reaping.
Production dependency/role/connection/readiness gates remain deployment work.
"""

from __future__ import annotations

import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from typing import Any

COORDINATOR_ENABLED = "TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED"
COORDINATOR_TIMEOUT_SECONDS = 115
ACTIVITY_TIMEOUT_SECONDS = 590
ACTIVITY_DEADLINE_ENV = "TRR_INSTAGRAM_ACTIVITY_DEADLINE_MONOTONIC"
ACTIVITY_OUTPUT_LIMIT_BYTES = 65536


def coordinator_enabled() -> bool:
    return os.getenv(COORDINATOR_ENABLED, "").strip().lower() in {"1", "true", "yes", "on"}


def _configured_runtime():
    from trr_backend.ingestion.composition import runtime_config

    config = runtime_config()
    if config is None or not config.system_database_url or not config.workflow_version or not config.runtime_role:
        raise ValueError("Explicit ingestion runtime configuration is required")
    return config


def _reap_process_group(child: subprocess.Popen) -> None:
    """The coordinator gets a private process group; never signal shared helpers."""
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait(timeout=5)


def run_ingestion_coordinator() -> dict[str, Any]:
    """Existing maintenance owner calls this once; no additional scheduler exists."""
    if not coordinator_enabled():
        return {"status": "disabled"}
    try:
        _configured_runtime()
    except Exception as exc:
        # Configuration errors may embed connection strings. Return only type.
        return {"status": "unconfigured", "failure_class": type(exc).__name__}
    child = subprocess.Popen(
        [sys.executable, "-m", "trr_backend.ingestion.runtime"],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        try:
            code = child.wait(timeout=COORDINATOR_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            return {"status": "timed_out"}
        status = {0: "completed", 124: "bounded_timeout", 125: "lease_lost"}.get(code, "failed")
        return {"status": status, "exit_code": code}
    finally:
        # Also remove any descendants after a normal leader exit. Never leave a
        # background executor after the maintenance sweep returns or is cancelled.
        _reap_process_group(child)


def activity_reservation(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Pass only the explicit business fence; never forward transport credentials."""
    keys = ("run_id", "account", "generation", "stage", "target_key", "owner_token")
    if not isinstance(payload, Mapping) or any(key not in payload for key in keys):
        raise ValueError("Incomplete ingestion activity reservation")
    safe = {key: payload[key] for key in keys}
    if type(safe["generation"]) is not int or safe["generation"] < 1:
        raise ValueError("Invalid ingestion generation")
    if any(not isinstance(safe[key], str) or not safe[key] for key in keys if key != "generation"):
        raise ValueError("Invalid ingestion reservation identity")
    if safe["stage"] not in {"listing", "post_details", "comments", "media"}:
        raise ValueError("Invalid ingestion activity stage")
    return safe


def run_ingestion_activity(
    reservation: Mapping[str, Any],
    *,
    execution_placement: str = "modal",
    timeout_seconds: float | None = None,
    abort: threading.Event | None = None,
) -> dict[str, Any]:
    """Run the worker within its lease; Modal callers keep existing function caps.

    Explicit local callers supply a serial admission owner and bounded abort;
    they do not occupy Modal's physical function limiter.
    Only JSON stdin carries the owner token. Output is bounded and projected to
    outcome/identities plus one validated public failure diagnostic. Source
    payloads, raw logs and stderr never leave this helper.
    """
    if execution_placement not in {"modal", "local"}:
        raise ValueError("Unsupported ingestion execution placement")
    if execution_placement == "modal" and (timeout_seconds is not None or abort is not None):
        raise ValueError("Local activity bounds cannot change Modal policy")
    timeout = ACTIVITY_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    if not math.isfinite(timeout) or not 0 < timeout <= ACTIVITY_TIMEOUT_SECONDS:
        raise ValueError("Activity deadline exceeds the existing bound")
    if not coordinator_enabled():
        return {"outcome": "fenced", "code": "ingestion_disabled"}
    safe = activity_reservation(reservation)
    if abort is not None and abort.is_set():
        return {"outcome": "fenced", "stage": safe["stage"], "code": "local_execution_aborted"}
    argv = [sys.executable, "-m", "trr_backend.ingestion.worker"]
    if execution_placement == "local":
        argv.extend(["--execution-placement", "local", "--expected-parent-pid", str(os.getpid())])
    # One absolute kernel clock bounds spawn, child startup, resume and source.
    # Never inherit a stale deadline from this coordinator's own environment.
    activity_deadline = time.monotonic() + timeout
    if execution_placement == "local" and os.getenv("TRR_INSTAGRAM_RECOVERY_WINDOW_ID"):
        remaining = float(os.environ["TRR_INSTAGRAM_LOCAL_WORKER_DEADLINE_AT"]) - time.time()
        if remaining <= 0:
            return {"outcome": "fenced", "code": "bounded_recovery_expired"}
        activity_deadline = min(activity_deadline, time.monotonic() + remaining)
    child_env = dict(os.environ, **{ACTIVITY_DEADLINE_ENV: repr(activity_deadline)})
    child = subprocess.Popen(
        argv,
        env=child_env,
        start_new_session=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    output = bytearray()
    overflow = threading.Event()

    def read_output():
        try:
            while True:
                block = child.stdout.read(4096)
                if not block:
                    return
                if len(output) + len(block) > ACTIVITY_OUTPUT_LIMIT_BYTES:
                    overflow.set()
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    return
                output.extend(block)
        except (OSError, ValueError):
            return

    reader = threading.Thread(target=read_output, daemon=True, name="instagram-activity-output")
    try:
        reader.start()
        try:
            child.stdin.write(json.dumps(safe, separators=(",", ":")).encode())
            child.stdin.close()
        except BrokenPipeError:
            pass
        try:
            if execution_placement == "modal":
                code = child.wait(timeout=max(0, activity_deadline - time.monotonic()))
            else:
                end = activity_deadline
                while True:
                    if abort is not None and abort.is_set():
                        return {"outcome": "fenced", "stage": safe["stage"], "code": "local_execution_aborted"}
                    remaining = end - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, timeout)
                    try:
                        code = child.wait(timeout=min(0.1, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        continue
        except subprocess.TimeoutExpired:
            return {"outcome": "unresolved", "stage": safe["stage"], "code": "activity_timeout"}
        reader.join(timeout=5)
        if overflow.is_set():
            return {"outcome": "unresolved", "stage": safe["stage"], "code": "activity_output_limit"}
        if code != 0:
            return {"outcome": "unresolved", "stage": safe["stage"], "code": "activity_process_failed"}
        try:
            result = json.loads(output)
        except (ValueError, UnicodeDecodeError):
            return {"outcome": "unresolved", "stage": safe["stage"], "code": "activity_output_invalid"}
        outcomes = {"committed", "cached_satisfied", "source_unavailable", "unresolved", "fenced"}
        if (
            not isinstance(result, dict)
            or not isinstance(result.get("outcome"), str)
            or result["outcome"] not in outcomes
            or result.get("stage", safe["stage"]) != safe["stage"]
        ):
            return {"outcome": "unresolved", "stage": safe["stage"], "code": "activity_output_invalid"}
        summary = {"outcome": result["outcome"], "stage": safe["stage"]}
        code = result.get("code")
        if isinstance(code, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", code):
            summary["code"] = code
        from trr_backend.ingestion.public_diagnostics import publish_public_diagnostic

        diagnostic = publish_public_diagnostic(result.get("public_diagnostic"))
        if diagnostic is not None:
            summary["public_diagnostic"] = diagnostic
        return summary
    finally:
        _reap_process_group(child)
        if child.stdin and not child.stdin.closed:
            child.stdin.close()
        if child.stdout:
            child.stdout.close()
        if reader.is_alive():
            reader.join(timeout=5)


def main() -> int:
    if not coordinator_enabled():
        return 0
    try:
        config = _configured_runtime()
        # Imported only in the isolated, explicitly enabled child process.
        import psycopg

        from trr_backend.ingestion.composition import build_scheduler
        from trr_backend.ingestion.workflows import DBOSAdapter

        adapter = DBOSAdapter(build_scheduler(), config)
        adapter.run_bounded(lambda: psycopg.connect(config.system_database_url, autocommit=True, connect_timeout=5))
        return 0  # Busy returns before SDK launch; initialized runtimes exit.
    except Exception:
        # Do not emit a traceback containing a credential-bearing DSN/config.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
