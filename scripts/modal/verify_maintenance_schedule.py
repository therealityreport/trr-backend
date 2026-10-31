#!/usr/bin/env python3
"""Verify provider schedules and effective runtime ownership after deployment."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.modal.deploy_backend import pinned_modal_env  # noqa: E402
from trr_backend.modal_runtime_config import (  # noqa: E402
    MAINTENANCE_CRON_ENV,
    validate_maintenance_deployment,
)

SCHEDULES = {
    "heartbeat_remote_executors": "* * * * *",
    "sweep_social_dispatch_queue": "*/2 * * * *",
}


def validate_observed_maintenance(
    expected: dict[str, str], runtime: dict, schedules: dict[str, dict], *, timezone: str
) -> None:
    cron_owner = expected[MAINTENANCE_CRON_ENV] == "1"
    owner = "modal_singleton_cron" if cron_owner else "api_runtime_scheduler"
    if runtime.get("state_free") is not True or runtime.get("owner") != owner:
        raise RuntimeError("Effective maintenance owner differs from deployment configuration.")
    if runtime.get("env") != expected:
        raise RuntimeError("Effective maintenance environment or intervals differ from deployment configuration.")
    for name, expression in SCHEDULES.items():
        wanted = {"cron": {"cronString": expression, "timezone": timezone}} if cron_owner else {}
        if name not in schedules or schedules[name] != wanted:
            raise RuntimeError(f"Deployed schedule for {name} disagrees with the effective maintenance owner.")


async def collect_schedules(environment: str) -> dict[str, dict]:
    from google.protobuf.json_format import MessageToDict
    from modal.client import _Client
    from modal_proto import api_pb2

    client = await _Client.from_env()
    schedules = {}
    for name in SCHEDULES:
        result = await client.stub.FunctionGet(
            api_pb2.FunctionGetRequest(app_name="trr-backend-jobs", object_tag=name, environment_name=environment),
            timeout=30,
            retry=None,
        )
        schedules[name] = MessageToDict(result.function.schedule)
    return schedules


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, choices=["main"])
    args = parser.parse_args()
    os.environ.update(pinned_modal_env())
    expected = validate_maintenance_deployment(os.environ)
    # Import only after pinning the profile: Modal resolves its profile on import.
    import modal

    probe = modal.Function.from_name("trr-backend-jobs", "probe_modal_maintenance_config", environment_name=args.env)
    call = probe.spawn()
    try:
        runtime = call.get(timeout=60)
    except TimeoutError:
        call.cancel(terminate_containers=True)
        raise
    schedules = asyncio.run(collect_schedules(args.env))
    validate_observed_maintenance(
        expected, runtime, schedules, timezone=os.getenv("TRR_MODAL_TIMEZONE") or "America/New_York"
    )
    print(json.dumps({"ok": True, "environment": args.env, "runtime": runtime, "schedules": schedules}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
