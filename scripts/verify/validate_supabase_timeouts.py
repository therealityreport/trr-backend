#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import time

import httpx

from trr_backend.db.admin import (
    create_supabase_httpx_client,
    get_supabase_timeout_config,
    is_timeout_error,
)

DEFAULT_TEST_URL = "http://10.255.255.1:81"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate Supabase timeout behavior against a dead URL. "
            "Set SUPABASE_POSTGREST_TIMEOUT_SEC=1 to confirm fast failure."
        )
    )
    parser.add_argument(
        "--url",
        default=os.getenv("SUPABASE_TIMEOUT_TEST_URL", DEFAULT_TEST_URL),
        help="Dead URL to probe (default: env SUPABASE_TIMEOUT_TEST_URL or 10.255.255.1:81).",
    )
    parser.add_argument(
        "--slack-sec",
        type=float,
        default=1.0,
        help="Allowed slack beyond the connect timeout (seconds).",
    )
    args = parser.parse_args()

    postgrest_timeout, storage_timeout, pool_timeout = get_supabase_timeout_config()
    client = create_supabase_httpx_client()
    connect_timeout = client.timeout.connect if isinstance(client.timeout, httpx.Timeout) else None

    print(f"Testing timeout against: {args.url}")
    print(
        "Configured timeouts: "
        f"postgrest={postgrest_timeout}s "
        f"storage={storage_timeout}s "
        f"pool={pool_timeout}s "
        f"connect={connect_timeout}s"
    )

    start = time.monotonic()
    try:
        client.get(args.url)
        elapsed = time.monotonic() - start
        print(f"ERROR: request unexpectedly succeeded in {elapsed:.2f}s")
        return 1
    except Exception as exc:
        elapsed = time.monotonic() - start
        limit = connect_timeout or postgrest_timeout
        if elapsed > (limit + args.slack_sec):
            print(f"ERROR: request took {elapsed:.2f}s; expected <= {(limit + args.slack_sec):.2f}s")
            print(f"Exception: {exc!r}")
            return 1

        if is_timeout_error(exc):
            print(f"OK: timed out in {elapsed:.2f}s (limit {limit:.2f}s)")
        else:
            print(f"OK: failed fast in {elapsed:.2f}s ({exc.__class__.__name__})")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
