#!/usr/bin/env python3
"""Measure first-page admin show and season asset route timings.

This helper is read-only. It calls the local backend admin asset routes and
prints one JSON object per request so before/after timings are easy to compare.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:8000/api/v1/admin/trr-api"
DEFAULT_SHOW_ID = "0306e098-f671-4815-972c-696c359243b6"
DEFAULT_SEASON_NUMBER = 4


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure admin gallery asset first-page timings.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Backend admin TRR API base URL.")
    parser.add_argument("--show-id", default=DEFAULT_SHOW_ID, help="Show UUID to measure.")
    parser.add_argument("--season-number", type=int, default=DEFAULT_SEASON_NUMBER, help="Season number to measure.")
    parser.add_argument("--limit", type=int, default=48, help="Requested visible page size.")
    parser.add_argument("--repeat", type=int, default=1, help="Number of times to call each route.")
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout in seconds.")
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="NAME:VALUE",
        help="Optional request header. Repeat for auth or local proxy headers.",
    )
    return parser.parse_args(argv)


def _parse_headers(raw_headers: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    for raw_header in raw_headers:
        name, separator, value = raw_header.partition(":")
        if not separator or not name.strip():
            raise ValueError(f"Invalid --header value: {raw_header!r}")
        headers[name.strip()] = value.strip()
    return headers


def _request_json(url: str, timeout: float, headers: dict[str, str]) -> dict[str, Any]:
    started_at = time.perf_counter()
    request = Request(url, headers=headers)
    status = 0
    body = b""
    error: str | None = None
    try:
        with urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            body = response.read()
    except HTTPError as exc:
        status = int(exc.code)
        body = exc.read()
        error = str(exc)
    except URLError as exc:
        error = str(exc.reason)
    except TimeoutError as exc:
        error = str(exc)

    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    payload: dict[str, Any] | None = None
    if body:
        try:
            parsed = json.loads(body.decode("utf-8"))
            payload = parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            payload = None

    assets = payload.get("assets") if payload else None
    pagination = payload.get("pagination") if payload else None
    return {
        "url": url,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "bytes": len(body),
        "asset_count": len(assets) if isinstance(assets, list) else None,
        "has_more": pagination.get("has_more") if isinstance(pagination, dict) else None,
        "error": error,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    base_url = str(args.base_url).rstrip("/")
    headers = _parse_headers(args.header)
    query = urlencode({"limit": args.limit})
    targets = [
        ("show-assets", f"{base_url}/shows/{args.show_id}/assets?{query}"),
        ("season-assets", f"{base_url}/shows/{args.show_id}/seasons/{args.season_number}/assets?{query}"),
    ]
    failed = False
    for iteration in range(1, max(args.repeat, 1) + 1):
        for route, url in targets:
            result = _request_json(url, timeout=args.timeout, headers=headers)
            result["route"] = route
            result["iteration"] = iteration
            print(json.dumps(result, sort_keys=True))
            if result["status"] >= 400 or result["status"] == 0:
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
