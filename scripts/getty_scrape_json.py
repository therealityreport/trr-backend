#!/usr/bin/env python3
"""
Getty scraper — JSON-to-stdout mode for subprocess invocation.

Called by the admin UI's Next.js route handler to scrape Getty images
via the local machine's residential IP.  Progress goes to stderr;
clean JSON goes to stdout.

Usage:
    python scripts/getty_scrape_json.py "Brandi Glanville"
"""

from __future__ import annotations

import argparse
import builtins
import json
import sys
from pathlib import Path
from typing import Any

# Ensure project root is on path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

# Redirect all print() calls to stderr so stdout stays clean JSON
_original_print = builtins.print
builtins.print = lambda *args, **kwargs: _original_print(*args, **{**kwargs, "file": sys.stderr})


def _scrape(
    person_name: str,
    *,
    show_name: str | None = None,
    mode: str = "full",
    transport_mode: str = "auto",
) -> dict[str, Any]:
    from trr_backend.integrations.getty_local_prefetch import fetch_person_getty_prefetch_payload

    result = fetch_person_getty_prefetch_payload(
        person_name,
        show_name=show_name,
        mode=mode,
        transport_mode=transport_mode,
    )
    print(
        f"[getty] DONE — {int(result.get('merged_total') or 0)} images, "
        f"{int(result.get('merged_events_total') or 0)} events in "
        f"{float(result.get('elapsed_seconds') or 0):.1f}s "
        f"(auth_mode={result.get('auth_mode') or 'unknown'})"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Getty JSON scrape")
    parser.add_argument("person_name", help="Getty person phrase")
    parser.add_argument("--show-name", dest="show_name", default=None, help="Optional show name")
    parser.add_argument(
        "--mode",
        dest="mode",
        default="full",
        choices=("discovery", "full"),
        help="Getty prefetch mode",
    )
    parser.add_argument(
        "--transport-mode",
        dest="transport_mode",
        default="auto",
        choices=("auto", "decodo_remote", "local_browser", "cookies_only"),
        help="Getty transport strategy",
    )
    args = parser.parse_args()

    result = _scrape(
        args.person_name,
        show_name=args.show_name,
        mode=args.mode,
        transport_mode=args.transport_mode,
    )

    # Write clean JSON to stdout (print was redirected to stderr)
    builtins.print = _original_print
    print(json.dumps(result, default=str))
