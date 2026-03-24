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

import builtins
import json
import sys
import time
from pathlib import Path
from typing import Any

# Ensure project root is on path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

# Redirect all print() calls to stderr so stdout stays clean JSON
_original_print = builtins.print
builtins.print = lambda *args, **kwargs: _original_print(*args, **{**kwargs, "file": sys.stderr})


def _scrape(person_name: str) -> dict[str, Any]:
    from trr_backend.integrations import getty as getty_integration

    bravo_phrase = f"{person_name} Bravo"
    broad_phrase = person_name
    t0 = time.perf_counter()

    # Individual images
    print(f"[getty] Searching images: '{bravo_phrase}' (unlimited)...")
    bravo_assets = getty_integration.search_editorial_assets(bravo_phrase, limit=0)
    for a in bravo_assets:
        a["source_query_scope"] = "bravo"
    print(f"[getty]   → {len(bravo_assets)} Bravo images ({time.perf_counter() - t0:.1f}s)")

    t1 = time.perf_counter()
    print(f"[getty] Searching images: '{broad_phrase}' (unlimited)...")
    broad_assets = getty_integration.search_editorial_assets(
        broad_phrase, limit=0, query_params={"sort": "best"},
    )
    for a in broad_assets:
        a.setdefault("source_query_scope", "broad")
    print(f"[getty]   → {len(broad_assets)} broad images ({time.perf_counter() - t1:.1f}s)")

    # Cross-dedup images
    seen_ids: set[str] = set()
    merged: list[dict[str, Any]] = []
    for a in bravo_assets + broad_assets:
        eid = str(a.get("editorial_id") or "").strip()
        if eid and eid in seen_ids:
            continue
        if eid:
            seen_ids.add(eid)
        merged.append(a)
    image_overlap = len(bravo_assets) + len(broad_assets) - len(merged)

    # Grouped events / albums
    t2 = time.perf_counter()
    print(f"[getty] Searching events: '{bravo_phrase}' (unlimited)...")
    bravo_events = getty_integration.search_grouped_events(
        bravo_phrase, limit=0, person_name=person_name, source_query_scope="bravo",
    )
    print(f"[getty]   → {len(bravo_events)} Bravo events ({time.perf_counter() - t2:.1f}s)")

    t3 = time.perf_counter()
    print(f"[getty] Searching events: '{broad_phrase}' (unlimited)...")
    broad_events = getty_integration.search_grouped_events(
        broad_phrase, limit=0, person_name=person_name,
        source_query_scope="broad", query_params={"sort": "best"},
    )
    print(f"[getty]   → {len(broad_events)} broad events ({time.perf_counter() - t3:.1f}s)")

    # Cross-dedup events
    seen_urls: set[str] = set()
    merged_events: list[dict[str, Any]] = []
    for e in bravo_events + broad_events:
        url = str(e.get("event_url") or "").strip()
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)
        merged_events.append(e)
    event_overlap = len(bravo_events) + len(broad_events) - len(merged_events)

    elapsed = time.perf_counter() - t0
    print(f"[getty] DONE — {len(merged)} images, {len(merged_events)} events in {elapsed:.1f}s")

    return {
        "merged": merged,
        "merged_total": len(merged),
        "merged_events": merged_events,
        "merged_events_total": len(merged_events),
        "image_overlap_count": image_overlap,
        "event_overlap_count": event_overlap,
        "elapsed_seconds": round(elapsed, 1),
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        _original_print("Usage: python getty_scrape_json.py <person_name>", file=sys.stderr)
        sys.exit(1)

    result = _scrape(sys.argv[1])

    # Write clean JSON to stdout (print was redirected to stderr)
    builtins.print = _original_print
    print(json.dumps(result, default=str))
