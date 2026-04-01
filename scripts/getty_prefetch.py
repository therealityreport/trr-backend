#!/usr/bin/env python3
"""
Getty Prefetch — Hybrid Pipeline Helper

Fetches ALL Getty editorial images and events locally (residential IP required
since Getty blocks cloud IPs) and sends them to the Modal pipeline API for
R2 storage and Supabase persistence.

Runs two search terms per person (Bravo-scoped + broad), deduplicates across
them, and scrapes both individual images and grouped events/albums.

Usage:
    # Full scrape → pipeline (recommended):
    python scripts/getty_prefetch.py "Brandi Glanville" \
        --person-id <uuid> \
        --api-url https://your-modal-app.modal.run \
        --auth-token <supabase-service-role-key>

    # With show context:
    python scripts/getty_prefetch.py "Brandi Glanville" \
        --person-id <uuid> \
        --api-url https://your-modal-app.modal.run \
        --auth-token <key> \
        --show-id <uuid> --show-name "Real Housewives of Beverly Hills"

    # Save to file (for debugging/inspection):
    python scripts/getty_prefetch.py "Brandi Glanville" --save /tmp/getty-brandi.json

    # Load from file and send to pipeline:
    python scripts/getty_prefetch.py "Brandi Glanville" \
        --load /tmp/getty-brandi.json \
        --person-id <uuid> \
        --api-url https://your-modal-app.modal.run \
        --auth-token <key>
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Add project root to path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))


def _dedup_assets(assets: list[dict], seen: set[str] | None = None) -> list[dict]:
    """Deduplicate assets by editorial_id, preserving order. Mutates `seen`."""
    if seen is None:
        seen = set()
    deduped: list[dict] = []
    for a in assets:
        eid = str(a.get("editorial_id") or "").strip()
        if eid and eid in seen:
            continue
        if eid:
            seen.add(eid)
        deduped.append(a)
    return deduped


def _dedup_events(events: list[dict], seen: set[str] | None = None) -> list[dict]:
    """Deduplicate events by event_url, preserving order. Mutates `seen`."""
    if seen is None:
        seen = set()
    deduped: list[dict] = []
    for e in events:
        url = str(e.get("event_url") or "").strip()
        if url and url in seen:
            continue
        if url:
            seen.add(url)
        deduped.append(e)
    return deduped


def _fetch_all_getty(person_name: str) -> dict:
    """Run Getty search locally for images and events.

    Uses limit=0 (unlimited) to fetch everything Getty has and returns
    merged image/event payloads plus per-query summaries.
    """
    from trr_backend.integrations.getty_local_prefetch import fetch_person_getty_prefetch_payload

    result = fetch_person_getty_prefetch_payload(person_name)
    print("\n[getty-prefetch] ═══ SUMMARY ═══")
    print(
        f"  Images: {int(result.get('merged_total') or 0)} unique"
        f" | Events: {int(result.get('merged_events_total') or 0)} unique"
        f" | Auth: {result.get('auth_mode') or 'unknown'}"
    )
    result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    return result


def _load_from_file(path: str) -> dict:
    """Load previously saved Getty results from JSON file."""
    with open(path) as f:
        data = json.load(f)
    # Rebuild merged lists if not present
    if "merged" not in data:
        seen: set[str] = set()
        merged = _dedup_assets(
            data.get("bravo_assets", []) + data.get("broad_assets", []),
            seen,
        )
        data["merged"] = merged
        data["merged_total"] = len(merged)
    if "merged_events" not in data:
        seen_ev: set[str] = set()
        merged_ev = _dedup_events(
            data.get("bravo_events", []) + data.get("broad_events", []),
            seen_ev,
        )
        data["merged_events"] = merged_ev
        data["merged_events_total"] = len(merged_ev)
    return data


def _send_to_pipeline(
    api_url: str,
    person_id: str,
    assets: list[dict],
    events: list[dict],
    *,
    show_id: str | None = None,
    show_name: str | None = None,
    auth_token: str | None = None,
) -> None:
    """Send prefetched Getty assets and events to the pipeline API."""
    import uuid as _uuid

    import requests

    url = f"{api_url.rstrip('/')}/api/v1/admin/person/{person_id}/refresh-images/stream"

    payload: dict = {
        "sources": ["nbcumv"],
        "getty_prefetched_assets": assets,
        "getty_prefetched_events": events,
        "limit_per_source": 10000,
    }
    if show_id:
        payload["show_id"] = show_id
    if show_name:
        payload["show_name"] = show_name

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    # Force a unique workflow ID so we never attach to an existing operation
    headers["x-trr-flow-key"] = f"getty-prefetch-{_uuid.uuid4().hex[:12]}"
    headers["x-trr-tab-session-id"] = f"cli-prefetch-{_uuid.uuid4().hex[:8]}"

    print("\n[getty-prefetch] Sending to pipeline:")
    print(f"  URL: {url}")
    print(f"  Assets: {len(assets)}")
    print(f"  Events: {len(events)}")
    payload_bytes = len(json.dumps(payload, default=str))
    print(f"  Payload size: {payload_bytes / 1024:.0f} KB ({payload_bytes / 1024 / 1024:.1f} MB)")

    resp = requests.post(url, json=payload, headers=headers, stream=True, timeout=1800)
    if resp.status_code != 200:
        print(f"[getty-prefetch] ERROR: {resp.status_code} — {resp.text[:500]}")
        sys.exit(1)

    # Stream SSE output
    for line in resp.iter_lines(decode_unicode=True):
        if line:
            print(line)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Getty Prefetch — scrapes ALL Getty images & events for a person",
    )
    parser.add_argument("person_name", help="Person name for Getty search")
    parser.add_argument("--save", help="Save results to JSON file (for debugging)")
    parser.add_argument("--load", help="Load results from existing JSON file instead of fetching")
    parser.add_argument("--person-id", help="Person UUID (required to send to pipeline)")
    parser.add_argument("--api-url", help="Pipeline API base URL (e.g., https://your-app.modal.run)")
    parser.add_argument("--show-id", help="Optional show UUID for pipeline context")
    parser.add_argument("--show-name", help="Optional show name for pipeline context")
    parser.add_argument("--auth-token", help="Auth token for pipeline API (Supabase service role key)")

    args = parser.parse_args()

    # Fetch or load
    if args.load:
        print(f"[getty-prefetch] Loading from {args.load}")
        data = _load_from_file(args.load)
        print(
            f"[getty-prefetch] Loaded {data.get('merged_total', 0)} images, {data.get('merged_events_total', 0)} events"
        )
    else:
        data = _fetch_all_getty(args.person_name)

    # Save if requested
    if args.save:
        with open(args.save, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"[getty-prefetch] Saved to {args.save}")

    # Send to pipeline if API URL provided
    if args.api_url:
        if not args.person_id:
            print("[getty-prefetch] ERROR: --person-id is required when using --api-url")
            sys.exit(1)
        merged_assets = data.get("merged", data.get("bravo_assets", []) + data.get("broad_assets", []))
        merged_events = data.get("merged_events", data.get("bravo_events", []) + data.get("broad_events", []))
        _send_to_pipeline(
            args.api_url,
            args.person_id,
            merged_assets,
            merged_events,
            show_id=args.show_id,
            show_name=args.show_name,
            auth_token=args.auth_token,
        )

    if not args.save and not args.api_url:
        # Just print summary — no pipeline target specified
        print("\nTo send to pipeline, add:")
        print("  --person-id <uuid> --api-url <url> --auth-token <key>")
        print("\nTo save for inspection:")
        print(f"  --save /tmp/getty-{args.person_name.lower().replace(' ', '-')}.json")


if __name__ == "__main__":
    main()
