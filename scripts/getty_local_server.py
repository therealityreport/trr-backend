#!/usr/bin/env python3
"""
Getty Local Scraper — lightweight server running on your machine (residential IP).

Getty blocks cloud/datacenter IPs. This server runs locally so that the admin UI
can scrape Getty via the browser → Next.js proxy → this server → gettyimages.com
chain, all on your residential IP.  Results are returned to the frontend which
then forwards them to the Modal pipeline as getty_prefetched_assets/events.

Usage:
    # Start (default port 3456):
    python scripts/getty_local_server.py

    # Custom port:
    python scripts/getty_local_server.py --port 8765

    # Or via the workspace Makefile:
    make getty-server
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Bootstrap: ensure the project root is on sys.path so we can import the
# trr_backend package regardless of how this script is invoked.
# ---------------------------------------------------------------------------
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
)
logger = logging.getLogger("getty-local-server")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Getty Local Scraper",
    description="Residential-IP Getty scraper for the TRR hybrid pipeline.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ScrapeRequest(BaseModel):
    person_name: str = Field(..., min_length=1, description="Full name to search Getty for")


class ScrapeResponse(BaseModel):
    person: str
    merged: list[dict[str, Any]]
    merged_total: int
    merged_events: list[dict[str, Any]]
    merged_events_total: int
    image_overlap_count: int
    event_overlap_count: int
    elapsed_seconds: float


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "getty-local-scraper"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_getty(req: ScrapeRequest) -> dict[str, Any]:
    """Scrape ALL Getty editorial images and events for a person.

    Runs two search terms (Bravo-scoped + broad), deduplicates across them,
    and returns the merged results.  This can take 1-5 minutes for persons
    with large Getty catalogs.
    """
    from trr_backend.integrations import getty as getty_integration

    person_name = req.person_name.strip()
    bravo_phrase = f"{person_name} Bravo"
    broad_phrase = person_name
    t0 = time.perf_counter()

    # ------------------------------------------------------------------
    # Individual images
    # ------------------------------------------------------------------
    logger.info("Searching images for '%s' (unlimited)...", bravo_phrase)
    bravo_assets = getty_integration.search_editorial_assets(bravo_phrase, limit=0)
    for a in bravo_assets:
        a["source_query_scope"] = "bravo"
    logger.info("  → %d Bravo images in %.1fs", len(bravo_assets), time.perf_counter() - t0)

    t1 = time.perf_counter()
    logger.info("Searching images for '%s' (unlimited)...", broad_phrase)
    broad_assets = getty_integration.search_editorial_assets(
        broad_phrase, limit=0, query_params={"sort": "best"},
    )
    for a in broad_assets:
        a.setdefault("source_query_scope", "broad")
    logger.info("  → %d broad images in %.1fs", len(broad_assets), time.perf_counter() - t1)

    # Cross-dedup images (bravo takes priority)
    seen_ids: set[str] = set()
    merged_assets: list[dict[str, Any]] = []
    for a in bravo_assets + broad_assets:
        eid = str(a.get("editorial_id") or "").strip()
        if eid and eid in seen_ids:
            continue
        if eid:
            seen_ids.add(eid)
        merged_assets.append(a)
    image_overlap = len(bravo_assets) + len(broad_assets) - len(merged_assets)

    # ------------------------------------------------------------------
    # Grouped events / albums
    # ------------------------------------------------------------------
    t2 = time.perf_counter()
    logger.info("Searching events for '%s' (unlimited)...", bravo_phrase)
    bravo_events = getty_integration.search_grouped_events(
        bravo_phrase, limit=0, person_name=person_name, source_query_scope="bravo",
    )
    logger.info("  → %d Bravo events in %.1fs", len(bravo_events), time.perf_counter() - t2)

    t3 = time.perf_counter()
    logger.info("Searching events for '%s' (unlimited)...", broad_phrase)
    broad_events = getty_integration.search_grouped_events(
        broad_phrase, limit=0, person_name=person_name,
        source_query_scope="broad", query_params={"sort": "best"},
    )
    logger.info("  → %d broad events in %.1fs", len(broad_events), time.perf_counter() - t3)

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
    logger.info(
        "DONE — %d images, %d events in %.1fs",
        len(merged_assets), len(merged_events), elapsed,
    )

    return {
        "person": person_name,
        "merged": merged_assets,
        "merged_total": len(merged_assets),
        "merged_events": merged_events,
        "merged_events_total": len(merged_events),
        "image_overlap_count": image_overlap,
        "event_overlap_count": event_overlap,
        "elapsed_seconds": round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Getty Local Scraper server")
    parser.add_argument("--port", type=int, default=3456, help="Port to listen on (default: 3456)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    args = parser.parse_args()

    import uvicorn

    logger.info("Starting Getty Local Scraper on %s:%d", args.host, args.port)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
