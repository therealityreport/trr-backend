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
import asyncio
import logging
import os
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

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402

from trr_backend.utils.env import load_env  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
)
logger = logging.getLogger("getty-local-server")
load_env()

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

# ---------------------------------------------------------------------------
# Shared-secret auth — when TRR_GETTY_SCRAPER_SECRET is set, all requests
# must include a matching X-Scraper-Secret header.  This prevents the
# Cloudflare Tunnel endpoint from being an open scraper on the internet.
# When the env var is unset (local dev), auth is skipped.
# ---------------------------------------------------------------------------
_SCRAPER_SECRET = os.getenv("TRR_GETTY_SCRAPER_SECRET", "").strip()


@app.middleware("http")
async def _check_scraper_secret(request: Request, call_next):  # noqa: ANN001
    if _SCRAPER_SECRET and request.url.path != "/health":
        provided = (request.headers.get("x-scraper-secret") or "").strip()
        if provided != _SCRAPER_SECRET:
            return JSONResponse(
                status_code=403,
                content={"detail": "Invalid or missing X-Scraper-Secret header"},
            )
    return await call_next(request)


class ScrapeRequest(BaseModel):
    person_name: str = Field(..., min_length=1, description="Full name to search Getty for")
    show_name: str | None = Field(default=None, description="Optional show name for show-scoped Getty query")
    mode: str = Field(default="full", description="Getty prefetch mode: discovery or full")


class ScrapeResponse(BaseModel):
    person: str
    show_name: str | None = None
    merged: list[dict[str, Any]]
    merged_total: int
    merged_events: list[dict[str, Any]]
    merged_events_total: int
    image_overlap_count: int
    event_overlap_count: int
    query_summaries: list[dict[str, Any]] = Field(default_factory=list)
    auth_mode: str | None = None
    auth_warning: str | None = None
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
    person_name = req.person_name.strip()
    from trr_backend.integrations.getty_local_prefetch import fetch_person_getty_prefetch_payload

    t0 = time.perf_counter()
    result = await asyncio.to_thread(
        fetch_person_getty_prefetch_payload,
        person_name,
        show_name=req.show_name,
        mode=req.mode,
    )
    logger.info(
        "DONE — %d images, %d events in %.1fs (auth_mode=%s)",
        int(result.get("merged_total") or 0),
        int(result.get("merged_events_total") or 0),
        time.perf_counter() - t0,
        result.get("auth_mode") or "unknown",
    )
    return result


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
