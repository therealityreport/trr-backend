# External Integrations

This repo pulls metadata from external sources (IMDb, TMDb, etc.). To keep those
concerns from spreading across the FastAPI app and pipeline scripts, **all new
external clients should live under** `trr_backend/integrations/`.

## Goals

- Keep network/client code out of `api/routers/` and out of one-off scripts.
- Make it obvious where to add new providers (TMDb, TVMaze, Peacock, etc.).
- Allow the pipeline and API to share integrations without importing each other.

## Layout

- `trr_backend/integrations/<provider>/…` — provider-specific clients + normalization
- `api/` — FastAPI app entrypoint + routers (should call into `trr_backend`)
- `scripts/` — pipeline stages (should call into `trr_backend` for shared logic)

## IMDb Landing Zone (episodic credits)

IMDb episodic credits client lives at:

- `trr_backend/integrations/imdb/episodic_client.py`

This module defines:

- `HttpImdbEpisodicClient` (real HTTP client for IMDb's persisted GraphQL query)
- `ImdbEpisodicClient` (protocol/port used by the rest of the codebase)
- `ImdbEpisodicCredits` / `ImdbEpisodeCredit` (normalized output types)
- Normalization helpers (private functions) used by the client and unit tests
- A manual debug harness (`python -m trr_backend.integrations.imdb.episodic_client`)

This module is intentionally decoupled from any specific pipeline/service layer,
so it can be reused by TRR backend ingestion and future screen-time analytics.

Live HTTP usage should stay in the integration layer; automated tests should rely
on fixtures and call normalization helpers (no network).
