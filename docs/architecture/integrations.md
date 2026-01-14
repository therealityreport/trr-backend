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

## IMDb Full Credits Resilience

The full credits scraper (`trr_backend/integrations/imdb/fullcredits_cast_parser.py`) implements a **layered resilience strategy** to handle IMDb blocking:

### Resilience Layers

1. **Retry Logic**: Exponential backoff for 202 (Accepted), 403 (Forbidden), and 429 (Too Many Requests) responses
   - Configurable: `IMDB_FULLCREDITS_MAX_RETRIES` (default: 2, meaning 3 total attempts)
   - Base delay: `IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC` (default: 5.0s)
   - Jitter: ±25% randomization to avoid thundering herd

2. **JSON API Fallback**: Automatic switch to `api.imdbapi.dev` on blocked HTML
   - Normalizes JSON credits to `CastRow` format via `normalize_api_credits_to_cast_rows()`
   - Applies same "Self" category filtering via `job_category_id`
   - Continues existing upsert pipeline
   - Can be disabled: `IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0`

3. **Debug Artifacts**: Saves blocked HTML responses when `--verbose` flag enabled
   - Path: `debug_html/imdb_fullcredits_<imdb_id>_<timestamp>.html`
   - Security: Strips `Cookie` and `Authorization` headers
   - Saves only once per show (on final blocked attempt)

4. **Source Tracking**: `core.show_cast.source_type` column tracks data origin
   - Values: `fullcredits_html`, `credits_api_fallback`, `manual`
   - Enables analytics on fallback usage

### Usage

**Recommended:** Use `fetch_fullcredits_cast_with_fallback()` for automatic fallback:

```python
from trr_backend.integrations.imdb.fullcredits_cast_parser import fetch_fullcredits_cast_with_fallback

rows, source_type = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=True)
print(f"Got {len(rows)} rows from {source_type}")
```

**Sync script:**

```bash
# Sync with automatic fallback
PYTHONPATH=. python scripts/sync_show_cast.py --imdb-id tt1720601 --verbose

# Configure retry behavior
IMDB_FULLCREDITS_MAX_RETRIES=1 \
IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC=3.0 \
PYTHONPATH=. python scripts/sync_show_cast.py --imdb-id tt1720601

# Disable fallback (for testing/rollback)
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 \
PYTHONPATH=. python scripts/sync_show_cast.py --imdb-id tt1720601
```

See [IMDb Full Credits Resilience Spec](./imdb_fullcredits_resilience_spec.md) for full design rationale.
