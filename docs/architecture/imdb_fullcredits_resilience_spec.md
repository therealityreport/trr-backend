# Specification: IMDb Full Cast Sync Resilience

**Status:** Draft
**Author:** TRR Backend Team
**Date:** 2026-01-14
**Target Release:** Next

## Problem Statement

### Current Behavior

The `sync_show_cast.py` script fails completely when IMDb's `/fullcredits/` endpoint returns HTTP 202 (Accepted) or 403 (Forbidden) responses. These responses indicate anti-bot challenges or rate limiting, not actual missing data.

**Impact:**
- `core.show_cast` table remains empty for affected shows
- Pipeline fails with `ImdbFullCreditsError`
- No fallback mechanism exists
- Users must manually paste cookies via `IMDB_EXTRA_HEADERS_JSON` (fragile, expires frequently)

**Example Failure:**
```bash
$ PYTHONPATH=. python scripts/sync_show_cast.py --imdb-id tt1720601 --verbose
# Output: IMDb request failed with HTTP 202.
# cast_rows_total=0
# core.show_cast: empty
```

### Root Cause

1. **Brittle HTTP parsing:** `HttpImdbFullCreditsClient.fetch_fullcredits_page()` treats all non-200 status codes as fatal errors (line 71-76 in `fullcredits_cast_parser.py`)
2. **No retry logic:** Single HTTP request with no backoff/jitter on 202 responses
3. **No fallback data source:** IMDb provides a JSON credits API (`api.imdbapi.dev`) that's not blocked, but the code doesn't use it
4. **Cookie dependency:** `IMDB_EXTRA_HEADERS_JSON` is required for unblocking but is undocumented and unreliable

## Proposed Solution

Implement a **layered resilience strategy** with retry logic, automatic API fallback, and improved observability:

### 1. Enhanced Error Handling for 202/403 Responses

**Treat 202/403 as "blocked/challenged" (not generic failure):**
- Extend `ImdbFullCreditsError` to include a `is_blocked` flag
- Log clear messages: `"IMDb fullcredits blocked/challenge (status=202)"`
- Save response body to debug artifact when `--verbose` is enabled

**Implementation:**
```python
class ImdbFullCreditsError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_snippet: str | None = None,
        is_blocked: bool = False,  # NEW
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet
        self.is_blocked = is_blocked  # NEW
```

**Debug artifact saving:**
- Path: `debug_html/imdb_fullcredits_<imdb_id>_<timestamp>.html`
- Triggered by: `--verbose` flag + status in `{202, 403}`
- **Security:** Strip `Cookie`, `Authorization` headers from logs

### 2. Retry Logic with Exponential Backoff

**For 202 responses specifically:**
- Retry 2-3 times before giving up (configurable via env)
- Exponential backoff: start at 5s, double each attempt
- Add jitter: ±25% randomization to avoid thundering herd

**Environment variables:**
```bash
IMDB_FULLCREDITS_MAX_RETRIES=3  # default: 3
IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC=5  # default: 5
```

**Implementation pattern** (reuse logic from `credits_client.py` lines 42-71):
```python
def fetch_fullcredits_page(self, imdb_series_id: str) -> str:
    max_retries = int(os.getenv("IMDB_FULLCREDITS_MAX_RETRIES", "3"))
    base_delay = float(os.getenv("IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC", "5.0"))

    for attempt in range(max_retries):
        resp = self._session.get(url, ...)

        if resp.status_code == 200:
            return resp.text

        if resp.status_code in {202, 403} and attempt < max_retries - 1:
            delay = base_delay * (2 ** attempt)
            jitter = random.uniform(0, delay * 0.25)
            time.sleep(delay + jitter)
            continue

        # Exhausted retries or non-retryable error
        raise ImdbFullCreditsError(..., is_blocked=(resp.status_code in {202, 403}))
```

### 3. JSON API Fallback for Cast Ingestion

**When HTML fails with 202/403:**
- Automatically fall back to `fetch_title_credits()` from `credits_client.py`
- Normalize JSON response into `CastRow` structure
- Apply same "Self" category filtering
- Continue existing upsert pipeline

**New normalization function:**
```python
def normalize_api_credits_to_cast_rows(
    credits_response: ImdbTitleCredits,
    *,
    job_category_filter: str | None = None,
) -> list[CastRow]:
    """
    Map JSON API credits response to CastRow format.

    Args:
        credits_response: Response from api.imdbapi.dev
        job_category_filter: Optional filter (e.g., IMDB_JOB_CATEGORY_SELF)

    Returns:
        List of CastRow instances compatible with sync_show_cast pipeline
    """
    rows: list[CastRow] = []
    for idx, credit in enumerate(credits_response.credits, start=1):
        # Map category (e.g., "self", "actor") to job_category_id
        category = (credit.get("category") or "").strip().lower()
        if job_category_filter and category != "self":
            continue

        name_id = credit.get("id")  # e.g., "nm0000148"
        name = credit.get("name")
        role = credit.get("characters", [])  # List of character names
        role_text = ", ".join(role) if role else None

        if name_id and name:
            rows.append(CastRow(
                name_id=name_id,
                name=name,
                billing_order=idx,
                raw_role_text=role_text,
                job_category_id=IMDB_JOB_CATEGORY_SELF if category == "self" else None,
            ))
    return rows
```

**Integration point** (in `sync_show_cast.py`):
```python
try:
    cast_rows = fetch_fullcredits_cast(imdb_id, extra_headers=extra_headers)
    source = "fullcredits_html"
except ImdbFullCreditsError as exc:
    if exc.is_blocked:
        # Automatic fallback to JSON API
        if args.verbose:
            print(f"Falling back to JSON API for {imdb_id} (HTML blocked)")
        credits_response = fetch_title_credits(imdb_id)
        cast_rows = normalize_api_credits_to_cast_rows(
            credits_response,
            job_category_filter="self",
        )
        source = "credits_api_fallback"
    else:
        raise
```

### 4. Data Source Tracking

**Database schema change:**
Add `source_type` column to `core.show_cast`:

```sql
-- New migration: supabase/migrations/00XX_add_show_cast_source_tracking.sql
ALTER TABLE core.show_cast
ADD COLUMN source_type TEXT DEFAULT 'fullcredits_html'
CHECK (source_type IN ('fullcredits_html', 'credits_api_fallback', 'manual'));

CREATE INDEX idx_show_cast_source_type ON core.show_cast(source_type);

COMMENT ON COLUMN core.show_cast.source_type IS
'Data source for this cast entry: fullcredits_html (scraped), credits_api_fallback (JSON API), manual (user-entered)';
```

**Repository update:**
```python
def upsert_show_cast(
    db: Client,
    rows: Iterable[Mapping[str, Any]],
    *,
    on_conflict: str = "show_id,person_id,credit_category",
    source_type: str = "fullcredits_html",  # NEW
) -> list[dict[str, Any]]:
    payload = [
        {**dict(r), "source_type": source_type}  # NEW
        for r in rows
    ]
    # ... rest of function
```

### 5. IMDB_EXTRA_HEADERS_JSON Documentation Update

**Make it clear this is optional/debugging only:**

**`.env.example` addition:**
```bash
# ----------------------------
# IMDb Integration (Optional)
# ----------------------------

# Optional: Custom headers for IMDb HTML scraping (debugging/local unblock only)
# WARNING: Cookies are sensitive and expire frequently. Do NOT rely on this for production.
# The pipeline will automatically fall back to JSON API if HTML is blocked.
# Example: IMDB_EXTRA_HEADERS_JSON={"Cookie": "your-session-cookie", "User-Agent": "..."}
IMDB_EXTRA_HEADERS_JSON=
```

**Code comment in `fullcredits_cast_parser.py`:**
```python
class HttpImdbFullCreditsClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        extra_headers: Mapping[str, str] | None = None,  # Optional, for debugging only
        timeout_seconds: float = 30.0,
    ) -> None:
        """
        Initialize IMDb full credits HTTP client.

        Args:
            session: Optional requests.Session for connection pooling
            extra_headers: Optional headers (e.g., from IMDB_EXTRA_HEADERS_JSON).
                          **WARNING:** Cookies are fragile and should not be relied upon.
                          The client will automatically fall back to JSON API on 202/403.
            timeout_seconds: HTTP request timeout
        """
```

## API / Interface Design

### New Functions

**`fullcredits_cast_parser.py` additions:**
```python
def normalize_api_credits_to_cast_rows(
    credits_response: ImdbTitleCredits,
    *,
    job_category_filter: str | None = None,
) -> list[CastRow]:
    """Map JSON API credits to CastRow format."""
    pass

def fetch_fullcredits_cast_with_fallback(
    series_id: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
    verbose: bool = False,
) -> tuple[list[CastRow], str]:
    """
    Fetch cast with automatic fallback.

    Returns:
        (cast_rows, source) where source is "fullcredits_html" or "credits_api_fallback"
    """
    pass
```

### Modified Functions

**`HttpImdbFullCreditsClient.fetch_fullcredits_page()`:**
- Add retry logic with exponential backoff
- Save debug HTML on 202/403 when verbose mode enabled
- Raise `ImdbFullCreditsError` with `is_blocked=True` for 202/403

**`upsert_show_cast()` in `show_cast.py`:**
- Add `source_type` parameter (default: "fullcredits_html")
- Include `source_type` in upserted rows

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IMDB_FULLCREDITS_MAX_RETRIES` | `3` | Max retry attempts for 202 responses |
| `IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC` | `5.0` | Base delay for exponential backoff (seconds) |
| `IMDB_EXTRA_HEADERS_JSON` | (empty) | **Optional** custom headers for debugging (fragile, not recommended) |

## Data Model Changes

### Migration: `supabase/migrations/00XX_add_show_cast_source_tracking.sql`

```sql
-- Add source tracking column
ALTER TABLE core.show_cast
ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'fullcredits_html';

-- Add constraint
ALTER TABLE core.show_cast
DROP CONSTRAINT IF EXISTS show_cast_source_type_check;

ALTER TABLE core.show_cast
ADD CONSTRAINT show_cast_source_type_check
CHECK (source_type IN ('fullcredits_html', 'credits_api_fallback', 'manual'));

-- Add index for analytics queries
CREATE INDEX IF NOT EXISTS idx_show_cast_source_type
ON core.show_cast(source_type);

-- Add column comment
COMMENT ON COLUMN core.show_cast.source_type IS
'Data source: fullcredits_html (HTML scrape), credits_api_fallback (JSON API), manual (user entry)';
```

**Schema impact:**
- **Backwards compatible:** Existing rows default to `'fullcredits_html'`
- **New column:** `source_type TEXT NOT NULL DEFAULT 'fullcredits_html'`
- **Index added:** `idx_show_cast_source_type` for analytics queries

## Testing Strategy

### Unit Tests (Offline Fixtures)

**Test file:** `tests/integrations/imdb/test_fullcredits_resilience.py`

1. **Test 202 retry behavior:**
   - Mock sequence: `[202, 202, 200]` → assert 3 attempts, final success
   - Mock sequence: `[202, 202, 202]` → assert raises `ImdbFullCreditsError(is_blocked=True)`

2. **Test fallback to JSON API:**
   - Mock `/fullcredits/` returning 202
   - Mock `fetch_title_credits()` returning valid JSON
   - Assert `normalize_api_credits_to_cast_rows()` produces valid `CastRow` instances

3. **Test normalization layer:**
   - Fixture: JSON API response with "self" category credits
   - Assert: Maps to `CastRow` with correct `name_id`, `name`, `raw_role_text`
   - Assert: Filters non-"self" categories when `job_category_filter="self"`

4. **Test debug HTML saving:**
   - Mock 202 response with `verbose=True`
   - Assert: File written to `debug_html/imdb_fullcredits_ttXXXXX_*.html`
   - Assert: Cookie/Authorization headers **not** in saved file

### Integration Tests (Script-Level)

**Test file:** `tests/scripts/test_sync_show_cast_resilience.py`

1. **End-to-end with fallback:**
   - Mock IMDb HTML blocked (202)
   - Mock JSON API success
   - Assert: `core.show_cast` populated with `source_type='credits_api_fallback'`

2. **End-to-end with HTML success:**
   - Mock IMDb HTML success (200)
   - Assert: `core.show_cast` populated with `source_type='fullcredits_html'`

### Manual Testing Checklist

- [ ] Run `sync_show_cast.py --imdb-id tt1720601 --verbose` (show known to return 202)
- [ ] Verify `source_type` column populated correctly
- [ ] Verify debug HTML saved to `debug_html/` directory
- [ ] Verify logs show: "Falling back to JSON API for tt1720601 (HTML blocked)"
- [ ] Verify `core.show_cast` has non-zero rows for tt1720601
- [ ] Test with `IMDB_FULLCREDITS_MAX_RETRIES=1` → faster failure
- [ ] Test with valid `IMDB_EXTRA_HEADERS_JSON` → HTML path succeeds

## Security & Performance Implications

### Security

**✅ Improvements:**
- **No cookie logging:** Debug HTML saving strips sensitive headers
- **Reduced cookie dependency:** JSON API fallback eliminates need for fragile cookie management
- **Clear documentation:** `.env.example` warns against relying on cookies

**⚠️ Considerations:**
- JSON API endpoint (`api.imdbapi.dev`) is third-party; validate it's trustworthy
- Rate limiting on JSON API unknown; may need future throttling

### Performance

**Latency:**
- **Retry overhead:** ~5-20s additional latency on 202 responses (acceptable for batch pipeline)
- **Fallback overhead:** JSON API is typically faster than HTML parsing (~2-3s vs ~5s)

**Throughput:**
- No impact on successful HTML fetches (hot path unchanged)
- Failed shows now succeed via fallback (improves overall pipeline success rate)

**Database:**
- New `source_type` column: minimal storage impact (~10 bytes per row)
- New index: small overhead, improves analytics queries

### Observability

**Logging enhancements:**
- Clear distinction between blocked (202/403) and failed (500, network errors)
- Source type logged per show: `"source=fullcredits_html"` vs `"source=credits_api_fallback"`
- Debug artifacts saved for blocked responses (when `--verbose`)

**Metrics to track (future):**
- % of shows using HTML vs JSON API fallback
- Retry success rate (202 → 200 after N attempts)
- Average latency: HTML vs JSON API

## Migration Plan

### Phase 1: Database Schema (Non-Breaking)
1. Apply migration `00XX_add_show_cast_source_tracking.sql`
2. Verify existing rows default to `'fullcredits_html'`
3. Verify index created successfully

### Phase 2: Code Changes
1. Update `ImdbFullCreditsError` with `is_blocked` flag
2. Add retry logic to `fetch_fullcredits_page()`
3. Implement `normalize_api_credits_to_cast_rows()`
4. Update `sync_show_cast.py` to use fallback
5. Update `upsert_show_cast()` to accept `source_type`

### Phase 3: Testing
1. Run unit tests (offline fixtures)
2. Run integration tests (mocked script tests)
3. Manual testing with tt1720601 (known 202 case)

### Phase 4: Documentation
1. Update `.env.example` with IMDb variables
2. Add docstrings to new functions
3. Update `docs/architecture/integrations.md` with fallback behavior

### Phase 5: Deployment
1. Deploy to dev/staging environment
2. Run `sync_show_cast.py` on full show catalog
3. Verify `source_type` distribution (expect ~5-10% API fallback)
4. Deploy to production

## Rollback Plan

If issues arise:
1. **Code rollback:** Revert PR, fallback is opt-in (no user impact)
2. **Schema rollback:** `ALTER TABLE core.show_cast DROP COLUMN source_type;` (safe, column is nullable with default)
3. **Data integrity:** No existing data affected (backwards compatible)

## Success Criteria

**Must Have:**
- ✅ `sync_show_cast.py --imdb-id tt1720601` succeeds (non-zero `core.show_cast` rows)
- ✅ Logs indicate source: "source=fullcredits_html" or "source=credits_api_fallback"
- ✅ No sensitive headers in debug HTML files
- ✅ Unit tests cover 200, 202, and fallback scenarios
- ✅ `source_type` column populated correctly

**Nice to Have:**
- ⭐ <5% of shows use API fallback (HTML path is preferred)
- ⭐ Retry success rate >50% (202 → 200 after backoff)
- ⭐ Debug HTML artifacts saved for all blocked responses (when `--verbose`)

## Open Questions / Future Work

1. **JSON API rate limits:** What are the limits for `api.imdbapi.dev`? Need throttling?
2. **Cache HTML responses:** Should we cache successful HTML responses to reduce IMDb load?
3. **Telemetry:** Add structured logging (JSON) for easier analytics?
4. **Parallel fallback:** Try HTML and JSON API concurrently, use first successful response?

## References

- [IMDb Full Credits Parser](../../trr_backend/integrations/imdb/fullcredits_cast_parser.py)
- [IMDb Credits Client (JSON API)](../../trr_backend/integrations/imdb/credits_client.py)
- [Sync Show Cast Script](../../scripts/sync_show_cast.py)
- [Architecture: Integrations](./integrations.md)
- [Database Schema Docs](../db/schema.md)
