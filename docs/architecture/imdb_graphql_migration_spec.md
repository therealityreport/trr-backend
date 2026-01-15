# IMDb GraphQL Migration Specification

**Status:** Draft (Revised)
**Created:** 2026-01-14
**Last Updated:** 2026-01-14 (Critical fixes applied)
**Author:** Claude Sonnet 4.5
**Related:** PR #17 (IMDb Full Credits Resilience)
**Target:** Q1 2026

**Revision Notes (2026-01-14):**
- ✅ Fixed category ID inconsistency (use `IMDB_JOB_CATEGORY_SELF` constant)
- ✅ Added cast selection policy to filter 945 credits → ~75-100 main cast
- ✅ Added `IMDB_CAST_PRIMARY_SOURCE` config for tier order flexibility
- ✅ Fixed `execute_query()` return type ambiguity
- ✅ Added operational/ToS risk section (legal review required)
- ✅ Added partial result signaling (`credits_graphql_paginated_partial`)
- ✅ Added hard caps with circuit breaker behavior

---

## Executive Summary

This specification outlines the migration from IMDb's incomplete REST-like JSON API (`api.imdbapi.dev`) to IMDb's internal GraphQL persisted query API (`caching.graphql.imdb.com`) for fetching cast credits, lists, episodes, and other data. The primary goal is to replace the "top-billed only" fallback implemented in PR #17 with a complete, paginated cast data source that returns all credits (e.g., ~945 credits for `tt1720601` instead of ~18 top-billed).

**Key Benefits:**
- **Complete data**: GraphQL pagination provides full cast lists, not just top-billed
- **Less blocking**: Cached GraphQL endpoint is more reliable than HTML scraping
- **Unified client**: Single reusable GraphQL client for Lists, Cast, Episodes, etc.
- **Future-proof**: Centralized persisted query hashes, easy to update

---

## Problem Statement

### Current State (Post-PR #17)

PR #17 implemented a 2-tier fallback system:
1. **Primary**: HTML scraping (`/fullcredits/`)
2. **Fallback**: JSON API (`api.imdbapi.dev/titles/{id}/credits`)

**Limitations:**
- JSON API returns only **top-billed cast** (~18-25 credits)
- No pagination support
- Incomplete data prevents accurate cast sync for large shows
- Users see "PARTIAL - top-billed only" warnings
- Multiple point-to-point integrations (Lists use GraphQL, Cast uses JSON, Episodes use HTML)

### Example: The Real Housewives of Beverly Hills (tt1720601)

| Source | Credits Returned | Completeness |
|--------|------------------|--------------|
| HTML `/fullcredits/` | ~945 | ✅ Complete (when not blocked) |
| JSON API fallback | ~18 | ❌ Top-billed only |
| GraphQL (target) | ~945 (paginated) | ✅ Complete |

---

## Proposed Solution

Replace the JSON API fallback with IMDb's internal GraphQL persisted query API, and standardize all IMDb integrations on GraphQL where possible.

### Architecture Overview

**Configurable tier order** (via `IMDB_CAST_PRIMARY_SOURCE`):

**Default (html-first) - Conservative rollout:**
```
┌─────────────────────────────────────────────────────────┐
│  Cast Sync Entry Point (fetch_fullcredits_cast_with_fallback) │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  Tier 1: HTML Scraping (/fullcredits/)                  │
│  - Most complete when available                          │
│  - Most likely to be blocked (202/403/429)              │
└──────────────────────────────────────────────────────────┘
                           │ (on block)
                           ▼
┌──────────────────────────────────────────────────────────┐
│  Tier 2: GraphQL Persisted Queries (NEW)                │
│  - caching.graphql.imdb.com                              │
│  - Complete paginated data (filtered for main cast)      │
│  - More reliable than HTML                               │
└──────────────────────────────────────────────────────────┘
                           │ (on failure)
                           ▼
┌──────────────────────────────────────────────────────────┐
│  Tier 3: JSON API Top-Billed (DEPRECATED)               │
│  - api.imdbapi.dev                                       │
│  - Partial data (last resort only)                       │
└──────────────────────────────────────────────────────────┘
```

**Alternative (graphql-first) - Maximum reliability:**
```
Tier 1: GraphQL (most reliable, filtered for main cast)
   ↓ (on failure)
Tier 2: HTML (most complete when available)
   ↓ (on block)
Tier 3: JSON API (deprecated, last resort)
```

**Configuration:**
```bash
# Primary source determines tier order
IMDB_CAST_PRIMARY_SOURCE=html  # or 'graphql' for maximum reliability
```

### Unified GraphQL Client

All IMDb integrations will use a single reusable GraphQL client:

```python
# trr_backend/integrations/imdb/graphql_persisted_client.py

class ImdbGraphQLPersistedClient:
    """
    Unified client for IMDb's GraphQL persisted query API.

    Supports:
    - Persisted queries (operationName + sha256Hash)
    - Pagination with cursor-based "after" tokens
    - Retry/backoff for 202/403/429/5xx
    - Multiple endpoints (caching.graphql.imdb.com + fallback)
    """

    def execute_query(
        self,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a single persisted query request.

        Returns:
            Raw GraphQL response dict

        Note: For pagination, use paginate_edges() helper or
        specialized wrappers like fetch_title_credits_paginated_v2()
        """

    def paginate_edges(
        self,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any],
        *,
        edges_path: str = "data.title.credits.edges",
        page_info_path: str = "data.title.credits.pageInfo",
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute paginated query and collect all edges.

        Handles cursor pagination automatically until hasNextPage=false
        or max_pages reached.

        Returns:
            List of all edge nodes collected across pages
        """
```

---

## Technical Requirements

### A) Core GraphQL Client

**New module:** `trr_backend/integrations/imdb/graphql_persisted_client.py`

**Responsibilities:**
- Execute persisted query requests against:
  - **Primary**: `https://caching.graphql.imdb.com/`
  - **Fallback**: `https://api.graphql.imdb.com/` (if caching host fails)
- Support request structure:
  ```json
  {
    "operationName": "TitleCreditPaginationV2",
    "variables": {
      "const": "tt1720601",
      "tconst": "tt1720601",
      "first": 250,
      "after": "eyJlc1Rva2VuIjpbIjI1MCJdLCJmaWx0ZXIiOnt9fQ==",
      "locale": "en-US",
      "category": "amzn1.imdb.concept.name_credit_group.self"
    },
    "extensions": {
      "persistedQuery": {
        "version": 1,
        "sha256Hash": "c2df29603060d12b6a76c48e2b47ac0ceee80e471f8cd8ee79abd672393e4bd8"
      }
    }
  }
  ```
- Retry logic with exponential backoff for:
  - 202 (Accepted - queued)
  - 403 (Forbidden - rate limit)
  - 429 (Too Many Requests)
  - 5xx (Server errors)
- Request throttling hooks
- Optional `IMDB_EXTRA_HEADERS_JSON` merge

**Configuration (`.env.example`):**
```bash
# IMDb GraphQL API Configuration
IMDB_GRAPHQL_BASE_URL=https://caching.graphql.imdb.com/
IMDB_GRAPHQL_FALLBACK_URL=https://api.graphql.imdb.com/
IMDB_GRAPHQL_MAX_RETRIES=2
IMDB_GRAPHQL_RETRY_BASE_DELAY_SEC=2.0
IMDB_GRAPHQL_LOCALE=en-US
IMDB_GRAPHQL_TIMEOUT_SEC=30.0

# Pagination defaults
IMDB_GRAPHQL_PAGE_SIZE=250
IMDB_GRAPHQL_MAX_PAGES=10  # Hard cap to prevent runaway pagination

# Cast sync configuration
IMDB_CAST_PRIMARY_SOURCE=html  # 'html' (conservative) or 'graphql' (reliable)
IMDB_SHOW_CAST_MIN_EPISODES=3  # Min episodes to qualify as series cast
IMDB_SHOW_CAST_MAX_MEMBERS=100  # Max cast members per show (prevent pollution)

# Feature flags
IMDB_GRAPHQL_ENABLED=1  # Set to 0 to disable GraphQL fallback entirely
```

**IMPORTANT**: `IMDB_GRAPHQL_MAX_PAGES` serves as a circuit breaker. If exceeded:
- Log warning with actual total count
- Mark result as partial (`source_type` suffix or status table)
- Do NOT silently return truncated results

---

### B) Credits Pagination (Primary Use Case)

**New module:** `trr_backend/integrations/imdb/graphql_operations.py`

**Operation:** `TitleCreditPaginationV2`

**Persisted Query Hash:** `c2df29603060d12b6a76c48e2b47ac0ceee80e471f8cd8ee79abd672393e4bd8`

**Implementation:**
```python
def fetch_title_credits_paginated_v2(
    tconst: str,
    category_id: str,
    *,
    first: int = 250,
    max_pages: int | None = None,
    client: ImdbGraphQLPersistedClient | None = None,
) -> list[CreditNode]:
    """
    Fetch complete cast credits using GraphQL pagination.

    Args:
        tconst: IMDb title ID (e.g., "tt1720601")
        category_id: Job category (e.g., IMDB_JOB_CATEGORY_SELF)
        first: Page size (default: 250)
        max_pages: Max pages to fetch (None = all pages)
        client: Optional client instance (creates default if None)

    Returns:
        List of credit nodes with complete cast data

    Example:
        >>> from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF
        >>> credits = fetch_title_credits_paginated_v2("tt1720601", IMDB_JOB_CATEGORY_SELF)
        >>> len(credits)
        945
    """
```

**GraphQL Variables:**
```python
from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF

{
    "const": "tt1720601",  # Title ID (required by API)
    "tconst": "tt1720601",  # Title ID (duplicate required)
    "first": 250,           # Page size
    "after": None,          # Cursor token (None for first page)
    "locale": "en-US",      # Language
    "category": IMDB_JOB_CATEGORY_SELF  # Use constant, NOT string literal
}
```

**CRITICAL**: Always use `IMDB_JOB_CATEGORY_SELF` constant from `episodic_client.py`. Never hardcode category IDs as string literals.

**Response Structure:**
```json
{
  "data": {
    "title": {
      "credits": {
        "edges": [
          {
            "node": {
              "name": {
                "id": "nm0000148",
                "nameText": {"text": "Lisa Vanderpump"}
              },
              "attributes": null,
              "category": {
                "id": "amzn1.imdb.concept.name_credit_group.self",
                "text": "Self"
              },
              "characters": [{"name": "Self"}]
            }
          }
        ],
        "pageInfo": {
          "hasNextPage": true,
          "endCursor": "eyJlc1Rva2VuIjpbIjI1MCJdLCJmaWx0ZXIiOnt9fQ=="
        },
        "total": 945
      }
    }
  }
}
```

**Cast Selection Policy for `show_cast`**

**CRITICAL ISSUE**: GraphQL returns **all credits** for a title (~945 for tt1720601), which includes:
- Series regulars (main cast)
- Recurring guests
- One-off cameos
- Episode-specific appearances
- Potentially duplicates across seasons/specials

**Problem**: Dumping all 945 credits into `core.show_cast` pollutes "main cast" and breaks downstream assumptions.

**Solution**: Filter GraphQL results to select only "series cast" for `show_cast`:

**Primary Strategy** (if fields available):
- Use explicit "series regular" or "principal" flags from GraphQL response (field TBD)
- Use `attributes` field if it contains role type information

**Fallback Heuristic** (when explicit fields unavailable):
```python
# Filter by episode count threshold
def select_show_cast_from_graphql(
    credits: list[CreditNode],
    *,
    min_episodes: int = 3,  # Configurable: IMDB_SHOW_CAST_MIN_EPISODES
    max_members: int = 100,  # Configurable: IMDB_SHOW_CAST_MAX_MEMBERS
) -> list[CreditNode]:
    """
    Select series cast from GraphQL credits.

    Strategy:
    1. Filter credits where episodeCount >= min_episodes
    2. Sort by episodeCount descending
    3. Take top max_members

    Returns:
        Filtered list suitable for core.show_cast
    """
```

**Environment Variables:**
```bash
# Cast selection thresholds
IMDB_SHOW_CAST_MIN_EPISODES=3     # Min episodes to qualify as "series cast"
IMDB_SHOW_CAST_MAX_MEMBERS=100    # Max cast members to prevent runaway lists
```

**Partial Result Signaling:**
- If `total > max_members`, mark result as partial:
  - Log warning: "GraphQL returned X credits, capped to Y for show_cast"
  - Set `source_type = "credits_graphql_paginated_partial"`
  - OR: Add show-level `sync_status` table (preferred long-term)

**Guest/Episode-Level Credits:**
- **Phase 1**: Ignore credits below threshold (acceptable data loss)
- **Phase 2**: Route to `episode_appearances` table (requires episode mapping)

**Example Counts for tt1720601:**
| Total GraphQL Credits | After min_episodes=3 | After max_members=100 | Result |
|-----------------------|----------------------|-----------------------|--------|
| 945 | ~75-120 (estimated) | 100 | ✅ Manageable for show_cast |

---

### C) Integration with Fallback System

**Update:** `trr_backend/integrations/imdb/fullcredits_cast_parser.py`

**New function:**
```python
def normalize_graphql_credits_to_cast_rows(
    credits: list[CreditNode],
    *,
    job_category_id: str | None = None,
) -> list[CastRow]:
    """
    Map GraphQL credit nodes to CastRow format.

    Args:
        credits: List of GraphQL credit nodes from TitleCreditPaginationV2
        job_category_id: Optional job category override

    Returns:
        List of CastRow instances compatible with sync_show_cast pipeline

    Example:
        >>> credits = fetch_title_credits_paginated_v2("tt1720601", IMDB_JOB_CATEGORY_SELF)
        >>> rows = normalize_graphql_credits_to_cast_rows(credits)
        >>> len(rows)
        945
    """
```

**Updated fallback order in `fetch_fullcredits_cast_with_fallback()`:**
```python
def fetch_fullcredits_cast_with_fallback(
    series_id: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
    verbose: bool = False,
) -> tuple[list[CastRow], str]:
    """
    Fetch full credits cast with 3-tier fallback:
    1. HTML /fullcredits/ (fastest when available)
    2. GraphQL cached pagination (complete data)
    3. JSON API top-billed (last resort, deprecated)
    """

    # Tier 1: Try HTML first
    try:
        html = client.fetch_fullcredits_page(series_id, verbose=verbose)
        cast_rows = parse_fullcredits_cast_html(html, series_id=series_id)
        return cast_rows, "fullcredits_html"
    except ImdbFullCreditsError as exc:
        if not exc.is_blocked:
            raise

    # Tier 2: Try GraphQL pagination (NEW)
    try:
        credits = fetch_title_credits_paginated_v2(series_id, IMDB_JOB_CATEGORY_SELF)
        cast_rows = normalize_graphql_credits_to_cast_rows(credits)

        if verbose:
            print(f"✅ GraphQL fallback succeeded: {len(cast_rows)} complete credits")

        return cast_rows, "credits_graphql_paginated"
    except Exception as graphql_exc:
        if verbose:
            print(f"⚠️  GraphQL fallback failed: {graphql_exc}")

    # Tier 3: JSON API top-billed (last resort, deprecated)
    try:
        credits_response = fetch_title_credits(series_id)
        cast_rows = normalize_api_credits_to_cast_rows(credits_response)

        if verbose:
            print(
                f"⚠️  JSON API fallback (PARTIAL): {len(cast_rows)} top-billed credits only"
            )

        return cast_rows, "credits_api_top_billed"
    except Exception as api_exc:
        raise ImdbFullCreditsError(
            f"All fallback methods failed for {series_id}. "
            f"HTML: {exc}. GraphQL: {graphql_exc}. API: {api_exc}",
            status_code=exc.status_code,
            is_blocked=exc.is_blocked,
        ) from exc
```

---

### D) Source Type Tracking

**Database Schema Update:**

Update `core.show_cast.source_type` check constraint to include new GraphQL source:

```sql
-- supabase/migrations/0054_add_graphql_source_type.sql

ALTER TABLE core.show_cast
DROP CONSTRAINT IF EXISTS show_cast_source_type_check;

ALTER TABLE core.show_cast
ADD CONSTRAINT show_cast_source_type_check
CHECK (source_type IN (
    'fullcredits_html',
    'credits_graphql_paginated',           -- NEW: Complete GraphQL data
    'credits_graphql_paginated_partial',   -- NEW: GraphQL capped by MAX_PAGES or MAX_MEMBERS
    'credits_api_top_billed',              -- Renamed from credits_api_fallback
    'credits_api_fallback',                -- Deprecated (keep for existing data)
    'manual'
));

COMMENT ON COLUMN core.show_cast.source_type IS
'Data source:
- fullcredits_html: HTML scraping (complete when available)
- credits_graphql_paginated: GraphQL pagination (complete, filtered for main cast)
- credits_graphql_paginated_partial: GraphQL pagination hit limits (MAX_PAGES/MAX_MEMBERS)
- credits_api_top_billed: JSON API (partial - top-billed only)
- credits_api_fallback: Deprecated JSON API fallback (legacy)
- manual: Human entered';
```

**Partial Result Detection:**
```python
# Set source_type based on completion
if hit_max_pages or hit_max_members:
    source_type = "credits_graphql_paginated_partial"
    logger.warning(
        f"GraphQL pagination limited: total={total_count}, "
        f"returned={len(cast_rows)}, reason={'max_pages' if hit_max_pages else 'max_members'}"
    )
else:
    source_type = "credits_graphql_paginated"
```

---

### E) Standardize Lists on Unified Client

**Refactor:** `trr_backend/integrations/imdb/list_graphql_client.py`

**Goal:** Use the new `ImdbGraphQLPersistedClient` instead of direct `requests.post()`

**Before:**
```python
# Direct requests.post() with duplicate retry logic
resp = requests.post(
    "https://caching.graphql.imdb.com/",
    json=payload,
    headers=headers,
    timeout=timeout,
)
```

**After:**
```python
# Use unified client
from trr_backend.integrations.imdb.graphql_persisted_client import ImdbGraphQLPersistedClient

client = ImdbGraphQLPersistedClient()
result = client.execute_query(
    operation_name="TitleListQuery",
    sha256_hash="...",
    variables={"listId": list_id},
)
```

**Benefits:**
- Single retry/backoff implementation
- Consistent error handling
- Shared configuration
- Easier to mock for testing

---

### F) Migrate Episodes to GraphQL (Optional Phase 2)

**Target operations:**
- Episode list for series
- Season/episode pagination
- Episode-level credits

**Investigation needed:**
- Identify GraphQL persisted queries for episodes
- Extract sha256 hashes from IMDb network traffic
- Document response structures

**Deferred to Phase 2** (out of scope for initial implementation)

---

## Implementation Phases

### Phase 1: Core GraphQL Client + Credits (Target: Week 1)

**Tasks:**
1. ✅ Create `graphql_persisted_client.py` with retry/pagination support
2. ✅ Implement `fetch_title_credits_paginated_v2()` in `graphql_operations.py`
3. ✅ Add `normalize_graphql_credits_to_cast_rows()` normalization
4. ✅ Update `fetch_fullcredits_cast_with_fallback()` to use 3-tier fallback
5. ✅ Create migration 0054 for new source_type value
6. ✅ Add environment variables to `.env.example`
7. ✅ Update documentation in `docs/architecture/integrations.md`

**Validation:**
- ✅ `PYTHONPATH=. python scripts/sync_show_cast.py --imdb-id tt1720601 --verbose --dry-run`
  - Returns 900+ credits (not 18)
  - `source_type = "credits_graphql_paginated"`
  - Logs show "GraphQL fallback succeeded: 945 complete credits"

### Phase 2: Standardize Lists (Target: Week 2)

**Tasks:**
1. ✅ Refactor `list_graphql_client.py` to use `ImdbGraphQLPersistedClient`
2. ✅ Remove duplicate retry/backoff logic
3. ✅ Update tests to mock unified client

**Validation:**
- ✅ `PYTHONPATH=. python scripts/import_shows_from_lists.py --list-id ls123456789`
- ✅ All existing list functionality works
- ✅ No regressions in list imports

### Phase 3: Episodes Migration (Target: Future)

**Tasks:**
1. 🔄 Reverse-engineer episode GraphQL operations
2. 🔄 Implement episode pagination
3. 🔄 Update episode sync scripts

**Deferred:** Out of scope for initial release

---

## Testing Requirements

### Unit Tests (Mandatory)

**File:** `tests/integrations/imdb/test_graphql_persisted_client.py`

**Tests:**
1. ✅ `test_execute_query_success()` - Basic query execution
2. ✅ `test_execute_query_with_pagination()` - Multi-page pagination
3. ✅ `test_execute_query_retry_on_blocked()` - 202/403/429 retry
4. ✅ `test_execute_query_fallback_endpoint()` - Fallback to api.graphql.imdb.com
5. ✅ `test_execute_query_max_retries_exceeded()` - Exhausted retries

**File:** `tests/integrations/imdb/test_graphql_operations.py`

**Tests:**
1. ✅ `test_fetch_title_credits_paginated_v2()` - Credits pagination
2. ✅ `test_normalize_graphql_credits_to_cast_rows()` - Node → CastRow mapping
3. ✅ `test_normalize_filters_crew_categories()` - Crew filtering
4. ✅ `test_normalize_sets_job_category_for_self()` - Self role detection

**File:** `tests/integrations/imdb/test_fullcredits_cast_parser.py` (update)

**New tests:**
1. ✅ `test_fetch_with_fallback_uses_graphql_on_html_block()` - Tier 2 fallback
2. ✅ `test_fetch_with_fallback_uses_json_api_on_graphql_failure()` - Tier 3 fallback
3. ✅ `test_source_type_graphql_paginated()` - Correct source_type

### Integration Tests (Optional)

**Live API tests** (require network, run sparingly):
```python
@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("RUN_INTEGRATION_TESTS"), reason="Skipped")
def test_graphql_credits_live():
    """Test live GraphQL credits fetch for tt1720601."""
    credits = fetch_title_credits_paginated_v2("tt1720601", IMDB_JOB_CATEGORY_SELF)
    assert len(credits) > 900  # Should be ~945
```

---

## Migration Strategy

### Backward Compatibility

**Existing data:**
- Keep `credits_api_fallback` source_type for existing rows
- New rows use `credits_graphql_paginated` or `credits_api_top_billed`

**Script compatibility:**
- All 4 cast sync scripts automatically use new fallback order
- No script changes required (backward compatible)

**Rollback plan:**
- Disable GraphQL fallback via env: `IMDB_GRAPHQL_ENABLED=0`
- Falls back to JSON API (existing behavior)

### Data Quality Monitoring

**Analytics queries:**
```sql
-- Source type distribution
SELECT
    source_type,
    COUNT(*) as row_count,
    COUNT(DISTINCT show_id) as show_count
FROM core.show_cast
GROUP BY source_type
ORDER BY row_count DESC;

-- GraphQL vs HTML completeness
SELECT
    s.title,
    COUNT(*) as cast_count,
    MAX(sc.source_type) as source_type
FROM core.show_cast sc
JOIN core.shows s ON s.id = sc.show_id
GROUP BY s.id, s.title
HAVING MAX(sc.source_type) = 'credits_graphql_paginated'
ORDER BY cast_count DESC
LIMIT 20;
```

---

## Risks and Mitigations

### Risk 0: Operational and Terms of Service Risk

**CRITICAL**: This implementation uses IMDb's **internal GraphQL API**, not a public/documented API.

**Legal/Operational Concerns:**
- **No SLA or Support**: IMDb can change, deprecate, or block this API without notice
- **Terms of Service**: Scraping/automated access may violate IMDb ToS (review required)
- **Rate Limiting**: Aggressive usage can trigger IP blocks or legal action
- **Data Ownership**: IMDb owns the data; redistribution may violate copyright

**Mitigation:**
- **Conservative usage**: Respect rate limits, implement backoff
- **Monitoring**: Alert on sudden failure rate increases (indicates API change/blocking)
- **Failover chain**: Always maintain fallback to JSON API and manual entry
- **No auth/cookies**: Do not store or log authentication headers
- **Legal review**: Consult legal team before production deployment
- **Attribution**: Include IMDb attribution where data is displayed publicly

**Circuit Breakers:**
- If GraphQL fails >50% for 1 hour → auto-disable via `IMDB_GRAPHQL_ENABLED=0`
- If IP blocked → manual intervention required (rotate IPs or pause sync)

**Action Items Before Production:**
1. ✅ Legal team approval for internal API usage
2. ✅ Define acceptable use policy (rate limits, retry budgets)
3. ✅ Implement IP rotation strategy (if needed)
4. ✅ Set up monitoring for API health metrics

---

### Risk 1: Persisted Query Hashes May Change

**Impact:** GraphQL queries fail if IMDb updates hashes

**Mitigation:**
- Centralize hashes in `graphql_operations.py` with comments
- Add hash discovery script: `scripts/discover_imdb_graphql_hashes.py`
- Monitor for GraphQL errors in production logs
- Fallback to JSON API if GraphQL fails

### Risk 2: GraphQL Endpoint Rate Limiting

**Impact:** `caching.graphql.imdb.com` blocks requests

**Mitigation:**
- Implement exponential backoff (already planned)
- Respect `Retry-After` headers
- Fallback to `api.graphql.imdb.com` endpoint
- Ultimate fallback to JSON API

### Risk 3: Response Structure Changes

**Impact:** Normalization breaks if IMDb changes response format

**Mitigation:**
- Comprehensive unit tests with real response fixtures
- Schema validation before normalization
- Graceful degradation on parse errors
- Log schema mismatches for investigation

### Risk 4: Performance Impact

**Impact:** Pagination may be slower than single JSON API call

**Mitigation:**
- Cache GraphQL responses (future enhancement)
- Parallel page fetches (future enhancement)
- Monitor p95 latency in production
- Only fetch needed pages (early termination)

---

## Success Metrics

### Primary Metrics

1. **Completeness:** % of shows with >50 cast members (vs. <25 with JSON API)
2. **Fallback rate:** % of syncs using GraphQL fallback (vs. HTML success rate)
3. **Error rate:** % of syncs failing after all 3 tiers

### Target Goals (30 days post-deployment)

| Metric | Current (JSON API) | Target (GraphQL) |
|--------|-------------------|------------------|
| Shows with >50 cast | ~10% | >60% |
| Complete cast data | ~40% | >85% |
| Fallback success rate | ~70% (partial) | >95% (complete) |
| Total sync failures | ~15% | <5% |

---

## Open Questions

1. **Persisted query hash stability:**
   - How often does IMDb rotate hashes?
   - Is there a public API for hash discovery?

2. **Rate limit thresholds:**
   - What are the actual rate limits for `caching.graphql.imdb.com`?
   - Do we need IP rotation or request throttling?

3. **Episode GraphQL operations:**
   - Which operations support episode-level data?
   - Can we fetch episode credits via GraphQL?

4. **Caching strategy:**
   - Should we cache GraphQL responses in Redis?
   - What TTL is appropriate for cast data?

---

## Related Documents

- [IMDb Full Credits Resilience Spec](./imdb_fullcredits_resilience_spec.md) (PR #17)
- [IMDb Full Credits Resilience Implementation Plan](./imdb_fullcredits_resilience_implementation_plan.md) (PR #17)
- [Architecture: IMDb Integrations](./integrations.md)
- [Database Schema: show_cast](../../supabase/schema_docs/core.show_cast.md)

---

## Appendix A: GraphQL Query Examples

### TitleCreditPaginationV2 (First Page)

**Request:**
```json
{
  "operationName": "TitleCreditPaginationV2",
  "variables": {
    "const": "tt1720601",
    "tconst": "tt1720601",
    "first": 250,
    "after": null,
    "locale": "en-US",
    "category": "amzn1.imdb.concept.name_credit_group.self"
  },
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "c2df29603060d12b6a76c48e2b47ac0ceee80e471f8cd8ee79abd672393e4bd8"
    }
  }
}
```

**Response (truncated):**
```json
{
  "data": {
    "title": {
      "id": "tt1720601",
      "credits": {
        "total": 945,
        "pageInfo": {
          "hasNextPage": true,
          "hasPreviousPage": false,
          "endCursor": "eyJlc1Rva2VuIjpbIjI1MCJdLCJmaWx0ZXIiOnt9fQ=="
        },
        "edges": [
          {
            "node": {
              "name": {
                "id": "nm0000148",
                "nameText": {"text": "Lisa Vanderpump"}
              },
              "attributes": null,
              "category": {
                "id": "amzn1.imdb.concept.name_credit_group.self",
                "text": "Self"
              },
              "characters": [{"name": "Self"}]
            }
          }
          // ... 249 more edges
        ]
      }
    }
  }
}
```

### TitleCreditPaginationV2 (Second Page)

**Request:**
```json
{
  "operationName": "TitleCreditPaginationV2",
  "variables": {
    "const": "tt1720601",
    "tconst": "tt1720601",
    "first": 250,
    "after": "eyJlc1Rva2VuIjpbIjI1MCJdLCJmaWx0ZXIiOnt9fQ==",  // Cursor from page 1
    "locale": "en-US",
    "category": "amzn1.imdb.concept.name_credit_group.self"
  },
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "c2df29603060d12b6a76c48e2b47ac0ceee80e471f8cd8ee79abd672393e4bd8"
    }
  }
}
```

---

## Appendix B: Environment Variable Reference

```bash
# IMDb GraphQL Configuration (add to .env.example)

# GraphQL API endpoints
IMDB_GRAPHQL_BASE_URL=https://caching.graphql.imdb.com/
IMDB_GRAPHQL_FALLBACK_URL=https://api.graphql.imdb.com/

# Retry configuration
IMDB_GRAPHQL_MAX_RETRIES=2
IMDB_GRAPHQL_RETRY_BASE_DELAY_SEC=2.0
IMDB_GRAPHQL_TIMEOUT_SEC=30.0

# Request configuration
IMDB_GRAPHQL_LOCALE=en-US
IMDB_GRAPHQL_USER_AGENT=Mozilla/5.0

# Pagination defaults
IMDB_GRAPHQL_PAGE_SIZE=250
IMDB_GRAPHQL_MAX_PAGES=10

# Feature flags
IMDB_GRAPHQL_ENABLED=1  # Set to 0 to disable GraphQL fallback
IMDB_GRAPHQL_USE_CACHE_ENDPOINT=1  # Set to 0 to skip caching.graphql.imdb.com
```

---

## Appendix C: Error Code Mapping

| HTTP Status | Meaning | Retry? | Fallback? |
|-------------|---------|--------|-----------|
| 200 | Success | N/A | N/A |
| 202 | Accepted (queued) | ✅ Yes (3x) | ✅ Next tier |
| 400 | Bad Request | ❌ No | ✅ Next tier |
| 403 | Forbidden (rate limit) | ✅ Yes (3x) | ✅ Next tier |
| 429 | Too Many Requests | ✅ Yes (3x) | ✅ Next tier |
| 500 | Internal Server Error | ✅ Yes (2x) | ✅ Next tier |
| 502/503 | Bad Gateway/Unavailable | ✅ Yes (2x) | ✅ Next tier |
| 504 | Gateway Timeout | ✅ Yes (2x) | ✅ Next tier |

---

**End of Specification**
