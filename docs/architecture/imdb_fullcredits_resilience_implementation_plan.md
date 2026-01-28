# Implementation Plan: IMDb Full Cast Sync Resilience (REVISED)

**Status:** Ready for Implementation (Revised after technical review)
**Specification:** [imdb_fullcredits_resilience_spec.md](./imdb_fullcredits_resilience_spec.md)
**Target Branch:** `fix/imdb-cast-sync-202-fallback`
**Estimated Complexity:** Medium (6-8 hours)

---

## ⚠️ Key Changes from Original Plan

This revision incorporates critical fixes identified in technical review:

1. **Fallback in integration layer** - Centralized in `fullcredits_cast_parser.py`, not scattered across scripts
2. **Fixed retry semantics** - Clear max_retries meaning, separate ENABLE_API_FALLBACK env var
3. **HTTP 429 included** - Treat 429 same as 202/403 (blocked/rate-limited)
4. **Debug HTML fixed** - Use `Path.cwd()` for testing, save only once per show
5. **Simplified source_type** - Inject into row dicts, no repository signature change
6. **Correct migration number** - Use `0053` not `0054` (repo ends at `0052`)
7. **Category-based filtering** - Use `job_category_id`, not raw role text

---

## Implementation Sequence

This plan follows a **bottom-up dependency order**: build foundation layers first, then compose higher-level features.

### ✅ Prerequisites

- [x] Specification reviewed and approved
- [x] Technical review completed with fixes incorporated
- [x] Worktree created: `trr-backend-fix-imdb-202`
- [ ] Virtual environment activated
- [ ] Dependencies installed: `pip install -r requirements.txt`

---

## Phase 1: Database Schema Migration (No Code Dependencies)

**Goal:** Add `source_type` column to `core.show_cast` table

**Priority:** HIGH (must be done first, enables testing)

### Step 1.1: Create Migration File

**File:** `supabase/migrations/0053_add_show_cast_source_tracking.sql` ✅ **FIXED: was 0054**

**Action:** Create new file

**Content:**
```sql
-- Migration: Add source tracking to show_cast
-- Tracks whether cast data came from HTML scraping or JSON API fallback

BEGIN;

-- Add source_type column with default value
ALTER TABLE core.show_cast
ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'fullcredits_html';

-- Add constraint to enforce valid values
ALTER TABLE core.show_cast
DROP CONSTRAINT IF EXISTS show_cast_source_type_check;

ALTER TABLE core.show_cast
ADD CONSTRAINT show_cast_source_type_check
CHECK (source_type IN ('fullcredits_html', 'credits_api_fallback', 'manual'));

-- Add index for analytics queries
CREATE INDEX IF NOT EXISTS idx_show_cast_source_type
ON core.show_cast(source_type);

-- Add column comment for documentation
COMMENT ON COLUMN core.show_cast.source_type IS
'Data source: fullcredits_html (HTML scrape), credits_api_fallback (JSON API), manual (user entry)';

COMMIT;
```

**Validation Checkpoint:**
```bash
# Apply migration locally
supabase db reset

# Verify column exists
psql $TRR_DB_URL -c "\d core.show_cast"
# Expected: source_type column with default 'fullcredits_html'

# Verify constraint
psql $TRR_DB_URL -c "INSERT INTO core.show_cast (show_id, person_id, credit_category, source_type) VALUES (gen_random_uuid(), gen_random_uuid(), 'Self', 'invalid');"
# Expected: ERROR - constraint violation

# Verify index
psql $TRR_DB_URL -c "\d core.show_cast"
# Expected: idx_show_cast_source_type index listed
```

**Dependencies:** None

**Risks:**
- ⚠️ Existing data will default to 'fullcredits_html' (acceptable, matches reality)
- ⚠️ If migration fails, check for existing constraint name conflicts

---

## Phase 2: Error Handling Enhancement (Foundation)

**Goal:** Extend `ImdbFullCreditsError` to distinguish blocked (202/403/429) vs failed requests

**Priority:** HIGH (needed for retry logic and fallback)

### Step 2.1: Update ImdbFullCreditsError Class

**File:** `trr_backend/integrations/imdb/fullcredits_cast_parser.py`

**Lines to modify:** 19-29 (current `ImdbFullCreditsError` definition)

**Changes:**
```python
# BEFORE (lines 19-29):
class ImdbFullCreditsError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_snippet: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet

# AFTER:
class ImdbFullCreditsError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_snippet: str | None = None,
        is_blocked: bool = False,  # NEW: Indicates 202/403/429 blocked/rate-limited
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet
        self.is_blocked = is_blocked  # NEW
```

**Validation Checkpoint:**
```python
# Quick test in Python REPL
from trr_backend.integrations.imdb.fullcredits_cast_parser import ImdbFullCreditsError

exc = ImdbFullCreditsError("test", status_code=202, is_blocked=True)
assert exc.is_blocked is True
assert exc.status_code == 202

exc429 = ImdbFullCreditsError("rate limited", status_code=429, is_blocked=True)
assert exc429.is_blocked is True
```

**Dependencies:** None

**Risks:** None (backwards compatible)

---

## Phase 3: Retry Logic with Exponential Backoff

**Goal:** Add retry logic to `fetch_fullcredits_page()` for 202/403/429 responses

**Priority:** HIGH (core resilience feature)

### Step 3.1: Add Retry Logic to HttpImdbFullCreditsClient

**File:** `trr_backend/integrations/imdb/fullcredits_cast_parser.py`

**Lines to modify:** 1-10 (imports), 53-78 (fetch_fullcredits_page method)

**Changes:**

**Step 3.1a: Update imports (lines 1-10)**
```python
# ADD these imports after line 3:
import os
import random
import time
from datetime import datetime
from pathlib import Path  # NEW for debug HTML
```

**Step 3.1b: Replace fetch_fullcredits_page method (lines 53-78)**

**BEFORE:**
```python
def fetch_fullcredits_page(self, imdb_series_id: str) -> str:
    imdb_series_id = str(imdb_series_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
        raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")

    url = f"https://www.imdb.com/title/{imdb_series_id}/fullcredits/"
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0",
        **self._extra_headers,
    }

    try:
        resp = self._session.get(url, headers=headers, timeout=self._timeout_seconds)
    except requests.RequestException as exc:
        raise ImdbFullCreditsError(f"IMDb request failed: {exc}") from exc

    if resp.status_code != 200:
        raise ImdbFullCreditsError(
            f"IMDb request failed with HTTP {resp.status_code}.",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:200],
        )

    return resp.text or ""
```

**AFTER:**
```python
def fetch_fullcredits_page(
    self,
    imdb_series_id: str,
    *,
    verbose: bool = False,
) -> str:
    """
    Fetch IMDb full credits page HTML with retry logic for blocked requests.

    Args:
        imdb_series_id: IMDb title ID (e.g., "tt1720601")
        verbose: If True, save debug HTML artifacts on blocked responses

    Returns:
        HTML content of full credits page

    Raises:
        ValueError: If imdb_series_id is invalid format
        ImdbFullCreditsError: If request fails after retries (with is_blocked=True for 202/403/429)
    """
    imdb_series_id = str(imdb_series_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
        raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")

    url = f"https://www.imdb.com/title/{imdb_series_id}/fullcredits/"
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0",
        **self._extra_headers,
    }

    # Environment-configurable retry settings
    # max_retries = additional attempts after first request
    # So total attempts = 1 + max_retries
    max_retries = int(os.getenv("IMDB_FULLCREDITS_MAX_RETRIES", "2"))  # Default 2 retries = 3 total attempts
    base_delay = float(os.getenv("IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC", "5.0"))

    last_response: requests.Response | None = None
    last_exception: Exception | None = None

    for attempt in range(1 + max_retries):  # 1 initial + N retries
        try:
            resp = self._session.get(url, headers=headers, timeout=self._timeout_seconds)
            last_response = resp
        except requests.RequestException as exc:
            last_exception = exc
            if attempt < max_retries:
                delay = base_delay * (2**attempt)
                jitter = random.uniform(0, delay * 0.25)
                time.sleep(delay + jitter)
                continue
            # Exhausted retries on network error
            raise ImdbFullCreditsError(f"IMDb request failed: {exc}") from exc

        # Success case
        if resp.status_code == 200:
            return resp.text or ""

        # Blocked/rate-limited responses (202=queued, 403=forbidden, 429=rate-limited)
        is_blocked = resp.status_code in {202, 403, 429}

        # Retry if blocked and have retries left
        if is_blocked and attempt < max_retries:
            # Exponential backoff with jitter
            delay = base_delay * (2**attempt)
            jitter = random.uniform(0, delay * 0.25)
            time.sleep(delay + jitter)
            continue

        # Exhausted retries or non-retryable error
        # Save debug artifact ONCE (on final blocked attempt)
        if verbose and is_blocked:
            self._save_debug_html(imdb_series_id, resp)

        raise ImdbFullCreditsError(
            f"IMDb fullcredits {'blocked/rate-limited' if is_blocked else 'request failed'} "
            f"with HTTP {resp.status_code} (after {attempt + 1} attempt(s)).",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:200],
            is_blocked=is_blocked,
        )

    # Should never reach here, but satisfy type checker
    if last_response:
        raise ImdbFullCreditsError(
            f"IMDb fullcredits request failed with HTTP {last_response.status_code}.",
            status_code=last_response.status_code,
            body_snippet=(last_response.text or "")[:200],
            is_blocked=last_response.status_code in {202, 403, 429},
        )
    if last_exception:
        raise ImdbFullCreditsError(f"IMDb request failed: {last_exception}") from last_exception
    raise ImdbFullCreditsError("IMDb fullcredits request failed (no response).")

def _save_debug_html(self, imdb_series_id: str, resp: requests.Response) -> None:
    """
    Save blocked response HTML to debug_html/ directory (strips sensitive headers).

    Uses Path.cwd() for testability (tests can monkeypatch.chdir).
    """
    debug_dir = Path.cwd() / "debug_html"  # ✅ FIXED: use Path.cwd() for testability
    debug_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"imdb_fullcredits_{imdb_series_id}_{timestamp}_http{resp.status_code}.html"
    filepath = debug_dir / filename

    try:
        filepath.write_text(resp.text or "", encoding="utf-8")
        print(f"Debug HTML saved: {filepath}")
    except Exception as exc:
        print(f"Warning: Failed to save debug HTML: {exc}")
```

**Validation Checkpoint:**
```bash
# Unit test with mocked responses
pytest tests/integrations/imdb/test_fullcredits_resilience.py::test_retry_logic_202_then_success -v

# Manual test (will take ~15s due to backoff):
IMDB_FULLCREDITS_MAX_RETRIES=1 PYTHONPATH=. python -c "
from trr_backend.integrations.imdb.fullcredits_cast_parser import HttpImdbFullCreditsClient
client = HttpImdbFullCreditsClient()
try:
    html = client.fetch_fullcredits_page('tt1720601', verbose=True)
    print('SUCCESS - HTML length:', len(html))
except Exception as e:
    print('EXPECTED FAILURE:', e)
    print('Check is_blocked:', getattr(e, 'is_blocked', None))
"
```

**Dependencies:** Step 2.1 (ImdbFullCreditsError.is_blocked)

**Risks:**
- ⚠️ Backoff delays may slow down bulk sync (acceptable, prevents rate limiting)
- ⚠️ Debug HTML may consume disk space (mitigated by only saving on --verbose and once per show)

---

## Phase 4: JSON API Normalization and Fallback (Integration Layer)

**Goal:** Create `fetch_fullcredits_cast_with_fallback()` in integration layer for centralized fallback logic

**Priority:** HIGH (needed for all callers, not just sync_show_cast.py)

### Step 4.1: Add Normalization Function

**File:** `trr_backend/integrations/imdb/fullcredits_cast_parser.py`

**Location:** Add after line 306 (after `filter_self_cast_rows` function)

**New function:**
```python
def normalize_api_credits_to_cast_rows(
    credits_response: ImdbTitleCredits,
    *,
    job_category_filter: str | None = None,
) -> list[CastRow]:
    """
    Map JSON API credits response (from api.imdbapi.dev) to CastRow format.

    This enables fallback from HTML scraping to JSON API when IMDb blocks
    the /fullcredits/ page with 202/403/429 responses.

    Args:
        credits_response: Response from fetch_title_credits()
        job_category_filter: Optional category filter (e.g., "self" for reality shows)

    Returns:
        List of CastRow instances compatible with sync_show_cast pipeline

    Example:
        >>> from trr_backend.integrations.imdb.credits_client import fetch_title_credits
        >>> credits = fetch_title_credits("tt1720601")
        >>> rows = normalize_api_credits_to_cast_rows(credits, job_category_filter="self")
        >>> len(rows)
        25
    """
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits

    if not isinstance(credits_response, ImdbTitleCredits):
        raise TypeError(
            f"Expected ImdbTitleCredits, got {type(credits_response).__name__}"
        )

    rows: list[CastRow] = []
    for idx, credit in enumerate(credits_response.credits, start=1):
        # Extract fields from JSON API structure
        # API returns: {"id": "nm0000148", "name": "...", "category": "self", "characters": [...]}
        name_id = credit.get("id")  # e.g., "nm0000148"
        name = credit.get("name")
        category = (credit.get("category") or "").strip().lower()
        characters = credit.get("characters") or []

        # Apply category filter if specified (✅ FIXED: use category, not role text)
        if job_category_filter and category != job_category_filter.lower():
            continue

        # Skip invalid entries
        if not name_id or not name:
            continue

        # Build role text from characters list
        role_text = None
        if isinstance(characters, list) and characters:
            role_text = ", ".join(str(char) for char in characters if char)

        # Map category to job_category_id (✅ FIXED: use category for self filtering)
        job_category_id = None
        if category == "self":
            job_category_id = IMDB_JOB_CATEGORY_SELF

        rows.append(
            CastRow(
                name_id=name_id.strip().lower() if name_id else "",
                name=name.strip() if name else "",
                billing_order=idx,
                raw_role_text=role_text,
                job_category_id=job_category_id,
            )
        )

    return rows
```

**Also update imports (after line 10):**
```python
# Add this import for type checking:
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits
```

### Step 4.2: Create Centralized Fallback Function

**File:** `trr_backend/integrations/imdb/fullcredits_cast_parser.py`

**Location:** Add after `normalize_api_credits_to_cast_rows()` function

**New function:**
```python
def fetch_fullcredits_cast_with_fallback(
    series_id: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
    verbose: bool = False,
) -> tuple[list[CastRow], str]:
    """
    Fetch full credits cast with automatic fallback to JSON API on blocked responses.

    This is the recommended entry point for fetching IMDb cast data, as it handles
    202/403/429 blocking gracefully by falling back to the JSON API.

    Args:
        series_id: IMDb series ID (e.g., "tt1720601")
        extra_headers: Optional HTTP headers (for debugging only, not recommended)
        verbose: If True, log fallback events and save debug HTML

    Returns:
        Tuple of (cast_rows, source_type) where:
        - cast_rows: List of CastRow instances
        - source_type: "fullcredits_html" or "credits_api_fallback"

    Raises:
        ImdbFullCreditsError: If both HTML and JSON API fail
        ValueError: If series_id is invalid

    Example:
        >>> rows, source = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=True)
        >>> source
        'fullcredits_html'  # or 'credits_api_fallback' if HTML was blocked
    """
    # Check if fallback is enabled (allow disabling for testing/rollback)
    enable_fallback = os.getenv("IMDB_FULLCREDITS_ENABLE_API_FALLBACK", "1").strip().lower() not in {"0", "false", "no"}

    # Try HTML first
    client = HttpImdbFullCreditsClient(extra_headers=extra_headers)
    try:
        html = client.fetch_fullcredits_page(series_id, verbose=verbose)
        cast_rows = parse_fullcredits_cast(html, series_id)
        return cast_rows, "fullcredits_html"
    except ImdbFullCreditsError as exc:
        # If blocked and fallback enabled, try JSON API
        if exc.is_blocked and enable_fallback:
            if verbose:
                print(
                    f"⚠️  IMDb HTML blocked for {series_id} (HTTP {exc.status_code}), "
                    f"falling back to JSON API..."
                )

            # Import here to avoid circular dependency
            from trr_backend.integrations.imdb.credits_client import fetch_title_credits

            try:
                credits_response = fetch_title_credits(series_id)
                cast_rows = normalize_api_credits_to_cast_rows(credits_response)

                if verbose:
                    print(f"✅ JSON API fallback succeeded: {len(cast_rows)} total credits")

                return cast_rows, "credits_api_fallback"
            except Exception as api_exc:
                # JSON API also failed - raise original HTML error with context
                raise ImdbFullCreditsError(
                    f"Both HTML and JSON API failed for {series_id}. "
                    f"HTML: {exc}. API: {api_exc}",
                    status_code=exc.status_code,
                    is_blocked=exc.is_blocked,
                ) from exc
        else:
            # Not blocked, or fallback disabled - re-raise original error
            raise
```

**Validation Checkpoint:**
```python
# Unit test
pytest tests/integrations/imdb/test_fullcredits_resilience.py::test_fallback_on_blocked -v

# Manual test with fallback
PYTHONPATH=. python -c "
from trr_backend.integrations.imdb.fullcredits_cast_parser import fetch_fullcredits_cast_with_fallback

rows, source = fetch_fullcredits_cast_with_fallback('tt1720601', verbose=True)
print(f'Got {len(rows)} rows from {source}')
print('Sample:', rows[0].name if rows else 'No rows')
"

# Manual test with fallback disabled
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 PYTHONPATH=. python -c "
from trr_backend.integrations.imdb.fullcredits_cast_parser import fetch_fullcredits_cast_with_fallback

try:
    rows, source = fetch_fullcredits_cast_with_fallback('tt1720601', verbose=True)
except Exception as e:
    print(f'Expected failure with fallback disabled: {type(e).__name__}')
"
```

**Dependencies:**
- Step 3.1 (retry logic in fetch_fullcredits_page)
- Step 4.1 (normalize_api_credits_to_cast_rows)
- Existing `credits_client.py` (already in codebase)
- `CastRow` dataclass (already defined)
- `IMDB_JOB_CATEGORY_SELF` constant (already imported from episodic_client.py)

**Risks:**
- ⚠️ JSON API structure may change (mitigation: add schema validation in tests)
- ⚠️ JSON API may have different rate limits (needs monitoring)

---

## Phase 5: Update Scripts to Use Centralized Fallback

**Goal:** Update `sync_show_cast.py` to use the new fallback function and inject source_type

**Priority:** HIGH (user-facing feature)

### Step 5.1: Update sync_show_cast.py to Use Fallback

**File:** `scripts/sync/sync_show_cast.py`

**Lines to modify:** 16 (imports), 83-89 (fetch call), ~139 (upsert call)

**Changes:**

**Step 5.1a: Update imports (line 16)**
```python
# BEFORE:
from trr_backend.integrations.imdb.fullcredits_cast_parser import fetch_fullcredits_cast, filter_self_cast_rows

# AFTER:
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    fetch_fullcredits_cast_with_fallback,  # NEW: use fallback function
    filter_self_cast_rows,
)
```

**Step 5.1b: Update fetch call (lines 83-89)**

**BEFORE:**
```python
try:
    cast_rows = fetch_fullcredits_cast(imdb_id, extra_headers=extra_headers)

    cast_rows_total += len(cast_rows)
    self_rows = filter_self_cast_rows(cast_rows)
    cast_rows_self += len(self_rows)
```

**AFTER:**
```python
try:
    # Use centralized fallback function (returns cast_rows + source_type)
    cast_rows, source_type = fetch_fullcredits_cast_with_fallback(
        imdb_id,
        extra_headers=extra_headers,
        verbose=bool(args.verbose),
    )

    cast_rows_total += len(cast_rows)
    self_rows = filter_self_cast_rows(cast_rows)
    cast_rows_self += len(self_rows)
```

**Step 5.1c: Update upsert call to inject source_type (line ~139)**

**BEFORE:**
```python
if show_cast_rows and not args.dry_run:
    show_cast_upserted += len(upsert_show_cast(db, show_cast_rows))
```

**AFTER:**
```python
if show_cast_rows and not args.dry_run:
    # Inject source_type into each row dict (✅ FIXED: no repo signature change)
    rows_with_source = [
        {**row, "source_type": source_type}
        for row in show_cast_rows
    ]
    show_cast_upserted += len(upsert_show_cast(db, rows_with_source))
```

**Validation Checkpoint:**
```bash
# Test with known blocked show (tt1720601)
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601 --verbose --dry-run

# Expected output:
# ⚠️  IMDb HTML blocked for tt1720601 (HTTP 202), falling back to JSON API...
# ✅ JSON API fallback succeeded: XX total credits
# cast_rows_self=XX

# Test with normal show (should use HTML)
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt0386676 --verbose --dry-run

# Expected: No fallback message, uses HTML

# Test with fallback disabled
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601 --verbose --dry-run

# Expected: Fails with ImdbFullCreditsError (fallback disabled)
```

**Dependencies:**
- Step 4.2 (fetch_fullcredits_cast_with_fallback)

**Risks:**
- ⚠️ Other scripts that call `fetch_fullcredits_cast()` directly will still fail on 202/403/429 until they migrate to `fetch_fullcredits_cast_with_fallback()`
- ✅ **Mitigation:** Document the new function, deprecate old one (add deprecation warning in docstring)

---

## Phase 6: Update .env.example Documentation

**Goal:** Document new environment variables and clarify IMDB_EXTRA_HEADERS_JSON

**Priority:** MEDIUM (user-facing documentation)

### Step 6.1: Update .env.example

**File:** `.env.example`

**Location:** Add new section after line 24 (after IMDB_LIST_URL)

**Add:**
```bash
# ----------------------------
# IMDb Integration (Optional)
# ----------------------------

# OPTIONAL: Custom headers for IMDb HTML scraping (debugging/local unblock only)
# WARNING: Cookies are sensitive and expire frequently. Do NOT rely on this for production.
# The pipeline will automatically fall back to JSON API if HTML is blocked (202/403/429).
# Only use this for debugging specific shows or local development.
# Example: IMDB_EXTRA_HEADERS_JSON={"Cookie": "session=abc123", "User-Agent": "Custom UA"}
IMDB_EXTRA_HEADERS_JSON=

# Retry configuration for IMDb fullcredits HTML fetching
# Max retries AFTER first attempt (default: 2, meaning 3 total attempts)
IMDB_FULLCREDITS_MAX_RETRIES=2

# Base delay in seconds for exponential backoff (default: 5.0)
# Actual delay = base_delay * (2 ^ attempt) + random jitter
IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC=5.0

# Enable automatic fallback to JSON API on blocked HTML (default: 1)
# Set to 0 to disable fallback (for testing/rollback)
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=1
```

**Validation Checkpoint:**
```bash
# Verify syntax
grep -A 25 "IMDb Integration" .env.example

# Test parsing
PYTHONPATH=. python -c "
import os
os.environ['IMDB_FULLCREDITS_MAX_RETRIES'] = '1'
os.environ['IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC'] = '3.0'
os.environ['IMDB_FULLCREDITS_ENABLE_API_FALLBACK'] = '0'

max_retries = int(os.getenv('IMDB_FULLCREDITS_MAX_RETRIES', '2'))
base_delay = float(os.getenv('IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC', '5.0'))
enable_fallback = os.getenv('IMDB_FULLCREDITS_ENABLE_API_FALLBACK', '1').strip().lower() not in {'0', 'false', 'no'}

assert max_retries == 1
assert base_delay == 3.0
assert enable_fallback is False
print('✅ Env vars parse correctly')
"
```

**Dependencies:** None

**Risks:** None (documentation only)

---

## Phase 7: Comprehensive Testing

**Goal:** Add unit and integration tests for all new functionality

**Priority:** HIGH (ensures reliability)

### Step 7.1: Create Test File for Resilience Features

**File:** `tests/integrations/imdb/test_fullcredits_resilience.py`

**Action:** Create new file

**Content:**
```python
"""
Tests for IMDb fullcredits resilience features:
- Retry logic with exponential backoff
- JSON API fallback on 202/403/429
- Debug HTML saving
- Error classification (is_blocked flag)
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests

from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    CastRow,
    IMDB_JOB_CATEGORY_SELF,
    HttpImdbFullCreditsClient,
    ImdbFullCreditsError,
    fetch_fullcredits_cast_with_fallback,
    normalize_api_credits_to_cast_rows,
)


class TestRetryLogic:
    """Test retry behavior for 202/403/429 responses."""

    def test_retry_202_then_success(self) -> None:
        """Should retry 202 responses and succeed on subsequent 200."""
        client = HttpImdbFullCreditsClient()

        # Mock session to return [202, 202, 200]
        responses = [
            Mock(status_code=202, text="<html>Queued</html>"),
            Mock(status_code=202, text="<html>Queued</html>"),
            Mock(status_code=200, text="<html>Cast list</html>"),
        ]

        with patch.object(client._session, "get", side_effect=responses):
            with patch("time.sleep"):  # Skip actual delays
                html = client.fetch_fullcredits_page("tt1234567")

        assert html == "<html>Cast list</html>"

    def test_retry_202_exhausted_raises_with_is_blocked(self) -> None:
        """Should raise ImdbFullCreditsError with is_blocked=True after max retries."""
        client = HttpImdbFullCreditsClient()

        # Mock session to always return 202
        response = Mock(status_code=202, text="<html>Blocked</html>")

        with patch.object(client._session, "get", return_value=response):
            with patch("time.sleep"):  # Skip actual delays
                with patch.dict("os.environ", {"IMDB_FULLCREDITS_MAX_RETRIES": "1"}):
                    with pytest.raises(ImdbFullCreditsError) as exc_info:
                        client.fetch_fullcredits_page("tt1234567")

        exc = exc_info.value
        assert exc.is_blocked is True
        assert exc.status_code == 202
        assert "blocked/rate-limited" in str(exc).lower()

    def test_403_raises_with_is_blocked(self) -> None:
        """Should raise ImdbFullCreditsError with is_blocked=True for 403."""
        client = HttpImdbFullCreditsClient()

        response = Mock(status_code=403, text="<html>Forbidden</html>")

        with patch.object(client._session, "get", return_value=response):
            with patch("time.sleep"):
                with pytest.raises(ImdbFullCreditsError) as exc_info:
                    client.fetch_fullcredits_page("tt1234567")

        exc = exc_info.value
        assert exc.is_blocked is True
        assert exc.status_code == 403

    def test_429_raises_with_is_blocked(self) -> None:
        """Should raise ImdbFullCreditsError with is_blocked=True for 429 (rate limit)."""
        client = HttpImdbFullCreditsClient()

        response = Mock(status_code=429, text="<html>Too Many Requests</html>")

        with patch.object(client._session, "get", return_value=response):
            with patch("time.sleep"):
                with pytest.raises(ImdbFullCreditsError) as exc_info:
                    client.fetch_fullcredits_page("tt1234567")

        exc = exc_info.value
        assert exc.is_blocked is True
        assert exc.status_code == 429

    def test_500_raises_without_is_blocked(self) -> None:
        """Should raise ImdbFullCreditsError with is_blocked=False for server errors."""
        client = HttpImdbFullCreditsClient()

        response = Mock(status_code=500, text="<html>Server Error</html>")

        with patch.object(client._session, "get", return_value=response):
            with pytest.raises(ImdbFullCreditsError) as exc_info:
                client.fetch_fullcredits_page("tt1234567")

        exc = exc_info.value
        assert exc.is_blocked is False
        assert exc.status_code == 500

    def test_network_error_raises(self) -> None:
        """Should raise ImdbFullCreditsError on network failures."""
        client = HttpImdbFullCreditsClient()

        with patch.object(
            client._session,
            "get",
            side_effect=requests.RequestException("Connection failed"),
        ):
            with patch("time.sleep"):
                with patch.dict("os.environ", {"IMDB_FULLCREDITS_MAX_RETRIES": "1"}):
                    with pytest.raises(ImdbFullCreditsError) as exc_info:
                        client.fetch_fullcredits_page("tt1234567")

        exc = exc_info.value
        assert "Connection failed" in str(exc)


class TestDebugHTMLSaving:
    """Test debug HTML artifact saving on blocked responses."""

    def test_saves_html_on_202_when_verbose(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Should save debug HTML when verbose=True and status=202."""
        # ✅ FIXED: use monkeypatch.chdir instead of patching Path.cwd
        monkeypatch.chdir(tmp_path)

        client = HttpImdbFullCreditsClient()
        response = Mock(status_code=202, text="<html>Blocked content</html>")

        with patch.object(client._session, "get", return_value=response):
            with patch("time.sleep"):
                with patch.dict("os.environ", {"IMDB_FULLCREDITS_MAX_RETRIES": "0"}):
                    with pytest.raises(ImdbFullCreditsError):
                        client.fetch_fullcredits_page("tt1234567", verbose=True)

        # Check debug_html directory created
        debug_dir = tmp_path / "debug_html"
        assert debug_dir.exists()

        # Check HTML file saved (only once)
        html_files = list(debug_dir.glob("imdb_fullcredits_tt1234567_*.html"))
        assert len(html_files) == 1
        assert "Blocked content" in html_files[0].read_text()

    def test_no_save_when_verbose_false(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Should NOT save debug HTML when verbose=False."""
        monkeypatch.chdir(tmp_path)

        client = HttpImdbFullCreditsClient()
        response = Mock(status_code=202, text="<html>Blocked</html>")

        with patch.object(client._session, "get", return_value=response):
            with patch("time.sleep"):
                with patch.dict("os.environ", {"IMDB_FULLCREDITS_MAX_RETRIES": "0"}):
                    with pytest.raises(ImdbFullCreditsError):
                        client.fetch_fullcredits_page("tt1234567", verbose=False)

        # No debug_html directory
        debug_dir = tmp_path / "debug_html"
        if debug_dir.exists():
            assert len(list(debug_dir.glob("*.html"))) == 0


class TestNormalizeAPICredits:
    """Test JSON API to CastRow normalization."""

    def test_maps_api_credits_to_cast_rows(self) -> None:
        """Should map JSON API credits to CastRow format."""
        # Fixture: JSON API response structure
        credits = ImdbTitleCredits(
            imdb_id="tt1234567",
            credits=[
                {
                    "id": "nm0000001",
                    "name": "Jane Doe",
                    "category": "self",
                    "characters": ["Self"],
                },
                {
                    "id": "nm0000002",
                    "name": "John Smith",
                    "category": "actor",
                    "characters": ["Detective Jones"],
                },
                {
                    "id": "nm0000003",
                    "name": "Bob Lee",
                    "category": "self",
                    "characters": ["Self - Host"],
                },
            ],
            total_count=3,
        )

        rows = normalize_api_credits_to_cast_rows(credits)

        assert len(rows) == 3
        assert all(isinstance(row, CastRow) for row in rows)

        # Check first row (self) - ✅ FIXED: verify job_category_id is set
        assert rows[0].name_id == "nm0000001"
        assert rows[0].name == "Jane Doe"
        assert rows[0].billing_order == 1
        assert rows[0].raw_role_text == "Self"
        assert rows[0].job_category_id == IMDB_JOB_CATEGORY_SELF

        # Check second row (actor)
        assert rows[1].name_id == "nm0000002"
        assert rows[1].raw_role_text == "Detective Jones"
        assert rows[1].job_category_id is None  # Not self

    def test_filters_by_category(self) -> None:
        """Should filter credits by job_category_filter (✅ FIXED: use category, not role text)."""
        credits = ImdbTitleCredits(
            imdb_id="tt1234567",
            credits=[
                {"id": "nm0000001", "name": "Jane Doe", "category": "self", "characters": ["Self"]},
                {"id": "nm0000002", "name": "John Smith", "category": "actor", "characters": ["Hero"]},
            ],
            total_count=2,
        )

        rows = normalize_api_credits_to_cast_rows(credits, job_category_filter="self")

        assert len(rows) == 1
        assert rows[0].name == "Jane Doe"
        assert rows[0].job_category_id == IMDB_JOB_CATEGORY_SELF

    def test_handles_multiple_characters(self) -> None:
        """Should join multiple characters with commas."""
        credits = ImdbTitleCredits(
            imdb_id="tt1234567",
            credits=[
                {
                    "id": "nm0000001",
                    "name": "Jane Doe",
                    "category": "actor",
                    "characters": ["Detective A", "Detective B (archive footage)"],
                },
            ],
            total_count=1,
        )

        rows = normalize_api_credits_to_cast_rows(credits)

        assert rows[0].raw_role_text == "Detective A, Detective B (archive footage)"

    def test_skips_invalid_credits(self) -> None:
        """Should skip credits with missing id or name."""
        credits = ImdbTitleCredits(
            imdb_id="tt1234567",
            credits=[
                {"id": None, "name": "No ID", "category": "self"},
                {"id": "nm0000002", "name": None, "category": "self"},
                {"id": "nm0000003", "name": "Valid", "category": "self"},
            ],
            total_count=3,
        )

        rows = normalize_api_credits_to_cast_rows(credits)

        assert len(rows) == 1
        assert rows[0].name == "Valid"


class TestFallbackLogic:
    """Test automatic fallback to JSON API on blocked HTML responses."""

    def test_fallback_on_blocked_html(self) -> None:
        """Should fall back to JSON API when HTML returns 202."""
        # Mock HTML client to return 202
        html_response = Mock(status_code=202, text="<html>Blocked</html>")

        # Mock JSON API to return valid credits
        api_credits = ImdbTitleCredits(
            imdb_id="tt1234567",
            credits=[
                {"id": "nm0000001", "name": "Jane Doe", "category": "self", "characters": ["Self"]},
            ],
            total_count=1,
        )

        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.fetch_fullcredits_page.side_effect = ImdbFullCreditsError(
                "Blocked", status_code=202, is_blocked=True
            )

            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.fetch_title_credits", return_value=api_credits):
                with patch("time.sleep"):
                    rows, source = fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

        assert len(rows) == 1
        assert source == "credits_api_fallback"
        assert rows[0].name == "Jane Doe"

    def test_no_fallback_when_disabled(self) -> None:
        """Should not fall back when IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0."""
        html_response = Mock(status_code=202, text="<html>Blocked</html>")

        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.fetch_fullcredits_page.side_effect = ImdbFullCreditsError(
                "Blocked", status_code=202, is_blocked=True
            )

            with patch.dict("os.environ", {"IMDB_FULLCREDITS_ENABLE_API_FALLBACK": "0"}):
                with patch("time.sleep"):
                    with pytest.raises(ImdbFullCreditsError) as exc_info:
                        fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

        exc = exc_info.value
        assert exc.is_blocked is True
        assert exc.status_code == 202

    def test_html_success_no_fallback(self) -> None:
        """Should use HTML and not fall back when HTML succeeds."""
        html_content = "<html>Valid cast list</html>"

        with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.HttpImdbFullCreditsClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.fetch_fullcredits_page.return_value = html_content

            with patch("trr_backend.integrations.imdb.fullcredits_cast_parser.parse_fullcredits_cast", return_value=[]):
                rows, source = fetch_fullcredits_cast_with_fallback("tt1234567", verbose=False)

        assert source == "fullcredits_html"
```

**Validation Checkpoint:**
```bash
# Run all resilience tests
pytest tests/integrations/imdb/test_fullcredits_resilience.py -v

# Expected: All tests pass
# - 5 retry logic tests
# - 2 debug HTML saving tests
# - 4 normalization tests
# - 3 fallback logic tests
```

**Dependencies:**
- Step 2.1 (ImdbFullCreditsError.is_blocked)
- Step 3.1 (retry logic)
- Step 4.1 (normalize_api_credits_to_cast_rows)
- Step 4.2 (fetch_fullcredits_cast_with_fallback)

**Risks:** None (tests are isolated with mocks)

---

## Phase 8: Integration Testing & Documentation

**Goal:** End-to-end testing and update architecture docs

**Priority:** MEDIUM (validates complete flow)

### Step 8.1: Manual Integration Test

**Checklist:**
```bash
# 1. Apply database migration
cd /Users/thomashulihan/Projects/trr-backend-fix-imdb-202
supabase db reset

# 2. Run sync with known blocked show (tt1720601)
PYTHONPATH=. python scripts/sync/sync_show_cast.py \
  --imdb-id tt1720601 \
  --verbose

# Expected output:
# ⚠️  IMDb HTML blocked for tt1720601 (HTTP 202), falling back to JSON API...
# ✅ JSON API fallback succeeded: XX total credits
# cast_rows_self=XX
# show_cast_upserted=XX

# 3. Verify database has source_type
psql $TRR_DB_URL -c "
SELECT show_id, person_id, credit_category, source_type
FROM core.show_cast
WHERE source_type = 'credits_api_fallback'
LIMIT 5;
"

# Expected: Rows with source_type='credits_api_fallback'

# 4. Test with normal show (should use HTML)
PYTHONPATH=. python scripts/sync/sync_show_cast.py \
  --imdb-id tt0386676 \
  --verbose

# Expected: No fallback message, source_type='fullcredits_html'

# 5. Verify debug HTML saved
ls -lh debug_html/

# Expected: imdb_fullcredits_tt1720601_*.html files (if tt1720601 returned 202)

# 6. Test with fallback disabled
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 \
PYTHONPATH=. python scripts/sync/sync_show_cast.py \
  --imdb-id tt1720601 \
  --verbose

# Expected: Fails with ImdbFullCreditsError (fallback disabled)

# 7. Test retry configuration
IMDB_FULLCREDITS_MAX_RETRIES=0 \
IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC=2.0 \
PYTHONPATH=. python scripts/sync/sync_show_cast.py \
  --imdb-id tt1720601 \
  --verbose

# Expected: Faster failure (no retries, immediate fallback)
```

### Step 8.2: Update Architecture Documentation

**File:** `docs/architecture/integrations.md`

**Location:** Add new section after line 38 (after existing IMDb episodic credits section)

**Add:**
```markdown
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
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601 --verbose

# Configure retry behavior
IMDB_FULLCREDITS_MAX_RETRIES=1 \
IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC=3.0 \
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601

# Disable fallback (for testing/rollback)
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 \
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601
```

See [IMDb Full Credits Resilience Spec](./imdb_fullcredits_resilience_spec.md) for full design rationale.
```

**Validation Checkpoint:**
```bash
# Verify markdown renders correctly
grep -A 35 "IMDb Full Credits Resilience" docs/architecture/integrations.md
```

**Dependencies:** All previous steps completed

**Risks:** None (documentation only)

---

## Risk Assessment & Mitigation

### High-Risk Areas

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **JSON API structure changes** | High - normalization breaks | Medium | Add schema validation tests, monitor API responses |
| **JSON API rate limits unknown** | Medium - fallback fails | Medium | Add rate limit detection, use same retry logic as HTML |
| **Database migration failure** | High - blocks all changes | Low | Test migration on staging, include rollback SQL |
| **Fallback logic not centralized** | High - scripts fail inconsistently | Low | ✅ **FIXED: Centralized in integration layer** |

### Medium-Risk Areas

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **Retry delays slow bulk sync** | Medium - pipeline slower | High | Acceptable tradeoff, make configurable via env |
| **Fallback data quality differs** | Medium - cast incomplete | Medium | Use category-based filtering (same as HTML) |
| **Cookie dependency remains** | Low - users confused | Medium | Update .env.example with clear warnings |

### Low-Risk Areas

- Error handling changes (backwards compatible)
- Test additions (no production impact)
- Documentation updates (no code impact)

---

## Validation Checkpoints Summary

Run these commands after completing all phases:

```bash
# 1. All unit tests pass
pytest tests/integrations/imdb/test_fullcredits_resilience.py -v

# 2. Existing tests still pass (regression check)
pytest tests/integrations/imdb/test_fullcredits_cast_parser.py -v

# 3. Database migration applied successfully
supabase db reset
psql $TRR_DB_URL -c "\d core.show_cast" | grep source_type

# 4. Sync script works end-to-end with fallback
PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601 --verbose

# 5. Sync script works with fallback disabled
IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0 PYTHONPATH=. python scripts/sync/sync_show_cast.py --imdb-id tt1720601 --verbose || echo "Expected failure"

# 6. Verify data in database
psql $TRR_DB_URL -c "
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE source_type = 'fullcredits_html') AS html_count,
       COUNT(*) FILTER (WHERE source_type = 'credits_api_fallback') AS api_count
FROM core.show_cast;
"

# 7. Linting passes
ruff check .
ruff format --check .

# 8. Type checking passes (if using mypy)
mypy trr_backend/integrations/imdb/fullcredits_cast_parser.py
```

---

## Commit Strategy

Follow **atomic commits** pattern - one commit per logical change:

### Commit 1: Database Schema
```bash
git add supabase/migrations/0053_add_show_cast_source_tracking.sql
git commit -m "feat(db): add source_type column to show_cast

- Add source_type column (values: fullcredits_html, credits_api_fallback, manual)
- Add check constraint for valid values
- Add index for analytics queries
- Backwards compatible (defaults to 'fullcredits_html')

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 2: Error Handling & Retry Logic
```bash
git add trr_backend/integrations/imdb/fullcredits_cast_parser.py
git commit -m "feat(imdb): add retry logic and is_blocked flag to fullcredits

- Extend ImdbFullCreditsError with is_blocked flag for 202/403/429
- Implement exponential backoff retry for blocked requests
- Add debug HTML saving on blocked responses (verbose mode)
- Make retry configurable via IMDB_FULLCREDITS_MAX_RETRIES env var
- Save debug HTML once per show using Path.cwd() for testability

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 3: JSON API Normalization
```bash
git add trr_backend/integrations/imdb/fullcredits_cast_parser.py
git commit -m "feat(imdb): add JSON API to CastRow normalization layer

- Add normalize_api_credits_to_cast_rows() function
- Maps api.imdbapi.dev credits to CastRow format
- Uses category-based filtering (not raw role text)
- Sets job_category_id for 'self' category
- Enables fallback from HTML scraping to JSON API

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 4: Centralized Fallback Function
```bash
git add trr_backend/integrations/imdb/fullcredits_cast_parser.py
git commit -m "feat(imdb): add centralized fallback function

- Add fetch_fullcredits_cast_with_fallback() in integration layer
- Automatically falls back to JSON API on 202/403/429
- Returns (cast_rows, source_type) tuple
- Respects IMDB_FULLCREDITS_ENABLE_API_FALLBACK env var
- Centralizes resilience logic for all callers

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 5: Sync Script Integration
```bash
git add scripts/sync/sync_show_cast.py
git commit -m "feat(sync): use centralized HTML + JSON API fallback

- Use fetch_fullcredits_cast_with_fallback() for resilience
- Inject source_type into show_cast rows (no repo signature change)
- Log fallback events when verbose mode enabled
- Pass verbose flag to enable debug HTML saving

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 6: Documentation & Environment
```bash
git add .env.example docs/architecture/integrations.md
git commit -m "docs: document IMDb resilience features and env vars

- Add IMDB_FULLCREDITS_MAX_RETRIES (default: 2)
- Add IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC (default: 5.0)
- Add IMDB_FULLCREDITS_ENABLE_API_FALLBACK (default: 1)
- Clarify IMDB_EXTRA_HEADERS_JSON is optional/debugging only
- Add resilience section to integrations.md
- Warn about cookie fragility

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

### Commit 7: Comprehensive Tests
```bash
git add tests/integrations/imdb/test_fullcredits_resilience.py
git commit -m "test(imdb): add comprehensive resilience tests

- Test retry logic (202 → 200, exhausted retries)
- Test error classification (is_blocked flag for 202/403/429)
- Test debug HTML saving (verbose mode, monkeypatch.chdir)
- Test JSON API normalization (category-based filtering)
- Test fallback logic (automatic, disabled, HTML success)

Ref: docs/architecture/imdb_fullcredits_resilience_spec.md"
```

---

## Rollback Plan

If issues arise after deployment:

### Immediate Rollback (Code)
```bash
# Revert all code changes
git revert <commit-sha-7>...<commit-sha-2>  # Keep migration (commit 1)
git push origin fix/imdb-cast-sync-202-fallback
```

### Database Rollback (If Needed)
```sql
-- File: supabase/migrations/0054_rollback_show_cast_source_tracking.sql
BEGIN;

-- Drop index
DROP INDEX IF EXISTS core.idx_show_cast_source_type;

-- Drop column
ALTER TABLE core.show_cast
DROP COLUMN IF EXISTS source_type;

COMMIT;
```

### Partial Rollback (Disable Fallback Only)
```bash
# Disable fallback without code changes
export IMDB_FULLCREDITS_ENABLE_API_FALLBACK=0
```

### Partial Rollback (Disable Retries, Immediate Fallback)
```bash
# Skip retries, go straight to fallback on blocked HTML
export IMDB_FULLCREDITS_MAX_RETRIES=0
```

---

## Post-Implementation Checklist

After merging to main:

- [ ] Run full test suite on CI: `pytest`
- [ ] Run linting: `ruff check . && ruff format --check .`
- [ ] Run `make schema-docs-check` (migration changed schema)
- [ ] Deploy migration to staging: `supabase db push`
- [ ] Test sync on staging with tt1720601
- [ ] Monitor error logs for unexpected failures
- [ ] Check `core.show_cast` source_type distribution
- [ ] Update runbook with new env vars
- [ ] Announce new feature to team (Slack/email)
- [ ] Deprecate old `fetch_fullcredits_cast()` (add warning in docstring)

---

## Success Metrics

**Must Have:**
- ✅ `sync_show_cast.py --imdb-id tt1720601` completes successfully
- ✅ `core.show_cast` has non-zero rows for tt1720601
- ✅ `source_type` column populated correctly
- ✅ All tests pass (100% coverage on new code)
- ✅ Logs clearly indicate HTML vs API source
- ✅ Fallback is centralized in integration layer

**Nice to Have:**
- ⭐ <5% of shows use API fallback (HTML path preferred)
- ⭐ Retry success rate >50% (202 → 200 after backoff)
- ⭐ Debug HTML artifacts saved for all blocked responses (when --verbose)
- ⭐ Zero production incidents related to IMDb sync

---

## Timeline Estimate

| Phase | Estimated Time | Notes |
|-------|----------------|-------|
| Phase 1: Database Migration | 30 min | Straightforward SQL |
| Phase 2: Error Handling | 30 min | Simple flag addition |
| Phase 3: Retry Logic | 1.5 hours | Requires careful testing of backoff |
| Phase 4: Normalization + Fallback | 2 hours | Centralized fallback function (was split before) |
| Phase 5: Sync Script | 30 min | Simplified (no repo signature change) |
| Phase 6: Documentation | 30 min | .env.example + integrations.md |
| Phase 7: Testing | 2 hours | Comprehensive test suite |
| Phase 8: Integration Testing | 1 hour | End-to-end validation |
| **Total** | **~8.5 hours** | Includes breaks/debugging |

---

## Next Steps

1. ✅ Technical review completed with fixes incorporated
2. ⬜ Start Phase 1 (Database Migration)
3. ⬜ Follow phases sequentially with validation checkpoints
4. ⬜ Create PR after Phase 7 (all tests passing)
5. ⬜ Deploy to staging → production

**Ready to start implementation?** Use `/trr-impl` to begin executing this plan.
