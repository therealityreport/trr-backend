# TikTok yt-dlp Fallback And Auth Follow-Ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the full TikTok fallback chain by fixing the pre-existing yt-dlp datetime crash, then run a deterministic authenticated-vs-unauthenticated comparison using the repo's canonical TikTok cookie loader.

**Architecture:** Keep the fix narrow. The code change is limited to the direct yt-dlp fallback path in the TikTok scraper plus one regression test. The auth comparison reuses existing cookie-loading and auto-refresh infrastructure instead of adding new auth code, and records the outcome through the existing TikTok CLI diagnostics flow.

**Tech Stack:** Python 3.11, pytest, yt-dlp, requests/curl_cffi TikTok scraper path, repo-local TikTok auth loader in `trr_backend.repositories.social_season_analytics`

---

## File Structure

- Modify: `trr_backend/socials/tiktok/scraper.py`
  - Fix the naive-vs-aware datetime subtraction in `_scrape_via_ytdlp()`.
- Modify: `tests/socials/test_comment_scraper_fixes.py`
  - Add a regression test that exercises the yt-dlp fallback with the same naive `datetime` shape the CLI produces.
- Use existing operational entrypoint: `scripts/socials/tiktok/scrape.py`
  - No code change required for the auth comparison. It already loads cookies via the canonical repo auth loader when `--no-auth` is omitted.
- Use existing cookie loader: `trr_backend/repositories/social_season_analytics.py`
  - No code change required for the auth availability audit. Reuse `_load_tiktok_cookies_from_sources()` and `_load_tiktok_cookies()`.
- Optionally update status: `docs/ai/local-status/tiktok-http-triage-followups.md`
  - Capture the result of the fallback fix and auth comparison after execution.

## Task 1: Reproduce The yt-dlp Datetime Crash With A Focused Regression Test

**Files:**
- Modify: `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
- Read for context: `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py:834-900`

- [ ] **Step 1: Write the failing regression test for a naive `date_start`**

Add this test near the existing TikTok yt-dlp fallback coverage in `tests/socials/test_comment_scraper_fixes.py`:

```python
def test_tiktok_ytdlp_fallback_accepts_naive_start_date(monkeypatch: pytest.MonkeyPatch) -> None:
    scraper = TikTokScraper()
    config = TikTokScrapeConfig(
        username="bravotv",
        hashtags=["RHOSLC"],
        date_start=datetime(2026, 1, 1),
        date_end=datetime(2026, 1, 2),
    )

    class _Proc:
        returncode = 0
        stdout = ""
        stderr = ""

    monkeypatch.setattr(scraper, "_has_ytdlp", lambda: True)
    monkeypatch.setattr(scraper, "_find_ytdlp_cookie_file", lambda: None)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: _Proc())

    assert scraper._scrape_via_ytdlp(config) == []
```

- [ ] **Step 2: Run the new regression test and verify it fails on the current code**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
pytest -q tests/socials/test_comment_scraper_fixes.py -k naive_start_date
```

Expected before the fix:

```text
E   TypeError: can't subtract offset-naive and offset-aware datetimes
```

- [ ] **Step 3: Commit the failing test before implementation**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add tests/socials/test_comment_scraper_fixes.py
git commit -m "test: reproduce tiktok ytdlp naive datetime crash"
```

## Task 2: Fix `_scrape_via_ytdlp()` To Normalize CLI Dates Before Arithmetic

**Files:**
- Modify: `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py:834-900`
- Test: `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`

- [ ] **Step 1: Add a tiny UTC-normalization helper close to the yt-dlp fallback code**

Add a helper in `scraper.py` near `_scrape_via_ytdlp()`:

```python
def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
```

Do not broaden the change beyond TikTok fallback. This helper exists only to make date arithmetic safe for both:
- naive `datetime` objects produced by `scripts/socials/tiktok/scrape.py::parse_date()`
- aware `datetime` objects that may already carry timezone information

- [ ] **Step 2: Use the helper inside `_scrape_via_ytdlp()`**

Replace the current date arithmetic:

```python
if config.date_start:
    days_back = (datetime.now(tz=UTC) - config.date_start).days
    max_videos = max(500, min(12000, days_back * 22))
```

with:

```python
start_dt = _coerce_utc_datetime(config.date_start)
if start_dt:
    days_back = max(0, (datetime.now(tz=UTC) - start_dt).days)
    max_videos = max(500, min(12000, days_back * 22))
```

The `max(0, ...)` clamp is intentional. It prevents future-dated inputs from generating negative playlist limits.

- [ ] **Step 3: Run the focused regression test and verify it passes**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
pytest -q tests/socials/test_comment_scraper_fixes.py -k naive_start_date
```

Expected:

```text
1 passed
```

- [ ] **Step 4: Run the nearby TikTok fallback coverage to make sure behavior did not drift**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
pytest -q tests/socials/test_comment_scraper_fixes.py -k "ytdlp or tiktok_auto_mode_uses_ytdlp"
```

Expected:

```text
... passed
```

- [ ] **Step 5: Commit the fix**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add trr_backend/socials/tiktok/scraper.py tests/socials/test_comment_scraper_fixes.py
git commit -m "fix: handle naive datetimes in tiktok ytdlp fallback"
```

## Task 3: Validate The Full Fallback Chain Locally After The Fix

**Files:**
- Use existing entrypoint: `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/tiktok/scrape.py`
- Read existing fallback metadata writes in: `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`

- [ ] **Step 1: Run the same baseline CLI command without hiding `yt-dlp` from `PATH`**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python -m scripts.socials.tiktok.scrape \
  --username bravotv \
  --hashtags RHOSLC \
  --start 2025-08-14 \
  --end 2026-02-04 \
  --max-pages 2 \
  --no-auth \
  --http-client requests \
  --diagnostics-json /tmp/tiktok-triage/posts-baseline-after-ytdlp-fix.json
```

Expected:
- No `TypeError: can't subtract offset-naive and offset-aware datetimes`
- Diagnostics JSON is written
- `yt_dlp_used` and `ytdlp_cookie_file_present` are present in the diagnostics payload when fallback is attempted

- [ ] **Step 2: Confirm the fallback chain reaches yt-dlp instead of crashing**

Run:

```bash
jq '{retrieval_mode, fallback_chain, yt_dlp_used, ytdlp_cookie_file_present, stop_reason, api_fail_reason, api_pagination_blocked_reason}' \
  /tmp/tiktok-triage/posts-baseline-after-ytdlp-fix.json
```

Expected shape:

```json
{
  "retrieval_mode": "...",
  "fallback_chain": "...",
  "yt_dlp_used": true,
  "ytdlp_cookie_file_present": false
}
```

The exact `retrieval_mode` may still be `"none"` or `"ytdlp_fallback"` depending on the target. The success condition for this task is "fallback executes and reports cleanly," not "TikTok returns data."

- [ ] **Step 3: Commit only if you needed a small adjustment after this smoke run**

If the smoke run exposes a fallback metadata bug caused by the datetime fix, patch it, rerun the same command, then commit:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add trr_backend/socials/tiktok/scraper.py tests/socials/test_comment_scraper_fixes.py
git commit -m "fix: preserve tiktok ytdlp fallback diagnostics"
```

If no code changes were needed, skip this commit.

## Task 4: Audit TikTok Cookie Availability Using The Canonical Repo Loader

**Files:**
- Read only: `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py:7348-7443`
- Use existing CLI: `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/tiktok/scrape.py:325-356`
- Read env contract reference: `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example:347-355`

- [ ] **Step 1: Inspect raw cookie availability without triggering auto-refresh**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python - <<'PY'
from trr_backend.repositories.social_season_analytics import _load_tiktok_cookies_from_sources

cookies = _load_tiktok_cookies_from_sources()
print(
    {
        "count": len(cookies),
        "keys": sorted(cookies.keys()),
        "has_sessionid": "sessionid" in cookies,
        "has_sessionid_ss": "sessionid_ss" in cookies,
        "has_sid_tt": "sid_tt" in cookies,
    }
)
PY
```

Expected in the current local environment, based on the earlier audit:

```python
{"count": 0, "keys": [], "has_sessionid": False, "has_sessionid_ss": False, "has_sid_tt": False}
```

- [ ] **Step 2: If cookies exist, verify the canonical freshness path too**

Run only when Step 1 returns any session cookies:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python - <<'PY'
from trr_backend.socials.control_plane import _load_tiktok_cookies

cookies = _load_tiktok_cookies()
print(
    {
        "count": len(cookies),
        "keys": sorted(cookies.keys()),
        "has_sessionid": "sessionid" in cookies,
        "has_sessionid_ss": "sessionid_ss" in cookies,
        "has_sid_tt": "sid_tt" in cookies,
    }
)
PY
```

Expected:
- Either a valid cookie set is returned
- Or the loader logs that refresh/validation failed and returns an empty or stale map

Do not add new code here. This step exists to reuse the same loader the CLI already calls.

## Task 5: Run The Auth-Presence Comparison On The Existing TikTok Triage Path

**Files:**
- Use existing CLI: `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/tiktok/scrape.py`
- Compare diagnostics already emitted by: `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`

- [ ] **Step 1: Run the authenticated comparison only if Task 4 found valid cookies**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python -m scripts.socials.tiktok.scrape \
  --username bravotv \
  --hashtags RHOSLC \
  --start 2025-08-14 \
  --end 2026-02-04 \
  --max-pages 2 \
  --http-client requests \
  --diagnostics-json /tmp/tiktok-triage/posts-auth-requests.json
```

Then run the hardened transport variant:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python -m scripts.socials.tiktok.scrape \
  --username bravotv \
  --hashtags RHOSLC \
  --start 2025-08-14 \
  --end 2026-02-04 \
  --max-pages 2 \
  --http-client curl_cffi \
  --diagnostics-json /tmp/tiktok-triage/posts-auth-curl-cffi.json
```

- [ ] **Step 2: Compare auth vs no-auth on the same IP before adding a proxy**

Run:

```bash
jq '{http_client, auth_mode, retrieval_mode, api_fail_reason, api_pagination_blocked_reason, total_posts, posts_checked, yt_dlp_used, endpoint_responses}' \
  /tmp/tiktok-triage/posts-baseline-after-ytdlp-fix.json \
  /tmp/tiktok-triage/posts-auth-requests.json \
  /tmp/tiktok-triage/posts-auth-curl-cffi.json
```

Interpretation:
- If authenticated runs still show empty-body `fetch_user_detail`, auth is not the immediate fix
- If authenticated runs return valid JSON where unauthenticated runs do not, auth presence matters even before proxying
- If auth improves the API path but still yields low/blocked results, keep auth in the stack and still proceed to the proxy trial

- [ ] **Step 3: If Task 4 found no cookies, record the block and stop**

Do not fake this test. Record:
- auth comparison blocked because no TikTok cookies were available in env/file sources
- next dependency is obtaining valid TikTok cookie material

No code change is required for this branch.

## Task 6: Record Findings And Hand Off The Next Dependency

**Files:**
- Create or modify: `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/local-status/tiktok-http-triage-followups.md`

- [ ] **Step 1: Capture the outcome in a short status note**

Write a status note with:
- whether the yt-dlp fallback crash is fixed
- whether fallback now runs end-to-end
- whether TikTok cookies were available in this environment
- whether auth changed the result on the same IP
- the next recommended action: get a residential/ISP proxy trial and run the proxy-backed comparison

Suggested outline:

```markdown
# TikTok HTTP Triage Follow-Ups

- yt-dlp fallback datetime crash: fixed / not fixed
- fallback execution after fix: working / blocked
- TikTok cookie availability: present / absent
- auth-vs-no-auth result: ...
- next step: obtain proxy trial and run `curl_cffi + proxy`
```

- [ ] **Step 2: Run the touched-repo validation commands**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
ruff check .
ruff format --check .
pytest -q
```

Expected:
- no new failures from the yt-dlp datetime fix
- broader pre-existing failures, if any, should be documented separately rather than conflated with this change

- [ ] **Step 3: Commit the follow-up status note if it was added**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git add docs/ai/local-status/tiktok-http-triage-followups.md
git commit -m "docs: record tiktok triage follow-up findings"
```

## Self-Review

- Spec coverage check:
  - yt-dlp datetime bug: covered in Tasks 1-3
  - auth cookie availability check: covered in Task 4
  - auth-presence comparison on current IP: covered in Task 5
  - reminder about the next external dependency: covered in Task 6
- Placeholder scan:
  - no `TODO`, `TBD`, or "handle appropriately" placeholders remain
  - commands, files, and expected outcomes are explicit
- Type consistency:
  - plan consistently uses the existing `TikTokScraper`, `TikTokScrapeConfig`, `_load_tiktok_cookies_from_sources()`, and `_load_tiktok_cookies()` names already present in the repo

