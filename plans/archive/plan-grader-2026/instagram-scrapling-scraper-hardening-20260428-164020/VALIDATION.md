# Validation

## Files Inspected

- `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md`
- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- `trr_backend/socials/instagram/runtimes/dispatcher.py`
- `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- `trr_backend/socials/instagram/posts_scrapling/session.py`
- `trr_backend/socials/instagram/comments_scrapling/session.py`
- `trr_backend/socials/instagram/auth_resolver.py`
- `trr_backend/db/pg.py`
- `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`

## Commands Run

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git status --short
```

Result from final packaging pass: `.plan-grader/` and `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md` are untracked; no tracked scraper implementation files were changed by the grading pass.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest tests/socials/instagram/runtimes/test_dispatcher.py -q
```

Result from source-plan creation pass: `5 passed`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python - <<'PY'
from scrapling.fetchers import StealthyFetcher, Fetcher, DynamicFetcher
import inspect
print("StealthyFetcher.async_fetch", inspect.signature(StealthyFetcher.async_fetch))
PY
```

Result from source-plan creation pass: backend venv has `scrapling==0.4.6`; `StealthyFetcher.async_fetch` exists and accepts `StealthSession` kwargs.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python -m json.tool .plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/result.json
```

Result from Plan Grader packaging pass: valid JSON.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
test -f .plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md
```

Result from Plan Grader packaging pass: required revised plan exists.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
find .plan-grader/instagram-scrapling-scraper-hardening-20260428-164020 -maxdepth 1 -type f -print | sort
```

Result from Plan Grader packaging pass: package contains `AUDIT.md`, `COMPARISON.md`, `PATCHES.md`, `REVISED_PLAN.md`, `SCORECARD.md`, `SUGGESTIONS.md`, `VALIDATION.md`, and `result.json`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "TBD|TODO|implement later|fill in details|Similar to Task|\\.\\.\\.|locals\\(\\)" .plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md
```

Result from Plan Grader packaging pass: no placeholder or rejected `locals()` snippets found.

## Evidence Gaps

- Scrapling MCP tools were not exposed in this Codex session after tool discovery, so this audit relies on installed Scrapling skills and local package inspection.
- No live Instagram fetch was run. That is appropriate for plan grading; implementation should keep live smoke checks optional and operator-controlled.
- The source plan was not executed. The audit is static plus focused current-state validation.

## Current-State Findings That Changed the Plan

- `InstagramAuthSession` is not safe to construct with partial fields in a test.
- `pg.fetch_one(..., conn=...)` exists, so cancellation checks can avoid extra pool checkouts while the comments persist connection is open.
- Comments runner already has degraded summary handling; posts runner should mirror it.
