# TRR Backend Bug Report

Date: 2026-05-19
Scope: TRR-Backend admin social/profile routes, database pool behavior, social catalog freshness
DebugPro status: INCONCLUSIVE root cause, OPEN bug

## Root Cause

The backend is intermittently exhausting its local database pool and hitting statement timeouts while serving admin social/profile reads. The most visible failure is the Instagram catalog freshness route for `thetraitorsus`, but the same pressure window also degrades the live-status stream and unrelated admin show reads.

The bounded cause is query and pool pressure in admin read paths that share the backend process. The exact root cause is not fully proven yet because the current evidence is from live logs and source inspection, not an isolated query plan or load reproduction.

## Evidence

- `api/routers/socials/__init__.py:5255` exposes `POST /profiles/{platform}/{account_handle}/catalog/freshness` with a default `statement_timeout_ms=3000`.
- `trr_backend/socials/social_season_analytics_impl.py:59123` opens the `catalog-freshness` connection from the `social_profile` pool, sets a local statement timeout, and then calls `_catalog_recent_runs`.
- `trr_backend/socials/social_season_analytics_impl.py:31515` builds `_catalog_recent_runs` as a multi-stage query over `social.scrape_runs` and lateral joins into `social.scrape_jobs`.
- `.logs/workspace/trr-backend.log` shows `catalog-freshness` timing out with `psycopg2.errors.QueryCanceled: canceling statement due to statement timeout`, followed by `POST /api/v1/admin/socials/profiles/instagram/thetraitorsus/catalog/freshness` returning `503 Service Unavailable`.
- The same log window shows pool saturation: `acquire_failed ... error=PoolError in_use=3 available=0` for `fetch_one`, `fetch_all`, and `read`.
- `api/routers/socials/__init__.py:6486` runs `/live-status/stream`; the log shows `Social live-status stream tick degraded` after `asyncio.wait_for` times out around the same pool-pressure window.
- `.logs/workspace/runtime-reconcile.json` reports `overall_state=ok`, database history in sync, Modal readiness ok, and Instagram remote auth ready. That bounds this away from migration drift, missing Modal functions, or missing Instagram cookies.

## Minimal Reproduction or Failing Signal

Start the workspace in the normal TRR browser-verification lane, open the Instagram account profile, and trigger the catalog freshness check while the page is also loading catalog posts, review queue, cookie health, and live status.

Observed failing signals from the current log:

- Backend route: `POST /api/v1/admin/socials/profiles/instagram/thetraitorsus/catalog/freshness`
- Backend response: `503 Service Unavailable`
- Backend exception: `psycopg2.errors.QueryCanceled: canceling statement due to statement timeout`
- Pool pressure: `PoolError in_use=3 available=0`

## Fix

Practical fix direction:

1. Make `catalog-freshness` cheap before it reaches the request timeout. The first target is `_catalog_recent_runs`, because the failing stack enters that query before the route returns 503.
2. Add a focused index or query rewrite for the `social.scrape_runs` to `social.scrape_jobs` lookup pattern used by account catalog recent-run reads.
3. Keep the route bounded: if the recent-run query times out, return a partial freshness payload with `latest_run` marked unavailable instead of failing the entire freshness check.
4. Add a narrow regression test that simulates timeout from `_catalog_recent_runs` and verifies that the route returns a degraded freshness response rather than a 503.

Likely files:

- `trr_backend/socials/social_season_analytics_impl.py`
- `api/routers/socials/__init__.py`
- `tests/api/routers/test_socials_season_analytics.py`
- `tests/repositories/test_socialblade_growth.py` or a new social catalog freshness test
- possibly a Supabase migration under `supabase/migrations/` if the query needs a new index

## Verification

- Passed: `.venv/bin/python -m pytest tests/api/test_admin_socialblade.py tests/repositories/test_socialblade_growth.py -q`
- Result: `22 passed in 2.50s`
- Not yet run: live browser reproduction after a backend fix, because this report only documents the bug.
- Not yet run: query plan capture for `_catalog_recent_runs`; that is the next evidence step before choosing index versus query rewrite.

## Prevention

- Add a backend test for catalog freshness timeout degradation.
- Add a query-plan check or documented index rationale for the account catalog recent-run lookup.
- Keep `make status` or a focused pressure probe in the verification path so future fixes prove both route behavior and pool pressure.

## Adjacent Sweep

Bounded sweep completed by log/source inspection only:

- Catalog freshness route
- Live-status stream
- Admin show/social read routes affected in the same pressure window
- Runtime reconcile and Modal readiness

No code sweep or fix was applied.

## Open Questions

- Does `_catalog_recent_runs` need a new composite/expression index, or can the query be split into cheaper run and job lookups?
- Should catalog freshness return partial data when recent-run lookup times out?
- Are the current local backend pool caps intentionally low for this page load, or should the account profile page defer more secondary reads?
