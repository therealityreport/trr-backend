# Twitter Search Persistence Query-Run Provenance

Last updated: 2026-03-23

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-23
  current_phase: "backend consolidation complete on main"
  next_action: "Use backend main as the source of truth and monitor downstream screenalytics/TRR-APP smoke coverage only if regressions appear"
  detail: self
```

## Summary
- Created a clean backend worktree branch at `feat/twitter-search-persistence-clean` to isolate the Twitter hashtag search feature from the contaminated PR branch.
- Replaced single-label standalone persistence with scrape-run provenance via `social.twitter_scrape_queries` and `social.twitter_scrape_query_tweets`.
- Hardened the Twitter search contract to whole-day windows and additive completeness diagnostics for API and CLI callers.

## What Changed
- Added migration `20260322153000_twitter_scrape_query_runs.sql` for scrape-run provenance tables.
- Updated `trr_backend/repositories/twitter_standalone.py` to persist tweet rows plus per-run query membership history.
- Updated `trr_backend/socials/twitter/scraper.py` to normalize whole-day windows and mark `complete` vs partial search outcomes.
- Updated `api/routers/socials.py` and `scripts/socials/twitter/scrape.py` to expose `retrieval_meta`, `complete`, `persist_summary`, and `scrape_run_id`.
- Expanded targeted tests for repository, API, CLI, and scraper contract coverage.

## Validation
- Passed:
  - `pytest tests/repositories/test_twitter_standalone_upsert.py tests/api/routers/test_twitter_persist_endpoint.py tests/scripts/test_twitter_scrape_persist.py tests/socials/test_twitter_query_building.py tests/socials/test_twitter_rate_limiting.py`
  - `ruff check` on all touched Twitter feature files
  - `ruff format --check` on all touched Twitter feature files
  - `./scripts/reload_postgrest_schema.sh` with env sourced from the main backend worktree `.env`
  - `make schema-docs-check`

## Notes
- The clean worktree intentionally leaves the original contaminated branch untouched.
- The missing query-run provenance delta from the clean worktree is now preserved on backend `main`.
