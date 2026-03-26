# Reddit Stable Reads Backend Read Cutover

Last updated: 2026-03-26

## Status
- Batch 2.5 stable Reddit backend read lane is implemented and validated against the live local backend.
- App proxy work landed separately in `TRR-APP`, but this note tracks the backend-owned cutover only.

## What changed
- Added backend-owned stable Reddit read endpoints under `GET /api/v1/admin/reddit/...` for:
  - communities
  - community detail
  - threads
  - thread detail
  - stored post counts
  - analytics summary
  - stored posts
  - analytics posts
  - post resolve
- Added backend repository shaping in `trr_backend/repositories/admin_reddit_reads.py`.
- Added focused backend API and repository tests.
- Kept route contracts aligned with the current app surfaces and preserved the stable-vs-live boundary.

## Validation
- Passed: `pytest tests/api/test_admin_reddit_reads.py tests/repositories/test_admin_reddit_reads_repository.py`
- Passed: `ruff check api/main.py api/routers/admin_reddit_reads.py trr_backend/repositories/admin_reddit_reads.py tests/api/test_admin_reddit_reads.py tests/repositories/test_admin_reddit_reads_repository.py`
- Added repository assertions that stable communities, threads, and stored-post-counts queries bind the full psycopg parameter set. This closed a live `IndexError` gap that mocked tests had missed.

## Smoke / evidence
- Direct backend smoke succeeded against `http://127.0.0.1:8000/api/v1/admin/reddit/...` with authenticated local credentials:
  - `communities` -> `200`, `5310` bytes, `52.7ms`
  - `community detail` -> `200`, `1477` bytes, `48.5ms`
  - `threads` -> `200`, `52036` bytes, `107.7ms`
  - `thread detail` -> `200`, `924` bytes, `53.5ms`
  - `stored-post-counts` -> `200`, `20173` bytes, `960.0ms`
  - `analytics summary` -> `200`, `16850` bytes, `2048.6ms`
  - `stored posts` -> `200`, `26764` bytes, `216.9ms`
  - `analytics posts` -> `200`, `65520` bytes, `1340.1ms`
  - `post resolve` -> sampled `post_id` path returned `404`, `61` bytes, `147.1ms`
- Route-level cache and in-flight dedupe are in place for the stable read endpoints.
- Query-count expectations from the backend-owned narrow reads are:
  - communities `1`
  - community detail `1`
  - threads `1`
  - thread detail `1`
  - stored-post-counts `4`
  - analytics summary `2`
  - stored posts `2`
  - analytics posts `2`
  - post resolve `1`

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-26
  current_phase: "backend-owned stable reddit reads shipped for Batch 2.5 with live local smoke"
  next_action: "keep live/discover/backfill/mutation flows out of scope; treat sampled resolve 404 as a legacy stable-read behavior note unless later parity work promotes it"
  detail: self
```
