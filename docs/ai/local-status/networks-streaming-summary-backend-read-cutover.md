# Networks Streaming Summary Backend Read Cutover

Last updated: 2026-03-26

## Status
- Backend Batch 2.4 summary lane complete.

## What changed
- Added backend-owned summary reads at `GET /api/v1/admin/shows/networks-streaming/summary`.
- Added backend cache invalidation at `POST /api/v1/admin/shows/networks-streaming/summary/cache/invalidate`.
- Kept the existing app summary payload contract intact: `totals`, `rows[]`, derived booleans, and `generated_at`.
- Moved the summary query shape into `trr_backend/repositories/admin_networks_streaming_reads.py` with backend-side cache, in-flight dedupe, and read logging for `latency_ms`, `payload_bytes`, `query_count`, and cache status.
- No migration or schema change was required for the Batch 2.4 summary cutover.

## Validation
- Passed: `pytest tests/api/test_admin_networks_streaming_reads.py tests/repositories/test_admin_networks_streaming_reads_repository.py`
- Passed: `ruff check api/routers/admin_networks_streaming_reads.py trr_backend/repositories/admin_networks_streaming_reads.py tests/api/test_admin_networks_streaming_reads.py tests/repositories/test_admin_networks_streaming_reads_repository.py api/main.py`
- Passed: `ruff format --check api/routers/admin_networks_streaming_reads.py trr_backend/repositories/admin_networks_streaming_reads.py tests/api/test_admin_networks_streaming_reads.py tests/repositories/test_admin_networks_streaming_reads_repository.py api/main.py`
- Live smoke: `GET /api/v1/admin/shows/networks-streaming/summary` returned `200` with `totals.total_available_shows=223`, `totals.total_added_shows=12`, `rows=403`, `query_count=2`, `payload_bytes=328998`, `latency_ms=2214.2`, cache=`miss`.

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-26
  current_phase: "backend-owned networks-streaming summary reads shipped for Batch 2.4"
  next_action: "keep the summary route on the measured query/payload envelope while app consumers finish Batch 2 summary cutover"
  detail: self
```
