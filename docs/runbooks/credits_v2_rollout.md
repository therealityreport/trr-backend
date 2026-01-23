# Credits V2 Production Rollout (Phase 5d)

This runbook covers the production burn-in and cutover for Credits V2, using a
read-first then write pattern with explicit verification and rollback criteria.

## Required Environment Variables

- `ENABLE_CREDITS_V2_READ` (1 to read from v2 views)
- `ENABLE_CREDITS_V2_WRITE` (1 to write v2 credits tables)
- `RUN_DB_TESTS` (only for integration tests; not required for prod rollout)

## Staged Rollout Steps

1) **READ-only burn-in**
   - Set `ENABLE_CREDITS_V2_READ=1`
   - Set `ENABLE_CREDITS_V2_WRITE=0`

2) **Validate API + parity**
   - Verify cast endpoint returns non-empty data and correct pagination shape.
   - Run parity verification script (sample or per-show).

3) **WRITE enablement**
   - Flip `ENABLE_CREDITS_V2_WRITE=1`

4) **Validate again**
   - Run 1–2 single-show sync jobs.
   - Confirm no fallback warning flood.
   - Confirm parity still passes.

5) **Rollback (if needed)**
   - Set `ENABLE_CREDITS_V2_READ=0`
   - Keep `ENABLE_CREDITS_V2_WRITE=0` until issues resolved.

## Production Verification Commands

These commands work against any host (local, staging, prod). Replace placeholders
as needed.

Parity sample check:

```bash
PYTHONPATH=. python scripts/verify_credits_parity.py --limit 25 --spot-check 20
```

Parity check for a single high-signal show:

```bash
PYTHONPATH=. python scripts/verify_credits_parity.py --show-id "<uuid>"
```

Cast endpoint verification (expects count/total_count/has_more):

```bash
API_BASE="https://<prod-api-host>"
SHOW_ID="<uuid-known-to-have-cast>"
curl -s "$API_BASE/api/v1/shows/$SHOW_ID/cast?limit=50&offset=0" \
  | jq '{count,total_count,has_more,first:.cast[0]}'
```

Shows list sanity check:

```bash
curl -s "$API_BASE/api/v1/shows?limit=5&offset=0" | jq '.[0]'
```

## Success Criteria

- Cast endpoint returns non-empty data for known populated shows.
- Parity script reports zero mismatches.
- After write enablement, sync jobs complete without fallback warning flood.
- Pagination invariants hold:
  - `count == len(cast)`
  - `has_more == (offset + count < total_count)`

## Rollback Criteria

- Any parity mismatches after read or write enablement.
- API responses empty or malformed for known populated shows.
- Fallback warnings spike after enabling writes.

Rollback action:

```bash
ENABLE_CREDITS_V2_READ=0
ENABLE_CREDITS_V2_WRITE=0
```

## Notes

- Pagination invariants are covered by `tests/test_api_smoke.py`.
- `scripts/verify_credits_parity.py` supports `--show-id` for targeted checks.
