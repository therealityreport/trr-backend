# Facebank Seed Flagging — Task 1 Plan

Repo: TRR-Backend  
Last updated: February 6, 2026

Goal
Allow admins to flag person gallery images as facebank seed candidates for SCREENALYTICS and keep downstream ingestion contract-safe.

Status Snapshot (As of February 6, 2026)
- TRR-Backend PR `#44` merged.
- TRR-APP PRs `#18` and `#19` merged.
- SCREENALYTICS PR `#187` merged.
- Code and CI are complete across all three repos.
- Staging + production rollout smoke (including DB side effects) has been completed end-to-end.
- Task 1 is complete.

Status Matrix
| Repo | Code/CI | Rollout Smoke | Notes |
| --- | --- | --- | --- |
| TRR-Backend | Complete | Complete | Prod auth + `seed_only` contract smoke passed |
| TRR-APP | Complete | Complete | Prod proxy path verified end-to-end |
| SCREENALYTICS | Complete | Complete | `sync_cast_from_trr` import hook verified + DB writes + dedupe |

Locked Contracts (Final)
1. Backend toggle endpoint: `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`.
2. Toggle payload: `{ "facebank_seed": boolean }`.
3. Toggle response: `{ "link_id": string, "person_id": string, "facebank_seed": boolean }`.
4. Screenalytics photos endpoint: `GET /api/v1/screenalytics/people/{person_id}/photos?seed_only={bool}`.
5. Service auth for app proxy path: `Authorization: Bearer <service_role>` + `X-TRR-Internal-Admin-Secret`.
6. Screenalytics must use `served_url` and strict fallback: only call `seed_only=false` after successful empty `seed_only=true`.

Execution Evidence Completed
1. Targeted regression tests passed:
   - TRR-Backend: `python -m pytest tests/api/routers/test_admin_person_images.py tests/api/test_screenalytics_ingest_endpoints.py -q` -> `22 passed`.
   - TRR-APP: `pnpm -C apps/web test -- tests/facebank-seed-proxy-route.test.ts tests/facebank-seed-state.test.ts tests/person-photo-utils.test.ts` -> all tests passed.
   - SCREENALYTICS: `python -m pytest tests/unit/test_trr_ingest.py tests/api/test_sync_cast_from_trr.py -q` -> `10 passed`.
2. Backend local contract smoke passed:
   - `403` without internal secret.
   - `403` with wrong internal secret.
   - `200` with valid internal secret.
   - `seed_only=true` returned seeded subset; `seed_only=false` returned superset.
3. Strict fallback request pattern confirmed in backend logs:
   - Seeded person: only `seed_only=true` call.
   - Unseeded person: `seed_only=true` then `seed_only=false`.
4. Production backend auth guard smoke (service role + internal secret):
   - `403` without internal secret header.
   - `403` with wrong internal secret.
   - `200` with correct internal secret (toggle succeeded).
5. Production TRR-APP proxy smoke (allowlisted admin):
   - Proxy returned `200` for toggle OFF then ON (end-to-end through app -> backend).
6. Production `seed_only` filtering contract smoke:
   - Seeded person: `seed_only=true` returned `1` row; `seed_only=false` returned `20` rows (superset).
   - Unseeded person: `seed_only=true` returned `0` rows; `seed_only=false` returned `10` rows.
7. Production strict fallback observed in backend logs (February 6, 2026):
   - Seeded person: only `seed_only=true` request.
   - Unseeded person: `seed_only=true` then `seed_only=false`.
8. SCREENALYTICS `sync_cast_from_trr` smoke (local API + local Postgres DB via `tools/dev-up.sh`):
   - Seeded run: `imported_facebank_images=1`, `imported_facebank_people=1`, `import_errors=0`.
   - Unseeded run: `imported_facebank_images=5`, `imported_facebank_people=1`, `import_errors=0`.
   - Seeded rerun: `imported_facebank_images=0` (dedupe confirmed).
9. SCREENALYTICS DB side effects (local Postgres via docker compose):
   - Inserted `face_bank_images` rows have `is_seed=true`, `approved=false`.
   - Rerun did not create duplicates.

Open Blockers (Preventing Final Closeout)
- None.

Remaining Steps To Complete Task 1
- None. Completed February 6, 2026.

Completion Metadata
- Completion date: February 6, 2026
- PR references:
  - TRR-Backend `#44`
  - TRR-APP `#18`, `#19`
  - SCREENALYTICS `#187`
- Smoke IDs used:
  - Seeded toggle: `person_id=551c8e8f-9714-43ef-a93b-26400eeffb91`, `link_id=1b3cb751-e6d0-594c-acdb-57958c463495`
  - Unseeded strict-fallback check: `person_id=a9df125f-f210-4dbd-8ca5-244c2c9a886c`

Acceptance Criteria (Final Gate)
1. Admin toggle works through TRR-APP proxy for allowlisted users.
2. Backend rejects service-role calls without valid internal secret.
3. Backend `seed_only` filtering behavior is correct in live env.
4. Screenalytics strict fallback behavior is observed in live logs.
5. `face_bank_images` side effects are correct and deduped.
6. Task 1 docs are synchronized and marked completed across all repos.
