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
- Task 1 is not closed yet because staging + production rollout smoke (with DB side effects) has not been completed end-to-end.

Status Matrix
| Repo | Code/CI | Rollout Smoke | Notes |
| --- | --- | --- | --- |
| TRR-Backend | Complete | In progress | Local auth + `seed_only` contract smoke passed |
| TRR-APP | Complete | In progress | Proxy + UI path merged; live env smoke still pending |
| SCREENALYTICS | Complete | In progress | Seed-first + strict fallback code merged; live DB side-effect smoke pending |

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

Open Blockers (Preventing Final Closeout)
1. Staging and production deploy verification still requires environment access and authenticated service tooling.
2. Local env files are missing required Task 1 secrets for true app-proxy/live smoke:
   - TRR-APP missing `TRR_INTERNAL_ADMIN_SHARED_SECRET` locally.
   - TRR-Backend missing `TRR_INTERNAL_ADMIN_SHARED_SECRET` and `SCREENALYTICS_SERVICE_TOKEN` locally.
   - SCREENALYTICS missing `SCREENALYTICS_SERVICE_TOKEN` locally.
3. SCREENALYTICS local `sync_cast_from_trr` import hook currently fails persistence on local `DB_URL` with:
   - `relation "person" does not exist`
   - This is an environment DB-schema mismatch (not a locked-contract regression).

Remaining Steps To Complete Task 1
1. Staging rollout in order: backend -> app -> screenalytics.
2. Staging completion-gate smoke:
   - TRR-APP allowlisted admin toggle ON/OFF.
   - Backend auth guard curl checks (403/403/200).
   - Backend `seed_only=true/false` behavior check.
   - Screenalytics `sync_cast_from_trr` seeded vs unseeded runs.
   - DB assertions on `face_bank_images` (`is_seed=true`, `approved=false`, dedupe confirmed).
3. Production rollout in same order + minimal confirmation smoke.
4. After both environments pass, update Task 1 docs in all repos from "in progress" to "completed" with evidence links.

Acceptance Criteria (Final Gate)
1. Admin toggle works through TRR-APP proxy for allowlisted users.
2. Backend rejects service-role calls without valid internal secret.
3. Backend `seed_only` filtering behavior is correct in live env.
4. Screenalytics strict fallback behavior is observed in live logs.
5. `face_bank_images` side effects are correct and deduped.
6. Task 1 docs are synchronized and marked completed across all repos.
