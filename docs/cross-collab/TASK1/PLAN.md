# Facebank Seed Flagging — Task 1 Plan

Repo: TRR-Backend

Goal
Flag person gallery images as facebank seed candidates so SCREENALYTICS can bootstrap videos with no existing episodes/facebank.

Status
- Implementation in progress.

Status Matrix
| Repo | Status | Notes |
| --- | --- | --- |
| TRR-Backend | In progress | Endpoint/auth hardening + tests in progress |
| TRR-APP | In progress | Proxy endpoint + UI toggle pending |
| SCREENALYTICS | In progress | Seed-first fetch + fallback pending |

Locked Contracts
1. Admin toggle endpoint remains `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`.
2. Request payload remains `{ "facebank_seed": boolean }`.
3. Response payload remains `{ "link_id": string, "person_id": string, "facebank_seed": boolean }`.
4. `facebank_seed` lives on `core.media_links` and is exposed in `core.v_person_images` and `core.v_person_images_served_media_v2`.
5. SCREENALYTICS photo contract remains `GET /api/v1/screenalytics/people/{person_id}/photos?seed_only={bool}`.
6. `seed_only=true` returns only flagged rows; fallback to unfiltered is client-side in SCREENALYTICS.

Auth Contract (Task 1)
- Human path: allowlisted Supabase user JWT (`ADMIN_EMAIL_ALLOWLIST`) can toggle seed flags.
- Internal app proxy path: `service_role` JWT is allowed for this endpoint only when both are true:
  - Header `X-TRR-Internal-Admin-Secret` matches env `TRR_INTERNAL_ADMIN_SHARED_SECRET`.
  - Request is routed through TRR-APP server proxy that already passed `requireAdmin`.
- `FACEBANK_SEED_LOCAL_BYPASS` is removed from Task 1 contract.

Backend Scope
- Keep existing endpoint path/payload/schema contracts unchanged.
- Harden endpoint-scoped auth for facebank seed toggle.
- Preserve default admin behavior for all other endpoints.
- Keep migration and views as-is (already in migration `0100_facebank_seed_media_links.sql`).

Dependencies
1. TRR-Backend auth + tests
2. TRR-APP proxy + UI
3. SCREENALYTICS seed-first ingestion

Rollout Sequence
1. Deploy TRR-Backend auth hardening + tests.
2. Deploy TRR-APP proxy + gallery toggle UI.
3. Deploy SCREENALYTICS seed-first + fallback fetch logic.
4. Run cross-repo smoke:
   - Toggle seed in TRR-APP
   - Verify backend `seed_only=true` returns seeded subset
   - Verify SCREENALYTICS falls back to `seed_only=false` when seeded subset is empty

Acceptance Scenarios
- Allowlisted user can toggle seed.
- `service_role` without internal secret header is rejected.
- `service_role` with invalid internal secret header is rejected.
- `service_role` with valid internal secret header is accepted.
- 404/409/502 behavior is explicit and tested for toggle endpoint.
