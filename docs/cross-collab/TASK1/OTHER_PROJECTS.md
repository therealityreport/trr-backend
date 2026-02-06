# Other Projects — Task 1

This repo (TRR-Backend) owns schema and API contracts for facebank seed flagging.

Shared Cross-Repo Contract
- Toggle endpoint: `PATCH /api/v1/admin/person/{person_id}/gallery/{link_id}/facebank-seed`
- Toggle payload: `{ "facebank_seed": boolean }`
- Screenalytics read endpoint: `GET /api/v1/screenalytics/people/{person_id}/photos?seed_only={bool}`
- Shared field: `core.media_links.facebank_seed` (also exposed via views)

Auth Contract
- Human admins: allowlist-only via `ADMIN_EMAIL_ALLOWLIST`.
- Internal app proxy: `service_role` accepted only for facebank toggle when header
  `X-TRR-Internal-Admin-Secret` matches `TRR_INTERNAL_ADMIN_SHARED_SECRET`.
- `FACEBANK_SEED_LOCAL_BYPASS` is not part of Task 1 final contract.

TRR-APP Responsibilities
- Show `facebank_seed` state in person gallery UI.
- Add seed toggle for media-link-backed photos (`origin === "media_links"` and `link_id` exists).
- Call backend via internal proxy route with:
  - `Authorization: Bearer ${TRR_CORE_SUPABASE_SERVICE_ROLE_KEY}`
  - `X-TRR-Internal-Admin-Secret: ${TRR_INTERNAL_ADMIN_SHARED_SECRET}`

SCREENALYTICS Responsibilities
- Fetch seeded images first via `seed_only=true`.
- Fall back to `seed_only=false` only when seeded result set is empty.
- Use backend `served_url` and preserve backend ordering.

Touchpoints
- Backend toggle endpoint (admin)
- Backend screenalytics photos endpoint (`seed_only`)
- Supabase views:
  - `core.v_person_images`
  - `core.v_person_images_served_media_v2`

Dependency Order
1. Backend hardening + tests
2. App proxy/UI
3. Screenalytics seed-first integration
