# Facebank Seed Flagging — Task 1 Plan

Repo: TRR-Backend

Goal
Flag person gallery images as facebank seed candidates so Screenalytics can bootstrap videos with no existing episodes/facebank.

Locked Decisions
- Flag type: `facebank_seed` boolean on `core.media_links`
- Screenalytics behavior: seed-preferred (use flagged if any; otherwise fallback to all)
- TRR App data path: Supabase read + backend update endpoint
- Admin auth: allowlist-only for new admin endpoints

Backend TODOs
- Add DB migration: `core.media_links.facebank_seed boolean not null default false`
- Update views: `core.v_person_images` and `core.v_person_images_served_media_v2` to include `facebank_seed`
- Add allowlist-only admin auth dependency
- Add admin endpoint to toggle `facebank_seed` for person gallery `media_links` rows
- Add `seed_only` filter on `/api/v1/screenalytics/people/{person_id}/photos`
- Update schema/API docs to reflect the new field and endpoint behavior

Constraints
- No code changes in this phase beyond documentation
- Keep existing admin auth behavior untouched for other endpoints

Coordination
- Update cross-collab docs first
- Implement backend changes before app UI and Screenalytics consumption
