# Other Projects — Task 1

This repo (TRR-Backend) owns the database and API surface for facebank seed flagging.

TRR App responsibilities
- Display person gallery images and current `facebank_seed` state
- Provide UI toggle to set/unset `facebank_seed`
- Call backend allowlist-only endpoint to update the flag

Screenalytics responsibilities
- Fetch seed images via backend screenalytics endpoint with `seed_only=true`
- Fall back to all gallery images when no seeds are flagged
- Use `served_url` as the seed image source

Touchpoints
- Backend admin endpoint: toggle `facebank_seed` for a person gallery image
- Backend screenalytics endpoint: `seed_only` filter support
- Supabase view: `core.v_person_images_served_media_v2` (includes `facebank_seed`)

Ownership
- Backend owns schema, views, and endpoints
- TRR App owns UI and user workflow
- Screenalytics owns seed selection behavior
