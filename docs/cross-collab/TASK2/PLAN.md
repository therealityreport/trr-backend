# Show-Level SYNC Facebank Seeds — Task 2 Plan

Repo: TRR-Backend  
Last updated: February 6, 2026

Goal
Add a show-level SYNC flow that imports TRR cast face images into SCREENALYTICS `person` + `face_bank_images`.

Status Snapshot (As of February 6, 2026)
- Not yet implemented. (Task 2 tracks the show-level SYNC work after Task 1 contract closeout.)

Locked Day-1 Scope (Import-Only)
- Day 1 is **import-only into `person` + `face_bank_images`**.
- Day 1 does **not** run the crop/enhance/embedding seed pipeline and does **not** produce facebank manifests/embeddings.

Locked Contracts (Day 1)
1. Endpoint: `POST /shows/{show_id}/sync_facebank_seeds`
   - Placement: SCREENALYTICS API `apps/api/routers/cast.py` (avoid additional “shows v2” surface)
2. Auth (required):
   - Header: `X-Screenalytics-Admin-Secret`
   - Env: `SCREENALYTICS_ADMIN_SECRET`
   - `503` if env missing/unset
   - `403` if header missing/wrong
3. Request body: `{ "max_images_per_person"?: int }`
   - Default: `5`
   - Clamp: `1–20`
4. Response body:
   - `status: ok|partial|error`
   - Counts + per-person results (no secret leakage)
5. TRR cast source:
   - Read-only query of TRR metadata DB `core.show_cast` joined to `core.people`
   - IMDb person ID read from `core.people.external_ids->>'imdb'` (nullable)
   - Internal default filter: `credit_category='Self'` (not request-exposed Day 1)

Implementation Outline (Day 1)
1. Load local show record and require it be TRR-linked (`trr_show_id` present).
2. Fetch TRR show cast from TRR metadata DB (`core.show_cast`).
3. Match or auto-create local cast members:
   - Strict match order: `trr_person_id`, then `imdb_id`, then exact normalized name/alias.
   - Auto-create with conservative defaults: `role="other"`, `status="active"`.
   - Persist `trr_person_id` and set `person_id == trr_person_id` (Day 1) to align with existing ingest expectations.
4. For each matched/created TRR person, import photos using the existing ingestion helper which enforces:
   - Seed-only-first fetch behavior + strict fallback
   - Append-only dedupe (idempotent re-runs)
5. Continue on per-person failures:
   - Return `status="partial"` with per-person error entries when some imports fail.

Deferred Scope (Not Day 1)
- Crop/enhance/embedding seed pipeline reuse.
- Overrides / non-`Self` credit categories (request-configurable).
- Any fuzzy matching logic (Day 1 is exact-only).
- Any browser-visible admin secret handling (client must not receive secret).

