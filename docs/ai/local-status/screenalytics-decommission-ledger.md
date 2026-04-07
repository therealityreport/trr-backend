# Screenalytics Decommission Ledger

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-03
  current_phase: "phase 5 screentime runtime retirement captured"
  next_action: "Use this ledger as the canonical record that screentime is backend-only; remaining Screenalytics references are legacy or out of scope."
  detail: self
```

Definitions:
- Live caller: a current TRR-APP route, shim route, backend endpoint, or backend module that is part of the active admin/product flow.
- Live job: a current backend-owned or backend-triggered job path that still has an active TRR consumer.
- Not live: no current app route, backend caller, backend job, or active operator workflow depends on it.

## Definite Rebuild List
- `TRR-APP` `/screenalytics`
- `TRR-APP` `/screenlaytics` redirect compatibility
- `TRR-APP` `/api/admin/trr-api/people/[personId]/gallery/[linkId]/facebank-seed`
- `TRR-APP` `/api/admin/trr-api/cast-screentime/[...path]`
- `TRR-Backend` `api/routers/admin_person_images.py` seed toggle contract
- `TRR-Backend` `api/routers/admin_cast_screentime.py` retained control plane
- `TRR-Backend` `api/routers/admin_face_references.py` internal-admin identity governance surface
- `TRR-Backend` face reference storage and embedding queue state
- `TRR-Backend` retained cast-screentime storage

## Canonical Runtime Ownership After Phase 1
- `ml.analysis_media_upload_sessions`
  - Canonical retained intake session source for cast-screentime uploads and imports.
- `ml.analysis_media_assets`
  - Canonical retained asset identity source for cast-screentime videos across direct upload, external import, YouTube import, and social import.
- `screenalytics.video_assets`
  - Legacy bridge input only. Phase 1 preserves addressability through `ml.analysis_media_assets.legacy_screenalytics_video_asset_id`.

## Canonical Runtime Ownership After Phase 2
- `ml.face_reference_images`
  - Canonical retained face-reference image source for reviewed identity material, duplicate state, and donor bridging.
- `ml.face_reference_embeddings`
  - Canonical retained embedding source for DeepFace ArcFace register/search/verify flows and approved reader consumption.
- `screenalytics.face_bank_images`
  - Donor/bridge input only. Phase 2 preserves deterministic linkage through `ml.face_reference_images.legacy_screenalytics_face_bank_image_id`.

## Canonical Runtime Ownership After Phase 3
- `trr_backend/services/retained_cast_screentime_runtime.py`
  - Canonical backend-owned screentime execution lane for retained runs, artifact persistence, and generated clips.
- `trr_backend/services/retained_cast_screentime_dispatch.py`
  - Canonical runtime gate. Chooses between backend execution and donor HTTP fallback without changing admin routes.
- `screenalytics.apps.api.services.cast_screentime`
  - Donor reference only for screentime execution behavior. No longer the intended primary runtime owner.

## Canonical Runtime Ownership After Phase 4
- `trr_backend/services/retained_cast_screentime_review.py`
  - Canonical backend-owned reviewed-summary and publication-mode layer for screentime runs.
- `api/routers/admin_cast_screentime.py`
  - Canonical backend-owned review and publication read/write surface for the retained admin flow.
- `TRR-APP/apps/web/src/app/admin/cast-screentime/CastScreentimePageClient.tsx`
  - Canonical operator surface for retained screentime review/publication workflows through the existing app route.
- `screenalytics.cast_screentime_publish_versions`
  - Legacy publication lineage still present while Phase 5 removes the remaining rollback-only donor dependency.

## Canonical Runtime Ownership After Phase 5
- `trr_backend/services/retained_cast_screentime_dispatch.py`
  - Backend-only screentime dispatch. The donor HTTP lane is retired from active production use.
- `api/main.py`
  - Deployed screentime runtime validation no longer requires `SCREENALYTICS_SERVICE_TOKEN`.
- `api/screenalytics_auth.py`
  - Legacy compatibility auth only. Accepts internal-admin JWTs and no longer represents a required dedicated screentime service token boundary.
- `TRR-APP/apps/web/src/app/api/admin/trr-api/cast-screentime/[...path]/route.ts`
  - Stable app proxy seam for the backend-only screentime runtime.

## Definite Transitional List
- `trr_backend/clients/screenalytics.py`
  - Transitional adapter until retained vision entry points stop referring to Screenalytics naming.
- `api/screenalytics_auth.py`
  - Transitional only for compatibility routes. Not part of active screentime runtime dependency anymore.
- `api/routers/screenalytics.py`
  - Transitional only for legacy ingest/metadata compatibility paths.
- `api/routers/screenalytics_runs_v2.py`
  - Transitional only for legacy v2 run callback/update compatibility paths.
- `screenalytics.face_bank_images`
  - Transitional donor input only for retained bridge/backfill semantics; no longer an active identity source of truth.
- `screenalytics.cast_screentime_publish_versions`
  - Transitional donor publication lineage only. Phase 4 made the review/publication contract backend-canonical; Phase 5 removes the remaining donor dependency.

## Definite Delete List
- Standalone Screenalytics UI/workspace surface
- Standalone Screenalytics API surface with no live TRR caller
- Screenalytics audio pipeline
- Screenalytics Celery/bootstrap runtime
- `packages/py-screenalytics`
- Generic Screenalytics metadata/sync/router families without a proven live caller

## Screentime Runtime Env Status After Phase 5
- `SCREENALYTICS_API_URL`
  - No longer required for screentime production flows. Keep only for explicit legacy outbound callers.
- `SCREENALYTICS_SERVICE_TOKEN`
  - No longer required for screentime production flows. Keep only for explicit legacy compatibility callers that still use service-token auth.

## Definite Legacy Tables To Stop Treating As Active
- `screenalytics.media_upload_sessions`
- `screenalytics.video_assets`
- `screenalytics.video_asset_cast_candidates`
- `screenalytics.face_bank_images`
- `screenalytics.runs_v2`
- `screenalytics.run_artifacts`
- `screenalytics.run_person_metrics`
- `screenalytics.cast_screentime_segments`
- `screenalytics.cast_screentime_evidence`
- `screenalytics.cast_screentime_excluded_sections`
- `screenalytics.cast_screentime_publish_versions`
- `screenalytics.cast_screentime_reference_fingerprints`
- `screenalytics.cast_screentime_suggestion_decisions`
- `screenalytics.cast_screentime_unknown_review_state`
- `screenalytics.unknown_clusters`

## Definite Routes/Modules To Archive Or Delete
- `screenalytics/apps/workspace-ui`
- `screenalytics/web`
- `screenalytics/apps/api/jobs_audio.py`
- `screenalytics/apps/api/routers/audio.py`
- `screenalytics/apps/api/celery_app.py`
- `screenalytics/apps/api/main.py`
- `screenalytics/packages/py-screenalytics`
