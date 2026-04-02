# Screenalytics Decommission Ledger

Date: 2026-04-02

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
- `TRR-Backend` face reference storage and embedding queue state
- `TRR-Backend` retained cast-screentime storage

## Definite Transitional List
- `trr_backend/clients/screenalytics.py`
  - Transitional adapter until retained vision entry points stop referring to Screenalytics naming.
- `trr_backend/clients/screenalytics_cast_screentime.py`
  - Transitional dispatch shim until retained cast-screentime execution no longer depends on the external Screenalytics runtime.
- `api/screenalytics_auth.py`
  - Transitional only while legacy internal callback/service-token paths remain active.
- `api/routers/screenalytics.py`
  - Transitional only for legacy internal ingest/metadata paths.
- `api/routers/screenalytics_runs_v2.py`
  - Transitional only for legacy v2 run callback/update paths.

## Definite Delete List
- Standalone Screenalytics UI/workspace surface
- Standalone Screenalytics API surface with no live TRR caller
- Screenalytics audio pipeline
- Screenalytics Celery/bootstrap runtime
- `packages/py-screenalytics`
- Generic Screenalytics metadata/sync/router families without a proven live caller

## Definite Env Vars To Remove
- `SCREENALYTICS_API_URL`
- `SCREENALYTICS_SERVICE_TOKEN`

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
