# Cast Screentime Phase 1 Asset Contract Freeze

Date: 2026-04-02

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-02
  current_phase: "phase 1 asset contract freeze implemented"
  next_action: "port retained execution and DeepFace identity work onto the canonical ml asset contract"
  detail: self
```

## Summary

Phase 1 freezes the retained cast-screentime asset contract on `ml.*` without changing the execution boundary to the standalone `screenalytics` runtime.

## Canonical Intake Contract

- Admin intake routes stay unchanged:
  - `POST /api/v1/admin/cast-screentime/upload-sessions`
  - `POST /api/v1/admin/cast-screentime/upload-sessions/{id}/complete`
  - `POST /api/v1/admin/cast-screentime/video-assets/import`
- All source modes now target the same canonical asset surface in `ml.analysis_media_upload_sessions` and `ml.analysis_media_assets`.
- Returned asset payloads are normalized around:
  - `media_type`
  - `media_kind`
  - `video_class`
  - `promo_subtype`
  - `source_import_type`
  - `source_json`
  - `metadata`
  - `duration_seconds`

## Legacy Asset Bridge

- `ml.analysis_media_assets.legacy_screenalytics_video_asset_id` is the explicit bridge to `screenalytics.video_assets.id`.
- Phase 1 backfills legacy `screenalytics.video_assets` rows into `ml.analysis_media_assets`.
- Backfill scope is asset-only:
  - owner linkage
  - classification
  - provenance
- Phase 1 does not backfill legacy runs, review state, or publications.

## Artifact Registry Freeze

Backend-owned retained artifact registry now freezes the keys future phases must preserve:

- `shots.json`
- `segments.json`
- `scenes.json`
- `excluded_sections.json`
- `person_metrics.json`
- `title_card_candidates.json`
- `title_card_reference_signatures.json`
- `confessional_candidates.json`
- `cast_suggestions.json`
- `unknown_review_queues.json`
- `reference_fingerprints.json`

Schema-version ownership for these retained artifacts now lives in backend code, not scattered router literals.

## Explicitly Not Changed

- `retained_cast_screentime_dispatch` still points at the external `screenalytics` runtime.
- `SCREENALYTICS_API_URL` and `SCREENALYTICS_SERVICE_TOKEN` are still transitional dependencies.
- TRR-APP admin routes and `/screenalytics` parity surfaces are unchanged in this phase.
