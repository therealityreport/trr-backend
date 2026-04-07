# Cast Screentime Phase 2 Identity Reset

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-03
  current_phase: "phase 2 identity reset implemented"
  next_action: "port retained screentime execution onto the reviewed face-reference and DeepFace embedding contract"
  detail: self
```

## Summary

Phase 2 moves retained face-reference governance into `TRR-Backend` so approved identity material, donor bridging, and DeepFace-backed register/search/verify flows all live on backend-owned `ml.*` state.

## Canonical Face-Reference Ownership

- `ml.face_reference_images` is now the canonical retained face-reference table.
- `ml.face_reference_images.review_status` is the operator-owned state machine:
  - `pending_review`
  - `approved`
  - `rejected`
  - `duplicate`
- `approved=true` is now a governance outcome, not an enrollment default.
- The existing gallery `facebank_seed` toggle remains enrollment input only. It does not auto-promote a row into the active matching seed set.

## Legacy Donor Bridge

- `ml.face_reference_images.legacy_screenalytics_face_bank_image_id` is the explicit bridge to `screenalytics.face_bank_images.image_id`.
- Phase 2 backfills donor facebank rows only when linkage to a TRR gallery media link is deterministic.
- `screenalytics.face_bank_images` is now donor/bridge input, not an active face-reference source of truth.

## DeepFace ArcFace Contract

- Backend-owned identity operations use `DeepFace` with the v1 ArcFace-class contract:
  - provider: `deepface`
  - model: `ArcFace`
  - detector: `retinaface`
  - normalization: `base`
  - dimensions: `512`
  - contract key: `deepface:arcface:retinaface:base:512d:l2_unit`
- `ml.face_reference_embeddings` stores this provenance in `metadata.contract_key` together with provider/model metadata.
- Retained readers such as `people_count_engine.py` filter to:
  - approved references
  - active references
  - `review_status = 'approved'`
  - `embedding_status = 'ready'`
  - the explicit DeepFace ArcFace contract key

## Admin Surface

- New backend-owned admin routes now cover:
  - list-by-person
  - review / approve / reject / duplicate
  - search
  - verify
  - re-embed
- TRR-APP was intentionally left unchanged in Phase 2 because the new backend surface is admin-internal and parity-preserving for the existing seed toggle.

## Explicitly Not Changed

- Cast-screentime execution still runs through the transitional `screenalytics` dispatch boundary.
- `SCREENALYTICS_API_URL` and `SCREENALYTICS_SERVICE_TOKEN` still exist for execution/runtime phases.
- Phase 2 does not yet move screentime run computation, artifact generation, or publication state off the external runtime.
