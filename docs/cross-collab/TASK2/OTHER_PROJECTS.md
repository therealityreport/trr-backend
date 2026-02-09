# Other Projects — Task 2

Repo: TRR-Backend  
Last updated: February 9, 2026

Scope Clarification (Task 1 vs Task 2)
- Task 1: seed flagging + `seed_only` contract + strict fallback + episode-level import side effects (completed).
- Task 2: show-level SYNC endpoint + UI + orchestration (tracked here).

Cross-Repo Responsibilities (Day 1)
- TRR-Backend
  - Ensures TRR metadata DB data is reliable for read-only consumption:
    - `core.v_show_cast`
    - `core.people.external_ids->>'imdb'`
  - No Day-1 API/UI changes required for Task 2.
- TRR-APP
  - No Day-1 work required for Task 2 (SYNC lives in SCREENALYTICS UIs).
- SCREENALYTICS
  - Owns `POST /shows/{show_id}/sync_facebank_seeds` endpoint, admin-secret guard, cast matching/auto-create, and import side effects (`person`, `face_bank_images`).
  - Wires Streamlit and Next.js UIs without leaking the admin secret to the browser.

Known Preconditions / Risks
- `SCREENALYTICS_ADMIN_SECRET` must be set in environments where SYNC runs.
- TRR metadata DB must be reachable from SCREENALYTICS API for show-cast query.
- Local/staging DB must have `person` and `face_bank_images` tables to avoid persistence failures.
- Day 1 is import-only: no facebank manifests/embeddings are produced yet.
