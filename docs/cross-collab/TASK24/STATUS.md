# Status — Task 24 (Final Supabase connection audit and donor transition inventory)

Repo: TRR-Backend
Last updated: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-03
  current_phase: "phase 5 screentime runtime retirement implemented"
  next_action: "treat the inventory as historical donor context; active screentime runtime dependency is retired"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Canonical runtime contract audit | Complete | Backend runtime DB contract already correct; active ambiguity lived in screenalytics and app docs |
| 2 | screenalytics transition-runtime wording cleanup | Complete | Updated runtime/operator messaging to reflect `TRR_DB_URL` + `TRR_DB_FALLBACK_URL` |
| 3 | TRR-APP env contract cleanup | Complete | Removed stale app `SCREENALYTICS_API_URL` env example and clarified Supabase env roles |
| 4 | Donor transition inventory | Complete | Final audit matrix, app-facing dependency list, and donor file list captured in `PLAN.md` |

## Blockers
- None for the audit itself.
- Follow-on work: record one real-media backend-only sanity run and continue any non-screentime donor cleanup outside this task.

## Recent Activity
- 2026-04-02: Ran `scripts/handoff-lifecycle.sh pre-plan` and scaffolded `TRR-Backend TASK24`, `screenalytics TASK13`, and `TRR-APP TASK23`.
- 2026-04-02: Audited backend DB resolution, JWT verification, screenalytics transition runtime, and app-facing Supabase env usage.
- 2026-04-02: Updated screenalytics runtime/operator messages to stop implying `TRR_DB_URL` is the only valid runtime source.
- 2026-04-02: Updated TRR-APP env/docs to classify server-side Supabase auth envs correctly and removed stale `SCREENALYTICS_API_URL` from the app env example.
- 2026-04-02: Phase 1 asset-contract work landed in `TRR-Backend`; `ml.analysis_media_assets` is now the canonical screentime asset identity surface and `screenalytics.video_assets` is treated as legacy bridge input only.
- 2026-04-03: Phase 2 identity-contract work landed in `TRR-Backend`; `ml.face_reference_images` / `ml.face_reference_embeddings` are now canonical retained identity sources and `screenalytics.face_bank_images` is donor/bridge input only.
- 2026-04-03: Phase 3 backend execution port landed in `TRR-Backend`; retained screentime execution and generated clips now run through backend-owned runtime code behind a reversible dispatch gate.
- 2026-04-03: Phase 4 review/publication cutover landed in `TRR-Backend`; reviewed screentime summaries, supplementary internal-reference publication, and publication-mode lineage are now backend-canonical for the retained admin flow.
- 2026-04-03: Phase 5 runtime retirement landed in `TRR-Backend`; screentime dispatch is backend-only, `SCREENALYTICS_SERVICE_TOKEN` is no longer a required screentime runtime secret, and the old Screenalytics-tagged backend routes are compatibility surfaces rather than active runtime dependencies.
