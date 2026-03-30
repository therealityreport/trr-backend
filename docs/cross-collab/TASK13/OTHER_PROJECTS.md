# Other Projects — Task 13 (Show refresh full pipeline gallery media)

Repo: TRR-Backend
Last updated: 2026-03-27

## Cross-Repo Snapshot
- TRR-Backend: Completed backend contract and stream behavior for gallery-only photo refresh.
- TRR-APP: Consumed new contract and added gallery phase to full refresh.
- screenalytics: Not touched; no dependency required for this change.

## Responsibility Alignment
- TRR-Backend
  - Own `skip_cast_photos` request contract and stream-stage skip behavior.
  - Preserve show/season/episode gallery refresh while avoiding duplicate cast-photo work.
- TRR-APP
  - Call unified refresh first, then run gallery media fast pass with `skip_cast_photos=true`.
  - Keep header button modal-only and make Health Center entry copy explicit.
- screenalytics
  - No change required.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Keep shared contracts aligned with owning repo PLAN.md.
- `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream` now accepts `skip_cast_photos?: boolean`.
