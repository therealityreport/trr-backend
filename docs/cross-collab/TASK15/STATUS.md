# Status — Task 15 (Credits slug and IMDb refresh)

Repo: TRR-Backend
Last updated: 2026-03-30

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Implementation | Completed | IMDb full-credits crew ingest, cast-only view migration, and show credits endpoint added. |

## Blockers
- None.

## Recent Activity
- 2026-03-30: Task scaffolding created.
- 2026-03-30: Added allowlisted IMDb crew parsing, safe refresh ingestion, cast-only `core.v_show_cast`, and `GET /api/v1/admin/trr-api/shows/{show_id}/credits`.
- 2026-03-30: Validated backend changes with `pytest -q tests/integrations/imdb/test_fullcredits_cast_parser.py` and targeted `ruff check`.
