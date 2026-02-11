# Bravo Import + Cast Eligibility + Videos/News — Task 7 Plan

Repo: TRR-Backend
Last updated: February 11, 2026

## Goal

Add Bravo parsing + persisted snapshot APIs for admin import/sync, and support TRR-APP rendering from persisted Bravo payloads (no live runtime scraping).

## Scope

1. Add Bravo source migration (`0117_add_bravo_source.sql`).
2. Add Bravo parser service under `trr_backend/scraping/` for:
- show details + cast URLs + image candidates + airs text
- show videos/news
- person bio/hero/social + person videos/news
3. Add admin Bravo endpoints:
- `POST /api/v1/admin/shows/{show_id}/import-bravo/preview`
- `POST /api/v1/admin/shows/{show_id}/import-bravo/commit`
- `GET /api/v1/admin/shows/{show_id}/bravo/videos`
- `GET /api/v1/admin/shows/{show_id}/bravo/news`
4. Persist show/person snapshots in:
- `core.show_source_latest/history` (`source_id='bravo'`)
- `core.person_source_latest/history` (`source_id='bravo'`)
5. Commit behavior:
- update `core.shows.description`
- update people `biography/homepage/profile_image_url` with `['bravo']`
- merge social handles into `external_ids` fill-missing-only
- import selected show images through existing admin scrape pipeline

## Out of Scope

- schema expansion for a dedicated `airs` column
- screenalytics code changes

## Acceptance Criteria

1. Bravo preview/commit endpoints return expected payload shapes.
2. Bravo videos/news endpoints read persisted snapshots only.
3. Show/person snapshot rows are written to latest + history tables.
4. Social merge does not overwrite non-empty existing handles.
5. Backend targeted tests and lint pass.
