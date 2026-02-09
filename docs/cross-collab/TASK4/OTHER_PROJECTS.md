# Other Projects — Task 4 (Supabase Schema Cleanup)

Repo: TRR-Backend
Last updated: February 9, 2026

## Cross-Repo Snapshot

- TRR-Backend: Complete. Merged via PR #48; staging Supabase up to migrations 0102-0115.
- TRR-APP: Complete. Merged via PR #23; Vercel preview deployed. See TRR-APP TASK4.
- screenalytics: Complete. Pushed to `main`; Supabase-only wiring live. See screenalytics TASK5.

## Responsibility Alignment

- TRR-Backend
  - Writes all Supabase migrations (0106-0113).
  - Writes data migration scripts (6b cast data, 6d shows data).
  - Creates replacement view `core.v_show_cast` before dropping cast tables.
  - Updates all TRR-Backend routers/services/pipeline code.
- TRR-APP
  - Updates cast display queries (5+ sites in `trr-shows-repository.ts`) to use `credits` + `credit_occurrences`.
  - Displays enriched `core.people` multi-source fields.
  - Updates frontend types/interfaces.
- screenalytics
  - Updates `trr_metadata_db.py` to read from `core.v_show_cast` instead of `core.show_cast`.
  - Must complete before TRR-Backend drops `core.show_cast`.

## Dependency Order

### Phase 6c (Cast Model Migration) — Sequential, Cross-Repo

1. TRR-Backend writes replacement view `core.v_show_cast` (credits-based) and deploys.
2. TRR-APP updates cast query sites to use `credits` + `credit_occurrences` (TASK4).
3. screenalytics updates `trr_metadata_db.py` to read from `core.v_show_cast` (TASK5).
4. After ALL consumers are switched, TRR-Backend runs cast data migration (6b) on staging.
5. After verification, TRR-Backend applies DROP migration (0107).

### Independent Phases (Can Run in Parallel)

- Phase 6a (games drop), 6d (shows consolidation), 6e (people enrichment), 6g-6i (social/overrides/reddit) — all independent.
- Phase 6e has a TRR-APP dependency (display code) but not a blocking one; migration can deploy first.

### Blocked Phases

- Phase 6k (legacy image drops) — blocked until all image code migrated.
- Phase 6j (Reddit tables) — blocked until Reddit scraping implemented.

## Locked Contracts (Mirrored)

- Credits model: `{person_id, show_id, credit_category, billing_order, role, ...}`
- Credit occurrences: `{credit_id, episode_id, air_year, credit_text, attributes, is_archive_footage, ...}`
- `most_recent_episode` jsonb: keyed by source (`imdb`, `tmdb`)
- `core.people` multi-source fields: jsonb keyed by source, resolution order tmdb > fandom > manual
- Replacement view `core.v_show_cast`: must maintain `person_id`, `show_id`, `credit_category`, `billing_order`
