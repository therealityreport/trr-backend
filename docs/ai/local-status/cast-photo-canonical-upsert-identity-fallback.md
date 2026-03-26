# Cast photo canonical upsert identity fallback

Last updated: 2026-03-24

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-24
  current_phase: "archived continuity note"
  next_action: "See newer continuity notes if follow-up is needed"
  detail: self
```

- Root cause: `core.upsert_cast_photos_by_canonical` only handled conflicts on `(person_id, source, image_url_canonical)`, but legacy Fandom rows still existed with matching `(person_id, source, source_image_id)` and differently normalized `image_url_canonical` values.
- Evidence from Supabase: the reported row for person `32ddc0a5-2bea-4a62-ba53-eda033af8efd` already existed under `source_image_id = fandom-gallery-19cddd39db52b22a`, and `1354` Fandom rows currently store uppercase canonical URLs (`164` of them are `fandom-gallery-*` rows).
- Fix: migration `0198_cast_photo_canonical_upsert_identity_fallback.sql` is now applied on the live Supabase project, so `core.upsert_cast_photos_by_canonical` normalizes canonical URLs in SQL and falls back to updating an existing row by `source_image_id` before insert.
- Validation:
  - `pg_get_functiondef('core.upsert_cast_photos_by_canonical(jsonb)'::regprocedure)` now shows the `matched_id` fallback path and `_normalize_cast_photo_canonical_url(...)` normalization logic live in Supabase.
  - A rollback-safe probe against Lisa Barlow (`person_id = 32ddc0a5-2bea-4a62-ba53-eda033af8efd`, `source_image_id = fandom-gallery-19cddd39db52b22a`) completed without the old `cast_photos_person_source_source_image_id_key` duplicate-key failure.
  - Local targeted regression check passed: `PYTHONPATH=. ./.venv/bin/pytest tests/repositories/test_cast_photos_upsert.py -q` (`9 passed`).
