# Fandom person gallery confessional-only cleanup

Last updated: 2026-03-17

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-17
  current_phase: "real-housewives Fandom person media now only admits confessional/intro inventory, and legacy promo/reunion/theme rows were removed"
  next_action: "Monitor the next person-image refresh and confirm only confessional/intro Fandom rows can re-enter when NBCUMV lacks coverage"
  detail: self
```

- Root cause was split across live data and code:
  - legacy `core.cast_photos` / `core.media_assets` rows still contained RH Fandom promo, reunion, theme-song, finale, and other non-confessional/non-intro images
  - `fetch_fandom_person_cast_photos(...)` did not enforce the RH Fandom allowlist, so unsupported rows could still enter through the person-page source even though gallery imports had already been narrowed
- `trr_backend/ingestion/cast_photo_sources.py` now classifies RH Fandom rows from all available text signals, including filenames and URLs, and only keeps inferred `CONFESSIONAL` / `INTRO` rows for `real-housewives.fandom.com`.
- `scripts/import/import_fandom_gallery_photos.py` now uses the same RH-only keep rule so manual imports cannot reintroduce promo/reunion/theme rows.
- Added `scripts/cleanup/cleanup_non_confessional_fandom_person_media.py` for dry-run/apply cleanup of stale RH Fandom person-gallery inventory:
  - targets `core.cast_photos` rows that are RH Fandom sourced but not inferred as confessional/intro
  - deletes linked `core.media_links` / `core.media_assets` rows when the linked asset is only referenced by the targeted cast-photo rows
  - deletes hosted object keys discovered from the cast-photo/media rows and metadata-hosted variant URLs
- Live cleanup result on 2026-03-17:
  - deleted `16` stale `core.cast_photos` rows
  - deleted `16` linked `core.media_links` rows
  - deleted `16` linked `core.media_assets` rows
  - deleted `84` hosted objects
  - verified the reported Lisa Barlow promotional portrait row no longer exists in either `core.cast_photos` or `core.media_assets`
- Focused validation passed:
  - `pytest tests/ingestion/test_cast_photo_sources_fandom.py tests/ingestion/test_fandom_person_scraper.py -q`
  - `ruff check trr_backend/ingestion/cast_photo_sources.py scripts/import/import_fandom_gallery_photos.py scripts/cleanup/cleanup_non_confessional_fandom_person_media.py tests/ingestion/test_cast_photo_sources_fandom.py tests/ingestion/test_fandom_person_scraper.py`
  - post-cleanup dry-run: `PYTHONPATH=. python scripts/cleanup/cleanup_non_confessional_fandom_person_media.py` -> `target_cast_photos=0`
