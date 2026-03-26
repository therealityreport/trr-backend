# YouTube Shorts week inclusion precise timestamp recovery

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

- Hardened YouTube Shorts timestamp recovery in `trr_backend/socials/youtube/scraper.py` so exact publish time is recovered from both `/watch` and `/shorts` pages, including JSON-LD, itemprop, and microformat signals.
- Updated `_ingest_youtube(...)` in `trr_backend/repositories/social_season_analytics.py` so Shorts with epoch/missing timestamps are repaired before bounded week filtering excludes them, and rediscovered Shorts can heal existing epoch-dated rows that were previously invisible to the date-scoped lookup.
- Added backend-only retrieval diagnostics for Shorts timestamp behavior, including `shorts_candidates_found`, precise publish attempts/successes/failures, `shorts_epoch_rows_repaired`, and `shorts_undated_skipped`.
- Added `scripts/socials/repair_youtube_short_timestamps.py` for targeted historical repair of epoch-dated Shorts without rerunning full comment/media ingestion.
- Normalized YouTube Shorts text fields so the caption is persisted and served as the main post text while the fake Shorts `title` field is cleared.
- Transcript fetch no longer hard-depends on `yt-dlp`; the scraper now reads caption tracks directly from the YouTube watch/Shorts page player response first and only falls back to `yt-dlp` metadata when needed.
- Focused validation passed:
  - `tests/socials/test_comment_scraper_fixes.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/scripts/test_repair_youtube_short_timestamps.py`
  - `ruff check` and `py_compile` on the touched Shorts files
- Live RHOSLC S6 validation:
  - Shorts rows in `social.youtube_videos`: `248`
  - Remaining epoch-dated Shorts rows: `0`
  - Repair script dry-run for RHOSLC S6 now returns `0` candidate rows
  - RHOSLC week window `2025-08-14` through `2025-09-16` now returns `7` Shorts and `2` long-form YouTube videos
- Live Shorts caption-field repair:
  - Historical Shorts rows repaired in place from `title -> description`: `246`
  - Remaining title-only Shorts rows: `0`
