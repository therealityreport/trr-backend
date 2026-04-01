# TikTok Profile Mentions Username Resolution

- Date: 2026-03-23 16:03:05 EDT
- Repo: `TRR-Backend`
- Scope: social account profile mentions aggregation and exact-handle search for TikTok shared catalog rows

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-23
  current_phase: "archived continuity note"
  next_action: "Refer to newer status notes if TikTok mention normalization work resumes."
  detail: self
```

## Root Cause

TikTok shared catalog rows mixed three different mention shapes:

- exact usernames like `@paige_desorbo`
- display-label mentions like `@Paige DeSorbo`
- malformed joined tokens like `@paige_desorboand`

The profile mentions aggregate and exact `@handle` search were trusting the stored `mentions` array too literally. Older Bravo TikTok rows only exposed the full mention inside `raw_data.description`, and the profile content helper was not reading that fallback. That let short aliases such as `@Paige` survive as if they were real usernames.

## Fix

- Added TikTok-specific mention resolution in `trr_backend/repositories/social_season_analytics.py`.
- Profile content text now falls back to `raw_data.description`, `raw_data.caption`, and `raw_data.text`.
- Display-name mentions are parsed and resolved to a known exact username when the account dataset already contains that exact handle.
- Joined-token artifacts like `@paige_desorboand` are suppressed once the exact resolved username is known.
- The same resolution path now feeds both:
  - the `Collaborators / Tagged Accounts / Mentions` aggregate
  - the profile posts search endpoint used by caption search

## Verification

- Targeted pytest:
  - `pytest tests/repositories/test_social_season_analytics.py -k 'matches_exact_mentions or search_uses_analysis_rows or resolves_display_mentions or collaborators_tags_separates_mentions_and_tags'`
- Syntax:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py`
- Live proxy checks after backend restart:
  - `GET /api/admin/trr-api/social/profiles/tiktok/bravotv/collaborators-tags`
    - `paige_desorbo` remains
    - `paige` is removed
    - `paige_desorboand` is removed
  - `GET /api/admin/trr-api/social/profiles/tiktok/bravotv/posts?search=%40paige`
    - returns `0`
  - `GET /api/admin/trr-api/social/profiles/tiktok/bravotv/posts?search=%40paige_desorbo`
    - returns `30`

## Notes

- Some other short display aliases still exist on TikTok rows for other people, for example `@lindsay`, where the dataset does not yet provide a provable exact username mapping. Those are outside this Paige-specific fix.
