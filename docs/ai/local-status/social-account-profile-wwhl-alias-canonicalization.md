# Social Account Profile WWHL Alias Canonicalization

Last updated: 2026-03-30

## Status
- Backend phase complete.

## What changed
- Canonicalized the legacy/mistyped social profile handle alias `wwhlbravo` to the stored Bravo WWHL account handle `bravowwhl` inside the shared social-account profile normalizer.
- This alias normalization now applies across the social account profile surfaces that reuse the same handle normalization path, including summary, posts, hashtags, catalog, and run-progress lookups.
- Added a repository regression test proving `get_social_account_profile_summary("instagram", "wwhlbravo")` resolves through the canonical `bravowwhl` handle.

## Validation
- Passed: `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py`
- Passed: `pytest -q tests/repositories/test_social_season_analytics.py -k 'normalizes_wwhl_alias'`

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-30
  current_phase: "archived continuity note"
  next_action: "Refer to newer status notes if follow-up work resumes on this thread."
  detail: self
```
