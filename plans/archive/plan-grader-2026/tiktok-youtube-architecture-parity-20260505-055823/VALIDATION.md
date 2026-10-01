# Validation

## Supplied Context Evidence

- User invoked `[@plan-grader](plugin://plan-grader@local-plugins)` after creation of `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`.
- Source plan was read directly from disk.
- Plan Grader plugin contract and rubric were read from local plugin and rubric files.

## Inspected Files

- `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`
- `docs/architecture/social-platform-module-checklist.md`
- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`
- `trr_backend/socials/tiktok/posts_scrapling/fetcher.py`
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- `trr_backend/socials/instagram/catalog_ingest.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `trr_backend/modal_jobs.py`
- `trr_backend/socials/youtube/`

## Concrete Commands Or Checks

```bash
sed -n '1,260p' docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md
nl -ba trr_backend/socials/tiktok/posts_scrapling/job_runner.py | sed -n '1,240p'
nl -ba trr_backend/socials/tiktok/posts_scrapling/fetcher.py | sed -n '1,190p'
nl -ba trr_backend/socials/social_season_analytics_impl.py | sed -n '2440,2515p'
nl -ba trr_backend/socials/social_season_analytics_impl.py | sed -n '31560,31720p'
find trr_backend/socials/youtube -maxdepth 2 -type f | sort
git status --short
```

## Expected Results

- TikTok job runner evidence shows no cancellation helper and direct final run finalization/read.
- TikTok fetcher evidence shows hardcoded `TIKTOK_POST_PAGE_SIZE = 30`.
- Remote auth evidence shows `probe_remote_auth_health()` rejects non-Instagram platforms.
- YouTube evidence shows shared catalog orchestration in the monolith and no `posts_catalog/` module.
- Git status shows a dirty worktree, so execution must preserve unrelated changes.

## Evidence Gaps

- No tests were run during Plan Grader artifact creation.
- No live Modal or Supabase checks were run.
- Batch-upsert equivalence for TikTok and YouTube was not proven; revised plan treats it as optional and gated.
- App-facing impacts were not inspected because the revised plan keeps API/route payloads unchanged.

## Local / Cloud Assumptions

- Work is local backend repo work.
- Modal checks are optional smoke validation after unit tests.
- Supabase schema is treated as unchanged for this plan.

## Unresolved Thread / File Conflict Risks

The backend worktree is already dirty across social-control-plane, Instagram, TikTok, Modal readiness, and tests. Execution must inspect diffs before editing and avoid reverting unrelated user changes.

High-risk overlap files:

- `trr_backend/socials/social_season_analytics_impl.py`
- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`
- `trr_backend/modal_jobs.py`
- `scripts/modal/verify_modal_readiness.py`
- `tests/repositories/test_social_season_analytics.py`
- `tests/socials/tiktok/posts_scrapling/test_job_runner.py`
