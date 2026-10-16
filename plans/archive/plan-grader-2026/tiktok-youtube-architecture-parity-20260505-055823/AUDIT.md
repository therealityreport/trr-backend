# Plan Grader Audit

Source plan: `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`

## VERDICT

APPROVE AFTER REVISION.

The source plan is useful and grounded in the current repo, but it should not be handed directly to an implementation agent without tightening. The required `REVISED_PLAN.md` in this package is the execution-ready version.

## Current-State Fit

The source plan correctly identifies the main current-state gaps:

- TikTok already has the expected posts lane shape in `trr_backend/socials/tiktok/posts_scrapling/`, and the checklist confirms that lane shape is intended.
- TikTok job execution is thinner than Instagram: `run_tiktok_posts_scrapling_job()` finishes and fetches the job row directly without cancellation or degraded DB handling.
- TikTok page size is currently a hardcoded `TIKTOK_POST_PAGE_SIZE = 30`.
- `probe_remote_auth_health()` currently rejects every platform except Instagram even though TikTok is treated as remote-auth-required elsewhere.
- YouTube catalog orchestration still lives in `_scrape_shared_youtube_posts()` inside `social_season_analytics_impl.py`; there is no `trr_backend/socials/youtube/posts_catalog/` module today.

## Benefit Score

High. If implemented, the work reduces operator uncertainty in exactly the active social scraper paths: TikTok posts, YouTube catalog backfills, and shared catalog persistence. The benefit is strongest for failure diagnosis and retry/cancel behavior.

## Biggest Risks

1. The source plan says to add a YouTube `job_runner.py`, but no new YouTube stage or worker lane is authorized. This could cause an executor to invent a new queue surface.
2. The source plan asks for batch upsert parity, but TikTok and YouTube payload construction depends on optional columns and assignment behavior. Batch upsert must remain gated behind contract equivalence.
3. The source plan has validation commands, but not enough expected pre-fix failing assertions. The revised plan adds a test-first phase.
4. The original handoff recommends sequential execution. TikTok and YouTube can be split into parallel workstreams after a short coordination phase, with integration kept in the main thread.

## Approval Decision

Do not execute the original source plan as-is. Execute `REVISED_PLAN.md`.

## Supplied-Context Evidence

- User requested Plan Grader immediately after the plan was written.
- Source plan path: `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`.
- Repo instructions require current files to be treated as authority and saved notes as stale.

## Thread / File Conflict Risks

The worktree is already dirty. The revised plan calls this out and requires an executor to inspect current diffs before editing:

- `trr_backend/socials/social_season_analytics_impl.py`
- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`
- `trr_backend/socials/tiktok/posts_scrapling/session.py`
- `trr_backend/modal_jobs.py`
- `scripts/modal/verify_modal_readiness.py`
- many social-control-plane tests

Treat those as active user/workday changes, not disposable state.
