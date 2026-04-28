# Instagram Scrapling Scraper Hardening Plan Audit

Source plan: `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md`

## Verdict

`APPROVED_WITH_REVISIONS`

The source plan identifies the right scraper surfaces and catches real operational risks, especially the healthy-but-unimplemented `ScraplingRuntime`, transport retry classification, stale posts runtime metadata, and cancellation gaps. It is close to executable, but it needs revisions before handoff because several code snippets would fail or create new runtime risk.

## Current-State Fit

Confirmed against the repo:

- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py` currently returns `RuntimeHealth(healthy=True)` when Scrapling imports, then raises `NotImplementedError` from all endpoint methods.
- `InstagramRuntimeDispatcher` only falls through on `RuntimeUnsupported`, so the source plan targets a real dispatcher failure mode.
- `comments_scrapling/job_runner.py` already has a degraded DB summary fallback and captures final `fetcher.runtime_metadata`; `posts_scrapling/job_runner.py` does not.
- `InstagramAuthSession` requires many constructor fields, so the proposed shared-session test that instantiates it with only `cookies`, `browser_account_id`, and `metadata` is invalid.
- `comments_scrapling/job_runner.py` holds a persist connection while iterating target posts. A cancellation check that calls `pg.fetch_one()` without reusing that connection would add another pool checkout inside the hot path.

## Blocking Fixes

1. Fix the shared-session test so it uses `SimpleNamespace` or a fully constructed `InstagramAuthSession`.
2. Change cooperative cancellation to accept an optional connection and use the existing comments persist connection while inside the comment loop.
3. Check cancellation once before opening the comments persist connection so the cancellation test does not require a DB connection checkout.
4. Catch `InstagramPostsWarmupError` in `posts_scrapling/job_runner.py` and preserve `fetcher.runtime_metadata`, matching the comments lane.
5. Fix the runbook markdown patch block so nested code fences do not break the plan.

## Approval Decision

Approved only if implementation uses `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md` as the execution source. The original source plan is useful evidence, but the revised plan closes the current execution blockers.

## Biggest Risks Remaining

- The cancellation check still cannot interrupt a single in-flight Instagram request; it only stops between pages or target posts.
- The plan intentionally keeps `ScraplingRuntime` unsupported rather than implementing the pluggable runtime. That is correct for this hardening pass, but operators may still expect "Scrapling runtime" to mean the posts/comments lanes.
- Broad retry handling can mask proxy instability unless job metadata and runbook guidance are followed.

## Recommended Execution

Use `orchestrate-subagents` with disjoint write scopes:

- Runtime/dispatcher worker.
- Fetcher/retry worker.
- Job-runner/cancellation worker.
- Session/docs worker.

