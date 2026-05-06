# Optional Suggestions

These are optional follow-ups, not required fixes for execution approval.

1. Title: Add Social Run Metadata Fixtures
   - Type: Medium
   - Why: Metadata shape is central to this work.
   - Where it would apply: `tests/fixtures/socials/`
   - How it could improve the plan: Makes queue/admin metadata regressions easier to catch.

2. Title: Add A One-Page Operator Runbook
   - Type: Small
   - Why: Operators need to know how to interpret new TikTok/YouTube error codes.
   - Where it would apply: `docs/runbooks/social_worker_queue_ops.md`
   - How it could improve the plan: Connects code changes to day-to-day debugging.

3. Title: Add Error-Code Registry Tests
   - Type: Medium
   - Why: Platform error codes drift easily.
   - Where it would apply: social queue status tests
   - How it could improve the plan: Ensures new codes render consistently in queue and run diagnostics.

4. Title: Add YouTube Catalog Golden Fixture
   - Type: Medium
   - Why: YouTube page JSON shape changes frequently.
   - Where it would apply: `tests/fixtures/socials/youtube/`
   - How it could improve the plan: Reduces reliance on hand-built mocks.

5. Title: Add TikTok Fetcher Runtime Metadata Snapshot
   - Type: Small
   - Why: Cookie/proxy metadata must stay safe.
   - Where it would apply: `tests/socials/tiktok/posts_scrapling/test_fetcher.py`
   - How it could improve the plan: Prevents accidental secret/cookie leakage.

6. Title: Add A Local Smoke Wrapper
   - Type: Small
   - Why: Smoke commands are currently separate and easy to forget.
   - Where it would apply: `scripts/socials/`
   - How it could improve the plan: Gives operators one local command for TikTok and YouTube post-path smoke checks.

7. Title: Document Batch-Upsert Decision
   - Type: Small
   - Why: The batch path may be deferred.
   - Where it would apply: `docs/architecture/social-platform-module-checklist.md`
   - How it could improve the plan: Prevents future agents from re-proposing unsafe batch changes without evidence.

8. Title: Add Import-Cycle Check
   - Type: Medium
   - Why: YouTube extraction touches monolith compatibility wrappers.
   - Where it would apply: `tests/repositories/test_social_control_plane_imports.py`
   - How it could improve the plan: Catches extraction-induced circular imports early.

9. Title: Add Queue Dashboard Copy Check
   - Type: Medium
   - Why: New metadata is only useful if admin surfaces expose it coherently.
   - Where it would apply: TRR-APP social queue/admin tests, if backend payloads change later.
   - How it could improve the plan: Keeps operator-facing truth aligned after backend hardening.

10. Title: Add 30-Day Follow-Up Query
   - Type: Small
   - Why: The revised plan names a 30-day outcome.
   - Where it would apply: docs or a lightweight SQL snippet in the runbook.
   - How it could improve the plan: Makes the value claim auditable after adoption.
