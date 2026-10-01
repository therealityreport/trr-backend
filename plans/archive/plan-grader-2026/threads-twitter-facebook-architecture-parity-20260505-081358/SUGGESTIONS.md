# Suggestions

These are optional improvements beyond the revised execution plan. They should not block implementation.

1. Add a small smoke wrapper for `twitter`, `facebook`, and `threads` remote-auth probes once the focused tests pass.

2. Add a generated fixture validator that scans `tests/fixtures/socials/run_metadata/` for raw cookie/token-like keys before fixtures are committed.

3. Add a short architecture ledger entry for every compatibility wrapper left in `social_season_analytics_impl.py`, including owner Module and deletion criteria.

4. Add a one-page operator query snippet that finds recent `twitter`, `facebook`, and `threads` remote-auth failures by `last_error_code`.

5. Add import-cycle checks for every `posts_catalog` Module after Twitter/X, Facebook, and Threads land, not just the three new Modules.

6. Add a reusable fake persistence adapter for platform posts catalog tests so each platform does not invent a slightly different mock shape.

7. Add a catalog metadata golden fixture per platform after the first implementation lands, then use it to prevent future field-name drift.

8. Add a narrow benchmark only if the catalog extraction materially changes runtime path length. Do not benchmark before correctness and compatibility are stable.

9. Add a future batch-upsert equivalence checklist for Twitter/X, Facebook, and Threads, but keep it out of this implementation unless requested.

10. Add a follow-up cleanup plan to retire compatibility wrappers after each platform has enough direct Module callers.

11. Add a Modal readiness smoke command section that can be copied as a block during deploy checks.

12. Add a review checklist item that explicitly asks: "Did this change add a worker lane?" for Twitter/X and Facebook PRs.
